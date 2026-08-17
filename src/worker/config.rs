// Project:   scalo
// File:      src/worker/config.rs
// Purpose:   Configuration for adaptive worker pool
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use serde::{Deserialize, Serialize};

/// Configuration for the adaptive worker pool.
///
/// All values are overridable via the 7-layer config cascade
/// (CLI > ENV > .env > settings.{env}.yaml > settings.yaml > defaults > scalo > hard-coded).
///
/// Every field is also emitted as a gauge metric for Grafana overlay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPoolConfig {
    /// Minimum active worker threads (floor for scaling).
    #[serde(default = "default_min_threads")]
    pub min_threads: usize,

    /// Maximum worker threads. 0 = auto-detect from cgroup / `available_parallelism`.
    #[serde(default)]
    pub max_threads: usize,

    /// CPU utilisation below this threshold triggers thread growth.
    #[serde(default = "default_grow_below")]
    pub grow_below: f64,

    /// CPU utilisation above this threshold triggers gentle thread reduction.
    #[serde(default = "default_shrink_above")]
    pub shrink_above: f64,

    /// CPU utilisation above this threshold triggers aggressive thread reduction.
    #[serde(default = "default_emergency_above")]
    pub emergency_above: f64,

    /// Memory pressure above this threshold hard-caps threads at `min_threads`.
    #[serde(default = "default_memory_pressure_cap")]
    pub memory_pressure_cap: f64,

    /// How often to re-evaluate scaling (seconds).
    #[serde(default = "default_scale_interval_secs")]
    pub scale_interval_secs: u64,

    /// Maximum concurrent async fan-out tasks.
    #[serde(default = "default_async_concurrency")]
    pub async_concurrency: usize,

    /// Seconds the pool must be saturated before reporting unhealthy.
    #[serde(default = "default_health_saturation_timeout_secs")]
    pub health_saturation_timeout_secs: u64,
}

fn default_min_threads() -> usize {
    2
}
fn default_grow_below() -> f64 {
    0.60
}
fn default_shrink_above() -> f64 {
    0.85
}
fn default_emergency_above() -> f64 {
    0.95
}
fn default_memory_pressure_cap() -> f64 {
    0.80
}
fn default_scale_interval_secs() -> u64 {
    5
}
fn default_async_concurrency() -> usize {
    32
}
fn default_health_saturation_timeout_secs() -> u64 {
    30
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            min_threads: default_min_threads(),
            max_threads: 0,
            grow_below: default_grow_below(),
            shrink_above: default_shrink_above(),
            emergency_above: default_emergency_above(),
            memory_pressure_cap: default_memory_pressure_cap(),
            scale_interval_secs: default_scale_interval_secs(),
            async_concurrency: default_async_concurrency(),
            health_saturation_timeout_secs: default_health_saturation_timeout_secs(),
        }
    }
}

impl WorkerPoolConfig {
    /// Load config from the cascade under the given key (e.g. "worker_pool").
    ///
    /// Falls back to defaults if the config cascade is not initialised or the
    /// key is absent. A DERIVED (defaulted, not user-set) `min_threads` that
    /// exceeds the CPU-derived thread ceiling is clamped down with an INFO log
    /// so small-CPU containers resolve to a working pool; a user-explicit
    /// `min_threads` is never clamped. Validates after loading.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails (e.g. thresholds out of order, or
    /// a user-explicit `min_threads > max_threads`).
    pub fn from_cascade(key: &str) -> Result<Self, crate::config::ConfigError> {
        let (mut pool_cfg, min_explicit) = if let Some(cfg) = crate::config::try_get() {
            let parsed: Self = cfg.unmarshal_key(key).unwrap_or_default();
            // contains() separates a user-supplied min_threads from the serde default.
            let explicit = cfg.contains(&format!("{key}.min_threads"));
            (parsed, explicit)
        } else {
            tracing::debug!("Config cascade not initialised, using default WorkerPoolConfig");
            (Self::default(), false)
        };
        if !min_explicit {
            pool_cfg.clamp_derived_min(detected_parallelism());
        }
        pool_cfg.validate()?;
        Ok(pool_cfg)
    }

    /// Validate configuration invariants.
    ///
    /// # Errors
    ///
    /// Returns an error if thresholds are out of order or min > max.
    pub fn validate(&self) -> Result<(), crate::config::ConfigError> {
        if self.max_threads != 0 && self.min_threads > self.max_threads {
            return Err(crate::config::ConfigError::InvalidValue {
                key: "worker_pool.min_threads".into(),
                reason: format!(
                    "min_threads ({}) > max_threads ({})",
                    self.min_threads, self.max_threads
                ),
            });
        }
        if self.grow_below >= self.shrink_above {
            return Err(crate::config::ConfigError::InvalidValue {
                key: "worker_pool.grow_below".into(),
                reason: format!(
                    "grow_below ({}) >= shrink_above ({})",
                    self.grow_below, self.shrink_above
                ),
            });
        }
        if self.shrink_above >= self.emergency_above {
            return Err(crate::config::ConfigError::InvalidValue {
                key: "worker_pool.shrink_above".into(),
                reason: format!(
                    "shrink_above ({}) >= emergency_above ({})",
                    self.shrink_above, self.emergency_above
                ),
            });
        }
        // `fan_out_async` does `step_by(async_concurrency)`; 0 panics.
        if self.async_concurrency == 0 {
            return Err(crate::config::ConfigError::InvalidValue {
                key: "worker_pool.async_concurrency".into(),
                reason: "must be >= 1 (fan_out_async iterates via step_by)".into(),
            });
        }
        // Zero CPU workers leaves the rayon semaphore
        // spinning in `yield_now()` forever. Reject upfront; the
        // scaler's clamp uses min_threads as the floor, so this
        // also guarantees the scaler never drives permits below 1.
        if self.min_threads == 0 {
            return Err(crate::config::ConfigError::InvalidValue {
                key: "worker_pool.min_threads".into(),
                reason: "must be >= 1 (zero permits busy-spin the pool)".into(),
            });
        }
        Ok(())
    }

    /// Resolve `max_threads` to the effective CPU count.
    ///
    /// - `max_threads = 0` -> auto-detect from `available_parallelism` (cgroup-aware)
    /// - `max_threads > 0` -> cap at `min(configured, available_parallelism)`
    ///   to avoid creating more threads than physical cores
    pub fn resolve_max_threads(&mut self) {
        self.max_threads = self.effective_max(detected_parallelism());
    }

    /// The value `max_threads` resolves to for a given detected CPU count.
    fn effective_max(&self, available: usize) -> usize {
        if self.max_threads == 0 {
            available
        } else {
            self.max_threads.min(available)
        }
    }

    /// Clamp a DERIVED (defaulted, not user-set) `min_threads` down to the
    /// ceiling `max_threads` will resolve to, so a small-CPU container gets a
    /// working pool instead of a min > max validation failure. Callers must
    /// skip this for a user-explicit `min_threads` -- a contradictory explicit
    /// pair keeps failing `validate()`.
    fn clamp_derived_min(&mut self, available: usize) {
        let ceiling = self.effective_max(available).max(1);
        if self.min_threads > ceiling {
            tracing::info!(
                derived_min = self.min_threads,
                clamped_min = ceiling,
                available,
                "worker_pool min_threads default exceeds the CPU-derived max_threads; clamping min down to max"
            );
            self.min_threads = ceiling;
        }
    }
}

/// Effective CPU count: `available_parallelism` (cgroup-aware), falling back
/// to 4 when detection fails.
fn detected_parallelism() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: `async_concurrency: 0` previously
    /// passed validation and panicked at `step_by(0)` in
    /// `fan_out_async`.
    #[test]
    fn validate_rejects_zero_async_concurrency() {
        let cfg = WorkerPoolConfig {
            async_concurrency: 0,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(
            err,
            crate::config::ConfigError::InvalidValue { .. }
        ));
    }

    #[test]
    fn validate_accepts_one_async_concurrency() {
        let cfg = WorkerPoolConfig {
            async_concurrency: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    /// Regression (#21): a 1-CPU cgroup derives max_threads = 1 while the
    /// default min_threads is 2; the derived min clamps down to the ceiling
    /// instead of failing validation.
    #[test]
    fn one_cpu_derivation_clamps_default_min() {
        let mut cfg = WorkerPoolConfig::default();
        cfg.clamp_derived_min(1);
        assert_eq!(cfg.min_threads, 1);
        cfg.max_threads = cfg.effective_max(1);
        assert_eq!(cfg.max_threads, 1);
        assert!(cfg.validate().is_ok());
    }

    /// A user-explicit `min_threads` is never clamped: from_cascade skips the
    /// clamp, so a 1-CPU resolution still ends in the existing error.
    #[test]
    fn one_cpu_explicit_min_still_fails_validation() {
        let mut cfg = WorkerPoolConfig {
            min_threads: 4,
            ..Default::default()
        };
        cfg.max_threads = cfg.effective_max(1);
        let err = cfg.validate().unwrap_err();
        assert!(matches!(
            err,
            crate::config::ConfigError::InvalidValue { .. }
        ));
    }

    /// The clamp is a no-op when the ceiling already accommodates the default.
    #[test]
    fn clamp_derived_min_noop_when_ceiling_suffices() {
        let mut cfg = WorkerPoolConfig::default();
        cfg.clamp_derived_min(8);
        assert_eq!(cfg.min_threads, 2);
    }

    /// Regression: `min_threads: 0` previously passed
    /// validation and pinned the rayon semaphore in a yield loop.
    #[test]
    fn validate_rejects_zero_min_threads() {
        let cfg = WorkerPoolConfig {
            min_threads: 0,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(
            err,
            crate::config::ConfigError::InvalidValue { .. }
        ));
    }
}
