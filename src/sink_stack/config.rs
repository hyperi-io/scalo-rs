// Project:   scalo
// File:      src/sink_stack/config.rs
// Purpose:   Sink-control stack configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sink-control stack configuration.

use std::time::Duration;

use backon::ExponentialBuilder;
use serde::{Deserialize, Serialize};

use crate::governor::RateLimitConfig;

/// Configuration for a [`SinkStack`](super::SinkStack).
///
/// All knobs default to behaviour-preserving values: no concurrency cap, no
/// rate limit, no load-shedding, a generous per-attempt timeout, and the same
/// 3-retry exponential schedule used elsewhere. An app opts into tighter
/// control by setting the relevant field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkStackConfig {
    /// Max in-flight send attempts. `0` = unlimited (no concurrency gate).
    /// Ignored when [`adaptive`](Self::adaptive) is set (ARC owns concurrency).
    #[serde(default)]
    pub max_concurrency: usize,

    /// Adaptive request concurrency (AIMD). When set, the sink discovers the
    /// downstream's safe concurrency from RTT/error feedback instead of using
    /// the static `max_concurrency` cap. Opt-in; `None` keeps the fixed gate.
    #[serde(default)]
    pub adaptive: Option<AdaptiveConfig>,

    /// Per-attempt timeout in milliseconds. Bounds a single send attempt (not
    /// the rate-limit admission wait, which sits outside it). Default 30000.
    #[serde(default = "default_attempt_timeout_ms")]
    pub attempt_timeout_ms: u64,

    /// Shed (fail fast) instead of queueing when the concurrency gate is full.
    /// Default `false` (queue/backpressure). Only meaningful with a non-zero
    /// `max_concurrency`. Default false.
    #[serde(default)]
    pub load_shed: bool,

    /// Maximum retry attempts for a transient failure. Default 3.
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Minimum (first) retry backoff in milliseconds. Default 100.
    #[serde(default = "default_min_backoff_ms")]
    pub min_backoff_ms: u64,

    /// Maximum retry backoff in milliseconds. Default 30000. Default 30s.
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,

    /// Outbound rate limit (GCRA token bucket). `rps == 0` disables it.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

fn default_attempt_timeout_ms() -> u64 {
    30_000
}
fn default_max_retries() -> usize {
    3
}
fn default_min_backoff_ms() -> u64 {
    100
}
fn default_max_backoff_ms() -> u64 {
    30_000
}

impl Default for SinkStackConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 0,
            adaptive: None,
            attempt_timeout_ms: default_attempt_timeout_ms(),
            load_shed: false,
            max_retries: default_max_retries(),
            min_backoff_ms: default_min_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

impl SinkStackConfig {
    /// Load from the config cascade under the `sink_stack` key (or defaults).
    #[must_use]
    pub fn from_cascade() -> Self {
        Self::from_cascade_key("sink_stack")
    }

    /// Load from the config cascade under an explicit `key` (or defaults). Lets a
    /// runtime with multiple sinks give each its own `sink_stack`-shaped section.
    #[must_use]
    pub fn from_cascade_key(key: &str) -> Self {
        #[cfg(feature = "config")]
        {
            if let Some(cfg) = crate::config::try_get()
                && let Ok(s) = cfg.unmarshal_key_registered::<Self>(key)
            {
                return s;
            }
        }
        #[cfg(not(feature = "config"))]
        let _ = key;
        Self::default()
    }

    /// Per-attempt timeout as a `Duration`. A configured `0` is treated as a
    /// very long timeout (effectively disabled) so a misconfiguration cannot
    /// instantly fail every send.
    #[must_use]
    pub fn attempt_timeout(&self) -> Duration {
        if self.attempt_timeout_ms == 0 {
            Duration::from_secs(86_400)
        } else {
            Duration::from_millis(self.attempt_timeout_ms)
        }
    }

    /// Build the jittered exponential backoff schedule from config (the
    /// `max_times` cap is applied at send time from `max_retries`).
    #[must_use]
    pub fn backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(self.min_backoff_ms))
            .with_max_delay(Duration::from_millis(self.max_backoff_ms))
            .with_jitter()
    }
}

/// Adaptive request concurrency (AIMD) configuration.
///
/// Loss-based additive-increase / multiplicative-decrease: raise the in-flight
/// limit by `increase_by` while the downstream is healthy and the limit is well
/// used, multiply it by `decrease_factor` on an overload signal (a sink
/// backpressure or a per-attempt timeout). The limit is clamped to
/// `[min_limit, max_limit]`; `min_limit` floors at 1 so a failing downstream can
/// never drive concurrency to zero (which would deadlock the sink).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Starting concurrency limit. Default 10.
    #[serde(default = "default_initial_limit")]
    pub initial_limit: usize,
    /// Floor for the limit (>= 1). Default 1.
    #[serde(default = "default_min_limit")]
    pub min_limit: usize,
    /// Ceiling for the limit. Default 100.
    #[serde(default = "default_max_limit")]
    pub max_limit: usize,
    /// Additive increase per healthy, well-utilised window. Default 1.
    #[serde(default = "default_increase_by")]
    pub increase_by: usize,
    /// Multiplicative decrease on an overload signal. Clamped to `[0.5, 1.0)`.
    /// Default 0.5.
    #[serde(default = "default_decrease_factor")]
    pub decrease_factor: f64,
}

fn default_initial_limit() -> usize {
    10
}
fn default_min_limit() -> usize {
    1
}
fn default_max_limit() -> usize {
    100
}
fn default_increase_by() -> usize {
    1
}
fn default_decrease_factor() -> f64 {
    0.5
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_limit: default_initial_limit(),
            min_limit: default_min_limit(),
            max_limit: default_max_limit(),
            increase_by: default_increase_by(),
            decrease_factor: default_decrease_factor(),
        }
    }
}

impl AdaptiveConfig {
    /// Build the AIMD concurrency limiter from this config. Field clamping (min
    /// floored at 1 -- the deadlock guard; initial into `[min, max]`;
    /// decrease_factor into `[0.5, 1.0)`) happens in
    /// [`AdaptiveLimiter::new`](super::adaptive::AdaptiveLimiter::new).
    #[must_use]
    pub fn build_limiter(&self) -> std::sync::Arc<super::adaptive::AdaptiveLimiter> {
        super::adaptive::AdaptiveLimiter::new(
            self.initial_limit,
            self.min_limit,
            self.max_limit,
            self.increase_by,
            self.decrease_factor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_behaviour() {
        let c = SinkStackConfig::default();
        assert_eq!(c.max_concurrency, 0, "no concurrency cap by default");
        assert!(c.adaptive.is_none(), "ARC off by default (static gate)");
        assert!(!c.load_shed, "queue, do not shed, by default");
        assert_eq!(c.max_retries, 3);
        assert!(!c.rate_limit.is_enabled(), "no rate limit by default");
        assert_eq!(c.attempt_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn zero_timeout_means_effectively_disabled() {
        let c = SinkStackConfig {
            attempt_timeout_ms: 0,
            ..Default::default()
        };
        assert!(c.attempt_timeout() >= Duration::from_secs(3600));
    }

    #[test]
    fn deserialise_from_yaml() {
        let yaml = r"
max_concurrency: 8
attempt_timeout_ms: 5000
load_shed: true
max_retries: 5
rate_limit:
  rps: 100
";
        let c: SinkStackConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(c.max_concurrency, 8);
        assert_eq!(c.attempt_timeout_ms, 5000);
        assert!(c.load_shed);
        assert_eq!(c.max_retries, 5);
        assert_eq!(c.rate_limit.rps, 100);
    }
}
