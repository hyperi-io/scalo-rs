// Project:   scalo
// File:      src/tiered_sink/config.rs
// Purpose:   TieredSink configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TieredSink configuration.

use crate::tiered_sink::CompressionCodec;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for TieredSink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredSinkConfig {
    /// Path to the spool file for disk fallback.
    pub spool_path: PathBuf,

    /// Timeout for primary sink operations.
    /// If exceeded, message is spooled to disk.
    #[serde(default = "default_send_timeout_ms")]
    pub send_timeout_ms: u64,

    /// Compression codec for spooled messages.
    #[serde(default)]
    pub compression: CompressionCodec,

    /// Strategy for draining spooled messages back to primary.
    #[serde(default)]
    pub drain_strategy: DrainStrategy,

    /// Ordering mode for message delivery.
    #[serde(default)]
    pub ordering: OrderingMode,

    /// Maximum spool file size in bytes.
    /// Spool operations fail when exceeded.
    #[serde(default)]
    pub max_spool_bytes: Option<u64>,

    /// Maximum messages in spool.
    #[serde(default)]
    pub max_spool_items: Option<usize>,

    /// Policy applied when the spool is full (see [`WhenFull`]).
    /// Default `Block` (lossless backpressure -- preserves current behaviour).
    #[serde(default)]
    pub when_full: WhenFull,

    /// Circuit breaker: failures before opening circuit.
    #[serde(default = "default_circuit_failure_threshold")]
    pub circuit_failure_threshold: u32,

    /// Circuit breaker: how long to wait before probing.
    #[serde(default = "default_circuit_reset_timeout_ms")]
    pub circuit_reset_timeout_ms: u64,

    /// Interval for drain task to check spool.
    #[serde(default = "default_drain_interval_ms")]
    pub drain_interval_ms: u64,

    /// Disk-aware capacity management. When configured, a background poller
    /// checks available disk space and stops spooling if the filesystem
    /// exceeds the configured usage threshold.
    #[serde(default)]
    pub disk_aware: Option<DiskAwareConfig>,
}

fn default_send_timeout_ms() -> u64 {
    1000 // 1 second
}

fn default_circuit_failure_threshold() -> u32 {
    5
}

fn default_circuit_reset_timeout_ms() -> u64 {
    30_000 // 30 seconds
}

fn default_drain_interval_ms() -> u64 {
    100 // 100ms
}

impl TieredSinkConfig {
    /// Create a new config with the given spool path.
    #[must_use]
    pub fn new(spool_path: impl Into<PathBuf>) -> Self {
        Self {
            spool_path: spool_path.into(),
            send_timeout_ms: default_send_timeout_ms(),
            compression: CompressionCodec::default(),
            drain_strategy: DrainStrategy::default(),
            ordering: OrderingMode::default(),
            max_spool_bytes: None,
            max_spool_items: None,
            when_full: WhenFull::default(),
            circuit_failure_threshold: default_circuit_failure_threshold(),
            circuit_reset_timeout_ms: default_circuit_reset_timeout_ms(),
            drain_interval_ms: default_drain_interval_ms(),
            disk_aware: None,
        }
    }

    /// Set send timeout.
    #[must_use]
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self
    }

    /// Set compression codec.
    #[must_use]
    pub fn compression(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    /// Set drain strategy.
    #[must_use]
    pub fn drain_strategy(mut self, strategy: DrainStrategy) -> Self {
        self.drain_strategy = strategy;
        self
    }

    /// Set ordering mode.
    #[must_use]
    pub fn ordering(mut self, mode: OrderingMode) -> Self {
        self.ordering = mode;
        self
    }

    /// Set maximum spool size.
    #[must_use]
    pub fn max_spool_bytes(mut self, max: u64) -> Self {
        self.max_spool_bytes = Some(max);
        self
    }

    /// Set the spool-full overflow policy.
    #[must_use]
    pub fn when_full(mut self, policy: WhenFull) -> Self {
        self.when_full = policy;
        self
    }

    /// Enable disk-aware capacity management with default settings.
    #[must_use]
    pub fn disk_aware(mut self, config: DiskAwareConfig) -> Self {
        self.disk_aware = Some(config);
        self
    }

    /// Get send timeout as Duration.
    #[must_use]
    pub fn send_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.send_timeout_ms)
    }

    /// Get circuit reset timeout as Duration.
    #[must_use]
    pub fn circuit_reset_timeout(&self) -> Duration {
        Duration::from_millis(self.circuit_reset_timeout_ms)
    }

    /// Get drain interval as Duration.
    #[must_use]
    pub fn drain_interval(&self) -> Duration {
        Duration::from_millis(self.drain_interval_ms)
    }
}

/// Configuration for disk-aware capacity management.
///
/// When enabled, a background poller checks filesystem usage and pauses
/// spool writes when the disk exceeds the configured threshold. Writes
/// resume automatically when space is recovered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskAwareConfig {
    /// Maximum filesystem usage percentage (0.0 - 1.0) before pausing spool writes.
    /// Default: 0.8 (80%).
    #[serde(default = "default_max_usage_percent")]
    pub max_usage_percent: f64,

    /// How often to check disk usage, in seconds.
    /// Default: 5 seconds.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

fn default_max_usage_percent() -> f64 {
    0.8
}

fn default_poll_interval_secs() -> u64 {
    5
}

impl Default for DiskAwareConfig {
    fn default() -> Self {
        Self {
            max_usage_percent: default_max_usage_percent(),
            poll_interval_secs: default_poll_interval_secs(),
        }
    }
}

/// Strategy for draining spooled messages back to the primary sink.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DrainStrategy {
    /// Adaptive rate: starts slow, speeds up based on success rate.
    /// This is the default and recommended strategy.
    Adaptive {
        /// Initial drain rate (messages per second).
        #[serde(default = "default_initial_rate")]
        initial_rate: usize,
        /// Maximum drain rate (messages per second).
        #[serde(default = "default_max_rate")]
        max_rate: usize,
    },

    /// Fixed rate limit (messages per second).
    RateLimited {
        /// Messages per second.
        msgs_per_sec: usize,
    },

    /// Drain as fast as possible.
    /// Use with caution - may overwhelm a recovering sink.
    Greedy,
}

fn default_initial_rate() -> usize {
    100
}

fn default_max_rate() -> usize {
    10_000
}

impl Default for DrainStrategy {
    fn default() -> Self {
        Self::Adaptive {
            initial_rate: default_initial_rate(),
            max_rate: default_max_rate(),
        }
    }
}

impl DrainStrategy {
    /// Create adaptive strategy with custom rates.
    #[must_use]
    pub fn adaptive(initial_rate: usize, max_rate: usize) -> Self {
        Self::Adaptive {
            initial_rate,
            max_rate,
        }
    }

    /// Create rate-limited strategy.
    #[must_use]
    pub fn rate_limited(msgs_per_sec: usize) -> Self {
        Self::RateLimited { msgs_per_sec }
    }
}

/// Policy applied when the spool (disk fallback) is full -- a write that would
/// exceed `max_spool_bytes` / `max_spool_items`.
///
/// `Block` (default) preserves scalo's lossless contract: the write errors so
/// the caller backpressures the inbound source (doctrine: gate the source,
/// never silently drop the drain). The other variants are EXPLICIT opt-in loss
/// / diversion and emit `tiered_sink_overflow_total{policy}` (plus a
/// `tiered_sink_dropped_total{policy}` for the drop variants) so loss is never
/// silent and always alertable.
///
/// Pattern cribbed from Vector's `WhenFull` (block / drop_newest / overflow);
/// `Dlq` is the scalo no-silent-drop addition (overflow goes to the DLQ instead
/// of being dropped or crashing the spool).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenFull {
    /// Backpressure: the spool write fails so the caller slows the inbound
    /// source. Lossless. Default (matches pre-2.10 behaviour).
    #[default]
    Block,
    /// Divert the overflowing record to the DLQ -- no silent loss, no crash.
    Dlq,
    /// Drop the incoming (newest) record. Explicit opt-in loss; keeps the
    /// older, lower-latency records already spooled.
    DropNewest,
    /// Drop the oldest spooled record to make room for the newest. Explicit
    /// opt-in loss; keeps the freshest data.
    DropOldest,
}

/// Ordering mode for message delivery during drain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    /// New messages go hot path, spool drains in background (default).
    /// Maximizes throughput with slight ordering relaxation.
    /// New messages may arrive before older spooled messages.
    #[default]
    Interleaved,

    /// Drain spool completely before new messages use hot path.
    /// Guarantees strict FIFO ordering but blocks new traffic during drain.
    StrictFifo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TieredSinkConfig::new("/tmp/test.queue");
        assert_eq!(config.send_timeout_ms, 1000);
        assert_eq!(config.compression, CompressionCodec::default());
        assert!(matches!(
            config.compression,
            CompressionCodec::Zstd { level: 1 }
        ));
        assert!(matches!(
            config.drain_strategy,
            DrainStrategy::Adaptive { .. }
        ));
        assert_eq!(config.ordering, OrderingMode::Interleaved);
        assert_eq!(config.circuit_failure_threshold, 5);
        assert!(config.disk_aware.is_none());
    }

    #[test]
    fn test_builder_pattern() {
        let config = TieredSinkConfig::new("/tmp/test.queue")
            .send_timeout(Duration::from_secs(5))
            .compression(CompressionCodec::Snappy)
            .drain_strategy(DrainStrategy::Greedy)
            .ordering(OrderingMode::StrictFifo)
            .max_spool_bytes(1024 * 1024 * 100);

        assert_eq!(config.send_timeout_ms, 5000);
        assert_eq!(config.compression, CompressionCodec::Snappy);
        assert!(matches!(config.drain_strategy, DrainStrategy::Greedy));
        assert_eq!(config.ordering, OrderingMode::StrictFifo);
        assert_eq!(config.max_spool_bytes, Some(100 * 1024 * 1024));
    }

    #[test]
    fn test_drain_strategy_constructors() {
        let adaptive = DrainStrategy::adaptive(50, 5000);
        assert!(matches!(
            adaptive,
            DrainStrategy::Adaptive {
                initial_rate: 50,
                max_rate: 5000
            }
        ));

        let rate_limited = DrainStrategy::rate_limited(1000);
        assert!(matches!(
            rate_limited,
            DrainStrategy::RateLimited { msgs_per_sec: 1000 }
        ));
    }

    #[test]
    fn test_duration_conversions() {
        let config = TieredSinkConfig::new("/tmp/test.queue");
        assert_eq!(config.send_timeout_duration(), Duration::from_secs(1));
        assert_eq!(config.circuit_reset_timeout(), Duration::from_secs(30));
        assert_eq!(config.drain_interval(), Duration::from_millis(100));
    }
}
