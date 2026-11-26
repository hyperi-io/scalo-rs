// Project:   scalo
// File:      src/metrics/groups/sink.rs
// Purpose:   Sink metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sink/insert metrics for apps with a downstream.

use metrics::Gauge;

use super::super::MetricsManager;
use super::super::Series;

/// Sink write metrics.
///
/// Tracks write latency, errors, bytes sent, and concurrent insert count.
#[derive(Clone)]
pub struct SinkMetrics {
    pub concurrent_inserts: Gauge,
}

impl SinkMetrics {
    #[must_use]
    pub fn new(manager: &MetricsManager) -> Self {
        // All names are BARE -- the prefix layer on the global recorder and the
        // manifest registry apply the namespace.

        manager.registry().describe(
            "sink",
            &[
                Series::histogram("sink_duration_seconds", "Sink write latency")
                    .labels(&["backend"]),
                Series::counter("sink_errors_total", "Sink write errors").labels(&["backend"]),
                Series::counter("bytes_sent_total", "Bytes sent to sink").labels(&["format"]),
            ],
        );

        Self {
            concurrent_inserts: manager.gauge_with_labels(
                "concurrent_inserts",
                "In-flight insert/write operations",
                &[],
                "sink",
            ),
        }
    }

    /// Record a sink write with backend label.
    #[inline]
    pub fn record_duration(&self, backend: &str, seconds: f64) {
        metrics::histogram!("sink_duration_seconds", "backend" => backend.to_string())
            .record(seconds);
    }

    /// Record a sink write error with backend label.
    #[inline]
    pub fn record_error(&self, backend: &str) {
        metrics::counter!("sink_errors_total", "backend" => backend.to_string()).increment(1);
    }

    /// Record bytes sent with format label.
    #[inline]
    pub fn record_bytes_sent(&self, format: &str, bytes: u64) {
        metrics::counter!("bytes_sent_total", "format" => format.to_string()).increment(bytes);
    }

    #[inline]
    pub fn set_concurrent_inserts(&self, count: usize) {
        self.concurrent_inserts.set(count as f64);
    }
}
