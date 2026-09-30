// Project:   scalo
// File:      src/metrics/groups/sink.rs
// Purpose:   DFE sink metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sink/insert metrics for apps with a downstream.

use metrics::Gauge;

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

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

        // sink_duration_seconds -- label-based, register descriptor manually
        metrics::describe_histogram!(
            "sink_duration_seconds",
            metrics::Unit::Seconds,
            "Sink write latency"
        );
        manager.registry().push(MetricDescriptor {
            name: "sink_duration_seconds".into(),
            metric_type: MetricType::Histogram,
            description: "Sink write latency".into(),
            unit: "seconds".into(),
            labels: vec!["backend".into()],
            group: "sink".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // sink_errors_total -- label-based
        metrics::describe_counter!("sink_errors_total", "Sink write errors");
        manager.registry().push(MetricDescriptor {
            name: "sink_errors_total".into(),
            metric_type: MetricType::Counter,
            description: "Sink write errors".into(),
            unit: String::new(),
            labels: vec!["backend".into()],
            group: "sink".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // bytes_sent_total -- label-based
        metrics::describe_counter!("bytes_sent_total", "Bytes sent to sink");
        manager.registry().push(MetricDescriptor {
            name: "bytes_sent_total".into(),
            metric_type: MetricType::Counter,
            description: "Bytes sent to sink".into(),
            unit: String::new(),
            labels: vec!["format".into()],
            group: "sink".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

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
