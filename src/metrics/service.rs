// Project:   scalo
// File:      src/metrics/service.rs
// Purpose:   Standard DFE metric definitions with transport labels
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Standard DFE metrics for pipeline components (receiver, loader, engine).
//!
//! Call [`ServiceMetrics::register`] **after** creating a
//! [`MetricsManager`](super::MetricsManager): the manager must exist so platform
//! metrics land in the manifest registry. Methods are `#[inline]` for hot-path use.
//!
//! ## Example
//!
//! ```rust,no_run
//! use scalo::metrics::{MetricsManager, ServiceMetrics, TransportKind};
//!
//! let mgr = MetricsManager::new("myapp");
//! let svc = ServiceMetrics::register(&mgr);
//!
//! svc.transport_sent(TransportKind::Kafka, 100);
//! svc.records_received(500);
//! svc.scaling_pressure(42.0);
//! ```

use super::manifest::{MetricDescriptor, MetricType};

/// Standard DFE metric set: labelled counters, gauges, and histograms across
/// transport, pipeline, records, scaling, spool, and security.
///
/// Construct via [`ServiceMetrics::register`] -- describes all metrics with the
/// global recorder AND pushes descriptors into the manifest registry.
pub struct ServiceMetrics {
    /// Prevent external construction.
    _private: (),
}

/// Deprecated brand alias for [`ServiceMetrics`]. Removed before GA.
#[deprecated(since = "2.9.0", note = "renamed to ServiceMetrics; removed before GA")]
pub type DfeMetrics = ServiceMetrics;

impl ServiceMetrics {
    /// Register all DFE metric descriptions with the global recorder and
    /// manifest registry. Call **once** after creating a
    /// [`MetricsManager`](super::MetricsManager). Returned handle is zero-sized
    /// (recording goes through the global `metrics!` macros).
    ///
    /// **Breaking change (v1.22):** takes `&MetricsManager` so platform metrics
    /// are tightly coupled with the manifest registry.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn register(manager: &super::MetricsManager) -> Self {
        let reg = manager.registry();

        // --- Transport ---
        metrics::describe_counter!(
            "transport_sent_total",
            "Messages successfully sent to transport"
        );
        metrics::describe_counter!(
            "transport_send_errors_total",
            "Messages that failed to send"
        );
        metrics::describe_counter!(
            "transport_backpressured_total",
            "Messages delayed due to backpressure"
        );
        metrics::describe_counter!(
            "transport_refused_total",
            "Messages refused by transport (circuit open, capacity)"
        );
        metrics::describe_gauge!(
            "transport_healthy",
            "Transport health (1=healthy, 0=unhealthy)"
        );
        metrics::describe_gauge!(
            "transport_queue_size",
            "Current number of messages in transport queue"
        );
        metrics::describe_gauge!(
            "transport_queue_capacity",
            "Maximum transport queue capacity"
        );
        metrics::describe_gauge!(
            "transport_inflight",
            "Messages currently in-flight (sent but not acked)"
        );
        metrics::describe_histogram!(
            "transport_send_duration_seconds",
            metrics::Unit::Seconds,
            "Time to send a batch to transport"
        );
        // Byte/event throughput (Vector-modelled; batch-incremented). Bytes are
        // raw wire bytes (summed payload.len() per WorkBatch), not decoded size.
        metrics::describe_counter!(
            "transport_sent_bytes_total",
            metrics::Unit::Bytes,
            "Raw bytes written to transport (egress)"
        );
        metrics::describe_counter!(
            "transport_received_bytes_total",
            metrics::Unit::Bytes,
            "Raw bytes read from transport (ingress)"
        );
        metrics::describe_counter!(
            "transport_received_events_total",
            "Events received off the transport (ingress count)"
        );

        // Push transport descriptors into manifest registry
        for (name, desc, mt) in [
            (
                "transport_sent_total",
                "Messages successfully sent to transport",
                MetricType::Counter,
            ),
            (
                "transport_send_errors_total",
                "Messages that failed to send",
                MetricType::Counter,
            ),
            (
                "transport_backpressured_total",
                "Messages delayed due to backpressure",
                MetricType::Counter,
            ),
            (
                "transport_refused_total",
                "Messages refused by transport (circuit open, capacity)",
                MetricType::Counter,
            ),
            (
                "transport_healthy",
                "Transport health (1=healthy, 0=unhealthy)",
                MetricType::Gauge,
            ),
            (
                "transport_queue_size",
                "Current number of messages in transport queue",
                MetricType::Gauge,
            ),
            (
                "transport_queue_capacity",
                "Maximum transport queue capacity",
                MetricType::Gauge,
            ),
            (
                "transport_inflight",
                "Messages currently in-flight (sent but not acked)",
                MetricType::Gauge,
            ),
        ] {
            reg.push(MetricDescriptor {
                name: name.into(),
                metric_type: mt,
                description: desc.into(),
                unit: String::new(),
                labels: vec!["transport".into()],
                group: "platform".into(),
                buckets: None,
                use_cases: vec![],
                dashboard_hint: None,
            });
        }
        reg.push(MetricDescriptor {
            name: "transport_send_duration_seconds".into(),
            metric_type: MetricType::Histogram,
            description: "Time to send a batch to transport".into(),
            unit: "seconds".into(),
            labels: vec!["transport".into()],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        for (name, desc, unit) in [
            (
                "transport_sent_bytes_total",
                "Raw bytes written to transport (egress)",
                "bytes",
            ),
            (
                "transport_received_bytes_total",
                "Raw bytes read from transport (ingress)",
                "bytes",
            ),
            (
                "transport_received_events_total",
                "Events received off the transport (ingress count)",
                "",
            ),
        ] {
            reg.push(MetricDescriptor {
                name: name.into(),
                metric_type: MetricType::Counter,
                description: desc.into(),
                unit: unit.into(),
                labels: vec!["transport".into()],
                group: "platform".into(),
                buckets: None,
                use_cases: vec![],
                dashboard_hint: None,
            });
        }

        // --- Pipeline ---
        metrics::describe_gauge!(
            "pipeline_ready",
            "Pipeline readiness (1=ready, 0=not ready)"
        );
        metrics::describe_counter!(
            "pipeline_stall_seconds_total",
            "Cumulative seconds the pipeline was stalled"
        );

        reg.push(MetricDescriptor {
            name: "pipeline_ready".into(),
            metric_type: MetricType::Gauge,
            description: "Pipeline readiness (1=ready, 0=not ready)".into(),
            unit: String::new(),
            labels: vec![],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        reg.push(MetricDescriptor {
            name: "pipeline_stall_seconds_total".into(),
            metric_type: MetricType::Counter,
            description: "Cumulative seconds the pipeline was stalled".into(),
            unit: "seconds".into(),
            labels: vec![],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // --- Records ---
        metrics::describe_counter!(
            "records_received_total",
            "Records received from all sources"
        );
        metrics::describe_counter!(
            "records_delivered_total",
            "Records successfully delivered to sink"
        );
        metrics::describe_counter!(
            "records_filtered_total",
            "Records dropped by filter/routing rules"
        );
        metrics::describe_counter!("records_dlq_total", "Records sent to dead letter queue");

        for (name, desc) in [
            (
                "records_received_total",
                "Records received from all sources",
            ),
            (
                "records_delivered_total",
                "Records successfully delivered to sink",
            ),
            (
                "records_filtered_total",
                "Records dropped by filter/routing rules",
            ),
            ("records_dlq_total", "Records sent to dead letter queue"),
        ] {
            reg.push(MetricDescriptor {
                name: name.into(),
                metric_type: MetricType::Counter,
                description: desc.into(),
                unit: String::new(),
                labels: vec![],
                group: "platform".into(),
                buckets: None,
                use_cases: vec![],
                dashboard_hint: None,
            });
        }

        // --- Scaling ---
        metrics::describe_gauge!("scaling_pressure", "Normalised scaling pressure (0-100)");
        metrics::describe_gauge!(
            "scaling_circuit_open",
            "Circuit breaker state (1=open, 0=closed)"
        );
        metrics::describe_gauge!("scaling_memory_pressure", "Memory pressure ratio (0.0-1.0)");

        for (name, desc) in [
            ("scaling_pressure", "Normalised scaling pressure (0-100)"),
            (
                "scaling_circuit_open",
                "Circuit breaker state (1=open, 0=closed)",
            ),
            ("scaling_memory_pressure", "Memory pressure ratio (0.0-1.0)"),
        ] {
            reg.push(MetricDescriptor {
                name: name.into(),
                metric_type: MetricType::Gauge,
                description: desc.into(),
                unit: String::new(),
                labels: vec![],
                group: "platform".into(),
                buckets: None,
                use_cases: vec![],
                dashboard_hint: None,
            });
        }

        // --- Spool ---
        metrics::describe_gauge!("spool_bytes", "Current spool size in bytes");
        metrics::describe_gauge!("spool_messages", "Current spool message count");
        metrics::describe_gauge!(
            "spool_disk_available",
            "Available disk space for spool in bytes"
        );

        for (name, desc) in [
            ("spool_bytes", "Current spool size in bytes"),
            ("spool_messages", "Current spool message count"),
            (
                "spool_disk_available",
                "Available disk space for spool in bytes",
            ),
        ] {
            reg.push(MetricDescriptor {
                name: name.into(),
                metric_type: MetricType::Gauge,
                description: desc.into(),
                unit: String::new(),
                labels: vec![],
                group: "platform".into(),
                buckets: None,
                use_cases: vec![],
                dashboard_hint: None,
            });
        }

        // --- Security ---
        metrics::describe_counter!("auth_failures_total", "Authentication failures by reason");
        metrics::describe_counter!("validation_failures_total", "Validation failures by reason");

        reg.push(MetricDescriptor {
            name: "auth_failures_total".into(),
            metric_type: MetricType::Counter,
            description: "Authentication failures by reason".into(),
            unit: String::new(),
            labels: vec!["reason".into()],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        reg.push(MetricDescriptor {
            name: "validation_failures_total".into(),
            metric_type: MetricType::Counter,
            description: "Validation failures by reason".into(),
            unit: String::new(),
            labels: vec!["reason".into()],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        Self { _private: () }
    }

    // ── Transport ────────────────────────────────────────────────────

    /// Record messages successfully sent to a transport.
    #[inline]
    pub fn transport_sent(&self, transport: super::TransportKind, count: u64) {
        metrics::counter!("transport_sent_total", "transport" => transport.as_label())
            .increment(count);
    }

    /// Record send errors for a transport.
    #[inline]
    pub fn transport_send_errors(&self, transport: super::TransportKind, count: u64) {
        metrics::counter!("transport_send_errors_total", "transport" => transport.as_label())
            .increment(count);
    }

    /// Record backpressure events for a transport.
    #[inline]
    pub fn transport_backpressured(&self, transport: &str, count: u64) {
        metrics::counter!("transport_backpressured_total", "transport" => transport.to_string())
            .increment(count);
    }

    /// Record refused messages for a transport.
    #[inline]
    pub fn transport_refused(&self, transport: &str, count: u64) {
        metrics::counter!("transport_refused_total", "transport" => transport.to_string())
            .increment(count);
    }

    /// Set transport health status.
    #[inline]
    pub fn transport_healthy(&self, transport: &str, healthy: bool) {
        metrics::gauge!("transport_healthy", "transport" => transport.to_string())
            .set(if healthy { 1.0 } else { 0.0 });
    }

    /// Set current transport queue size.
    #[inline]
    pub fn transport_queue_size(&self, transport: &str, size: f64) {
        metrics::gauge!("transport_queue_size", "transport" => transport.to_string()).set(size);
    }

    /// Set transport queue capacity.
    #[inline]
    pub fn transport_queue_capacity(&self, transport: &str, capacity: f64) {
        metrics::gauge!("transport_queue_capacity", "transport" => transport.to_string())
            .set(capacity);
    }

    /// Set in-flight message count for a transport.
    #[inline]
    pub fn transport_inflight(&self, transport: &str, count: f64) {
        metrics::gauge!("transport_inflight", "transport" => transport.to_string()).set(count);
    }

    /// Record batch send duration for a transport.
    #[inline]
    pub fn transport_send_duration(&self, transport: &str, seconds: f64) {
        metrics::histogram!(
            "transport_send_duration_seconds",
            "transport" => transport.to_string()
        )
        .record(seconds);
    }

    /// Record raw bytes written to a transport (egress). Sum `payload.len()`
    /// across a `WorkBatch` and call once per send, not per record.
    #[inline]
    pub fn transport_sent_bytes(&self, transport: super::TransportKind, bytes: u64) {
        metrics::counter!("transport_sent_bytes_total", "transport" => transport.as_label())
            .increment(bytes);
    }

    /// Record raw bytes read off a transport (ingress). Call once per received
    /// batch with the summed `payload.len()`.
    #[inline]
    pub fn transport_received_bytes(&self, transport: super::TransportKind, bytes: u64) {
        metrics::counter!("transport_received_bytes_total", "transport" => transport.as_label())
            .increment(bytes);
    }

    /// Record events received off a transport (ingress count). Call once per
    /// received batch with the record count.
    #[inline]
    pub fn transport_received_events(&self, transport: super::TransportKind, count: u64) {
        metrics::counter!("transport_received_events_total", "transport" => transport.as_label())
            .increment(count);
    }

    // ── Pipeline ─────────────────────────────────────────────────────

    /// Set pipeline readiness state.
    #[inline]
    pub fn pipeline_ready(&self, ready: bool) {
        metrics::gauge!("pipeline_ready").set(if ready { 1.0 } else { 0.0 });
    }

    /// Add stall duration to the cumulative stall counter (whole seconds).
    #[inline]
    pub fn pipeline_stall(&self, seconds: u64) {
        metrics::counter!("pipeline_stall_seconds_total").increment(seconds);
    }

    // ── Records ──────────────────────────────────────────────────────

    /// Record incoming records.
    #[inline]
    pub fn records_received(&self, count: u64) {
        metrics::counter!("records_received_total").increment(count);
    }

    /// Record successfully delivered records.
    #[inline]
    pub fn records_delivered(&self, count: u64) {
        metrics::counter!("records_delivered_total").increment(count);
    }

    /// Record filtered/dropped records.
    #[inline]
    pub fn records_filtered(&self, count: u64) {
        metrics::counter!("records_filtered_total").increment(count);
    }

    /// Record records sent to dead letter queue.
    #[inline]
    pub fn records_dlq(&self, count: u64) {
        metrics::counter!("records_dlq_total").increment(count);
    }

    // ── Scaling ──────────────────────────────────────────────────────

    /// Set normalised scaling pressure (0-100).
    #[inline]
    pub fn scaling_pressure(&self, pressure: f64) {
        metrics::gauge!("scaling_pressure").set(pressure);
    }

    /// Set circuit breaker state.
    #[inline]
    pub fn scaling_circuit_open(&self, open: bool) {
        metrics::gauge!("scaling_circuit_open").set(if open { 1.0 } else { 0.0 });
    }

    /// Set memory pressure ratio (0.0-1.0).
    #[inline]
    pub fn scaling_memory_pressure(&self, ratio: f64) {
        metrics::gauge!("scaling_memory_pressure").set(ratio);
    }

    // ── Spool ────────────────────────────────────────────────────────

    /// Set current spool size in bytes.
    #[inline]
    pub fn spool_bytes(&self, bytes: f64) {
        metrics::gauge!("spool_bytes").set(bytes);
    }

    /// Set current spool message count.
    #[inline]
    pub fn spool_messages(&self, count: f64) {
        metrics::gauge!("spool_messages").set(count);
    }

    /// Set available disk space for spool.
    #[inline]
    pub fn spool_disk_available(&self, bytes: f64) {
        metrics::gauge!("spool_disk_available").set(bytes);
    }

    // ── Security ─────────────────────────────────────────────────────

    /// Record authentication failure.
    #[inline]
    pub fn auth_failure(&self, reason: super::AuthFailureReason) {
        metrics::counter!("auth_failures_total", "reason" => reason.as_label()).increment(1);
    }

    /// Record validation failure.
    #[inline]
    pub fn validation_failure(&self, reason: super::ValidationFailureReason) {
        metrics::counter!("validation_failures_total", "reason" => reason.as_label()).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_does_not_panic() {
        let mgr = super::super::MetricsManager::new_for_test("test_app");
        let _dfe = ServiceMetrics::register(&mgr);
    }

    #[tokio::test]
    async fn test_register_populates_registry() {
        // Bare-name contract: ServiceMetrics pushes BARE names; the registry
        // (namespace "test_app") applies the `test_app_` prefix to the manifest.
        let mgr = super::super::MetricsManager::new_for_test("test_app");
        let _dfe = ServiceMetrics::register(&mgr);
        let manifest = mgr.registry().manifest();
        let names: Vec<&str> = manifest.metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"test_app_transport_sent_total"));
        assert!(names.contains(&"test_app_transport_sent_bytes_total"));
        assert!(names.contains(&"test_app_transport_received_bytes_total"));
        assert!(names.contains(&"test_app_transport_received_events_total"));
        assert!(names.contains(&"test_app_pipeline_ready"));
        assert!(names.contains(&"test_app_records_received_total"));
        assert!(names.contains(&"test_app_scaling_pressure"));
        assert!(names.contains(&"test_app_spool_bytes"));
        assert!(names.contains(&"test_app_auth_failures_total"));
        // All should be group=platform
        for m in &manifest.metrics {
            assert_eq!(m.group, "platform");
        }
        // Transport metrics should have "transport" label
        let sent = manifest
            .metrics
            .iter()
            .find(|m| m.name == "test_app_transport_sent_total")
            .unwrap();
        assert_eq!(sent.labels, vec!["transport"]);
        // Security metrics should have "reason" label
        let auth = manifest
            .metrics
            .iter()
            .find(|m| m.name == "test_app_auth_failures_total")
            .unwrap();
        assert_eq!(auth.labels, vec!["reason"]);
    }

    #[tokio::test]
    async fn test_register_bare_namespace_keeps_names_bare() {
        // Empty namespace -> manifest names are bare (no prefix).
        let mgr = super::super::MetricsManager::new_for_test("");
        let _dfe = ServiceMetrics::register(&mgr);
        let manifest = mgr.registry().manifest();
        let names: Vec<&str> = manifest.metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"transport_sent_total"));
        assert!(names.contains(&"pipeline_ready"));
    }

    #[tokio::test]
    async fn test_methods_callable_without_recorder() {
        let mgr = super::super::MetricsManager::new("test_app");
        let svc = ServiceMetrics::register(&mgr);

        svc.transport_sent(super::super::TransportKind::Kafka, 1);
        svc.transport_send_errors(super::super::TransportKind::Kafka, 1);
        svc.transport_backpressured("kafka", 1);
        svc.transport_refused("kafka", 1);
        svc.transport_healthy("kafka", true);
        svc.transport_queue_size("kafka", 100.0);
        svc.transport_queue_capacity("kafka", 1000.0);
        svc.transport_inflight("kafka", 50.0);
        svc.transport_send_duration("kafka", 0.042);
        svc.transport_sent_bytes(super::super::TransportKind::Kafka, 4096);
        svc.transport_received_bytes(super::super::TransportKind::Grpc, 8192);
        svc.transport_received_events(super::super::TransportKind::Grpc, 64);

        svc.pipeline_ready(true);
        svc.pipeline_stall(1);

        svc.records_received(100);
        svc.records_delivered(99);
        svc.records_filtered(1);
        svc.records_dlq(0);

        svc.scaling_pressure(42.0);
        svc.scaling_circuit_open(false);
        svc.scaling_memory_pressure(0.65);

        svc.spool_bytes(1024.0);
        svc.spool_messages(10.0);
        svc.spool_disk_available(1_000_000.0);

        svc.auth_failure(super::super::AuthFailureReason::MalformedToken);
        svc.validation_failure(super::super::ValidationFailureReason::FieldMissing);
    }
}
