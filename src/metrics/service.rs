// Project:   scalo
// File:      src/metrics/service.rs
// Purpose:   Standard metric definitions with transport labels
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Standard metrics for pipeline components (receiver, loader, engine).
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
//! svc.records_received(500);
//! // A sink scalo does not provide, such as a database client over HTTP.
//! svc.transport_sent(TransportKind::Http, 100);
//! svc.scaling_pressure(42.0);
//! ```

use metrics::Unit;

use super::catalogue;
use super::manifest::Series;

/// Standard metric set: labelled counters, gauges, and histograms across
/// transport, pipeline, records, scaling, spool, and security.
///
/// Construct via [`ServiceMetrics::register`] -- describes all metrics with the
/// global recorder AND pushes descriptors into the manifest registry.
///
/// ## Count each thing once
///
/// scalo's Kafka, gRPC, HTTP, file and pipe transports record their own
/// `transport_*` series under their own `transport` label. The `transport_*`
/// methods here are for a sink or source scalo does not provide, such as a
/// database client. Calling them after a scalo transport's send or receive
/// counts that traffic twice. `records_received_total` is counted here, not
/// also through `AppMetrics`.
pub struct ServiceMetrics {
    /// Prevent external construction.
    _private: (),
}

impl ServiceMetrics {
    /// Register all metric descriptions with the global recorder and
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

        // Labels are every key an emitter sets: `path` on gRPC RouteBatch
        // sends, `reason` on pressure sheds.
        let transport = ["transport"];
        reg.describe(
            "platform",
            &[
                // --- Transport ---
                Series::counter(
                    "transport_sent_total",
                    "Messages successfully sent to transport",
                )
                .labels(&["transport", "path"]),
                Series::counter("transport_send_errors_total", "Messages that failed to send")
                    .labels(&transport),
                Series::counter(
                    "transport_backpressured_total",
                    "Messages delayed due to backpressure",
                )
                .labels(&["transport", "reason"]),
                Series::counter(
                    "transport_refused_total",
                    "Messages refused by transport (circuit open, capacity)",
                )
                .labels(&transport),
                Series::gauge(
                    "transport_healthy",
                    "Transport health (1=healthy, 0=unhealthy)",
                )
                .labels(&transport),
                Series::gauge(
                    "transport_queue_size",
                    "Current number of messages in transport queue",
                )
                .labels(&transport),
                Series::gauge(
                    "transport_queue_capacity",
                    "Maximum transport queue capacity",
                )
                .labels(&transport),
                Series::gauge(
                    "transport_inflight",
                    "Messages currently in-flight (sent but not acked)",
                )
                .labels(&transport),
                Series::counter(
                    "transport_commit_errors_total",
                    "Source commits that failed after the block was delivered",
                )
                .labels(&transport),
                Series::counter(
                    "transport_message_too_large_total",
                    "Records over a sender's size ceiling, refused before sending or by the receiver",
                )
                .labels(&transport),
                Series::counter(
                    "transport_truncated_total",
                    "Messages cut to a sender's size ceiling and sent",
                )
                .labels(&transport),
                Series::counter(
                    "transport_coalesced_calls_total",
                    "gRPC RouteBatch calls that joined send_batch blocks",
                )
                .labels(&transport),
                Series::counter(
                    "transport_coalesced_blocks_total",
                    "send_batch blocks the joined gRPC calls carried",
                )
                .labels(&transport),
                Series::counter(
                    "transport_coalesced_records_total",
                    "Records the joined gRPC calls carried",
                )
                .labels(&transport),
                Series::counter(
                    "transport_coalesced_timeouts_total",
                    "gRPC send_batch blocks answered at send_timeout_ms before their joined call did",
                )
                .labels(&transport),
                Series::counter(
                    "transport_recv_errors_total",
                    "Receive failures by class (transient: retried, permanent: returned)",
                )
                .labels(&["transport", "class"]),
                Series::counter(
                    "transport_consumer_rebuilds_total",
                    "Consumers replaced after librdkafka flagged them fatal, by the protocol rejoined with",
                )
                .labels(&["transport", "protocol"]),
                Series::counter(
                    "transport_revoke_discarded_total",
                    "Records a revoke kept from a write, as the partition's next owner reads \
                     them again, by stage: receive (left out by recv) or buffer (discarded \
                     by the caller)",
                )
                .labels(&["transport", "stage"]),
                Series::histogram(
                    "transport_send_duration_seconds",
                    "Time to send a batch to transport",
                )
                .labels(&transport),
                // Byte/event throughput, incremented per batch. Bytes are raw
                // wire bytes (summed payload.len() per WorkBatch), not decoded size.
                Series::counter(
                    "transport_sent_bytes_total",
                    "Raw bytes written to transport (egress)",
                )
                .unit(Unit::Bytes)
                .labels(&transport),
                Series::counter(
                    "transport_received_bytes_total",
                    "Raw bytes read from transport (ingress)",
                )
                .unit(Unit::Bytes)
                .labels(&transport),
                Series::counter(
                    "transport_received_events_total",
                    "Events received off the transport (ingress count)",
                )
                .labels(&transport),
                // --- Pipeline ---
                Series::counter(
                    "pipeline_retries_total",
                    "Run-loop steps retried after a transient source or sink failure",
                )
                .labels(&["stage"]),
                Series::gauge(
                    "pipeline_idle",
                    "Service has no work configured (1=idle, 0=working)",
                ),
                Series::gauge(
                    "pipeline_ready",
                    "Pipeline readiness (1=ready, 0=not ready)",
                ),
                Series::counter(
                    "pipeline_stall_seconds_total",
                    "Cumulative seconds the pipeline was stalled",
                )
                .unit(Unit::Seconds),
                // --- Source acknowledgements ---
                Series::gauge(
                    "pipeline_delivery_guarantee",
                    "Delivery guarantee the pipeline gives (1 = active), with the reason, and \
                     the listener when the app names one",
                )
                .labels(&["guarantee", "reason", "listener"]),
                Series::counter(
                    "pipeline_dead_letters_dropped_total",
                    "Dead letters dropped with nowhere to go: no DLQ, a disabled DLQ, a record \
                     a gRPC send_batch left out of a block, or one a downstream peer refused for good",
                )
                .labels(&["reason"]),
                Series::gauge(
                    "transport_ack_held",
                    "Records whose source acknowledgement is held until delivery",
                )
                .labels(&transport),
                Series::gauge(
                    "transport_ack_held_bytes",
                    "Payload bytes whose source acknowledgement is held until delivery",
                )
                .unit(Unit::Bytes)
                .labels(&transport),
                Series::gauge(
                    "transport_ack_withheld",
                    "Kafka offsets released Errored and still held: each pins its partition's \
                     commit until a restart or a revoke",
                )
                .labels(&transport),
                Series::counter(
                    "transport_ack_released_total",
                    "Records whose source acknowledgement was released, by outcome",
                )
                .labels(&["transport", "outcome"]),
                Series::counter(
                    "transport_ack_refused_total",
                    "Requests refused before their acknowledgement was held, by reason",
                )
                .labels(&["transport", "reason"]),
                Series::histogram(
                    "transport_ack_latency_seconds",
                    "Time from receipt to the release of a source acknowledgement",
                )
                .labels(&["transport", "outcome"]),
                Series::counter(
                    "transport_redelivered_total",
                    "Sends retried after the receiver may already have taken them",
                )
                .labels(&["transport", "reason"]),
                // --- Records ---
                catalogue::records_received_total(),
                Series::counter(
                    "records_delivered_total",
                    "Records successfully delivered to sink",
                ),
                Series::counter(
                    "records_filtered_total",
                    "Records dropped by filter/routing rules",
                ),
                Series::counter("records_dlq_total", "Records sent to dead letter queue"),
                // --- Scaling ---
                Series::gauge("scaling_pressure", "Normalised scaling pressure (0-100)"),
                Series::gauge(
                    "scaling_circuit_open",
                    "Circuit breaker state (1=open, 0=closed)",
                ),
                Series::gauge("scaling_memory_pressure", "Memory pressure ratio (0.0-1.0)"),
                // --- Spool ---
                Series::gauge("spool_bytes", "Current spool size in bytes"),
                Series::gauge("spool_messages", "Current spool message count"),
                Series::gauge(
                    "spool_disk_available",
                    "Available disk space for spool in bytes",
                ),
            ],
        );
        // A feature-gated series is described only in a build that can write it.
        #[cfg(feature = "tiered-sink")]
        reg.describe(
            "platform",
            &[
                Series::counter(
                    "spool_hot_path_total",
                    "Records the tiered sink's sender took on the hot path",
                ),
                Series::counter(
                    "spool_cold_path_total",
                    "Tiered sink sends that ended on the spool path without error",
                ),
                Series::counter(
                    "spool_circuit_trips_total",
                    "Tiered sink hot-path sends that failed (backpressured or timed out), one per send",
                ),
                Series::gauge(
                    "spool_circuit_state",
                    "Tiered sink circuit breaker state at the last send (0=closed, 1=half-open, 2=open)",
                ),
                Series::counter(
                    "tiered_sink_overflow_total",
                    "Records the tiered sink shed because the spool was full, by when_full policy",
                )
                .labels(&["policy"]),
                Series::counter(
                    "tiered_sink_dropped_total",
                    "Records the tiered sink dropped on spool overflow, by when_full policy",
                )
                .labels(&["policy"]),
                Series::counter(
                    "tiered_sink_dead_letters_total",
                    "Records the tiered sink's drain or full spool could not deliver, by reason and whether the DLQ took them",
                )
                .labels(&["reason", "outcome"]),
                Series::gauge(
                    "tiered_sink_drain_held",
                    "1 while a tiered sink's drain holds at a spilled entry its codec cannot read, else 0",
                ),
                Series::gauge(
                    "spool_disk_available_bytes",
                    "Bytes free to unprivileged users on the tiered sink spool's filesystem",
                )
                .unit(Unit::Bytes),
                Series::gauge(
                    "spool_disk_total_bytes",
                    "Size of the tiered sink spool's filesystem in bytes",
                )
                .unit(Unit::Bytes),
                Series::counter(
                    "spool_dropped_without_shutdown_total",
                    "Tiered sinks dropped with records spooled and no completed shutdown",
                ),
                Series::counter(
                    "tiered_sink_drain_read_errors_total",
                    "Tiered sink drain passes whose spill read failed",
                ),
                Series::gauge(
                    "tiered_sink_drain_stalled",
                    "1 while the tiered sink's spool counts records its drain does not find, else 0",
                ),
            ],
        );
        #[cfg(any(feature = "spool", feature = "tiered-sink"))]
        {
            let store = ["store"];
            let store_codec = ["store", "codec"];
            reg.describe(
                "platform",
                &[
                    Series::counter(
                        "spool_quarantined_total",
                        "Spool caches moved aside into a corrupt-* subdirectory, by trigger",
                    )
                    .labels(&["trigger"]),
                    Series::counter(
                        "spool_bytes_in_total",
                        "Bytes a spool took in, before compression",
                    )
                    .unit(Unit::Bytes)
                    .labels(&store_codec),
                    Series::counter(
                        "spool_bytes_written_total",
                        "Bytes a spool appended to its segment files",
                    )
                    .unit(Unit::Bytes)
                    .labels(&store_codec),
                    Series::counter(
                        "spool_codec_microseconds_total",
                        "Time a spool spent in its codec",
                    )
                    .unit(Unit::Microseconds)
                    .labels(&["store", "codec", "op"]),
                    Series::gauge(
                        "spool_disk_bytes",
                        "Bytes a spool's segment files hold on disk",
                    )
                    .unit(Unit::Bytes)
                    .labels(&store),
                    Series::counter(
                        "spool_disk_full_refused_bytes_total",
                        "Bytes a spool refused because its disk was full",
                    )
                    .unit(Unit::Bytes)
                    .labels(&store),
                    Series::counter(
                        "spool_disk_full_wait_milliseconds_total",
                        "Time a spool waited for its full disk to have room again",
                    )
                    .unit(Unit::Milliseconds)
                    .labels(&store),
                    Series::gauge(
                        "spool_memory_reserved_bytes",
                        "Bytes a spool's read holds reserved with the memory guard",
                    )
                    .unit(Unit::Bytes)
                    .labels(&store),
                    Series::counter(
                        "spool_drain_cut_short_total",
                        "Spool reads the memory guard stopped after their first record",
                    )
                    .labels(&store),
                    Series::counter(
                        "spool_damaged_entries_total",
                        "Spooled records dropped because they failed their CRC",
                    )
                    .labels(&store),
                    Series::histogram(
                        "spool_sync_seconds",
                        "Time one sync of a spool to disk took",
                    )
                    .labels(&store),
                    Series::counter(
                        "spool_position_save_failures_total",
                        "Saves of a spool's read position that failed",
                    )
                    .labels(&store),
                    Series::counter(
                        "spool_segment_delete_failures_total",
                        "Consumed spool segment files that could not be deleted",
                    )
                    .labels(&store),
                    Series::counter(
                        "spool_torn_entries_cut_total",
                        "Torn entries cut off the end of a spool when it opened",
                    )
                    .labels(&store),
                ],
            );
        }
        // Only the tiered sink reports the age of what it holds.
        #[cfg(feature = "tiered-sink")]
        reg.describe(
            "platform",
            &[Series::gauge(
                "spool_oldest_age_seconds",
                "Age of the oldest record a spool holds, 0 when it holds none",
            )
            .unit(Unit::Seconds)
            .labels(&["store"])],
        );
        #[cfg(feature = "spool")]
        reg.describe(
            "platform",
            &[Series::gauge(
                "spool_queue_depth",
                "Records in a standalone spool queue",
            )],
        );
        #[cfg(feature = "rules")]
        reg.describe(
            "platform",
            &[
                Series::counter(
                    "rules_hits_total",
                    "Records a rule set row set its target on, by rule set and row id",
                )
                .labels(&["rule_set", "rule"]),
                Series::counter(
                    "rules_clamped_total",
                    "Records whose clamped target the default replaced, by the row that set the refused value, or (record)",
                )
                .labels(&["rule_set", "rule"]),
            ],
        );

        reg.describe(
            "platform",
            &[
                // --- DLQ ---
                Series::counter(
                    "dlq_dropped_total",
                    "Dead letters no DLQ backend holds, by reason",
                )
                .labels(&["reason"]),
                Series::counter(
                    "dlq_cascade_fallthrough_total",
                    "Dead letters a later cascade backend took after an earlier one gave them up",
                )
                .labels(&["from", "to", "reason"]),
                Series::counter(
                    "dlq_payload_truncated_total",
                    "Dead letters whose payload was cut to max_payload_bytes",
                ),
                // --- Security ---
                Series::counter("auth_failures_total", "Authentication failures by reason")
                    .labels(&["reason"]),
                Series::counter("validation_failures_total", "Validation failures by reason")
                    .labels(&["reason"]),
            ],
        );

        Self { _private: () }
    }

    // -- Transport ----------------------------------------------------

    /// Record messages successfully sent to a transport that is not a scalo
    /// transport (see [`ServiceMetrics`]).
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
    /// across a `WorkBatch` and call once per send, not per record, and only
    /// for a sink that is not a scalo transport (see [`ServiceMetrics`]).
    #[inline]
    pub fn transport_sent_bytes(&self, transport: super::TransportKind, bytes: u64) {
        metrics::counter!("transport_sent_bytes_total", "transport" => transport.as_label())
            .increment(bytes);
    }

    /// Record raw bytes read off a transport (ingress). Call once per received
    /// batch with the summed `payload.len()`, and only for a source that is not
    /// a scalo transport (see [`ServiceMetrics`]).
    #[inline]
    pub fn transport_received_bytes(&self, transport: super::TransportKind, bytes: u64) {
        metrics::counter!("transport_received_bytes_total", "transport" => transport.as_label())
            .increment(bytes);
    }

    /// Record events received off a transport (ingress count). Call once per
    /// received batch with the record count, and only for a source that is not
    /// a scalo transport (see [`ServiceMetrics`]).
    #[inline]
    pub fn transport_received_events(&self, transport: super::TransportKind, count: u64) {
        metrics::counter!("transport_received_events_total", "transport" => transport.as_label())
            .increment(count);
    }

    // -- Pipeline -----------------------------------------------------

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

    // -- Records ------------------------------------------------------

    /// Record incoming records, once per record.
    ///
    /// Owns `records_received_total`. `AppMetrics` (feature `service-metrics`)
    /// holds a handle on the same series, so counting a record here and there
    /// as well adds it twice.
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

    // -- Scaling ------------------------------------------------------

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

    // -- Spool --------------------------------------------------------

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
    ///
    /// The tiered sink's disk poller writes `spool_disk_available_bytes` and
    /// `spool_disk_total_bytes`, not this series.
    #[inline]
    pub fn spool_disk_available(&self, bytes: f64) {
        metrics::gauge!("spool_disk_available").set(bytes);
    }

    // -- Security -----------------------------------------------------

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
        // Bare-name contract: ServiceMetrics pushes BARE names. The registry
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
        // Security metrics should have "reason" label
        let auth = manifest
            .metrics
            .iter()
            .find(|m| m.name == "test_app_auth_failures_total")
            .unwrap();
        assert_eq!(auth.labels, vec!["reason"]);
        // Each series lists every label key its emitters set: `path` on gRPC
        // RouteBatch sends, `reason` on pressure sheds.
        for (name, labels) in [
            ("test_app_transport_sent_total", vec!["transport", "path"]),
            ("test_app_transport_sent_bytes_total", vec!["transport"]),
            (
                "test_app_transport_backpressured_total",
                vec!["transport", "reason"],
            ),
            (
                "test_app_transport_received_events_total",
                vec!["transport"],
            ),
            (
                "test_app_transport_recv_errors_total",
                vec!["transport", "class"],
            ),
            ("test_app_transport_commit_errors_total", vec!["transport"]),
            (
                "test_app_transport_message_too_large_total",
                vec!["transport"],
            ),
            (
                "test_app_transport_coalesced_records_total",
                vec!["transport"],
            ),
            ("test_app_pipeline_retries_total", vec!["stage"]),
            (
                "test_app_pipeline_delivery_guarantee",
                vec!["guarantee", "reason", "listener"],
            ),
            (
                "test_app_pipeline_dead_letters_dropped_total",
                vec!["reason"],
            ),
            ("test_app_transport_ack_held", vec!["transport"]),
            ("test_app_transport_ack_held_bytes", vec!["transport"]),
            ("test_app_transport_ack_withheld", vec!["transport"]),
            (
                "test_app_transport_ack_released_total",
                vec!["transport", "outcome"],
            ),
            (
                "test_app_transport_ack_refused_total",
                vec!["transport", "reason"],
            ),
            (
                "test_app_transport_ack_latency_seconds",
                vec!["transport", "outcome"],
            ),
            (
                "test_app_transport_redelivered_total",
                vec!["transport", "reason"],
            ),
        ] {
            let found = manifest
                .metrics
                .iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("{name} missing from the manifest"));
            assert_eq!(found.labels, labels, "{name}");
        }
    }

    /// Every series the tiered sink writes, with the type and label keys its emitters use.
    #[cfg(feature = "tiered-sink")]
    #[tokio::test]
    async fn test_register_lists_tiered_sink_series() {
        use crate::metrics::MetricType;

        let mgr = super::super::MetricsManager::new_for_test("test_app");
        let _dfe = ServiceMetrics::register(&mgr);
        let manifest = mgr.registry().manifest();
        for (name, metric_type, labels) in [
            ("test_app_spool_hot_path_total", MetricType::Counter, vec![]),
            (
                "test_app_spool_cold_path_total",
                MetricType::Counter,
                vec![],
            ),
            (
                "test_app_spool_circuit_trips_total",
                MetricType::Counter,
                vec![],
            ),
            ("test_app_spool_circuit_state", MetricType::Gauge, vec![]),
            (
                "test_app_tiered_sink_overflow_total",
                MetricType::Counter,
                vec!["policy"],
            ),
            (
                "test_app_tiered_sink_dropped_total",
                MetricType::Counter,
                vec!["policy"],
            ),
            (
                "test_app_tiered_sink_dead_letters_total",
                MetricType::Counter,
                vec!["reason", "outcome"],
            ),
            ("test_app_tiered_sink_drain_held", MetricType::Gauge, vec![]),
            ("test_app_spool_messages", MetricType::Gauge, vec![]),
            ("test_app_spool_bytes", MetricType::Gauge, vec![]),
            (
                "test_app_spool_disk_available_bytes",
                MetricType::Gauge,
                vec![],
            ),
            ("test_app_spool_disk_total_bytes", MetricType::Gauge, vec![]),
            (
                "test_app_spool_dropped_without_shutdown_total",
                MetricType::Counter,
                vec![],
            ),
            (
                "test_app_spool_quarantined_total",
                MetricType::Counter,
                vec!["trigger"],
            ),
            (
                "test_app_spool_bytes_written_total",
                MetricType::Counter,
                vec!["store", "codec"],
            ),
            (
                "test_app_spool_disk_bytes",
                MetricType::Gauge,
                vec!["store"],
            ),
            (
                "test_app_spool_sync_seconds",
                MetricType::Histogram,
                vec!["store"],
            ),
            (
                "test_app_spool_damaged_entries_total",
                MetricType::Counter,
                vec!["store"],
            ),
            (
                "test_app_spool_oldest_age_seconds",
                MetricType::Gauge,
                vec!["store"],
            ),
            (
                "test_app_tiered_sink_drain_stalled",
                MetricType::Gauge,
                vec![],
            ),
        ] {
            let found = manifest
                .metrics
                .iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("{name} missing from the manifest"));
            assert_eq!(found.metric_type, metric_type, "{name}");
            assert_eq!(found.labels, labels, "{name}");
        }
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
