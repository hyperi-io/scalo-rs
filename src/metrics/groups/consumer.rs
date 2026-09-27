// Project:   scalo
// File:      src/metrics/groups/consumer.rs
// Purpose:   DFE consumer metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka consumer metrics for DFE apps.

use metrics::{Counter, Gauge, Histogram};

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

/// Kafka consumer metrics.
///
/// Tracks consumer lag, partition assignments, rebalances, poll latency,
/// and offset commits.
///
/// A scalo `KafkaTransport` consumer fills `consumer_lag`,
/// `consumer_partitions_assigned` and `consumer_rebalance_total` itself, from
/// its statistics and rebalances. A service on that transport records only
/// poll latency and offset commits. The other setters are for a consumer scalo
/// does not own.
#[derive(Clone)]
pub struct ConsumerMetrics {
    pub partitions_assigned: Gauge,
    pub rebalance: Counter,
    pub poll_duration: Histogram,
    pub offsets_committed: Counter,
}

impl ConsumerMetrics {
    #[must_use]
    pub fn new(manager: &MetricsManager) -> Self {
        // BARE names -- the recorder prefix layer and registry apply the namespace.

        // consumer_lag -- label-based, register descriptor manually
        metrics::describe_gauge!("consumer_lag", "Kafka consumer lag per topic/partition");
        manager.registry().push(MetricDescriptor {
            name: "consumer_lag".into(),
            metric_type: MetricType::Gauge,
            description: "Kafka consumer lag per topic/partition".into(),
            unit: String::new(),
            labels: vec!["topic".into(), "partition".into()],
            group: "consumer".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        Self {
            partitions_assigned: manager.gauge_with_labels(
                "consumer_partitions_assigned",
                "Current assigned partition count",
                &[],
                "consumer",
            ),
            rebalance: manager.counter_with_labels(
                "consumer_rebalance_total",
                "Consumer group rebalance events",
                &[],
                "consumer",
            ),
            poll_duration: manager.histogram_with_labels(
                "consumer_poll_duration_seconds",
                "Time per Kafka poll/recv call",
                &[],
                "consumer",
                None,
            ),
            offsets_committed: manager.counter_with_labels(
                "offsets_committed_total",
                "Kafka offsets committed after successful processing",
                &[],
                "consumer",
            ),
        }
    }

    /// Set consumer lag for a specific topic/partition.
    #[inline]
    pub fn set_lag(&self, topic: &str, partition: i32, lag: i64) {
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(
            "consumer_lag",
            "topic" => topic.to_string(),
            "partition" => partition.to_string()
        )
        .set(lag as f64);
    }

    #[inline]
    pub fn set_partitions_assigned(&self, count: usize) {
        self.partitions_assigned.set(count as f64);
    }

    #[inline]
    pub fn record_rebalance(&self) {
        self.rebalance.increment(1);
    }

    #[inline]
    pub fn record_poll_duration(&self, seconds: f64) {
        self.poll_duration.record(seconds);
    }

    #[inline]
    pub fn record_offsets_committed(&self, count: u64) {
        self.offsets_committed.increment(count);
    }
}
