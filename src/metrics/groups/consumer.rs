// Project:   scalo
// File:      src/metrics/groups/consumer.rs
// Purpose:   DFE consumer metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka consumer metrics for pipeline apps.

use std::sync::Arc;

use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram};

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

const LAG: &str = "consumer_lag";
const PARTITIONS_ASSIGNED: &str = "consumer_partitions_assigned";
const REBALANCES: &str = "consumer_rebalance_total";

/// Kafka consumer metrics.
///
/// Tracks consumer lag, partition assignments, rebalances, poll latency,
/// and offset commits.
///
/// A scalo `KafkaTransport` consumer fills `consumer_lag`,
/// `consumer_partitions_assigned` and `consumer_rebalance_total` itself, from
/// its statistics and rebalances, each labelled `group_id` with the group its
/// consumer joins. A service on that transport records only poll latency and
/// offset commits. The other setters, and the `partitions_assigned` and
/// `rebalance` handles, are for a consumer scalo does not own: they write
/// those series with no `group_id`, from their first write.
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
        describe(
            manager,
            LAG,
            MetricType::Gauge,
            "Kafka consumer lag per group/topic/partition",
            &["group_id", "topic", "partition"],
        );
        describe(
            manager,
            PARTITIONS_ASSIGNED,
            MetricType::Gauge,
            "Current assigned partition count per group",
            &["group_id"],
        );
        describe(
            manager,
            REBALANCES,
            MetricType::Counter,
            "Consumer group rebalance events per group",
            &["group_id"],
        );

        Self {
            partitions_assigned: Gauge::from_arc(Arc::new(Unlabelled(PARTITIONS_ASSIGNED))),
            rebalance: Counter::from_arc(Arc::new(Unlabelled(REBALANCES))),
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
            LAG,
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

/// Describe `name`, labelled by `labels`, to the recorder and the manifest,
/// registering no series.
fn describe(
    manager: &MetricsManager,
    name: &'static str,
    metric_type: MetricType,
    description: &'static str,
    labels: &[&str],
) {
    match metric_type {
        MetricType::Counter => metrics::describe_counter!(name, description),
        MetricType::Gauge => metrics::describe_gauge!(name, description),
        MetricType::Histogram => metrics::describe_histogram!(name, description),
    }
    manager.registry().push(MetricDescriptor {
        name: name.into(),
        metric_type,
        description: description.into(),
        unit: String::new(),
        labels: labels.iter().map(|label| (*label).to_string()).collect(),
        group: "consumer".into(),
        buckets: None,
        use_cases: vec![],
        dashboard_hint: None,
    });
}

/// A handle on the series `name` with no labels, registered at its first
/// write: registered up front it would sit at 0 beside every group's series.
struct Unlabelled(&'static str);

impl GaugeFn for Unlabelled {
    fn increment(&self, value: f64) {
        metrics::gauge!(self.0).increment(value);
    }

    fn decrement(&self, value: f64) {
        metrics::gauge!(self.0).decrement(value);
    }

    fn set(&self, value: f64) {
        metrics::gauge!(self.0).set(value);
    }
}

impl CounterFn for Unlabelled {
    fn increment(&self, value: u64) {
        metrics::counter!(self.0).increment(value);
    }

    fn absolute(&self, value: u64) {
        metrics::counter!(self.0).absolute(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rendered lines of every series named `name`.
    fn lines<'a>(rendered: &'a str, name: &str) -> Vec<&'a str> {
        rendered
            .lines()
            .filter(|line| line.split([' ', '{']).next() == Some(name))
            .collect()
    }

    /// The transport writes these three by `group_id`, so building the group
    /// describes them with it and adds no unlabelled series stuck at 0 beside
    /// each group's.
    #[test]
    fn the_transport_filled_series_are_described_by_group_with_no_bare_series() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let manager = MetricsManager::new_for_test("");

        let _consumer = ConsumerMetrics::new(&manager);

        let rendered = handle.render();
        for name in [
            "consumer_lag",
            "consumer_partitions_assigned",
            "consumer_rebalance_total",
        ] {
            assert_eq!(lines(&rendered, name), Vec::<&str>::new(), "{rendered}");
        }
        let manifest = manager.registry().manifest();
        let labels = |name: &str| {
            manifest
                .metrics
                .iter()
                .find(|m| m.name == name)
                .map(|m| m.labels.clone())
        };
        assert_eq!(
            labels("consumer_lag"),
            Some(vec![
                "group_id".to_string(),
                "topic".to_string(),
                "partition".to_string()
            ])
        );
        assert_eq!(
            labels("consumer_partitions_assigned"),
            Some(vec!["group_id".to_string()])
        );
        assert_eq!(
            labels("consumer_rebalance_total"),
            Some(vec!["group_id".to_string()])
        );
    }

    /// A service with a consumer scalo does not own still writes the three
    /// series, through the setters or the handles, without a group.
    #[test]
    fn the_setters_and_handles_write_the_series_without_a_group() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let manager = MetricsManager::new_for_test("");
        let consumer = ConsumerMetrics::new(&manager);

        consumer.set_partitions_assigned(3);
        consumer.record_rebalance();
        consumer.rebalance.increment(1);
        consumer.set_lag("events", 0, 9);

        let rendered = handle.render();
        assert_eq!(
            lines(&rendered, "consumer_partitions_assigned"),
            vec!["consumer_partitions_assigned 3"]
        );
        assert_eq!(
            lines(&rendered, "consumer_rebalance_total"),
            vec!["consumer_rebalance_total 2"]
        );
        assert_eq!(
            lines(&rendered, "consumer_lag"),
            vec!["consumer_lag{topic=\"events\",partition=\"0\"} 9"]
        );
    }
}
