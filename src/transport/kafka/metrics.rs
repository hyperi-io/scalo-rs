// Project:   scalo
// File:      src/transport/kafka/metrics.rs
// Purpose:   Kafka metrics collection via librdkafka statistics
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka metrics collection via librdkafka statistics callback.
//!
//! Provides a `StatsContext` implementation that collects librdkafka statistics
//! and exposes them through a `KafkaMetrics` snapshot. Matches the Python
//! `scalo.kafka.KafkaMetricsCollector` API.
//!
//! ## Usage
//!
//! Enable statistics by setting `statistics.interval.ms` in the Kafka config:
//!
//! ```rust,ignore
//! use scalo::transport::kafka::{KafkaConfig, KafkaMetrics, StatsContext};
//! use std::sync::Arc;
//!
//! let stats = Arc::new(StatsContext::new());
//! let mut config = KafkaConfig::default();
//! config.extra_config.insert("statistics.interval.ms".to_string(), "5000".to_string());
//!
//! // Use stats.clone() as the context when creating consumer/producer
//! // Then periodically:
//! let metrics = stats.get_metrics();
//! println!("Messages sent: {}", metrics.messages_sent);
//! println!("Consumer lag: {:?}", metrics.partition_lag);
//! ```

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::statistics::Statistics;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

/// Kafka metrics snapshot. Mirrors the Python `KafkaMetrics` dataclass.
#[derive(Debug, Clone, Default)]
pub struct KafkaMetrics {
    // --- Client-level metrics ---
    /// Total messages sent (produced).
    pub messages_sent: i64,
    /// Total messages received (consumed).
    pub messages_received: i64,
    /// Total bytes sent.
    pub bytes_sent: i64,
    /// Total bytes received.
    pub bytes_received: i64,
    /// Current messages in producer queue.
    pub queue_message_count: u64,
    /// Current bytes in producer queue.
    pub queue_byte_count: u64,

    // --- Per-broker metrics ---
    /// Per-broker statistics keyed by broker name.
    pub brokers: HashMap<String, BrokerMetrics>,

    // --- Per-partition metrics (consumer) ---
    /// Per-partition statistics keyed by (topic, partition).
    pub partition_lag: HashMap<(String, i32), i64>,
    /// Per-partition committed offsets.
    pub partition_committed: HashMap<(String, i32), i64>,
    /// Per-partition high watermarks.
    pub partition_high_watermark: HashMap<(String, i32), i64>,

    // --- Consumer group metrics ---
    /// Consumer group state (e.g., "up", "rebalancing").
    pub consumer_group_state: Option<String>,
    /// Total number of rebalances.
    pub rebalance_count: i64,
    /// Time since last rebalance in milliseconds.
    pub rebalance_age_ms: i64,

    // --- Timestamp ---
    /// Unix timestamp when these stats were collected.
    pub timestamp: i64,
}

/// Per-broker metrics.
#[derive(Debug, Clone, Default)]
pub struct BrokerMetrics {
    /// Broker state ("UP", "DOWN", "INIT", etc.).
    pub state: String,
    /// Average round-trip time in milliseconds.
    pub rtt_avg_ms: f64,
    /// 99th percentile RTT in milliseconds.
    pub rtt_p99_ms: f64,
    /// Total throttle time in milliseconds.
    pub throttle_time_ms: i64,
    /// Messages in output buffer.
    pub outbuf_msg_cnt: i64,
    /// Requests waiting for response.
    pub waitresp_cnt: i64,
    /// Total requests sent.
    pub requests_sent: u64,
    /// Total responses received.
    pub responses_received: u64,
    /// Total request errors.
    pub request_errors: u64,
}

/// Statistics-collecting client context.
///
/// Implements `ClientContext` to receive librdkafka statistics callbacks.
/// Thread-safe: shareable across multiple Kafka clients.
#[derive(Debug)]
pub struct StatsContext {
    stats: RwLock<Option<Statistics>>,
    latest_metrics: RwLock<KafkaMetrics>,
    /// Broker-side delivery outcomes, when this context drives a producer.
    delivery: super::classify::DeliveryState,
    /// Set once statistics report a broker `UP`, which librdkafka reaches only
    /// after the TLS and SASL handshakes succeed.
    connected: AtomicBool,
    /// Records past this consumer's read position, summed over its partitions.
    position_lag: AtomicI64,
    /// Ownership changes since the transport last looked, each numbered as
    /// `rebalances` counted it. std's lock keeps the context unwind-safe, as
    /// its other fields are.
    rebalanced: Mutex<Vec<(u64, Rebalanced)>>,
    /// Ownership changes served so far, so a record can be ordered against
    /// them.
    rebalances: AtomicU64,
    /// Whether the inbound gate has this consumer's assignment paused.
    paused: AtomicBool,
    /// Log ends asked of the broker while paused, since librdkafka learns a
    /// partition's end only from a fetch and fetches nothing it has paused.
    paused_ends: RwLock<HashMap<(String, i32), i64>>,
}

/// A change a rebalance made to what this consumer owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rebalanced {
    /// These partitions were taken from this consumer.
    Revoked(Vec<(String, i32)>),
    /// These partitions were given to this consumer.
    Assigned(Vec<(String, i32)>),
}

impl Default for StatsContext {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsContext {
    /// Create a new statistics context.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(None),
            latest_metrics: RwLock::new(KafkaMetrics::default()),
            delivery: super::classify::DeliveryState::default(),
            connected: AtomicBool::new(false),
            position_lag: AtomicI64::new(0),
            rebalanced: Mutex::new(Vec::new()),
            rebalances: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            paused_ends: RwLock::new(HashMap::new()),
        }
    }

    /// Record that the inbound gate paused or resumed the assignment. A
    /// resume drops the ends asked of the broker: fetches report the end
    /// again from then on.
    pub(crate) fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
        if !paused && let Ok(mut ends) = self.paused_ends.write() {
            ends.clear();
        }
    }

    /// Whether the inbound gate has the assignment paused.
    pub(crate) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// The log ends the broker reported for the paused assignment, used by
    /// every statistics callback until the next refresh or a resume.
    pub(crate) fn set_paused_ends(&self, ends: HashMap<(String, i32), i64>) {
        // Checked under the lock `set_paused` clears under, so a refresh that
        // lands after a resume leaves nothing behind.
        if let Ok(mut held) = self.paused_ends.write()
            && self.is_paused()
        {
            *held = ends;
        }
    }

    /// Ownership changes served so far. A record polled now carries this
    /// count, so it orders against [`take_rebalanced`](Self::take_rebalanced):
    /// librdkafka runs a rebalance inside the poll, before the poll returns.
    pub(crate) fn rebalances(&self) -> u64 {
        self.rebalances.load(Ordering::Acquire)
    }

    /// Ownership changes since the last call, in the order they were served,
    /// each with its number.
    pub(crate) fn take_rebalanced(&self) -> Vec<(u64, Rebalanced)> {
        std::mem::take(
            &mut *self
                .rebalanced
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Record one ownership change, numbered under the lock so the log and
    /// the count agree.
    fn note_rebalanced(&self, change: Rebalanced) {
        let mut log = self
            .rebalanced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let number = self.rebalances.fetch_add(1, Ordering::AcqRel) + 1;
        log.push((number, change));
    }

    /// Records past this consumer's read position, summed over its partitions,
    /// as of the last statistics callback.
    ///
    /// [`total_consumer_lag`] counts from the COMMITTED offset, so a commit held
    /// until delivery reads as backlog there. This counts from where the
    /// consumer has read to: unread backlog, whatever the commit policy. A
    /// scaling signal that should not grow while acknowledgements are held
    /// reads this one.
    #[must_use]
    pub fn total_position_lag(&self) -> i64 {
        self.position_lag.load(Ordering::Relaxed)
    }

    /// Whether any broker has ever reached `UP`, meaning this client's
    /// credentials were accepted at least once.
    pub(crate) fn has_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Messages the broker refused or never acknowledged.
    ///
    /// Only moves when this context drives a producer directly; the
    /// `FutureProducer` path reports delivery through its own send future.
    #[must_use]
    pub fn delivery_failures(&self) -> u64 {
        self.delivery.failures()
    }

    /// Get the latest metrics snapshot (clone).
    #[must_use]
    pub fn get_metrics(&self) -> KafkaMetrics {
        self.latest_metrics
            .read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    /// Get the raw librdkafka statistics.
    ///
    /// Returns the full `Statistics` struct from the last callback.
    #[must_use]
    pub fn get_raw_stats(&self) -> Option<Statistics> {
        self.stats.read().ok().and_then(|s| s.clone())
    }

    /// Convert raw statistics to our metrics format, taking each partition's
    /// end as the later of the statistics' and `asked_ends`.
    fn convert_stats(stats: &Statistics, asked_ends: &HashMap<(String, i32), i64>) -> KafkaMetrics {
        let mut metrics = KafkaMetrics {
            messages_sent: stats.txmsgs,
            messages_received: stats.rxmsgs,
            bytes_sent: stats.tx_bytes,
            bytes_received: stats.rx_bytes,
            queue_message_count: stats.msg_cnt,
            queue_byte_count: stats.msg_size,
            timestamp: stats.time,
            ..Default::default()
        };

        // Per-broker metrics
        for (name, broker) in &stats.brokers {
            let rtt_avg_ms = broker.rtt.as_ref().map_or(0.0, |w| w.avg as f64 / 1000.0);
            let rtt_p99_ms = broker.rtt.as_ref().map_or(0.0, |w| w.p99 as f64 / 1000.0);
            let throttle_time_ms = broker.throttle.as_ref().map_or(0, |w| w.sum);

            metrics.brokers.insert(
                name.clone(),
                BrokerMetrics {
                    state: broker.state.clone(),
                    rtt_avg_ms,
                    rtt_p99_ms,
                    throttle_time_ms,
                    outbuf_msg_cnt: broker.outbuf_msg_cnt,
                    waitresp_cnt: broker.waitresp_cnt,
                    requests_sent: broker.tx,
                    responses_received: broker.rx,
                    request_errors: broker.txerrs,
                },
            );
        }

        // Per-partition metrics from topics
        for (topic_name, topic) in &stats.topics {
            for (partition_id, partition) in &topic.partitions {
                let key = (topic_name.clone(), *partition_id);
                let asked_end = asked_ends.get(&key).copied();

                // Consumer lag
                let consumer_lag = match asked_end {
                    Some(end) if partition.committed_offset >= 0 => {
                        partition.consumer_lag.max(end - partition.committed_offset)
                    }
                    _ => partition.consumer_lag,
                };
                if consumer_lag >= 0 {
                    metrics.partition_lag.insert(key.clone(), consumer_lag);
                }

                // Committed offset
                if partition.committed_offset >= 0 {
                    metrics
                        .partition_committed
                        .insert(key.clone(), partition.committed_offset);
                }

                // High watermark
                let hi_offset =
                    asked_end.map_or(partition.hi_offset, |end| partition.hi_offset.max(end));
                if hi_offset >= 0 {
                    metrics.partition_high_watermark.insert(key, hi_offset);
                }
            }
        }

        // Consumer group metrics
        if let Some(ref cgrp) = stats.cgrp {
            metrics.consumer_group_state = Some(cgrp.state.clone());
            metrics.rebalance_count = cgrp.rebalance_cnt;
            metrics.rebalance_age_ms = cgrp.rebalance_age;
        }

        metrics
    }
}

impl ClientContext for StatsContext {
    fn stats(&self, statistics: Statistics) {
        if !self.connected.load(Ordering::Relaxed)
            && statistics.brokers.values().any(|b| b.state == "UP")
        {
            self.connected.store(true, Ordering::Relaxed);
        }
        let asked_ends = self
            .paused_ends
            .read()
            .map(|ends| ends.clone())
            .unwrap_or_default();
        let metrics = Self::convert_stats(&statistics, &asked_ends);
        self.position_lag
            .store(position_lag(&statistics, &asked_ends), Ordering::Relaxed);

        if let Ok(mut lock) = self.latest_metrics.write() {
            *lock = metrics;
        }

        // Keep the raw stats too.
        if let Ok(mut lock) = self.stats.write() {
            *lock = Some(statistics);
        }

        // Auto-emit as Prometheus metrics if a recorder is installed.
        #[cfg(feature = "metrics")]
        self.emit_prometheus_metrics();
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                #[cfg(feature = "logger")]
                tracing::error!(target: "librdkafka", facility = fac, "{}", log_message);
                #[cfg(not(feature = "logger"))]
                eprintln!("ERROR librdkafka: {} {}", fac, log_message);
            }
            RDKafkaLogLevel::Warning => {
                #[cfg(feature = "logger")]
                tracing::warn!(target: "librdkafka", facility = fac, "{}", log_message);
                #[cfg(not(feature = "logger"))]
                eprintln!("WARN librdkafka: {} {}", fac, log_message);
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                // rdkafka INFO/Notice is too verbose for application-level INFO
                // (statistics JSON every statistics.interval.ms, connection lifecycle, etc.)
                #[cfg(feature = "logger")]
                tracing::debug!(target: "librdkafka", facility = fac, "{}", log_message);
                #[cfg(not(feature = "logger"))]
                {}
            }
            RDKafkaLogLevel::Debug => {
                #[cfg(feature = "logger")]
                tracing::debug!(target: "librdkafka", facility = fac, "{}", log_message);
                #[cfg(not(feature = "logger"))]
                {}
            }
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        #[cfg(feature = "logger")]
        tracing::error!(target: "librdkafka", error = %error, "{}", reason);
        #[cfg(not(feature = "logger"))]
        eprintln!("ERROR librdkafka: {}: {}", error, reason);
    }
}

impl StatsContext {
    /// Emit current metrics as Prometheus gauges/counters via the `metrics` crate.
    ///
    /// Call periodically (e.g. after each stats callback) to push rdkafka
    /// internal statistics to the global metrics recorder. No-op if no
    /// recorder is installed.
    ///
    /// Emits under the `rdkafka_` prefix per the metrics standard.
    /// Per-partition metrics are bounded by `max_partitions` (default 256).
    #[cfg(feature = "metrics")]
    pub fn emit_prometheus_metrics(&self) {
        let m = self.get_metrics();

        // Global counters
        metrics::gauge!("rdkafka_global_msg_cnt").set(m.queue_message_count as f64);
        metrics::gauge!("rdkafka_global_msg_size_bytes").set(m.queue_byte_count as f64);

        // Per-broker metrics
        for (name, broker) in &m.brokers {
            metrics::gauge!(
                "rdkafka_broker_rtt_avg_seconds",
                "broker" => name.clone()
            )
            .set(broker.rtt_avg_ms / 1000.0);

            metrics::gauge!(
                "rdkafka_broker_outbuf_cnt",
                "broker" => name.clone()
            )
            .set(broker.outbuf_msg_cnt as f64);

            metrics::gauge!(
                "rdkafka_broker_waitresp_cnt",
                "broker" => name.clone()
            )
            .set(broker.waitresp_cnt as f64);
        }

        // Per-partition consumer lag (capped at 256 partitions for cardinality safety)
        let max_partitions = 256;
        for (i, ((topic, partition), lag)) in m.partition_lag.iter().enumerate() {
            if i >= max_partitions {
                break;
            }
            metrics::gauge!(
                "rdkafka_topic_partition_consumer_lag",
                "topic" => topic.clone(),
                "partition" => partition.to_string()
            )
            .set(*lag as f64);
        }

        for (i, ((topic, partition), offset)) in m.partition_committed.iter().enumerate() {
            if i >= max_partitions {
                break;
            }
            metrics::gauge!(
                "rdkafka_topic_partition_committed_offset",
                "topic" => topic.clone(),
                "partition" => partition.to_string()
            )
            .set(*offset as f64);
        }

        // Rebalance count
        if m.rebalance_count > 0 {
            metrics::gauge!("rdkafka_consumer_rebalance_count").set(m.rebalance_count as f64);
        }
    }
}

/// Records each revoke and assignment, in order, before librdkafka applies
/// it, so the transport never holds or commits for a partition it lost.
impl rdkafka::consumer::ConsumerContext for StatsContext {
    fn pre_rebalance(
        &self,
        _consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        let partitions = |list: &rdkafka::TopicPartitionList| {
            list.elements()
                .iter()
                .map(|p| (p.topic().to_string(), p.partition()))
                .collect()
        };
        match rebalance {
            rdkafka::consumer::Rebalance::Revoke(list) => {
                self.note_rebalanced(Rebalanced::Revoked(partitions(list)));
            }
            rdkafka::consumer::Rebalance::Assign(list) => {
                self.note_rebalanced(Rebalanced::Assigned(partitions(list)));
            }
            rdkafka::consumer::Rebalance::Error(_) => {}
        }
    }
}

impl rdkafka::producer::ProducerContext for StatsContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        result: &rdkafka::producer::DeliveryResult<'_>,
        _opaque: Self::DeliveryOpaque,
    ) {
        // A dropped delivery report is a lost record reported as a success, so
        // record it even though this context exists for the statistics callback.
        self.delivery.record(result);
    }
}

/// Calculate total consumer lag across all partitions.
///
/// Helper function to sum lag from a `KafkaMetrics` snapshot. This is lag
/// behind the COMMITTED offset; see [`StatsContext::total_position_lag`] for
/// lag behind the read position.
#[must_use]
pub fn total_consumer_lag(metrics: &KafkaMetrics) -> i64 {
    metrics.partition_lag.values().sum()
}

/// One partition's records past the consumer's read position, or `None` for a
/// partition this consumer is not reading.
///
/// librdkafka measures `consumer_lag` from the committed offset to the end the
/// consumer may read to (the last stable offset under `read_committed`, the
/// high watermark otherwise), so that end is `consumer_lag + committed_offset`.
/// `asked_end`, the end the broker reported for a paused partition, wins when
/// it is later.
fn partition_position_lag(
    p: &rdkafka::statistics::Partition,
    asked_end: Option<i64>,
) -> Option<i64> {
    let position = if p.app_offset >= 0 {
        p.app_offset
    } else if p.committed_offset >= 0 {
        p.committed_offset
    } else {
        return None;
    };
    let reported = if p.consumer_lag >= 0 && p.committed_offset >= 0 {
        Some(p.consumer_lag + p.committed_offset)
    } else if p.hi_offset >= 0 {
        Some(p.hi_offset)
    } else {
        None
    };
    let end = match (reported, asked_end) {
        (Some(reported), Some(asked)) => reported.max(asked),
        (Some(end), None) | (None, Some(end)) => end,
        (None, None) => return None,
    };
    Some((end - position).max(0))
}

/// Records past the read position, summed over every partition being read.
fn position_lag(stats: &Statistics, asked_ends: &HashMap<(String, i32), i64>) -> i64 {
    stats
        .topics
        .iter()
        .flat_map(|(name, topic)| topic.partitions.iter().map(move |p| (name, p)))
        .filter(|(_, (id, _))| **id >= 0)
        .filter_map(|(name, (id, p))| {
            let asked_end = if asked_ends.is_empty() {
                None
            } else {
                asked_ends.get(&(name.clone(), *id)).copied()
            };
            partition_position_lag(p, asked_end)
        })
        .sum()
}

/// Get brokers in "UP" state.
#[must_use]
pub fn healthy_broker_count(metrics: &KafkaMetrics) -> usize {
    metrics.brokers.values().filter(|b| b.state == "UP").count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_context_creation() {
        let ctx = StatsContext::new();
        let metrics = ctx.get_metrics();
        assert_eq!(metrics.messages_sent, 0);
        assert_eq!(metrics.messages_received, 0);
    }

    #[test]
    fn test_kafka_metrics_default() {
        let metrics = KafkaMetrics::default();
        assert_eq!(metrics.messages_sent, 0);
        assert!(metrics.brokers.is_empty());
        assert!(metrics.partition_lag.is_empty());
    }

    #[test]
    fn test_broker_metrics_default() {
        let metrics = BrokerMetrics::default();
        assert_eq!(metrics.state, "");
        assert!(metrics.rtt_avg_ms.abs() < f64::EPSILON);
    }

    #[test]
    fn test_total_consumer_lag() {
        let mut metrics = KafkaMetrics::default();
        metrics.partition_lag.insert(("topic".to_string(), 0), 100);
        metrics.partition_lag.insert(("topic".to_string(), 1), 200);
        metrics.partition_lag.insert(("topic".to_string(), 2), 50);

        assert_eq!(total_consumer_lag(&metrics), 350);
    }

    #[test]
    fn test_healthy_broker_count() {
        let mut metrics = KafkaMetrics::default();
        metrics.brokers.insert(
            "broker1".to_string(),
            BrokerMetrics {
                state: "UP".to_string(),
                ..Default::default()
            },
        );
        metrics.brokers.insert(
            "broker2".to_string(),
            BrokerMetrics {
                state: "DOWN".to_string(),
                ..Default::default()
            },
        );
        metrics.brokers.insert(
            "broker3".to_string(),
            BrokerMetrics {
                state: "UP".to_string(),
                ..Default::default()
            },
        );

        assert_eq!(healthy_broker_count(&metrics), 2);
    }

    #[test]
    fn test_stats_context_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StatsContext>();
    }

    /// Statistics for one partition of `events`, committed to 5 and read to
    /// 20, whose last fetch saw the end at 20.
    fn stats_read_to_20() -> Statistics {
        let partition = rdkafka::statistics::Partition {
            partition: 0,
            app_offset: 20,
            committed_offset: 5,
            hi_offset: 20,
            consumer_lag: 15,
            ..Default::default()
        };
        let topic = rdkafka::statistics::Topic {
            topic: "events".to_string(),
            partitions: HashMap::from([(0, partition)]),
            ..Default::default()
        };
        Statistics {
            topics: HashMap::from([("events".to_string(), topic)]),
            ..Default::default()
        }
    }

    #[test]
    fn lag_counts_from_the_commit_and_the_read_position() {
        let stats = stats_read_to_20();
        let none = HashMap::new();
        assert_eq!(
            total_consumer_lag(&StatsContext::convert_stats(&stats, &none)),
            15
        );
        assert_eq!(position_lag(&stats, &none), 0);
    }

    #[test]
    fn an_end_asked_of_the_broker_moves_the_lag_the_last_fetch_left_behind() {
        let stats = stats_read_to_20();
        let asked = HashMap::from([(("events".to_string(), 0), 50)]);
        let metrics = StatsContext::convert_stats(&stats, &asked);
        assert_eq!(total_consumer_lag(&metrics), 45, "end 50 less committed 5");
        assert_eq!(
            metrics.partition_high_watermark[&("events".to_string(), 0)],
            50
        );
        assert_eq!(position_lag(&stats, &asked), 30, "end 50 less read-to 20");

        let behind = HashMap::from([(("events".to_string(), 0), 10)]);
        assert_eq!(
            position_lag(&stats, &behind),
            0,
            "an end older than the fetch's never lowers it"
        );
    }

    #[test]
    fn a_resume_drops_the_asked_ends_and_a_late_answer_is_ignored() {
        let context = StatsContext::new();
        let ends = || HashMap::from([(("events".to_string(), 0), 50)]);
        context.set_paused_ends(ends());
        assert!(
            context.paused_ends.read().expect("lock").is_empty(),
            "not paused, so nothing is kept"
        );
        context.set_paused(true);
        context.set_paused_ends(ends());
        assert_eq!(context.paused_ends.read().expect("lock").len(), 1);
        context.set_paused(false);
        assert!(context.paused_ends.read().expect("lock").is_empty());
    }
}
