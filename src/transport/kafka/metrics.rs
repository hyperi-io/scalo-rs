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
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError, RwLock};

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
    /// Records left to read per partition, keyed by (topic, partition).
    ///
    /// Once the context has served a rebalance, this covers the partitions
    /// the consumer holds: counted from the committed offset, or from the read
    /// position on a partition with nothing committed. A context that has
    /// served none, as with a consumer given partitions by `assign()`, covers
    /// every partition with a committed offset, counted from it.
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
    /// The partitions this consumer holds, as its rebalances left them.
    assigned: RwLock<HashSet<(String, i32)>>,
    /// Whether the inbound gate has this consumer's assignment paused.
    #[cfg(feature = "governor")]
    paused: AtomicBool,
    /// Log ends asked of the broker while paused, since librdkafka learns a
    /// partition's end only from a fetch and fetches nothing it has paused.
    #[cfg(feature = "governor")]
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
            assigned: RwLock::new(HashSet::new()),
            #[cfg(feature = "governor")]
            paused: AtomicBool::new(false),
            #[cfg(feature = "governor")]
            paused_ends: RwLock::new(HashMap::new()),
        }
    }

    /// Record that the inbound gate paused or resumed the assignment. A
    /// resume drops the ends asked of the broker: fetches report the end
    /// again from then on.
    #[cfg(feature = "governor")]
    pub(crate) fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
        if !paused && let Ok(mut ends) = self.paused_ends.write() {
            ends.clear();
        }
    }

    /// Whether the inbound gate has the assignment paused.
    #[cfg(feature = "governor")]
    pub(crate) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// The log ends the broker reported for the paused assignment, used by
    /// every statistics callback until the next refresh or a resume.
    #[cfg(feature = "governor")]
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

    /// Add partitions a rebalance gave this consumer to its assignment.
    fn assign(&self, given: &[(String, i32)]) {
        self.assigned
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(given.iter().cloned());
    }

    /// Take partitions a rebalance took from this consumer out of its
    /// assignment, and set their published lag to 0.
    fn revoke(&self, taken: &[(String, i32)]) {
        {
            let mut assigned = self
                .assigned
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for partition in taken {
                assigned.remove(partition);
            }
        }
        {
            let mut latest = self
                .latest_metrics
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for partition in taken {
                latest.partition_lag.remove(partition);
            }
        }
        // The recorder never drops a series, so a lost partition must not keep its last lag.
        #[cfg(feature = "metrics")]
        for (topic, partition) in taken {
            lag_gauge(topic, *partition).set(0.0);
        }
    }

    /// Records past this consumer's read position, summed over its partitions,
    /// as of the last statistics callback.
    ///
    /// [`total_consumer_lag`] counts from the COMMITTED offset where there is
    /// one, so a commit held until delivery reads as backlog there. This counts
    /// from where the consumer has read to: unread backlog, whatever the commit
    /// policy. A scaling signal that should not grow while acknowledgements are
    /// held reads this one.
    ///
    /// Once the context has served a rebalance, the sum covers the partitions
    /// the consumer holds, and one that has read nothing counts from
    /// librdkafka's fetch position. A context that has served none, as with a
    /// consumer given partitions by `assign()`, sums every partition with an
    /// application or committed position.
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
    ///
    /// `held` is the assignment when rebalances have given one: lag then
    /// covers those partitions, committed or not. `None` counts every
    /// partition with a committed offset, from that offset.
    fn convert_stats(
        stats: &Statistics,
        asked_ends: &HashMap<(String, i32), i64>,
        held: Option<&HashSet<(String, i32)>>,
    ) -> KafkaMetrics {
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

                let lag = match held {
                    Some(assigned) if assigned.contains(&key) => {
                        partition_lag(partition, asked_end)
                    }
                    Some(_) => None,
                    None => committed_lag(partition, asked_end),
                };
                if let Some(lag) = lag {
                    metrics.partition_lag.insert(key.clone(), lag);
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
        // Only the inbound gate pauses an assignment, so only it asks for ends.
        #[cfg(feature = "governor")]
        let asked_ends = self
            .paused_ends
            .read()
            .map(|ends| ends.clone())
            .unwrap_or_default();
        #[cfg(not(feature = "governor"))]
        let asked_ends = HashMap::new();
        let (metrics, unread) = {
            let assigned = self.assigned.read().unwrap_or_else(PoisonError::into_inner);
            // With no rebalance served, the empty set means unknown, not nothing held.
            let held = (self.rebalances() > 0).then_some(&*assigned);
            (
                Self::convert_stats(&statistics, &asked_ends, held),
                position_lag(&statistics, &asked_ends, held),
            )
        };
        self.position_lag.store(unread, Ordering::Relaxed);

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
    /// A context that has served a rebalance also sets
    /// `consumer_partitions_assigned` to the partitions it holds.
    #[cfg(feature = "metrics")]
    pub fn emit_prometheus_metrics(&self) {
        let m = self.get_metrics();

        // Only a context that has rebalanced knows the assignment; the others would write 0.
        if self.rebalances() > 0 {
            let held = self
                .assigned
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .len();
            metrics::gauge!("consumer_partitions_assigned").set(held as f64);
        }

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
            lag_gauge(topic, *partition).set(*lag as f64);
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

/// The `rdkafka_topic_partition_consumer_lag` series for one partition.
#[cfg(feature = "metrics")]
fn lag_gauge(topic: &str, partition: i32) -> metrics::Gauge {
    metrics::gauge!(
        "rdkafka_topic_partition_consumer_lag",
        "topic" => topic.to_string(),
        "partition" => partition.to_string()
    )
}

/// Records each revoke and assignment, in order, before librdkafka applies
/// it, so the transport never holds or commits for a partition it lost, and
/// keeps the assignment the lag and `consumer_partitions_assigned` report.
impl rdkafka::consumer::ConsumerContext for StatsContext {
    fn pre_rebalance(
        &self,
        _consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        let partitions = |list: &rdkafka::TopicPartitionList| -> Vec<(String, i32)> {
            list.elements()
                .iter()
                .map(|p| (p.topic().to_string(), p.partition()))
                .collect()
        };
        match rebalance {
            rdkafka::consumer::Rebalance::Revoke(list) => {
                let taken = partitions(list);
                self.revoke(&taken);
                self.note_rebalanced(Rebalanced::Revoked(taken));
            }
            rdkafka::consumer::Rebalance::Assign(list) => {
                let given = partitions(list);
                self.assign(&given);
                self.note_rebalanced(Rebalanced::Assigned(given));
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
/// Helper function to sum lag from a `KafkaMetrics` snapshot; see
/// [`KafkaMetrics::partition_lag`] for what each partition counts from, and
/// [`StatsContext::total_position_lag`] for lag behind the read position
/// throughout.
#[must_use]
pub fn total_consumer_lag(metrics: &KafkaMetrics) -> i64 {
    metrics.partition_lag.values().sum()
}

/// One partition's records past its committed offset, or `None` for a
/// partition librdkafka reports no `consumer_lag` for, as it does until
/// something is committed. `asked_end`, the end the broker reported for a
/// paused partition, wins when it is later.
fn committed_lag(p: &rdkafka::statistics::Partition, asked_end: Option<i64>) -> Option<i64> {
    let lag = match asked_end {
        Some(end) if p.committed_offset >= 0 => p.consumer_lag.max(end - p.committed_offset),
        _ => p.consumer_lag,
    };
    (lag >= 0).then_some(lag)
}

/// One held partition's records left to read: from the committed offset, or
/// from the read position while nothing is committed. `None` when the
/// statistics do not yet give the partition's end or read position.
fn partition_lag(p: &rdkafka::statistics::Partition, asked_end: Option<i64>) -> Option<i64> {
    committed_lag(p, asked_end).or_else(|| partition_position_lag(p, asked_end, true))
}

/// One partition's records past the consumer's read position, or `None` for a
/// partition this consumer is not reading.
///
/// The read position is the application's, else the committed offset, else,
/// for a partition the assignment shows is `held`, librdkafka's fetch
/// position, where a partition that has handed the application nothing
/// starts reading. librdkafka keeps copying a stopped partition's fetch
/// position into its statistics, so without an assignment to check against it
/// would count partitions the consumer no longer holds.
///
/// librdkafka measures `consumer_lag` from the committed offset to the end the
/// consumer may read to (the last stable offset under `read_committed`, the
/// high watermark otherwise), so that end is `consumer_lag + committed_offset`.
/// A partition with nothing committed measures to the high watermark, which
/// differs from the last stable offset only under `read_committed` with a
/// transaction open upstream. `asked_end`, the end the broker reported for a
/// paused partition, wins when it is later.
fn partition_position_lag(
    p: &rdkafka::statistics::Partition,
    asked_end: Option<i64>,
    held: bool,
) -> Option<i64> {
    let fetched = held.then_some(p.next_offset);
    let position = [Some(p.app_offset), Some(p.committed_offset), fetched]
        .into_iter()
        .flatten()
        .find(|&offset| offset >= 0)?;
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

/// Records past the read position, summed over the `held` partitions when
/// rebalances have given an assignment, else over every partition with an
/// application or committed position.
fn position_lag(
    stats: &Statistics,
    asked_ends: &HashMap<(String, i32), i64>,
    held: Option<&HashSet<(String, i32)>>,
) -> i64 {
    stats
        .topics
        .iter()
        .flat_map(|(name, topic)| topic.partitions.iter().map(move |p| (name, p)))
        .filter(|(_, (id, _))| **id >= 0)
        .filter_map(|(name, (id, p))| {
            let key = (name.clone(), *id);
            let is_held = match held {
                Some(assigned) if !assigned.contains(&key) => return None,
                Some(_) => true,
                None => false,
            };
            partition_position_lag(p, asked_ends.get(&key).copied(), is_held)
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

    /// librdkafka's `RD_KAFKA_OFFSET_INVALID`: no offset known.
    const INVALID: i64 = -1001;

    /// Statistics carrying `partition` as partition 0 of `events`.
    fn stats_with(partition: rdkafka::statistics::Partition) -> Statistics {
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

    /// Statistics for one partition of `events`, committed to 5 and read to
    /// 20, whose last fetch saw the end at 20.
    fn stats_read_to_20() -> Statistics {
        stats_with(rdkafka::statistics::Partition {
            partition: 0,
            app_offset: 20,
            committed_offset: 5,
            hi_offset: 20,
            consumer_lag: 15,
            ..Default::default()
        })
    }

    /// Statistics for partition 0 of `events` just assigned to a group that
    /// has committed nothing: librdkafka reports no `consumer_lag`, and the
    /// application has read nothing, so only the fetch position and `end` are
    /// known.
    fn stats_uncommitted(end: i64) -> Statistics {
        stats_with(rdkafka::statistics::Partition {
            partition: 0,
            app_offset: INVALID,
            committed_offset: INVALID,
            next_offset: 0,
            hi_offset: end,
            consumer_lag: -1,
            ..Default::default()
        })
    }

    fn events_0() -> (String, i32) {
        ("events".to_string(), 0)
    }

    fn assigned_events_0() -> HashSet<(String, i32)> {
        HashSet::from([events_0()])
    }

    #[test]
    fn lag_counts_from_the_commit_and_the_read_position() {
        let stats = stats_read_to_20();
        let none = HashMap::new();
        let assigned = assigned_events_0();
        assert_eq!(
            total_consumer_lag(&StatsContext::convert_stats(&stats, &none, Some(&assigned))),
            15
        );
        assert_eq!(position_lag(&stats, &none, Some(&assigned)), 0);
    }

    #[test]
    fn an_end_asked_of_the_broker_moves_the_lag_the_last_fetch_left_behind() {
        let stats = stats_read_to_20();
        let assigned = assigned_events_0();
        let asked = HashMap::from([(events_0(), 50)]);
        let metrics = StatsContext::convert_stats(&stats, &asked, Some(&assigned));
        assert_eq!(total_consumer_lag(&metrics), 45, "end 50 less committed 5");
        assert_eq!(metrics.partition_high_watermark[&events_0()], 50);
        assert_eq!(
            position_lag(&stats, &asked, Some(&assigned)),
            30,
            "end 50 less read-to 20"
        );

        let behind = HashMap::from([(events_0(), 10)]);
        assert_eq!(
            position_lag(&stats, &behind, Some(&assigned)),
            0,
            "an end older than the fetch's never lowers it"
        );
    }

    #[test]
    fn an_assigned_partition_with_nothing_committed_reports_lag_from_where_it_reads() {
        let none = HashMap::new();
        let assigned = assigned_events_0();

        let empty = stats_uncommitted(0);
        let metrics = StatsContext::convert_stats(&empty, &none, Some(&assigned));
        assert_eq!(
            metrics.partition_lag.get(&events_0()),
            Some(&0),
            "an empty partition publishes lag 0, not nothing"
        );
        assert_eq!(position_lag(&empty, &none, Some(&assigned)), 0);

        let backlog = stats_uncommitted(5);
        let metrics = StatsContext::convert_stats(&backlog, &none, Some(&assigned));
        assert_eq!(
            metrics.partition_lag.get(&events_0()),
            Some(&5),
            "end 5 less fetch position 0"
        );
        assert_eq!(position_lag(&backlog, &none, Some(&assigned)), 5);

        let asked = HashMap::from([(events_0(), 7)]);
        assert_eq!(
            total_consumer_lag(&StatsContext::convert_stats(
                &empty,
                &asked,
                Some(&assigned)
            )),
            7,
            "paused: the end asked of the broker, less fetch position 0"
        );
        assert_eq!(position_lag(&empty, &asked, Some(&assigned)), 7);
    }

    #[test]
    fn a_partition_this_consumer_does_not_hold_reports_no_lag() {
        let stats = stats_read_to_20();
        let none = HashMap::new();
        let metrics = StatsContext::convert_stats(&stats, &none, Some(&HashSet::new()));
        assert!(
            metrics.partition_lag.is_empty(),
            "committed but not assigned: {:?}",
            metrics.partition_lag
        );
        assert_eq!(
            metrics.partition_committed.get(&events_0()),
            Some(&5),
            "offsets are still reported"
        );
        let asked = HashMap::from([(events_0(), 50)]);
        assert_eq!(position_lag(&stats, &asked, Some(&HashSet::new())), 0);
    }

    /// A context that has served no rebalance, as with a consumer given its
    /// partitions by `assign()` or statistics fed straight in, counts every
    /// partition from its committed offset and its application's position.
    #[test]
    fn a_context_that_never_rebalanced_counts_every_committed_partition() {
        let context = StatsContext::new();
        context.stats(stats_with(rdkafka::statistics::Partition {
            partition: 0,
            committed_offset: 10,
            app_offset: 60,
            hi_offset: 100,
            ls_offset: 100,
            consumer_lag: 90,
            ..Default::default()
        }));
        assert_eq!(context.total_position_lag(), 40, "end 100 less read-to 60");
        assert_eq!(
            total_consumer_lag(&context.get_metrics()),
            90,
            "end 100 less committed 10"
        );
    }

    /// Without an assignment to check against, a fetch position may belong to a
    /// partition the consumer no longer holds, so it is never read as one.
    #[test]
    fn a_context_that_never_rebalanced_reads_no_fetch_position() {
        let context = StatsContext::new();
        context.stats(stats_uncommitted(5));
        assert!(
            context.get_metrics().partition_lag.is_empty(),
            "{:?}",
            context.get_metrics().partition_lag
        );
        assert_eq!(context.total_position_lag(), 0);
    }

    #[cfg(feature = "governor")]
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
