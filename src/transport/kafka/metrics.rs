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
//! use rdkafka::config::ClientConfig;
//! use rdkafka::consumer::BaseConsumer;
//! use scalo::transport::kafka::StatsContext;
//!
//! let consumer: BaseConsumer<StatsContext> = ClientConfig::new()
//!     .set("bootstrap.servers", "localhost:9092")
//!     .set("group.id", "example")
//!     .set("statistics.interval.ms", "5000")
//!     .create_with_context(StatsContext::new())?;
//!
//! // Then periodically:
//! let metrics = consumer.context().get_metrics();
//! println!("Messages received: {}", metrics.messages_received);
//! println!("Consumer lag: {:?}", metrics.partition_lag);
//! ```

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
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
    /// Per-partition committed offsets: once the context has served a
    /// rebalance, only for the partitions the consumer holds.
    pub partition_committed: HashMap<(String, i32), i64>,
    /// Per-partition high watermarks: once the context has served a
    /// rebalance, only for the partitions the consumer holds.
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
/// rdkafka takes the context by value, so each client has its own. A context
/// built with [`new`](Self::new) measures lag as a `read_committed` consumer
/// does, librdkafka's default, and writes its `consumer_` series with no
/// `group_id`.
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
    /// Where this consumer may read to, and so the end an uncommitted
    /// partition's lag counts to.
    isolation: Isolation,
    /// The client the statistics come from, which labels every `rdkafka_`
    /// series this context writes.
    #[cfg(feature = "metrics")]
    client: std::sync::OnceLock<Client>,
    /// The group this context's consumer joins, which labels every
    /// `consumer_` series it writes.
    #[cfg(feature = "metrics")]
    group: Group,
}

/// Where a consumer may read a partition to, as librdkafka's
/// `isolation.level` sets it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Isolation {
    /// To the last stable offset, which a transaction open upstream holds
    /// back. librdkafka's default.
    #[default]
    ReadCommitted,
    /// To the high watermark.
    ReadUncommitted,
}

impl Isolation {
    /// The level `config` sets, or librdkafka's default when it sets none.
    /// librdkafka matches the value in any case and fails client creation on
    /// any other.
    pub(crate) fn of(config: &ClientConfig) -> Self {
        match config.get("isolation.level") {
            Some(level) if level.eq_ignore_ascii_case("read_uncommitted") => Self::ReadUncommitted,
            _ => Self::ReadCommitted,
        }
    }

    /// The offset a consumer may read `p` to, which librdkafka measures its
    /// own `consumer_lag` to, or `None` before a fetch has reported it.
    fn end(self, p: &rdkafka::statistics::Partition) -> Option<i64> {
        let end = match self {
            Self::ReadCommitted => p.ls_offset,
            Self::ReadUncommitted => p.hi_offset,
        };
        (end >= 0).then_some(end)
    }
}

/// The librdkafka client a context's statistics come from.
///
/// Its configured `client.id` and its type (`consumer` or `producer`) hold
/// across a rebuild and a restart. librdkafka's handle `name` does not: it
/// numbers every client the process creates, so a rebuilt consumer would move
/// to a new series and leave its old one at its last value.
#[cfg(feature = "metrics")]
#[derive(Debug)]
struct Client {
    id: String,
    kind: String,
}

#[cfg(feature = "metrics")]
impl Client {
    fn of(stats: &Statistics) -> Self {
        Self {
            id: stats.client_id.clone(),
            kind: stats.client_type.clone(),
        }
    }

    /// This client's `name` series, keyed further by `labels`.
    fn gauge<const N: usize>(
        &self,
        name: &'static str,
        labels: [(&'static str, String); N],
    ) -> metrics::Gauge {
        let labels: Vec<metrics::Label> = [
            ("client_id", self.id.clone()),
            ("client_type", self.kind.clone()),
        ]
        .into_iter()
        .chain(labels)
        .map(|(key, value)| metrics::Label::new(key, value))
        .collect();
        metrics::gauge!(name, labels)
    }
}

/// The consumer group a context's consumer joins, as its client config names
/// it.
///
/// Like `client.id`, it holds across a rebuild and a restart, so each group in
/// a process keeps its own `consumer_` series. A context built with
/// [`StatsContext::new`] knows no group and writes them without the label.
#[cfg(feature = "metrics")]
#[derive(Debug, Default)]
struct Group(Option<String>);

#[cfg(feature = "metrics")]
impl Group {
    /// `labels`, led by `group_id` when the group is known.
    fn labels<const N: usize>(&self, labels: [(&'static str, String); N]) -> Vec<metrics::Label> {
        self.0
            .iter()
            .map(|group| metrics::Label::new("group_id", group.clone()))
            .chain(
                labels
                    .into_iter()
                    .map(|(key, value)| metrics::Label::new(key, value)),
            )
            .collect()
    }

    /// This group's `name` gauge, keyed further by `labels`.
    fn gauge<const N: usize>(
        &self,
        name: &'static str,
        labels: [(&'static str, String); N],
    ) -> metrics::Gauge {
        metrics::gauge!(name, self.labels(labels))
    }

    /// This group's `name` counter.
    fn counter(&self, name: &'static str) -> metrics::Counter {
        metrics::counter!(name, self.labels([]))
    }
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
            isolation: Isolation::default(),
            #[cfg(feature = "metrics")]
            client: std::sync::OnceLock::new(),
            #[cfg(feature = "metrics")]
            group: Group::default(),
        }
    }

    /// This context, counting lag to where a consumer at `isolation` may read.
    #[must_use]
    pub(crate) fn reading(mut self, isolation: Isolation) -> Self {
        self.isolation = isolation;
        self
    }

    /// This context, labelling its `consumer_` series with `group`, the
    /// `group.id` its consumer joins.
    #[cfg(feature = "metrics")]
    #[must_use]
    pub(crate) fn in_group(mut self, group: Option<&str>) -> Self {
        self.group = Group(group.map(str::to_owned));
        self
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
        // One event per revoke or assignment, as librdkafka's own `rebalance_cnt` counts them.
        #[cfg(feature = "metrics")]
        self.group.counter(CONSUMER_REBALANCES).increment(1);
    }

    /// Add partitions a rebalance gave this consumer to its assignment.
    fn assign(&self, given: &[(String, i32)]) {
        self.assigned
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(given.iter().cloned());
    }

    /// Take partitions a rebalance took from this consumer out of its
    /// assignment and its snapshot, and set the lag and committed offset its
    /// own group and client published for them to 0.
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
                latest.partition_committed.remove(partition);
                latest.partition_high_watermark.remove(partition);
            }
        }
        // The recorder cannot drop one series, so a lost partition must not keep its last values.
        #[cfg(feature = "metrics")]
        for (topic, partition) in taken {
            self.group
                .gauge(CONSUMER_LAG, partition_labels(topic, *partition))
                .set(0.0);
            // Before its first statistics a client has published no series to zero.
            if let Some(client) = self.client.get() {
                for name in [PARTITION_LAG, PARTITION_COMMITTED] {
                    client
                        .gauge(name, partition_labels(topic, *partition))
                        .set(0.0);
                }
            }
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
    /// `held` is the assignment when rebalances have given one: lag, committed
    /// offsets and high watermarks then cover only those partitions, and lag
    /// covers them committed or not. librdkafka keeps reporting a revoked
    /// partition's last committed offset and high watermark, so without the
    /// assignment they would outlive the revoke. `None` counts lag for every
    /// partition with a committed offset, from that offset, and reports every
    /// partition's offsets. `isolation` sets the end a held partition with
    /// nothing committed counts to.
    fn convert_stats(
        stats: &Statistics,
        asked_ends: &HashMap<(String, i32), i64>,
        held: Option<&HashSet<(String, i32)>>,
        isolation: Isolation,
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
                        partition_lag(partition, asked_end, isolation)
                    }
                    Some(_) => continue,
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
        #[cfg(feature = "metrics")]
        self.client.get_or_init(|| Client::of(&statistics));
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
                Self::convert_stats(&statistics, &asked_ends, held, self.isolation),
                position_lag(&statistics, &asked_ends, held, self.isolation),
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
                eprintln!("ERROR librdkafka: {fac} {log_message}");
            }
            RDKafkaLogLevel::Warning => {
                #[cfg(feature = "logger")]
                tracing::warn!(target: "librdkafka", facility = fac, "{}", log_message);
                #[cfg(not(feature = "logger"))]
                eprintln!("WARN librdkafka: {fac} {log_message}");
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
        eprintln!("ERROR librdkafka: {error}: {reason}");
    }
}

impl StatsContext {
    /// Emit current metrics as Prometheus gauges/counters via the `metrics` crate.
    ///
    /// Call periodically (e.g. after each stats callback) to push rdkafka
    /// internal statistics to the global metrics recorder. No-op if no
    /// recorder is installed.
    ///
    /// Emits under the `rdkafka_` prefix per the metrics standard. Every
    /// `rdkafka_` series carries `client_id` (the configured `client.id`) and
    /// `client_type` (`consumer` or `producer`) from librdkafka's statistics,
    /// so a transport's consumer and producer, or two transports in one
    /// process, each keep their own series. They are emitted from the first
    /// statistics callback on, which names the client.
    ///
    /// Per-partition metrics are bounded by `max_partitions` (default 256).
    /// A context that has served a rebalance also sets
    /// `consumer_partitions_assigned` to the partitions it holds, and
    /// `consumer_lag{topic,partition}` to each one's lag. Both carry
    /// `group_id`, the group the `KafkaTransport`'s consumer joins, so each
    /// group in a process keeps its own. A context built with
    /// [`new`](Self::new) writes them without it.
    #[cfg(feature = "metrics")]
    pub fn emit_prometheus_metrics(&self) {
        let m = self.get_metrics();

        // Only a context that has rebalanced knows the assignment, so only it writes these series.
        let rebalanced = self.rebalances() > 0;
        if rebalanced {
            let held = self
                .assigned
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .len();
            self.group.gauge(CONSUMER_PARTITIONS, []).set(held as f64);
        }

        // Before the first statistics there is no client to label and nothing to report.
        let Some(client) = self.client.get() else {
            return;
        };

        // Global counters
        client
            .gauge("rdkafka_global_msg_cnt", [])
            .set(m.queue_message_count as f64);
        client
            .gauge("rdkafka_global_msg_size_bytes", [])
            .set(m.queue_byte_count as f64);

        // Per-broker metrics
        for (name, broker) in &m.brokers {
            client
                .gauge("rdkafka_broker_rtt_avg_seconds", [("broker", name.clone())])
                .set(broker.rtt_avg_ms / 1000.0);
            client
                .gauge("rdkafka_broker_outbuf_cnt", [("broker", name.clone())])
                .set(broker.outbuf_msg_cnt as f64);
            client
                .gauge("rdkafka_broker_waitresp_cnt", [("broker", name.clone())])
                .set(broker.waitresp_cnt as f64);
        }

        // Per-partition consumer lag (capped at 256 partitions for cardinality safety)
        let max_partitions = 256;
        for (i, ((topic, partition), lag)) in m.partition_lag.iter().enumerate() {
            if i >= max_partitions {
                break;
            }
            client
                .gauge(PARTITION_LAG, partition_labels(topic, *partition))
                .set(*lag as f64);
            if rebalanced {
                self.group
                    .gauge(CONSUMER_LAG, partition_labels(topic, *partition))
                    .set(*lag as f64);
            }
        }

        for (i, ((topic, partition), offset)) in m.partition_committed.iter().enumerate() {
            if i >= max_partitions {
                break;
            }
            client
                .gauge(PARTITION_COMMITTED, partition_labels(topic, *partition))
                .set(*offset as f64);
        }

        // Rebalance count
        if m.rebalance_count > 0 {
            client
                .gauge("rdkafka_consumer_rebalance_count", [])
                .set(m.rebalance_count as f64);
        }
    }
}

/// Per-partition lag from librdkafka's statistics.
#[cfg(feature = "metrics")]
const PARTITION_LAG: &str = "rdkafka_topic_partition_consumer_lag";
/// Per-partition committed offset from librdkafka's statistics.
#[cfg(feature = "metrics")]
const PARTITION_COMMITTED: &str = "rdkafka_topic_partition_committed_offset";
/// `ConsumerMetrics`' per-partition lag, which the transport fills for the partitions it holds.
#[cfg(feature = "metrics")]
const CONSUMER_LAG: &str = "consumer_lag";
/// `ConsumerMetrics`' assigned partition count, which the transport sets.
#[cfg(feature = "metrics")]
const CONSUMER_PARTITIONS: &str = "consumer_partitions_assigned";
/// `ConsumerMetrics`' rebalance counter, which the transport counts.
#[cfg(feature = "metrics")]
const CONSUMER_REBALANCES: &str = "consumer_rebalance_total";

/// The labels that key one partition's series.
#[cfg(feature = "metrics")]
fn partition_labels(topic: &str, partition: i32) -> [(&'static str, String); 2] {
    [
        ("topic", topic.to_string()),
        ("partition", partition.to_string()),
    ]
}

/// Records each revoke and assignment, in order, before librdkafka applies
/// it, so the transport never holds or commits for a partition it lost,
/// keeps the assignment the lag and `consumer_partitions_assigned` report,
/// and counts each one in its group's `consumer_rebalance_total`.
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
fn partition_lag(
    p: &rdkafka::statistics::Partition,
    asked_end: Option<i64>,
    isolation: Isolation,
) -> Option<i64> {
    committed_lag(p, asked_end).or_else(|| partition_position_lag(p, asked_end, true, isolation))
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
/// A partition with nothing committed measures to the same end, read from the
/// offset `isolation` names. `asked_end`, the end the broker reported for a
/// paused partition, wins when it is later.
fn partition_position_lag(
    p: &rdkafka::statistics::Partition,
    asked_end: Option<i64>,
    held: bool,
    isolation: Isolation,
) -> Option<i64> {
    let fetched = held.then_some(p.next_offset);
    let position = [Some(p.app_offset), Some(p.committed_offset), fetched]
        .into_iter()
        .flatten()
        .find(|&offset| offset >= 0)?;
    let reported = if p.consumer_lag >= 0 && p.committed_offset >= 0 {
        Some(p.consumer_lag + p.committed_offset)
    } else {
        isolation.end(p)
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
    isolation: Isolation,
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
            partition_position_lag(p, asked_ends.get(&key).copied(), is_held, isolation)
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

    /// librdkafka's default isolation level.
    const COMMITTED: Isolation = Isolation::ReadCommitted;

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
    /// 20, whose last fetch saw the end at 20 with no transaction open.
    fn stats_read_to_20() -> Statistics {
        stats_with(rdkafka::statistics::Partition {
            partition: 0,
            app_offset: 20,
            committed_offset: 5,
            hi_offset: 20,
            ls_offset: 20,
            consumer_lag: 15,
            ..Default::default()
        })
    }

    /// Statistics for partition 0 of `events` just assigned to a group that
    /// has committed nothing: librdkafka reports no `consumer_lag`, and the
    /// application has read nothing, so only the fetch position and `end` are
    /// known. With no transaction open, `end` is both the high watermark and
    /// the last stable offset.
    fn stats_uncommitted(end: i64) -> Statistics {
        stats_with(rdkafka::statistics::Partition {
            partition: 0,
            app_offset: INVALID,
            committed_offset: INVALID,
            next_offset: 0,
            hi_offset: end,
            ls_offset: end,
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
            total_consumer_lag(&StatsContext::convert_stats(
                &stats,
                &none,
                Some(&assigned),
                COMMITTED
            )),
            15
        );
        assert_eq!(position_lag(&stats, &none, Some(&assigned), COMMITTED), 0);
    }

    #[test]
    fn an_end_asked_of_the_broker_moves_the_lag_the_last_fetch_left_behind() {
        let stats = stats_read_to_20();
        let assigned = assigned_events_0();
        let asked = HashMap::from([(events_0(), 50)]);
        let metrics = StatsContext::convert_stats(&stats, &asked, Some(&assigned), COMMITTED);
        assert_eq!(total_consumer_lag(&metrics), 45, "end 50 less committed 5");
        assert_eq!(metrics.partition_high_watermark[&events_0()], 50);
        assert_eq!(
            position_lag(&stats, &asked, Some(&assigned), COMMITTED),
            30,
            "end 50 less read-to 20"
        );

        let behind = HashMap::from([(events_0(), 10)]);
        assert_eq!(
            position_lag(&stats, &behind, Some(&assigned), COMMITTED),
            0,
            "an end older than the fetch's never lowers it"
        );
    }

    #[test]
    fn an_assigned_partition_with_nothing_committed_reports_lag_from_where_it_reads() {
        let none = HashMap::new();
        let assigned = assigned_events_0();

        let empty = stats_uncommitted(0);
        let metrics = StatsContext::convert_stats(&empty, &none, Some(&assigned), COMMITTED);
        assert_eq!(
            metrics.partition_lag.get(&events_0()),
            Some(&0),
            "an empty partition publishes lag 0, not nothing"
        );
        assert_eq!(position_lag(&empty, &none, Some(&assigned), COMMITTED), 0);

        let backlog = stats_uncommitted(5);
        let metrics = StatsContext::convert_stats(&backlog, &none, Some(&assigned), COMMITTED);
        assert_eq!(
            metrics.partition_lag.get(&events_0()),
            Some(&5),
            "end 5 less fetch position 0"
        );
        assert_eq!(position_lag(&backlog, &none, Some(&assigned), COMMITTED), 5);

        let asked = HashMap::from([(events_0(), 7)]);
        assert_eq!(
            total_consumer_lag(&StatsContext::convert_stats(
                &empty,
                &asked,
                Some(&assigned),
                COMMITTED
            )),
            7,
            "paused: the end asked of the broker, less fetch position 0"
        );
        assert_eq!(position_lag(&empty, &asked, Some(&assigned), COMMITTED), 7);
    }

    /// librdkafka keeps reporting a revoked partition's last committed offset
    /// and high watermark, so none of it may reach the snapshot.
    #[test]
    fn a_partition_this_consumer_does_not_hold_reports_no_lag_or_offsets() {
        let stats = stats_read_to_20();
        let none = HashMap::new();
        let metrics = StatsContext::convert_stats(&stats, &none, Some(&HashSet::new()), COMMITTED);
        assert!(
            metrics.partition_lag.is_empty(),
            "committed but not assigned: {:?}",
            metrics.partition_lag
        );
        assert!(
            metrics.partition_committed.is_empty(),
            "{:?}",
            metrics.partition_committed
        );
        assert!(
            metrics.partition_high_watermark.is_empty(),
            "{:?}",
            metrics.partition_high_watermark
        );
        let asked = HashMap::from([(events_0(), 50)]);
        assert_eq!(
            position_lag(&stats, &asked, Some(&HashSet::new()), COMMITTED),
            0
        );
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
        let metrics = context.get_metrics();
        assert_eq!(
            total_consumer_lag(&metrics),
            90,
            "end 100 less committed 10"
        );
        assert_eq!(metrics.partition_committed.get(&events_0()), Some(&10));
        assert_eq!(
            metrics.partition_high_watermark.get(&events_0()),
            Some(&100)
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

    /// Statistics for partition 0 of `events` just assigned, with nothing
    /// committed or read, while a transaction open upstream holds the last
    /// stable offset at 6 and the high watermark is at 10.
    fn stats_uncommitted_in_transaction() -> Statistics {
        stats_with(rdkafka::statistics::Partition {
            partition: 0,
            app_offset: INVALID,
            committed_offset: INVALID,
            next_offset: 0,
            hi_offset: 10,
            ls_offset: 6,
            consumer_lag: -1,
            ..Default::default()
        })
    }

    /// A context holding partition 0 of `events`, as a rebalance left it.
    fn holding_events_0(context: &StatsContext) {
        context.assign(&[events_0()]);
        context.note_rebalanced(Rebalanced::Assigned(vec![events_0()]));
    }

    #[test]
    fn an_uncommitted_partition_measures_to_the_last_stable_offset_by_default() {
        let context = StatsContext::new();
        holding_events_0(&context);
        context.stats(stats_uncommitted_in_transaction());
        assert_eq!(
            context.get_metrics().partition_lag.get(&events_0()),
            Some(&6),
            "last stable offset 6 less fetch position 0, not the high watermark"
        );
        assert_eq!(context.total_position_lag(), 6);
    }

    /// librdkafka reports the last stable offset at the high watermark under
    /// `read_uncommitted`, so this pulls the two apart to prove which one is read.
    #[test]
    fn an_uncommitted_partition_under_read_uncommitted_measures_to_the_high_watermark() {
        let context = StatsContext::new().reading(Isolation::ReadUncommitted);
        holding_events_0(&context);
        context.stats(stats_uncommitted_in_transaction());
        assert_eq!(
            context.get_metrics().partition_lag.get(&events_0()),
            Some(&10),
            "high watermark 10 less fetch position 0"
        );
        assert_eq!(context.total_position_lag(), 10);
    }

    #[test]
    fn the_isolation_level_comes_from_the_client_config() {
        let with = |level: &str| {
            let mut config = ClientConfig::new();
            config.set("isolation.level", level);
            Isolation::of(&config)
        };
        assert_eq!(
            Isolation::of(&ClientConfig::new()),
            Isolation::ReadCommitted
        );
        assert_eq!(with("read_committed"), Isolation::ReadCommitted);
        assert_eq!(with("read_uncommitted"), Isolation::ReadUncommitted);
        assert_eq!(
            with("READ_UNCOMMITTED"),
            Isolation::ReadUncommitted,
            "librdkafka matches the value in any case"
        );
    }

    #[cfg(feature = "metrics")]
    mod series {
        use std::sync::Arc;

        use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};

        use super::*;

        /// Keeps every gauge by its key, name and labels, as Prometheus keys a series.
        #[derive(Default)]
        struct GaugeCapture {
            gauges: Mutex<HashMap<Key, Arc<AtomicU64>>>,
        }

        impl GaugeCapture {
            /// Every series named `name`: its labels, sorted, and its value.
            fn series(&self, name: &str) -> Vec<(Vec<(String, String)>, f64)> {
                let mut found: Vec<_> = self
                    .gauges
                    .lock()
                    .expect("capture lock")
                    .iter()
                    .filter(|(key, _)| key.name() == name)
                    .map(|(key, cell)| {
                        let mut labels: Vec<(String, String)> = key
                            .labels()
                            .map(|l| (l.key().to_string(), l.value().to_string()))
                            .collect();
                        labels.sort();
                        (labels, f64::from_bits(cell.load(Ordering::Acquire)))
                    })
                    .collect();
                found.sort_by(|a, b| a.0.cmp(&b.0));
                found
            }
        }

        impl Recorder for GaugeCapture {
            fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

            fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> metrics::Counter {
                metrics::Counter::noop()
            }

            fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> metrics::Gauge {
                let cell = Arc::clone(
                    self.gauges
                        .lock()
                        .expect("capture lock")
                        .entry(key.clone())
                        .or_default(),
                );
                metrics::Gauge::from_arc(cell)
            }

            fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> metrics::Histogram {
                metrics::Histogram::noop()
            }
        }

        /// `stats` as librdkafka reports them for the `n`th handle this
        /// process made, `client_id` of `client_type`.
        fn from_client(
            client_id: &str,
            client_type: &str,
            n: u32,
            stats: Statistics,
        ) -> Statistics {
            Statistics {
                name: format!("{client_id}#{client_type}-{n}"),
                client_id: client_id.to_string(),
                client_type: client_type.to_string(),
                ..stats
            }
        }

        /// The labels one client's series carry, sorted, with `extra` added.
        fn client(
            client_id: &str,
            client_type: &str,
            extra: &[(&str, &str)],
        ) -> Vec<(String, String)> {
            let mut labels: Vec<(String, String)> =
                [("client_id", client_id), ("client_type", client_type)]
                    .iter()
                    .chain(extra)
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect();
            labels.sort();
            labels
        }

        /// Statistics with `queued` messages in the producer queue and `outbuf`
        /// waiting for broker `b1`.
        fn queued(queued: u64, outbuf: i64) -> Statistics {
            let broker = rdkafka::statistics::Broker {
                name: "b1".to_string(),
                state: "UP".to_string(),
                outbuf_msg_cnt: outbuf,
                ..Default::default()
            };
            Statistics {
                msg_cnt: queued,
                brokers: HashMap::from([("b1".to_string(), broker)]),
                ..Default::default()
            }
        }

        /// A transport's consumer and producer write the same `rdkafka_` names,
        /// so without a client label the last write wins and the value flips
        /// between them.
        #[test]
        fn a_consumer_and_a_producer_in_one_process_publish_separate_series() {
            let capture = GaugeCapture::default();
            let consumer = StatsContext::new();
            let producer = StatsContext::new();
            metrics::with_local_recorder(&capture, || {
                producer.stats(from_client("app", "producer", 2, queued(42, 7)));
                consumer.stats(from_client("app", "consumer", 1, queued(0, 0)));
            });
            assert_eq!(
                capture.series("rdkafka_global_msg_cnt"),
                vec![
                    (client("app", "consumer", &[]), 0.0),
                    (client("app", "producer", &[]), 42.0),
                ]
            );
            assert_eq!(
                capture.series("rdkafka_broker_outbuf_cnt"),
                vec![
                    (client("app", "consumer", &[("broker", "b1")]), 0.0),
                    (client("app", "producer", &[("broker", "b1")]), 7.0),
                ]
            );
        }

        /// Two consumers in one process reading the same partition in
        /// different groups each keep their own lag and committed offset.
        #[test]
        fn two_consumers_on_one_partition_publish_separate_lag_series() {
            let capture = GaugeCapture::default();
            let loader = StatsContext::new();
            let archiver = StatsContext::new();
            metrics::with_local_recorder(&capture, || {
                loader.stats(from_client("loader", "consumer", 1, behind_by(35)));
                archiver.stats(from_client("archiver", "consumer", 3, behind_by(20)));
            });
            let partition = [("partition", "0"), ("topic", "events")];
            assert_eq!(
                capture.series("rdkafka_topic_partition_consumer_lag"),
                vec![
                    (client("archiver", "consumer", &partition), 30.0),
                    (client("loader", "consumer", &partition), 15.0),
                ]
            );
            assert_eq!(
                capture.series("rdkafka_topic_partition_committed_offset"),
                vec![
                    (client("archiver", "consumer", &partition), 20.0),
                    (client("loader", "consumer", &partition), 35.0),
                ]
            );
        }

        /// A client rebuilt in place, or the same process restarted, takes a
        /// new handle number but keeps its client id and type, so it writes
        /// the series it wrote before rather than leaving that one stale.
        #[test]
        fn a_rebuilt_client_writes_the_series_it_wrote_before() {
            let capture = GaugeCapture::default();
            metrics::with_local_recorder(&capture, || {
                StatsContext::new().stats(from_client("app", "producer", 2, queued(42, 0)));
                StatsContext::new().stats(from_client("app", "producer", 5, queued(3, 0)));
            });
            assert_eq!(
                capture.series("rdkafka_global_msg_cnt"),
                vec![(client("app", "producer", &[]), 3.0)]
            );
        }

        /// A revoke zeroes the lag and committed offset its own client
        /// published, where the recorder keeps them.
        #[test]
        fn a_revoke_zeroes_the_series_its_client_published() {
            let capture = GaugeCapture::default();
            let context = StatsContext::new();
            holding_events_0(&context);
            metrics::with_local_recorder(&capture, || {
                context.stats(from_client("loader", "consumer", 1, stats_read_to_20()));
                context.revoke(&[events_0()]);
            });
            let partition = [("partition", "0"), ("topic", "events")];
            assert_eq!(
                capture.series("rdkafka_topic_partition_consumer_lag"),
                vec![(client("loader", "consumer", &partition), 0.0)]
            );
            assert_eq!(
                capture.series("rdkafka_topic_partition_committed_offset"),
                vec![(client("loader", "consumer", &partition), 0.0)]
            );
        }

        /// The labels one group's `consumer_` series carry, sorted, with `extra` added.
        fn group(group: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
            let mut labels: Vec<(String, String)> = [("group_id", group)]
                .iter()
                .chain(extra)
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            labels.sort();
            labels
        }

        /// Statistics for partition 0 of `events`, committed and read to
        /// `committed`, with the end at 50.
        fn behind_by(committed: i64) -> Statistics {
            stats_with(rdkafka::statistics::Partition {
                partition: 0,
                app_offset: committed,
                committed_offset: committed,
                hi_offset: 50,
                ls_offset: 50,
                consumer_lag: 50 - committed,
                ..Default::default()
            })
        }

        /// Two consumers in one process, in different groups, each publish
        /// their own assignment and lag.
        #[test]
        fn two_groups_in_one_process_publish_separate_consumer_series() {
            let capture = GaugeCapture::default();
            let loader = StatsContext::new().in_group(Some("loader"));
            let archiver = StatsContext::new().in_group(Some("archiver"));
            holding_events_0(&loader);
            let two = vec![events_0(), ("events".to_string(), 1)];
            archiver.assign(&two);
            archiver.note_rebalanced(Rebalanced::Assigned(two));
            metrics::with_local_recorder(&capture, || {
                loader.stats(behind_by(35));
                archiver.stats(behind_by(20));
            });
            let partition = [("partition", "0"), ("topic", "events")];
            assert_eq!(
                capture.series("consumer_partitions_assigned"),
                vec![(group("archiver", &[]), 2.0), (group("loader", &[]), 1.0)]
            );
            assert_eq!(
                capture.series("consumer_lag"),
                vec![
                    (group("archiver", &partition), 30.0),
                    (group("loader", &partition), 15.0),
                ]
            );
        }

        /// A revoke zeroes its own group's lag, and leaves another group's
        /// lag on the same partition as it was.
        #[test]
        fn a_revoke_zeroes_only_its_own_groups_consumer_lag() {
            let capture = GaugeCapture::default();
            let loader = StatsContext::new().in_group(Some("loader"));
            let archiver = StatsContext::new().in_group(Some("archiver"));
            holding_events_0(&loader);
            holding_events_0(&archiver);
            metrics::with_local_recorder(&capture, || {
                loader.stats(behind_by(35));
                archiver.stats(behind_by(20));
                loader.revoke(&[events_0()]);
            });
            let partition = [("partition", "0"), ("topic", "events")];
            assert_eq!(
                capture.series("consumer_lag"),
                vec![
                    (group("archiver", &partition), 30.0),
                    (group("loader", &partition), 0.0),
                ]
            );
        }

        /// A context built with `new` knows no group, so its `consumer_`
        /// series carry none.
        #[test]
        fn a_context_with_no_group_writes_the_consumer_series_without_one() {
            let capture = GaugeCapture::default();
            let context = StatsContext::new();
            holding_events_0(&context);
            metrics::with_local_recorder(&capture, || context.stats(behind_by(35)));
            assert_eq!(
                capture.series("consumer_partitions_assigned"),
                vec![(Vec::new(), 1.0)]
            );
            let partition = vec![
                ("partition".to_string(), "0".to_string()),
                ("topic".to_string(), "events".to_string()),
            ];
            assert_eq!(capture.series("consumer_lag"), vec![(partition, 15.0)]);
        }
    }
}
