// Project:   scalo
// File:      src/transport/kafka/mod.rs
// Purpose:   High-throughput Kafka transport for PB/day workloads
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Kafka Transport
//!
//! High-throughput Kafka transport optimized for PB/day batch processing.
//! Uses rdkafka (librdkafka wrapper) with batch-first design.
//!
//! ## Performance Characteristics
//!
//! - **Batch-first**: Designed for 10K+ messages per batch
//! - **Zero-copy where possible**: Minimizes allocations in hot path
//! - **Interned topic cache**: shared RwLock map, read-fast-path per message
//! - **Non-blocking batch drain**: Uses zero-timeout poll to drain internal queue
//! - **At-least-once delivery**: Manual commit after processing
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::{KafkaTransport, KafkaConfig, Transport};
//!
//! let config = KafkaConfig {
//!     brokers: vec!["kafka:9092".to_string()],
//!     group: "dfe-loader".to_string(),
//!     topics: vec!["events".to_string()],
//!     ..Default::default()
//! };
//!
//! let transport = KafkaTransport::new(&config).await?;
//!
//! // Batch processing loop
//! loop {
//!     // Poll for up to 10K messages
//!     let batch = transport.recv(10_000).await?;
//!     if batch.is_empty() {
//!         continue;
//!     }
//!
//!     // Process entire batch
//!     process_batch(&batch.records);
//!
//!     // Commit AFTER successful processing (at-least-once)
//!     transport.commit(&batch.commit_tokens).await?;
//! }
//! ```

mod admin;
mod classify;
mod config;
pub mod contract;
mod metrics;
mod producer;
pub mod providers;
mod token;
pub mod topic_resolver;

pub use admin::{KafkaAdmin, TopicInfo};
#[allow(deprecated)]
pub use config::{
    CLASSIC_ONLY_CONSUMER_KEYS, ConsumerKnobs, ConsumerProtocol, DEVTEST_PROFILE,
    HIGH_THROUGHPUT_CONSUMER_DEFAULTS, KafkaConfig, KafkaProfile, KafkaSizingConfig,
    LOW_LATENCY_CONSUMER_DEFAULTS, MESSAGE_MAX_BYTES, PRODUCER_DEFAULTS, PRODUCER_DEVTEST,
    PRODUCER_EXACTLY_ONCE, PRODUCER_HIGH_THROUGHPUT, PRODUCER_LOW_LATENCY, PRODUCTION_PROFILE,
    ProducerKnobs, SelfRegulationProfile, SuppressionRule, merge_with_overrides,
};
pub use metrics::{
    BrokerMetrics, KafkaMetrics, StatsContext, healthy_broker_count, total_consumer_lag,
};
pub use producer::{KafkaProducer, ProducerMetrics, ProducerProfile};
pub use providers::{
    AuthKind, KafkaProvider, KnownProvider, MetadataMode, ProviderCapabilities, SchemaRegistry,
};
pub use token::KafkaToken;
pub use topic_resolver::{TopicRefreshHandle, TopicResolver};

use super::error::{TransportError, TransportResult};
use super::traits::{RecvBatch, TransportBase, TransportReceiver, TransportSender};
use super::types::{Message, PayloadFormat, SendResult};
use super::work_batch::{Record, WorkBatch};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message as KafkaMessage;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// High-throughput tuning defaults.
///
/// These are optimized for PB/day batch workloads.
pub mod tuning {
    /// Default batch size for recv() - 10K messages.
    pub const DEFAULT_BATCH_SIZE: usize = 10_000;

    /// Maximum time to spend draining the internal queue (ms).
    /// After this, return what we have to maintain responsiveness.
    pub const MAX_DRAIN_MS: u64 = 100;

    /// Poll timeout when queue is empty - triggers network fetch.
    pub const POLL_TIMEOUT_MS: u64 = 50;

    /// Pre-allocated message vector capacity.
    pub const INITIAL_BATCH_CAPACITY: usize = 10_000;
}

/// How long a send keeps re-offering a record to a full producer queue before
/// reporting `Backpressured`.
const QUEUE_FULL_TIMEOUT: Duration = Duration::from_secs(5);

/// Pause between re-offers while the producer queue is full.
const QUEUE_FULL_RETRY: Duration = Duration::from_millis(100);

/// W3C traceparent headers for the active span, when propagation is built in.
fn trace_headers() -> Option<OwnedHeaders> {
    #[cfg(feature = "transport-trace")]
    {
        super::propagation::current_traceparent().map(|tp| {
            OwnedHeaders::new().insert(rdkafka::message::Header {
                key: super::propagation::TRACEPARENT_HEADER,
                value: Some(tp.as_str()),
            })
        })
    }
    #[cfg(not(feature = "transport-trace"))]
    {
        None
    }
}

/// Build one produce record for `destination`, carrying `headers` when set.
fn produce_record<'a>(
    destination: &'a str,
    payload: &'a [u8],
    headers: Option<&OwnedHeaders>,
) -> FutureRecord<'a, str, [u8]> {
    let record = FutureRecord::to(destination).payload(payload);
    match headers {
        Some(h) => record.headers(h.clone()),
        None => record,
    }
}

/// Fold per-record results into the block's result: the first `Backpressured`
/// or `Fatal` in record order, else `Ok`.
///
/// `Ok` (sent or dropped by a filter) and `FilteredDlq` are handled records, so
/// they never fail the block. Any other result means at least one record is
/// unconfirmed, and the caller must retry the block rather than commit it.
fn block_result(results: Vec<SendResult>) -> SendResult {
    results
        .into_iter()
        .find(|r| matches!(r, SendResult::Backpressured | SendResult::Fatal(_)))
        .unwrap_or(SendResult::Ok)
}

/// High-throughput Kafka transport using rdkafka.
///
/// Optimized for batch-oriented consumption at PB/day scale:
/// - Uses `BaseConsumer` for direct poll control (see recv() decision note)
/// - Interns topic strings in a shared cache (read-fast-path per message)
/// - Drains internal queue with zero-timeout polls
/// - Minimizes allocations in hot path
pub struct KafkaTransport {
    /// librdkafka consumer.
    ///
    /// Behind an `Arc` so the optional [`KafkaGateActuator`] (G3, behind the
    /// `governor` feature) can hold a clone and call `pause`/`resume` on the
    /// ASSIGNED partitions without `unsafe` and without taking ownership away
    /// from the recv poll loop. Every `Consumer` method we use (`poll`,
    /// `subscribe`, `commit`, `assignment`, `pause`, `resume`,
    /// `fetch_group_list`) takes `&self`, so a shared `Arc` serves both the
    /// transport's poll loop and the actuator's pause/resume with no lock --
    /// librdkafka is internally synchronised.
    consumer: Arc<BaseConsumer<StatsContext>>,
    producer: FutureProducer<StatsContext>,
    /// Persistent topic-string interner. Shared across `recv()` calls so a
    /// newly-discovered topic is interned once (not re-`Arc`'d every batch) --
    /// the previous per-recv clone discarded new entries. RwLock: reads
    /// dominate (topics repeat), writes only on first sight of a topic.
    topic_cache: parking_lot::RwLock<HashMap<String, Arc<str>>>,
    closed: AtomicBool,
    /// Shared healthy flag -- read by health registry closure, written by close().
    healthy: Arc<AtomicBool>,
    /// Latches a sustained retryable send failure so the warn fires on the edge,
    /// not once per record.
    send_degraded: classify::DegradedLatch,
    /// Topics we're subscribed to (for cache warming and Debug).
    /// Behind RwLock so recv() can update after topic refresh re-subscribe.
    subscribed_topics: parking_lot::RwLock<Vec<String>>,
    /// Consumer group id, retained so the partition-limited diagnostic can scope
    /// `fetch_group_list` to THIS group rather than reading every group on the
    /// (possibly shared, PB-scale) cluster. Only the governor-gated diagnostic
    /// reads it.
    #[cfg(feature = "governor")]
    group_id: String,
    /// Shutdown token -- cancelled on close() to stop background tasks.
    shutdown_token: tokio_util::sync::CancellationToken,
    /// Periodic topic refresh handle (auto-discovery mode only).
    /// Checked on each recv() call to detect new/removed topics.
    /// Uses parking_lot::Mutex (no poisoning, faster uncontended) since this
    /// is on the recv() hot path.
    topic_refresh: Option<parking_lot::Mutex<TopicRefreshHandle>>,
    /// Transport-level message filter engine.
    filter_engine: super::filter::TransportFilterEngine,
    /// Optional inbound gate (`governor` feature). `None` by default ->
    /// `recv()` makes no gate calls and behaviour is byte-identical to today.
    /// When `Some`, each `recv()` calls [`InboundGate::evaluate`], which drives
    /// the [`KafkaGateActuator`] on pause/resume edges. The poll is ALWAYS
    /// issued regardless of hold state -- paused partitions just return nothing,
    /// keeping the consumer-group heartbeat alive (no rebalance). It is purely
    /// additive and opt-in until a later release turns it on by default.
    #[cfg(feature = "governor")]
    inbound_gate: Option<crate::governor::InboundGate>,
    /// Diagnostic dedup latch for the `kafka_partition_limited` warning.
    /// Rate-limits the warning to once per cooldown window so a persistently
    /// partition-limited consumer does not spam the log. `None` until the
    /// diagnostic is consulted; behaviour is unchanged when the diagnostic is
    /// never invoked.
    #[cfg(feature = "governor")]
    partition_limited_warn: PartitionLimitedDiagnostic,
    /// Live partition-limited flag, read by a health check registered ONCE at
    /// construction. `check_partition_limited` stores into this flag instead of
    /// re-registering a health entry per tick: the registry does not dedupe, so
    /// the old per-tick `register` both grew the components Vec unboundedly and
    /// pinned health to `Degraded` forever after the first limited tick (one
    /// transient lag spike during scale-out would permanently fail `/readyz`).
    /// Reading the flag lets the status track the live condition in both
    /// directions.
    #[cfg(all(feature = "governor", feature = "health"))]
    partition_limited_flag: Arc<AtomicBool>,
}

/// Role naming a producer-only transport's idle consumer in its derived group id.
const PRODUCER_ONLY_GROUP_ROLE: &str = "producer-only";

/// Resolve the group.id to set on the consumer client: the caller's group when
/// set, else a stand-in for a producer-only transport.
///
/// librdkafka >= 2.x rejects an empty group.id at consumer creation, and the
/// consumer queries the stand-in's coordinator on connect, so the stand-in is
/// derived from the config (see [`KafkaConfig::internal_group_id`]) to sit
/// under the prefix the broker's group ACLs grant. A producer-only transport
/// never subscribes, so the stand-in group is never joined.
fn effective_consumer_group_id(config: &KafkaConfig) -> String {
    if config.group.is_empty() {
        config.internal_group_id(PRODUCER_ONLY_GROUP_ROLE)
    } else {
        config.group.clone()
    }
}

/// Build the consumer's librdkafka config for one group protocol.
///
/// Every documented layer runs first -- explicit fields, profile defaults, the
/// sizing surface, then `librdkafka_overrides` -- and the protocol is applied
/// LAST, because under `consumer` librdkafka refuses the client outright if a
/// classic-only property was set by any of them.
fn consumer_client_config(config: &KafkaConfig, protocol: ConsumerProtocol) -> ClientConfig {
    let mut client_config = ClientConfig::new();

    client_config.set("bootstrap.servers", config.brokers.join(","));
    // librdkafka >= 2.x refuses to create a consumer client with an EMPTY
    // group.id -- `rd_kafka_poll_set_consumer` returns "consumer queue not
    // available". A producer-only transport legitimately carries no consumer
    // group (callers signal this by clearing `config.group`), but the
    // constructor still builds a consumer client (it is non-optional). See
    // effective_consumer_group_id for the stand-in.
    client_config.set("group.id", effective_consumer_group_id(config));
    // Static membership (KIP-345): opt-in, must be unique per replica.
    if let Some(ref id) = config.group_instance_id {
        client_config.set("group.instance.id", id);
    }
    client_config.set("enable.auto.commit", config.enable_auto_commit.to_string());
    client_config.set(
        "auto.commit.interval.ms",
        config.auto_commit_interval_ms.to_string(),
    );
    client_config.set("session.timeout.ms", config.session_timeout_ms.to_string());
    client_config.set(
        "heartbeat.interval.ms",
        config.heartbeat_interval_ms.to_string(),
    );
    client_config.set(
        "max.poll.interval.ms",
        config.max_poll_interval_ms.to_string(),
    );
    client_config.set("fetch.min.bytes", config.fetch_min_bytes.to_string());
    client_config.set("fetch.max.bytes", config.fetch_max_bytes.to_string());
    client_config.set(
        "max.partition.fetch.bytes",
        config.max_partition_fetch_bytes.to_string(),
    );
    client_config.set("auto.offset.reset", &config.auto_offset_reset);
    client_config.set(
        "enable.partition.eof",
        config.enable_partition_eof.to_string(),
    );

    // Profile defaults (overridable by librdkafka_overrides).
    let rdkafka_config = config.build_librdkafka_config();
    for (key, value) in &rdkafka_config {
        client_config.set(key, value);
    }

    // Sizing surface:
    //   profile defaults < named consumer knobs < sizing.consumer_librdkafka
    for (key, value) in config.sizing.resolved_consumer_map() {
        client_config.set(key, value);
    }

    // Re-apply librdkafka_overrides LAST so they remain the highest-priority
    // layer the docs promise (config.rs precedence list). build_librdkafka_config
    // above also applied them, but the sizing surface in between would
    // otherwise clobber any fetch.* key an operator set via an override --
    // silently reverting a deployment's tuning on upgrade.
    for (key, value) in &config.librdkafka_overrides {
        client_config.set(key, value);
    }

    // Security.
    client_config.set("security.protocol", &config.security_protocol);
    if let Some(ref mechanism) = config.sasl_mechanism {
        client_config.set("sasl.mechanism", mechanism);
    }
    if let Some(ref username) = config.sasl_username {
        client_config.set("sasl.username", username);
    }
    if let Some(ref password) = config.sasl_password {
        client_config.set("sasl.password", password.expose());
    }

    // TLS.
    if let Some(ref ca) = config.ssl_ca_location {
        client_config.set("ssl.ca.location", ca);
    }
    if let Some(ref cert) = config.ssl_certificate_location {
        client_config.set("ssl.certificate.location", cert);
    }
    if let Some(ref key) = config.ssl_key_location {
        client_config.set("ssl.key.location", key);
    }
    if config.ssl_skip_verify {
        client_config.set("enable.ssl.certificate.verification", "false");
    }

    client_config.set("client.id", &config.client_id);
    // Fetch-from-follower (KIP-392): with a rack set, the consumer reads
    // from an in-zone replica instead of the leader. Consumer-side only,
    // and a no-op when the field is unset.
    if let Some(ref rack) = config.client_rack {
        client_config.set("client.rack", rack);
    }

    // Ensure statistics callbacks fire (all profiles already set this, but
    // guarantee it as a fallback for manual configs).
    if client_config.get("statistics.interval.ms").is_none() {
        client_config.set("statistics.interval.ms", "5000");
    }

    apply_group_protocol(&mut client_config, protocol);
    client_config
}

/// Set `group.protocol` and strip what that protocol forbids.
///
/// `group.remote.assignor` is deliberately left unset: with no client-side
/// choice the broker applies its own default (`uniform`), which is the
/// assignment KIP-848 exists to give.
fn apply_group_protocol(client_config: &mut ClientConfig, protocol: ConsumerProtocol) {
    client_config.set("group.protocol", protocol.as_str());
    if protocol != ConsumerProtocol::Consumer {
        return;
    }
    // librdkafka fails client creation if any of these was set AT ALL, so
    // remove rather than skip -- a raw override could otherwise reintroduce one.
    let dropped: Vec<&str> = CLASSIC_ONLY_CONSUMER_KEYS
        .iter()
        .copied()
        .filter(|key| client_config.get(key).is_some())
        .collect();
    for key in &dropped {
        client_config.remove(key);
    }
    if !dropped.is_empty() {
        tracing::debug!(
            keys = %dropped.join(", "),
            "kafka: dropped classic-only properties for group.protocol=consumer; \
             their replacements are broker-side"
        );
    }
}

/// Create the consumer client, `Arc`-wrapped so the optional gate actuator
/// (governor feature) can share it for pause/resume without `unsafe`.
fn create_consumer(
    client_config: &ClientConfig,
) -> TransportResult<Arc<BaseConsumer<StatsContext>>> {
    client_config
        .create_with_context(StatsContext::new())
        .map(Arc::new)
        .map_err(|e| TransportError::Connection(format!("Failed to create consumer: {e}")))
}

/// Subscribe the consumer to `topics`, or do nothing when the list is empty
/// (the producer-only case).
fn subscribe_consumer(
    consumer: &BaseConsumer<StatsContext>,
    topics: &[String],
) -> TransportResult<()> {
    if topics.is_empty() {
        return Ok(());
    }
    let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topics)
        .map_err(|e| TransportError::Connection(format!("Failed to subscribe: {e}")))
}

/// How long to wait for the broker to accept the consumer protocol, or `None`
/// when there is nothing to wait for.
///
/// `None` covers three cases: the protocol is already `classic`, the operator
/// set the window to zero, or `statistics.interval.ms` is off -- the probe
/// reads the group state from those statistics, so without them it could only
/// ever time out and downgrade a healthy consumer.
fn protocol_probe_window(
    config: &KafkaConfig,
    protocol: ConsumerProtocol,
    client_config: &ClientConfig,
) -> Option<Duration> {
    if protocol != ConsumerProtocol::Consumer || config.consumer_protocol_probe_ms == 0 {
        return None;
    }
    let stats_interval_ms: u64 = client_config
        .get("statistics.interval.ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if stats_interval_ms == 0 {
        tracing::warn!(
            "kafka: statistics are disabled, so the group.protocol=consumer probe cannot \
             observe the join -- keeping the consumer protocol with no fallback"
        );
        return None;
    }
    Some(Duration::from_millis(config.consumer_protocol_probe_ms))
}

/// Verdict of the one-shot KIP-848 negotiation probe.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ProtocolProbe {
    /// Keep the requested protocol.
    Keep,
    /// Rebuild as `classic`; the string is the reason for the warning.
    FallBack(String),
}

/// librdkafka error codes that mean the broker will not speak KIP-848.
///
/// A ConsumerGroupHeartbeat the broker cannot answer is raised as a FATAL
/// error carrying one of these, not as a poll error, so the fatal slot is where
/// the refusal shows up.
const PROTOCOL_REFUSAL_CODES: &[RDKafkaErrorCode] = &[
    RDKafkaErrorCode::UnsupportedVersion,
    RDKafkaErrorCode::UnsupportedFeature,
    RDKafkaErrorCode::UnsupportedAssignor,
];

/// Classify one tick of the negotiation probe.
///
/// `fatal` is librdkafka's fatal-error slot, `cgrp_state` the consumer-group
/// state from its statistics callback, and `expired` says the window is spent.
/// `None` means keep waiting.
fn classify_probe_tick(
    fatal: Option<(RDKafkaErrorCode, String)>,
    cgrp_state: Option<&str>,
    expired: bool,
) -> Option<ProtocolProbe> {
    if let Some((code, detail)) = fatal {
        return if PROTOCOL_REFUSAL_CODES.contains(&code) {
            Some(ProtocolProbe::FallBack(detail))
        } else {
            // Some other fatal -- auth, fencing. Classic would not fix it.
            Some(ProtocolProbe::Keep)
        };
    }
    if cgrp_state == Some("up") {
        return Some(ProtocolProbe::Keep);
    }
    if expired {
        return Some(ProtocolProbe::FallBack(
            "the consumer group did not reach state 'up' within the probe window".to_string(),
        ));
    }
    None
}

/// Wait, bounded, for the broker to accept `group.protocol=consumer`.
///
/// Reads librdkafka's own state rather than polling the consumer: a poll would
/// return a record, and dropping it here would advance the fetch position past
/// data nobody processed.
async fn probe_consumer_protocol(
    consumer: &BaseConsumer<StatsContext>,
    window: Duration,
) -> ProtocolProbe {
    const TICK: Duration = Duration::from_millis(100);

    let deadline = std::time::Instant::now() + window;
    loop {
        let state = consumer
            .client()
            .context()
            .get_metrics()
            .consumer_group_state;
        if let Some(verdict) = classify_probe_tick(
            consumer.client().fatal_error(),
            state.as_deref(),
            std::time::Instant::now() >= deadline,
        ) {
            return verdict;
        }
        tokio::time::sleep(TICK).await;
    }
}

impl KafkaTransport {
    /// Create a new high-throughput Kafka transport.
    ///
    /// The transport is optimized for batch consumption at PB/day scale.
    /// Configuration defaults are tuned for high throughput:
    /// - `fetch.max.bytes`: 50MB (controls network batch size)
    /// - `enable.auto.commit`: false (manual commit for at-least-once)
    ///
    /// A subscribing consumer negotiates the KIP-848 group protocol here, so
    /// construction can wait up to `consumer_protocol_probe_ms` before falling
    /// back to `classic`. See [`ConsumerProtocol`].
    ///
    /// # Errors
    ///
    /// Returns error if Kafka client creation fails.
    // Large but linear constructor (config -> client -> subscribe -> negotiate
    // -> assemble), over the 150-line soft cap with the config building
    // already factored out.
    #[allow(clippy::too_many_lines)]
    pub async fn new(config: &KafkaConfig) -> TransportResult<Self> {
        // Resolve the provider preset FIRST: if `config.provider` is set, derive
        // security_protocol + sasl_mechanism from it (never hand-set). Then enforce
        // the production guardrail (reject ssl_skip_verify / insecure transport) at
        // construction, not only when an app remembers to call validate().
        let mut owned = config.clone();
        owned.apply_provider().map_err(TransportError::Config)?;
        let config = &owned;
        config
            .validate(crate::env::is_production())
            .map_err(TransportError::Config)?;

        // StatsContext receives librdkafka statistics callbacks and auto-emits
        // rdkafka_* Prometheus metrics when a recorder is installed.
        // Consumer and producer each get their own context instance.
        let protocol = config.effective_consumer_protocol();
        let consumer_config = consumer_client_config(config, protocol);
        let probe_window = protocol_probe_window(config, protocol, &consumer_config);
        let consumer = create_consumer(&consumer_config)?;

        // Resolve effective topics:
        // - Empty group -> producer-only: no subscription, whatever `topics` holds
        // - Explicit list -> subscribe to those
        // - Empty + auto_discover -> auto-discover from broker
        // - Empty + !auto_discover -> no subscription (producer-only)
        let (effective_topics, topic_refresh, shutdown_token) = if config.group.is_empty() {
            // Subscribing here would join the producer-only stand-in group.
            if !config.topics.is_empty() || config.auto_discover {
                tracing::debug!(
                    topics = ?config.topics,
                    "kafka: group is empty, so this transport is producer-only and \
                     subscribes to nothing"
                );
            }
            (Vec::new(), None, tokio_util::sync::CancellationToken::new())
        } else if config.topics.is_empty() && config.auto_discover {
            tracing::info!("Topics empty -- auto-discovering from broker");
            let resolver = topic_resolver::TopicResolver::new(config)?;
            let discovered = resolver.resolve()?;
            if discovered.is_empty() {
                // No match yet is not a misconfiguration: the refresh loop
                // subscribes when the first matching topic appears.
                tracing::warn!(
                    "Auto-discovery found no matching topics -- consuming nothing until one appears"
                );
            }

            let token = tokio_util::sync::CancellationToken::new();
            let refresh = if config.topic_refresh_secs > 0 {
                let refresh_resolver = topic_resolver::TopicResolver::new(config)?;
                let handle = refresh_resolver.start_refresh_loop(
                    Duration::from_secs(config.topic_refresh_secs),
                    token.clone(),
                );
                tracing::info!(
                    interval_secs = config.topic_refresh_secs,
                    "Started periodic topic refresh"
                );
                Some(parking_lot::Mutex::new(handle))
            } else {
                None
            };

            (discovered, refresh, token)
        } else {
            (
                config.topics.clone(),
                None,
                tokio_util::sync::CancellationToken::new(),
            )
        };

        let subscribed_topics = effective_topics;
        subscribe_consumer(&consumer, &subscribed_topics)?;

        // KIP-848 negotiation. A producer-only transport joins no group, so
        // there is nothing to negotiate and no probe.
        let consumer = match probe_window {
            Some(window) if !subscribed_topics.is_empty() => {
                match probe_consumer_protocol(&consumer, window).await {
                    ProtocolProbe::Keep => consumer,
                    ProtocolProbe::FallBack(reason) => {
                        tracing::warn!(
                            brokers = %config.brokers.join(","),
                            group = %config.group,
                            reason = %reason,
                            "kafka: broker will not take group.protocol=consumer -- \
                             rebuilding this consumer as classic"
                        );
                        let classic = create_consumer(&consumer_client_config(
                            config,
                            ConsumerProtocol::Classic,
                        ))?;
                        subscribe_consumer(&classic, &subscribed_topics)?;
                        classic
                    }
                }
            }
            _ => consumer,
        };

        // Pre-populate topic cache -- eliminates locks in the hot path.
        let mut topic_cache = HashMap::with_capacity(subscribed_topics.len());
        for topic in &subscribed_topics {
            topic_cache.insert(topic.clone(), Arc::from(topic.as_str()));
        }

        // Build a SEPARATE producer ClientConfig. Creating the producer from the
        // CONSUMER client_config (which carries group.id, fetch.*,
        // session.timeout, ...) made it ignore the documented producer sizing --
        // it ran librdkafka producer DEFAULTS (no compression vs lz4, linger 5ms
        // vs 20ms, batch 1 MiB vs 128 KiB, queue 1 GiB vs 64 MiB; an unbounded
        // 1 GiB producer queue defeats container memory budgeting) and logged
        // "X is a consumer property" warnings. Apply the connection settings +
        // the producer sizing surface to a fresh config instead.
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", config.brokers.join(","));
        producer_config.set("security.protocol", &config.security_protocol);
        if let Some(ref mechanism) = config.sasl_mechanism {
            producer_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref username) = config.sasl_username {
            producer_config.set("sasl.username", username);
        }
        if let Some(ref password) = config.sasl_password {
            producer_config.set("sasl.password", password.expose());
        }
        if let Some(ref ca) = config.ssl_ca_location {
            producer_config.set("ssl.ca.location", ca);
        }
        if let Some(ref cert) = config.ssl_certificate_location {
            producer_config.set("ssl.certificate.location", cert);
        }
        if let Some(ref key) = config.ssl_key_location {
            producer_config.set("ssl.key.location", key);
        }
        if config.ssl_skip_verify {
            producer_config.set("enable.ssl.certificate.verification", "false");
        }
        producer_config.set("client.id", &config.client_id);
        // Producer sizing surface (compression, batch.size, linger.ms,
        // queue.buffering.max.kbytes, sticky.partitioning.linger.ms).
        for (key, value) in config.sizing.resolved_producer_map() {
            producer_config.set(key, value);
        }
        // librdkafka_overrides remain the highest-priority layer.
        for (key, value) in &config.librdkafka_overrides {
            producer_config.set(key, value);
        }
        if producer_config.get("statistics.interval.ms").is_none() {
            producer_config.set("statistics.interval.ms", "5000");
        }

        // Create producer with StatsContext for metrics collection.
        let producer: FutureProducer<StatsContext> = producer_config
            .create_with_context(StatsContext::new())
            .map_err(|e| TransportError::Connection(format!("Failed to create producer: {e}")))?;

        let healthy = Arc::new(AtomicBool::new(true));

        let filter_engine = super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade(),
        )?;

        #[cfg(feature = "health")]
        {
            let h = Arc::clone(&healthy);
            crate::health::HealthRegistry::register("transport:kafka", move || {
                if h.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Healthy
                } else {
                    crate::health::HealthStatus::Unhealthy
                }
            });
        }

        #[cfg(all(feature = "governor", feature = "health"))]
        let partition_limited_flag = Arc::new(AtomicBool::new(false));

        // Register the partition-limited health check ONCE, reading the live
        // flag, so it tracks the condition in both directions and never grows
        // the registry per tick.
        #[cfg(all(feature = "governor", feature = "health"))]
        {
            let f = Arc::clone(&partition_limited_flag);
            crate::health::HealthRegistry::register("kafka:partition_limited", move || {
                if f.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Degraded
                } else {
                    crate::health::HealthStatus::Healthy
                }
            });
        }

        Ok(Self {
            consumer,
            producer,
            topic_cache: parking_lot::RwLock::new(topic_cache),
            closed: AtomicBool::new(false),
            healthy,
            send_degraded: classify::DegradedLatch::default(),
            subscribed_topics: parking_lot::RwLock::new(subscribed_topics),
            #[cfg(feature = "governor")]
            group_id: config.group.clone(),
            shutdown_token,
            topic_refresh,
            filter_engine,
            #[cfg(feature = "governor")]
            inbound_gate: None,
            #[cfg(feature = "governor")]
            partition_limited_warn: PartitionLimitedDiagnostic::default(),
            #[cfg(all(feature = "governor", feature = "health"))]
            partition_limited_flag,
        })
    }

    /// Attach an [`InboundGate`](crate::governor::InboundGate) to this
    /// transport (`governor` feature).
    ///
    /// ADDITIVE + opt-in: the default is no gate, so a transport built without
    /// this call behaves byte-identically to before. When attached, every
    /// [`recv`](TransportReceiver::recv) calls [`evaluate`](crate::governor::InboundGate::evaluate)
    /// which drives a `KafkaGateActuator` on pause/resume edges. Build the
    /// gate with [`KafkaTransport::gate_actuator`] so it pauses the consumer's
    /// ASSIGNED partitions (member stays in the group -- no rebalance).
    ///
    /// CRUCIAL: even while held, `recv()` still issues the poll -- the
    /// actuator pauses partitions, not the poll, so the heartbeat is preserved.
    /// The CALLER must therefore keep calling `recv()` while the gate is held:
    /// `recv()` is what services the poll (and re-applies pause across any
    /// cooperative rebalance that lands during the hold). A driver that backs
    /// OFF `recv()` under pressure for longer than `max.poll.interval.ms`
    /// (default 300 s) is evicted from the group mid-hold -- so gate the SOURCE
    /// via this pause, never by pausing the recv loop itself.
    #[cfg(feature = "governor")]
    #[must_use]
    pub fn with_inbound_gate(mut self, gate: crate::governor::InboundGate) -> Self {
        self.inbound_gate = Some(gate);
        self
    }

    /// Whether an [`InboundGate`](crate::governor::InboundGate) is attached
    /// (`governor` feature).
    ///
    /// `true` once [`with_inbound_gate`](Self::with_inbound_gate) (directly or
    /// via [`SelfRegulationGovernor::attach_kafka_gate`](crate::SelfRegulationGovernor::attach_kafka_gate))
    /// has wired a gate. Used to assert governor-aware factory construction
    /// without reaching into the private field.
    #[cfg(feature = "governor")]
    #[must_use]
    pub fn has_inbound_gate(&self) -> bool {
        self.inbound_gate.is_some()
    }

    /// Build a [`GateActuator`](crate::governor::GateActuator) that pauses and
    /// resumes THIS transport's consumer (`governor` feature).
    ///
    /// The returned actuator holds an `Arc` clone of the shared consumer. On
    /// the rising edge it reads the current [`assignment`](rdkafka::consumer::Consumer::assignment)
    /// and [`pause`](rdkafka::consumer::Consumer::pause)s exactly those
    /// partitions; on the falling edge it [`resume`](rdkafka::consumer::Consumer::resume)s
    /// them. Pausing the ASSIGNED set (not unsubscribing) keeps the member in
    /// the consumer group, so no rebalance is triggered while we hold.
    ///
    /// Pass the result to [`InboundGate::new`](crate::governor::InboundGate::new),
    /// then [`with_inbound_gate`](Self::with_inbound_gate) the gate back onto
    /// the transport.
    #[cfg(feature = "governor")]
    #[must_use]
    pub fn gate_actuator(&self) -> Box<dyn crate::governor::GateActuator> {
        Box::new(KafkaGateActuator {
            consumer: Arc::clone(&self.consumer),
        })
    }

    /// Get the consumer's metrics snapshot.
    ///
    /// Returns statistics collected via librdkafka callbacks. Includes
    /// broker RTT, consumer lag, rebalance count, etc.
    #[must_use]
    pub fn stats(&self) -> KafkaMetrics {
        self.consumer.context().get_metrics()
    }

    /// Run the `kafka_partition_limited` DIAGNOSTIC against the live group
    /// (`governor` feature).
    ///
    /// Reads the consumer-group member count (via `fetch_group_list`), the
    /// topic partition count (from cached metadata), and the current total
    /// consumer lag (from `StatsContext`), then evaluates the pure
    /// [`partition_limited`] decision. When limited it:
    /// - sets the `kafka_partition_limited` gauge to `1.0` (else `0.0`),
    /// - records the diagnostic on the health registry, and
    /// - emits ONE rate-limited warning per cooldown window.
    ///
    /// NO topology mutation -- it never calls `createPartitions`. Returns the
    /// decision so callers can act on it. This is a
    /// metadata round-trip; call it periodically (e.g. once per refresh tick),
    /// NOT on the recv hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker metadata / group-list fetch fails.
    #[cfg(feature = "governor")]
    pub fn check_partition_limited(&self) -> TransportResult<bool> {
        // Total consumer lag across all assigned partitions.
        let metrics = self.consumer.context().get_metrics();
        let lag = u64::try_from(total_consumer_lag(&metrics).max(0)).unwrap_or(0);

        // Partition count: the TOPIC's TOTAL partitions, summed over the
        // subscribed topics -- NOT this member's assigned slice. (Using the
        // assignment count made `members >= partitions` fire whenever m^2 >= P,
        // a false positive with headroom for many more consumers.) Read from
        // broker metadata; a failed/empty fetch yields 0, which makes the
        // decision below false (never a false-positive "limited").
        let topics = self.subscribed_topics.read().clone();
        let mut partitions = 0usize;
        for topic in &topics {
            if let Ok(md) = self
                .consumer
                .fetch_metadata(Some(topic), Duration::from_secs(3))
            {
                partitions += md
                    .topics()
                    .iter()
                    .map(|t| t.partitions().len())
                    .sum::<usize>();
            }
        }

        // Member count: scope to THIS group, not every group on the cluster
        // (the old `None` read some other team's group on a shared cluster).
        // A transient/empty read is treated as a single member -- never a
        // false-positive "limited".
        let members = self
            .consumer
            .fetch_group_list(Some(&self.group_id), Duration::from_secs(3))
            .ok()
            .and_then(|list| list.groups().iter().map(|g| g.members().len()).max())
            .unwrap_or(1);

        let limited = partition_limited(members, partitions, lag);

        #[cfg(feature = "metrics")]
        ::metrics::gauge!("kafka_partition_limited").set(if limited { 1.0 } else { 0.0 });

        // Store into the flag the health check (registered once in `new`) reads,
        // so the status tracks the live condition in BOTH directions. The old
        // per-tick `register` leaked a Vec entry every tick and never cleared
        // Degraded once set.
        #[cfg(feature = "health")]
        self.partition_limited_flag
            .store(limited, Ordering::Relaxed);

        if limited
            && self
                .partition_limited_warn
                .should_warn_at(std::time::Instant::now())
        {
            tracing::warn!(
                members,
                partitions,
                lag,
                "kafka consumer group is partition-limited: members >= partitions \
                 with persistent lag -- extra consumers sit idle; the topic needs \
                 more partitions (diagnostic only, no topology change made)"
            );
        }

        Ok(limited)
    }

    /// Spawn a periodic background task that runs the
    /// [`check_partition_limited`](Self::check_partition_limited) diagnostic on
    /// `interval` until `shutdown` is cancelled (`governor` feature).
    ///
    /// This is the intended caller for the diagnostic: a COLD periodic tick OFF
    /// the hot recv path. Each tick is a broker metadata round-trip
    /// (`fetch_group_list`), so keep `interval` coarse (tens of seconds);
    /// pairing it with the topic-refresh cadence is a sensible default. The task
    /// only updates the `kafka_partition_limited` gauge + a rate-limited warning;
    /// it NEVER mutates topology.
    ///
    /// Wrap the transport in an `Arc` first (the actuator + receiver share it
    /// anyway), then call this with a clone.
    #[cfg(feature = "governor")]
    pub fn spawn_partition_limited_tick(
        self: Arc<Self>,
        interval: Duration,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // consume the immediate first tick
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = tick.tick() => {
                        // The diagnostic does synchronous broker round-trips
                        // (fetch_metadata + fetch_group_list). Run them OFF the
                        // async runtime worker so a slow broker cannot pin it.
                        let this = Arc::clone(&self);
                        match tokio::task::spawn_blocking(move || this.check_partition_limited()).await {
                            Ok(Err(e)) => tracing::debug!(error = %e, "partition-limited diagnostic tick failed"),
                            Err(e) => tracing::debug!(error = %e, "partition-limited diagnostic task join failed"),
                            Ok(Ok(_)) => {}
                        }
                    }
                }
            }
        });
    }
}

/// Produce-path steps shared by `send` and `send_batch`, so both paths filter,
/// classify and count a record the same way.
impl KafkaTransport {
    /// The settled result for a record the outbound filters keep off the wire,
    /// or `None` when it passes.
    fn outbound_disposition(&self, payload: &[u8]) -> Option<SendResult> {
        if !self.filter_engine.has_outbound_filters() {
            return None;
        }
        match self.filter_engine.apply_outbound(payload) {
            super::filter::FilterDisposition::Pass => None,
            super::filter::FilterDisposition::Drop => Some(SendResult::Ok),
            super::filter::FilterDisposition::Dlq => Some(SendResult::FilteredDlq),
        }
    }

    /// Offer one record to the producer queue without waiting for delivery.
    ///
    /// A full queue is re-offered every [`QUEUE_FULL_RETRY`] for up to
    /// [`QUEUE_FULL_TIMEOUT`], the same policy `send` gets from rdkafka. `Err`
    /// carries the settled result of a record the producer refused.
    async fn enqueue(
        &self,
        destination: &str,
        payload: &[u8],
        headers: Option<&OwnedHeaders>,
    ) -> Result<DeliveryFuture, SendResult> {
        let mut queue_full_since: Option<std::time::Instant> = None;
        loop {
            match self
                .producer
                .send_result(produce_record(destination, payload, headers))
            {
                Ok(delivery) => return Ok(delivery),
                Err((err, _)) => {
                    let queue_full =
                        classify::classify_send_failure(&err) == classify::SendFailure::QueueFull;
                    let since = *queue_full_since.get_or_insert_with(std::time::Instant::now);
                    if queue_full && since.elapsed() < QUEUE_FULL_TIMEOUT {
                        tokio::time::sleep(QUEUE_FULL_RETRY).await;
                        continue;
                    }
                    return Err(self.failure_result(destination, payload.len(), &err));
                }
            }
        }
    }

    /// Turn one delivery report into the caller's result.
    fn delivery_result(
        &self,
        destination: &str,
        bytes: usize,
        report: OwnedDeliveryResult,
    ) -> SendResult {
        match report {
            Ok(_) => {
                #[cfg(feature = "metrics")]
                {
                    ::metrics::counter!("transport_sent_total", "transport" => "kafka")
                        .increment(1);
                    ::metrics::counter!("transport_sent_bytes_total", "transport" => "kafka")
                        .increment(bytes as u64);
                }
                if self.send_degraded.clear() {
                    tracing::info!(destination, "kafka send recovered");
                }
                SendResult::Ok
            }
            Err((err, _)) => self.failure_result(destination, bytes, &err),
        }
    }

    /// Classify a failed produce, at enqueue or in the delivery report.
    fn failure_result(&self, destination: &str, bytes: usize, err: &KafkaError) -> SendResult {
        match classify::classify_send_failure(err) {
            classify::SendFailure::QueueFull => {
                #[cfg(feature = "metrics")]
                ::metrics::counter!(
                    "transport_backpressured_total",
                    "transport" => "kafka"
                )
                .increment(1);
                SendResult::Backpressured
            }
            classify::SendFailure::TooLarge => {
                let refused = TransportError::MessageTooLarge {
                    bytes,
                    detail: err.to_string(),
                };
                #[cfg(feature = "metrics")]
                ::metrics::counter!(
                    "transport_message_too_large_total",
                    "transport" => "kafka"
                )
                .increment(1);
                tracing::warn!(
                    destination,
                    error = %refused,
                    "kafka: record exceeds message.max.bytes -- dead-lettering it; \
                     raise the producer, broker and topic ceilings together"
                );
                SendResult::FilteredDlq
            }
            classify::SendFailure::Retryable => {
                #[cfg(feature = "metrics")]
                ::metrics::counter!(
                    "transport_send_errors_total",
                    "transport" => "kafka"
                )
                .increment(1);
                if self.send_degraded.enter() {
                    tracing::warn!(
                        destination,
                        error = %err,
                        "kafka send failed on a retryable condition; the caller retries"
                    );
                }
                SendResult::Backpressured
            }
            classify::SendFailure::Fatal => {
                #[cfg(feature = "metrics")]
                ::metrics::counter!(
                    "transport_send_errors_total",
                    "transport" => "kafka"
                )
                .increment(1);
                SendResult::Fatal(TransportError::Send(err.to_string()))
            }
        }
    }
}

/// One record's progress through `send_batch`.
enum Offered {
    /// Resolved without a delivery report: filtered, or refused at enqueue.
    Settled(SendResult),
    /// Queued; the delivery report arrives on the future.
    Queued(DeliveryFuture),
}

impl TransportBase for KafkaTransport {
    async fn close(&self) -> TransportResult<()> {
        self.closed.store(true, Ordering::Relaxed);
        self.healthy.store(false, Ordering::Relaxed);
        self.shutdown_token.cancel();
        // rdkafka handles cleanup on drop
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "kafka"
    }
}

impl TransportSender for KafkaTransport {
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        if let Some(settled) = self.outbound_disposition(&payload) {
            return settled;
        }

        let headers = trace_headers();
        let record = produce_record(destination, &payload, headers.as_ref());

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let result = self.delivery_result(
            destination,
            payload.len(),
            self.producer
                .send(record, Timeout::After(QUEUE_FULL_TIMEOUT))
                .await,
        );

        #[cfg(feature = "metrics")]
        ::metrics::histogram!(
            "transport_send_duration_seconds",
            "transport" => "kafka"
        )
        .record(start.elapsed().as_secs_f64());

        result
    }

    /// Offer every record of the block to the producer, then await all the
    /// delivery reports.
    ///
    /// Overrides the trait's per-record default, which waited out each record's
    /// delivery before offering the next and so paid one `linger.ms` window per
    /// record. Here the whole block is queued first, so librdkafka fills its
    /// MessageSets from it and the block costs roughly one linger window plus
    /// the broker round-trip. Records keep the default's routing: each goes to
    /// its own `key` (empty when `None`), payload only, headers not sent.
    ///
    /// ## Result -- never `Ok` unless every record is handled
    ///
    /// - Outbound filters apply per record before it is queued, exactly as in
    ///   [`send`](TransportSender::send): `Drop` and `FilteredDlq` records never
    ///   reach the wire and do not fail the block, and neither does a record
    ///   over `message.max.bytes`.
    /// - A record the producer refuses at enqueue (after the same queue-full
    ///   wait `send` allows) stops further offers.
    /// - Every queued record's report is awaited, even after a failure, so the
    ///   result describes the whole block and a caller retry never overlaps it.
    /// - The result is the first `Backpressured` or `Fatal` in record order,
    ///   else `Ok`. `Ok` means every record was confirmed by the broker or
    ///   handled by a filter.
    ///
    /// ## At-least-once caveat -- a failed block can be partly delivered
    ///
    /// Records are in flight together, so when one fails any subset of the
    /// others may already be confirmed (the default could only leave a prefix).
    /// The caller retries the whole block: duplicates, never loss. Per-partition
    /// order follows offer order while the idempotent producer is on (the
    /// default); with `idempotence: false`, a broker retry can reorder records
    /// within a partition.
    async fn send_batch(&self, records: &[Record]) -> SendResult {
        if records.is_empty() {
            return SendResult::Ok;
        }
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        let headers = trace_headers();
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let mut offered = Vec::with_capacity(records.len());
        for record in records {
            // A block of tens of thousands must not hold the runtime worker between awaits.
            tokio::task::coop::consume_budget().await;
            if let Some(settled) = self.outbound_disposition(&record.payload) {
                offered.push(Offered::Settled(settled));
                continue;
            }
            let destination = record.key.as_deref().unwrap_or("");
            match self
                .enqueue(destination, &record.payload, headers.as_ref())
                .await
            {
                Ok(delivery) => offered.push(Offered::Queued(delivery)),
                Err(settled) => {
                    let stop = matches!(settled, SendResult::Backpressured | SendResult::Fatal(_));
                    offered.push(Offered::Settled(settled));
                    // Records after a refusal are never offered; the refusal
                    // pushed above already fails the block.
                    if stop {
                        break;
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(offered.len());
        for (record, offer) in records.iter().zip(offered) {
            // Resolved reports are Ready at once, so this loop needs the same budget.
            tokio::task::coop::consume_budget().await;
            let result = match offer {
                Offered::Settled(settled) => settled,
                Offered::Queued(delivery) => {
                    let destination = record.key.as_deref().unwrap_or("");
                    let result = match delivery.await {
                        Ok(report) => {
                            self.delivery_result(destination, record.payload.len(), report)
                        }
                        // A report lost with the producer is an unknown outcome, never a delivery.
                        Err(_) => SendResult::Fatal(TransportError::Send(
                            "kafka delivery report lost: producer dropped".into(),
                        )),
                    };
                    #[cfg(feature = "metrics")]
                    ::metrics::histogram!(
                        "transport_send_duration_seconds",
                        "transport" => "kafka"
                    )
                    .record(start.elapsed().as_secs_f64());
                    result
                }
            };
            results.push(result);
        }
        block_result(results)
    }
}

impl TransportReceiver for KafkaTransport {
    type Token = KafkaToken;

    /// Receive a batch of messages.
    ///
    /// This is optimized for high-throughput batch processing:
    /// - Uses zero-timeout polls to drain librdkafka's internal queue
    /// - Returns up to `max` messages per call
    /// - Pre-populates topic cache to avoid allocations
    ///
    /// For PB/day workloads, call with `max = 10_000` or higher.
    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
        // Record-bounded poll only -- byte-identical to before. The byte-aware
        // governed path goes through `recv_limited`.
        self.recv_inner(max, None).await
    }

    /// Byte-aware receive (governed path): bound the poll by BOTH the record cap
    /// and `limits.max_bytes`. The drain stops once the recv-arena reaches
    /// `max_bytes` (floor: always take at least one record so an oversized
    /// record never stalls the loop), so each governed-recv arena is no larger
    /// than `max_bytes + one record`. This is what makes the self-regulation
    /// byte budget actually bound RECEIVE memory -- the whole-poll arena can no
    /// longer dwarf the budget before the driver's sub-block split runs.
    async fn recv_limited(
        &self,
        limits: super::traits::RecvLimits,
    ) -> TransportResult<WorkBatch<Self::Token>> {
        self.recv_inner(limits.max_records, Some(limits.max_bytes))
            .await
    }

    /// Commit offsets for processed messages.
    ///
    /// The commit is batched by partition -- only the HIGHEST offset per
    /// partition is committed (the offset list is built by
    /// `highest_offsets_per_partition`, which is unit-tested broker-free).
    ///
    /// ## Observable (synchronous) commit -- the ack barrier requires it
    ///
    /// This is the ENGINE-CRITICAL commit: the `BatchEngine` governed driver
    /// treats a commit failure as a TERMINAL ack-barrier error (it stops the run
    /// loop so no later block's ordered commit advances the watermark past
    /// un-acked offsets). For that to hold, a broker commit failure MUST be
    /// visible to the driver. rdkafka's `CommitMode::Async` returns `Ok` as soon
    /// as the request is ENQUEUED -- a broker-side rejection is never surfaced to
    /// the caller, so a fire-and-forget async commit would silently swallow the
    /// failure and DEFEAT the ack barrier. We therefore use the OBSERVABLE
    /// [`CommitMode::Sync`]: it blocks until the broker acks (or errors), so the
    /// driver sees the real outcome. The synchronous round-trip is the correct
    /// default for at-least-once; an app that wants the weaker
    /// throughput-over-correctness async commit can opt in explicitly via
    /// [`commit_weak_async`](Self::commit_weak_async) (NOT used by the governed
    /// driver -- it is documented as weaker).
    async fn commit(&self, tokens: &[Self::Token]) -> TransportResult<()> {
        if tokens.is_empty() {
            return Ok(());
        }

        let tpl = build_commit_tpl(tokens)?;

        // OBSERVABLE commit: block until the broker acks so a commit failure is
        // visible to the driver (the ack barrier depends on this).
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| TransportError::Commit(e.to_string()))?;

        Ok(())
    }
}

impl KafkaTransport {
    /// Shared poll + recv-arena body for [`recv`](TransportReceiver::recv) and
    /// [`recv_limited`](TransportReceiver::recv_limited).
    ///
    /// `max_msgs` bounds the poll by record count (as before). `max_bytes`, when
    /// `Some`, ADDITIONALLY stops the drain once the recv-arena has accumulated
    /// at least that many payload bytes -- with a FLOOR of one record so an
    /// oversized record is still returned (the loop never stalls). `None`
    /// (the bare `recv` path) is byte-identical to the pre-Phase-2 behaviour:
    /// no byte cap, record-bounded only.
    ///
    /// The recv-arena lifetime guarantee is UNCHANGED: every borrowed
    /// librdkafka payload is copied OUT into the growable `arena` inside its
    /// poll arm (never escapes the arm), the arena is frozen to ONE refcounted
    /// `Bytes` after the polls, and each `Message::payload` is a zero-copy slice
    /// into it. With a byte cap the arena is simply SMALLER -- bounded to
    /// `max_bytes + one record` -- not different in kind.
    #[allow(clippy::too_many_lines)]
    async fn recv_inner(
        &self,
        max_msgs: usize,
        max_bytes: Option<u64>,
    ) -> TransportResult<WorkBatch<KafkaToken>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TransportError::Closed);
        }

        // Inbound gate (governor feature, opt-in). Evaluate the gate so it
        // drives the actuator on pause/resume EDGES (pausing/resuming the
        // assigned partitions). We do NOT branch on the result: the poll below
        // ALWAYS runs. Paused partitions simply return no records, which keeps
        // the consumer-group heartbeat alive (no rebalance) while pressure
        // drains. Default `None` -> no call -> byte-identical to before.
        #[cfg(feature = "governor")]
        if let Some(ref gate) = self.inbound_gate {
            let _ = gate.evaluate();
            // Level-triggered re-pause while held. The edge actuator pauses the
            // assignment captured at the rising edge and never re-pauses while
            // latched; a cooperative rebalance during the hold (routine under
            // KEDA churn) then assigns NEW partitions UNPAUSED and ingest
            // silently reopens at full rate while pressure is still high,
            // defeating the brake. Re-applying pause to the CURRENT assignment
            // each recv while held is idempotent for already-paused partitions
            // and catches the newly assigned ones; the falling-edge resume()
            // resumes the current assignment, so nothing is stranded paused.
            // Off the per-record path -- once per recv, only while held.
            if gate.is_held()
                && let Ok(tpl) = self.consumer.assignment()
                && tpl.count() > 0
                && let Err(e) = self.consumer.pause(&tpl)
            {
                tracing::debug!(error = %e, "kafka gate: re-pause under hold failed");
            }
        }

        // Check for topic changes from the background refresh loop
        if let Some(ref refresh) = self.topic_refresh
            && let Some(new_topics) = refresh.lock().check_changed()
        {
            let topics: Vec<&str> = new_topics.iter().map(String::as_str).collect();
            match self.consumer.subscribe(&topics) {
                Ok(()) => {
                    tracing::info!(?new_topics, "Re-subscribed after topic refresh");
                    *self.subscribed_topics.write() = new_topics;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to re-subscribe after topic refresh");
                }
            }
        }

        let timeout = Duration::from_millis(tuning::POLL_TIMEOUT_MS);

        // DECISION (at-scale hardening 2.6): we KEEP synchronous
        // `BaseConsumer::poll` inside this async `recv` rather than switching
        // to `StreamConsumer`. Rationale:
        //   - librdkafka does the network fetch on its OWN background threads;
        //     `poll` just dequeues from an in-memory queue. The initial poll
        //     only blocks the worker (<= POLL_TIMEOUT_MS = 50ms) when the queue
        //     is EMPTY -- i.e. idle/low-traffic, when nothing else contends.
        //     Under load the poll returns immediately.
        //   - The drain loop uses ZERO-timeout polls bounded by MAX_DRAIN_MS
        //     (100ms); that is useful ingest work, not a block.
        //   - `StreamConsumer` would change commit/rebalance semantics on the
        //     critical ingress path -- real regression risk for an unmeasured
        //     latency benefit. So we MEASURE first (the poll-duration metric
        //     below); only escalate to spawn_blocking/StreamConsumer if it
        //     shows real Tokio-worker starvation. See the plan decision ledger.
        #[cfg(feature = "metrics")]
        let poll_start = std::time::Instant::now();

        // --- recv-arena ----------------------------------------------------
        // Instead of `payload.to_vec()` per message (N copies + N heap allocs),
        // we copy every record's payload ONCE into a single growable arena and
        // collect OWNED span metadata. After the polls we freeze the arena to
        // one refcounted `Bytes` and slice it -- so the whole batch shares ONE
        // allocation. See `build_batch_from_spans` and the per-arm safety note.
        let span_cap = max_msgs.min(tuning::INITIAL_BATCH_CAPACITY);
        let mut spans: Vec<Span> = Vec::with_capacity(span_cap);
        // Arena byte estimate: when a byte cap is set, size the up-front alloc
        // to the cap (plus a one-record cushion) so the governed arena is right-
        // sized; otherwise ~256 bytes/record (typical JSON event). It grows as
        // needed either way. One up-front alloc beats N small ones.
        let arena_hint = match max_bytes {
            Some(cap) => usize::try_from(cap)
                .unwrap_or(usize::MAX)
                .saturating_add(256),
            None => span_cap.saturating_mul(256),
        };
        let mut arena: Vec<u8> = Vec::with_capacity(arena_hint);
        let drain_deadline =
            std::time::Instant::now() + Duration::from_millis(tuning::MAX_DRAIN_MS);

        // Phase 1: Initial poll (drains librdkafka's queue; blocks <= timeout
        // only when the queue is empty).
        if let Some(result) = self.consumer.poll(timeout) {
            match result {
                Ok(msg) => {
                    // Extract W3C traceparent from Kafka headers (first message only,
                    // to associate the batch span with the upstream trace)
                    #[cfg(feature = "transport-trace")]
                    if let Some(headers) = msg.headers() {
                        use rdkafka::message::Headers;
                        for idx in 0..headers.count() {
                            if let Some(Ok(header)) = headers.try_get_as::<[u8]>(idx)
                                && header.key == super::propagation::TRACEPARENT_HEADER
                            {
                                if let Some(value) = header.value
                                    && let Ok(tp) = std::str::from_utf8(value)
                                    && super::propagation::is_valid_traceparent(tp)
                                {
                                    tracing::Span::current().record("traceparent", tp);
                                }
                                break;
                            }
                        }
                    }

                    let topic_str = msg.topic();
                    let topic: Arc<str> = get_or_insert_topic(&self.topic_cache, topic_str);
                    // SAFETY (lifetime, not unsafe): `msg.payload()` borrows
                    // librdkafka's internal buffer and is valid ONLY while this
                    // `BorrowedMessage` lives (until the next poll / its drop).
                    // We copy it OUT into the arena RIGHT HERE -- this is the
                    // one unavoidable copy out of the borrowed buffer -- and we
                    // never store the `&[u8]` or `msg` past this arm.
                    let start = arena.len();
                    arena.extend_from_slice(msg.payload().unwrap_or(&[]));
                    let end = arena.len();
                    // Extract OWNED metadata before `msg` drops at arm end.
                    let partition = msg.partition();
                    let offset = msg.offset();
                    let timestamp_ms = msg.timestamp().to_millis();

                    spans.push(Span {
                        key: Some(topic.clone()),
                        token: KafkaToken::new(topic, partition, offset),
                        timestamp_ms,
                        format: PayloadFormat::Auto,
                        range: start..end,
                    });
                }
                Err(e) => {
                    return Err(TransportError::Recv(e.to_string()));
                }
            }
        } else {
            #[cfg(feature = "metrics")]
            ::metrics::histogram!("kafka_poll_duration_seconds")
                .record(poll_start.elapsed().as_secs_f64());
            // No message available -- empty arena, empty spans, empty batch.
            return Ok(RecvBatch::from_messages(build_batch_from_spans(
                bytes::Bytes::new(),
                spans,
            ))
            .into());
        }

        // Phase 2: drain the queue with zero-timeout polls. librdkafka has
        // already fetched a batch from the network; we just drain it fast.
        //
        // BYTE-AWARE STOP: when `max_bytes` is set, stop
        // draining once the arena has reached the cap. Phase 1 already took one
        // record (the floor), so a single oversized record is always returned
        // and the loop never stalls; the arena is bounded to
        // `max_bytes + one record`. Without a cap (`None`) this check is skipped
        // and the drain is record-bounded exactly as before.
        while spans.len() < max_msgs {
            if std::time::Instant::now() >= drain_deadline {
                break;
            }
            if arena_byte_limit_reached(arena.len(), spans.len(), max_bytes) {
                // Arena hit the byte budget -- stop the governed poll here so the
                // whole-poll arena cannot dwarf the budget. Already drained >= 1
                // record (floor), so this never stalls.
                break;
            }

            match self.consumer.poll(Duration::ZERO) {
                Some(Ok(msg)) => {
                    let topic_str = msg.topic();
                    let topic: Arc<str> = get_or_insert_topic(&self.topic_cache, topic_str);
                    // Same lifetime contract as Phase 1: copy the borrowed
                    // payload into the arena HERE, extract owned metadata HERE,
                    // never let `msg`/`&[u8]` escape this arm.
                    let start = arena.len();
                    arena.extend_from_slice(msg.payload().unwrap_or(&[]));
                    let end = arena.len();
                    let partition = msg.partition();
                    let offset = msg.offset();
                    let timestamp_ms = msg.timestamp().to_millis();

                    spans.push(Span {
                        key: Some(topic.clone()),
                        token: KafkaToken::new(topic, partition, offset),
                        timestamp_ms,
                        format: PayloadFormat::Auto,
                        range: start..end,
                    });
                }
                Some(Err(e)) => {
                    if spans.is_empty() {
                        return Err(TransportError::Recv(e.to_string()));
                    }
                    break;
                }
                None => break,
            }
        }

        // Freeze the arena to ONE refcounted Bytes, then rebuild messages as
        // zero-copy slices into it. All borrowed Kafka buffers are long gone --
        // every byte we keep was copied into `arena` inside a poll arm above.
        let arena: bytes::Bytes = bytes::Bytes::from(arena);
        let messages = build_batch_from_spans(arena, spans);

        // Apply inbound filters via the shared partition helper; DLQ entries
        // are returned in the RecvBatch for the caller to route onward.
        let batch = self.filter_engine.partition_batch(
            messages,
            |m| m.payload.as_ref(),
            |m| m.key.clone(),
            |m| m.token.clone(),
        );
        let messages = batch.messages;
        let dlq_entries = batch.dlq_entries;
        let filtered_tokens = batch.filtered_tokens;

        // Transport-level ingress (raw wire receipt, post-filter). Batch-at-a-time,
        // distinct from the pipeline-level records_received_total (post-decode).
        #[cfg(feature = "metrics")]
        if !messages.is_empty() {
            let bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
            ::metrics::counter!("transport_received_bytes_total", "transport" => "kafka")
                .increment(bytes as u64);
            ::metrics::counter!("transport_received_events_total", "transport" => "kafka")
                .increment(messages.len() as u64);
        }

        Ok(RecvBatch {
            messages,
            dlq_entries,
            filtered_tokens,
        }
        .into())
    }

    /// WEAKER, opt-in fire-and-forget commit (throughput over correctness).
    ///
    /// Uses rdkafka's `CommitMode::Async`, which returns once the commit request
    /// is ENQUEUED -- it does NOT wait for the broker and does NOT surface a
    /// broker-side commit failure. This is strictly WEAKER than the default
    /// observable [`commit`](TransportReceiver::commit): a swallowed commit
    /// failure breaks the ack barrier and can silently weaken at-least-once.
    ///
    /// The `BatchEngine` governed driver does NOT use this. Reach for it only
    /// when a consumer has independently accepted the weaker guarantee (e.g. an
    /// at-most-once / best-effort pipeline) and wants the throughput.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Commit`] only if the offset list cannot be built
    /// or the request cannot be ENQUEUED -- never on a broker-side commit
    /// rejection (which is invisible to async commit by construction).
    pub fn commit_weak_async(&self, tokens: &[KafkaToken]) -> TransportResult<()> {
        if tokens.is_empty() {
            return Ok(());
        }
        let tpl = build_commit_tpl(tokens)?;
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| TransportError::Commit(e.to_string()))?;
        Ok(())
    }
}

/// Highest offset per `(topic, partition)` across `tokens`.
///
/// Kafka commit is CUMULATIVE -- "commit up to offset N" -- so per partition we
/// keep only the highest seen offset. The committed TPL then stores
/// `highest + 1` (the next offset to be read). Extracted as a free function so
/// the correctness-critical fold is unit-testable WITHOUT a live broker.
fn highest_offsets_per_partition(tokens: &[KafkaToken]) -> HashMap<(Arc<str>, i32), i64> {
    let mut partition_offsets: HashMap<(Arc<str>, i32), i64> =
        HashMap::with_capacity(tokens.len().min(1024));
    for token in tokens {
        let key = (Arc::clone(&token.topic), token.partition);
        partition_offsets
            .entry(key)
            .and_modify(|current| {
                if token.offset > *current {
                    *current = token.offset;
                }
            })
            .or_insert(token.offset);
    }
    partition_offsets
}

/// Build the commit [`TopicPartitionList`] from `tokens`: highest offset per
/// partition, stored as `highest + 1` (next-to-read). Shared by the observable
/// and weak-async commit paths.
fn build_commit_tpl(tokens: &[KafkaToken]) -> TransportResult<TopicPartitionList> {
    let mut tpl = TopicPartitionList::new();
    for ((topic, partition), offset) in highest_offsets_per_partition(tokens) {
        tpl.add_partition_offset(topic.as_ref(), partition, Offset::Offset(offset + 1))
            .map_err(|e| TransportError::Commit(format!("Failed to build TPL: {e}")))?;
    }
    Ok(tpl)
}

/// PURE byte-budget stop decision for the recv-arena drain loop.
///
/// Returns `true` when the governed poll should STOP draining because the
/// recv-arena has reached its byte budget:
///
/// - `max_bytes == None` -> never stop on bytes (the bare `recv` path is
///   record-bounded only, byte-identical to before);
/// - `span_count == 0` -> never stop (FLOOR one record: the arena must hold at
///   least one record before a byte cap can close the poll, so an oversized
///   record is still returned and the loop never stalls);
/// - otherwise stop once `arena_len >= max_bytes`.
///
/// Free-standing + side-effect-free so the correctness-critical stop condition
/// is unit-testable WITHOUT a live broker (the rest of the drain is a librdkafka
/// poll loop).
fn arena_byte_limit_reached(arena_len: usize, span_count: usize, max_bytes: Option<u64>) -> bool {
    match max_bytes {
        Some(cap) => span_count > 0 && arena_len as u64 >= cap,
        None => false,
    }
}

/// One record's worth of OWNED metadata collected during a poll, plus the
/// byte range of its payload inside the recv-arena.
///
/// Crucially this carries NO borrowed data: the `BorrowedMessage` and the
/// `&[u8]` payload it lends are valid only until the next poll / until that
/// message drops, so every field here is owned (`Arc<str>`, `i64`, indices).
/// The payload itself has already been copied into the shared arena; `range`
/// is where. See `recv()` for the poll-arm safety argument.
struct Span {
    /// Routing key (interned topic Arc), mirrors `Message::key`.
    key: Option<Arc<str>>,
    /// Commit token (topic/partition/offset), owned.
    token: KafkaToken,
    /// Transport timestamp in millis since epoch, if present.
    timestamp_ms: Option<i64>,
    /// Detected payload format (matches the legacy per-message default).
    format: PayloadFormat,
    /// Half-open byte range of this record's payload within the frozen arena.
    range: core::ops::Range<usize>,
}

/// Rebuild a batch of `Message`s from a frozen recv-arena and its spans.
///
/// The arena is ONE `Bytes` holding every record's payload back-to-back; each
/// span's `range` indexes into it. We build each `Message::payload` via
/// `arena.slice(range)` -- a refcount bump into the shared backing buffer, NOT
/// a per-record copy. So the whole batch shares ONE allocation and frees once
/// when the last record drops (the "whole batch shares one allocation"
/// contract).
///
/// This is a free function so the correctness-critical assembly is unit
/// testable WITHOUT a live Kafka broker.
fn build_batch_from_spans(arena: bytes::Bytes, spans: Vec<Span>) -> Vec<Message<KafkaToken>> {
    spans
        .into_iter()
        .map(|span| Message {
            key: span.key,
            // Zero-copy slice into the shared arena (refcount bump only).
            payload: arena.slice(span.range),
            token: span.token,
            timestamp_ms: span.timestamp_ms,
            format: span.format,
        })
        .collect()
}

/// Get or insert topic Arc into cache.
///
/// Inline helper for hot path - avoids method call overhead.
#[inline]
fn get_or_insert_topic(
    cache: &parking_lot::RwLock<HashMap<String, Arc<str>>>,
    topic: &str,
) -> Arc<str> {
    // Fast path: shared read lock, hit on the common case (topic seen before).
    if let Some(arc) = cache.read().get(topic) {
        return arc.clone();
    }
    // First sight: take the write lock and intern. A benign race (two callers
    // miss and both insert) just converges on equivalent Arcs -- harmless.
    let arc: Arc<str> = Arc::from(topic);
    cache.write().insert(topic.to_string(), arc.clone());
    arc
}

// --- Inbound gate actuator (governor feature) -------------------------------

/// A [`GateActuator`](crate::governor::GateActuator) that pauses/resumes a
/// shared Kafka consumer's ASSIGNED partitions.
///
/// Holds an `Arc` clone of the transport's consumer (see
/// [`KafkaTransport::gate_actuator`]). On the rising edge it reads the live
/// assignment and pauses exactly those partitions; on the falling edge it
/// resumes them. Pausing the assignment (not unsubscribing) keeps the member
/// in the group, so no rebalance fires while held. Pause/resume failures are
/// logged but never panic -- the gate's edge bookkeeping has already advanced,
/// and a missed pause degrades to "kept ingesting", never a deadlock.
#[cfg(feature = "governor")]
struct KafkaGateActuator {
    consumer: Arc<BaseConsumer<StatsContext>>,
}

#[cfg(feature = "governor")]
impl crate::governor::GateActuator for KafkaGateActuator {
    fn pause(&self) {
        match self.consumer.assignment() {
            Ok(tpl) => {
                if let Err(e) = self.consumer.pause(&tpl) {
                    tracing::warn!(error = %e, "kafka gate: pause(assignment) failed");
                    gate_actuator_error("pause");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "kafka gate: assignment() failed on pause");
                gate_actuator_error("pause");
            }
        }
    }

    fn resume(&self) {
        match self.consumer.assignment() {
            Ok(tpl) => {
                if let Err(e) = self.consumer.resume(&tpl) {
                    tracing::warn!(error = %e, "kafka gate: resume(assignment) failed");
                    gate_actuator_error("resume");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "kafka gate: assignment() failed on resume");
                gate_actuator_error("resume");
            }
        }
    }
}

/// Count a kafka gate pause/resume failure. A sustained failure silently
/// disables the governor's brake for the Kafka source, so it must be visible
/// (not just a log line) -- alert on a non-zero rate.
#[cfg(feature = "governor")]
fn gate_actuator_error(op: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("self_regulation_kafka_gate_errors_total", "op" => op).increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = op;
}

// --- kafka_partition_limited diagnostic -------------------------------------

/// PURE decision for the `kafka_partition_limited` diagnostic.
///
/// A consumer group is "partition limited" when it has at least as many member
/// consumers as the topic has partitions AND there is still backlog: extra
/// members sit idle (Kafka assigns at most one consumer per partition) yet lag
/// persists, so adding consumers cannot help -- the topic needs more
/// partitions. This is a DIAGNOSTIC only: it never mutates topology.
///
/// Truth table:
/// - `members >= partitions && lag > 0` -> `true`  (over-provisioned + backlog)
/// - `members <  partitions`            -> `false` (headroom to scale out)
/// - `lag == 0`                         -> `false` (no backlog, not limited)
/// - `partitions == 0`                  -> `false` (no topic info / not limited)
///
/// Kept free-standing and side-effect-free so it is unit-testable without a
/// live broker.
#[cfg(feature = "governor")]
#[must_use]
pub fn partition_limited(members: usize, partitions: usize, lag: u64) -> bool {
    partitions > 0 && members >= partitions && lag > 0
}

/// Time-windowed dedup latch for the `kafka_partition_limited` warning.
///
/// The kafka `SuppressionRule` in this crate is a topic-suffix suppressor
/// (auto-discovery), NOT a rate-limiter -- so the once-per-window dedup is a
/// small purpose-built latch here. [`should_warn`](Self::should_warn) returns
/// `true` at most once per `cooldown`, so a persistently partition-limited
/// consumer logs once per window rather than every recv.
#[cfg(feature = "governor")]
struct PartitionLimitedDiagnostic {
    last_warn: parking_lot::Mutex<Option<std::time::Instant>>,
    cooldown: Duration,
}

#[cfg(feature = "governor")]
impl Default for PartitionLimitedDiagnostic {
    fn default() -> Self {
        Self {
            last_warn: parking_lot::Mutex::new(None),
            // 5 minutes: long enough to avoid spam, short enough to re-surface
            // a persistent condition in logs/alerts. (from_secs over the
            // unstable from_mins; allow the readability lint.)
            #[allow(clippy::duration_suboptimal_units)]
            cooldown: Duration::from_secs(300),
        }
    }
}

#[cfg(feature = "governor")]
impl PartitionLimitedDiagnostic {
    /// Whether to emit the warning now, given the current monotonic time.
    ///
    /// Returns `true` on the first call and then at most once per `cooldown`.
    /// `now` is injected so the dedup window is unit-testable without sleeping.
    fn should_warn_at(&self, now: std::time::Instant) -> bool {
        let mut last = self.last_warn.lock();
        match *last {
            Some(prev) if now.duration_since(prev) < self.cooldown => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }
}

impl std::fmt::Debug for KafkaTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaTransport")
            .field("subscribed_topics", &*self.subscribed_topics.read())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("healthy", &self.healthy.load(Ordering::Relaxed))
            .field("topic_refresh_active", &self.topic_refresh.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A producer-only transport's consumer still asks for its group's
    /// coordinator, so the stand-in must sit under the prefix a DFE broker
    /// grants (`dfe-`), and must never be empty (librdkafka >= 2.x refuses).
    #[test]
    fn producer_only_stand_in_group_derives_from_the_client_id() {
        let producer = KafkaConfig {
            group: String::new(),
            client_id: "dfe-fetcher".to_string(),
            ..Default::default()
        };
        assert_eq!(
            effective_consumer_group_id(&producer),
            "dfe-fetcher-producer-only"
        );
        let built = consumer_client_config(&producer, ConsumerProtocol::Classic);
        assert_eq!(built.get("group.id"), Some("dfe-fetcher-producer-only"));

        // A real group is passed through unchanged.
        let consumer = KafkaConfig {
            group: "dfe-loader".to_string(),
            ..Default::default()
        };
        assert_eq!(effective_consumer_group_id(&consumer), "dfe-loader");
    }

    /// Topics on a producer-only config name where it sends, not what it reads:
    /// subscribing would join the stand-in group and leave an idle member there.
    /// Broker-free: construction connects lazily and nothing is polled.
    #[tokio::test]
    async fn producer_only_transport_subscribes_to_nothing() {
        let producer = KafkaConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            group: String::new(),
            client_id: "dfe-transform-vrl-producer-main".to_string(),
            topics: vec!["syslog_load".to_string()],
            ..Default::default()
        };
        let transport = KafkaTransport::new(&producer)
            .await
            .expect("a producer-only transport constructs broker-free");
        let subscription = transport
            .consumer
            .subscription()
            .expect("librdkafka reports the subscription");
        assert_eq!(
            subscription.count(),
            0,
            "a producer-only transport joined a group"
        );
        assert!(transport.subscribed_topics.read().is_empty());
    }

    /// The control for the test above: the same topics with a group set DO
    /// subscribe, so the empty result there is the group check, not a broken
    /// constructor.
    #[tokio::test]
    async fn consumer_transport_subscribes_to_its_topics() {
        let consumer = KafkaConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            group: "dfe-loader".to_string(),
            topics: vec!["syslog_load".to_string()],
            // No probe: nothing here can reach a broker to report the group up.
            consumer_protocol_probe_ms: 0,
            ..Default::default()
        };
        let transport = KafkaTransport::new(&consumer)
            .await
            .expect("a consumer transport constructs broker-free");
        let subscription = transport
            .consumer
            .subscription()
            .expect("librdkafka reports the subscription");
        let topics: Vec<String> = subscription
            .elements()
            .iter()
            .map(|e| e.topic().to_string())
            .collect();
        assert_eq!(topics, vec!["syslog_load".to_string()]);
    }

    // =========================================================================
    // Consumer group protocol (KIP-848)
    // =========================================================================

    /// librdkafka rejects the whole client if a classic-only property was set
    /// alongside `group.protocol=consumer`, so the built config must carry
    /// none of them -- including any an operator put there by hand.
    #[test]
    fn consumer_protocol_config_drops_every_classic_only_key() {
        let config = KafkaConfig {
            group: "loader".to_string(),
            topics: vec!["events".to_string()],
            librdkafka_overrides: HashMap::from([(
                "partition.assignment.strategy".to_string(),
                "range".to_string(),
            )]),
            ..Default::default()
        };
        let built = consumer_client_config(&config, ConsumerProtocol::Consumer);
        assert_eq!(built.get("group.protocol"), Some("consumer"));
        for key in CLASSIC_ONLY_CONSUMER_KEYS {
            assert_eq!(
                built.get(key),
                None,
                "{key} alongside group.protocol=consumer fails client creation"
            );
        }
        // The rest of the surface is untouched.
        assert_eq!(built.get("group.id"), Some("loader"));
        assert_eq!(built.get("enable.auto.commit"), Some("false"));
        assert!(built.get("max.poll.interval.ms").is_some());
    }

    /// Under classic the same properties are exactly what the protocol runs
    /// on, so nothing is stripped.
    #[test]
    fn classic_protocol_config_keeps_the_classic_keys() {
        let config = KafkaConfig {
            group: "loader".to_string(),
            ..Default::default()
        };
        let built = consumer_client_config(&config, ConsumerProtocol::Classic);
        assert_eq!(built.get("group.protocol"), Some("classic"));
        assert_eq!(
            built.get("session.timeout.ms"),
            Some(config.session_timeout_ms.to_string().as_str())
        );
        assert_eq!(
            built.get("heartbeat.interval.ms"),
            Some(config.heartbeat_interval_ms.to_string().as_str())
        );
        assert_eq!(
            built.get("partition.assignment.strategy"),
            Some("cooperative-sticky")
        );
    }

    /// The broker refusing the protocol is a FATAL error carrying one of the
    /// unsupported codes -- that, and only that, earns the rebuild as classic.
    #[test]
    fn protocol_refusal_falls_back_and_other_fatals_do_not() {
        for code in PROTOCOL_REFUSAL_CODES {
            let verdict = classify_probe_tick(
                Some((*code, "ConsumerGroupHeartbeat fatal error".to_string())),
                None,
                false,
            );
            assert_eq!(
                verdict,
                Some(ProtocolProbe::FallBack(
                    "ConsumerGroupHeartbeat fatal error".to_string()
                )),
                "{code:?} means the broker will not speak KIP-848"
            );
        }

        // An unrelated fatal is not the protocol's fault, and classic would
        // not clear it -- do not downgrade over it.
        let verdict = classify_probe_tick(
            Some((
                RDKafkaErrorCode::GroupAuthorizationFailed,
                "not authorized".to_string(),
            )),
            None,
            false,
        );
        assert_eq!(verdict, Some(ProtocolProbe::Keep));
    }

    /// A group that reaches `up` proves the broker took the protocol, and it
    /// is the early exit that keeps the probe off the startup critical path.
    #[test]
    fn a_joined_group_ends_the_probe_immediately() {
        assert_eq!(
            classify_probe_tick(None, Some("up"), false),
            Some(ProtocolProbe::Keep)
        );
        // Anything short of `up` keeps waiting.
        for state in ["init", "query-coord", "wait-coord", "wait-broker"] {
            assert_eq!(
                classify_probe_tick(None, Some(state), false),
                None,
                "state {state} is still mid-join"
            );
        }
        assert_eq!(classify_probe_tick(None, None, false), None);
    }

    /// A broker that never answers the heartbeat marks its coordinator dead
    /// and re-queries in silence, so the expired window is the only signal
    /// that case ever produces.
    #[test]
    fn an_expired_window_falls_back() {
        let verdict = classify_probe_tick(None, Some("query-coord"), true);
        assert!(matches!(verdict, Some(ProtocolProbe::FallBack(_))));
        // A join that landed on the last tick still wins over the deadline.
        assert_eq!(
            classify_probe_tick(None, Some("up"), true),
            Some(ProtocolProbe::Keep)
        );
    }

    /// The probe reads the group state out of librdkafka's statistics, so it
    /// must not run when they are switched off -- it could only time out and
    /// downgrade a healthy consumer.
    #[test]
    fn the_probe_is_skipped_when_it_could_only_time_out() {
        let config = KafkaConfig::default();
        let built = consumer_client_config(&config, ConsumerProtocol::Consumer);
        assert!(
            protocol_probe_window(&config, ConsumerProtocol::Consumer, &built).is_some(),
            "the shipped profiles enable statistics, so the probe runs"
        );
        assert!(
            protocol_probe_window(&config, ConsumerProtocol::Classic, &built).is_none(),
            "classic has nothing to negotiate"
        );

        let disabled = KafkaConfig {
            consumer_protocol_probe_ms: 0,
            ..Default::default()
        };
        assert!(protocol_probe_window(&disabled, ConsumerProtocol::Consumer, &built).is_none());

        let no_stats = KafkaConfig {
            librdkafka_overrides: HashMap::from([(
                "statistics.interval.ms".to_string(),
                "0".to_string(),
            )]),
            ..Default::default()
        };
        let built = consumer_client_config(&no_stats, ConsumerProtocol::Consumer);
        assert!(protocol_probe_window(&no_stats, ConsumerProtocol::Consumer, &built).is_none());
    }

    /// An oversize record is a poison record, not a transport failure: it is
    /// dead-lettered, so the block is neither retried nor spooled.
    #[test]
    fn oversize_record_is_dead_lettered_not_retried() {
        use rdkafka::error::KafkaError;

        assert_eq!(
            classify::classify_send_failure(&KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageSizeTooLarge
            )),
            classify::SendFailure::TooLarge
        );
        assert_eq!(
            classify::classify_send_failure(&KafkaError::MessageProduction(
                RDKafkaErrorCode::QueueFull
            )),
            classify::SendFailure::QueueFull
        );
        // A missing topic stays fatal: retrying past it hides a misconfigured
        // destination behind the retry loop.
        assert_eq!(
            classify::classify_send_failure(&KafkaError::MessageProduction(
                RDKafkaErrorCode::UnknownTopicOrPartition
            )),
            classify::SendFailure::Fatal
        );
    }

    /// The error the oversize path reports is permanent: a caller branching on
    /// `is_recoverable` must not retry it, and one branching on `is_fatal` must
    /// not tear the transport down over a single bad record.
    #[test]
    fn message_too_large_is_undeliverable_not_recoverable() {
        let err = TransportError::MessageTooLarge {
            bytes: 20_000_000,
            detail: "Broker: Message size too large".to_string(),
        };
        assert!(err.is_undeliverable());
        assert!(!err.is_recoverable());
        assert!(!err.is_fatal());
        assert!(
            err.to_string().contains("20000000"),
            "the refused size belongs in the message: {err}"
        );
        assert!(!TransportError::Send("boom".into()).is_undeliverable());
    }

    // ---- send_batch: block result and broker-free failure paths -------------

    fn batch_record(payload: &'static [u8]) -> Record {
        Record {
            payload: bytes::Bytes::from_static(payload),
            key: Some(Arc::from("events.load")),
            headers: Vec::new(),
            metadata: crate::transport::RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    /// A producer-only transport pointed at a port nothing listens on, so no
    /// record it queues can ever be confirmed.
    fn unreachable_config() -> KafkaConfig {
        KafkaConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            group: String::new(),
            ..Default::default()
        }
    }

    #[test]
    fn block_result_is_ok_only_when_every_record_is_handled() {
        assert!(block_result(Vec::new()).is_ok(), "an empty block is Ok");
        assert!(
            block_result(vec![
                SendResult::Ok,
                SendResult::FilteredDlq,
                SendResult::Ok
            ])
            .is_ok(),
            "sent, dropped and dead-lettered records are all handled"
        );
        // One unconfirmed record among a thousand confirmed ones fails the
        // block, so the caller retries it rather than committing past it.
        let mut results: Vec<SendResult> = (0..999).map(|_| SendResult::Ok).collect();
        results.push(SendResult::Backpressured);
        assert!(block_result(results).is_backpressured());
    }

    #[test]
    fn block_result_reports_the_first_failure_in_record_order() {
        let first_backpressured = block_result(vec![
            SendResult::Ok,
            SendResult::Backpressured,
            SendResult::Fatal(TransportError::Closed),
        ]);
        assert!(first_backpressured.is_backpressured());

        let first_fatal = block_result(vec![
            SendResult::FilteredDlq,
            SendResult::Fatal(TransportError::Closed),
            SendResult::Backpressured,
        ]);
        assert!(first_fatal.is_fatal());
    }

    #[tokio::test]
    async fn send_batch_of_nothing_is_ok_and_after_close_is_fatal() {
        let transport = KafkaTransport::new(&unreachable_config())
            .await
            .expect("a producer-only transport constructs broker-free");
        assert!(transport.send_batch(&[]).await.is_ok());

        transport.close().await.expect("close");
        let result = transport.send_batch(&[batch_record(b"{\"a\":1}")]).await;
        assert!(
            matches!(result, SendResult::Fatal(TransportError::Closed)),
            "a closed transport must refuse the block, got {result:?}"
        );
    }

    /// Filtered records never reach the producer. With no broker, a queued
    /// record could not resolve inside the deadline, so an `Ok` in time proves
    /// every record was filtered before it was offered.
    #[tokio::test]
    async fn send_batch_filters_every_record_before_queueing_it() {
        let config = KafkaConfig {
            filters_out: vec![
                crate::transport::filter::FilterRule {
                    expression: "has(drop_me)".to_string(),
                    action: crate::transport::filter::FilterAction::Drop,
                },
                crate::transport::filter::FilterRule {
                    expression: "has(dead)".to_string(),
                    action: crate::transport::filter::FilterAction::Dlq,
                },
            ],
            ..unreachable_config()
        };
        let transport = KafkaTransport::new(&config).await.expect("construct");
        let records = [
            batch_record(b"{\"drop_me\":1}"),
            batch_record(b"{\"dead\":1}"),
            batch_record(b"{\"drop_me\":2}"),
        ];
        let result = tokio::time::timeout(Duration::from_secs(5), transport.send_batch(&records))
            .await
            .expect("a filtered record was queued and waited on a broker that does not exist");
        assert!(
            result.is_ok(),
            "filtered records are handled, got {result:?}"
        );
    }

    /// A record the broker never confirms fails the block, and the filtered
    /// records beside it cannot mask that.
    #[tokio::test]
    async fn send_batch_is_not_ok_when_a_delivery_fails() {
        let config = KafkaConfig {
            filters_out: vec![crate::transport::filter::FilterRule {
                expression: "has(drop_me)".to_string(),
                action: crate::transport::filter::FilterAction::Drop,
            }],
            ..unreachable_config()
        }
        .with_override("message.timeout.ms", "1000");
        let transport = KafkaTransport::new(&config).await.expect("construct");
        let records = [
            batch_record(b"{\"drop_me\":1}"),
            batch_record(b"{\"a\":1}"),
            batch_record(b"{\"drop_me\":2}"),
            batch_record(b"{\"a\":2}"),
        ];
        let result = tokio::time::timeout(Duration::from_secs(20), transport.send_batch(&records))
            .await
            .expect("the delivery reports must arrive once message.timeout.ms expires");
        assert!(
            result.is_backpressured(),
            "an unconfirmed record must fail the block as retryable, got {result:?}"
        );
    }

    #[test]
    fn test_tuning_constants() {
        assert_eq!(tuning::DEFAULT_BATCH_SIZE, 10_000);
        assert_eq!(tuning::MAX_DRAIN_MS, 100);
        assert_eq!(tuning::POLL_TIMEOUT_MS, 50);
    }

    #[test]
    fn test_get_or_insert_topic_cached() {
        let mut map = HashMap::new();
        map.insert("events".to_string(), Arc::from("events"));
        let cache = parking_lot::RwLock::new(map);

        let arc1 = get_or_insert_topic(&cache, "events");
        let arc2 = get_or_insert_topic(&cache, "events");

        // Should return same Arc (pointer equality)
        assert!(Arc::ptr_eq(&arc1, &arc2));
    }

    #[test]
    fn test_get_or_insert_topic_new() {
        let cache = parking_lot::RwLock::new(HashMap::new());

        let arc = get_or_insert_topic(&cache, "new-topic");
        assert_eq!(&*arc, "new-topic");
        assert!(cache.read().contains_key("new-topic"));
        // Insert persists across calls (the previous per-recv clone lost it).
        let arc2 = get_or_insert_topic(&cache, "new-topic");
        assert!(Arc::ptr_eq(&arc, &arc2));
    }

    #[test]
    fn test_kafka_config_defaults() {
        let config = KafkaConfig::default();
        assert_eq!(config.fetch_max_bytes, 52_428_800); // 50MB
        assert!(!config.enable_auto_commit); // Manual commit
    }

    #[tokio::test]
    async fn test_topic_refresh_check_changed_detects_updates() {
        // Simulate the watch channel that TopicRefreshHandle uses internally
        let (tx, rx) = tokio::sync::watch::channel(vec!["events_load".to_string()]);

        let mut handle = topic_resolver::TopicRefreshHandle::new_for_test(rx);

        // Initially no change (first check sees initial value as "no change")
        assert!(handle.check_changed().is_none());

        tx.send(vec!["events_load".to_string(), "logs_load".to_string()])
            .unwrap();

        // A pending update yields the new list.
        let changed = handle.check_changed();
        assert!(changed.is_some());
        let topics = changed.unwrap();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"logs_load".to_string()));

        // No further change -> None.
        assert!(handle.check_changed().is_none());
    }

    // --- recv-arena: build_batch_from_spans -------------------------------
    //
    // These de-risk the recv-arena WITHOUT a live broker: they prove the free
    // function rebuilds messages as zero-copy slices into one shared arena.

    /// Assert that `slice` is a zero-copy view INTO `blob` (a refcounted slice,
    /// not a fresh allocation): its byte range must fall within `blob`'s range.
    /// Mirrors the helper in `work_batch.rs` tests.
    fn assert_within(slice: &bytes::Bytes, blob: &bytes::Bytes) {
        let blob_start = blob.as_ptr() as usize;
        let blob_end = blob_start + blob.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        assert!(
            slice_start >= blob_start && slice_end <= blob_end,
            "slice [{slice_start:#x}, {slice_end:#x}) is not within arena \
             [{blob_start:#x}, {blob_end:#x}) -- it is a copy, not a view"
        );
    }

    /// Build an arena + spans by appending payloads back-to-back, exactly as
    /// the poll arms do, returning the frozen arena and the spans.
    fn arena_with(payloads: &[&[u8]]) -> (bytes::Bytes, Vec<Span>) {
        let mut arena: Vec<u8> = Vec::new();
        let mut spans: Vec<Span> = Vec::new();
        for (i, p) in payloads.iter().enumerate() {
            let start = arena.len();
            arena.extend_from_slice(p);
            let end = arena.len();
            let offset = i64::try_from(i).expect("test index fits i64");
            spans.push(Span {
                key: Some(Arc::from("events")),
                token: KafkaToken::new(Arc::from("events"), 0, offset),
                timestamp_ms: Some(1_000 + offset),
                format: PayloadFormat::Auto,
                range: start..end,
            });
        }
        (bytes::Bytes::from(arena), spans)
    }

    #[test]
    fn build_batch_payloads_match_and_in_order() {
        let payloads: &[&[u8]] = &[b"{\"a\":1}", b"hello world", b"[1,2,3]"];
        let (arena, spans) = arena_with(payloads);
        let msgs = build_batch_from_spans(arena, spans);

        assert_eq!(msgs.len(), 3);
        for (i, expected) in payloads.iter().enumerate() {
            assert_eq!(msgs[i].payload.as_ref(), *expected, "payload {i} mismatch");
            // Metadata carried through the span, in order.
            let offset = i64::try_from(i).expect("test index fits i64");
            assert_eq!(msgs[i].token.offset, offset);
            assert_eq!(msgs[i].timestamp_ms, Some(1_000 + offset));
        }
    }

    #[test]
    fn build_batch_payloads_are_views_into_shared_arena() {
        let payloads: &[&[u8]] = &[b"first-record", b"second", b"third-payload-xyz"];
        let (arena, spans) = arena_with(payloads);
        // Keep a clone of the arena Bytes to compare pointer ranges against.
        let arena_ref = arena.clone();
        let msgs = build_batch_from_spans(arena, spans);

        // (b) every payload pointer lies WITHIN the arena -- zero-copy slicing,
        // not copies. This is the core recv-arena contract.
        for m in &msgs {
            assert_within(&m.payload, &arena_ref);
        }
        // All records share the SAME backing allocation (one arena).
        let base = arena_ref.as_ptr() as usize;
        for m in &msgs {
            let off = m.payload.as_ptr() as usize - base;
            assert!(off < arena_ref.len() || m.payload.is_empty());
        }
    }

    #[test]
    fn build_batch_empty_payload_span_yields_empty_slice() {
        // (c) a record with an empty payload (start == end) -> empty slice.
        let payloads: &[&[u8]] = &[b"before", b"", b"after"];
        let (arena, spans) = arena_with(payloads);
        let msgs = build_batch_from_spans(arena, spans);

        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0].payload.as_ref(), b"before");
        assert!(
            msgs[1].payload.is_empty(),
            "empty span must yield empty slice"
        );
        assert_eq!(msgs[2].payload.as_ref(), b"after");
    }

    #[test]
    fn build_batch_no_spans_yields_empty_batch() {
        // (d) empty spans -> empty batch (the empty-poll early-return path).
        let msgs = build_batch_from_spans(bytes::Bytes::new(), Vec::new());
        assert!(msgs.is_empty());
    }

    #[test]
    fn build_batch_preserves_key_token_format() {
        let (arena, mut spans) = arena_with(&[b"{\"k\":1}"]);
        // Override the single span's format to assert it is carried through.
        spans[0].format = PayloadFormat::Json;
        let msgs = build_batch_from_spans(arena, spans);
        assert_eq!(msgs[0].key.as_deref(), Some("events"));
        assert_eq!(msgs[0].token.topic.as_ref(), "events");
        assert_eq!(msgs[0].format, PayloadFormat::Json);
    }

    // --- Byte-aware recv-arena stop (broker-free) -------------------------
    //
    // The drain loop itself is a librdkafka poll loop (needs a broker), but the
    // byte-budget STOP decision is a pure predicate extracted as
    // `arena_byte_limit_reached`. These prove it stops the governed poll near
    // the budget AND honours the one-oversized-record floor, WITHOUT a broker.

    #[test]
    fn arena_stop_record_bounded_when_no_byte_cap() {
        // No byte cap (bare recv path) -> never stop on bytes, however large.
        assert!(!arena_byte_limit_reached(10_000_000, 5, None));
        assert!(!arena_byte_limit_reached(0, 0, None));
    }

    #[test]
    fn arena_stop_floors_at_one_record() {
        // Byte cap reached but ZERO records drained yet -> do NOT stop. The
        // floor guarantees an oversized record (bigger than the whole budget) is
        // still admitted as its own poll, so the loop never stalls forever.
        assert!(
            !arena_byte_limit_reached(50_000, 0, Some(1024)),
            "floor: must take at least one record before a byte cap can stop"
        );
    }

    #[test]
    fn arena_stop_when_cap_reached_with_records() {
        // >= 1 record AND arena at/over the cap -> stop the governed poll.
        assert!(
            arena_byte_limit_reached(1024, 1, Some(1024)),
            "arena_len == cap with a record present -> stop"
        );
        assert!(
            arena_byte_limit_reached(2048, 3, Some(1024)),
            "arena_len > cap with records present -> stop"
        );
        // Under the cap with records present -> keep draining.
        assert!(
            !arena_byte_limit_reached(512, 2, Some(1024)),
            "arena_len < cap -> keep draining toward the budget"
        );
    }

    // --- Highest-offset-per-partition commit list -------------------------
    //
    // The ack barrier commits the HIGHEST offset per partition (cumulative,
    // Kafka "commit up to N"). These prove the fold is correct WITHOUT a live
    // broker -- broker-free, exactly as the recv-arena tests above.

    #[test]
    fn highest_offsets_picks_max_per_partition() {
        let topic: Arc<str> = Arc::from("events");
        // Out-of-order offsets across two partitions: p0 sees {5, 2, 9, 7},
        // p1 sees {3, 1}. Highest per partition: p0 -> 9, p1 -> 3.
        let tokens = vec![
            KafkaToken::new(Arc::clone(&topic), 0, 5),
            KafkaToken::new(Arc::clone(&topic), 1, 3),
            KafkaToken::new(Arc::clone(&topic), 0, 2),
            KafkaToken::new(Arc::clone(&topic), 0, 9),
            KafkaToken::new(Arc::clone(&topic), 1, 1),
            KafkaToken::new(Arc::clone(&topic), 0, 7),
        ];
        let map = highest_offsets_per_partition(&tokens);
        assert_eq!(map.len(), 2, "two partitions");
        assert_eq!(
            map.get(&(Arc::clone(&topic), 0)),
            Some(&9),
            "partition 0 keeps the highest offset 9, not the last-seen 7"
        );
        assert_eq!(
            map.get(&(Arc::clone(&topic), 1)),
            Some(&3),
            "partition 1 keeps the highest offset 3"
        );
    }

    #[test]
    fn highest_offsets_separates_distinct_topics() {
        let a: Arc<str> = Arc::from("topic-a");
        let b: Arc<str> = Arc::from("topic-b");
        // Same partition number on two topics must NOT collide.
        let tokens = vec![
            KafkaToken::new(Arc::clone(&a), 0, 10),
            KafkaToken::new(Arc::clone(&b), 0, 4),
            KafkaToken::new(Arc::clone(&a), 0, 11),
        ];
        let map = highest_offsets_per_partition(&tokens);
        assert_eq!(map.len(), 2, "two (topic, partition) keys");
        assert_eq!(map.get(&(a, 0)), Some(&11));
        assert_eq!(map.get(&(b, 0)), Some(&4));
    }

    #[test]
    fn highest_offsets_empty_tokens_yield_empty_map() {
        let map = highest_offsets_per_partition(&[]);
        assert!(map.is_empty());
    }

    #[test]
    fn build_commit_tpl_stores_highest_plus_one() {
        let topic: Arc<str> = Arc::from("events");
        let tokens = vec![
            KafkaToken::new(Arc::clone(&topic), 0, 5),
            KafkaToken::new(Arc::clone(&topic), 0, 9),
            KafkaToken::new(Arc::clone(&topic), 1, 3),
        ];
        let tpl = build_commit_tpl(&tokens).expect("valid tpl");
        // Next-to-read offset is highest + 1: p0 -> 10, p1 -> 4.
        let e0 = tpl
            .find_partition("events", 0)
            .expect("partition 0 present");
        assert_eq!(
            e0.offset(),
            Offset::Offset(10),
            "p0 commits highest(9) + 1 = 10 (next-to-read)"
        );
        let e1 = tpl
            .find_partition("events", 1)
            .expect("partition 1 present");
        assert_eq!(
            e1.offset(),
            Offset::Offset(4),
            "p1 commits highest(3) + 1 = 4 (next-to-read)"
        );
    }

    // --- partition_limited diagnostic + gate wiring -----------------------

    #[cfg(feature = "governor")]
    #[test]
    fn partition_limited_truth_table() {
        // members >= partitions && lag > 0 -> limited.
        assert!(
            partition_limited(4, 4, 10),
            "equal members+partitions, lag>0"
        );
        assert!(partition_limited(6, 4, 1), "more members than partitions");

        // members < partitions -> headroom to scale out, NOT limited.
        assert!(
            !partition_limited(2, 4, 10),
            "fewer members -> can scale out"
        );

        // lag == 0 -> no backlog, not limited even when over-provisioned.
        assert!(!partition_limited(8, 4, 0), "no lag -> not limited");

        // partitions == 0 (no topic info) -> never a false positive.
        assert!(
            !partition_limited(4, 0, 10),
            "no partition info -> not limited"
        );
        assert!(!partition_limited(0, 0, 0), "all zero -> not limited");
    }

    #[cfg(feature = "governor")]
    #[test]
    fn partition_limited_warn_dedups_within_window() {
        use std::time::{Duration, Instant};

        let diag = PartitionLimitedDiagnostic {
            last_warn: parking_lot::Mutex::new(None),
            #[allow(clippy::duration_suboptimal_units)]
            cooldown: Duration::from_secs(300),
        };
        let t0 = Instant::now();

        // First call in the window -> warns.
        assert!(diag.should_warn_at(t0), "first warning fires");
        // Same window -> suppressed.
        assert!(
            !diag.should_warn_at(t0 + Duration::from_secs(10)),
            "second warning within cooldown is suppressed"
        );
        assert!(
            !diag.should_warn_at(t0 + Duration::from_secs(299)),
            "still within cooldown -> suppressed"
        );
        // Past the cooldown -> warns again exactly once.
        assert!(
            diag.should_warn_at(t0 + Duration::from_secs(301)),
            "after cooldown the warning re-fires once"
        );
        assert!(
            !diag.should_warn_at(t0 + Duration::from_secs(305)),
            "new window re-armed; immediate repeat suppressed"
        );
    }

    /// The Kafka gate actuator drives pause/resume EXACTLY ONCE per edge
    /// through an `InboundGate`, broker-free. We prove the EDGE wiring (the
    /// risky part) with a counting actuator; the live consumer pause/resume
    /// path is left to a broker integration test. The `recv` gate
    /// hook is verified `None`-default no-op by every existing recv test.
    #[cfg(feature = "governor")]
    #[test]
    fn inbound_gate_edge_wiring_drives_actuator_once_per_edge() {
        use crate::governor::{Admit, GateActuator, Hysteresis, InboundGate, UnifiedPressure};
        use crate::governor::{Pressure, PressureSource};
        use std::sync::atomic::{AtomicU64, AtomicUsize};

        struct MockSource(AtomicU64);
        impl PressureSource for MockSource {
            fn name(&self) -> &'static str {
                "mock"
            }
            fn sample(&self) -> Pressure {
                Pressure::new(f64::from_bits(self.0.load(Ordering::Relaxed)))
            }
            fn is_hard(&self) -> bool {
                true
            }
        }

        struct Counter {
            pauses: AtomicUsize,
            resumes: AtomicUsize,
        }
        struct Forward(Arc<Counter>);
        impl GateActuator for Forward {
            fn pause(&self) {
                self.0.pauses.fetch_add(1, Ordering::Relaxed);
            }
            fn resume(&self) {
                self.0.resumes.fetch_add(1, Ordering::Relaxed);
            }
        }

        let src = Arc::new(MockSource(AtomicU64::new(0.1_f64.to_bits())));
        let pressure = Arc::new(UnifiedPressure::new(
            vec![Arc::clone(&src) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("valid band"),
        ));
        let counter = Arc::new(Counter {
            pauses: AtomicUsize::new(0),
            resumes: AtomicUsize::new(0),
        });
        let gate = InboundGate::new(
            Arc::clone(&pressure),
            Box::new(Forward(Arc::clone(&counter))),
        );

        // Low -> open, no calls.
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert_eq!(counter.pauses.load(Ordering::Relaxed), 0);

        // Rising edge -> pause once even across repeated evaluates.
        src.0.store(0.95_f64.to_bits(), Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(
            counter.pauses.load(Ordering::Relaxed),
            1,
            "pause once per edge"
        );

        // Falling edge -> resume once.
        src.0.store(0.10_f64.to_bits(), Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert_eq!(
            counter.resumes.load(Ordering::Relaxed),
            1,
            "resume once per edge"
        );
    }

    #[test]
    fn test_subscribed_topics_rwlock_update() {
        // Verify the RwLock pattern used in recv() for subscribed_topics
        let topics = parking_lot::RwLock::new(vec!["events_load".to_string()]);

        // Read path (Debug, metrics)
        assert_eq!(topics.read().len(), 1);

        // Write path (after topic refresh re-subscribe)
        *topics.write() = vec!["events_load".to_string(), "logs_load".to_string()];
        assert_eq!(topics.read().len(), 2);
        assert_eq!(topics.read()[1], "logs_load");
    }
}
