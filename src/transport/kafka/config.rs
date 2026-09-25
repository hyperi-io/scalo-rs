// Project:   scalo
// File:      src/transport/kafka/config.rs
// Purpose:   Kafka transport configuration with profiles and config-driven overrides
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka configuration with profile-based defaults and config-driven overrides.
//!
//! ## Profile System
//!
//! Kafka uses a profile-based configuration system where:
//! 1. A **profile** provides opinionated librdkafka defaults for a use case
//! 2. The **sizing surface** ([`KafkaSizingConfig`]) sets batching, codec and
//!    delivery settings over it
//! 3. **User config** can override any librdkafka setting via `librdkafka_overrides`
//! 4. Overrides always win: every producer and consumer path applies
//!    `librdkafka_overrides` after the profile and the sizing surface
//!
//! ## Available Profiles
//!
//! - **`production`**: High-throughput, PB/day workloads. Large queues, cooperative
//!   rebalancing, disabled CRC checks, optimized fetch parameters.
//! - **`devtest`**: Development and testing. Relaxed SSL validation, smaller queues,
//!   faster reconnection, debug-friendly settings.
//!
//! ## Example YAML Config
//!
//! ```yaml
//! kafka:
//!   profile: production
//!   brokers:
//!     - kafka1:9092
//!     - kafka2:9092
//!   group: my-consumer-group
//!   topics:
//!     - events
//!   # Override specific librdkafka settings
//!   librdkafka_overrides:
//!     fetch.min.bytes: "2097152"  # 2MB instead of profile's 1MB
//!     statistics.interval.ms: "5000"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

// ============================================================================
// Message-size ceiling (one number across the three layers)
// ============================================================================

/// The largest single record the pipeline carries, in bytes (16 MiB).
///
/// This is ONE number shared by three layers that must agree: the broker's
/// `message.max.bytes` (and `replica.fetch.max.bytes`), the topic's
/// `max.message.bytes`, and the producer's own `message.max.bytes` set here.
/// Raise one without the others and the odd layer rejects or stalls -- the
/// producer refuses locally with `MSG_SIZE_TOO_LARGE`, or the consumer never
/// fetches the record.
///
/// 16 MiB is derived from filebeat's own `message_max_bytes` ceiling of 10 MiB
/// (it truncates past that, so no input can deliver more) plus headroom for the
/// ~1.5x growth an enrichment pass adds when it re-enters Kafka. It is a
/// CEILING, not a tuning dial: the sizing profiles vary batching and latency,
/// never the largest record the pipeline accepts.
pub const MESSAGE_MAX_BYTES: i32 = 16_777_216;

// ============================================================================
// Producer codec
// ============================================================================

/// The codec every sizing profile produces with.
///
/// Every app image has to link a librdkafka built with zstd: without it,
/// producer creation fails on this default, and a consumer cannot read the
/// batches it writes.
pub(crate) const DEFAULT_PRODUCER_CODEC: &str = "zstd";

/// The `compression.level` zstd runs at unless a raw map names one.
///
/// librdkafka reads a level against whichever codec is set, and lz4 switches
/// to its slow high-compression mode from 3, so the level is only ever set
/// while the codec is zstd.
pub(crate) const ZSTD_COMPRESSION_LEVEL: &str = "3";

// ============================================================================
// Consumer group protocol (KIP-848)
// ============================================================================

/// Which consumer-group rebalance protocol the consumer joins with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ConsumerProtocol {
    /// KIP-848: the broker's group coordinator computes the assignment and
    /// pushes it on the heartbeat, so adding or removing a member costs no
    /// stop-the-world rebalance -- the difference a KEDA scale event feels.
    /// Requires a Kafka 4.0+ broker, and the transport falls back to
    /// [`Classic`](Self::Classic) when the broker will not speak it.
    #[default]
    Consumer,

    /// The pre-4.0 protocol: the group leader computes the assignment and
    /// every member stops consuming while it does.
    Classic,
}

impl ConsumerProtocol {
    /// The librdkafka `group.protocol` value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Consumer => "consumer",
            Self::Classic => "classic",
        }
    }
}

impl FromStr for ConsumerProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "consumer" => Ok(Self::Consumer),
            "classic" => Ok(Self::Classic),
            _ => Err(format!(
                "unknown kafka consumer protocol {s:?}; expected one of: consumer, classic"
            )),
        }
    }
}

impl std::fmt::Display for ConsumerProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Consumer properties librdkafka refuses alongside `group.protocol=consumer`.
///
/// `rd_kafka_conf_finalize` rejects the whole client if any of these was set at
/// all -- the value is irrelevant, only that it was touched -- so they are
/// stripped after every config layer has run rather than skipped in one of
/// them. Their KIP-848 replacements are broker-side
/// (`group.consumer.session.timeout.ms`,
/// `group.consumer.heartbeat.interval.ms`) or renamed
/// (`partition.assignment.strategy` becomes `group.remote.assignor`).
pub const CLASSIC_ONLY_CONSUMER_KEYS: &[&str] = &[
    "partition.assignment.strategy",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "group.protocol.type",
];

// ============================================================================
// Self-Regulation Profile (Kafka sizing surface)
// ============================================================================

/// Opinionated sizing profile for the Kafka GET/SEND surface.
///
/// The profile sets default values for all named knobs below. An explicit
/// per-knob value in [`KafkaSizingConfig`] always wins over the profile
/// default. The raw librdkafka maps win over both, and
/// [`KafkaConfig::librdkafka_overrides`] is applied after all of them.
///
/// Profiles target the BYTE-level throughput budget and latency envelope.
/// They differ in batching and latency only: every profile produces with
/// `zstd` at `compression.level` 3.
///
/// | Profile | Use case |
/// |---|---|
/// | `throughput` (default) | PB/day batch ingest, large fanout topics |
/// | `balanced` | Mixed OLTP + analytics, moderate batch size |
/// | `low_latency` | Near-real-time, event-driven, small messages |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SelfRegulationProfile {
    /// Maximum throughput: generous byte budgets, tolerates batching delay.
    ///
    /// Consumer: 1 MiB fetch.min.bytes, 50 ms wait, 16 MiB per-partition,
    /// 50 MiB total, 2000 poll-safety cap.
    /// Producer: 128 KiB batch, 20 ms linger, zstd level 3, 64 MiB buffer,
    /// 5 in-flight, 16 MiB record ceiling.
    #[default]
    Throughput,

    /// Balanced: moderate batching, 5 ms linger, smaller per-partition budget.
    ///
    /// Consumer: 256 KiB fetch.min.bytes, 25 ms wait, 16 MiB per-partition,
    /// 50 MiB total, 1000 poll-safety cap.
    /// Producer: 64 KiB batch, 5 ms linger, zstd level 3, 32 MiB buffer,
    /// 5 in-flight, 16 MiB record ceiling.
    Balanced,

    /// Low latency: minimal batching delay, smaller buffers.
    ///
    /// Consumer: 1 byte fetch.min.bytes, 5 ms wait, 16 MiB per-partition,
    /// 16 MiB total, 500 poll-safety cap.
    /// Producer: 16 KiB batch, 0 ms linger, zstd level 3, 16 MiB buffer,
    /// 5 in-flight, 16 MiB record ceiling.
    LowLatency,
}

impl SelfRegulationProfile {
    /// Return the consumer knob defaults for this profile.
    #[must_use]
    pub fn consumer_defaults(self) -> ConsumerKnobs {
        match self {
            Self::Throughput => ConsumerKnobs {
                // 1 MiB -- forces broker to batch at least one full record page.
                fetch_min_bytes: Some(1_048_576),
                // 50 ms -- gives broker time to fill the 1 MiB budget.
                fetch_max_wait_ms: Some(50),
                // The record ceiling: a partition must be able to yield one
                // maximum-size record in a single fetch.
                max_partition_fetch_bytes: Some(MESSAGE_MAX_BYTES),
                // 50 MiB -- caps total network fetch per round-trip, and stays
                // under the 55 MiB the broker holds read-only on MSK Express.
                fetch_max_bytes: Some(52_428_800),
                // 2000 -- poll-safety cap enforced by the recv() loop.
                max_poll_records: Some(2000),
            },
            Self::Balanced => ConsumerKnobs {
                fetch_min_bytes: Some(262_144), // 256 KiB
                fetch_max_wait_ms: Some(25),
                max_partition_fetch_bytes: Some(MESSAGE_MAX_BYTES),
                fetch_max_bytes: Some(52_428_800), // 50 MiB
                max_poll_records: Some(1000),
            },
            Self::LowLatency => ConsumerKnobs {
                fetch_min_bytes: Some(1),   // no batching threshold
                fetch_max_wait_ms: Some(5), // return fast
                max_partition_fetch_bytes: Some(MESSAGE_MAX_BYTES),
                // The smallest total budget that can still carry one
                // maximum-size record: equal to the per-partition ceiling.
                fetch_max_bytes: Some(MESSAGE_MAX_BYTES),
                max_poll_records: Some(500),
            },
        }
    }

    /// Return the producer knob defaults for this profile.
    #[must_use]
    pub fn producer_defaults(self) -> ProducerKnobs {
        match self {
            Self::Throughput => ProducerKnobs {
                // 128 KiB per MessageSet -- batches up fast but not excessive.
                batch_size_bytes: Some(131_072),
                // 20 ms -- enough time to fill the 128 KiB batch.
                linger_ms: Some(20),
                // The same codec on every profile; the level follows it.
                compression_type: Some(DEFAULT_PRODUCER_CODEC.to_string()),
                // 64 MiB total producer queue (queue.buffering.max.kbytes in KiB).
                buffer_memory_bytes: Some(67_108_864),
                // 5 in-flight per connection -- matches exactly-once safe limit.
                max_in_flight: Some(5),
                // Profiles leave idempotence unset; the default (on) is applied
                // in resolved_producer_map, independent of profile.
                idempotence: None,
                // The pipeline-wide record ceiling -- identical on every
                // profile, see MESSAGE_MAX_BYTES.
                message_max_bytes: Some(MESSAGE_MAX_BYTES),
            },
            Self::Balanced => ProducerKnobs {
                batch_size_bytes: Some(65_536), // 64 KiB
                linger_ms: Some(5),
                compression_type: Some(DEFAULT_PRODUCER_CODEC.to_string()),
                buffer_memory_bytes: Some(33_554_432), // 32 MiB
                max_in_flight: Some(5),
                idempotence: None,
                message_max_bytes: Some(MESSAGE_MAX_BYTES),
            },
            Self::LowLatency => ProducerKnobs {
                batch_size_bytes: Some(16_384), // 16 KiB
                linger_ms: Some(0),             // send immediately
                compression_type: Some(DEFAULT_PRODUCER_CODEC.to_string()),
                buffer_memory_bytes: Some(16_777_216), // 16 MiB
                max_in_flight: Some(5),
                idempotence: None,
                message_max_bytes: Some(MESSAGE_MAX_BYTES),
            },
        }
    }
}

/// Named consumer sizing knobs.
///
/// All fields are `Option<T>`: `None` means "use the profile default".
/// An explicit `Some(v)` wins over the profile default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct ConsumerKnobs {
    /// Minimum bytes the broker must have ready before responding to a Fetch.
    ///
    /// librdkafka: `fetch.min.bytes` (default 1 byte).
    /// Raising this batches more data per round-trip but adds latency when
    /// topic traffic is low.
    #[serde(default)]
    pub fetch_min_bytes: Option<i32>,

    /// Maximum milliseconds the broker may wait to fill `fetch.min.bytes`.
    ///
    /// librdkafka: `fetch.wait.max.ms` (default 500 ms).
    /// Works in tandem with `fetch_min_bytes` -- the broker returns whatever
    /// it has when this timer fires even if `fetch.min.bytes` is not met.
    #[serde(default)]
    pub fetch_max_wait_ms: Option<u32>,

    /// Maximum bytes returned per partition per Fetch request.
    ///
    /// librdkafka: `max.partition.fetch.bytes` (alias `fetch.message.max.bytes`,
    /// default 1 MiB). Must be >= the topic's `max.message.bytes`, so every
    /// profile sets it to [`MESSAGE_MAX_BYTES`]. librdkafka does grow it on
    /// sight of a larger record, but the explicit value keeps the consumer's
    /// memory envelope predictable instead of discovered.
    #[serde(default)]
    pub max_partition_fetch_bytes: Option<i32>,

    /// Maximum total bytes returned by the broker for a single Fetch request
    /// across all partitions.
    ///
    /// librdkafka: `fetch.max.bytes` (default 50 MiB). Keep it at or under
    /// 50 MiB: MSK Express holds the broker's own 55 MiB fetch ceiling
    /// read-only, so asking for more is a number that can never be honoured.
    #[serde(default)]
    pub fetch_max_bytes: Option<i32>,

    /// Maximum number of messages the recv() loop returns per call.
    ///
    /// NOTE: `max.poll.records` does NOT exist in librdkafka -- there is no
    /// broker-level property for this. This is a purely CLIENT-SIDE cap,
    /// enforced by passing this value as the `max` argument to
    /// `KafkaTransport::recv()` via `KafkaSizingConfig::effective_poll_cap()`.
    /// It bounds the batch size delivered to the WorkBatch layer, not the
    /// network fetch size (which is byte-governed by the knobs above).
    #[serde(default)]
    pub max_poll_records: Option<usize>,
}

/// Named producer sizing knobs.
///
/// All fields are `Option<T>`: `None` means "use the profile default".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct ProducerKnobs {
    /// Maximum bytes per MessageSet (librdkafka `batch.size`, default 1 MiB).
    ///
    /// This is the PER-BATCH ceiling, not the total queue size. Raise this for
    /// fewer, larger network writes. Note: `batch.size` in librdkafka is in
    /// bytes (matches the Java Kafka client name and unit).
    #[serde(default)]
    pub batch_size_bytes: Option<i32>,

    /// Accumulation delay before transmitting a MessageSet.
    ///
    /// librdkafka: `linger.ms` (alias for `queue.buffering.max.ms`, default 5 ms).
    /// Higher values fill larger batches; 0 sends immediately.
    #[serde(default)]
    pub linger_ms: Option<u32>,

    /// Compression codec for MessageSets.
    ///
    /// librdkafka: `compression.type` (alias for `compression.codec`).
    /// Valid values: `none`, `gzip`, `snappy`, `lz4`, `zstd`.
    /// Default (all profiles): `zstd` at `compression.level` 3.
    ///
    /// The level is set only while the resolved codec is `zstd`, so another
    /// codec named here runs at librdkafka's own default level for it. To run
    /// zstd at another level, set `compression.level` in `producer_librdkafka`.
    #[serde(default)]
    pub compression_type: Option<String>,

    /// Total byte budget for the producer's in-memory queue.
    ///
    /// librdkafka: `queue.buffering.max.kbytes` (in KiB, default 1 GiB).
    /// This is the TOTAL queue, not per-batch. Set lower to bound memory
    /// usage in containers. Stored as bytes in this struct; divided by 1024
    /// when applied to librdkafka.
    #[serde(default)]
    pub buffer_memory_bytes: Option<u64>,

    /// Maximum concurrent in-flight requests per broker connection.
    ///
    /// librdkafka: `max.in.flight.requests.per.connection` (default 1,000,000).
    /// Set to 5 to match the exactly-once safe limit (KIP-98) and to bound
    /// memory/reorder window. Matches the Java Kafka producer default for
    /// idempotent producers.
    #[serde(default)]
    pub max_in_flight: Option<u32>,

    /// Enable the Kafka idempotent producer (`enable.idempotence`).
    ///
    /// `None` (default) -> ON: scalo enables idempotence by default (v2.10) as
    /// the cheap "effectively-once" step -- it dedups producer-retry writes in
    /// the broker at near-zero cost. Idempotence REQUIRES `acks=all`,
    /// `max.in.flight<=5` and `retries>0`, so when on
    /// [`resolved_producer_map`](KafkaSizingConfig::resolved_producer_map)
    /// forces `acks=all` and clamps `max.in.flight` to 5. Set `Some(false)` to
    /// opt out (e.g. a latency-bound topic that wants `acks=1`). The raw
    /// `producer_librdkafka` escape hatch still wins over this.
    #[serde(default)]
    pub idempotence: Option<bool>,

    /// Largest single record the producer will put on the wire, in bytes.
    ///
    /// librdkafka: `message.max.bytes`, whose default of 1,000,000 rejects an
    /// oversize record LOCALLY with `MSG_SIZE_TOO_LARGE` -- the broker never
    /// sees it, so raising the broker's ceiling alone changes nothing. Three
    /// layers have to agree: the broker's `message.max.bytes`, the topic's
    /// `max.message.bytes`, and this. Default on every profile:
    /// [`MESSAGE_MAX_BYTES`] (16 MiB). `batch.size` is unrelated -- it is a
    /// soft target and a larger record still ships as its own batch.
    #[serde(default)]
    pub message_max_bytes: Option<i32>,
}

/// Kafka sizing surface: profile + named per-knob overrides + raw escape hatch.
///
/// Resolution precedence (lowest to highest), the same on every producer and
/// consumer path:
/// 1. `SelfRegulationProfile` defaults
/// 2. Named knobs in `consumer` / `producer` (explicit `Some(v)` wins)
/// 3. Raw librdkafka maps `consumer_librdkafka` / `producer_librdkafka`
/// 4. [`KafkaConfig::librdkafka_overrides`], which the transport applies after
///    this whole surface
///
/// The raw maps are logged (one line per key) when they override a property
/// that the sizing surface depends on (the fetch byte sizes and
/// `enable.auto.commit`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct KafkaSizingConfig {
    /// Sizing profile (throughput / balanced / low_latency).
    #[serde(default)]
    pub profile: SelfRegulationProfile,

    /// Per-knob consumer overrides (any `Some(v)` beats the profile default).
    #[serde(default)]
    pub consumer: ConsumerKnobs,

    /// Per-knob producer overrides (any `Some(v)` beats the profile default).
    #[serde(default)]
    pub producer: ProducerKnobs,

    /// Raw librdkafka consumer properties, winning over the profile and the
    /// named knobs. [`KafkaConfig::librdkafka_overrides`] is applied after
    /// them.
    ///
    /// Keys must be valid librdkafka property names (e.g. `fetch.wait.max.ms`).
    /// An invalid key silently no-ops in librdkafka -- double-check spelling.
    #[serde(default)]
    pub consumer_librdkafka: BTreeMap<String, String>,

    /// Raw librdkafka producer properties, winning over the profile and the
    /// named knobs. Only [`KafkaConfig::librdkafka_overrides`] is applied after
    /// them.
    ///
    /// Keys must be valid librdkafka property names (e.g. `linger.ms`). A key
    /// replaces its librdkafka alias from the layers below it
    /// (`compression.codec` for `compression.type`, `queue.buffering.max.ms`
    /// for `linger.ms`).
    #[serde(default)]
    pub producer_librdkafka: BTreeMap<String, String>,
}

// ============================================================================
// Keys the sizing governor depends on -- logged when raw-overridden.
// ============================================================================

/// Consumer property names whose values the sizing governor reads to compute
/// byte budgets. When the raw escape hatch overrides one of these, we log a
/// warning so the operator knows the governor's assumptions have changed.
const GOVERNOR_CONSUMER_KEYS: &[&str] = &[
    "fetch.min.bytes",
    "fetch.max.bytes",
    "fetch.wait.max.ms",
    "max.partition.fetch.bytes",
    "fetch.message.max.bytes",
    "enable.auto.commit",
];

/// Producer property names the sizing governor sets.
const GOVERNOR_PRODUCER_KEYS: &[&str] = &[
    "batch.size",
    "linger.ms",
    "queue.buffering.max.ms",
    "compression.type",
    "compression.codec",
    "compression.level",
    "queue.buffering.max.kbytes",
    "max.in.flight.requests.per.connection",
    "message.max.bytes",
    "partitioner",
    "sticky.partitioning.linger.ms",
    // Effectively-once invariants (v2.10): the sizing surface sets these when
    // idempotence is on; a raw override changes the delivery guarantee.
    "enable.idempotence",
    "acks",
    "retries",
];

impl KafkaSizingConfig {
    /// Resolve the effective consumer librdkafka key/value map.
    ///
    /// Precedence: profile defaults < named knobs < raw `consumer_librdkafka`.
    ///
    /// This is a PURE function -- suitable for unit testing without a live
    /// broker. The caller feeds the returned map into `ClientConfig::set`.
    #[must_use]
    pub fn resolved_consumer_map(&self) -> BTreeMap<String, String> {
        let profile_knobs = self.profile.consumer_defaults();

        // Merge: explicit `Some` wins over profile default.
        let fetch_min_bytes = self
            .consumer
            .fetch_min_bytes
            .or(profile_knobs.fetch_min_bytes)
            .unwrap_or(1);
        let fetch_max_wait_ms = self
            .consumer
            .fetch_max_wait_ms
            .or(profile_knobs.fetch_max_wait_ms)
            .unwrap_or(500);
        let max_partition_fetch_bytes = self
            .consumer
            .max_partition_fetch_bytes
            .or(profile_knobs.max_partition_fetch_bytes)
            .unwrap_or(1_048_576);
        let fetch_max_bytes = self
            .consumer
            .fetch_max_bytes
            .or(profile_knobs.fetch_max_bytes)
            .unwrap_or(52_428_800);

        let mut map = BTreeMap::new();
        map.insert("fetch.min.bytes".to_string(), fetch_min_bytes.to_string());
        map.insert(
            "fetch.wait.max.ms".to_string(),
            fetch_max_wait_ms.to_string(),
        );
        map.insert(
            "max.partition.fetch.bytes".to_string(),
            max_partition_fetch_bytes.to_string(),
        );
        map.insert("fetch.max.bytes".to_string(), fetch_max_bytes.to_string());

        // Apply the raw escape hatch last -- it wins.
        for (k, v) in &self.consumer_librdkafka {
            if GOVERNOR_CONSUMER_KEYS.contains(&k.as_str()) {
                tracing::warn!(
                    key = k.as_str(),
                    value = v.as_str(),
                    "kafka sizing: raw consumer_librdkafka overrides a governor key"
                );
            }
            map.insert(k.clone(), v.clone());
        }

        map
    }

    /// Resolve the effective producer librdkafka key/value map.
    ///
    /// Precedence: profile defaults < named knobs < raw `producer_librdkafka`.
    /// The transport applies [`KafkaConfig::librdkafka_overrides`] after this
    /// map on every producer path.
    ///
    /// `compression.level` follows the codec: 3 while the resolved codec is
    /// `zstd` and no raw map names a level, unset for any other codec. A raw
    /// key also replaces its librdkafka alias from the layers below it.
    ///
    /// KIP-794 note: librdkafka does not support `partitioner.ignore.keys` (a
    /// Java-client-only property). The librdkafka equivalent for uniform sticky
    /// null-key distribution is `sticky.partitioning.linger.ms` (default 10 ms,
    /// works with the `consistent_random` default partitioner). We set this to
    /// `linger_ms` so null-key batches accumulate for one full linger window
    /// before rotation, which is the closest functional match to KIP-794's
    /// intent for the librdkafka client.
    ///
    /// This is a PURE function -- suitable for unit testing without a live broker.
    #[must_use]
    pub fn resolved_producer_map(&self) -> BTreeMap<String, String> {
        self.producer_map_under(Vec::new())
    }

    /// The producer map with `outer`, a raw layer applied after
    /// `producer_librdkafka`, laid over it.
    pub(crate) fn producer_map_under(&self, outer: RawLayer<'_>) -> BTreeMap<String, String> {
        let mut map = self.named_producer_map();
        for (k, v) in &self.producer_librdkafka {
            if GOVERNOR_PRODUCER_KEYS.contains(&k.as_str()) {
                tracing::warn!(
                    key = k.as_str(),
                    value = v.as_str(),
                    "kafka sizing: raw producer_librdkafka overrides a governor key"
                );
            }
        }
        overlay_raw_producer_layers(&mut map, &[raw_layer(&self.producer_librdkafka), outer]);
        map
    }

    /// The producer map from the profile and the named knobs alone.
    fn named_producer_map(&self) -> BTreeMap<String, String> {
        let profile_knobs = self.profile.producer_defaults();

        let batch_size_bytes = self
            .producer
            .batch_size_bytes
            .or(profile_knobs.batch_size_bytes)
            .unwrap_or(1_000_000);
        let linger_ms = self
            .producer
            .linger_ms
            .or(profile_knobs.linger_ms)
            .unwrap_or(5);
        let compression_type = self
            .producer
            .compression_type
            .clone()
            .or(profile_knobs.compression_type)
            .unwrap_or_else(|| "none".to_string());
        let buffer_memory_bytes = self
            .producer
            .buffer_memory_bytes
            .or(profile_knobs.buffer_memory_bytes)
            .unwrap_or(1_073_741_824); // 1 GiB (librdkafka default)
        let message_max_bytes = self
            .producer
            .message_max_bytes
            .or(profile_knobs.message_max_bytes)
            .unwrap_or(MESSAGE_MAX_BYTES);
        // Effectively-once (v2.10): idempotence ON by default. It REQUIRES
        // max.in.flight<=5, so when on we clamp the resolved value to 5 (a
        // higher value would make librdkafka reject the producer at init).
        let idempotence = self.producer.idempotence.unwrap_or(true);
        let mut max_in_flight = self
            .producer
            .max_in_flight
            .or(profile_knobs.max_in_flight)
            .unwrap_or(1_000_000);
        if idempotence && max_in_flight > 5 {
            tracing::warn!(
                requested = max_in_flight,
                "kafka sizing: idempotence requires max.in.flight<=5; clamping to 5"
            );
            max_in_flight = 5;
        }

        // queue.buffering.max.kbytes is in KiB -- convert from bytes.
        let buffer_kib = (buffer_memory_bytes / 1024).max(1);

        let mut map = BTreeMap::new();
        map.insert("batch.size".to_string(), batch_size_bytes.to_string());
        map.insert("linger.ms".to_string(), linger_ms.to_string());
        map.insert("compression.type".to_string(), compression_type);
        map.insert(
            "queue.buffering.max.kbytes".to_string(),
            buffer_kib.to_string(),
        );
        map.insert(
            "max.in.flight.requests.per.connection".to_string(),
            max_in_flight.to_string(),
        );
        // The client-side record ceiling. Without it librdkafka rejects
        // anything over 1,000,000 bytes before the broker is consulted.
        map.insert(
            "message.max.bytes".to_string(),
            message_max_bytes.to_string(),
        );

        // Effectively-once (v2.10): enable the idempotent producer by default.
        // Dedups producer-retry writes in the broker at near-zero cost. It
        // REQUIRES acks=all (forced here) and retries>0 (librdkafka default is
        // high, left alone). Opt out with producer.idempotence=Some(false), or
        // override either key via a raw map, which is laid over this one and
        // wins. Disabling restores the prior leader-/profile-ack behaviour.
        if idempotence {
            map.insert("enable.idempotence".to_string(), "true".to_string());
            map.insert("acks".to_string(), "all".to_string());
        } else {
            map.insert("enable.idempotence".to_string(), "false".to_string());
        }

        // KIP-794 / uniform sticky for null-keyed messages.
        // `partitioner.ignore.keys` is a Java-client-only property and does
        // NOT exist in librdkafka. The librdkafka equivalent is to keep the
        // default `consistent_random` partitioner (null keys -> random
        // partition) and set `sticky.partitioning.linger.ms` equal to the
        // linger window so null-key batches stick to one partition until the
        // batch is full, then rotate. This is the closest functional match to
        // KIP-794 available in librdkafka.
        //
        // We do NOT set `partitioner` here to avoid overriding any caller-
        // supplied value (keyed RoutedSender paths set their own partitioner).
        map.insert(
            "sticky.partitioning.linger.ms".to_string(),
            linger_ms.to_string(),
        );

        map
    }

    /// Return the effective poll-safety cap (max messages per recv() call).
    ///
    /// This is a CLIENT-SIDE cap only -- there is no librdkafka property for
    /// `max.poll.records`. The value is passed as the `max` argument to
    /// `KafkaTransport::recv()` by the ServiceRuntime / WorkBatch layer.
    #[must_use]
    pub fn effective_poll_cap(&self) -> usize {
        self.consumer
            .max_poll_records
            .or(self.profile.consumer_defaults().max_poll_records)
            .unwrap_or(10_000)
    }
}

// ============================================================================
// Raw producer layers
// ============================================================================

/// One raw librdkafka override layer, as key/value pairs.
pub(crate) type RawLayer<'a> = Vec<(&'a str, &'a str)>;

/// A raw override map as a [`RawLayer`].
pub(crate) fn raw_layer<'a>(
    map: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> RawLayer<'a> {
    map.into_iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

/// Producer property names librdkafka treats as one property.
const PRODUCER_KEY_ALIASES: &[(&str, &str)] = &[
    ("compression.type", "compression.codec"),
    ("linger.ms", "queue.buffering.max.ms"),
];

/// The other librdkafka name for `key`, when it has one.
fn producer_key_alias(key: &str) -> Option<&'static str> {
    PRODUCER_KEY_ALIASES.iter().find_map(|&(a, b)| {
        if key == a {
            Some(b)
        } else if key == b {
            Some(a)
        } else {
            None
        }
    })
}

/// Lay raw override layers over a producer map, lowest first.
///
/// A key a layer names drops its alias from the layers below, because rdkafka
/// hands its settings to librdkafka in hash order and two names for one
/// property would leave the winner to chance. zstd then gets
/// [`ZSTD_COMPRESSION_LEVEL`] unless some layer named a level.
fn overlay_raw_producer_layers(map: &mut BTreeMap<String, String>, layers: &[RawLayer<'_>]) {
    let mut level_named = false;
    for layer in layers {
        for &(key, value) in layer {
            if let Some(alias) = producer_key_alias(key)
                && !layer.iter().any(|&(k, _)| k == alias)
            {
                map.remove(alias);
            }
            level_named |= key == "compression.level";
            map.insert(key.to_string(), value.to_string());
        }
    }
    let codec = map
        .get("compression.type")
        .or_else(|| map.get("compression.codec"));
    if !level_named && codec.is_some_and(|c| c.eq_ignore_ascii_case(DEFAULT_PRODUCER_CODEC)) {
        map.insert(
            "compression.level".to_string(),
            ZSTD_COMPRESSION_LEVEL.to_string(),
        );
    }
}

// ============================================================================
// Topic Resolution Types
// ============================================================================

/// Topic suppression rule for auto-discovery.
///
/// When auto-discovering topics, if a topic with `preferred_suffix` exists
/// for a base name, the topic with `suppressed_suffix` for that same base
/// is removed. Default: `_load` suppresses `_land`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct SuppressionRule {
    /// The suffix of the preferred (kept) topic.
    pub preferred_suffix: String,
    /// The suffix of the suppressed (removed) topic.
    pub suppressed_suffix: String,
}

// ============================================================================
// Profile System
// ============================================================================

/// Kafka configuration profile.
///
/// Profiles provide opinionated librdkafka defaults for specific use cases.
/// Users can override any setting via `librdkafka_overrides`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum KafkaProfile {
    /// Production profile: lean baseline for all data-plane services.
    ///
    /// Only sets values that differ from librdkafka defaults.
    /// Services add overrides via `librdkafka_overrides`.
    #[default]
    Production,

    /// Development/test profile: fast iteration, low memory.
    ///
    /// Cooperative rebalancing, fast reconnects, debug logging.
    /// SSL certificate verification disabled by default.
    DevTest,
}

impl FromStr for KafkaProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "production" | "prod" => Ok(Self::Production),
            "devtest" | "dev" | "test" | "development" => Ok(Self::DevTest),
            _ => Err(format!(
                "Unknown Kafka profile: {s}. Valid: production, devtest"
            )),
        }
    }
}

impl std::fmt::Display for KafkaProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Production => write!(f, "production"),
            Self::DevTest => write!(f, "devtest"),
        }
    }
}

// ============================================================================
// Merge Helper
// ============================================================================

/// Merge profile defaults with user overrides.
///
/// Starts with `profile` defaults, then applies `overrides` on top.
/// User overrides always win. Returns the final merged map.
///
/// # Example
///
/// ```rust,ignore
/// use std::collections::HashMap;
/// use scalo::transport::kafka::config::{merge_with_overrides, PRODUCTION_PROFILE};
///
/// let mut overrides = HashMap::new();
/// overrides.insert("fetch.min.bytes".to_string(), "2097152".to_string());
///
/// let merged = merge_with_overrides(PRODUCTION_PROFILE, &overrides);
/// assert_eq!(merged.get("fetch.min.bytes").unwrap(), "2097152");
/// assert_eq!(merged.get("partition.assignment.strategy").unwrap(), "cooperative-sticky");
/// ```
#[must_use]
pub fn merge_with_overrides<S: std::hash::BuildHasher>(
    profile: &[(&str, &str)],
    overrides: &HashMap<String, String, S>,
) -> HashMap<String, String> {
    let mut config = HashMap::with_capacity(profile.len() + overrides.len());

    for (key, value) in profile {
        config.insert((*key).to_string(), (*value).to_string());
    }
    for (key, value) in overrides {
        config.insert(key.clone(), value.clone());
    }

    config
}

// ============================================================================
// Profile Defaults
// ============================================================================

/// Production consumer profile -- lean baseline.
///
/// Only settings that differ from librdkafka defaults with clear justification.
/// Services override on an exception basis via `librdkafka_overrides`.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | KIP-429: avoids stop-the-world rebalances |
/// | `fetch.min.bytes` | 1 MiB | 1 byte | Batch fetches for throughput |
/// | `fetch.wait.max.ms` | 100 ms | 500 ms | Bound latency when fetch.min.bytes not met |
/// | `queued.min.messages` | 20000 | 100000 | 10-20K batches are most efficient |
/// | `enable.auto.commit` | false | true | data-plane services manage offset commits |
/// | `statistics.interval.ms` | 1000 ms | 0 (disabled) | Enable Prometheus metrics |
pub const PRODUCTION_PROFILE: &[(&str, &str)] = &[
    ("partition.assignment.strategy", "cooperative-sticky"),
    ("fetch.min.bytes", "1048576"),
    ("fetch.wait.max.ms", "100"),
    ("queued.min.messages", "20000"),
    ("enable.auto.commit", "false"),
    ("statistics.interval.ms", "1000"),
];

/// Development/test consumer profile -- minimal latency, low memory.
///
/// Inherits the same "only non-defaults" philosophy. Optimised for fast
/// iteration on developer machines: no fetch batching, smaller queues,
/// fast reconnects, debug logging.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | Consistent across all environments |
/// | `queued.min.messages` | 1000 | 100000 | Lower memory for dev machines |
/// | `enable.auto.commit` | false | true | data-plane services manage commits |
/// | `reconnect.backoff.ms` | 10 ms | 100 ms | Fast reconnect for quick iteration |
/// | `reconnect.backoff.max.ms` | 100 ms | 10000 ms | Cap quickly |
/// | `log.connection.close` | true | false | Debug-friendly |
/// | `statistics.interval.ms` | 1000 ms | 0 (disabled) | Enable metrics even in dev |
pub const DEVTEST_PROFILE: &[(&str, &str)] = &[
    ("partition.assignment.strategy", "cooperative-sticky"),
    ("queued.min.messages", "1000"),
    ("enable.auto.commit", "false"),
    ("reconnect.backoff.ms", "10"),
    ("reconnect.backoff.max.ms", "100"),
    ("log.connection.close", "true"),
    ("statistics.interval.ms", "1000"),
];

// ============================================================================
// Producer Profile Defaults
// ============================================================================

/// High-throughput producer -- lean baseline.
///
/// Only settings that differ from librdkafka defaults and that the sizing
/// surface leaves alone. Batching, codec, queue size and delivery settings
/// come from [`KafkaSizingConfig`], which every producer path applies after
/// this constant. Services override via `librdkafka_overrides`.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `socket.nagle.disable` | true | false | Kafka batches at app level |
/// | `statistics.interval.ms` | 1000 ms | 0 (disabled) | Enable Prometheus metrics |
pub const PRODUCER_HIGH_THROUGHPUT: &[(&str, &str)] = &[
    ("socket.nagle.disable", "true"),
    ("statistics.interval.ms", "1000"),
];

/// Exactly-once producer -- idempotence + ordering.
///
/// The delivery invariants are explicit here. The sizing surface, applied
/// after this constant, also sets `enable.idempotence` and
/// `max.in.flight.requests.per.connection` on every path, and `acks` while
/// idempotence is on. The table holds while `sizing.producer.idempotence` is
/// unset or `true`, the default; with it `false` the producer is not
/// idempotent whatever this profile says. Batching and codec come from the
/// sizing surface.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `enable.idempotence` | true | false | Exactly-once within partition |
/// | `acks` | all | all (-1) | Invariant for EOS (explicit) |
/// | `max.in.flight.requests.per.connection` | 5 | 1000000 | Max for idempotent producer |
/// | `socket.nagle.disable` | true | false | Kafka batches at app level |
/// | `statistics.interval.ms` | 1000 ms | 0 | Enable metrics |
pub const PRODUCER_EXACTLY_ONCE: &[(&str, &str)] = &[
    ("enable.idempotence", "true"),
    ("acks", "all"),
    ("max.in.flight.requests.per.connection", "5"),
    ("socket.nagle.disable", "true"),
    ("statistics.interval.ms", "1000"),
];

/// Low-latency producer -- leader-ack only.
///
/// `acks=1` takes effect only with `sizing.producer.idempotence: false`: the
/// idempotent producer, on by default, requires `acks=all`, and the sizing
/// surface sets it after this constant. The batching delay comes from the
/// sizing profile, and `low_latency` lingers 0 ms.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `acks` | 1 | all (-1) | Leader ack only for speed |
/// | `socket.nagle.disable` | true | false | No TCP coalescing |
/// | `statistics.interval.ms` | 1000 ms | 0 | Enable metrics |
pub const PRODUCER_LOW_LATENCY: &[(&str, &str)] = &[
    ("acks", "1"),
    ("socket.nagle.disable", "true"),
    ("statistics.interval.ms", "1000"),
];

/// DevTest producer -- fast acks.
///
/// `acks=1` takes effect only with `sizing.producer.idempotence: false`, as
/// for [`PRODUCER_LOW_LATENCY`]. Batching and codec come from the sizing
/// surface.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `acks` | 1 | all (-1) | Faster for dev |
/// | `socket.nagle.disable` | true | false | No TCP coalescing |
/// | `statistics.interval.ms` | 1000 ms | 0 | Enable metrics in dev |
pub const PRODUCER_DEVTEST: &[(&str, &str)] = &[
    ("acks", "1"),
    ("socket.nagle.disable", "true"),
    ("statistics.interval.ms", "1000"),
];

/// Legacy producer defaults -- now aliases `PRODUCER_HIGH_THROUGHPUT`.
#[deprecated(since = "2.0.0", note = "Use PRODUCER_HIGH_THROUGHPUT instead")]
pub const PRODUCER_DEFAULTS: &[(&str, &str)] = PRODUCER_HIGH_THROUGHPUT;

// ============================================================================
// Legacy Constants (for backward compatibility)
// ============================================================================

/// Alias for `PRODUCTION_PROFILE` (backward compatibility).
#[deprecated(since = "2.0.0", note = "Use PRODUCTION_PROFILE instead")]
pub const HIGH_THROUGHPUT_CONSUMER_DEFAULTS: &[(&str, &str)] = PRODUCTION_PROFILE;

/// Low-latency consumer -- minimal fetch delay.
///
/// Only settings that differ from librdkafka defaults.
///
/// | Setting | Value | librdkafka default | Why |
/// |---|---|---|---|
/// | `partition.assignment.strategy` | `cooperative-sticky` | `range,roundrobin` | Consistent across envs |
/// | `fetch.wait.max.ms` | 10 ms | 500 ms | Return quickly |
/// | `queued.min.messages` | 1000 | 100000 | Smaller pre-fetch queue |
/// | `enable.auto.commit` | false | true | the data plane manages commits |
/// | `reconnect.backoff.ms` | 10 ms | 100 ms | Fast reconnect |
/// | `reconnect.backoff.max.ms` | 100 ms | 10000 ms | Cap quickly |
/// | `statistics.interval.ms` | 1000 ms | 0 | Enable metrics |
pub const LOW_LATENCY_CONSUMER_DEFAULTS: &[(&str, &str)] = &[
    ("partition.assignment.strategy", "cooperative-sticky"),
    ("fetch.wait.max.ms", "10"),
    ("queued.min.messages", "1000"),
    ("enable.auto.commit", "false"),
    ("reconnect.backoff.ms", "10"),
    ("reconnect.backoff.max.ms", "100"),
    ("statistics.interval.ms", "1000"),
];

// ============================================================================
// Configuration Struct
// ============================================================================

/// Kafka transport configuration.
///
/// Uses a profile-based system where profiles provide opinionated defaults,
/// and `librdkafka_overrides` allows overriding any setting.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[allow(clippy::struct_excessive_bools)] // Kafka config legitimately has many boolean flags
pub struct KafkaConfig {
    /// Configuration profile (production, devtest).
    ///
    /// The profile provides baseline librdkafka settings optimized for the use case.
    /// Use `librdkafka_overrides` to customize specific settings.
    #[serde(default)]
    pub profile: KafkaProfile,

    /// Kafka broker addresses.
    #[serde(default = "default_brokers")]
    pub brokers: Vec<String>,

    /// Consumer group ID.
    ///
    /// Empty marks a producer-only transport: it subscribes to nothing, even
    /// when `topics` is set. scalo's own consumer clients (the admin's
    /// offset-query consumer, the idle consumer of a producer-only transport)
    /// take a group id derived from this field, or from `client_id` when it is
    /// empty, so a broker granting groups by prefix covers them too.
    #[serde(default = "default_group")]
    pub group: String,

    /// Client ID for identification in broker logs.
    #[serde(default = "default_client_id")]
    pub client_id: String,

    /// Rack (availability zone) this client runs in, for fetch-from-follower.
    ///
    /// librdkafka: `client.rack` (KIP-392). When it matches a replica's
    /// `broker.rack` the consumer fetches from that replica instead of the
    /// partition leader, which halves cross-AZ traffic on a three-AZ MSK or
    /// Strimzi cluster and removes its inter-AZ charge. `None` (the default)
    /// leaves the property unset and every fetch goes to the leader.
    ///
    /// Wire it from the platform's own zone label -- the K8s
    /// `topology.kubernetes.io/zone` node label via the downward API, or the
    /// EC2 instance's availability zone -- into `KAFKA_CLIENT_RACK`. The
    /// broker side has to be configured for it too: replicas need `broker.rack`
    /// set and the topic needs `min.insync.replicas` satisfied in-zone.
    #[serde(default)]
    pub client_rack: Option<String>,

    /// Static group membership id (`group.instance.id`). Opt-in: `None` (the
    /// default) uses dynamic membership.
    ///
    /// Set this to a STABLE, UNIQUE-per-replica value (the K8s pod name is the
    /// canonical choice -- e.g. from `$HOSTNAME` / the downward API) to take
    /// static membership (KIP-345). A static member that restarts rejoins with
    /// its prior partitions WITHOUT triggering a group-wide rebalance, which
    /// turns a rolling restart of a large consumer fleet from dozens of
    /// stop-the-world rebalances into zero. Two replicas sharing one value get
    /// fenced -- the value MUST be unique per replica.
    #[serde(default)]
    pub group_instance_id: Option<String>,

    /// Consumer-group rebalance protocol to join with (default: `consumer`,
    /// KIP-848).
    ///
    /// Not every broker speaks it, so the resolved value is
    /// [`effective_consumer_protocol`](Self::effective_consumer_protocol) --
    /// a provider known not to implement KIP-848 is forced to `classic`, and
    /// a broker that refuses it at join time drops the transport back to
    /// `classic` once, with a warning. Set `classic` to opt out entirely.
    #[serde(default)]
    pub consumer_protocol: ConsumerProtocol,

    /// How long construction waits for the broker to accept
    /// `group.protocol=consumer` before rebuilding the consumer as `classic`,
    /// in milliseconds.
    ///
    /// The wait ends as soon as librdkafka's statistics report the group `up`,
    /// so on a broker that does speak KIP-848 it costs one
    /// `statistics.interval.ms` (1 s on the shipped profiles) rather than the
    /// whole window. A broker that is simply unreachable at startup also
    /// exhausts the window and falls back -- classic works everywhere, so the
    /// cost of that misfire is a warning line.
    ///
    /// `0` disables the probe: the requested protocol is used as-is with no
    /// fallback. Only a subscribing consumer probes at all, since a
    /// producer-only transport joins no group.
    #[serde(default = "default_consumer_protocol_probe_ms")]
    pub consumer_protocol_probe_ms: u64,

    /// Topics to subscribe to. Ignored when `group` is empty.
    #[serde(default)]
    pub topics: Vec<String>,

    /// Enable auto-discovery when `topics` is empty.
    /// When false (default), empty `topics` means no subscription (producer-only).
    /// When true, empty `topics` triggers broker auto-discovery with
    /// `topic_include`/`topic_exclude` filters and suppression rules.
    #[serde(default)]
    pub auto_discover: bool,

    /// Regex patterns for topic include filtering (empty = accept all).
    /// Topics must match at least one pattern (OR logic).
    #[serde(default)]
    pub topic_include: Vec<String>,

    /// Regex patterns for topic exclude filtering.
    /// Topics matching any pattern are excluded. Exclude wins over include.
    /// Default: `["^__", "_dlq$"]` -- Kafka internal topics and the DFE
    /// standard's dead-letter topics (a DLQ consumer sets its own include).
    #[serde(default = "default_topic_exclude")]
    pub topic_exclude: Vec<String>,

    /// Periodic topic refresh interval in seconds (0 = disabled).
    /// Only applies when `topics` is empty (auto-discovery mode).
    #[serde(default = "default_topic_refresh_secs")]
    pub topic_refresh_secs: u64,

    /// Suppression rules: if a topic with preferred_suffix exists,
    /// suppress the topic with suppressed_suffix for the same base name.
    /// Default: _load suppresses _land (the convention).
    #[serde(default = "default_topic_suppression_rules")]
    pub topic_suppression_rules: Vec<SuppressionRule>,

    /// Security protocol (plaintext, ssl, sasl_plaintext, sasl_ssl).
    #[serde(default = "default_security_protocol")]
    pub security_protocol: String,

    /// Optional managed-Kafka provider name (strimzi / redpanda / confluent-cloud /
    /// redpanda-cloud / msk / plaintext). When set, DERIVES security_protocol +
    /// sasl_mechanism from the opt-in provider presets -- the caller does NOT
    /// hand-set them. Applied by [`apply_provider`](KafkaConfig::apply_provider)
    /// before validate(). See [`providers`](super::providers).
    #[serde(default)]
    pub provider: Option<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER).
    #[serde(default)]
    pub sasl_mechanism: Option<String>,

    /// SASL username.
    #[serde(default)]
    pub sasl_username: Option<String>,

    /// SASL password.
    #[serde(default)]
    pub sasl_password: Option<crate::SensitiveString>,

    // --- TLS Configuration ---
    //
    // Kafka TLS is configured by FILE PATH because librdkafka (the C library)
    // owns the TLS stack and reads PEM files directly -- it does not accept a
    // Rust rustls `ClientConfig`, so the unified `crate::tls` module does not
    // apply here. The mapping from the unified `TlsTrust` vocabulary is:
    //   TlsTrust.extra_roots (private CA, single bundle ok) -> ssl_ca_location
    //   client cert / key (mTLS)                             -> ssl_certificate_location / ssl_key_location
    //   native/webpki roots                                  -> librdkafka uses the system store by default (omit ssl_ca_location)
    //   exclusive private-CA pin                             -> set ssl_ca_location to the private CA only
    /// SSL CA certificate file path (private-CA bundle; maps to
    /// `TlsTrust.extra_roots` -- a single combined root+intermediate PEM is
    /// accepted).
    #[serde(default)]
    pub ssl_ca_location: Option<String>,

    /// SSL client certificate file path (mTLS).
    #[serde(default)]
    pub ssl_certificate_location: Option<String>,

    /// SSL client key file path (mTLS).
    #[serde(default)]
    pub ssl_key_location: Option<String>,

    /// Skip SSL certificate verification.
    ///
    /// Automatically enabled for `devtest` profile.
    #[serde(default)]
    pub ssl_skip_verify: bool,

    /// Deliberately permit an unencrypted transport (`plaintext` /
    /// `sasl_plaintext`) in production. Default `false`: production
    /// [`validate`](KafkaConfig::validate) rejects unencrypted transports so a
    /// misconfiguration cannot ship data/credentials in the clear. Set `true`
    /// only for an audited case (e.g. mesh-encrypted in-cluster traffic).
    #[serde(default)]
    pub allow_insecure_transport: bool,

    // --- Consumer Settings (explicit fields for common options) ---
    /// Enable auto-commit (default: false for manual commit).
    #[serde(default)]
    pub enable_auto_commit: bool,

    /// Auto-commit interval in milliseconds.
    #[serde(default = "default_auto_commit_interval")]
    pub auto_commit_interval_ms: u32,

    /// Session timeout in milliseconds.
    #[serde(default = "default_session_timeout")]
    pub session_timeout_ms: u32,

    /// Heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u32,

    /// Maximum poll interval in milliseconds.
    #[serde(default = "default_max_poll_interval")]
    pub max_poll_interval_ms: u32,

    /// Fetch minimum bytes.
    #[serde(default = "default_fetch_min_bytes")]
    pub fetch_min_bytes: i32,

    /// Fetch maximum bytes.
    #[serde(default = "default_fetch_max_bytes")]
    pub fetch_max_bytes: i32,

    /// Maximum messages per partition per poll.
    #[serde(default = "default_max_partition_fetch_bytes")]
    pub max_partition_fetch_bytes: i32,

    /// Auto offset reset (earliest, latest, none).
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,

    /// Enable partition EOF events.
    #[serde(default)]
    pub enable_partition_eof: bool,

    /// Kafka sizing surface: profile + named knobs + raw librdkafka escape hatch.
    ///
    /// Controls the byte-budget and latency envelope for GET (consumer) and
    /// SEND (producer) paths. See [`KafkaSizingConfig`] for full documentation.
    ///
    /// Example YAML:
    /// ```yaml
    /// kafka:
    ///   sizing:
    ///     profile: throughput
    ///     consumer:
    ///       fetch_min_bytes: 2097152  # 2 MiB, overrides profile default
    ///     producer:
    ///       compression_type: lz4     # the default is zstd at level 3
    ///     consumer_librdkafka:
    ///       fetch.wait.max.ms: "75"   # raw override wins over the knobs
    ///     producer_librdkafka:
    ///       linger.ms: "50"
    /// ```
    #[serde(default)]
    pub sizing: KafkaSizingConfig,

    /// Librdkafka configuration overrides.
    ///
    /// These settings override the profile defaults, the explicit config
    /// fields and the whole sizing surface: every producer and consumer path
    /// applies them after all three. Use this to customize any librdkafka
    /// setting not exposed as an explicit field.
    ///
    /// Example:
    /// ```yaml
    /// librdkafka_overrides:
    ///   statistics.interval.ms: "5000"
    ///   fetch.min.bytes: "2097152"
    /// ```
    #[serde(default)]
    pub librdkafka_overrides: HashMap<String, String>,

    /// Legacy field - use `librdkafka_overrides` instead.
    #[serde(default)]
    #[deprecated(since = "1.3.0", note = "Use `librdkafka_overrides` instead")]
    pub extra_config: HashMap<String, String>,

    /// Inbound message filters (applied on recv before caller sees messages).
    #[serde(default)]
    pub filters_in: Vec<crate::transport::filter::FilterRule>,

    /// Outbound message filters (applied on send before transport dispatches).
    #[serde(default)]
    pub filters_out: Vec<crate::transport::filter::FilterRule>,
}

fn default_topic_exclude() -> Vec<String> {
    // `_dlq$`: the DFE DLQ standard pre-creates per-app dead-letter topics;
    // auto-discovery must never feed dead letters back into a data path.
    vec!["^__".to_string(), "_dlq$".to_string()]
}

fn default_topic_refresh_secs() -> u64 {
    60
}

fn default_topic_suppression_rules() -> Vec<SuppressionRule> {
    vec![SuppressionRule {
        preferred_suffix: "_load".into(),
        suppressed_suffix: "_land".into(),
    }]
}

fn default_brokers() -> Vec<String> {
    vec!["localhost:9092".to_string()]
}

fn default_group() -> String {
    "scalo-consumer".to_string()
}

fn default_client_id() -> String {
    "scalo".to_string()
}

fn default_security_protocol() -> String {
    "plaintext".to_string()
}

fn default_auto_commit_interval() -> u32 {
    5000
}

fn default_session_timeout() -> u32 {
    45000
}

fn default_heartbeat_interval() -> u32 {
    3000
}

fn default_max_poll_interval() -> u32 {
    300_000
}

fn default_fetch_min_bytes() -> i32 {
    1
}

fn default_fetch_max_bytes() -> i32 {
    52_428_800 // 50 MB
}

fn default_max_partition_fetch_bytes() -> i32 {
    1_048_576 // 1 MB
}

fn default_auto_offset_reset() -> String {
    "earliest".to_string()
}

fn default_consumer_protocol_probe_ms() -> u64 {
    5000
}

impl Default for KafkaConfig {
    fn default() -> Self {
        #[allow(deprecated)]
        Self {
            profile: KafkaProfile::default(),
            brokers: default_brokers(),
            group: default_group(),
            client_id: default_client_id(),
            client_rack: None,
            group_instance_id: None,
            consumer_protocol: ConsumerProtocol::default(),
            consumer_protocol_probe_ms: default_consumer_protocol_probe_ms(),
            topics: Vec::new(),
            auto_discover: false,
            topic_include: Vec::new(),
            topic_exclude: default_topic_exclude(),
            topic_refresh_secs: default_topic_refresh_secs(),
            topic_suppression_rules: default_topic_suppression_rules(),
            security_protocol: default_security_protocol(),
            provider: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_skip_verify: false,
            allow_insecure_transport: false,
            enable_auto_commit: false,
            auto_commit_interval_ms: default_auto_commit_interval(),
            session_timeout_ms: default_session_timeout(),
            heartbeat_interval_ms: default_heartbeat_interval(),
            max_poll_interval_ms: default_max_poll_interval(),
            fetch_min_bytes: default_fetch_min_bytes(),
            fetch_max_bytes: default_fetch_max_bytes(),
            max_partition_fetch_bytes: default_max_partition_fetch_bytes(),
            auto_offset_reset: default_auto_offset_reset(),
            enable_partition_eof: false,
            sizing: KafkaSizingConfig::default(),
            librdkafka_overrides: HashMap::new(),
            extra_config: HashMap::new(),
            filters_in: Vec::new(),
            filters_out: Vec::new(),
        }
    }
}

impl KafkaConfig {
    /// Create a config with the production profile.
    #[must_use]
    pub fn production() -> Self {
        Self {
            profile: KafkaProfile::Production,
            ..Default::default()
        }
    }

    /// Create a config with the devtest profile.
    ///
    /// Automatically enables SSL skip verify.
    #[must_use]
    pub fn devtest() -> Self {
        Self {
            profile: KafkaProfile::DevTest,
            ssl_skip_verify: true,
            ..Default::default()
        }
    }

    /// Create a minimal config for testing.
    #[must_use]
    pub fn for_testing(brokers: &str, group: &str, topics: Vec<String>) -> Self {
        Self {
            profile: KafkaProfile::DevTest,
            brokers: vec![brokers.to_string()],
            group: group.to_string(),
            topics,
            ssl_skip_verify: true,
            ..Default::default()
        }
    }

    /// Set the configuration profile.
    #[must_use]
    pub fn with_profile(mut self, profile: KafkaProfile) -> Self {
        self.profile = profile;
        if profile == KafkaProfile::DevTest {
            self.ssl_skip_verify = true;
        }
        self
    }

    /// Get the profile's librdkafka defaults.
    #[must_use]
    pub fn profile_defaults(&self) -> &'static [(&'static str, &'static str)] {
        match self.profile {
            KafkaProfile::Production => PRODUCTION_PROFILE,
            KafkaProfile::DevTest => DEVTEST_PROFILE,
        }
    }

    /// Build the final librdkafka config map.
    ///
    /// Order of precedence (lowest to highest):
    /// 1. Profile defaults
    /// 2. Explicit config fields (fetch_min_bytes, etc.)
    /// 3. `extra_config` (legacy, deprecated)
    /// 4. `librdkafka_overrides` (highest priority)
    #[must_use]
    #[allow(deprecated)]
    pub fn build_librdkafka_config(&self) -> HashMap<String, String> {
        let mut config = HashMap::new();

        // 1. Profile defaults.
        for (key, value) in self.profile_defaults() {
            config.insert((*key).to_string(), (*value).to_string());
        }

        // 2. Explicit config fields are applied directly by the transport layer.

        // 3. Legacy extra_config.
        for (key, value) in &self.extra_config {
            config.insert(key.clone(), value.clone());
        }

        // 4. librdkafka_overrides (highest priority).
        for (key, value) in &self.librdkafka_overrides {
            config.insert(key.clone(), value.clone());
        }

        config
    }

    /// The producer settings every producer path applies after its profile
    /// constants: the sizing surface, then `librdkafka_overrides`, which wins.
    pub(crate) fn resolved_producer_settings(&self) -> BTreeMap<String, String> {
        self.sizing
            .producer_map_under(raw_layer(&self.librdkafka_overrides))
    }

    /// Add a librdkafka override.
    ///
    /// This has the highest priority and will override profile defaults.
    #[must_use]
    pub fn with_override(mut self, key: &str, value: &str) -> Self {
        self.librdkafka_overrides
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Add multiple librdkafka overrides.
    #[must_use]
    pub fn with_overrides(mut self, overrides: &[(&str, &str)]) -> Self {
        for (key, value) in overrides {
            self.librdkafka_overrides
                .insert((*key).to_string(), (*value).to_string());
        }
        self
    }

    // ========================================================================
    // Authentication Methods
    // ========================================================================

    /// Create a config with SASL/SCRAM authentication.
    #[must_use]
    pub fn with_scram(mut self, mechanism: &str, username: &str, password: &str) -> Self {
        self.security_protocol = "sasl_plaintext".to_string();
        self.sasl_mechanism = Some(mechanism.to_string());
        self.sasl_username = Some(username.to_string());
        self.sasl_password = Some(crate::SensitiveString::new(password));
        self
    }

    /// Create a config with SASL/SSL authentication.
    #[must_use]
    pub fn with_scram_ssl(mut self, mechanism: &str, username: &str, password: &str) -> Self {
        self.security_protocol = "sasl_ssl".to_string();
        self.sasl_mechanism = Some(mechanism.to_string());
        self.sasl_username = Some(username.to_string());
        self.sasl_password = Some(crate::SensitiveString::new(password));
        self
    }

    /// Add TLS configuration.
    #[must_use]
    pub fn with_tls(mut self, ca_location: Option<&str>) -> Self {
        if self.security_protocol == "plaintext" {
            self.security_protocol = "ssl".to_string();
        } else if self.security_protocol == "sasl_plaintext" {
            self.security_protocol = "sasl_ssl".to_string();
        }
        self.ssl_ca_location = ca_location.map(String::from);
        self
    }

    /// Add client certificate for mutual TLS.
    #[must_use]
    pub fn with_client_cert(mut self, cert_location: &str, key_location: &str) -> Self {
        self.ssl_certificate_location = Some(cert_location.to_string());
        self.ssl_key_location = Some(key_location.to_string());
        self
    }

    /// Skip SSL certificate verification.
    ///
    /// **WARNING**: Only use in development/test environments! Rejected in
    /// production by [`validate`](Self::validate).
    #[must_use]
    pub fn with_ssl_skip_verify(mut self) -> Self {
        self.ssl_skip_verify = true;
        self
    }

    /// Resolve the provider preset: if [`provider`](Self::provider) is set, derive
    /// `security_protocol` + `sasl_mechanism` from it (never hand-set). No-op when
    /// `provider` is `None`. Call BEFORE [`validate`](Self::validate).
    ///
    /// # Errors
    /// Returns `Err` if `provider` names an unknown provider.
    pub fn apply_provider(&mut self) -> Result<(), String> {
        use super::providers::{KafkaProvider, KnownProvider};
        let Some(name) = self.provider.clone() else {
            return Ok(());
        };
        KnownProvider::parse(&name)?.apply_auth(self);
        Ok(())
    }

    /// The consumer-group protocol this config will actually join with.
    ///
    /// A provider whose brokers do not implement KIP-848 is forced to
    /// [`ConsumerProtocol::Classic`] here, so it never pays the startup probe
    /// to learn what is already known. An unrecognised provider name is left
    /// alone -- [`apply_provider`](Self::apply_provider) is what rejects it.
    #[must_use]
    pub fn effective_consumer_protocol(&self) -> ConsumerProtocol {
        use super::providers::{KafkaProvider, KnownProvider};

        if self.consumer_protocol == ConsumerProtocol::Classic {
            return ConsumerProtocol::Classic;
        }
        match self.provider.as_deref().map(KnownProvider::parse) {
            Some(Ok(provider)) if !provider.supports_consumer_group_protocol() => {
                ConsumerProtocol::Classic
            }
            _ => ConsumerProtocol::Consumer,
        }
    }

    /// The group id for one of scalo's own consumer clients, `role` naming it.
    ///
    /// librdkafka queries the group coordinator for any consumer carrying a
    /// `group.id`, and a broker that grants groups by prefix refuses a fixed
    /// literal, so the id is `<group>-<role>`, falling back to
    /// `<client_id>-<role>` for a producer-only config.
    pub(crate) fn internal_group_id(&self, role: &str) -> String {
        let anchor = [&self.group, &self.client_id]
            .into_iter()
            .find(|field| !field.is_empty())
            .cloned()
            .unwrap_or_else(default_client_id);
        format!("{anchor}-{role}")
    }

    /// Validate the Kafka config against the deployment profile.
    ///
    /// `ssl_skip_verify` disables TLS certificate verification (MITM-exposed),
    /// and is set by the `devtest`/`for_testing` profiles by design. It is
    /// permitted only in dev/test; under a production profile this returns an
    /// error. Call at startup with [`crate::env::is_production`].
    ///
    /// NOTE: `ssl_skip_verify` is slated for removal at GA -- supply the broker
    /// CA via `ssl_ca_location` (private-CA trust) instead.
    ///
    /// # Errors
    ///
    /// Returns `Err` (in ANY environment) when `sasl_mechanism` is `PLAIN` but the
    /// transport is not `sasl_ssl` -- a PLAIN password must never cross a plaintext
    /// transport. Additionally, when `is_production`, returns `Err` if
    /// `ssl_skip_verify` is set, or an unencrypted transport
    /// (`plaintext`/`sasl_plaintext`) is configured without the explicit
    /// `allow_insecure_transport` opt-in.
    pub fn validate(&self, is_production: bool) -> Result<(), String> {
        // Universal floor (dev AND prod): PLAIN sends the password in cleartext,
        // so it MUST ride an encrypted transport (sasl_ssl). SCRAM challenges are
        // safe over a plaintext transport, so only PLAIN is gated here. Mirrors the
        // opt-in provider presets + the Python contract (dfe-engine#98).
        if self.sasl_mechanism.as_deref() == Some("PLAIN")
            && !self.security_protocol.eq_ignore_ascii_case("sasl_ssl")
        {
            return Err(format!(
                "kafka: SASL PLAIN requires security_protocol=sasl_ssl (got '{}') -- \
                 never send a PLAIN password over a plaintext transport",
                self.security_protocol
            ));
        }
        if !is_production {
            return Ok(());
        }
        if self.ssl_skip_verify {
            return Err(
                "kafka: ssl_skip_verify (TLS verification disabled) is not permitted \
                 in production -- configure ssl_ca_location for private-CA trust instead"
                    .to_string(),
            );
        }
        // An unencrypted transport ships data (and SASL/PLAIN credentials) in
        // the clear. Reject in prod unless deliberately opted into.
        let proto = self.security_protocol.to_ascii_lowercase();
        if !self.allow_insecure_transport && (proto == "plaintext" || proto == "sasl_plaintext") {
            return Err(format!(
                "kafka: security_protocol='{}' sends data/credentials unencrypted and is not \
                 permitted in production -- use 'ssl'/'sasl_ssl', or set \
                 allow_insecure_transport=true to deliberately opt in (e.g. mesh-encrypted \
                 in-cluster traffic)",
                self.security_protocol
            ));
        }
        Ok(())
    }

    /// Enable SSL but accept any certificate (for dev/test with self-signed certs).
    #[must_use]
    pub fn with_ssl_insecure(mut self) -> Self {
        if self.security_protocol == "plaintext" {
            self.security_protocol = "ssl".to_string();
        } else if self.security_protocol == "sasl_plaintext" {
            self.security_protocol = "sasl_ssl".to_string();
        }
        self.ssl_skip_verify = true;
        self
    }

    // ========================================================================
    // Convenience Methods (apply common patterns as overrides)
    // ========================================================================

    /// Apply producer defaults.
    #[must_use]
    #[deprecated(since = "2.0.0", note = "Use producer profile constants directly")]
    #[allow(deprecated)]
    pub fn with_producer_defaults(mut self) -> Self {
        for (key, value) in PRODUCER_HIGH_THROUGHPUT {
            self.extra_config
                .entry((*key).to_string())
                .or_insert_with(|| (*value).to_string());
        }
        self
    }

    /// Apply high-throughput consumer defaults.
    #[must_use]
    #[deprecated(since = "2.0.0", note = "Use KafkaConfig::production() instead")]
    #[allow(deprecated)]
    pub fn with_high_throughput(mut self) -> Self {
        for (key, value) in PRODUCTION_PROFILE {
            self.extra_config
                .entry((*key).to_string())
                .or_insert_with(|| (*value).to_string());
        }
        self
    }

    /// Apply low-latency consumer defaults.
    #[must_use]
    #[deprecated(since = "2.0.0", note = "Use LOW_LATENCY_CONSUMER_DEFAULTS directly")]
    #[allow(deprecated)]
    pub fn with_low_latency(mut self) -> Self {
        for (key, value) in LOW_LATENCY_CONSUMER_DEFAULTS {
            self.extra_config
                .entry((*key).to_string())
                .or_insert_with(|| (*value).to_string());
        }
        self
    }

    /// Enable statistics collection at the specified interval.
    #[must_use]
    pub fn with_statistics(mut self, interval_ms: u32) -> Self {
        self.librdkafka_overrides.insert(
            "statistics.interval.ms".to_string(),
            interval_ms.to_string(),
        );
        self
    }

    /// Apply cloud-optimized connection settings.
    #[must_use]
    pub fn with_cloud_connection_tuning(mut self) -> Self {
        let cloud_settings = [
            ("socket.keepalive.enable", "true"),
            ("metadata.max.age.ms", "180000"),
            ("socket.connection.setup.timeout.ms", "30000"),
            ("connections.max.idle.ms", "540000"),
        ];
        for (key, value) in cloud_settings {
            self.librdkafka_overrides
                .entry(key.to_string())
                .or_insert_with(|| value.to_string());
        }
        self
    }

    // ========================================================================
    // Environment Loading
    // ========================================================================

    /// Load configuration from environment variables with prefix.
    ///
    /// Reads environment variables with the given prefix:
    /// - `{PREFIX}_PROFILE` -> profile (production, devtest)
    /// - `{PREFIX}_BOOTSTRAP_SERVERS` -> brokers (legacy: `{PREFIX}_BROKERS`)
    /// - `{PREFIX}_GROUP_ID` -> group
    /// - `{PREFIX}_CLIENT_RACK` -> client_rack (legacy: `{PREFIX}_AVAILABILITY_ZONE`)
    /// - `{PREFIX}_CONSUMER_PROTOCOL` -> consumer_protocol (consumer, classic)
    /// - `{PREFIX}_CONSUMER_PROTOCOL_PROBE_MS` -> consumer_protocol_probe_ms
    /// - `{PREFIX}_PROVIDER` -> provider (derives security_protocol + sasl_mechanism)
    /// - `{PREFIX}_SECURITY_PROTOCOL` -> security_protocol
    /// - `{PREFIX}_SASL_MECHANISM` -> sasl_mechanism
    /// - `{PREFIX}_SASL_USERNAME` -> sasl_username (legacy: `{PREFIX}_SASL_USER`)
    /// - `{PREFIX}_SASL_PASSWORD` -> sasl_password
    /// - `{PREFIX}_SSL_SKIP_VERIFY` -> ssl_skip_verify
    /// - `{PREFIX}_TOPICS` -> topics (comma-separated)
    ///
    /// Also supports standard `KAFKA_*` environment variables as fallback
    /// when using a custom prefix.
    #[cfg(feature = "config")]
    #[must_use]
    pub fn from_env(prefix: &str) -> Self {
        use crate::config::env_compat::EnvVar;

        let mut config = Self::default();

        // Prefixed lookup with legacy aliases, plus KAFKA_* as a final fallback.
        let prefixed = |name: &str, legacy: &[&str]| {
            let mut var = EnvVar::new(&format!("{prefix}_{name}"));
            for l in legacy {
                var = var.with_legacy(&format!("{prefix}_{l}"));
            }
            var = var.with_legacy(&format!("KAFKA_{name}"));
            var
        };

        if let Some(val) = prefixed("PROFILE", &[]).get()
            && let Ok(profile) = val.parse()
        {
            config.profile = profile;
            if config.profile == KafkaProfile::DevTest {
                config.ssl_skip_verify = true;
            }
        }

        if let Some(brokers) = prefixed("BOOTSTRAP_SERVERS", &["BROKERS"]).get_list() {
            config.brokers = brokers;
        }

        if let Some(val) = prefixed("GROUP_ID", &["GROUP", "CONSUMER_GROUP"]).get() {
            config.group = val;
        }

        if let Some(val) = prefixed("CLIENT_ID", &[]).get() {
            config.client_id = val;
        }

        // Fetch-from-follower (KIP-392). Wired from the platform's zone label;
        // an empty value is treated as unset so an unpopulated downward-API
        // variable does not pin every fetch to a rack named "".
        if let Some(val) = prefixed("CLIENT_RACK", &["AVAILABILITY_ZONE"]).get()
            && !val.is_empty()
        {
            config.client_rack = Some(val);
        }

        // Static group membership id (KIP-345). Opt-in; canonically wired from
        // the pod name (e.g. `<PREFIX>_GROUP_INSTANCE_ID` set to the downward
        // API pod name). An empty value is treated as unset.
        if let Some(val) = prefixed("GROUP_INSTANCE_ID", &[]).get()
            && !val.is_empty()
        {
            config.group_instance_id = Some(val);
        }

        // KIP-848 opt-out and the probe window that guards it.
        if let Some(val) = prefixed("CONSUMER_PROTOCOL", &[]).get()
            && let Ok(protocol) = val.parse()
        {
            config.consumer_protocol = protocol;
        }
        if let Some(val) = prefixed("CONSUMER_PROTOCOL_PROBE_MS", &[]).get()
            && let Ok(ms) = val.parse()
        {
            config.consumer_protocol_probe_ms = ms;
        }

        // PROVIDER derives security_protocol + sasl_mechanism at construction (see
        // apply_provider); read it here so KAFKA_PROVIDER wires the abstraction into
        // the env-configured data-plane services.
        if let Some(val) = prefixed("PROVIDER", &[]).get() {
            config.provider = Some(val);
        }
        if let Some(val) = prefixed("SECURITY_PROTOCOL", &[]).get() {
            config.security_protocol = val;
        }

        if let Some(val) = prefixed("SASL_MECHANISM", &[]).get() {
            config.sasl_mechanism = Some(val);
        }

        if let Some(val) = prefixed("SASL_USERNAME", &["SASL_USER"]).get() {
            config.sasl_username = Some(val);
        }

        if let Some(val) = prefixed("SASL_PASSWORD", &[]).get() {
            config.sasl_password = Some(crate::SensitiveString::from(val));
        }

        if let Some(val) = prefixed("SSL_CA_LOCATION", &["CA_CERT", "SSL_CA"]).get() {
            config.ssl_ca_location = Some(val);
        }

        if let Some(val) = prefixed("SSL_SKIP_VERIFY", &["SSL_INSECURE", "INSECURE"]).get_bool() {
            config.ssl_skip_verify = val;
        }

        if let Some(topics) = prefixed("TOPICS", &["TOPIC"]).get_list() {
            config.topics = topics;
        }

        config
    }

    /// Load configuration from standard `KAFKA_*` environment variables.
    ///
    /// This is a convenience method that uses the standard Kafka prefix.
    /// Supports legacy aliases with deprecation warnings.
    ///
    /// Standard variables:
    /// - `KAFKA_BOOTSTRAP_SERVERS` (legacy: `KAFKA_BROKERS`)
    /// - `KAFKA_SASL_USERNAME` (legacy: `KAFKA_SASL_USER`)
    /// - `KAFKA_SECURITY_PROTOCOL`
    /// - `KAFKA_SASL_MECHANISM`
    /// - `KAFKA_SASL_PASSWORD`
    /// - `KAFKA_SSL_SKIP_VERIFY`
    /// - `KAFKA_TOPICS`
    /// - `KAFKA_GROUP_ID`
    /// - `KAFKA_CLIENT_ID`
    /// - `KAFKA_CLIENT_RACK`
    /// - `KAFKA_CONSUMER_PROTOCOL`
    /// - `KAFKA_PROFILE`
    #[cfg(feature = "config")]
    #[must_use]
    pub fn from_env_standard() -> Self {
        Self::from_env("KAFKA")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A DFE broker grants consumer groups by the `dfe-` prefix, so an
    /// internal client's group id has to start with whatever the app is
    /// already granted, never with a literal of scalo's own.
    #[test]
    fn internal_group_ids_share_the_app_prefix() {
        let consumer = KafkaConfig {
            group: "dfe-loader".to_string(),
            client_id: "dfe-loader".to_string(),
            ..Default::default()
        };
        assert_eq!(consumer.internal_group_id("admin"), "dfe-loader-admin");

        // A producer-only config has no group, so the client id anchors it.
        let producer = KafkaConfig {
            group: String::new(),
            client_id: "dfe-fetcher".to_string(),
            ..Default::default()
        };
        assert_eq!(
            producer.internal_group_id("producer-only"),
            "dfe-fetcher-producer-only"
        );

        // The group wins over the client id when both are set.
        let both = KafkaConfig {
            group: "dfe-archiver".to_string(),
            client_id: "archiver-pod-7".to_string(),
            ..Default::default()
        };
        assert_eq!(both.internal_group_id("admin"), "dfe-archiver-admin");
    }

    /// With both identifying fields cleared the id is still non-empty --
    /// librdkafka refuses to build a consumer with an empty `group.id`.
    #[test]
    fn internal_group_id_falls_back_to_the_default_client_id() {
        let bare = KafkaConfig {
            group: String::new(),
            client_id: String::new(),
            ..Default::default()
        };
        assert_eq!(
            bare.internal_group_id("admin"),
            format!("{}-admin", default_client_id())
        );
    }

    #[test]
    fn validate_rejects_ssl_skip_verify_in_production() {
        // devtest sets ssl_skip_verify -- fine in dev, rejected in prod.
        let dev = KafkaConfig::devtest();
        assert!(dev.ssl_skip_verify);
        assert!(dev.validate(false).is_ok(), "dev allows skip_verify");
        assert!(
            dev.validate(true).is_err(),
            "production must reject ssl_skip_verify"
        );

        // A TLS-verifying config validates in production.
        let prod = KafkaConfig {
            security_protocol: "ssl".to_string(),
            ..Default::default()
        };
        assert!(!prod.ssl_skip_verify);
        assert!(prod.validate(true).is_ok());
    }

    #[test]
    fn validate_rejects_unencrypted_transport_in_production() {
        // Default is plaintext -> rejected in prod, allowed in dev.
        let cfg = KafkaConfig::default();
        assert_eq!(cfg.security_protocol, "plaintext");
        assert!(cfg.validate(false).is_ok(), "dev allows plaintext");
        assert!(
            cfg.validate(true).is_err(),
            "production must reject plaintext transport"
        );

        // sasl_plaintext is likewise rejected in prod.
        let sasl = KafkaConfig {
            security_protocol: "sasl_plaintext".to_string(),
            ..Default::default()
        };
        assert!(sasl.validate(true).is_err());

        // The explicit, auditable override permits it (e.g. mesh-encrypted).
        let opted_in = KafkaConfig {
            security_protocol: "plaintext".to_string(),
            allow_insecure_transport: true,
            ..Default::default()
        };
        assert!(
            opted_in.validate(true).is_ok(),
            "allow_insecure_transport opts into plaintext in prod"
        );
    }

    #[test]
    fn validate_refuses_plain_over_plaintext_in_any_env() {
        // The universal floor: PLAIN sends the password in cleartext, so it must
        // ride sasl_ssl -- rejected even in dev (is_production=false).
        let plain_plaintext = KafkaConfig {
            security_protocol: "sasl_plaintext".to_string(),
            sasl_mechanism: Some("PLAIN".to_string()),
            ..Default::default()
        };
        assert!(
            plain_plaintext.validate(false).is_err(),
            "dev must still reject PLAIN over plaintext"
        );
        assert!(plain_plaintext.validate(true).is_err());

        // PLAIN over sasl_ssl is fine (the Confluent Cloud shape).
        let plain_tls = KafkaConfig {
            security_protocol: "sasl_ssl".to_string(),
            sasl_mechanism: Some("PLAIN".to_string()),
            ..Default::default()
        };
        assert!(plain_tls.validate(false).is_ok());
        assert!(plain_tls.validate(true).is_ok());

        // SCRAM over sasl_plaintext stays allowed in dev (challenge-based, no
        // cleartext password on the wire).
        let scram_plaintext = KafkaConfig {
            security_protocol: "sasl_plaintext".to_string(),
            sasl_mechanism: Some("SCRAM-SHA-512".to_string()),
            ..Default::default()
        };
        assert!(
            scram_plaintext.validate(false).is_ok(),
            "SCRAM over plaintext is ok in dev"
        );
    }

    #[test]
    fn apply_provider_derives_from_preset() {
        let mut cfg = KafkaConfig {
            provider: Some("confluent-cloud".to_string()),
            ..Default::default()
        };
        cfg.apply_provider().unwrap();
        assert_eq!(cfg.security_protocol, "sasl_ssl");
        assert_eq!(cfg.sasl_mechanism.as_deref(), Some("PLAIN"));
    }

    #[test]
    fn apply_provider_is_a_noop_without_a_provider() {
        let mut cfg = KafkaConfig::default();
        cfg.apply_provider().unwrap();
        assert_eq!(cfg.security_protocol, "plaintext");
        assert_eq!(cfg.sasl_mechanism, None);
    }

    #[test]
    fn apply_provider_rejects_an_unknown_provider() {
        let mut cfg = KafkaConfig {
            provider: Some("kinesis".to_string()),
            ..Default::default()
        };
        assert!(cfg.apply_provider().is_err());
    }

    /// `client.rack` is opt-in: unset means every fetch goes to the leader,
    /// which is what a single-AZ or unlabelled deployment wants.
    #[cfg(feature = "config")]
    #[test]
    fn client_rack_is_unset_unless_the_environment_names_one() {
        assert_eq!(KafkaConfig::default().client_rack, None);

        temp_env::with_var("KAFKA_CLIENT_RACK", Some("ap-southeast-2a"), || {
            assert_eq!(
                KafkaConfig::from_env("KAFKA").client_rack.as_deref(),
                Some("ap-southeast-2a")
            );
        });

        // An unpopulated downward-API variable must not pin every fetch to a
        // rack named "".
        temp_env::with_var("KAFKA_CLIENT_RACK", Some(""), || {
            assert_eq!(KafkaConfig::from_env("KAFKA").client_rack, None);
        });
    }

    // =========================================================================
    // Consumer group protocol (KIP-848)
    // =========================================================================

    /// KIP-848 is on by default; the whole point of the change is that nobody
    /// has to opt in.
    #[test]
    fn consumer_protocol_defaults_to_kip_848() {
        let cfg = KafkaConfig::default();
        assert_eq!(cfg.consumer_protocol, ConsumerProtocol::Consumer);
        assert_eq!(
            cfg.effective_consumer_protocol(),
            ConsumerProtocol::Consumer
        );
        assert_eq!(ConsumerProtocol::Consumer.as_str(), "consumer");
        assert_eq!(ConsumerProtocol::Classic.as_str(), "classic");
    }

    /// Redpanda implements neither KIP-848 nor KIP-932, so naming it as the
    /// provider resolves to classic without paying the startup probe.
    #[test]
    fn redpanda_resolves_to_classic() {
        for provider in ["redpanda", "redpanda-cloud"] {
            let cfg = KafkaConfig {
                provider: Some(provider.to_string()),
                ..Default::default()
            };
            assert_eq!(
                cfg.effective_consumer_protocol(),
                ConsumerProtocol::Classic,
                "{provider} answers no ConsumerGroupHeartbeat"
            );
        }
        for provider in ["strimzi", "msk", "confluent-cloud", "plaintext"] {
            let cfg = KafkaConfig {
                provider: Some(provider.to_string()),
                ..Default::default()
            };
            assert_eq!(
                cfg.effective_consumer_protocol(),
                ConsumerProtocol::Consumer,
                "{provider} is Kafka-protocol"
            );
        }
    }

    /// An explicit `classic` wins over the provider gate: it is the opt-out.
    #[test]
    fn explicit_classic_is_never_upgraded() {
        let cfg = KafkaConfig {
            consumer_protocol: ConsumerProtocol::Classic,
            provider: Some("strimzi".to_string()),
            ..Default::default()
        };
        assert_eq!(cfg.effective_consumer_protocol(), ConsumerProtocol::Classic);
    }

    /// An unknown provider is `apply_provider`'s error to raise, not a reason
    /// to silently downgrade the protocol here.
    #[test]
    fn unknown_provider_does_not_downgrade_the_protocol() {
        let cfg = KafkaConfig {
            provider: Some("kinesis".to_string()),
            ..Default::default()
        };
        assert_eq!(
            cfg.effective_consumer_protocol(),
            ConsumerProtocol::Consumer
        );
    }

    #[test]
    fn consumer_protocol_parses_and_serialises_snake_case() {
        assert_eq!(
            "classic".parse::<ConsumerProtocol>().unwrap(),
            ConsumerProtocol::Classic
        );
        assert_eq!(
            "CONSUMER".parse::<ConsumerProtocol>().unwrap(),
            ConsumerProtocol::Consumer
        );
        assert!("eager".parse::<ConsumerProtocol>().is_err());
        assert_eq!(
            serde_json::to_string(&ConsumerProtocol::Classic).unwrap(),
            "\"classic\""
        );
    }

    #[test]
    fn kafka_config_topic_resolution_defaults() {
        let config = KafkaConfig::default();
        assert!(config.topic_include.is_empty());
        assert_eq!(
            config.topic_exclude,
            vec!["^__".to_string(), "_dlq$".to_string()]
        );
        assert!(!config.auto_discover);
        assert_eq!(config.topic_refresh_secs, 60);
        assert_eq!(config.topic_suppression_rules.len(), 1);
        assert_eq!(config.topic_suppression_rules[0].preferred_suffix, "_load");
        assert_eq!(config.topic_suppression_rules[0].suppressed_suffix, "_land");
    }

    // =========================================================================
    // SelfRegulationProfile + KafkaSizingConfig tests
    // =========================================================================

    /// Helper: build a KafkaSizingConfig with the given profile and no
    /// per-knob overrides or raw maps. Tests the pure profile -> resolved map
    /// path.
    fn sizing_for_profile(profile: SelfRegulationProfile) -> KafkaSizingConfig {
        KafkaSizingConfig {
            profile,
            ..Default::default()
        }
    }

    // --- Consumer knob defaults by profile ---

    #[test]
    fn throughput_profile_consumer_knobs() {
        let s = sizing_for_profile(SelfRegulationProfile::Throughput);
        let map = s.resolved_consumer_map();
        assert_eq!(map["fetch.min.bytes"], "1048576", "1 MiB fetch.min.bytes");
        assert_eq!(map["fetch.wait.max.ms"], "50");
        assert_eq!(
            map["max.partition.fetch.bytes"],
            MESSAGE_MAX_BYTES.to_string(),
            "a partition must yield one maximum-size record in a single fetch"
        );
        assert_eq!(
            map["fetch.max.bytes"], "52428800",
            "50 MiB total -- MSK Express holds the broker's 55 MiB fetch \
             ceiling read-only, so a larger ask can never be honoured"
        );
        assert_eq!(
            s.effective_poll_cap(),
            2000,
            "throughput poll-safety cap = 2000"
        );
    }

    #[test]
    fn low_latency_profile_consumer_knobs() {
        let s = sizing_for_profile(SelfRegulationProfile::LowLatency);
        let map = s.resolved_consumer_map();
        assert_eq!(map["fetch.min.bytes"], "1", "no batching threshold");
        assert_eq!(map["fetch.wait.max.ms"], "5", "return fast");
        assert_eq!(
            map["max.partition.fetch.bytes"],
            MESSAGE_MAX_BYTES.to_string(),
            "low latency still has to be able to fetch a maximum-size record"
        );
        assert_eq!(
            map["fetch.max.bytes"],
            MESSAGE_MAX_BYTES.to_string(),
            "the smallest total budget that can still carry one maximum-size \
             record is the per-partition ceiling itself"
        );
        assert_eq!(s.effective_poll_cap(), 500);
    }

    #[test]
    fn balanced_profile_consumer_knobs() {
        let s = sizing_for_profile(SelfRegulationProfile::Balanced);
        let map = s.resolved_consumer_map();
        // Balanced sits between throughput and low_latency.
        let fmb: i32 = map["fetch.min.bytes"].parse().unwrap();
        let ll_min: i32 = 1;
        let tp_min: i32 = 1_048_576;
        assert!(
            fmb > ll_min && fmb < tp_min,
            "balanced fetch.min.bytes={fmb} should be between low_latency({ll_min}) and throughput({tp_min})"
        );
        assert_eq!(s.effective_poll_cap(), 1000);
    }

    /// The per-partition fetch budget is part of the record-size chain, so it
    /// does not vary by profile: any profile that fetched less than the record
    /// ceiling would stall on a maximum-size record.
    #[test]
    fn every_profile_fetches_a_whole_maximum_size_record() {
        for profile in [
            SelfRegulationProfile::Throughput,
            SelfRegulationProfile::Balanced,
            SelfRegulationProfile::LowLatency,
        ] {
            let map = sizing_for_profile(profile).resolved_consumer_map();
            assert_eq!(
                map["max.partition.fetch.bytes"],
                MESSAGE_MAX_BYTES.to_string(),
                "profile {profile:?} must fetch a whole maximum-size record"
            );
            let total: i32 = map["fetch.max.bytes"].parse().unwrap();
            assert!(
                total >= MESSAGE_MAX_BYTES,
                "profile {profile:?} fetch.max.bytes={total} is below the \
                 record ceiling, so one maximum-size record never fits"
            );
            assert!(
                total <= 52_428_800,
                "profile {profile:?} fetch.max.bytes={total} exceeds 50 MiB, \
                 over the broker's read-only 55 MiB ceiling on MSK Express"
            );
        }
    }

    /// Throughput and low_latency must differ on every key consumer knob.
    #[test]
    fn throughput_vs_low_latency_consumer_differ() {
        let tp = sizing_for_profile(SelfRegulationProfile::Throughput).resolved_consumer_map();
        let ll = sizing_for_profile(SelfRegulationProfile::LowLatency).resolved_consumer_map();
        for key in &["fetch.min.bytes", "fetch.wait.max.ms", "fetch.max.bytes"] {
            assert_ne!(
                tp[*key], ll[*key],
                "throughput and low_latency must differ on {key}"
            );
        }
    }

    // --- Producer knob defaults by profile ---

    #[test]
    fn throughput_profile_producer_knobs() {
        let s = sizing_for_profile(SelfRegulationProfile::Throughput);
        let map = s.resolved_producer_map();
        assert_eq!(map["batch.size"], "131072", "128 KiB batch");
        assert_eq!(map["linger.ms"], "20");
        assert_eq!(map["compression.type"], "zstd");
        assert_eq!(map["compression.level"], "3");
        // 64 MiB -> 65536 KiB
        assert_eq!(map["queue.buffering.max.kbytes"], "65536");
        assert_eq!(map["max.in.flight.requests.per.connection"], "5");
    }

    #[test]
    fn low_latency_profile_producer_knobs() {
        let s = sizing_for_profile(SelfRegulationProfile::LowLatency);
        let map = s.resolved_producer_map();
        assert_eq!(map["linger.ms"], "0", "send immediately");
        assert_eq!(map["compression.type"], "zstd");
        assert_eq!(map["compression.level"], "3");
        let batch: i32 = map["batch.size"].parse().unwrap();
        assert!(batch < 131_072, "low_latency batch should be < throughput");
    }

    /// The record ceiling is a chain-wide constant, so every profile carries
    /// the same value -- a profile that batches differently still has to accept
    /// the same largest record the broker and topic do.
    #[test]
    fn every_profile_sets_the_same_message_max_bytes() {
        for profile in [
            SelfRegulationProfile::Throughput,
            SelfRegulationProfile::Balanced,
            SelfRegulationProfile::LowLatency,
        ] {
            let map = sizing_for_profile(profile).resolved_producer_map();
            assert_eq!(
                map["message.max.bytes"],
                MESSAGE_MAX_BYTES.to_string(),
                "profile {profile:?} must set the 16 MiB record ceiling; \
                 librdkafka's 1,000,000-byte default rejects locally with \
                 MSG_SIZE_TOO_LARGE before the broker is consulted"
            );
        }
    }

    /// The ceiling is a dial, not a hard-coded value.
    #[test]
    fn explicit_message_max_bytes_beats_the_profile() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer: ProducerKnobs {
                message_max_bytes: Some(4_194_304), // 4 MiB
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(s.resolved_producer_map()["message.max.bytes"], "4194304");
    }

    /// Throughput and low_latency must differ on every key producer knob.
    #[test]
    fn throughput_vs_low_latency_producer_differ() {
        let tp = sizing_for_profile(SelfRegulationProfile::Throughput).resolved_producer_map();
        let ll = sizing_for_profile(SelfRegulationProfile::LowLatency).resolved_producer_map();
        for key in &["batch.size", "linger.ms", "queue.buffering.max.kbytes"] {
            assert_ne!(
                tp[*key], ll[*key],
                "throughput and low_latency must differ on {key}"
            );
        }
    }

    // --- Effectively-once: idempotent producer default (v2.10) ---

    /// Idempotence is ON by default and forces acks=all on every profile.
    #[test]
    fn idempotence_on_by_default_forces_acks_all() {
        for profile in [
            SelfRegulationProfile::Throughput,
            SelfRegulationProfile::Balanced,
            SelfRegulationProfile::LowLatency,
        ] {
            let map = sizing_for_profile(profile).resolved_producer_map();
            assert_eq!(
                map["enable.idempotence"], "true",
                "idempotence must default on for {profile:?}"
            );
            assert_eq!(
                map["acks"], "all",
                "idempotence requires acks=all for {profile:?}"
            );
        }
    }

    /// Opting out (idempotence=Some(false)) disables it and does NOT force acks.
    #[test]
    fn idempotence_opt_out_disables_and_leaves_acks() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer: ProducerKnobs {
                idempotence: Some(false),
                ..Default::default()
            },
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(map["enable.idempotence"], "false");
        assert!(
            !map.contains_key("acks"),
            "opt-out must not force acks (leaves librdkafka/profile default)"
        );
    }

    /// Idempotence clamps an over-large max.in.flight to the safe limit of 5.
    #[test]
    fn idempotence_clamps_max_in_flight_to_five() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer: ProducerKnobs {
                max_in_flight: Some(100), // illegal under idempotence
                ..Default::default()
            },
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(
            map["max.in.flight.requests.per.connection"], "5",
            "idempotence must clamp max.in.flight to 5"
        );
    }

    /// The raw escape hatch still wins -- an operator can override acks even
    /// with idempotence on (at their own risk).
    #[test]
    fn raw_override_beats_idempotence_acks() {
        let mut producer_librdkafka = BTreeMap::new();
        producer_librdkafka.insert("acks".to_string(), "1".to_string());
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer_librdkafka,
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(
            map["acks"], "1",
            "raw producer_librdkafka must win over forced acks=all"
        );
    }

    // --- Named knob overrides beat the profile ---

    #[test]
    fn explicit_consumer_knob_beats_profile() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            consumer: ConsumerKnobs {
                fetch_min_bytes: Some(2_097_152), // 2 MiB, not the 1 MiB profile default
                ..Default::default()
            },
            ..Default::default()
        };
        let map = s.resolved_consumer_map();
        assert_eq!(
            map["fetch.min.bytes"], "2097152",
            "explicit override must win over profile default"
        );
        // Other knobs still come from the throughput profile.
        assert_eq!(map["fetch.wait.max.ms"], "50");
    }

    #[test]
    fn explicit_producer_knob_beats_profile() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer: ProducerKnobs {
                linger_ms: Some(99), // override the 20 ms throughput default
                compression_type: Some("lz4".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(map["linger.ms"], "99");
        assert_eq!(map["compression.type"], "lz4");
        assert!(
            !map.contains_key("compression.level"),
            "the zstd level must not follow the codec to lz4"
        );
        // sticky linger tracks the overridden linger_ms.
        assert_eq!(map["sticky.partitioning.linger.ms"], "99");
        // batch.size still comes from the throughput profile.
        assert_eq!(map["batch.size"], "131072");
    }

    // --- Raw escape hatch wins over named knob ---

    #[test]
    fn raw_consumer_librdkafka_wins_over_named_knob() {
        let mut consumer_raw = BTreeMap::new();
        consumer_raw.insert("fetch.min.bytes".to_string(), "9999".to_string());

        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            consumer: ConsumerKnobs {
                fetch_min_bytes: Some(2_097_152), // named knob
                ..Default::default()
            },
            consumer_librdkafka: consumer_raw,
            ..Default::default()
        };
        let map = s.resolved_consumer_map();
        // Raw map must win over both profile AND named knob.
        assert_eq!(
            map["fetch.min.bytes"], "9999",
            "raw consumer_librdkafka must win over named knob"
        );
    }

    #[test]
    fn raw_producer_librdkafka_wins_over_named_knob() {
        let mut producer_raw = BTreeMap::new();
        producer_raw.insert("linger.ms".to_string(), "777".to_string());
        producer_raw.insert("compression.type".to_string(), "gzip".to_string());

        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer: ProducerKnobs {
                linger_ms: Some(20),                       // named knob
                compression_type: Some("lz4".to_string()), // named knob
                ..Default::default()
            },
            producer_librdkafka: producer_raw,
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(
            map["linger.ms"], "777",
            "raw producer_librdkafka linger must win"
        );
        assert_eq!(
            map["compression.type"], "gzip",
            "raw producer_librdkafka compression must win"
        );
        assert!(!map.contains_key("compression.level"));
    }

    // --- KIP-794 / sticky partitioner ---

    #[test]
    fn producer_map_sets_sticky_partitioning_linger() {
        let s = sizing_for_profile(SelfRegulationProfile::Throughput);
        let map = s.resolved_producer_map();
        // sticky.partitioning.linger.ms should be set (not absent).
        assert!(
            map.contains_key("sticky.partitioning.linger.ms"),
            "sticky.partitioning.linger.ms must be present"
        );
        // Its value must match the resolved linger.ms.
        assert_eq!(
            map["sticky.partitioning.linger.ms"], map["linger.ms"],
            "sticky linger must track linger.ms"
        );
        // partitioner must NOT be set by default (we don't override the caller).
        assert!(
            !map.contains_key("partitioner"),
            "producer map must NOT set partitioner to preserve caller's choice"
        );
    }

    // --- the producer codec and its level ---

    /// The profiles differ in batching and latency, never in codec.
    #[test]
    fn every_profile_produces_zstd_at_level_3() {
        for profile in [
            SelfRegulationProfile::Throughput,
            SelfRegulationProfile::Balanced,
            SelfRegulationProfile::LowLatency,
        ] {
            let map = sizing_for_profile(profile).resolved_producer_map();
            assert_eq!(map["compression.type"], "zstd", "profile {profile:?}");
            assert_eq!(map["compression.level"], "3", "profile {profile:?}");
        }
    }

    /// A level means something different to each codec, and lz4 turns to its
    /// slow high-compression mode at 3, so no other codec inherits it.
    #[test]
    fn a_named_codec_other_than_zstd_gets_no_level() {
        for codec in ["lz4", "gzip", "snappy", "none"] {
            let s = KafkaSizingConfig {
                producer: ProducerKnobs {
                    compression_type: Some(codec.to_string()),
                    ..Default::default()
                },
                ..Default::default()
            };
            let map = s.resolved_producer_map();
            assert_eq!(map["compression.type"], codec);
            assert!(
                !map.contains_key("compression.level"),
                "{codec} must run at librdkafka's own level for it"
            );
        }
    }

    /// A raw codec override is the one the producer runs, so the zstd level
    /// goes with the zstd codec it replaced.
    #[test]
    fn a_raw_codec_override_drops_the_zstd_level() {
        let s = KafkaSizingConfig {
            producer_librdkafka: BTreeMap::from([(
                "compression.type".to_string(),
                "lz4".to_string(),
            )]),
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(map["compression.type"], "lz4");
        assert!(!map.contains_key("compression.level"));
    }

    /// A raw layer that picks zstd over a named codec gets the zstd level too.
    #[test]
    fn a_raw_zstd_override_gets_the_zstd_level() {
        let s = KafkaSizingConfig {
            producer: ProducerKnobs {
                compression_type: Some("lz4".to_string()),
                ..Default::default()
            },
            producer_librdkafka: BTreeMap::from([(
                "compression.type".to_string(),
                "zstd".to_string(),
            )]),
            ..Default::default()
        };
        assert_eq!(s.resolved_producer_map()["compression.level"], "3");
    }

    /// A level an operator names is theirs, whatever the codec.
    #[test]
    fn a_raw_level_is_kept() {
        let zstd_six = KafkaSizingConfig {
            producer_librdkafka: BTreeMap::from([(
                "compression.level".to_string(),
                "6".to_string(),
            )]),
            ..Default::default()
        };
        let map = zstd_six.resolved_producer_map();
        assert_eq!(map["compression.type"], "zstd");
        assert_eq!(map["compression.level"], "6");

        let lz4_one = KafkaSizingConfig {
            producer_librdkafka: BTreeMap::from([
                ("compression.type".to_string(), "lz4".to_string()),
                ("compression.level".to_string(), "1".to_string()),
            ]),
            ..Default::default()
        };
        assert_eq!(lz4_one.resolved_producer_map()["compression.level"], "1");
    }

    /// rdkafka hands its settings to librdkafka in hash order, so a raw key
    /// left beside its alias from a lower layer wins only by chance.
    #[test]
    fn a_raw_alias_replaces_the_lower_layer_name() {
        let s = KafkaSizingConfig {
            producer_librdkafka: BTreeMap::from([
                ("compression.codec".to_string(), "lz4".to_string()),
                ("queue.buffering.max.ms".to_string(), "50".to_string()),
            ]),
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(map["compression.codec"], "lz4");
        assert!(!map.contains_key("compression.type"));
        assert!(!map.contains_key("compression.level"));
        assert_eq!(map["queue.buffering.max.ms"], "50");
        assert!(!map.contains_key("linger.ms"));
    }

    /// `librdkafka_overrides` is laid over the sizing raw map, so it wins a
    /// key both set, and it takes the codec level with it.
    #[test]
    fn librdkafka_overrides_win_over_producer_librdkafka() {
        let mut config = KafkaConfig::default();
        config
            .sizing
            .producer_librdkafka
            .insert("compression.type".to_string(), "gzip".to_string());
        config
            .librdkafka_overrides
            .insert("compression.type".to_string(), "lz4".to_string());
        let settings = config.resolved_producer_settings();
        assert_eq!(settings["compression.type"], "lz4");
        assert!(!settings.contains_key("compression.level"));

        // An override by the alias name clears the sizing name below it.
        let mut by_alias = KafkaConfig::default();
        by_alias
            .librdkafka_overrides
            .insert("compression.codec".to_string(), "snappy".to_string());
        let settings = by_alias.resolved_producer_settings();
        assert_eq!(settings["compression.codec"], "snappy");
        assert!(!settings.contains_key("compression.type"));
        assert!(!settings.contains_key("compression.level"));
    }

    /// With no overrides the settings are the sizing surface's own.
    #[test]
    fn resolved_producer_settings_without_overrides_is_the_sizing_map() {
        let config = KafkaConfig::default();
        assert_eq!(
            config.resolved_producer_settings(),
            config.sizing.resolved_producer_map()
        );
    }

    /// The producer profile constants are laid under the sizing surface, so a
    /// batching or codec key in one would read as a setting and never take
    /// effect.
    #[test]
    fn producer_profiles_leave_batching_and_codec_to_the_sizing_surface() {
        const SIZING_OWNED: &[&str] = &[
            "batch.size",
            "linger.ms",
            "queue.buffering.max.ms",
            "compression.type",
            "compression.codec",
            "compression.level",
            "queue.buffering.max.kbytes",
            "message.max.bytes",
            "sticky.partitioning.linger.ms",
        ];
        for (name, profile) in [
            ("PRODUCER_HIGH_THROUGHPUT", PRODUCER_HIGH_THROUGHPUT),
            ("PRODUCER_EXACTLY_ONCE", PRODUCER_EXACTLY_ONCE),
            ("PRODUCER_LOW_LATENCY", PRODUCER_LOW_LATENCY),
            ("PRODUCER_DEVTEST", PRODUCER_DEVTEST),
        ] {
            for (key, _) in profile {
                assert!(
                    !SIZING_OWNED.contains(key),
                    "{name} names {key}, which the sizing surface always sets"
                );
            }
        }
    }

    // --- poll cap (max.poll.records is client-side only) ---

    #[test]
    fn poll_cap_override_beats_profile() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            consumer: ConsumerKnobs {
                max_poll_records: Some(500), // override throughput's 2000
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(s.effective_poll_cap(), 500);
    }

    #[test]
    fn poll_cap_absent_falls_back_to_profile() {
        // Throughput profile => 2000
        let s = sizing_for_profile(SelfRegulationProfile::Throughput);
        assert_eq!(s.effective_poll_cap(), 2000);
    }

    // --- buffer_memory_bytes KiB conversion ---

    #[test]
    fn buffer_memory_converts_to_kib() {
        let s = KafkaSizingConfig {
            profile: SelfRegulationProfile::Throughput,
            producer: ProducerKnobs {
                buffer_memory_bytes: Some(1_048_576), // exactly 1 MiB
                ..Default::default()
            },
            ..Default::default()
        };
        let map = s.resolved_producer_map();
        assert_eq!(
            map["queue.buffering.max.kbytes"], "1024",
            "1 MiB = 1024 KiB"
        );
    }

    // --- KafkaConfig.sizing field is default-initialised ---

    #[test]
    fn kafka_config_default_has_sizing_field() {
        let cfg = KafkaConfig::default();
        // Default profile is Throughput.
        assert_eq!(cfg.sizing.profile, SelfRegulationProfile::Throughput);
        // Raw maps are empty.
        assert!(cfg.sizing.consumer_librdkafka.is_empty());
        assert!(cfg.sizing.producer_librdkafka.is_empty());
    }

    /// The Kafka sizing profile must serialise as snake_case so the
    /// `self_regulation.profile` cascade key reads identically to the governor
    /// profile (scalo-rs<->scalo-py config-consistency rule). The doc table at the
    /// enum definition uses `low_latency`; this asserts the wire form matches.
    #[test]
    fn sizing_profile_serialises_snake_case() {
        let j = serde_json::to_string(&SelfRegulationProfile::LowLatency).unwrap();
        assert_eq!(j, "\"low_latency\"");
        let j = serde_json::to_string(&SelfRegulationProfile::Throughput).unwrap();
        assert_eq!(j, "\"throughput\"");
        let j = serde_json::to_string(&SelfRegulationProfile::Balanced).unwrap();
        assert_eq!(j, "\"balanced\"");
    }
}
