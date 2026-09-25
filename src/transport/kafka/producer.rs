// Project:   scalo
// File:      src/transport/kafka/producer.rs
// Purpose:   High-throughput Kafka producer for PB/day workloads
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! High-throughput Kafka producer optimized for PB/day workloads.
//!
//! # Performance Characteristics
//!
//! - **Batch-first design**: batching, codec and queue size come from the
//!   sizing surface ([`KafkaSizingConfig`](super::KafkaSizingConfig)). The
//!   default `throughput` profile batches 128 KiB over 20 ms, compresses with
//!   zstd at level 3 and caps the queue at 64 MiB.
//! - **Non-blocking sends**: Fire-and-forget with delivery callbacks
//! - **Idempotent by default**: 5 in-flight requests per connection, `acks=all`
//!
//! # Profiles
//!
//! A [`ProducerProfile`] adds the few settings the sizing surface leaves alone.
//! Every setting resolves in one order: profile < sizing profile < named sizing
//! knobs < `sizing.producer_librdkafka` < `librdkafka_overrides`.
//!
//! - **high_throughput**: Nagle off, statistics on
//! - **exactly_once**: Idempotent producer with ordering guarantees
//! - **low_latency**: Leader-ack only when idempotence is off
//!
//! # Example
//!
//! ```rust,no_run
//! use std::time::Duration;
//!
//! use scalo::transport::kafka::{KafkaConfig, KafkaProducer, ProducerProfile};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let messages = vec![String::from("event")];
//! // High-throughput producer
//! let config = KafkaConfig::production();
//! let producer = KafkaProducer::new(&config, ProducerProfile::HighThroughput)?;
//!
//! // Send messages (fire-and-forget batching)
//! for msg in &messages {
//!     producer.send("events", None, msg.as_bytes())?;
//! }
//!
//! // Flush before shutdown; the count is messages with no delivery report
//! let unreported = producer.flush(Duration::from_secs(30));
//! if unreported > 0 {
//!     eprintln!("{unreported} messages had no delivery report at shutdown");
//! }
//! # Ok(())
//! # }
//! ```

use super::classify::{DegradedLatch, DeliveryState, SendFailure, classify_send_failure};
use super::config::KafkaConfig;
use crate::transport::error::{TransportError, TransportResult};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Producer profile for different use cases.
///
/// A profile sets only what the sizing surface leaves alone; batch size,
/// linger, codec and queue size come from `KafkaConfig::sizing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProducerProfile {
    /// Maximum throughput, at-least-once delivery.
    ///
    /// Nagle off and statistics on ([`PRODUCER_HIGH_THROUGHPUT`](super::PRODUCER_HIGH_THROUGHPUT)).
    #[default]
    HighThroughput,

    /// Exactly-once semantics with ordering guarantees.
    ///
    /// - Idempotence enabled, `acks=all`, max 5 in-flight requests
    /// - Holds only while `sizing.producer.idempotence` is unset or `true`
    ExactlyOnce,

    /// Leader-ack for real-time use cases.
    ///
    /// - `acks=1`, taking effect only with `sizing.producer.idempotence: false`
    /// - Pair with the `low_latency` sizing profile for no batching delay
    LowLatency,

    /// Development/testing settings.
    DevTest,
}

impl ProducerProfile {
    /// The librdkafka settings this profile lays under the sizing surface.
    fn settings(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::HighThroughput => super::config::PRODUCER_HIGH_THROUGHPUT,
            Self::ExactlyOnce => super::config::PRODUCER_EXACTLY_ONCE,
            Self::LowLatency => super::config::PRODUCER_LOW_LATENCY,
            Self::DevTest => super::config::PRODUCER_DEVTEST,
        }
    }
}

/// The librdkafka config `KafkaProducer::new` builds a producer from.
fn client_config(config: &KafkaConfig, profile: ProducerProfile) -> ClientConfig {
    super::producer_client_config(config, profile.settings())
}

impl std::fmt::Display for ProducerProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HighThroughput => write!(f, "high_throughput"),
            Self::ExactlyOnce => write!(f, "exactly_once"),
            Self::LowLatency => write!(f, "low_latency"),
            Self::DevTest => write!(f, "devtest"),
        }
    }
}

/// High-throughput Kafka producer.
///
/// Uses `ThreadedProducer` for background message delivery with:
/// - Automatic batching and compression
/// - Non-blocking sends
/// - Delivery callbacks (optional)
pub struct KafkaProducer {
    producer: ThreadedProducer<ProducerContext>,
    profile: ProducerProfile,
    /// Shared with the rdkafka context, which owns the delivery callback.
    delivery: Arc<DeliveryState>,
    /// Latches a sustained retryable enqueue failure to one warn per outage.
    enqueue_degraded: DegradedLatch,
    /// The effective `message.max.bytes`.
    message_max_bytes: usize,
    // Metrics
    messages_sent: AtomicU64,
    bytes_sent: AtomicU64,
    errors: AtomicU64,
}

/// Producer context for delivery callbacks and metrics.
#[derive(Clone, Default)]
pub struct ProducerContext {
    state: Arc<DeliveryState>,
}

impl rdkafka::ClientContext for ProducerContext {}

impl rdkafka::producer::ProducerContext for ProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        result: &rdkafka::producer::DeliveryResult<'_>,
        _opaque: Self::DeliveryOpaque,
    ) {
        self.state.record(result);
    }
}

impl KafkaProducer {
    /// Create a new high-throughput producer.
    ///
    /// # Arguments
    ///
    /// * `config` - Kafka configuration (brokers, security, etc.)
    /// * `profile` - Producer profile (throughput vs latency vs exactly-once)
    ///
    /// # Errors
    ///
    /// Returns error if producer creation fails.
    pub fn new(config: &KafkaConfig, profile: ProducerProfile) -> TransportResult<Self> {
        let client_config = client_config(config, profile);
        let message_max_bytes = client_config
            .get("message.max.bytes")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(super::LIBRDKAFKA_MESSAGE_MAX_BYTES);
        let context = ProducerContext::default();
        let delivery = Arc::clone(&context.state);
        let producer: ThreadedProducer<ProducerContext> = client_config
            .create_with_context(context)
            .map_err(|e| TransportError::Connection(format!("Failed to create producer: {e}")))?;

        Ok(Self {
            producer,
            profile,
            delivery,
            enqueue_degraded: DegradedLatch::default(),
            message_max_bytes,
            messages_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    /// The largest payload `send` takes: `message.max.bytes` less the framing
    /// librdkafka adds to a record with no key and no headers.
    pub(crate) fn payload_ceiling(&self) -> usize {
        self.message_max_bytes
            .saturating_sub(super::RECORD_WIRE_OVERHEAD)
    }

    /// Create a high-throughput producer (convenience method).
    pub fn high_throughput(config: &KafkaConfig) -> TransportResult<Self> {
        Self::new(config, ProducerProfile::HighThroughput)
    }

    /// Create an exactly-once producer (convenience method).
    pub fn exactly_once(config: &KafkaConfig) -> TransportResult<Self> {
        Self::new(config, ProducerProfile::ExactlyOnce)
    }

    /// Create a low-latency producer (convenience method).
    pub fn low_latency(config: &KafkaConfig) -> TransportResult<Self> {
        Self::new(config, ProducerProfile::LowLatency)
    }

    /// Send a message to a topic.
    ///
    /// This is a non-blocking operation that queues the message for delivery.
    /// Messages are batched and sent in the background.
    ///
    /// # Arguments
    ///
    /// * `topic` - Target topic name
    /// * `key` - Optional message key (for partitioning)
    /// * `payload` - Message payload bytes
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Message queued successfully
    /// * `Err(TransportError::Backpressure)` - Queue full or a retryable broker
    ///   condition; the message is still deliverable, so retry it
    /// * `Err(TransportError::MessageTooLarge)` - Over the message-size
    ///   ceiling, so a retry can never succeed -- dead-letter it
    /// * `Err(TransportError::Send(_))` - Retrying cannot help
    ///
    /// # Errors
    ///
    /// Returns the classified produce failure above; the caller decides
    /// between retry, dead-letter and abort.
    pub fn send(&self, topic: &str, key: Option<&[u8]>, payload: &[u8]) -> TransportResult<()> {
        let mut record = BaseRecord::to(topic).payload(payload);
        if let Some(k) = key {
            record = record.key(k);
        }

        match self.producer.send(record) {
            Ok(()) => {
                self.messages_sent.fetch_add(1, Ordering::Relaxed);
                self.bytes_sent
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
                if self.enqueue_degraded.clear() {
                    tracing::info!(topic, "kafka enqueue recovered");
                }
                Ok(())
            }
            Err((err, _)) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                match classify_send_failure(&err) {
                    SendFailure::QueueFull => Err(TransportError::Backpressure),
                    SendFailure::TooLarge => Err(TransportError::MessageTooLarge {
                        bytes: payload.len(),
                        detail: err.to_string(),
                    }),
                    SendFailure::Retryable => {
                        // Backpressure is the recoverable outcome the caller
                        // acts on; the cause only survives in this line.
                        if self.enqueue_degraded.enter() {
                            tracing::warn!(
                                topic,
                                error = %err,
                                "kafka enqueue failed on a retryable condition; the caller retries"
                            );
                        }
                        Err(TransportError::Backpressure)
                    }
                    SendFailure::Fatal => Err(TransportError::Send(err.to_string())),
                }
            }
        }
    }

    /// Send a message with a string key.
    ///
    /// Convenience method when key is a string.
    pub fn send_keyed(&self, topic: &str, key: &str, payload: &[u8]) -> TransportResult<()> {
        self.send(topic, Some(key.as_bytes()), payload)
    }

    /// Send a batch of messages.
    ///
    /// More efficient than individual sends as it reduces function call overhead.
    /// All messages go to the same topic.
    ///
    /// # Returns
    ///
    /// Number of messages successfully queued. If less than input length,
    /// the producer queue is full - call `poll()` or `flush()` and retry.
    pub fn send_batch(&self, topic: &str, messages: &[(Option<&[u8]>, &[u8])]) -> usize {
        let mut sent = 0;
        for (key, payload) in messages {
            let mut record = BaseRecord::to(topic).payload(*payload);
            if let Some(k) = key {
                record = record.key(*k);
            }

            if self.producer.send(record).is_ok() {
                self.messages_sent.fetch_add(1, Ordering::Relaxed);
                self.bytes_sent
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
                sent += 1;
            } else {
                self.errors.fetch_add(1, Ordering::Relaxed);
                break; // Queue full
            }
        }
        sent
    }

    /// Poll the producer for delivery callbacks.
    ///
    /// Call this periodically to process delivery reports and free memory.
    /// For high-throughput, call every 100ms or so.
    pub fn poll(&self, timeout: Duration) {
        self.producer.poll(Timeout::After(timeout));
    }

    /// Flush all queued messages.
    ///
    /// Blocks until every message has its delivery report or timeout expires.
    /// Call this before shutdown to ensure no message loss.
    ///
    /// # Returns
    ///
    /// Number of messages with no delivery report yet. `0` means every
    /// message sent so far was either acknowledged or failed -- read
    /// [`Self::delivery_failures`] for the failures. Statistics, error and log
    /// events the client has still to serve are not messages and are not
    /// counted.
    pub fn flush(&self, timeout: Duration) -> usize {
        let _ = self.producer.flush(Timeout::After(timeout));
        let unreported = self
            .messages_sent
            .load(Ordering::Relaxed)
            .saturating_sub(self.delivery.reports());
        usize::try_from(unreported).unwrap_or(usize::MAX)
    }

    /// Wait until every queued message has its delivery report handled;
    /// `false` when `timeout` ran out first.
    #[cfg(feature = "dlq-kafka")]
    pub(crate) fn drain_within(&self, timeout: Duration) -> bool {
        self.producer.flush(Timeout::After(timeout)).is_ok()
    }

    /// Discard every message still queued or in flight. Each gets a failed
    /// delivery report, but one already in flight can still reach the broker.
    #[cfg(feature = "dlq-kafka")]
    pub(crate) fn purge_outstanding(&self) {
        self.producer
            .purge(rdkafka::producer::PurgeConfig::default().queue().inflight());
    }

    /// librdkafka's out-queue length: messages waiting to be sent or
    /// acknowledged, plus delivery reports and client events (statistics,
    /// errors, logs) not yet served.
    ///
    /// Not a message count -- it can be non-zero with every message
    /// delivered. [`Self::flush`] returns the messages with no delivery
    /// report yet.
    #[allow(clippy::cast_sign_loss)]
    pub fn in_flight_count(&self) -> usize {
        self.producer.in_flight_count().max(0) as usize
    }

    /// Messages the broker refused or never acknowledged.
    ///
    /// Distinct from [`ProducerMetrics::errors`], which counts local enqueue
    /// failures: a message can queue cleanly and still never reach the broker.
    #[must_use]
    pub fn delivery_failures(&self) -> u64 {
        self.delivery.failures()
    }

    /// Get producer metrics.
    #[allow(clippy::cast_sign_loss)]
    pub fn metrics(&self) -> ProducerMetrics {
        ProducerMetrics {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            in_flight: self.producer.in_flight_count().max(0) as u64,
            profile: self.profile,
        }
    }
}

/// Producer metrics snapshot.
#[derive(Debug, Clone)]
pub struct ProducerMetrics {
    /// Total messages sent (queued).
    pub messages_sent: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Total errors encountered.
    pub errors: u64,
    /// librdkafka's out-queue length at the snapshot: messages plus client
    /// events not yet served. See [`KafkaProducer::in_flight_count`].
    pub in_flight: u64,
    /// Producer profile in use.
    pub profile: ProducerProfile,
}

impl std::fmt::Debug for KafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProducer")
            .field("profile", &self.profile)
            .field("messages_sent", &self.messages_sent.load(Ordering::Relaxed))
            .field("bytes_sent", &self.bytes_sent.load(Ordering::Relaxed))
            .field("errors", &self.errors.load(Ordering::Relaxed))
            .field("delivery_failures", &self.delivery.failures())
            .field("in_flight", &self.producer.in_flight_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_profile_display() {
        assert_eq!(
            ProducerProfile::HighThroughput.to_string(),
            "high_throughput"
        );
        assert_eq!(ProducerProfile::ExactlyOnce.to_string(), "exactly_once");
        assert_eq!(ProducerProfile::LowLatency.to_string(), "low_latency");
        assert_eq!(ProducerProfile::DevTest.to_string(), "devtest");
    }

    #[test]
    fn test_producer_profile_default() {
        assert_eq!(ProducerProfile::default(), ProducerProfile::HighThroughput);
    }

    const PROFILES: [ProducerProfile; 4] = [
        ProducerProfile::HighThroughput,
        ProducerProfile::ExactlyOnce,
        ProducerProfile::LowLatency,
        ProducerProfile::DevTest,
    ];

    /// Every profile produces with the sizing default, and librdkafka takes it.
    #[test]
    fn every_profile_defaults_to_zstd_at_level_3() {
        for profile in PROFILES {
            let built = client_config(&KafkaConfig::default(), profile);
            assert_eq!(built.get("compression.type"), Some("zstd"), "{profile}");
            assert_eq!(built.get("compression.level"), Some("3"), "{profile}");
            assert_eq!(
                super::super::librdkafka_resolves(&built, "compression.codec"),
                "zstd",
                "{profile}"
            );
        }
    }

    /// `librdkafka_overrides` is the last layer on this path too: its codec
    /// wins over the zstd default and over `sizing.producer_librdkafka`, and
    /// the zstd level goes with the codec it replaced.
    #[test]
    fn every_profile_takes_the_raw_codec_override() {
        let mut config = KafkaConfig::default();
        config
            .sizing
            .producer_librdkafka
            .insert("compression.type".to_string(), "gzip".to_string());
        config
            .librdkafka_overrides
            .insert("compression.type".to_string(), "lz4".to_string());
        for profile in PROFILES {
            let built = client_config(&config, profile);
            assert_eq!(built.get("compression.type"), Some("lz4"), "{profile}");
            assert_eq!(built.get("compression.level"), None, "{profile}");
            assert_eq!(
                super::super::librdkafka_resolves(&built, "compression.codec"),
                "lz4",
                "{profile}"
            );
        }
    }

    /// An override of any key the sizing surface sets wins here, as it does
    /// on the transport path.
    #[test]
    fn an_override_wins_over_a_sizing_key() {
        let mut config = KafkaConfig::default();
        config
            .librdkafka_overrides
            .insert("linger.ms".to_string(), "250".to_string());
        let built = client_config(&config, ProducerProfile::HighThroughput);
        assert_eq!(built.get("linger.ms"), Some("250"));
    }

    /// Building a producer on the default config proves the librdkafka this
    /// process links takes zstd at level 3.
    #[test]
    fn a_producer_builds_on_the_zstd_default() {
        let config = KafkaConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            group: String::new(),
            ..KafkaConfig::default()
        };
        KafkaProducer::new(&config, ProducerProfile::HighThroughput)
            .expect("librdkafka refuses zstd only when built without it");
    }

    /// Produce to a broker that cannot be reached and let `message.timeout.ms`
    /// expire, so librdkafka fires a real delivery report through the real
    /// callback. Port 1 on loopback refuses immediately -- no external network.
    #[test]
    fn a_message_that_never_reaches_a_broker_is_counted_as_a_delivery_failure() {
        let producer = unreachable_producer(&[
            ("message.timeout.ms", "400"),
            ("statistics.interval.ms", "0"),
            ("reconnect.backoff.max.ms", "100"),
            ("log_level", "0"),
        ]);
        producer
            .send("unreachable.topic", None, b"payload")
            .expect("the message queues locally even with no broker");

        // Flush waits for the delivery report; the timeout above bounds it.
        producer.flush(Duration::from_secs(5));

        assert_eq!(
            producer.delivery_failures(),
            1,
            "the broker never took the message, so the delivery callback must \
             record it"
        );
    }

    /// A producer pointed at a port that refuses at once, with `settings` on top.
    fn unreachable_producer(settings: &[(&str, &str)]) -> KafkaProducer {
        let mut config = KafkaConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            group: String::new(),
            ..KafkaConfig::default()
        };
        config.sizing.producer.idempotence = Some(false);
        for (key, value) in settings {
            config
                .sizing
                .producer_librdkafka
                .insert((*key).to_string(), (*value).to_string());
        }
        KafkaProducer::new(&config, ProducerProfile::LowLatency)
            .expect("producer creation does not contact a broker")
    }

    #[test]
    fn flush_returns_the_messages_still_undelivered() {
        let producer = unreachable_producer(&[("message.timeout.ms", "60000"), ("log_level", "0")]);
        for _ in 0..3 {
            producer
                .send("unreachable.topic", None, b"payload")
                .expect("queued");
        }
        assert_eq!(producer.flush(Duration::from_millis(300)), 3);
    }

    /// A log writer that parks whichever thread writes to it while shut.
    #[cfg(feature = "logger")]
    #[derive(Clone, Default)]
    struct Gate(Arc<(std::sync::Mutex<GateState>, std::sync::Condvar)>);

    #[cfg(feature = "logger")]
    #[derive(Default)]
    struct GateState {
        shut: bool,
        parked: bool,
    }

    #[cfg(feature = "logger")]
    impl Gate {
        fn state(&self) -> std::sync::MutexGuard<'_, GateState> {
            self.0
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn set_shut(&self, shut: bool) {
            self.state().shut = shut;
            self.0.1.notify_all();
        }

        fn wait_parked(&self, within: Duration) -> bool {
            let (state, _) = self
                .0
                .1
                .wait_timeout_while(self.state(), within, |s| !s.parked)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.parked
        }
    }

    #[cfg(feature = "logger")]
    impl std::io::Write for Gate {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut state = self.state();
            if state.shut {
                state.parked = true;
                self.0.1.notify_all();
            }
            while state.shut {
                state = self
                    .0
                    .1
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Run `test` in a child process of this test binary that runs nothing
    /// else, so a global subscriber another test installed cannot be in the
    /// way. `name` is the test's own name; the parent fails unless the child
    /// ran exactly that one test and it passed.
    #[cfg(feature = "logger")]
    fn in_own_process(name: &str, test: impl FnOnce()) {
        const CHILD: &str = "SCALO_TEST_IN_OWN_PROCESS";
        if std::env::var_os(CHILD).is_some() {
            test();
            return;
        }
        let exe = std::env::current_exe().expect("test binary path");
        let output = std::process::Command::new(exe)
            .args([name, "--exact"])
            .env(CHILD, "1")
            .output()
            .expect("run the test in a process of its own");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("test result: ok. 1 passed"),
            "{name} in its own process:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// A completed flush counts messages only. The poll thread is parked on a
    /// log line, so the statistics events it would serve stay queued, and
    /// `in_flight_count` shows them. The parked thread is rdkafka's, which
    /// logs through the process-wide `log` logger and never enters a scoped
    /// subscriber, so the gate has to be the global one.
    #[cfg(feature = "logger")]
    #[test]
    fn a_completed_flush_does_not_count_client_events_as_messages() {
        let path = concat!(
            module_path!(),
            "::a_completed_flush_does_not_count_client_events_as_messages"
        );
        let name = path.split_once("::").map_or(path, |(_, name)| name);
        in_own_process(name, completed_flush_with_client_events_queued);
    }

    #[cfg(feature = "logger")]
    fn completed_flush_with_client_events_queued() {
        let gate = Gate::default();
        let writer = gate.clone();
        tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::INFO)
            .try_init()
            .expect("the global subscriber is free in a process running one test");
        let producer = unreachable_producer(&[("message.timeout.ms", "50")]);
        producer
            .send("unreachable.topic", None, b"payload")
            .expect("queued");
        assert_eq!(
            producer.flush(Duration::from_secs(5)),
            0,
            "the message's delivery report arrived"
        );

        gate.set_shut(true);
        assert!(
            gate.wait_parked(Duration::from_secs(5)),
            "the poll thread logged nothing to park on"
        );
        // Statistics arrive every second and queue behind the parked thread.
        std::thread::sleep(Duration::from_millis(2500));
        let unreported = producer.flush(Duration::from_millis(100));
        let queued = producer.in_flight_count();
        gate.set_shut(false);

        assert!(
            queued > 0,
            "no client event queued behind the parked thread"
        );
        assert_eq!(
            unreported, 0,
            "{queued} queued client events are not messages"
        );
    }
}
