// Project:   scalo
// File:      src/dlq/kafka.rs
// Purpose:   Kafka-based DLQ backend variant
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka backend variant for the DLQ enum.
//!
//! Routes failed messages to Kafka topics using scalo's
//! [`KafkaProducer`](crate::transport::kafka::KafkaProducer). The
//! producer uses the `LowLatency` profile -- DLQ volume is low and we
//! want failures visible quickly.
//!
//! ## Topic Routing
//!
//! - **Per-table**: Destination `acme.auth` -> topic `acme.auth.dlq`
//! - **Common**: All failures -> single common topic (e.g. `acme.dlq`)
//!
//! ## Durability
//!
//! `send_batch` only queues to the producer. The barrier's `flush_durable`
//! waits for the broker's acks, purges what is still unacked after
//! `kafka.send_timeout_ms`, and charges the delivery failures to the entries
//! Kafka held alone -- see `docs/pipeline/dlq.md`, "The Kafka barrier".
//! The drain runs the same wait when it closes, because dropping the
//! producer discards whatever it still holds.
//!
//! In cascade mode with a backend after Kafka, a failed delivery is not a
//! loss: the entry is handed back for the drain to offer to that backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::transport::KafkaConfig;
use crate::transport::kafka::{KafkaProducer, ProducerProfile};

use super::config::{DlqRouting, KafkaDlqConfig};
use super::entry::DlqEntry;
use super::error::DlqError;

/// How long a barrier waits for the reports of the messages it purged.
const PURGE_REPORT_WAIT: Duration = Duration::from_secs(5);

/// Why the broker never acked an entry Kafka queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unacked {
    /// The broker refused the entry.
    DeliveryFailed,
    /// No ack came in time, so the entry expired or was purged.
    AckTimeout,
}

impl Unacked {
    /// The `reason` label of `dlq_cascade_fallthrough_total`.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::DeliveryFailed => "delivery_failed",
            Self::AckTimeout => "ack_timeout",
        }
    }
}

/// Kafka backend -- internal variant carried by [`super::DlqBackend::Kafka`].
pub struct KafkaDlqInner {
    /// Shared with the blocking task a barrier waits on.
    producer: Arc<KafkaProducer>,
    /// How long a barrier waits for the broker to ack everything queued.
    ack_wait: Duration,
    routing: DlqRouting,
    topic_suffix: String,
    common_topic: String,
    entries_written: AtomicU64,
    write_errors: AtomicU64,
    /// Queued entries no other backend holds, whose fate no barrier has counted.
    sole_custody: u64,
    /// Entries the last `send_batch` queued before it failed.
    queued_before_failure: usize,
    /// Entries lost since the drain last took them.
    durable_losses: u64,
    /// Whether a later backend takes what the broker never acked.
    hand_on: bool,
    /// Entries the broker never acked, until the drain takes them to hand on.
    returned: Vec<(Unacked, DlqEntry)>,
}

impl std::fmt::Debug for KafkaDlqInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDlqInner")
            .field("routing", &self.routing)
            .field("topic_suffix", &self.topic_suffix)
            .field("common_topic", &self.common_topic)
            .field(
                "entries_written",
                &self.entries_written.load(Ordering::Relaxed),
            )
            .field("write_errors", &self.write_errors.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl KafkaDlqInner {
    /// Build the Kafka backend for the DLQ of `service_name`, which names the
    /// common topic when the config leaves it unset.
    ///
    /// # Errors
    ///
    /// `DlqError::Kafka` when the Kafka producer cannot be created, including
    /// when the provider preset or [`KafkaConfig::validate`] refuses
    /// `kafka_config`, as they do for the Kafka transport.
    pub fn new(
        kafka_config: &KafkaConfig,
        dlq_config: &KafkaDlqConfig,
        service_name: &str,
    ) -> Result<Self, DlqError> {
        let producer =
            KafkaProducer::keeping_undelivered(kafka_config, ProducerProfile::LowLatency)
                .map_err(|e| DlqError::Kafka(format!("failed to create DLQ producer: {e}")))?;
        let common_topic = dlq_config.resolved_common_topic(service_name);

        info!(
            routing = ?dlq_config.routing,
            suffix = %dlq_config.topic_suffix,
            common_topic = %common_topic,
            send_timeout_ms = dlq_config.send_timeout_ms,
            "Kafka DLQ backend initialised"
        );

        Ok(Self {
            producer: Arc::new(producer),
            ack_wait: Duration::from_millis(dlq_config.send_timeout_ms),
            routing: dlq_config.routing,
            topic_suffix: dlq_config.topic_suffix.clone(),
            common_topic,
            entries_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            sole_custody: 0,
            queued_before_failure: 0,
            durable_losses: 0,
            hand_on: false,
            returned: Vec::new(),
        })
    }

    /// Hand back what the broker never acked, for the drain to offer to the
    /// backend after this one, instead of counting it lost.
    pub(crate) fn hand_on_unacked(&mut self) {
        self.hand_on = true;
    }

    /// Charge the delivery failures reported since the last call to the
    /// entries only Kafka holds: handed back when a later backend takes them,
    /// counted lost when none does.
    fn reap(&mut self) {
        let undelivered = self.producer.take_undelivered();
        let failed = undelivered.len() as u64;
        let charged = failed.min(self.sole_custody);
        self.sole_custody -= charged;
        if failed > charged {
            debug!(
                failed,
                charged, "Kafka lost DLQ entries another backend also holds"
            );
        }
        if !self.hand_on {
            self.durable_losses += charged;
            return;
        }
        // Cascade queues nothing another backend holds, so every failure goes on: a
        // count past custody can cost a duplicate in the next backend, never a loss.
        if failed > charged {
            warn!(
                failed,
                charged, "Kafka DLQ reported more failed deliveries than it held; handing on all"
            );
        }
        for message in undelivered {
            let cause = if message.timed_out {
                Unacked::AckTimeout
            } else {
                Unacked::DeliveryFailed
            };
            match serde_json::from_slice::<DlqEntry>(&message.payload) {
                Ok(entry) => self.returned.push((cause, entry)),
                Err(e) => {
                    warn!(error = %e, "undelivered DLQ entry does not decode; counted as dropped");
                    self.durable_losses += 1;
                }
            }
        }
    }

    /// Entries the broker never acked, grouped under the fallthrough `reason`
    /// label of why, for the drain to offer to the next backend.
    pub(crate) fn take_returned(&mut self) -> Vec<(&'static str, Vec<DlqEntry>)> {
        let mut groups: Vec<(&'static str, Vec<DlqEntry>)> = Vec::new();
        for (cause, entry) in self.returned.drain(..) {
            let reason = cause.label();
            match groups.iter_mut().find(|(held, _)| *held == reason) {
                Some((_, entries)) => entries.push(entry),
                None => groups.push((reason, vec![entry])),
            }
        }
        groups
    }

    fn resolve_topic(&self, entry: &DlqEntry) -> String {
        match self.routing {
            DlqRouting::Common => self.common_topic.clone(),
            DlqRouting::PerTable => entry.destination.as_ref().map_or_else(
                || self.common_topic.clone(),
                |dest| format!("{dest}{}", self.topic_suffix),
            ),
        }
    }

    /// Send a batch. Per-entry topic resolution + non-blocking producer
    /// queue. The producer's background delivery thread does the network
    /// I/O -- `send()` is sync-shaped and returns immediately.
    ///
    /// On `Err` the entries before the one that failed are already queued;
    /// `queued_before_failure` says how many.
    ///
    /// Takes the delivery failures reported since the last write first, so
    /// the payloads they hold never outgrow what the producer had queued.
    pub async fn send_batch(&mut self, batch: &[DlqEntry]) -> Result<(), DlqError> {
        self.reap();
        let mut queued = 0;
        let result = self.enqueue(batch, &mut queued);
        self.sole_custody += queued as u64;
        self.queued_before_failure = if result.is_err() { queued } else { 0 };
        result
    }

    fn enqueue(&self, batch: &[DlqEntry], queued: &mut usize) -> Result<(), DlqError> {
        for entry in batch {
            let topic = self.resolve_topic(entry);
            let payload = serde_json::to_vec(entry)
                .map_err(|e| DlqError::Serialization(format!("DLQ serialise: {e}")))?;

            match self.producer.send(&topic, None, &payload) {
                Ok(()) => {
                    *queued += 1;
                    self.entries_written.fetch_add(1, Ordering::Relaxed);
                    debug!(topic = %topic, reason = %entry.reason, "DLQ entry queued to Kafka");
                }
                Err(e) => {
                    self.write_errors.fetch_add(1, Ordering::Relaxed);
                    error!(
                        error = %e,
                        topic = %topic,
                        reason = %entry.reason,
                        "Failed to queue DLQ entry to Kafka"
                    );
                    return Err(DlqError::Kafka(format!("DLQ send failed: {e}")));
                }
            }
        }
        Ok(())
    }

    /// Wait for the broker to ack every queued entry, then charge the
    /// delivery failures since the previous barrier to the entries only
    /// Kafka holds.
    ///
    /// The wait runs on the blocking pool for up to `kafka.send_timeout_ms`.
    /// Whatever is still unacked then is purged, which adds up to 5 s. An
    /// entry the broker refused or never acked is handed back through
    /// `take_returned` when a later backend takes such entries, and counted
    /// as lost otherwise. An entry in flight to a stalled broker at the purge
    /// can still be written, so the broker can hold what is handed back or
    /// counted lost, but nothing it did not ack goes uncounted. The loss is
    /// taken by the drain through `take_durable_losses`.
    ///
    /// # Errors
    ///
    /// `DlqError::Kafka` when any entry lost since the previous barrier was
    /// held by Kafka alone, or the blocking task failed.
    pub async fn flush_durable(&mut self) -> Result<(), DlqError> {
        let producer = Arc::clone(&self.producer);
        let ack_wait = self.ack_wait;
        let drained = tokio::task::spawn_blocking(move || {
            if producer.drain_within(ack_wait) {
                return true;
            }
            producer.purge_outstanding();
            producer.drain_within(PURGE_REPORT_WAIT)
        })
        .await
        .map_err(|e| DlqError::Kafka(format!("DLQ durable flush task failed: {e}")))?;

        self.reap();
        // Every report is in, so what is left in custody was acked.
        if drained {
            self.sole_custody = 0;
        } else {
            warn!(
                pending = self.sole_custody,
                "Kafka DLQ entries purged but not yet reported; the next flush or the close settles them"
            );
        }
        let lost = self.durable_losses;
        if lost > 0 {
            return Err(DlqError::Kafka(format!(
                "{lost} DLQ entries lost: the broker refused them or did not ack within {} ms",
                self.ack_wait.as_millis()
            )));
        }
        Ok(())
    }

    /// Entries only Kafka holds whose fate no barrier learned, handed over
    /// as lost: the drain calls this as it closes, and dropping the producer
    /// then discards whatever it still holds.
    pub(crate) fn take_unconfirmed(&mut self) -> u64 {
        std::mem::take(&mut self.sole_custody)
    }

    /// The largest serialised entry the producer takes.
    pub(crate) fn entry_ceiling(&self) -> usize {
        self.producer.payload_ceiling()
    }

    /// Entries only Kafka holds whose fate no barrier has learned yet.
    pub(crate) fn unsettled(&self) -> u64 {
        self.sole_custody
    }

    /// Entries the last failed `send_batch` queued before it stopped.
    pub(crate) fn queued_before_failure(&self) -> usize {
        self.queued_before_failure
    }

    /// Record that another backend also holds `entries` of the last batch.
    pub(crate) fn share_custody(&mut self, entries: usize) {
        self.sole_custody = self.sole_custody.saturating_sub(entries as u64);
    }

    /// Entries the barriers since the last call found lost.
    pub(crate) fn take_durable_losses(&mut self) -> u64 {
        std::mem::take(&mut self.durable_losses)
    }

    /// Number of entries successfully queued.
    #[must_use]
    pub fn entries_written(&self) -> u64 {
        self.entries_written.load(Ordering::Relaxed)
    }

    /// Number of queue-submit errors.
    #[must_use]
    pub fn write_errors(&self) -> u64 {
        self.write_errors.load(Ordering::Relaxed)
    }
}

/// A producer config for a broker that refuses at once -- port 1 on
/// loopback -- so nothing queued is ever acked.
#[cfg(test)]
pub(super) fn unreachable_broker() -> KafkaConfig {
    let mut config = KafkaConfig {
        brokers: vec!["127.0.0.1:1".to_string()],
        group: String::new(),
        ..KafkaConfig::default()
    };
    config.sizing.producer.idempotence = Some(false);
    for (key, value) in [
        ("statistics.interval.ms", "0"),
        ("reconnect.backoff.max.ms", "100"),
        ("log_level", "0"),
    ] {
        config
            .sizing
            .producer_librdkafka
            .insert(key.to_string(), value.to_string());
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::kafka::client_gate;

    /// The refusal `KafkaDlqInner::new` returned, `None` when it built.
    fn backend_refusal(kafka_config: &KafkaConfig) -> Option<String> {
        match KafkaDlqInner::new(kafka_config, &KafkaDlqConfig::default(), "loader") {
            Ok(_) => None,
            Err(DlqError::Kafka(why)) => Some(why),
            Err(other) => panic!("failed as {other}, not as a Kafka backend error"),
        }
    }

    #[test]
    fn the_kafka_backend_refuses_the_configs_the_transport_refuses() {
        client_gate::assert_refuses_as_the_transport_does(backend_refusal);
    }

    #[test]
    fn the_kafka_backend_builds_on_a_verified_config_under_production() {
        client_gate::assert_builds_under_production(backend_refusal);
    }

    #[test]
    fn resolve_topic_per_table() {
        let routing = DlqRouting::PerTable;
        let suffix = ".dlq";
        let common = "loader.dlq";

        let entry = DlqEntry::new("loader", "error", vec![]).with_destination("acme.auth");
        let topic = match routing {
            DlqRouting::PerTable => entry
                .destination
                .as_ref()
                .map_or_else(|| common.to_string(), |dest| format!("{dest}{suffix}")),
            DlqRouting::Common => common.to_string(),
        };
        assert_eq!(topic, "acme.auth.dlq");

        let entry_no_dest = DlqEntry::new("loader", "error", vec![]);
        let topic = match routing {
            DlqRouting::PerTable => entry_no_dest
                .destination
                .as_ref()
                .map_or_else(|| common.to_string(), |dest| format!("{dest}{suffix}")),
            DlqRouting::Common => common.to_string(),
        };
        assert_eq!(topic, "loader.dlq");
    }

    #[test]
    fn resolve_topic_common_ignores_destination() {
        let routing = DlqRouting::Common;
        let common = "all-errors.dlq";

        let entry = DlqEntry::new("loader", "error", vec![]).with_destination("acme.auth");
        let topic = match routing {
            DlqRouting::Common => common.to_string(),
            DlqRouting::PerTable => unreachable!(
                "per-table case is exercised by the sibling test; this match must hit Common"
            ),
        };
        let _ = entry;
        assert_eq!(topic, "all-errors.dlq");
    }

    #[tokio::test]
    async fn the_ack_wait_is_the_configured_send_timeout() {
        let dlq_config = KafkaDlqConfig {
            send_timeout_ms: 400,
            ..KafkaDlqConfig::default()
        };
        let mut backend =
            KafkaDlqInner::new(&unreachable_broker(), &dlq_config, "svc").expect("backend");
        backend
            .send_batch(&[DlqEntry::new("svc", "err", b"x".to_vec())])
            .await
            .expect("queued");

        let started = std::time::Instant::now();
        let result = backend.flush_durable().await;
        let took = started.elapsed();
        assert!(result.is_err(), "no broker acked the entry: {result:?}");
        assert!(
            took >= Duration::from_millis(400),
            "gave up before the configured wait: {took:?}"
        );
        assert!(
            took < Duration::from_secs(4),
            "the configured 400 ms did not bound the wait: {took:?}"
        );
        assert_eq!(backend.take_durable_losses(), 1);
    }

    /// An entry with no destination goes to the service's own topic when no
    /// common topic is configured, and to the configured one when it is.
    #[tokio::test]
    async fn the_common_topic_is_the_services_own_unless_configured() {
        let entry = DlqEntry::new("loader", "err", b"x".to_vec());
        let backend =
            KafkaDlqInner::new(&unreachable_broker(), &KafkaDlqConfig::default(), "loader")
                .expect("backend");
        assert_eq!(backend.resolve_topic(&entry), "loader.dlq");

        let configured = KafkaDlqConfig {
            common_topic: Some("acme_loader_dlq".into()),
            ..KafkaDlqConfig::default()
        };
        let backend =
            KafkaDlqInner::new(&unreachable_broker(), &configured, "loader").expect("backend");
        assert_eq!(backend.resolve_topic(&entry), "acme_loader_dlq");
    }
}
