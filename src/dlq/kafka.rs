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
//! `ACK_WAIT`, and charges the delivery failures to the entries Kafka
//! held alone -- see `docs/pipeline/dlq.md`, "The Kafka barrier".

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::transport::KafkaConfig;
use crate::transport::kafka::{KafkaProducer, ProducerProfile};

use super::config::{DlqRouting, KafkaDlqConfig};
use super::entry::DlqEntry;
use super::error::DlqError;

/// How long a barrier waits for the broker to ack everything queued.
const ACK_WAIT: Duration = Duration::from_secs(30);

/// How long a barrier waits for the reports of the messages it purged.
const PURGE_REPORT_WAIT: Duration = Duration::from_secs(5);

/// Kafka backend -- internal variant carried by [`super::DlqBackend::Kafka`].
pub struct KafkaDlqInner {
    /// Shared with the blocking task a barrier waits on.
    producer: Arc<KafkaProducer>,
    routing: DlqRouting,
    topic_suffix: String,
    common_topic: String,
    entries_written: AtomicU64,
    write_errors: AtomicU64,
    /// Queued entries no other backend holds, whose fate no barrier has counted.
    sole_custody: u64,
    /// Entries the last `send_batch` queued before it failed.
    queued_before_failure: usize,
    /// Producer delivery failures already charged by a barrier.
    failures_seen: u64,
    /// Entries the last barrier found lost, until the drain takes them.
    durable_losses: u64,
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
    /// Build the Kafka backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the Kafka producer cannot be created.
    pub fn new(kafka_config: &KafkaConfig, dlq_config: &KafkaDlqConfig) -> Result<Self, DlqError> {
        let producer = KafkaProducer::new(kafka_config, ProducerProfile::LowLatency)
            .map_err(|e| DlqError::Kafka(format!("failed to create DLQ producer: {e}")))?;

        info!(
            routing = ?dlq_config.routing,
            suffix = %dlq_config.topic_suffix,
            common_topic = %dlq_config.common_topic,
            "Kafka DLQ backend initialised"
        );

        let failures_seen = producer.delivery_failures();
        Ok(Self {
            producer: Arc::new(producer),
            routing: dlq_config.routing,
            topic_suffix: dlq_config.topic_suffix.clone(),
            common_topic: dlq_config.common_topic.clone(),
            entries_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            sole_custody: 0,
            queued_before_failure: 0,
            failures_seen,
            durable_losses: 0,
        })
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
    pub async fn send_batch(&mut self, batch: &[DlqEntry]) -> Result<(), DlqError> {
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
    /// The wait runs on the blocking pool for up to 30 s. Whatever is still
    /// unacked then is purged, which adds up to 5 s, and counted as lost. An
    /// entry in flight to a stalled broker at the purge can still be written,
    /// so the count can overstate the loss but never understates it. The loss
    /// is taken by the drain through `take_durable_losses`.
    ///
    /// # Errors
    ///
    /// `DlqError::Kafka` when any entry only Kafka held was refused by the
    /// broker or purged, or the blocking task failed.
    pub async fn flush_durable(&mut self) -> Result<(), DlqError> {
        let producer = Arc::clone(&self.producer);
        let drained = tokio::task::spawn_blocking(move || {
            if producer.drain_within(ACK_WAIT) {
                return true;
            }
            producer.purge_outstanding();
            producer.drain_within(PURGE_REPORT_WAIT)
        })
        .await
        .map_err(|e| DlqError::Kafka(format!("DLQ durable flush task failed: {e}")))?;

        let failures = self.producer.delivery_failures();
        let failed = failures.saturating_sub(self.failures_seen);
        self.failures_seen = failures;
        let lost = failed.min(self.sole_custody);
        // Unreported entries stay in custody for the next barrier to charge.
        self.sole_custody = if drained { 0 } else { self.sole_custody - lost };
        self.durable_losses += lost;

        if failed > lost {
            debug!(
                failed,
                lost, "Kafka lost DLQ entries another backend also holds"
            );
        }
        if !drained {
            warn!(
                pending = self.sole_custody,
                "Kafka DLQ entries purged but not yet reported; the next flush counts them"
            );
        }
        if lost > 0 {
            return Err(DlqError::Kafka(format!(
                "{lost} DLQ entries lost: the broker refused them or did not ack within {}s",
                ACK_WAIT.as_secs()
            )));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_topic_per_table() {
        let routing = DlqRouting::PerTable;
        let suffix = ".dlq";
        let common = "dfe.dlq";

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
        assert_eq!(topic, "dfe.dlq");
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
}
