// Project:   scalo
// File:      src/dlq/backend.rs
// Purpose:   DlqBackend enum -- variant per supported backend
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Static enum dispatch for DLQ backends.
//!
//! Replaces the previous `#[async_trait] trait DlqBackend` + `Box<dyn>`
//! shape. Each backend is a concrete variant; the drain matches over
//! variants. No vtable, no `async_trait` macro, no heap-boxed future.
//!
//! See [`super::orchestrator::Dlq`] for usage.

use super::entry::DlqEntry;
use super::error::DlqError;

/// A DLQ backend. One variant per supported destination.
///
/// Variants are feature-gated:
///
/// - [`Self::File`] -- always available
/// - [`Self::Kafka`] -- `dlq-kafka` feature
/// - [`Self::Http`] -- `dlq-http` feature
///
/// Each variant's inner struct lives in its sibling module
/// (`file::FileDlqInner`, `kafka::KafkaDlqInner`, etc.). They are
/// crate-private -- consumers configure DLQ via [`super::DlqConfig`] and
/// drive it via [`super::orchestrator::Dlq`].
#[non_exhaustive]
pub enum DlqBackend {
    /// NDJSON file backend with rotation.
    File(super::file::FileDlqInner),

    /// Kafka backend.
    #[cfg(feature = "dlq-kafka")]
    Kafka(super::kafka::KafkaDlqInner),

    /// HTTP POST backend.
    #[cfg(feature = "dlq-http")]
    Http(super::http::HttpDlqInner),
}

impl std::fmt::Debug for DlqBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DlqBackend::")?;
        f.write_str(self.name())
    }
}

impl DlqBackend {
    /// Write a batch of entries to this backend. Called only by the
    /// orchestrator's drain task -- never from a consumer hot path.
    ///
    /// # Errors
    ///
    /// Backend-specific. The orchestrator decides whether to cascade,
    /// fall back, or fan out based on the configured [`super::DlqMode`].
    pub async fn send_batch(&mut self, batch: &[DlqEntry]) -> Result<(), DlqError> {
        match self {
            Self::File(b) => b.send_batch(batch).await,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.send_batch(batch).await,
            #[cfg(feature = "dlq-http")]
            Self::Http(b) => b.send_batch(batch).await,
        }
    }

    /// Make every entry written so far DURABLE.
    ///
    /// Called by the drain at every barrier, so from each
    /// [`super::orchestrator::Dlq::flush`]. Each backend honours the
    /// strongest durability it can express:
    ///
    /// - **File**: `flush()` on the rotating writer. `file-rotate`
    ///   doesn't expose the inner `File`, so we can't `fsync()` -- this
    ///   only flushes to the kernel page cache, so power loss before
    ///   write-back can still lose data. Limitation, tracked until
    ///   `file-rotate` exposes a sync hook.
    /// - **Kafka**: waits on the blocking pool, up to `kafka.send_timeout_ms`,
    ///   for the broker to ack every queued entry (per the producer's `acks`
    ///   config), then purges what is left. The entries only Kafka held that
    ///   the broker refused or never acked are handed back for the drain to
    ///   offer to the next backend in cascade mode with a backend after
    ///   Kafka, and counted lost otherwise.
    /// - **HTTP**: no-op. `send_batch` already awaits the response.
    ///
    /// # Errors
    ///
    /// Backend-specific. The barrier returns it from the `Dlq::flush`
    /// that issued it.
    pub async fn flush_durable(&mut self) -> Result<(), DlqError> {
        match self {
            Self::File(b) => b.flush_durable().await,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.flush_durable().await,
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => Ok(()),
        }
    }

    /// Entries the last failed `send_batch` handed on before it stopped.
    /// Only Kafka queues entry by entry; the others fail a batch whole.
    #[allow(clippy::match_same_arms, reason = "only Kafka can fail part-way")]
    pub(crate) fn queued_before_failure(&self) -> usize {
        match self {
            Self::File(_) => 0,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.queued_before_failure(),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => 0,
        }
    }

    /// Record that another backend also holds `entries` of the last batch,
    /// so their loss here is not a loss of the dead letter.
    #[cfg_attr(
        not(feature = "dlq-kafka"),
        allow(unused_variables, reason = "only Kafka tracks custody")
    )]
    #[allow(clippy::match_same_arms, reason = "only Kafka tracks custody")]
    pub(crate) fn share_custody(&mut self, entries: usize) {
        match self {
            Self::File(_) => {}
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.share_custody(entries),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => {}
        }
    }

    /// Entries this backend's durable flushes found lost since the last
    /// call. Only Kafka learns of a loss after the write returned.
    #[allow(clippy::match_same_arms, reason = "only Kafka loses after a write")]
    pub(crate) fn take_durable_losses(&mut self) -> u64 {
        match self {
            Self::File(_) => 0,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.take_durable_losses(),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => 0,
        }
    }

    /// Entries this backend's broker never acked, grouped under the
    /// `dlq_cascade_fallthrough_total` reason, for the drain to offer to the
    /// backends after it. Only Kafka hands any back, and only in cascade mode.
    #[allow(clippy::match_same_arms, reason = "only Kafka hands entries back")]
    pub(crate) fn take_returned(&mut self) -> Vec<(&'static str, Vec<DlqEntry>)> {
        match self {
            Self::File(_) => Vec::new(),
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.take_returned(),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => Vec::new(),
        }
    }

    /// Entries in this backend's hands whose fate it has not heard, without
    /// handing them over. Only Kafka holds entries past a write, and after a
    /// durable flush only those it purged and has no report for yet.
    #[allow(clippy::match_same_arms, reason = "only Kafka holds past a write")]
    pub(crate) fn unsettled(&self) -> u64 {
        match self {
            Self::File(_) => 0,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.unsettled(),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => 0,
        }
    }

    /// Entries still in this backend's hands whose fate is unknown, handed
    /// over as lost when the drain closes. Only Kafka holds entries past a
    /// write.
    #[allow(clippy::match_same_arms, reason = "only Kafka holds past a write")]
    pub(crate) fn take_unconfirmed(&mut self) -> u64 {
        match self {
            Self::File(_) => 0,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => b.take_unconfirmed(),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => 0,
        }
    }

    /// The largest serialised entry this backend can ever hold, or `None` when
    /// it has no ceiling of its own.
    #[allow(clippy::match_same_arms, reason = "only Kafka has a ceiling")]
    pub(crate) fn entry_ceiling(&self) -> Option<usize> {
        match self {
            Self::File(_) => None,
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(b) => Some(b.entry_ceiling()),
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => None,
        }
    }

    /// Backend name for log / metric labels.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            #[cfg(feature = "dlq-kafka")]
            Self::Kafka(_) => "kafka",
            #[cfg(feature = "dlq-http")]
            Self::Http(_) => "http",
        }
    }
}
