// Project:   scalo
// File:      src/dlq/error.rs
// Purpose:   DLQ error types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Error types for the DLQ module.

use std::hash::Hasher as _;

use thiserror::Error;

use super::entry::DlqEntry;

/// Errors from DLQ operations. Matches need a wildcard arm, as variants are
/// added.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DlqError {
    /// File backend I/O error.
    #[error("file DLQ I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialisation error.
    #[error("DLQ serialisation error: {0}")]
    Serialization(String),

    /// File backend error (non-I/O).
    #[error("file DLQ error: {0}")]
    File(String),

    /// Kafka backend error.
    #[cfg(feature = "dlq-kafka")]
    #[error("kafka DLQ error: {0}")]
    Kafka(String),

    /// Generic backend error (HTTP).
    #[error("DLQ backend error: {0}")]
    BackendError(String),

    /// All configured backends failed.
    #[error("all DLQ backends failed: {0}")]
    AllBackendsFailed(String),

    /// DLQ is disabled or not configured.
    #[error("DLQ not configured")]
    NotConfigured,

    /// In-memory queue is full. Caller submitted faster than the drain
    /// can write. The entry was rejected.
    #[error("DLQ queue is full")]
    QueueFull,

    /// DLQ has been shut down -- no further entries accepted.
    #[error("DLQ has shut down")]
    Closed,

    /// Every entry lost since the last barrier was refused for its size by
    /// the only backend, so no retry can land it. Every other entry that
    /// write covered is held.
    #[error("{} DLQ entries refused for good: the broker will not take them at their size", .0.len())]
    Refused(RefusedEntries),
}

/// The entries a [`DlqError::Refused`] names, kept as digests of the bytes
/// the backend sent.
#[derive(Debug, Clone, Default)]
pub struct RefusedEntries {
    digests: Vec<u64>,
}

impl RefusedEntries {
    /// How many entries were refused.
    #[must_use]
    pub fn len(&self) -> usize {
        self.digests.len()
    }

    /// Whether none was.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.digests.is_empty()
    }

    /// Whether `entry`, as the DLQ wrote it, is one of the refused entries.
    /// Pass it through [`Dlq::cap`](super::Dlq::cap) first, as the write did.
    #[must_use]
    pub fn contains(&self, entry: &DlqEntry) -> bool {
        serde_json::to_vec(entry).is_ok_and(|written| self.digests.contains(&digest(&written)))
    }

    /// Add the entry whose serialised bytes are `written`.
    #[cfg(feature = "dlq-kafka")]
    pub(crate) fn push(&mut self, written: &[u8]) {
        self.digests.push(digest(written));
    }
}

/// One digest per serialised entry, the same for both halves of [`RefusedEntries`].
fn digest(written: &[u8]) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    hasher.write(written);
    hasher.finish()
}

#[cfg(all(test, feature = "dlq-kafka"))]
mod tests {
    use super::*;

    /// An entry is named by the bytes it was written as, so a different
    /// payload, reason or destination is a different entry.
    #[test]
    fn a_refused_entry_is_named_by_the_bytes_it_was_written_as() {
        let refused = DlqEntry::new("svc", "oversize", vec![0xff; 64]).with_destination("a.b");
        let mut entries = RefusedEntries::default();
        assert!(entries.is_empty());
        entries.push(&serde_json::to_vec(&refused).expect("serialise"));

        assert_eq!(entries.len(), 1);
        assert!(entries.contains(&refused));
        assert!(
            entries.contains(&refused.clone()),
            "a clone writes the same bytes"
        );
        let mut longer = refused.clone();
        longer.payload.push(0xff);
        let mut renamed = refused.clone();
        renamed.reason = "other".into();
        for other in [longer, renamed, refused.clone().with_destination("a.c")] {
            assert!(!entries.contains(&other), "{other:?}");
        }
    }
}
