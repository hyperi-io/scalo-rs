// Project:   scalo
// File:      src/transport/grpc/queue_bytes.rs
// Purpose:   The payload bytes a gRPC listener holds in its receive queue
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Payload bytes pushed into the receive queue and not yet taken by `recv`,
//! bounded so a pusher's large messages can't outgrow what the record count
//! suggests.

use std::sync::atomic::{AtomicU64, Ordering};

/// The queued payload bytes, against a limit.
#[derive(Debug)]
pub(crate) struct QueueBytes {
    queued: AtomicU64,
    limit: u64,
}

impl QueueBytes {
    /// A budget of `limit` bytes. `0` turns it off.
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            queued: AtomicU64::new(0),
            limit,
        }
    }

    /// Count `bytes` in, or `false` when they would pass the limit. An empty
    /// queue always admits, so a request over the limit alone still lands.
    pub(crate) fn admit(&self, bytes: u64) -> bool {
        if self.limit == 0 {
            return true;
        }
        self.queued
            .try_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                (queued == 0 || queued.saturating_add(bytes) <= self.limit)
                    .then(|| queued.saturating_add(bytes))
            })
            .is_ok()
    }

    /// Count `bytes` out, once `recv` has taken them or a send was refused.
    pub(crate) fn release(&self, bytes: u64) {
        if self.limit == 0 || bytes == 0 {
            return;
        }
        self.queued
            .update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                queued.saturating_sub(bytes)
            });
    }

    /// The bytes queued now.
    #[cfg(test)]
    fn queued(&self) -> u64 {
        self.queued.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Admits until the limit, always admits into an empty queue, frees what
    /// is released, and admits everything with the limit off.
    #[test]
    fn admits_to_the_limit_and_frees_on_release() {
        let budget = QueueBytes::new(100);
        assert!(budget.admit(60));
        assert!(budget.admit(40), "exactly at the limit");
        assert!(!budget.admit(1), "past the limit");
        budget.release(60);
        assert!(budget.admit(60));
        budget.release(100);
        assert_eq!(budget.queued(), 0);
        assert!(budget.admit(500), "an empty queue takes one over the limit");
        assert!(!budget.admit(1));
        budget.release(1_000);
        assert_eq!(budget.queued(), 0, "an over-release floors at zero");

        let off = QueueBytes::new(0);
        assert!(off.admit(u64::MAX));
        assert!(off.admit(u64::MAX));
    }
}
