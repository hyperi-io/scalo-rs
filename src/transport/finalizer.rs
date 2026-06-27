// Project:   scalo
// File:      src/transport/finalizer.rs
// Purpose:   Batch delivery finalizer (effectively-once ack accounting)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Batch delivery finalizer -- worst-status ack accounting for a fanned-out
//! batch.
//!
//! When one input batch fans out to several sub-sends (per-route splits, a
//! transform that grows the record count, a multi-sink tee), the source must be
//! acked only when EVERY piece has reached a terminal-good state. Acking on a
//! partial success silently loses the un-delivered pieces on the next restart.
//!
//! A [`BatchFinalizer`] is created once per input batch with a callback that
//! performs the source ack (commit the offsets / XACK / advance the cursor).
//! Each fanned-out piece takes a [`PieceFinalizer`] handle and, when its send
//! resolves, reports a [`DeliveryStatus`]. The finalizer:
//!
//! - **merges worst-status** monotonically across all pieces (worst wins);
//! - **fires the ack exactly once**, when the LAST piece (and the base seal)
//!   has dropped -- with the merged status;
//! - treats a piece dropped WITHOUT a report (panic, early `?`, forgotten
//!   await) as [`DeliveryStatus::Errored`], so a lost piece can never be
//!   mistaken for success.
//!
//! The callback decides what the merged status means for the source via
//! [`DeliveryStatus::should_commit`]: commit (advance) for
//! Delivered/Dropped/Rejected; do NOT commit for Errored (the whole block is
//! redelivered -- at-least-once). This is the second half of effectively-once:
//! the idempotent producer removes producer-retry duplicates on the wire, the
//! [`dedup_key`](super::Record::dedup_key) lets the sink remove the replay
//! duplicates that at-least-once redelivery can produce.

// NOTE: a loom (concurrency model-checker) pass over these atomics was attempted
// and is blocked by the dependency graph -- `--cfg loom` is global, and tokio
// gates `net` out under loom, breaking net-using dev-deps (bollard/hyper-util
// via testcontainers) that `cargo test` always compiles. Running loom here would
// need a separate minimal-dep test crate. The exactly-once + worst-status
// concurrency is meanwhile covered by the proptest and the multi-thread
// `concurrent_pieces_fire_ack_exactly_once` test below.
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

/// Terminal outcome of delivering a single piece of a batch.
///
/// Ordered by ack-blocking severity (lowest -> highest): `Delivered` < `Dropped`
/// < `Rejected` < `Errored`. The finalizer keeps the MAX. Only `Errored` blocks
/// the source commit (it is the "retry the block" signal); the rest are terminal
/// and let the source advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeliveryStatus {
    /// Successfully delivered to the sink.
    Delivered = 0,
    /// Intentionally dropped (an outbound filter / policy) -- not a loss.
    Dropped = 1,
    /// Permanently rejected and routed to the DLQ -- will not be retried, so the
    /// source may advance (the record is accounted for, just not at the sink).
    Rejected = 2,
    /// Transient failure -- the source must NOT advance; the block is redelivered.
    Errored = 3,
}

impl DeliveryStatus {
    fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Delivered,
            1 => Self::Dropped,
            2 => Self::Rejected,
            _ => Self::Errored,
        }
    }

    /// Whether the source may advance (commit) given this merged outcome.
    ///
    /// True for every terminal outcome except [`Errored`](Self::Errored): an
    /// errored piece means the block is unconfirmed and must be redelivered, so
    /// the commit is withheld (at-least-once: duplicates on replay, never loss).
    #[must_use]
    pub fn should_commit(self) -> bool {
        self != Self::Errored
    }
}

/// Shared finalizer state: the merged worst-status, an outstanding-handle
/// count, and the one-shot ack callback fired when the count reaches zero.
struct Shared {
    /// Merged worst [`DeliveryStatus`] code (monotonic, max-wins).
    worst: AtomicU8,
    /// Live handles: the base seal (+1 at construction) plus every outstanding
    /// [`PieceFinalizer`]. The callback fires when this hits zero.
    outstanding: AtomicUsize,
    /// Fired exactly once with the merged status. `Mutex<Option<..>>` so it can
    /// be taken on the final decrement without `unsafe`.
    on_finalize: Mutex<Option<Box<dyn FnOnce(DeliveryStatus) + Send>>>,
}

impl Shared {
    /// Merge a status (monotonic max-wins) into the shared worst.
    fn merge(&self, status: DeliveryStatus) {
        // fetch_max keeps the highest-severity code seen.
        self.worst.fetch_max(status as u8, Ordering::AcqRel);
    }

    /// Drop one outstanding handle; if it was the last, fire the callback.
    fn release(&self) {
        // fetch_sub returns the PREVIOUS value; 1 -> this was the last handle.
        if self.outstanding.fetch_sub(1, Ordering::AcqRel) == 1 {
            let status = DeliveryStatus::from_code(self.worst.load(Ordering::Acquire));
            if let Some(cb) = self
                .on_finalize
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                cb(status);
            }
        }
    }
}

/// Per-batch finalizer. Hand out a [`PieceFinalizer`] per fanned-out piece, then
/// [`seal`](Self::seal) when no more pieces will be created. The ack callback
/// fires once, after the last piece AND the seal have completed.
pub struct BatchFinalizer {
    shared: Arc<Shared>,
}

impl BatchFinalizer {
    /// Create a finalizer whose callback performs the source ack with the merged
    /// worst-status. Hold the returned value until all pieces are handed out,
    /// then call [`seal`](Self::seal).
    #[must_use]
    pub fn new<F>(on_finalize: F) -> Self
    where
        F: FnOnce(DeliveryStatus) + Send + 'static,
    {
        Self {
            shared: Arc::new(Shared {
                // Start at the best status; pieces only ever raise it.
                worst: AtomicU8::new(DeliveryStatus::Delivered as u8),
                // The base seal is the first outstanding holder.
                outstanding: AtomicUsize::new(1),
                on_finalize: Mutex::new(Some(Box::new(on_finalize))),
            }),
        }
    }

    /// Take a handle for one fanned-out piece. The piece MUST report its outcome
    /// (or be dropped, which counts as [`DeliveryStatus::Errored`]).
    #[must_use]
    pub fn piece(&self) -> PieceFinalizer {
        self.shared.outstanding.fetch_add(1, Ordering::AcqRel);
        PieceFinalizer {
            shared: Arc::clone(&self.shared),
            reported: false,
        }
    }

    /// Seal the batch: signals that no further pieces will be created. Releases
    /// the base holder; the ack fires now if all pieces have already resolved,
    /// otherwise when the last one does.
    pub fn seal(self) {
        // The Arc refcount and the `outstanding` count are independent: release()
        // decrements `outstanding` (the ack trigger); when `self` then drops, its
        // Arc clone is released normally (keeping `Shared` alive while any
        // PieceFinalizer still holds a clone). No double-decrement.
        self.shared.release();
    }
}

/// Handle for a single fanned-out piece. Report the outcome with
/// [`report`](Self::report); if dropped without a report it counts as
/// [`DeliveryStatus::Errored`] so a lost piece never acks as success.
pub struct PieceFinalizer {
    shared: Arc<Shared>,
    reported: bool,
}

impl PieceFinalizer {
    /// Report this piece's terminal outcome, merging it (worst-wins) into the
    /// batch. Consumes the handle.
    pub fn report(mut self, status: DeliveryStatus) {
        self.shared.merge(status);
        self.reported = true;
        // `self` drops here -> Drop sees reported = true and just releases.
    }
}

impl Drop for PieceFinalizer {
    fn drop(&mut self) {
        // A piece dropped without an explicit report is a lost delivery: merge
        // Errored so the batch cannot ack as success.
        if !self.reported {
            self.shared.merge(DeliveryStatus::Errored);
        }
        self.shared.release();
    }
}

impl std::fmt::Debug for BatchFinalizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchFinalizer")
            .field(
                "outstanding",
                &self.shared.outstanding.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PieceFinalizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceFinalizer")
            .field("reported", &self.reported)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// Build a finalizer whose ack delivers the merged status down a channel.
    fn capturing() -> (BatchFinalizer, mpsc::Receiver<DeliveryStatus>) {
        let (tx, rx) = mpsc::channel();
        let fin = BatchFinalizer::new(move |status| {
            tx.send(status).unwrap();
        });
        (fin, rx)
    }

    #[test]
    fn status_ordering_and_commit_rule() {
        assert!(DeliveryStatus::Delivered < DeliveryStatus::Dropped);
        assert!(DeliveryStatus::Dropped < DeliveryStatus::Rejected);
        assert!(DeliveryStatus::Rejected < DeliveryStatus::Errored);
        // Only Errored blocks the source commit.
        assert!(DeliveryStatus::Delivered.should_commit());
        assert!(DeliveryStatus::Dropped.should_commit());
        assert!(DeliveryStatus::Rejected.should_commit());
        assert!(!DeliveryStatus::Errored.should_commit());
    }

    #[test]
    fn all_delivered_acks_delivered() {
        let (fin, rx) = capturing();
        let pieces: Vec<_> = (0..4).map(|_| fin.piece()).collect();
        for p in pieces {
            p.report(DeliveryStatus::Delivered);
        }
        // Not fired until the base is sealed.
        assert!(rx.try_recv().is_err(), "ack must wait for seal");
        fin.seal();
        assert_eq!(rx.recv().unwrap(), DeliveryStatus::Delivered);
    }

    #[test]
    fn one_errored_piece_blocks_commit() {
        let (fin, rx) = capturing();
        let p1 = fin.piece();
        let p2 = fin.piece();
        let p3 = fin.piece();
        p1.report(DeliveryStatus::Delivered);
        p2.report(DeliveryStatus::Errored); // one transient failure
        p3.report(DeliveryStatus::Delivered);
        fin.seal();
        let status = rx.recv().unwrap();
        assert_eq!(status, DeliveryStatus::Errored, "worst wins");
        assert!(
            !status.should_commit(),
            "an errored piece must withhold the ack"
        );
    }

    #[test]
    fn rejected_still_allows_commit() {
        // A permanently-rejected (DLQ'd) piece must NOT block the source: the
        // record is accounted for, so the source advances past it.
        let (fin, rx) = capturing();
        let p1 = fin.piece();
        let p2 = fin.piece();
        p1.report(DeliveryStatus::Delivered);
        p2.report(DeliveryStatus::Rejected);
        fin.seal();
        let status = rx.recv().unwrap();
        assert_eq!(status, DeliveryStatus::Rejected);
        assert!(
            status.should_commit(),
            "DLQ'd record lets the source advance"
        );
    }

    #[test]
    fn dropped_piece_without_report_counts_as_errored() {
        // The crux: a piece dropped WITHOUT reporting (panic / early return /
        // forgotten await) must NOT let the batch ack as success.
        let (fin, rx) = capturing();
        let p1 = fin.piece();
        let p2 = fin.piece();
        p1.report(DeliveryStatus::Delivered);
        drop(p2); // lost piece -- never reported
        fin.seal();
        let status = rx.recv().unwrap();
        assert_eq!(status, DeliveryStatus::Errored, "a lost piece is Errored");
        assert!(!status.should_commit(), "lost piece must block the ack");
    }

    #[test]
    fn seal_before_pieces_resolve_defers_ack() {
        // Sealing while a piece is still outstanding must NOT fire early; the ack
        // waits for the last piece to resolve.
        let (fin, rx) = capturing();
        let p = fin.piece();
        fin.seal();
        assert!(rx.try_recv().is_err(), "outstanding piece defers the ack");
        p.report(DeliveryStatus::Delivered);
        assert_eq!(rx.recv().unwrap(), DeliveryStatus::Delivered);
    }

    #[test]
    fn empty_batch_acks_delivered_on_seal() {
        // No pieces at all: sealing fires immediately with the best status (an
        // empty fan-out is trivially fully delivered).
        let (fin, rx) = capturing();
        fin.seal();
        assert_eq!(rx.recv().unwrap(), DeliveryStatus::Delivered);
    }

    use proptest::prelude::*;

    proptest! {
        /// For ANY mix of piece outcomes -- each piece either reports one of the
        /// four statuses, or is dropped WITHOUT reporting (code 4 -> Errored) --
        /// the finalizer acks the worst (max) status, and fires exactly once.
        #[test]
        fn finalizer_acks_the_worst_status(actions in prop::collection::vec(0u8..=4, 0..64)) {
            let (tx, rx) = mpsc::channel();
            let fin = BatchFinalizer::new(move |s| {
                tx.send(s).unwrap();
            });

            let mut expected = DeliveryStatus::Delivered; // best until raised
            for &code in &actions {
                let p = fin.piece();
                if code <= 3 {
                    let st = DeliveryStatus::from_code(code);
                    p.report(st);
                    expected = expected.max(st);
                } else {
                    // Dropped without report -> counts as Errored.
                    drop(p);
                    expected = expected.max(DeliveryStatus::Errored);
                }
            }
            fin.seal();

            let got = rx.recv().expect("ack must fire once");
            prop_assert_eq!(got, expected);
            // Exactly once: no second ack.
            prop_assert!(rx.try_recv().is_err(), "ack must fire exactly once");
            prop_assert_eq!(got.should_commit(), expected != DeliveryStatus::Errored);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_pieces_fire_ack_exactly_once() {
        use std::sync::atomic::AtomicUsize;
        let fired = Arc::new(AtomicUsize::new(0));
        let fired2 = Arc::clone(&fired);
        let fin = BatchFinalizer::new(move |_status| {
            fired2.fetch_add(1, Ordering::SeqCst);
        });

        // 64 pieces resolve concurrently across threads.
        let mut handles = Vec::new();
        for i in 0..64 {
            let p = fin.piece();
            handles.push(tokio::spawn(async move {
                let status = if i % 2 == 0 {
                    DeliveryStatus::Delivered
                } else {
                    DeliveryStatus::Dropped
                };
                p.report(status);
            }));
        }
        fin.seal();
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "the ack callback must fire exactly once"
        );
    }
}
