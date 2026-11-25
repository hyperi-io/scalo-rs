// Project:   scalo
// File:      src/tiered_sink/acks.rs
// Purpose:   Pieces of work waiting for the sync or drain that makes their spill safe
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pieces of work waiting on a spilled entry.
//!
//! A piece handed to `send_acked` or `send_batch_acked` whose records spill
//! waits here, keyed by where its entry ends. A sync reports every piece up to
//! the end it read `Delivered` once its flush holds, and `Errored` when the
//! flush fails. A drain commit reports every piece up to the committed
//! position `Delivered`, since the sink confirmed them.

use std::collections::VecDeque;

use crate::spool_codec::Waiters;
use crate::transport::{DeliveryStatus, PieceFinalizer};
use crate::yaque::{QueueState, Shared};

/// The pieces waiting, oldest first, each with the end of its entry.
pub(crate) type Waiting = VecDeque<(QueueState, PieceFinalizer)>;

/// Pieces waiting on their spilled entry, in write order.
#[derive(Default)]
pub(crate) struct Acks {
    waiting: parking_lot::Mutex<Waiting>,
}

impl Acks {
    /// The waiting pieces, locked. A writer holds this from its write to
    /// registering the entry's pieces, so a sync that reads the queue's end
    /// under it finds every piece at or below that end registered.
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, Waiting> {
        self.waiting.lock()
    }

    /// Reports `status` for every piece whose entry ends at or below `end`,
    /// outside the lock.
    pub(crate) fn report_through(&self, end: QueueState, status: DeliveryStatus) {
        let done: Vec<PieceFinalizer> = {
            let mut waiting = self.waiting.lock();
            let through = waiting.partition_point(|(at, _)| *at <= end);
            waiting.drain(..through).map(|(_, piece)| piece).collect()
        };
        for piece in done {
            piece.report(status);
        }
    }

    /// Reports every waiting piece `Errored`: the queue holding their entries
    /// was set aside.
    pub(crate) fn fail_all(&self) {
        let done: Vec<(QueueState, PieceFinalizer)> = self.waiting.lock().drain(..).collect();
        for (_, piece) in done {
            piece.report(DeliveryStatus::Errored);
        }
    }
}

impl Waiters for Acks {
    fn end(&self, shared: &Shared) -> Option<QueueState> {
        let _writers = self.waiting.lock();
        shared.published()
    }

    fn settle(&self, end: QueueState, synced: bool) {
        let status = if synced {
            DeliveryStatus::Delivered
        } else {
            DeliveryStatus::Errored
        };
        self.report_through(end, status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::BatchFinalizer;

    fn at(segment: u64, position: u64) -> QueueState {
        QueueState { segment, position }
    }

    /// A piece reports once its end is reached, in write order, across a
    /// segment boundary, and a failure reports every one it reaches Errored.
    #[test]
    fn pieces_report_through_an_end_in_write_order() {
        let (tx, rx) = std::sync::mpsc::channel();
        let acks = Acks::default();
        for (n, end) in [at(0, 10), at(0, 20), at(1, 5), at(1, 9)]
            .into_iter()
            .enumerate()
        {
            let tx = tx.clone();
            let finalizer = BatchFinalizer::new(move |status| tx.send((n, status)).unwrap());
            acks.lock().push_back((end, finalizer.piece()));
            finalizer.seal();
        }
        let reported = || rx.try_iter().collect::<Vec<_>>();
        acks.report_through(at(0, 15), DeliveryStatus::Delivered);
        assert_eq!(reported(), [(0, DeliveryStatus::Delivered)]);
        acks.settle(at(1, 5), false);
        assert_eq!(
            reported(),
            [(1, DeliveryStatus::Errored), (2, DeliveryStatus::Errored)]
        );
        acks.fail_all();
        assert_eq!(reported(), [(3, DeliveryStatus::Errored)]);
        assert!(acks.lock().is_empty());
    }
}
