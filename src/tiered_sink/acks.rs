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
//! flush fails. A drain commit reports each piece with its entry's outcome:
//! `Delivered` when the sink took its records, `Rejected` or `Dropped` when
//! some were dead letters, `Errored` when the entry failed its CRC.

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

    /// Reports each piece waiting on one of the `drained` entries, given in
    /// read order by where each ends, with that entry's status. A piece at or
    /// below the last that names none of them reports `Errored`, so its
    /// source delivers again rather than lose it.
    pub(crate) fn report_drained(&self, drained: &[(QueueState, DeliveryStatus)]) {
        let Some(&(last, _)) = drained.last() else {
            return;
        };
        let done: Vec<(QueueState, PieceFinalizer)> = {
            let mut waiting = self.waiting.lock();
            let through = waiting.partition_point(|(at, _)| *at <= last);
            waiting.drain(..through).collect()
        };
        for (at, piece) in done {
            let status = drained
                .binary_search_by_key(&at, |(end, _)| *end)
                .map_or(DeliveryStatus::Errored, |found| drained[found].1);
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

    /// A drain reports each piece with its own entry's status, and a piece
    /// below the last entry that names none of them Errored, never Delivered.
    #[test]
    fn drained_pieces_report_their_own_entry_status() {
        let (tx, rx) = std::sync::mpsc::channel();
        let acks = Acks::default();
        for (n, end) in [at(0, 10), at(0, 20), at(0, 25), at(0, 30), at(0, 40)]
            .into_iter()
            .enumerate()
        {
            let tx = tx.clone();
            let finalizer = BatchFinalizer::new(move |status| tx.send((n, status)).unwrap());
            acks.lock().push_back((end, finalizer.piece()));
            finalizer.seal();
        }
        acks.report_drained(&[
            (at(0, 10), DeliveryStatus::Delivered),
            (at(0, 20), DeliveryStatus::Errored),
            (at(0, 30), DeliveryStatus::Rejected),
        ]);
        assert_eq!(
            rx.try_iter().collect::<Vec<_>>(),
            [
                (0, DeliveryStatus::Delivered),
                (1, DeliveryStatus::Errored),
                (2, DeliveryStatus::Errored),
                (3, DeliveryStatus::Rejected),
            ],
            "entry 25 was not among the drained, so it errors"
        );
        assert_eq!(acks.lock().len(), 1, "the piece past the last stays");
    }
}
