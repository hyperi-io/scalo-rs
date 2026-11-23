// Project:   scalo
// File:      src/transport/test_support/delivery/mod.rs
// Purpose:   Delivery approaches behind one trait, their registry, leases and outcomes
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Delivery approaches: ways to take one caller's batch of records, each routed
//! to one or more named destinations, and answer every record.
//!
//! An approach implements [`DeliveryApproach`]. [`AnyApproach`] dispatches to
//! each by enum, and [`REGISTRY`] maps a name to a constructor, so the bench
//! and the tests pick approaches by name. Adding one is a file holding its
//! type, a variant and a match arm on [`AnyApproach`], and a registry line.
//!
//! Every approach takes a [`Lease`] from the [`Destinations`]' [`Ledger`] for
//! the batch's payload bytes when the batch is offered, and drops it when the
//! last outcome is known, so a cancelled delivery releases its bytes. The ledger
//! is the only way an approach reaches the memory guard: every byte an approach
//! buffers is leased through it, so its peak is the approach's held memory.
//!
//! Retry is not an approach's concern. An approach answers each record once,
//! and the caller decides whether to offer the refused ones again.
//!
//! A held batch ([`Answer::Held`]) is answered all or nothing: when any record
//! is not delivered, every record not dead-lettered gets the worst outcome.

mod inline_sequential;
mod per_record_futures;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::memory::MemoryGuard;
use crate::transport::{Record, SendResult, TransportSender};

pub use inline_sequential::InlineSequential;
pub use per_record_futures::PerRecordFutures;

/// Index of a destination in a [`Destinations`] set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DestinationId(pub usize);

/// The named destinations a batch can be routed to, and the memory guard every
/// delivery leases its bytes from.
pub struct Destinations<S> {
    entries: Vec<(Arc<str>, S)>,
    ledger: Ledger,
}

impl<S: TransportSender> Destinations<S> {
    /// An empty set leasing from `guard`.
    #[must_use]
    pub fn new(guard: Arc<MemoryGuard>) -> Self {
        Self {
            entries: Vec::new(),
            ledger: Ledger::new(guard),
        }
    }

    /// Add `sender` under `name`, returning its id. Ids follow the order of
    /// adding, which is the order an approach that sends one destination after
    /// another uses.
    pub fn add(&mut self, name: &str, sender: S) -> DestinationId {
        self.entries.push((Arc::from(name), sender));
        DestinationId(self.entries.len() - 1)
    }

    /// The id of the destination called `name`.
    #[must_use]
    pub fn id(&self, name: &str) -> Option<DestinationId> {
        self.entries
            .iter()
            .position(|(n, _)| &**n == name)
            .map(DestinationId)
    }

    /// The destination's name.
    ///
    /// # Panics
    ///
    /// When `id` is not from this set.
    #[must_use]
    pub fn name(&self, id: DestinationId) -> &str {
        &self.entries[id.0].0
    }

    /// The destination's sender.
    ///
    /// # Panics
    ///
    /// When `id` is not from this set.
    #[must_use]
    pub fn sender(&self, id: DestinationId) -> &S {
        &self.entries[id.0].1
    }

    /// Every destination, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (DestinationId, &str, &S)> {
        self.entries
            .iter()
            .enumerate()
            .map(|(i, (name, sender))| (DestinationId(i), &**name, sender))
    }

    /// Number of destinations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The lease ledger.
    #[must_use]
    pub fn ledger(&self) -> &Ledger {
        &self.ledger
    }
}

/// Byte leases on a [`MemoryGuard`], with the most held at once.
pub struct Ledger {
    guard: Arc<MemoryGuard>,
    held: AtomicU64,
    peak: AtomicU64,
}

impl Ledger {
    fn new(guard: Arc<MemoryGuard>) -> Self {
        Self {
            guard,
            held: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        }
    }

    /// Lease `bytes` until the returned [`Lease`] drops.
    #[must_use]
    pub fn lease(&self, bytes: u64) -> Lease<'_> {
        self.guard.add_bytes(bytes);
        let now = self.held.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.peak.fetch_max(now, Ordering::AcqRel);
        Lease {
            ledger: self,
            bytes,
        }
    }

    /// Bytes leased now.
    #[must_use]
    pub fn held_bytes(&self) -> u64 {
        self.held.load(Ordering::Acquire)
    }

    /// Most bytes leased at once since the last [`reset_peak`](Self::reset_peak).
    #[must_use]
    pub fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }

    /// Start the peak again from what is leased now.
    pub fn reset_peak(&self) {
        self.peak.store(self.held_bytes(), Ordering::Release);
    }

    /// Bytes reserved on the guard now, by this ledger's leases and any other.
    #[must_use]
    pub fn guard_reserved_bytes(&self) -> u64 {
        self.guard.reserved_bytes()
    }
}

/// Bytes leased on a [`Ledger`], released when dropped.
pub struct Lease<'a> {
    ledger: &'a Ledger,
    bytes: u64,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.ledger.guard.release(self.bytes);
        self.ledger.held.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// One record and the destinations it is routed to. The record's `key` is the
/// wire key every destination receives.
#[derive(Debug, Clone)]
pub struct RoutedRecord {
    /// The record. Its payload is never copied.
    pub record: Record,
    /// Where it goes. Shared, since a caller's records usually share one route.
    pub routes: Arc<[DestinationId]>,
}

/// Whether a caller holds its answer for the whole batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// The caller gets one answer for the batch: all or nothing.
    Held,
    /// The caller takes each record's outcome on its own.
    PerRecord,
}

/// One caller's offer.
#[derive(Debug, Clone)]
pub struct CallerBatch {
    /// The records, each with its routes.
    pub records: Vec<RoutedRecord>,
    /// How the caller is answered.
    pub answer: Answer,
}

impl CallerBatch {
    /// Payload bytes across the batch.
    #[must_use]
    pub fn payload_bytes(&self) -> u64 {
        self.records
            .iter()
            .map(|r| u64::try_from(r.record.payload.len()).unwrap_or(u64::MAX))
            .sum()
    }
}

/// What a record's caller is told. Ordered from best to worst, so merging the
/// outcomes of a record's destinations is `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Outcome {
    /// Every destination it was routed to confirmed it.
    Sent,
    /// A destination refused it for good. The caller routes it to its DLQ.
    DeadLettered,
    /// A destination could not take it now. The caller may offer it again.
    Backpressured,
    /// A destination failed. The caller may offer it again.
    Failed,
}

impl Outcome {
    /// The outcome a destination's answer gives the records it covered.
    #[must_use]
    pub fn from_send(result: &SendResult) -> Self {
        match result {
            SendResult::Ok => Self::Sent,
            SendResult::FilteredDlq | SendResult::Reject(_) => Self::DeadLettered,
            SendResult::Backpressured | SendResult::RetryAfter(_) => Self::Backpressured,
            SendResult::Fatal(_) => Self::Failed,
        }
    }

    /// Whether the record was not handled and is the caller's to offer again.
    #[must_use]
    pub fn is_undelivered(self) -> bool {
        self >= Self::Backpressured
    }
}

/// Apply the caller's answer rule to per-record outcomes: a held batch with any
/// undelivered record gives every record not dead-lettered the worst outcome.
#[must_use]
pub fn answer(mut outcomes: Vec<Outcome>, answer: Answer) -> Vec<Outcome> {
    if answer == Answer::Held
        && let Some(worst) = outcomes.iter().copied().max()
        && worst.is_undelivered()
    {
        for outcome in &mut outcomes {
            if *outcome != Outcome::DeadLettered {
                *outcome = worst;
            }
        }
    }
    outcomes
}

/// A way to deliver a caller's batch.
pub trait DeliveryApproach: Send + Sync {
    /// The approach's registry name.
    fn name(&self) -> &'static str;

    /// Deliver `batch` to its routes in `destinations` and return one outcome
    /// per record, in the batch's order. Every byte held meanwhile is leased
    /// from [`Destinations::ledger`].
    fn deliver<'a, S: TransportSender>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> impl Future<Output = Vec<Outcome>> + Send + 'a;
}

/// Every approach, dispatched by enum.
#[derive(Debug, Clone, Copy)]
pub enum AnyApproach {
    /// [`InlineSequential`].
    InlineSequential(InlineSequential),
    /// [`PerRecordFutures`].
    PerRecordFutures(PerRecordFutures),
}

impl DeliveryApproach for AnyApproach {
    fn name(&self) -> &'static str {
        match self {
            Self::InlineSequential(a) => a.name(),
            Self::PerRecordFutures(a) => a.name(),
        }
    }

    async fn deliver<'a, S: TransportSender>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        match self {
            Self::InlineSequential(a) => a.deliver(destinations, batch).await,
            Self::PerRecordFutures(a) => a.deliver(destinations, batch).await,
        }
    }
}

/// Approach names and their constructors, in the order the bench runs them by
/// default.
pub const REGISTRY: &[(&str, fn() -> AnyApproach)] = &[
    (InlineSequential::NAME, || {
        AnyApproach::InlineSequential(InlineSequential)
    }),
    (PerRecordFutures::NAME, || {
        AnyApproach::PerRecordFutures(PerRecordFutures::default())
    }),
];

/// The approach registered as `name`.
#[must_use]
pub fn approach(name: &str) -> Option<AnyApproach> {
    REGISTRY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, make)| make())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_batch_answers_all_or_nothing_and_keeps_dead_letters() {
        let got = answer(
            vec![Outcome::Sent, Outcome::DeadLettered, Outcome::Backpressured],
            Answer::Held,
        );
        assert_eq!(
            got,
            [
                Outcome::Backpressured,
                Outcome::DeadLettered,
                Outcome::Backpressured
            ]
        );
        let per_record = answer(vec![Outcome::Sent, Outcome::Failed], Answer::PerRecord);
        assert_eq!(per_record, [Outcome::Sent, Outcome::Failed]);
        let delivered = answer(vec![Outcome::Sent, Outcome::DeadLettered], Answer::Held);
        assert_eq!(delivered, [Outcome::Sent, Outcome::DeadLettered]);
    }

    #[test]
    fn every_registry_name_builds_the_approach_of_that_name() {
        for (name, _) in REGISTRY {
            assert_eq!(approach(name).map(|a| a.name()), Some(*name));
        }
        assert!(approach("nope").is_none());
    }
}
