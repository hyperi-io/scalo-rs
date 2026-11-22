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

mod auto_fanout;
mod batch_per_destination;
mod grpc_coalesce;
mod inline_sequential;
mod merge;
mod parallel_fanout;
mod parallel_pinned;
mod per_record_futures;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::memory::MemoryGuard;
use crate::transport::{Record, SendResult, TransportSender};

pub use auto_fanout::AutoFanout;
pub use batch_per_destination::BatchPerDestination;
pub use grpc_coalesce::GrpcCoalesce;
pub use inline_sequential::InlineSequential;
pub use merge::{Merge, MergeStore};
pub use parallel_fanout::ParallelFanout;
pub use parallel_pinned::ParallelPinned;
pub use per_record_futures::PerRecordFutures;

/// Index of a destination in a [`Destinations`] set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DestinationId(pub usize);

/// How a destination takes records, which an approach that treats the two
/// kinds apart reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// One call carries a batch, as a gRPC `RouteBatch` does.
    Batch,
    /// Records are queued one by one and confirmed on their own, as a Kafka
    /// producer does.
    PerRecord,
}

/// The named destinations a batch can be routed to, and the memory guard every
/// delivery leases its bytes from.
pub struct Destinations<S> {
    entries: Vec<(Arc<str>, Arc<S>, Shape)>,
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
        self.add_shaped(name, sender, Shape::Batch)
    }

    /// Add `sender` under `name` with the shape it takes records in.
    pub fn add_shaped(&mut self, name: &str, sender: S, shape: Shape) -> DestinationId {
        self.entries
            .push((Arc::from(name), Arc::new(sender), shape));
        DestinationId(self.entries.len() - 1)
    }

    /// The destination's shape.
    ///
    /// # Panics
    ///
    /// When `id` is not from this set.
    #[must_use]
    pub fn shape(&self, id: DestinationId) -> Shape {
        self.entries[id.0].2
    }

    /// The id of the destination called `name`.
    #[must_use]
    pub fn id(&self, name: &str) -> Option<DestinationId> {
        self.entries
            .iter()
            .position(|(n, _, _)| &**n == name)
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

    /// The destination's sender, shared, for an approach that sends from a task
    /// of its own.
    ///
    /// # Panics
    ///
    /// When `id` is not from this set.
    #[must_use]
    pub fn sender_arc(&self, id: DestinationId) -> Arc<S> {
        Arc::clone(&self.entries[id.0].1)
    }

    /// Every destination, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (DestinationId, &str, &S)> {
        self.entries
            .iter()
            .enumerate()
            .map(|(i, (name, sender, _))| (DestinationId(i), &**name, &**sender))
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
            SendResult::FilteredDlq => Self::DeadLettered,
            SendResult::Backpressured => Self::Backpressured,
            SendResult::Fatal(_) => Self::Failed,
        }
    }

    /// Whether the record was not handled and is the caller's to offer again.
    #[must_use]
    pub fn is_undelivered(self) -> bool {
        self >= Self::Backpressured
    }
}

/// The records of `batch` routed to `id` that `sender` would take, with their
/// places in the batch. Those it would dead-letter are marked in `outcomes`.
fn block_for<S: TransportSender>(
    batch: &CallerBatch,
    id: DestinationId,
    sender: &S,
    outcomes: &mut [Outcome],
) -> (Vec<Record>, Vec<usize>) {
    let split = split_for(batch, id, sender);
    for i in split.dead_lettered {
        outcomes[i] = outcomes[i].max(Outcome::DeadLettered);
    }
    (split.block, split.places)
}

/// One destination's share of a caller batch.
struct Split {
    block: Vec<Record>,
    places: Vec<usize>,
    dead_lettered: Vec<usize>,
}

/// Split `batch` for `id`: the records `sender` would take, their places, and
/// the places of those it would dead-letter. Sized up front, as
/// [`InlineSequential`] sizes its block.
fn split_for<S: TransportSender>(batch: &CallerBatch, id: DestinationId, sender: &S) -> Split {
    let routed = batch
        .records
        .iter()
        .filter(|r| r.routes.contains(&id))
        .count();
    let mut split = Split {
        block: Vec::with_capacity(routed),
        places: Vec::with_capacity(routed),
        dead_lettered: Vec::new(),
    };
    for (i, routed) in batch.records.iter().enumerate() {
        if !routed.routes.contains(&id) {
            continue;
        }
        if sender.dead_letter_reason(&routed.record).is_some() {
            split.dead_lettered.push(i);
        } else {
            split.block.push(routed.record.clone());
            split.places.push(i);
        }
    }
    split
}

/// Send `records` in one `send_batch` and return each record's outcome. A
/// `FilteredDlq` answer names no record, so each is then sent on its own.
async fn send_block<S: TransportSender>(sender: &S, records: &[Record]) -> Vec<Outcome> {
    if records.is_empty() {
        return Vec::new();
    }
    let result = sender.send_batch(records).await;
    if matches!(result, SendResult::FilteredDlq) {
        let mut outcomes = Vec::with_capacity(records.len());
        for record in records {
            outcomes.push(Outcome::from_send(&sender.send_record(record).await));
        }
        return outcomes;
    }
    vec![Outcome::from_send(&result); records.len()]
}

/// One destination's send: the places in the batch of its `block`, and the
/// outcome of each record in it. A future built here is large, so an approach
/// that holds several boxes each one rather than let a collection of them
/// move it by value.
pub async fn destination_send<S: TransportSender>(
    sender: &S,
    block: Vec<Record>,
    places: Vec<usize>,
) -> (Vec<usize>, Vec<Outcome>) {
    let outcomes = send_block(sender, &block).await;
    (places, outcomes)
}

/// Fold the results of [`destination_send`] into the batch's outcomes.
fn merge_results(
    outcomes: &mut [Outcome],
    sent: impl IntoIterator<Item = (Vec<usize>, Vec<Outcome>)>,
) {
    for (places, results) in sent {
        for (i, outcome) in places.into_iter().zip(results) {
            outcomes[i] = outcomes[i].max(outcome);
        }
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
    fn deliver<'a, S: TransportSender + 'static>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> impl Future<Output = Vec<Outcome>> + Send + 'a;
}

/// Every approach, dispatched by enum.
#[derive(Debug, Clone)]
pub enum AnyApproach {
    /// [`InlineSequential`].
    InlineSequential(InlineSequential),
    /// [`PerRecordFutures`].
    PerRecordFutures(PerRecordFutures),
    /// [`ParallelFanout`].
    ParallelFanout(ParallelFanout),
    /// [`ParallelPinned`].
    ParallelPinned(ParallelPinned),
    /// [`AutoFanout`].
    AutoFanout(AutoFanout),
    /// [`BatchPerDestination`].
    BatchPerDestination(BatchPerDestination),
    /// [`Merge`], with a channel or a swapped buffer.
    Merge(Merge),
    /// [`GrpcCoalesce`].
    GrpcCoalesce(GrpcCoalesce),
}

impl AnyApproach {
    /// Most payload bytes waiting in a merge buffer at once, for an approach
    /// that has one.
    #[must_use]
    pub fn peak_queued_bytes(&self) -> Option<u64> {
        match self {
            Self::Merge(a) => Some(a.peak_queued_bytes()),
            Self::GrpcCoalesce(a) => Some(a.peak_queued_bytes()),
            _ => None,
        }
    }
}

impl DeliveryApproach for AnyApproach {
    fn name(&self) -> &'static str {
        match self {
            Self::InlineSequential(a) => a.name(),
            Self::PerRecordFutures(a) => a.name(),
            Self::ParallelFanout(a) => a.name(),
            Self::ParallelPinned(a) => a.name(),
            Self::AutoFanout(a) => a.name(),
            Self::BatchPerDestination(a) => a.name(),
            Self::Merge(a) => a.name(),
            Self::GrpcCoalesce(a) => a.name(),
        }
    }

    async fn deliver<'a, S: TransportSender + 'static>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        match self {
            Self::InlineSequential(a) => a.deliver(destinations, batch).await,
            Self::PerRecordFutures(a) => a.deliver(destinations, batch).await,
            Self::ParallelFanout(a) => a.deliver(destinations, batch).await,
            Self::ParallelPinned(a) => a.deliver(destinations, batch).await,
            Self::AutoFanout(a) => a.deliver(destinations, batch).await,
            Self::BatchPerDestination(a) => a.deliver(destinations, batch).await,
            Self::Merge(a) => a.deliver(destinations, batch).await,
            Self::GrpcCoalesce(a) => a.deliver(destinations, batch).await,
        }
    }
}

/// Approach names and their constructors, in the order the bench runs them by
/// default.
pub const REGISTRY: &[(&str, fn() -> AnyApproach)] = &[
    (InlineSequential::NAME, || {
        AnyApproach::InlineSequential(InlineSequential::default())
    }),
    (InlineSequential::CONTINUE, || {
        AnyApproach::InlineSequential(InlineSequential {
            stop_at_failure: false,
        })
    }),
    (PerRecordFutures::NAME, || {
        AnyApproach::PerRecordFutures(PerRecordFutures::default())
    }),
    (ParallelFanout::NAME, || {
        AnyApproach::ParallelFanout(ParallelFanout::default())
    }),
    (ParallelFanout::IN_SEND, || {
        AnyApproach::ParallelFanout(ParallelFanout {
            build_in_send: true,
        })
    }),
    (ParallelPinned::NAME, || {
        AnyApproach::ParallelPinned(ParallelPinned)
    }),
    (AutoFanout::NAME, || AnyApproach::AutoFanout(AutoFanout)),
    (BatchPerDestination::NAME, || {
        AnyApproach::BatchPerDestination(BatchPerDestination)
    }),
    (Merge::CHANNEL, || {
        AnyApproach::Merge(Merge::new(MergeStore::Channel))
    }),
    (Merge::SWAP, || {
        AnyApproach::Merge(Merge::new(MergeStore::Swap))
    }),
    (GrpcCoalesce::NAME, || {
        AnyApproach::GrpcCoalesce(GrpcCoalesce::new())
    }),
];

/// The approach a spec names: a registry name, then for a merge approach
/// `/w<milliseconds>` for its merge window and `/b<bytes>` for its buffer, as
/// in `merge_channel/w1/b262144`, and for `grpc_coalesce` `/t<bytes>` for its
/// byte target, `/l<milliseconds>` for its linger and `/n<count>` for the sends
/// it keeps in flight, as in `grpc_coalesce/t131072/l2/n4`.
#[must_use]
pub fn approach(spec: &str) -> Option<AnyApproach> {
    let mut parts = spec.split('/');
    let name = parts.next()?;
    let mut found = REGISTRY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, make)| make())?;
    for part in parts {
        match &mut found {
            AnyApproach::Merge(merge) => {
                if let Some(ms) = part.strip_prefix('w') {
                    merge.window = milliseconds(ms)?;
                } else {
                    merge.buffer_bytes = part.strip_prefix('b')?.parse().ok()?;
                }
            }
            AnyApproach::GrpcCoalesce(coalesce) => {
                if let Some(bytes) = part.strip_prefix('t') {
                    coalesce.target_bytes = bytes.parse().ok().filter(|b| *b > 0)?;
                } else if let Some(ms) = part.strip_prefix('l') {
                    coalesce.linger = milliseconds(ms)?;
                } else {
                    coalesce.in_flight = part.strip_prefix('n')?.parse().ok().filter(|n| *n > 0)?;
                }
            }
            _ => return None,
        }
    }
    Some(found)
}

/// `text` as a number of milliseconds, never negative.
fn milliseconds(text: &str) -> Option<std::time::Duration> {
    let ms: f64 = text.parse().ok()?;
    (ms >= 0.0 && ms.is_finite()).then(|| std::time::Duration::from_secs_f64(ms / 1000.0))
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
    fn a_coalesce_spec_sets_its_target_linger_and_sends_in_flight() {
        let Some(AnyApproach::GrpcCoalesce(c)) = approach("grpc_coalesce/t65536/l0.5/n8") else {
            panic!("a coalesce spec parses");
        };
        assert_eq!(c.target_bytes, 65536);
        assert_eq!(c.linger, std::time::Duration::from_micros(500));
        assert_eq!(c.in_flight, 8);
        let Some(AnyApproach::GrpcCoalesce(zero)) = approach("grpc_coalesce/t65536/l0/n4") else {
            panic!("a zero linger parses");
        };
        assert!(zero.linger.is_zero());
        for bad in [
            "grpc_coalesce/t0",
            "grpc_coalesce/n0",
            "grpc_coalesce/l-1",
            "grpc_coalesce/x1",
            "inline_sequential/t1",
        ] {
            assert!(approach(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn every_registry_name_builds_the_approach_of_that_name() {
        for (name, _) in REGISTRY {
            assert_eq!(approach(name).map(|a| a.name()), Some(*name));
        }
        assert!(approach("nope").is_none());
    }
}
