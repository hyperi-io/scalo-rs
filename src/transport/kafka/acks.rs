// Project:   scalo
// File:      src/transport/kafka/acks.rs
// Purpose:   Kafka source acknowledgement state: config, arming, held offsets
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka's side of the at-least-once acknowledgement contract.
//!
//! A Kafka commit is cumulative: committing offset N for a partition says every
//! offset below N is done. Releases can arrive out of order -- a hand-rolled
//! loop may hold several blocks at once -- so once armed, the transport records
//! every offset `recv` hands out and commits each partition only up to its
//! lowest offset not yet released. An offset released `Errored` stays held, so
//! the commit never passes it: it is read again after a restart or rebalance.
//!
//! A partition a rebalance takes away is its next owner's to commit. From the
//! revoke until an assignment gives it back, nothing of it is held or
//! committed here, and an offset handed out before the revoke commits nothing
//! when it is released, even after the partition comes back.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::transport::ack::{AckControl, AckKind, AcknowledgementsConfig, HeldAcks, HeldSlot};
use crate::transport::finalizer::DeliveryStatus;

use super::KafkaToken;
use super::metrics::Rebalanced;

/// A partition, keyed as the tokens carry it.
type PartitionKey = (Arc<str>, i32);

/// A set of offsets, kept as ascending half-open ranges that neither overlap
/// nor touch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Offsets {
    ranges: Vec<(i64, i64)>,
    len: u64,
}

/// The count of offsets in `start..end`.
fn span_len(start: i64, end: i64) -> u64 {
    u64::try_from(end.saturating_sub(start)).unwrap_or(0)
}

impl Offsets {
    fn len(&self) -> u64 {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    fn first(&self) -> Option<i64> {
        self.ranges.first().map(|r| r.0)
    }

    /// Add `start..end`, which starts at or above every offset in the set.
    fn push(&mut self, start: i64, end: i64) {
        if start >= end {
            return;
        }
        match self.ranges.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => self.ranges.push((start, end)),
        }
        self.len += span_len(start, end);
    }

    /// The index of the first range that ends above `offset`.
    fn above(&self, offset: i64) -> usize {
        self.ranges.partition_point(|r| r.1 <= offset)
    }

    fn contains(&self, offset: i64) -> bool {
        self.ranges
            .get(self.above(offset))
            .is_some_and(|r| r.0 <= offset)
    }

    /// Add `offset`. `true` when it was not in the set.
    fn insert(&mut self, offset: i64) -> bool {
        let at = self.above(offset);
        if self.ranges.get(at).is_some_and(|r| r.0 <= offset) {
            return false;
        }
        let joins_below = at > 0 && self.ranges[at - 1].1 == offset;
        let joins_above = self.ranges.get(at).is_some_and(|r| r.0 == offset + 1);
        match (joins_below, joins_above) {
            (true, true) => {
                self.ranges[at - 1].1 = self.ranges[at].1;
                self.ranges.remove(at);
            }
            (true, false) => self.ranges[at - 1].1 = offset + 1,
            (false, true) => self.ranges[at].0 = offset,
            (false, false) => self.ranges.insert(at, (offset, offset + 1)),
        }
        self.len += 1;
        true
    }

    /// Take `offset` out. `true` when it was in the set.
    fn remove(&mut self, offset: i64) -> bool {
        let at = self.above(offset);
        let Some(&(start, end)) = self.ranges.get(at).filter(|r| r.0 <= offset) else {
            return false;
        };
        match (start == offset, end == offset + 1) {
            (true, true) => {
                self.ranges.remove(at);
            }
            (true, false) => self.ranges[at].0 = offset + 1,
            (false, true) => self.ranges[at].1 = offset,
            (false, false) => {
                self.ranges[at].1 = offset;
                self.ranges.insert(at + 1, (offset + 1, end));
            }
        }
        self.len -= 1;
        true
    }

    /// Take out every offset at or above `from`.
    fn remove_from(&mut self, from: i64) {
        let keep = self.ranges.partition_point(|r| r.0 < from);
        for (start, end) in self.ranges.drain(keep..) {
            self.len -= span_len(start, end);
        }
        if let Some(last) = self.ranges.last_mut()
            && last.1 > from
        {
            self.len -= span_len(from, last.1);
            last.1 = from;
        }
    }

    /// Take out every offset below `below`.
    fn remove_below(&mut self, below: i64) {
        let gone = self.above(below);
        for (start, end) in self.ranges.drain(..gone) {
            self.len -= span_len(start, end);
        }
        if let Some(first) = self.ranges.first_mut()
            && first.0 < below
        {
            self.len -= span_len(first.0, below);
            first.0 = below;
        }
    }

    /// Add every offset of `other`.
    fn union(&mut self, other: &Self) {
        let mut all: Vec<(i64, i64)> = self.ranges.iter().chain(&other.ranges).copied().collect();
        all.sort_unstable();
        let mut merged = Self::default();
        for (start, end) in all {
            match merged.ranges.last_mut() {
                Some(last) if start <= last.1 => {
                    if end > last.1 {
                        merged.len += span_len(last.1, end);
                        last.1 = end;
                    }
                }
                _ => merged.push(start, end),
            }
        }
        *self = merged;
    }

    /// The offsets of this set not in `other`.
    fn difference(&self, other: &Self) -> Self {
        let mut out = Self::default();
        let mut from = 0;
        for &(start, end) in &self.ranges {
            while other.ranges.get(from).is_some_and(|r| r.1 <= start) {
                from += 1;
            }
            let mut at = start;
            for &(cut_start, cut_end) in other.ranges[from..].iter().take_while(|r| r.0 < end) {
                out.push(at, cut_start.min(end));
                at = at.max(cut_end);
            }
            out.push(at, end);
        }
        out
    }
}

/// Offsets of one partition that `recv` handed out across one or more
/// receives, each one starting at or above where the one before it ended.
#[derive(Debug)]
struct Run {
    start: i64,
    /// One past the last offset handed out.
    end: i64,
    /// Offsets in `start..end` no receive handed out: compaction and
    /// transaction markers leave them.
    gaps: Offsets,
    unreleased: Offsets,
    /// Offsets released `Errored` and not released since, all unreleased.
    withheld: Offsets,
    /// Payload bytes of the offsets in `start..end` outside `gaps`.
    bytes: u64,
    /// When the receive that handed out `start` arrived.
    received: Instant,
    /// Each later receive taken into the run: its first offset less `start`,
    /// and the milliseconds it arrived after `received`.
    merged: Vec<(u32, u32)>,
}

impl Run {
    /// One receive's offsets of one partition, ascending and distinct, with
    /// their payload bytes. `None` when there are none.
    fn new(offsets: &[i64], bytes: u64, received: Instant) -> Option<Self> {
        let (&first, &last) = (offsets.first()?, offsets.last()?);
        let mut run = Self {
            start: first,
            end: last + 1,
            gaps: Offsets::default(),
            unreleased: Offsets::default(),
            withheld: Offsets::default(),
            bytes,
            received,
            merged: Vec::new(),
        };
        let mut from = first;
        for pair in offsets.windows(2) {
            if pair[1] != pair[0] + 1 {
                run.unreleased.push(from, pair[0] + 1);
                run.gaps.push(pair[0] + 1, pair[1]);
                from = pair[1];
            }
        }
        run.unreleased.push(from, last + 1);
        Some(run)
    }

    fn first(&self) -> i64 {
        self.start
    }

    fn last(&self) -> i64 {
        self.end - 1
    }

    /// Offsets in `start..end` a receive handed out.
    fn handed_out(&self) -> u64 {
        span_len(self.start, self.end) - self.gaps.len()
    }

    /// The payload bytes still held, as the unreleased share of the run's.
    fn unreleased_bytes(&self) -> u64 {
        let (held, all) = (self.unreleased.len(), self.handed_out());
        if held == all {
            return self.bytes;
        }
        share(self.bytes, held, all)
    }

    /// When the receive that handed out `offset` arrived.
    fn received_at(&self, offset: i64) -> Instant {
        let after = self
            .merged
            .partition_point(|&(from, _)| self.start + i64::from(from) <= offset);
        match after.checked_sub(1).map(|at| self.merged[at].1) {
            Some(ms) => self.received + std::time::Duration::from_millis(u64::from(ms)),
            None => self.received,
        }
    }

    fn release(&mut self, offset: i64) {
        if self.unreleased.remove(offset) {
            self.withheld.remove(offset);
        }
    }

    /// Mark `offset` released `Errored`: it stays held.
    fn withhold(&mut self, offset: i64) {
        if self.unreleased.contains(offset) {
            self.withheld.insert(offset);
        }
    }

    fn first_unreleased(&self) -> Option<i64> {
        self.unreleased.first()
    }

    /// Drop every offset at or above `from`.
    fn truncate_from(&mut self, from: i64) {
        if from >= self.end {
            return;
        }
        let before = self.handed_out();
        self.end = from.max(self.start);
        self.gaps.remove_from(from);
        self.unreleased.remove_from(from);
        self.withheld.remove_from(from);
        self.bytes = share(self.bytes, self.handed_out(), before);
        let start = self.start;
        self.merged
            .retain(|&(first, _)| start + i64::from(first) < from);
    }

    /// Drop the released offsets below the first unreleased one, so a run
    /// that keeps taking receives holds no more than what is unreleased.
    fn trim_released(&mut self) {
        let Some(first) = self.first_unreleased().filter(|&f| f > self.start) else {
            return;
        };
        let before = self.handed_out();
        let received = self.received_at(first);
        let cut = first - self.start;
        let gone = self
            .merged
            .partition_point(|&(from, _)| i64::from(from) <= cut);
        let base_ms = gone.checked_sub(1).map_or(0, |at| self.merged[at].1);
        self.merged.drain(..gone);
        for (from, ms) in &mut self.merged {
            *from = u32::try_from(i64::from(*from) - cut).unwrap_or(u32::MAX);
            *ms -= base_ms;
        }
        self.gaps.remove_below(first);
        self.start = first;
        self.received = received;
        self.bytes = share(self.bytes, self.handed_out(), before);
    }

    /// Take `next`, which starts at or above this run's end, into this run.
    /// `Err` gives it back when its start or arrival is past what `merged`
    /// can record.
    fn absorb(&mut self, next: Self) -> Result<(), Self> {
        self.trim_released();
        let from = u32::try_from(next.start - self.start);
        let ms = u32::try_from(
            next.received
                .saturating_duration_since(self.received)
                .as_millis(),
        );
        let (Ok(from), Ok(ms)) = (from, ms) else {
            return Err(next);
        };
        self.gaps.push(self.end, next.start);
        for &(start, end) in &next.gaps.ranges {
            self.gaps.push(start, end);
        }
        for &(start, end) in &next.unreleased.ranges {
            self.unreleased.push(start, end);
        }
        self.end = next.end;
        self.bytes = self.bytes.saturating_add(next.bytes);
        self.merged.push((from, ms));
        Ok(())
    }
}

/// `total` times `part` over `whole`, rounded down: 0 when `whole` is 0.
fn share(total: u64, part: u64, whole: u64) -> u64 {
    if whole == 0 {
        return 0;
    }
    u64::try_from(u128::from(total) * u128::from(part) / u128::from(whole)).unwrap_or(u64::MAX)
}

/// Held offsets of one partition, and where its commit stands.
#[derive(Debug, Default)]
struct PartitionHold {
    runs: VecDeque<Run>,
    /// Highest offset released, held here or not.
    highest_released: Option<i64>,
    /// The next-to-read offset last committed. Commits never go below it.
    committed_next: Option<i64>,
    /// Revoked and not assigned again: nothing is held or committed for it.
    unowned: bool,
    /// Offsets handed out before the last revoke and not released or withheld
    /// since. The next owner reads them again, so one release of each is the
    /// old copy's and commits nothing, even once the partition comes back.
    stale: Offsets,
}

impl PartitionHold {
    /// Record a run `recv` handed out. A run that starts at or below an offset
    /// already held means the consumer read again from there, so the held
    /// offsets from there on are replaced. One that starts at or above the
    /// last run's end is taken into that run.
    fn register(&mut self, run: Run) {
        let first = run.first();
        // The consumer reads from its committed offset, so nothing below the
        // first offset handed out needs committing.
        self.committed_next.get_or_insert(first);
        while self.runs.back().is_some_and(|r| r.first() >= first) {
            self.runs.pop_back();
        }
        if let Some(back) = self.runs.back_mut()
            && back.last() >= first
        {
            back.truncate_from(first);
        }
        // A released run tells the commit nothing.
        if self.runs.back().is_some_and(|r| r.unreleased.is_empty()) {
            self.runs.pop_back();
        }
        let run = match self.runs.back_mut() {
            Some(back) => back.absorb(run).err(),
            None => Some(run),
        };
        self.runs.extend(run);
    }

    /// The held run that covers `offset`. Runs are sorted and don't overlap.
    fn run_at(&self, offset: i64) -> Option<&Run> {
        let at = self.runs.partition_point(|r| r.last() < offset);
        self.runs.get(at).filter(|r| r.first() <= offset)
    }

    fn release(&mut self, offset: i64) {
        self.highest_released = Some(self.highest_released.map_or(offset, |h| h.max(offset)));
        let at = self.runs.partition_point(|r| r.last() < offset);
        if let Some(run) = self.runs.get_mut(at) {
            run.release(offset);
        }
    }

    /// The next-to-read offset every held offset below has been released
    /// for, when it is past the last commit.
    fn commit_target(&mut self) -> Option<i64> {
        while self.runs.front().is_some_and(|r| r.unreleased.is_empty()) {
            self.runs.pop_front();
        }
        let target = match self.runs.front() {
            Some(front) => front.first_unreleased()?,
            None => self.highest_released? + 1,
        };
        (self.committed_next.is_none_or(|c| target > c)).then_some(target)
    }
}

/// The acknowledgement state a [`KafkaTransport`](super::KafkaTransport) holds.
#[derive(Debug, Default)]
pub(super) struct KafkaAcks {
    config: AcknowledgementsConfig,
    armed: AtomicBool,
    partitions: parking_lot::Mutex<HashMap<PartitionKey, PartitionHold>>,
    /// Serialises commits so a lower target never lands after a higher one.
    commit_serial: tokio::sync::Mutex<()>,
    held_slot: HeldSlot,
}

impl KafkaAcks {
    pub(super) fn set_config(&mut self, config: AcknowledgementsConfig) {
        self.config = config;
    }

    pub(super) fn config(&self) -> AcknowledgementsConfig {
        self.config
    }

    /// Hold the offsets of one `recv`: `(token, payload bytes)` per record.
    pub(super) fn register<'t>(&self, handed_out: impl IntoIterator<Item = (&'t KafkaToken, u64)>) {
        let now = Instant::now();
        let mut by_partition: HashMap<PartitionKey, Vec<(i64, u64)>> = HashMap::new();
        for (token, bytes) in handed_out {
            by_partition
                .entry((Arc::clone(&token.topic), token.partition))
                .or_default()
                .push((token.offset, bytes));
        }
        if by_partition.is_empty() {
            return;
        }
        let mut partitions = self.partitions.lock();
        for (key, mut held) in by_partition {
            held.sort_unstable_by_key(|&(offset, _)| offset);
            held.dedup_by_key(|&mut (offset, _)| offset);
            let hold = partitions.entry(key).or_default();
            // A partition this member lost holds nothing: its next owner reads it.
            if hold.unowned {
                continue;
            }
            let bytes = held.iter().map(|&(_, bytes)| bytes).sum();
            let offsets: Vec<i64> = held.into_iter().map(|(offset, _)| offset).collect();
            if let Some(run) = Run::new(&offsets, bytes, now) {
                hold.register(run);
            }
        }
        drop(partitions);
        self.publish_held();
    }

    /// Wait for any commit in progress. Hold the guard across the next commit.
    pub(super) async fn serialise_commit(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.commit_serial.lock().await
    }

    /// Release `tokens` and return the next-to-read offset each partition may
    /// now commit to.
    pub(super) fn release(
        &self,
        tokens: &[KafkaToken],
        outcome: DeliveryStatus,
    ) -> Vec<(PartitionKey, i64)> {
        let mut partitions = self.partitions.lock();
        let mut oldest: Option<Instant> = None;
        let mut touched: Vec<PartitionKey> = Vec::new();
        for token in tokens {
            let key = (Arc::clone(&token.topic), token.partition);
            let hold = partitions.entry(key.clone()).or_default();
            // A lost partition commits nothing, and a stale offset's first release is the old copy's.
            let stale = hold.stale.remove(token.offset);
            if hold.unowned || stale {
                continue;
            }
            if let Some(run) = hold.run_at(token.offset) {
                let received = run.received_at(token.offset);
                oldest = Some(oldest.map_or(received, |o| o.min(received)));
            }
            hold.release(token.offset);
            if !touched.contains(&key) {
                touched.push(key);
            }
        }
        let targets = touched
            .into_iter()
            .filter_map(|key| {
                let target = partitions.get_mut(&key)?.commit_target()?;
                Some((key, target))
            })
            .collect();
        drop(partitions);
        Self::note_released(tokens.len(), outcome, oldest);
        self.publish_held();
        targets
    }

    /// Record an `Errored` release: the offsets stay held.
    pub(super) fn withhold(&self, tokens: &[KafkaToken]) {
        let oldest = {
            let mut partitions = self.partitions.lock();
            tokens
                .iter()
                .filter_map(|t| {
                    let hold = partitions.get_mut(&(Arc::clone(&t.topic), t.partition))?;
                    let run = hold
                        .runs
                        .iter_mut()
                        .find(|r| r.first() <= t.offset && t.offset <= r.last())?;
                    run.withhold(t.offset);
                    Some(run.received_at(t.offset))
                })
                .min()
        };
        Self::note_released(tokens.len(), DeliveryStatus::Errored, oldest);
        self.publish_held();
    }

    /// Offsets released `Errored` and still held: each pins its partition's
    /// commit until a restart or a revoke.
    #[cfg_attr(
        not(feature = "metrics"),
        allow(dead_code, reason = "published as a gauge only")
    )]
    fn withheld(&self) -> u64 {
        self.partitions
            .lock()
            .values()
            .flat_map(|p| p.runs.iter())
            .map(|r| r.withheld.len())
            .sum()
    }

    /// Apply ownership changes in the order librdkafka served them, and return
    /// the number of each revoked partition's last revoke.
    ///
    /// A revoke ends this member's claim on the partition: its next owner
    /// reads it again from the committed offset, which is below every offset
    /// held here. So the held offsets and the commit floor go, the offsets
    /// handed out and not yet released turn stale, and the partition holds and
    /// commits nothing until an assignment gives it back.
    pub(super) fn rebalanced(&self, changes: Vec<(u64, Rebalanced)>) -> HashMap<PartitionKey, u64> {
        let mut revoked_at = HashMap::new();
        if changes.is_empty() {
            return revoked_at;
        }
        let mut partitions = self.partitions.lock();
        for (number, change) in changes {
            match change {
                Rebalanced::Revoked(lost) => {
                    for (topic, partition) in lost {
                        let key = (Arc::<str>::from(topic), partition);
                        let old = partitions.remove(&key).unwrap_or_default();
                        let mut stale = old.stale;
                        // A withheld offset's holder already released it, so only the rest await a release.
                        for run in &old.runs {
                            stale.union(&run.unreleased.difference(&run.withheld));
                        }
                        partitions.insert(
                            key.clone(),
                            PartitionHold {
                                unowned: true,
                                stale,
                                ..PartitionHold::default()
                            },
                        );
                        revoked_at.insert(key, number);
                    }
                }
                Rebalanced::Assigned(given) => {
                    for (topic, partition) in given {
                        partitions
                            .entry((Arc::<str>::from(topic), partition))
                            .or_default()
                            .unowned = false;
                    }
                }
            }
        }
        drop(partitions);
        self.publish_held();
        revoked_at
    }

    /// Record the commits that landed.
    pub(super) fn committed(&self, targets: &[(PartitionKey, i64)]) {
        let mut partitions = self.partitions.lock();
        for (key, next) in targets {
            let hold = partitions.entry(key.clone()).or_default();
            let next = hold.committed_next.map_or(*next, |c| c.max(*next));
            hold.committed_next = Some(next);
            // A stale offset below the commit can no longer move it.
            hold.stale.remove_below(next);
        }
    }

    /// Count a release, never while unwinding: a recorder that panics then
    /// aborts the process, and an abandoned block is released from a drop.
    fn note_released(count: usize, outcome: DeliveryStatus, oldest: Option<Instant>) {
        #[cfg(feature = "metrics")]
        if !std::thread::panicking() {
            let label = outcome_label(outcome);
            ::metrics::counter!(
                "transport_ack_released_total",
                "transport" => "kafka",
                "outcome" => label
            )
            .increment(count as u64);
            if let Some(oldest) = oldest {
                ::metrics::histogram!(
                    "transport_ack_latency_seconds",
                    "transport" => "kafka",
                    "outcome" => label
                )
                .record(oldest.elapsed().as_secs_f64());
            }
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (count, outcome, oldest);
    }

    /// Publish what is held, and the withheld gauge never while unwinding, as
    /// `note_released`.
    fn publish_held(&self) {
        let held = self.held();
        self.held_slot.publish("kafka", held.count, held.bytes);
        #[cfg(feature = "metrics")]
        if !std::thread::panicking() {
            ::metrics::gauge!("transport_ack_withheld", "transport" => "kafka")
                .set(self.withheld() as f64);
        }
    }
}

/// The `outcome` label of a release.
#[cfg(feature = "metrics")]
fn outcome_label(outcome: DeliveryStatus) -> &'static str {
    match outcome {
        DeliveryStatus::Delivered => "delivered",
        DeliveryStatus::Dropped => "dropped",
        DeliveryStatus::Rejected => "rejected",
        DeliveryStatus::Errored => "errored",
    }
}

impl AckControl for KafkaAcks {
    fn enabled(&self) -> bool {
        self.config.enabled
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    fn kind(&self) -> AckKind {
        AckKind::Pull
    }

    fn held(&self) -> HeldAcks {
        let now = Instant::now();
        let partitions = self.partitions.lock();
        let mut count = 0_u64;
        let mut bytes = 0_u64;
        let mut oldest: Option<Instant> = None;
        for run in partitions.values().flat_map(|p| p.runs.iter()) {
            let Some(first) = run.first_unreleased() else {
                continue;
            };
            count += run.unreleased.len();
            bytes = bytes.saturating_add(run.unreleased_bytes());
            let received = run.received_at(first);
            oldest = Some(oldest.map_or(received, |o| o.min(received)));
        }
        HeldAcks::new(
            count,
            bytes,
            oldest.map(|o| now.saturating_duration_since(o)),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(partition: i32, offset: i64) -> KafkaToken {
        KafkaToken::new(Arc::from("events"), partition, offset)
    }

    fn register(
        acks: &KafkaAcks,
        partition: i32,
        offsets: std::ops::Range<i64>,
    ) -> Vec<KafkaToken> {
        let tokens: Vec<_> = offsets.map(|o| token(partition, o)).collect();
        acks.register(tokens.iter().map(|t| (t, 10)));
        tokens
    }

    fn targets(acks: &KafkaAcks, tokens: &[KafkaToken]) -> Vec<i64> {
        let targets = acks.release(tokens, DeliveryStatus::Delivered);
        acks.committed(&targets);
        targets.into_iter().map(|(_, next)| next).collect()
    }

    #[test]
    fn out_of_order_release_never_commits_past_an_unreleased_offset() {
        let acks = KafkaAcks::default();
        let first = register(&acks, 0, 0..10);
        let second = register(&acks, 0, 10..20);
        let third = register(&acks, 0, 20..30);

        assert!(
            targets(&acks, &third).is_empty(),
            "the newest block released first must not commit past 0..20"
        );
        assert!(targets(&acks, &second).is_empty(), "0..10 is still held");
        assert_eq!(
            targets(&acks, &first),
            vec![30],
            "once the oldest is released, the commit covers all three"
        );
        assert_eq!(acks.held().count, 0);
    }

    #[test]
    fn an_errored_offset_holds_the_commit_below_it() {
        let acks = KafkaAcks::default();
        let first = register(&acks, 0, 0..5);
        let second = register(&acks, 0, 5..10);
        acks.withhold(&first);
        assert!(
            targets(&acks, &second).is_empty(),
            "a later block delivered must not skip an Errored one"
        );
        assert_eq!(acks.held().count, 5);
    }

    #[test]
    fn a_partly_released_block_commits_its_released_prefix() {
        let acks = KafkaAcks::default();
        let tokens = register(&acks, 0, 0..4);
        assert_eq!(targets(&acks, &tokens[..2]), vec![2]);
        assert!(targets(&acks, &tokens[3..]).is_empty(), "offset 2 is held");
        assert_eq!(targets(&acks, &tokens[2..3]), vec![4]);
    }

    #[test]
    fn partitions_commit_independently() {
        let acks = KafkaAcks::default();
        let p0 = register(&acks, 0, 0..3);
        let p1 = register(&acks, 1, 100..103);
        assert_eq!(targets(&acks, &p1), vec![103]);
        assert_eq!(targets(&acks, &p0), vec![3]);
    }

    #[test]
    fn a_reread_replaces_the_offsets_from_where_it_starts() {
        let acks = KafkaAcks::default();
        let _lost = register(&acks, 0, 0..10);
        let reread = register(&acks, 0, 5..10);
        assert_eq!(acks.held().count, 10, "0..5 still held, 5..10 held once");
        assert!(targets(&acks, &reread).is_empty(), "0..5 not released yet");
    }

    #[test]
    fn a_commit_never_goes_back() {
        let acks = KafkaAcks::default();
        let tokens = register(&acks, 0, 0..5);
        assert_eq!(targets(&acks, &tokens), vec![5]);
        assert!(
            targets(&acks, &tokens[..2]).is_empty(),
            "a repeat release below the commit asks for nothing"
        );
    }

    fn change(number: u64, revoked: bool, partition: i32) -> (u64, Rebalanced) {
        let partitions = vec![("events".to_string(), partition)];
        let change = if revoked {
            Rebalanced::Revoked(partitions)
        } else {
            Rebalanced::Assigned(partitions)
        };
        (number, change)
    }

    /// An Errored block on a partition that is revoked, committed past by the
    /// member that took it, then handed back: the old hold must not block the
    /// commits of what is read after.
    #[test]
    fn a_revoked_partition_holds_nothing_when_it_comes_back() {
        let acks = KafkaAcks::default();
        let withheld = register(&acks, 0, 0..10);
        acks.withhold(&withheld);
        let other = register(&acks, 1, 0..5);

        let revoked_at = acks.rebalanced(vec![change(1, true, 0)]);
        assert_eq!(revoked_at.get(&(Arc::from("events"), 0)), Some(&1));
        assert_eq!(acks.held().count, 5, "only partition 1 is still held");
        acks.rebalanced(vec![change(2, false, 0)]);

        // Another member committed partition 0 up to 20 while it had it.
        let reread = register(&acks, 0, 20..30);
        assert_eq!(targets(&acks, &reread), vec![30]);
        assert_eq!(targets(&acks, &other), vec![5], "partition 1 is untouched");
    }

    /// The Errored floor of a revoked partition is gone, so records of it
    /// released after the revoke -- read in the same poll as the revoke, or
    /// held by a loop across it -- must commit nothing: a commit past the
    /// Errored offsets, landing before the next owner fetches its start,
    /// loses them.
    #[test]
    fn a_revoked_partition_commits_nothing_until_it_is_assigned_again() {
        let acks = KafkaAcks::default();
        let withheld = register(&acks, 0, 0..10);
        acks.withhold(&withheld);
        let read_before_the_revoke = register(&acks, 0, 10..15);
        let other = register(&acks, 1, 0..5);

        acks.rebalanced(vec![change(1, true, 0)]);
        assert!(
            targets(&acks, &read_before_the_revoke).is_empty(),
            "no commit past the Errored floor"
        );
        let served_after = register(&acks, 0, 15..20);
        assert_eq!(acks.held().count, 5, "a lost partition holds nothing");
        assert_eq!(targets(&acks, &served_after), [] as [i64; 0]);
        assert_eq!(targets(&acks, &other), vec![5], "partition 1 commits");

        acks.rebalanced(vec![change(2, false, 0)]);
        let reread = register(&acks, 0, 0..20);
        assert_eq!(
            targets(&acks, &reread),
            vec![20],
            "assigned again, it commits what it reads again: the old copies of 10..15 \
             were released while it was lost, so they take no release from the new ones"
        );
    }

    /// Revoked and assigned back in one rebalance, as the eager protocol does:
    /// a block handed out before holds offsets the consumer reads again, and
    /// its release must not release the copies read again.
    #[test]
    fn a_block_held_across_a_revoke_and_reassign_never_releases_the_copy_read_again() {
        let acks = KafkaAcks::default();
        let in_flight = register(&acks, 0, 10..15);
        acks.rebalanced(vec![change(1, true, 0), change(2, false, 0)]);
        let read_again = register(&acks, 0, 5..20);

        assert_eq!(targets(&acks, &in_flight), [] as [i64; 0]);
        assert_eq!(
            targets(&acks, &read_again[..5]),
            vec![10],
            "10..15 read again are still held: the old copies' release did not release them"
        );
        assert_eq!(targets(&acks, &read_again[5..]), vec![20]);
    }

    /// Releases the tokens it holds `Errored` when dropped, as the pipeline's
    /// guard does for a block abandoned by a panic.
    #[cfg(feature = "metrics")]
    struct ReleasedOnDrop<'a> {
        acks: &'a KafkaAcks,
        tokens: Vec<KafkaToken>,
    }

    #[cfg(feature = "metrics")]
    impl Drop for ReleasedOnDrop<'_> {
        fn drop(&mut self) {
            self.acks.withhold(&self.tokens);
            let _ = self.acks.release(&self.tokens, DeliveryStatus::Delivered);
            self.acks.rebalanced(vec![change(9, true, 0)]);
        }
    }

    /// A metric recorder that panics while a release runs from a drop during
    /// an unwind would abort the process: nothing records then.
    #[cfg(feature = "metrics")]
    #[test]
    fn a_release_during_an_unwind_records_no_metric() {
        let acks = KafkaAcks::default();
        let tokens = register(&acks, 0, 0..3);
        let _local = ::metrics::set_default_local_recorder(
            &crate::metrics::test_recorder::PanickingRecorder::ALL,
        );
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ReleasedOnDrop {
                acks: &acks,
                tokens,
            };
            panic!("the block's process panicked");
        }));
        assert!(caught.is_err(), "the first panic is caught, not aborted on");
        assert_eq!(acks.held().count, 0, "the revoke in the drop still applied");
    }

    /// A recorder that keeps the `transport_ack_withheld` gauge and drops the rest.
    #[cfg(feature = "metrics")]
    #[derive(Default)]
    struct WithheldGauge(std::sync::Arc<std::sync::atomic::AtomicU64>);

    #[cfg(feature = "metrics")]
    impl WithheldGauge {
        /// The gauge as the whole count it carries.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        fn count(&self) -> u64 {
            f64::from_bits(self.0.load(Ordering::Acquire)) as u64
        }
    }

    #[cfg(feature = "metrics")]
    impl ::metrics::Recorder for WithheldGauge {
        fn describe_counter(
            &self,
            _: ::metrics::KeyName,
            _: Option<::metrics::Unit>,
            _: ::metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: ::metrics::KeyName,
            _: Option<::metrics::Unit>,
            _: ::metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: ::metrics::KeyName,
            _: Option<::metrics::Unit>,
            _: ::metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            _: &::metrics::Key,
            _: &::metrics::Metadata<'_>,
        ) -> ::metrics::Counter {
            ::metrics::Counter::noop()
        }
        fn register_gauge(
            &self,
            key: &::metrics::Key,
            _: &::metrics::Metadata<'_>,
        ) -> ::metrics::Gauge {
            if key.name() == "transport_ack_withheld" {
                ::metrics::Gauge::from_arc(std::sync::Arc::clone(&self.0))
            } else {
                ::metrics::Gauge::noop()
            }
        }
        fn register_histogram(
            &self,
            _: &::metrics::Key,
            _: &::metrics::Metadata<'_>,
        ) -> ::metrics::Histogram {
            ::metrics::Histogram::noop()
        }
    }

    /// An `Errored` release pins its partition's commit, so the gauge an
    /// alert watches counts it until a revoke drops it.
    #[cfg(feature = "metrics")]
    #[test]
    fn a_withheld_offset_is_counted_until_its_partition_goes() {
        let recorder = WithheldGauge::default();
        let _local = ::metrics::set_default_local_recorder(&recorder);
        let acks = KafkaAcks::default();
        let block = register(&acks, 0, 0..10);
        acks.withhold(&block[3..5]);
        assert_eq!(recorder.count(), 2, "two offsets withheld");

        assert_eq!(targets(&acks, &block[..3]), vec![3]);
        let later = register(&acks, 0, 10..20);
        assert_eq!(targets(&acks, &block[5..]), [] as [i64; 0]);
        assert!(
            targets(&acks, &later).is_empty(),
            "the commit is pinned at the withheld offset"
        );
        assert_eq!(recorder.count(), 2, "still pinned after later releases");

        acks.rebalanced(vec![change(1, true, 0)]);
        assert_eq!(recorder.count(), 0, "a revoke hands them to the next owner");
    }

    #[test]
    fn without_a_revoke_the_withheld_block_holds_the_commit() {
        let acks = KafkaAcks::default();
        let withheld = register(&acks, 0, 0..10);
        acks.withhold(&withheld);
        let reread = register(&acks, 0, 20..30);
        assert!(
            targets(&acks, &reread).is_empty(),
            "an Errored offset this member still owns holds the commit below it"
        );
    }

    #[test]
    fn unregistered_tokens_commit_as_before() {
        let acks = KafkaAcks::default();
        let tokens = [token(0, 7), token(0, 3)];
        assert_eq!(targets(&acks, &tokens), vec![8]);
    }

    /// `Offsets` against a `BTreeSet` doing the same operations.
    #[test]
    fn offsets_match_a_set_of_single_offsets() {
        use std::collections::BTreeSet;
        /// The operations a case applies, in this order, to `{0, 1, 2, 5, 6, 9}`.
        struct Case {
            name: &'static str,
            insert: &'static [i64],
            remove: &'static [i64],
            from: i64,
            below: i64,
            union: &'static [i64],
            minus: &'static [i64],
        }
        let of = |set: &BTreeSet<i64>| -> Offsets {
            let mut offsets = Offsets::default();
            for &o in set {
                offsets.push(o, o + 1);
            }
            offsets
        };
        let start: BTreeSet<i64> = [0, 1, 2, 5, 6, 9].into();
        let none = Case {
            name: "",
            insert: &[],
            remove: &[],
            from: 100,
            below: -1,
            union: &[],
            minus: &[],
        };
        let cases = [
            Case {
                name: "joins two ranges",
                insert: &[3, 4],
                ..none
            },
            Case {
                name: "splits a range",
                remove: &[1, 6],
                minus: &[0],
                ..none
            },
            Case {
                name: "cuts at both ends",
                insert: &[7],
                from: 6,
                below: 1,
                ..none
            },
            Case {
                name: "already in, already out",
                insert: &[0, 9],
                remove: &[3, 42],
                below: -100,
                minus: &[42],
                ..none
            },
            Case {
                name: "union and difference",
                union: &[3, 4, 10, 11],
                minus: &[2, 3, 10],
                ..none
            },
        ];
        for Case {
            name: case,
            insert,
            remove,
            from,
            below,
            union,
            minus,
        } in cases
        {
            let (mut want, mut got) = (start.clone(), of(&start));
            for &o in insert {
                assert_eq!(got.insert(o), want.insert(o), "{case}: insert {o}");
            }
            for &o in remove {
                assert_eq!(got.remove(o), want.remove(&o), "{case}: remove {o}");
            }
            got.remove_from(from);
            want.retain(|&o| o < from);
            got.remove_below(below);
            want.retain(|&o| o >= below);
            let union: BTreeSet<i64> = union.iter().copied().collect();
            got.union(&of(&union));
            want.extend(&union);
            let minus: BTreeSet<i64> = minus.iter().copied().collect();
            let got = got.difference(&of(&minus));
            let want: BTreeSet<i64> = want.difference(&minus).copied().collect();
            assert_eq!(got, of(&want), "{case}");
            assert_eq!(got.len(), want.len() as u64, "{case}: len");
            assert!(want.iter().all(|&o| got.contains(o)), "{case}: contains");
        }
    }

    fn register_offsets(acks: &KafkaAcks, offsets: &[i64]) -> Vec<KafkaToken> {
        let tokens: Vec<_> = offsets.iter().map(|&o| token(0, o)).collect();
        acks.register(tokens.iter().map(|t| (t, 10)));
        tokens
    }

    /// Compaction and transaction markers leave offsets no receive hands out,
    /// inside one receive and between two. The commit steps over them, and
    /// never past an offset still held.
    #[test]
    fn a_gap_in_the_offsets_is_never_held() {
        let acks = KafkaAcks::default();
        let first = register_offsets(&acks, &[0, 1, 2, 5, 6]);
        let second = register_offsets(&acks, &[8, 9]);
        assert_eq!(acks.held().count, 7);
        assert_eq!(acks.held().bytes, 70);

        assert_eq!(
            targets(&acks, &first[..3]),
            vec![5],
            "3 and 4 were never handed out"
        );
        assert!(targets(&acks, &second).is_empty(), "5 and 6 are still held");
        assert_eq!(targets(&acks, &first[3..]), vec![10]);
        assert_eq!(acks.held().count, 0);
    }

    /// Released out of order inside one receive, with one record released
    /// `Errored` in the middle: the commit stops below it for good.
    #[test]
    fn a_withheld_record_mid_run_pins_the_commit_below_it() {
        let acks = KafkaAcks::default();
        let block = register(&acks, 0, 0..10);
        acks.withhold(&block[4..5]);
        assert!(targets(&acks, &block[5..]).is_empty(), "0..5 still held");
        assert_eq!(targets(&acks, &block[..4]), vec![4]);
        assert_eq!(acks.held().count, 1);
        assert_eq!(acks.withheld(), 1);

        let later = register(&acks, 0, 10..20);
        assert!(targets(&acks, &later).is_empty(), "pinned at 4");
        acks.rebalanced(vec![change(1, true, 0)]);
        assert_eq!(acks.withheld(), 0);
        assert_eq!(acks.held().count, 0);
    }

    /// Receives with gaps, released in a scrambled order: after every release
    /// the commit is at or below the lowest offset still held.
    #[test]
    fn the_commit_never_passes_an_unreleased_offset() {
        let acks = KafkaAcks::default();
        let mut tokens = Vec::new();
        for recv in 0..8_i64 {
            // Every seventh offset is a transaction marker.
            let offsets: Vec<i64> = (recv * 20..recv * 20 + 20).filter(|o| o % 7 != 6).collect();
            tokens.extend(register_offsets(&acks, &offsets));
        }
        // A fixed scramble: 37 is coprime to the 138 tokens, so this is a permutation.
        let mut order: Vec<usize> = (0..tokens.len()).collect();
        order.sort_by_key(|&i| (i * 37) % tokens.len());
        let mut held: std::collections::BTreeSet<i64> = tokens.iter().map(|t| t.offset).collect();
        let mut committed = 0;
        for i in order {
            held.remove(&tokens[i].offset);
            if let Some(&next) = targets(&acks, &tokens[i..=i]).last() {
                committed = next;
            }
            let floor = held.first().copied().unwrap_or(i64::MAX);
            assert!(
                committed <= floor,
                "committed {committed} past held {floor}"
            );
        }
        assert_eq!(committed, 160);
    }

    /// A run that keeps taking receives drops its released prefix, along with
    /// the arrival times of the receives in it.
    #[test]
    fn a_run_keeps_only_what_is_unreleased() {
        let at = Instant::now();
        let ms = |n: u64| at + std::time::Duration::from_millis(n);
        let offsets = |range: std::ops::Range<i64>| -> Vec<i64> { range.collect() };
        let mut run = Run::new(&offsets(0..10), 100, at).expect("a run");
        run.absorb(Run::new(&offsets(10..20), 100, ms(5)).expect("a run"))
            .expect("fits");
        assert_eq!(run.received_at(9), at);
        assert_eq!(run.received_at(15), ms(5));

        for offset in 0..12 {
            run.release(offset);
        }
        run.absorb(Run::new(&offsets(22..30), 80, ms(9)).expect("a run"))
            .expect("fits");
        assert_eq!((run.start, run.end), (12, 30));
        assert_eq!(run.received, ms(5), "12 arrived with the second receive");
        assert_eq!(run.merged, vec![(10, 4)]);
        assert_eq!(run.gaps.ranges, vec![(20, 22)]);
        assert_eq!(run.unreleased.len(), 16);
        assert_eq!(
            run.unreleased_bytes(),
            160,
            "8 of 10 of the second's, all of the third's"
        );
    }

    /// Heap and inline bytes of a partition's hold.
    fn footprint(hold: &PartitionHold) -> usize {
        let offsets = |o: &Offsets| o.ranges.capacity() * std::mem::size_of::<(i64, i64)>();
        std::mem::size_of::<PartitionHold>()
            + hold.runs.capacity() * std::mem::size_of::<Run>()
            + hold
                .runs
                .iter()
                .map(|r| {
                    offsets(&r.gaps)
                        + offsets(&r.unreleased)
                        + offsets(&r.withheld)
                        + r.merged.capacity() * std::mem::size_of::<(u32, u32)>()
                })
                .sum::<usize>()
            + offsets(&hold.stale)
    }

    /// An hour at 10k records/s on one partition, received 1000 at a time and
    /// all held, as an archiver holds an open file's records. Then the same
    /// hour released one receive behind, as a pipeline keeps a block in flight.
    #[test]
    fn an_hour_of_held_offsets_costs_kilobytes() {
        const RECEIVES: i64 = 36_000;
        const PER_RECEIVE: i64 = 1_000;
        let mut held = PartitionHold::default();
        let at = Instant::now();
        for recv in 0..RECEIVES {
            let offsets: Vec<i64> = (recv * PER_RECEIVE..(recv + 1) * PER_RECEIVE).collect();
            let received =
                at + std::time::Duration::from_millis(u64::try_from(recv * 100).expect("positive"));
            held.register(Run::new(&offsets, 1_000_000, received).expect("a run"));
        }
        let all_held = footprint(&held);
        let records = RECEIVES * PER_RECEIVE;
        eprintln!(
            "held: {records} offsets in {} run(s), {all_held} bytes, {:.4} bytes a record",
            held.runs.len(),
            all_held as f64 / records as f64
        );
        assert!(all_held < 1024 * 1024, "{all_held} bytes for an hour held");

        let mut pipelined = PartitionHold::default();
        for recv in 0..RECEIVES / 10 {
            let offsets: Vec<i64> = (recv * PER_RECEIVE..(recv + 1) * PER_RECEIVE).collect();
            pipelined.register(Run::new(&offsets, 1_000_000, at).expect("a run"));
            for offset in (recv - 1).max(0) * PER_RECEIVE..recv * PER_RECEIVE {
                pipelined.release(offset);
            }
            pipelined.commit_target();
        }
        let in_flight = footprint(&pipelined);
        eprintln!("one receive in flight: {in_flight} bytes");
        assert!(in_flight < 4096, "{in_flight} bytes with one receive held");
    }
}
