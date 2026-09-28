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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::transport::ack::{AckControl, AckKind, AcknowledgementsConfig, HeldAcks};
use crate::transport::finalizer::DeliveryStatus;

use super::KafkaToken;
use super::metrics::Rebalanced;

/// A partition, keyed as the tokens carry it.
type PartitionKey = (Arc<str>, i32);

/// One offset `recv` handed out.
#[derive(Debug, Clone, Copy)]
struct Held {
    offset: i64,
    bytes: u64,
    released: bool,
    /// Released `Errored`: still held, and its holder will not release it again.
    withheld: bool,
}

/// The offsets of one partition from one `recv`, ascending.
#[derive(Debug)]
struct Run {
    held: Vec<Held>,
    unreleased: usize,
    unreleased_bytes: u64,
    /// Offsets released `Errored` and not released since.
    withheld: usize,
    received: Instant,
}

impl Run {
    fn first(&self) -> i64 {
        self.held.first().map_or(i64::MAX, |h| h.offset)
    }

    fn last(&self) -> i64 {
        self.held.last().map_or(i64::MIN, |h| h.offset)
    }

    /// Mark `offset` released; `true` when this run holds it.
    fn release(&mut self, offset: i64) -> bool {
        let Ok(at) = self.held.binary_search_by_key(&offset, |h| h.offset) else {
            return false;
        };
        let entry = &mut self.held[at];
        if !entry.released {
            entry.released = true;
            self.unreleased -= 1;
            self.unreleased_bytes = self.unreleased_bytes.saturating_sub(entry.bytes);
            if entry.withheld {
                self.withheld -= 1;
            }
        }
        true
    }

    /// Mark `offset` released `Errored`: it stays held.
    fn withhold(&mut self, offset: i64) {
        if let Ok(at) = self.held.binary_search_by_key(&offset, |h| h.offset) {
            let entry = &mut self.held[at];
            if !entry.withheld && !entry.released {
                self.withheld += 1;
            }
            entry.withheld = true;
        }
    }

    /// The lowest offset not yet released.
    fn first_unreleased(&self) -> Option<i64> {
        self.held.iter().find(|h| !h.released).map(|h| h.offset)
    }

    /// Drop every offset at or above `from`.
    fn truncate_from(&mut self, from: i64) {
        let keep = self.held.partition_point(|h| h.offset < from);
        for dropped in self.held.drain(keep..) {
            if !dropped.released {
                self.unreleased -= 1;
                self.unreleased_bytes = self.unreleased_bytes.saturating_sub(dropped.bytes);
                if dropped.withheld {
                    self.withheld -= 1;
                }
            }
        }
    }
}

/// Held offsets of one partition, and where its commit stands.
#[derive(Debug, Default)]
struct PartitionHold {
    runs: VecDeque<Run>,
    /// Highest offset released, held here or not.
    highest_released: Option<i64>,
    /// The next-to-read offset last committed; commits never go below it.
    committed_next: Option<i64>,
    /// Revoked and not assigned again: nothing is held or committed for it.
    unowned: bool,
    /// Offsets handed out before the last revoke and not released or withheld
    /// since. The next owner reads them again, so one release of each is the
    /// old copy's and commits nothing, even once the partition comes back.
    stale: BTreeSet<i64>,
}

impl PartitionHold {
    /// Record a run `recv` handed out. A run that starts at or below an offset
    /// already held means the consumer read again from there, so the held
    /// offsets from there on are replaced.
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
        self.runs.push_back(run);
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
        while self.runs.front().is_some_and(|r| r.unreleased == 0) {
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
        let mut by_partition: HashMap<PartitionKey, Vec<Held>> = HashMap::new();
        for (token, bytes) in handed_out {
            by_partition
                .entry((Arc::clone(&token.topic), token.partition))
                .or_default()
                .push(Held {
                    offset: token.offset,
                    bytes,
                    released: false,
                    withheld: false,
                });
        }
        if by_partition.is_empty() {
            return;
        }
        let mut partitions = self.partitions.lock();
        for (key, mut held) in by_partition {
            held.sort_unstable_by_key(|h| h.offset);
            held.dedup_by_key(|h| h.offset);
            let hold = partitions.entry(key).or_default();
            // A partition this member lost holds nothing: its next owner reads it.
            if hold.unowned {
                continue;
            }
            hold.register(Run {
                unreleased: held.len(),
                unreleased_bytes: held.iter().map(|h| h.bytes).sum(),
                withheld: 0,
                held,
                received: now,
            });
        }
        drop(partitions);
        self.publish_held();
    }

    /// Wait for any commit in progress; hold the guard across the next commit.
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
            let stale = hold.stale.remove(&token.offset);
            if hold.unowned || stale {
                continue;
            }
            if let Some(run) = hold
                .runs
                .iter()
                .find(|r| r.first() <= token.offset && token.offset <= r.last())
            {
                oldest = Some(oldest.map_or(run.received, |o| o.min(run.received)));
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
                    Some(run.received)
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
            .map(|r| r.withheld as u64)
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
                        stale.extend(
                            old.runs
                                .iter()
                                .flat_map(|r| r.held.iter())
                                .filter(|h| !h.released && !h.withheld)
                                .map(|h| h.offset),
                        );
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
            hold.stale = hold.stale.split_off(&next);
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

    /// Record the held gauges, never while unwinding, as `note_released`.
    fn publish_held(&self) {
        #[cfg(feature = "metrics")]
        if !std::thread::panicking() {
            let held = self.held();
            ::metrics::gauge!("transport_ack_held", "transport" => "kafka").set(held.count as f64);
            ::metrics::gauge!("transport_ack_held_bytes", "transport" => "kafka")
                .set(held.bytes as f64);
            ::metrics::gauge!("transport_ack_withheld", "transport" => "kafka")
                .set(self.withheld() as f64);
        }
        #[cfg(not(feature = "metrics"))]
        let _ = self;
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
            if run.unreleased == 0 {
                continue;
            }
            count += run.unreleased as u64;
            bytes = bytes.saturating_add(run.unreleased_bytes);
            oldest = Some(oldest.map_or(run.received, |o| o.min(run.received)));
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
        assert!(targets(&acks, &served_after).is_empty());
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

        assert!(targets(&acks, &in_flight).is_empty());
        assert_eq!(
            targets(&acks, &read_again[..5]),
            vec![10],
            "10..15 read again are still held: the old copies' release did not release them"
        );
        assert_eq!(targets(&acks, &read_again[5..]), vec![20]);
    }

    /// A recorder whose every metric panics.
    #[cfg(feature = "metrics")]
    struct PanickingRecorder;

    #[cfg(feature = "metrics")]
    impl ::metrics::Recorder for PanickingRecorder {
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
            panic!("recorder refuses counters");
        }
        fn register_gauge(
            &self,
            _: &::metrics::Key,
            _: &::metrics::Metadata<'_>,
        ) -> ::metrics::Gauge {
            panic!("recorder refuses gauges");
        }
        fn register_histogram(
            &self,
            _: &::metrics::Key,
            _: &::metrics::Metadata<'_>,
        ) -> ::metrics::Histogram {
            panic!("recorder refuses histograms");
        }
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
        let _local = ::metrics::set_default_local_recorder(&PanickingRecorder);
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
        assert!(targets(&acks, &block[5..]).is_empty());
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
}
