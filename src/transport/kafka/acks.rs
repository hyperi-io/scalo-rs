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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::transport::ack::{AckControl, AckKind, AcknowledgementsConfig, HeldAcks};
use crate::transport::finalizer::DeliveryStatus;

use super::KafkaToken;

/// A partition, keyed as the tokens carry it.
type PartitionKey = (Arc<str>, i32);

/// One offset `recv` handed out.
#[derive(Debug, Clone, Copy)]
struct Held {
    offset: i64,
    bytes: u64,
    released: bool,
}

/// The offsets of one partition from one `recv`, ascending.
#[derive(Debug)]
struct Run {
    held: Vec<Held>,
    unreleased: usize,
    unreleased_bytes: u64,
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
        }
        true
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
                });
        }
        if by_partition.is_empty() {
            return;
        }
        let mut partitions = self.partitions.lock();
        for (key, mut held) in by_partition {
            held.sort_unstable_by_key(|h| h.offset);
            held.dedup_by_key(|h| h.offset);
            let run = Run {
                unreleased: held.len(),
                unreleased_bytes: held.iter().map(|h| h.bytes).sum(),
                held,
                received: now,
            };
            partitions.entry(key).or_default().register(run);
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
            let partitions = self.partitions.lock();
            tokens
                .iter()
                .filter_map(|t| {
                    let hold = partitions.get(&(Arc::clone(&t.topic), t.partition))?;
                    hold.runs
                        .iter()
                        .find(|r| r.first() <= t.offset && t.offset <= r.last())
                        .map(|r| r.received)
                })
                .min()
        };
        Self::note_released(tokens.len(), DeliveryStatus::Errored, oldest);
    }

    /// Record the commits that landed.
    pub(super) fn committed(&self, targets: &[(PartitionKey, i64)]) {
        let mut partitions = self.partitions.lock();
        for (key, next) in targets {
            let hold = partitions.entry(key.clone()).or_default();
            hold.committed_next = Some(hold.committed_next.map_or(*next, |c| c.max(*next)));
        }
    }

    fn note_released(count: usize, outcome: DeliveryStatus, oldest: Option<Instant>) {
        #[cfg(feature = "metrics")]
        {
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

    fn publish_held(&self) {
        #[cfg(feature = "metrics")]
        {
            let held = self.held();
            ::metrics::gauge!("transport_ack_held", "transport" => "kafka").set(held.count as f64);
            ::metrics::gauge!("transport_ack_held_bytes", "transport" => "kafka")
                .set(held.bytes as f64);
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

    #[test]
    fn unregistered_tokens_commit_as_before() {
        let acks = KafkaAcks::default();
        let tokens = [token(0, 7), token(0, 3)];
        assert_eq!(targets(&acks, &tokens), vec![8]);
    }
}
