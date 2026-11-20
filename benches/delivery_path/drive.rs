// Project:   scalo
// File:      benches/delivery_path/drive.rs
// Purpose:   Callers offering their batches to one approach, closed-loop or at a fixed rate
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Two ways to drive callers:
//!
//! - Closed-loop: a caller offers its batch again once it has its answer. It
//!   measures records per second, and a slow approach sheds offered load here.
//! - Open-loop at a fixed offered rate: each caller's offers are scheduled at
//!   fixed times, latency is measured from the scheduled time, and an offer
//!   that could not start on time is counted late. Latency is compared between
//!   approaches here, at a matched rate.
//!
//! The retry policy is the caller's and applies to any approach: none, bounded
//! (5 more tries from 200 ms, doubling) or hold (send again until taken, the
//! wait doubling from 200 ms to at most 5 s). First-answer latency and the
//! latency to the final answer are kept apart.

use std::sync::Arc;
use std::time::Duration;

use sketches_ddsketch::{Config, DDSketch};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::Instant;

use scalo::transport::TransportSender;
use scalo::transport::test_support::delivery::{
    Answer, AnyApproach, CallerBatch, DeliveryApproach, Destinations, Outcome, RoutedRecord,
};

use crate::loads::Group;
use crate::{alloc, host};

/// Slack past the deadline for the last round of every caller to answer.
const DRAIN_BOUND: Duration = Duration::from_secs(60);

/// Offers one open-loop caller may have in flight before the next is late.
const OPEN_LOOP_OUTSTANDING: usize = 64;

/// An offer that starts this long after its scheduled time is late.
const LATE_AFTER: Duration = Duration::from_millis(1);

/// What a caller does with records an approach did not take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Answer the caller with the refusal.
    None,
    /// Up to 5 more tries, waiting 200 ms and doubling.
    Bounded,
    /// Send again until taken, waiting 200 ms and doubling to at most 5 s.
    Hold,
}

impl Retry {
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "none" => Some(Self::None),
            "bounded" => Some(Self::Bounded),
            "hold" => Some(Self::Hold),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bounded => "bounded",
            Self::Hold => "hold",
        }
    }

    /// The wait before try `attempt` (1 for the first retry), or `None` when
    /// the policy has given up.
    fn wait(self, attempt: u32) -> Option<Duration> {
        let doubled =
            Duration::from_millis(200).saturating_mul(1 << attempt.saturating_sub(1).min(16));
        match self {
            Self::None => None,
            Self::Bounded => (attempt <= 5).then_some(doubled),
            Self::Hold => Some(doubled.min(Duration::from_secs(5))),
        }
    }
}

/// What one group's callers saw.
pub struct GroupTally {
    /// Latency to the final answer per batch, in microseconds.
    pub latency_us: DDSketch,
    /// Latency to the approach's first answer per batch, in microseconds.
    pub first_latency_us: DDSketch,
    pub batches: u64,
    /// Records by final outcome, indexed by `Outcome as usize`.
    pub records: [u64; 4],
    /// Offers made: batches the caller started.
    pub offers: u64,
    /// Open-loop offers that started more than [`LATE_AFTER`] after their time.
    pub late: u64,
    /// Records sent again by the retry policy.
    pub resent: u64,
}

impl GroupTally {
    fn new() -> Self {
        Self {
            latency_us: DDSketch::new(Config::defaults()),
            first_latency_us: DDSketch::new(Config::defaults()),
            batches: 0,
            records: [0; 4],
            offers: 0,
            late: 0,
            resent: 0,
        }
    }

    fn merge(&mut self, other: &Self) {
        // Sketches built from one config always merge.
        let _ = self.latency_us.merge(&other.latency_us);
        let _ = self.first_latency_us.merge(&other.first_latency_us);
        self.batches += other.batches;
        for (a, b) in self.records.iter_mut().zip(other.records) {
            *a += b;
        }
        self.offers += other.offers;
        self.late += other.late;
        self.resent += other.resent;
    }

    fn add(&mut self, offer: &Offer) {
        self.latency_us.add(offer.final_us);
        self.first_latency_us.add(offer.first_us);
        self.batches += 1;
        self.resent += offer.resent;
        for outcome in &offer.outcomes {
            self.records[*outcome as usize] += 1;
        }
    }

    pub fn count(&self, outcome: Outcome) -> u64 {
        self.records[outcome as usize]
    }

    pub fn quantile_us(&self, q: f64) -> Option<f64> {
        self.latency_us.quantile(q).ok().flatten()
    }

    pub fn first_quantile_us(&self, q: f64) -> Option<f64> {
        self.first_latency_us.quantile(q).ok().flatten()
    }
}

/// One run's raw measurements.
pub struct RawRun {
    /// When the callers started and when the last one finished, in
    /// milliseconds since the Unix epoch, to match a cell against host events.
    pub started_unix_ms: u64,
    pub ended_unix_ms: u64,
    pub elapsed: Duration,
    pub groups: Vec<GroupTally>,
    pub peak_held_bytes: u64,
    pub cpu_ns: Option<u64>,
    /// (allocations, bytes) while the callers ran, with `--allocs`.
    pub allocs: Option<(u64, u64)>,
}

/// How the callers are driven.
#[derive(Debug, Clone, Copy)]
pub enum Drive {
    Closed,
    /// Records offered per second across every caller.
    Open {
        rate: f64,
    },
}

/// Run every group's callers against `approach` for `duration`, then wait for
/// their last offers. `None` when a caller task fails or the drain overruns.
#[allow(clippy::too_many_arguments)]
pub async fn run<S: TransportSender + 'static>(
    approach: AnyApproach,
    retry: Retry,
    drive: Drive,
    destinations: &Arc<Destinations<S>>,
    groups: &[Group],
    duration: Duration,
    count_allocs: bool,
) -> Option<RawRun> {
    destinations.ledger().reset_peak();
    let cpu_start = host::cpu_ns();
    if count_allocs {
        alloc::start();
    }
    let started_unix_ms = unix_ms();
    let start = Instant::now();
    let deadline = start + duration;
    let callers: usize = groups.iter().map(|g| g.batches.len()).sum();
    let records_per_round: usize = groups
        .iter()
        .map(|g| g.batches.iter().map(|b| b.len()).sum::<usize>())
        .sum();

    let mut handles = Vec::new();
    let mut index_in_all = 0usize;
    for (index, group) in groups.iter().enumerate() {
        for batch in &group.batches {
            let caller = Caller {
                approach,
                retry,
                destinations: Arc::clone(destinations),
                records: Arc::clone(batch),
                answer: group.answer,
                deadline,
            };
            let task = match drive {
                Drive::Closed => tokio::spawn(caller.closed_loop()),
                Drive::Open { rate } => {
                    #[allow(clippy::cast_precision_loss)]
                    let interval =
                        Duration::from_secs_f64(records_per_round as f64 / rate.max(f64::EPSILON));
                    #[allow(clippy::cast_precision_loss)]
                    let phase = interval.mul_f64(index_in_all as f64 / callers.max(1) as f64);
                    tokio::spawn(caller.open_loop(start + phase, interval))
                }
            };
            handles.push((index, task));
            index_in_all += 1;
        }
    }

    let mut tallies: Vec<GroupTally> = groups.iter().map(|_| GroupTally::new()).collect();
    let drain = tokio::time::timeout(duration + DRAIN_BOUND, async {
        for (index, handle) in handles {
            match handle.await {
                Ok(tally) => tallies[index].merge(&tally),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })
    .await;
    let allocs = count_allocs.then(alloc::stop);
    match drain {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("delivery_path: a caller task failed: {e}");
            return None;
        }
        Err(_) => {
            eprintln!(
                "delivery_path: callers still running {} s past the deadline",
                DRAIN_BOUND.as_secs()
            );
            return None;
        }
    }
    let elapsed = start.elapsed();
    let cpu_ns = cpu_start
        .zip(host::cpu_ns())
        .map(|(a, b)| b.saturating_sub(a));
    Some(RawRun {
        started_unix_ms,
        ended_unix_ms: unix_ms(),
        elapsed,
        groups: tallies,
        peak_held_bytes: destinations.ledger().peak_bytes(),
        cpu_ns,
        allocs,
    })
}

/// One caller and what it offers.
struct Caller<S> {
    approach: AnyApproach,
    retry: Retry,
    destinations: Arc<Destinations<S>>,
    records: Arc<[RoutedRecord]>,
    answer: Answer,
    deadline: Instant,
}

impl<S> Clone for Caller<S> {
    fn clone(&self) -> Self {
        Self {
            approach: self.approach,
            retry: self.retry,
            destinations: Arc::clone(&self.destinations),
            records: Arc::clone(&self.records),
            answer: self.answer,
            deadline: self.deadline,
        }
    }
}

/// One offer's answer.
struct Offer {
    first_us: f64,
    final_us: f64,
    outcomes: Vec<Outcome>,
    resent: u64,
}

impl<S: TransportSender + 'static> Caller<S> {
    /// Offer, wait for the final answer, offer again until the deadline.
    async fn closed_loop(self) -> GroupTally {
        let mut tally = GroupTally::new();
        while Instant::now() < self.deadline {
            let offered = Instant::now();
            tally.offers += 1;
            let offer = self.offer(offered).await;
            tally.add(&offer);
        }
        tally
    }

    /// Offer at `first`, then every `interval`, until the deadline, measuring
    /// each offer from its scheduled time.
    async fn open_loop(self, first: Instant, interval: Duration) -> GroupTally {
        let mut tally = GroupTally::new();
        let slots = Arc::new(Semaphore::new(OPEN_LOOP_OUTSTANDING));
        let mut running: JoinSet<Offer> = JoinSet::new();
        let mut scheduled = first;
        while scheduled < self.deadline {
            tokio::time::sleep_until(scheduled).await;
            while let Some(done) = running.try_join_next() {
                if let Ok(offer) = done {
                    tally.add(&offer);
                }
            }
            let Ok(slot) = Arc::clone(&slots).acquire_owned().await else {
                break;
            };
            if Instant::now() > scheduled + LATE_AFTER {
                tally.late += 1;
            }
            tally.offers += 1;
            let caller = self.clone();
            let at = scheduled;
            running.spawn(async move {
                let offer = caller.offer(at).await;
                drop(slot);
                offer
            });
            scheduled += interval;
        }
        while let Some(done) = running.join_next().await {
            if let Ok(offer) = done {
                tally.add(&offer);
            }
        }
        tally
    }

    /// Deliver the batch, applying the retry policy to what was not taken.
    async fn offer(&self, scheduled: Instant) -> Offer {
        let mut outcomes = self
            .approach
            .deliver(
                &self.destinations,
                CallerBatch {
                    records: self.records.to_vec(),
                    answer: self.answer,
                },
            )
            .await;
        let first_us = micros(scheduled.elapsed());
        let mut resent = 0u64;
        let mut attempt = 0u32;
        loop {
            let undelivered: Vec<usize> = outcomes
                .iter()
                .enumerate()
                .filter(|(_, o)| o.is_undelivered())
                .map(|(i, _)| i)
                .collect();
            if undelivered.is_empty() || Instant::now() >= self.deadline {
                break;
            }
            attempt += 1;
            let Some(wait) = self.retry.wait(attempt) else {
                break;
            };
            tokio::time::sleep(wait).await;
            resent += undelivered.len() as u64;
            let again = self
                .approach
                .deliver(
                    &self.destinations,
                    CallerBatch {
                        records: undelivered
                            .iter()
                            .map(|&i| self.records[i].clone())
                            .collect(),
                        answer: self.answer,
                    },
                )
                .await;
            for (i, outcome) in undelivered.into_iter().zip(again) {
                outcomes[i] = outcome;
            }
        }
        Offer {
            first_us,
            final_us: micros(scheduled.elapsed()),
            outcomes,
            resent,
        }
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000_000.0
}
