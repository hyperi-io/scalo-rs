// Project:   scalo
// File:      benches/delivery_path/drive.rs
// Purpose:   Closed-loop callers offering their batches to one approach until a deadline
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;
use std::time::Duration;

use sketches_ddsketch::{Config, DDSketch};
use tokio::time::Instant;

use scalo::transport::TransportSender;
use scalo::transport::test_support::delivery::{
    AnyApproach, CallerBatch, DeliveryApproach, Destinations, Outcome,
};

use crate::loads::Group;
use crate::{alloc, host};

/// Slack past the deadline for the last round of every caller to answer.
const DRAIN_BOUND: Duration = Duration::from_secs(60);

/// What one group's callers saw.
pub struct GroupTally {
    /// Caller latency per batch, in microseconds.
    pub latency_us: DDSketch,
    pub batches: u64,
    /// Records by outcome, indexed by `Outcome as usize`.
    pub records: [u64; 4],
}

impl GroupTally {
    fn new() -> Self {
        Self {
            latency_us: DDSketch::new(Config::defaults()),
            batches: 0,
            records: [0; 4],
        }
    }

    fn merge(&mut self, other: &Self) {
        // Sketches built from one config always merge.
        let _ = self.latency_us.merge(&other.latency_us);
        self.batches += other.batches;
        for (a, b) in self.records.iter_mut().zip(other.records) {
            *a += b;
        }
    }

    pub fn count(&self, outcome: Outcome) -> u64 {
        self.records[outcome as usize]
    }

    pub fn quantile_us(&self, q: f64) -> Option<f64> {
        self.latency_us.quantile(q).ok().flatten()
    }
}

/// One run's raw measurements.
pub struct RawRun {
    pub elapsed: Duration,
    pub groups: Vec<GroupTally>,
    pub peak_held_bytes: u64,
    pub cpu_ns: Option<u64>,
    /// (allocations, bytes) while the callers ran, with `--allocs`.
    pub allocs: Option<(u64, u64)>,
}

/// Run every group's callers against `approach` for `duration`, then wait for
/// their last round. `None` when a caller task fails or the drain overruns.
pub async fn run<S: TransportSender + 'static>(
    approach: AnyApproach,
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
    let start = Instant::now();
    let deadline = start + duration;

    let mut handles = Vec::new();
    for (index, group) in groups.iter().enumerate() {
        for batch in &group.batches {
            let destinations = Arc::clone(destinations);
            let records = Arc::clone(batch);
            let answer = group.answer;
            handles.push((
                index,
                tokio::spawn(async move {
                    caller(approach, &destinations, &records, answer, deadline).await
                }),
            ));
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
        elapsed,
        groups: tallies,
        peak_held_bytes: destinations.ledger().peak_bytes(),
        cpu_ns,
        allocs,
    })
}

/// Offer the batch, wait for its answer, and offer it again until `deadline`.
async fn caller<S: TransportSender>(
    approach: AnyApproach,
    destinations: &Destinations<S>,
    records: &[scalo::transport::test_support::delivery::RoutedRecord],
    answer: scalo::transport::test_support::delivery::Answer,
    deadline: Instant,
) -> GroupTally {
    let mut tally = GroupTally::new();
    while Instant::now() < deadline {
        let batch = CallerBatch {
            records: records.to_vec(),
            answer,
        };
        let offered = Instant::now();
        let outcomes = approach.deliver(destinations, batch).await;
        tally
            .latency_us
            .add(offered.elapsed().as_secs_f64() * 1_000_000.0);
        tally.batches += 1;
        for outcome in outcomes {
            tally.records[outcome as usize] += 1;
        }
    }
    tally
}
