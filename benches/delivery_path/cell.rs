// Project:   scalo
// File:      benches/delivery_path/cell.rs
// Purpose:   One (approach, load, run, mode) cell, run in a child process of its own
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::TransportSender;
use scalo::transport::test_support::delivery::{AnyApproach, Destinations, Shape, approach};
use scalo::transport::test_support::shaped::ShapedSender;

use crate::args::{Args, Spec, Tier};
use crate::drive::{self, Drive};
use crate::host;
use crate::loads::{self, Load};
use crate::observe::{Counts, Observed};
use crate::report::{CellResult, KafkaQueue};

/// Run the one cell `args` names on a runtime of `--workers` threads and print
/// its JSON line.
pub fn main(args: &Args) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.workers)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("delivery_path cell: tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(run(args)) {
        Some(line) => {
            println!("{line}");
            ExitCode::SUCCESS
        }
        None => ExitCode::from(2),
    }
}

async fn run(args: &Args) -> Option<String> {
    let load = *args.loads.first()?;
    let spec = args.approaches.first()?;
    let approach = approach(&spec.name)?;
    match args.tier {
        Tier::One => {
            let mut destinations = Destinations::new(guard());
            let mut senders = Vec::new();
            let mut kafka_counts = None;
            let mut all_counts = Vec::new();
            for (name, config) in loads::topology(load, args.seed) {
                let sender = ShapedSender::new(name, config);
                senders.push(sender.clone());
                let (observed, counts) = Observed::new(sender);
                all_counts.push((name, Arc::clone(&counts)));
                if name == loads::KAFKA {
                    kafka_counts = Some(counts);
                }
                let shape = if name == loads::KAFKA {
                    Shape::PerRecord
                } else {
                    Shape::Batch
                };
                destinations.add_shaped(name, observed, shape);
            }
            let destinations = Arc::new(destinations);
            if args.idle {
                return idle().await;
            }
            let kafka_shaped = senders.first().cloned();
            let result = measure(args, load, spec, approach, &destinations, None).await?;
            let shaped = kafka_shaped.map(|k| k.stats());
            let queue = KafkaQueue {
                cap_bytes: loads::topology(load, args.seed)[0].1.queue_cap_bytes,
                peak_bytes: shaped.map(|s| s.peak_bytes_in_flight),
                full_waits: shaped.map(|s| s.queue_full_waits),
                full_refusals: shaped.map(|s| s.queue_full_refusals),
                backpressured_answers: kafka_counts.as_deref().map(Counts::backpressured),
                caller_side_peak_bytes: kafka_counts.as_deref().map(Counts::peak_bytes),
            };
            let confirmed = senders.iter().map(|s| s.stats().records_confirmed).sum();
            Some(
                result
                    .finish(Some(confirmed), queue)
                    .attempted(&all_counts)
                    .json_line(),
            )
        }
        Tier::Two => tier_two(args, load, spec, approach).await,
    }
}

#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
async fn tier_two(args: &Args, load: Load, spec: &Spec, approach: AnyApproach) -> Option<String> {
    let real = if load.grpc_only() {
        crate::tier2::RealDestinations::start_grpc(
            guard(),
            args.listener_cpus.as_deref(),
            args.listener_workers,
        )
        .await?
    } else {
        let bootstrap = args.bootstrap.as_deref()?;
        crate::tier2::RealDestinations::start(load, bootstrap, guard()).await?
    };
    let destinations = Arc::clone(&real.destinations);
    let line = if args.idle {
        idle().await
    } else {
        let result = measure(args, load, spec, approach, &destinations, real.listener_pid).await;
        let queue = if load.grpc_only() {
            KafkaQueue::default()
        } else {
            KafkaQueue {
                cap_bytes: if load == Load::W6 {
                    loads::W6_QUEUE_BYTES
                } else {
                    64 * 1024 * 1024
                },
                peak_bytes: None,
                full_waits: None,
                full_refusals: None,
                backpressured_answers: Some(real.kafka.backpressured()),
                caller_side_peak_bytes: Some(real.kafka.peak_bytes()),
            }
        };
        result.map(|r| r.finish(None, queue).attempted(&real.counts).json_line())
    };
    real.close().await;
    line
}

#[cfg(not(all(feature = "testcontainers", feature = "transport-grpc", unix)))]
#[allow(clippy::unused_async)]
async fn tier_two(
    _args: &Args,
    _load: Load,
    _spec: &Spec,
    _approach: AnyApproach,
) -> Option<String> {
    None
}

/// The RSS of a cell with its destinations built and nothing sent.
async fn idle() -> Option<String> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    Some(serde_json::json!({ "idle_rss_bytes": host::peak_rss_bytes() }).to_string())
}

async fn measure<S: TransportSender + 'static>(
    args: &Args,
    load: Load,
    spec: &Spec,
    approach: AnyApproach,
    destinations: &Arc<Destinations<S>>,
    listener_pid: Option<u32>,
) -> Option<CellResult> {
    let groups = loads::groups(load, destinations.as_ref());
    let drive = args.rate.map_or(Drive::Closed, |rate| Drive::Open { rate });
    let raw = drive::run(
        approach.clone(),
        spec.retry,
        drive,
        destinations,
        &groups,
        args.duration(),
        args.allocs,
        listener_pid,
    )
    .await?;
    let mut result = CellResult::new(args, load, spec, &groups, &raw, host::peak_rss_bytes());
    result.peak_queued_bytes = approach.peak_queued_bytes();
    Some(result)
}

/// A guard with a fixed limit, so leases are counted and never refused.
fn guard() -> Arc<MemoryGuard> {
    Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1 << 40,
        ..MemoryGuardConfig::default()
    }))
}
