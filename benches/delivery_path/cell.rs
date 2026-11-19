// Project:   scalo
// File:      benches/delivery_path/cell.rs
// Purpose:   One (approach, load, run) cell, run in a child process of its own
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::process::ExitCode;
use std::sync::Arc;

use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::TransportSender;
use scalo::transport::test_support::delivery::{AnyApproach, Destinations, approach};
use scalo::transport::test_support::shaped::ShapedSender;

use crate::args::{Args, Tier};
use crate::loads::{self, Load};
use crate::report::CellResult;
use crate::{drive, host};

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
        Some(result) => {
            println!("{}", result.json_line());
            ExitCode::SUCCESS
        }
        None => ExitCode::from(2),
    }
}

async fn run(args: &Args) -> Option<CellResult> {
    let load = *args.loads.first()?;
    let approach = approach(args.approaches.first()?)?;
    match args.tier {
        Tier::One => {
            let mut destinations = Destinations::new(guard());
            let mut senders = Vec::new();
            for (name, config) in loads::topology(load, args.seed) {
                let sender = ShapedSender::new(name, config);
                senders.push(sender.clone());
                destinations.add(name, sender);
            }
            let destinations = Arc::new(destinations);
            measure(args, load, approach, &destinations, || {
                Some(senders.iter().map(|s| s.stats().records_confirmed).sum())
            })
            .await
        }
        Tier::Two => tier_two(args, load, approach).await,
    }
}

#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
async fn tier_two(args: &Args, load: Load, approach: AnyApproach) -> Option<CellResult> {
    let bootstrap = args.bootstrap.as_deref()?;
    let real = crate::tier2::RealDestinations::start(load, bootstrap, guard()).await?;
    let destinations = Arc::clone(&real.destinations);
    let result = measure(args, load, approach, &destinations, || None).await;
    real.close().await;
    result
}

#[cfg(not(all(feature = "testcontainers", feature = "transport-grpc", unix)))]
#[allow(clippy::unused_async)]
async fn tier_two(_args: &Args, _load: Load, _approach: AnyApproach) -> Option<CellResult> {
    None
}

async fn measure<S: TransportSender + 'static>(
    args: &Args,
    load: Load,
    approach: AnyApproach,
    destinations: &Arc<Destinations<S>>,
    confirmed: impl FnOnce() -> Option<u64>,
) -> Option<CellResult> {
    let groups = loads::groups(load, destinations.as_ref());
    let raw = drive::run(
        approach,
        destinations,
        &groups,
        args.duration(),
        args.allocs,
    )
    .await?;
    Some(CellResult::new(
        args,
        load,
        args.cell_run.unwrap_or(0),
        approach,
        &groups,
        &raw,
        confirmed(),
        host::peak_rss_bytes(),
    ))
}

/// A guard with a fixed limit, so leases are counted and never refused.
fn guard() -> Arc<MemoryGuard> {
    Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1 << 40,
        ..MemoryGuardConfig::default()
    }))
}
