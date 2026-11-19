// Project:   scalo
// File:      benches/delivery_path/main.rs
// Purpose:   Delivery-path bench: approaches by name, loads by name, interleaved runs
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Compares delivery approaches under named loads.
//!
//! ```text
//! cargo bench --bench delivery_path --features transport-test-support -- \
//!     --approaches inline_sequential,per_record_futures --loads W1,W4 \
//!     --tier 1 --seconds 5 [--runs 2] [--save FILE] \
//!     [--compare FILE --max-regression 0.05]
//! cargo bench --bench delivery_path \
//!     --features transport-test-support,testcontainers,transport-grpc -- \
//!     --tier 2 --calibrate
//! ```
//!
//! Tier 1 runs on shaped senders in process. Tier 2 runs on a Kafka broker in a
//! container this binary starts and removes, and on gRPC listeners in process.
//!
//! Runs are interleaved (a, b, a, b) so host noise lands on every approach.
//! Each run prints one JSON line, and each load a table with the difference
//! from the first approach and the spread of the same code across its runs.
//! `--compare` exits 1 when an approach is slower than the saved file by more
//! than `--max-regression`.
//!
//! criterion is not used: it times one closure, so it cannot report per-caller
//! latency percentiles across concurrent callers or interleave approaches.

#![allow(clippy::cast_precision_loss)]

mod args;
mod drive;
mod host;
mod loads;
mod report;
#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
mod tier2;

use std::process::ExitCode;
use std::sync::Arc;

use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::test_support::delivery::{Destinations, approach};
use scalo::transport::test_support::shaped::{PRESETS_CALIBRATED, ShapedSender};

use args::{Args, Tier};
use report::RunResult;

fn main() -> ExitCode {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("delivery_path: {message}\n\n{}", args::USAGE);
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("delivery_path: tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(run(args))
}

async fn run(args: Args) -> ExitCode {
    println!(
        "# delivery_path tier {} presets {} load_avg_start {}",
        args.tier.number(),
        if PRESETS_CALIBRATED {
            "calibrated"
        } else {
            "UNCALIBRATED"
        },
        host::load_average()
    );

    let results = match args.tier {
        Tier::One => tier_one(&args).await,
        Tier::Two => tier_two(&args).await,
    };
    let Some(results) = results else {
        return ExitCode::from(2);
    };

    report::tables(&args, &results);
    println!("# load_avg_end {}", host::load_average());

    if let Some(path) = &args.save
        && let Err(e) = report::save(path, &args, &results)
    {
        eprintln!("delivery_path: save {}: {e}", path.display());
        return ExitCode::from(2);
    }
    if let Some(path) = &args.compare {
        return match report::compare(path, &results, args.max_regression) {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(1),
            Err(e) => {
                eprintln!("delivery_path: compare {}: {e}", path.display());
                ExitCode::from(2)
            }
        };
    }
    ExitCode::SUCCESS
}

async fn tier_one(args: &Args) -> Option<Vec<RunResult>> {
    let mut results = Vec::new();
    for load in &args.loads {
        for run in 0..args.runs {
            for name in &args.approaches {
                let approach = approach(name)?;
                let mut destinations = Destinations::new(guard());
                let mut senders = Vec::new();
                for (dest, config) in loads::topology(*load, args.seed) {
                    let sender = ShapedSender::new(dest, config);
                    senders.push(sender.clone());
                    destinations.add(dest, sender);
                }
                let destinations = Arc::new(destinations);
                let groups = loads::groups(*load, destinations.as_ref());
                let raw = drive::run(approach, &destinations, &groups, args.duration()).await?;
                let confirmed: u64 = senders.iter().map(|s| s.stats().records_confirmed).sum();
                let result =
                    RunResult::new(args, *load, run, approach, &groups, &raw, Some(confirmed));
                println!("{}", result.json_line());
                results.push(result);
            }
        }
    }
    Some(results)
}

#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
async fn tier_two(args: &Args) -> Option<Vec<RunResult>> {
    tier2::run(args).await
}

#[cfg(not(all(feature = "testcontainers", feature = "transport-grpc", unix)))]
#[allow(clippy::unused_async)]
async fn tier_two(_args: &Args) -> Option<Vec<RunResult>> {
    eprintln!(
        "delivery_path: tier 2 needs --features transport-test-support,testcontainers,transport-grpc on Unix"
    );
    None
}

/// A guard with a fixed limit, so leases are counted and never refused.
fn guard() -> Arc<MemoryGuard> {
    Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1 << 40,
        ..MemoryGuardConfig::default()
    }))
}
