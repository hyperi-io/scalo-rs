// Project:   scalo
// File:      benches/delivery_path/main.rs
// Purpose:   Delivery-path bench: approaches by name, loads by name, one process per cell
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Compares delivery approaches under named loads.
//!
//! ```text
//! cargo bench --bench delivery_path --features transport-test-support -- \
//!     --approaches inline_sequential,per_record_futures --loads W1,W4 \
//!     --tier 1 --seconds 5 [--runs 5] [--workers 8] [--allocs] [--save FILE] \
//!     [--compare FILE --max-regression 0.05]
//! cargo bench --bench delivery_path \
//!     --features transport-test-support,testcontainers,transport-grpc -- \
//!     --tier 2 [--calibrate | --order-check]
//! ```
//!
//! Tier 1 runs on shaped senders. Tier 2 runs on a Kafka broker in a container
//! this process starts and removes, and on gRPC listeners in each cell.
//!
//! Every (approach, load, run) cell runs in a child process of this binary, so
//! its peak RSS and CPU time are its own. Runs are interleaved (a, b, a, b) so
//! host noise lands on every approach. Each cell prints one JSON line, and each
//! load a table of medians with their spread. `--compare` exits 1 when an
//! approach is slower than the saved file by more than `--max-regression`.
//! `--allocs` counts heap allocations on every thread while the callers run,
//! which slows the run, so its timings are not for comparison.
//!
//! criterion is not used: it times one closure, so it cannot report per-caller
//! latency percentiles across concurrent callers or interleave approaches.

#![allow(clippy::cast_precision_loss)]

mod alloc;
mod args;
mod cell;
mod drive;
mod host;
#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
mod listener;
mod loads;
mod report;
#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
mod tier2;

use std::process::{Command, ExitCode, Stdio};

use scalo::transport::test_support::shaped::PRESETS_CALIBRATED;

use args::{Args, Tier};
use report::CellResult;

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

fn main() -> ExitCode {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("delivery_path: {message}\n\n{}", args::USAGE);
            return ExitCode::from(2);
        }
    };
    if args.cell_run.is_some() {
        return cell::main(&args);
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("delivery_path: tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(parent(args))
}

async fn parent(mut args: Args) -> ExitCode {
    println!(
        "# delivery_path tier {} presets {} workers {} runs {} seconds {} allocs {} load_avg_start {}",
        args.tier.number(),
        if PRESETS_CALIBRATED {
            "calibrated"
        } else {
            "UNCALIBRATED"
        },
        args.workers,
        args.runs,
        args.seconds,
        args.allocs,
        host::load_average()
    );

    let outcome = match args.tier {
        Tier::One => cells(&args).await,
        Tier::Two => tier_two(&mut args).await,
    };
    let Some(results) = outcome else {
        return ExitCode::from(2);
    };
    if results.is_empty() {
        return ExitCode::SUCCESS;
    }

    report::tables(&args, &results);
    println!("# load_avg_end {}", host::load_average());

    if let Some(path) = &args.save
        && let Err(e) = report::save(path, &args, &results)
    {
        eprintln!("delivery_path: save {}: {e}", path.display());
        return ExitCode::from(2);
    }
    if let Some(path) = &args.compare {
        return match report::compare(path, &args, &results) {
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

/// Run every cell in a child process of this binary, interleaving approaches.
async fn cells(args: &Args) -> Option<Vec<CellResult>> {
    let exe = std::env::current_exe().ok()?;
    let mut results = Vec::new();
    for load in &args.loads {
        for run in 0..args.runs {
            for approach in &args.approaches {
                let line = args.cell_command_line(approach, *load, run);
                let child = tokio::task::spawn_blocking({
                    let exe = exe.clone();
                    move || {
                        Command::new(exe)
                            .args(line)
                            .stdin(Stdio::null())
                            .stderr(Stdio::inherit())
                            .output()
                    }
                })
                .await
                .ok()?;
                let output = match child {
                    Ok(output) if output.status.success() => output,
                    Ok(output) => {
                        eprintln!(
                            "delivery_path: cell {approach} {} run {run} exited {}",
                            load.id(),
                            output.status
                        );
                        return None;
                    }
                    Err(e) => {
                        eprintln!("delivery_path: start cell: {e}");
                        return None;
                    }
                };
                let stdout = String::from_utf8_lossy(&output.stdout);
                let Some(json) = stdout.lines().find(|l| l.starts_with('{')) else {
                    eprintln!(
                        "delivery_path: cell {approach} {} printed no result",
                        load.id()
                    );
                    return None;
                };
                println!("{json}");
                match serde_json::from_str::<CellResult>(json) {
                    Ok(result) => results.push(result),
                    Err(e) => {
                        eprintln!("delivery_path: cell result: {e}");
                        return None;
                    }
                }
            }
        }
    }
    Some(results)
}

#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
async fn tier_two(args: &mut Args) -> Option<Vec<CellResult>> {
    let stack = tier2::Stack::start().await?;
    let outcome = if args.calibrate {
        tier2::calibrate(&stack).await;
        Some(Vec::new())
    } else if args.order_check {
        tier2::order_check(&stack).await;
        Some(Vec::new())
    } else {
        args.bootstrap = Some(stack.bootstrap.clone());
        cells(args).await
    };
    drop(stack);
    outcome
}

#[cfg(not(all(feature = "testcontainers", feature = "transport-grpc", unix)))]
#[allow(clippy::unused_async)]
async fn tier_two(_args: &mut Args) -> Option<Vec<CellResult>> {
    eprintln!(
        "delivery_path: tier 2 needs --features transport-test-support,testcontainers,transport-grpc on Unix"
    );
    None
}
