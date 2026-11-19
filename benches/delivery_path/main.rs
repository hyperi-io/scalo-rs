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
//! `--warmup SECONDS` runs one discarded cell first, 20 s on tier 2 by
//! default, so a broker that has just started is not measured cold.
//! `--stop-file FILE` stops the run before the next cell once FILE exists, so
//! a detached run can be ended without a signal.
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
    if let Some(list) = &args.cpus {
        // Before any thread starts, so the runtime, its workers and every cell inherit it.
        if let Err(e) = args::cpu_list(list).and_then(|cpus| host::pin(&cpus)) {
            eprintln!("delivery_path: pin to CPUs {list}: {e}");
            return ExitCode::from(2);
        }
    }
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
        "# delivery_path tier {} presets {} workers {} runs {} seconds {} allocs {} cpus {} allowed_cpus {} broker_cpus {} load_avg_start {}",
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
        args.cpus.as_deref().unwrap_or("all"),
        host::allowed_cpus().map_or_else(|| "unknown".to_string(), |n| n.to_string()),
        args.broker_cpus.as_deref().unwrap_or("all"),
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

/// Run every cell in a child process of this binary, interleaving approaches,
/// after `--warmup` seconds of a cell whose result is discarded.
async fn cells(args: &Args) -> Option<Vec<CellResult>> {
    let exe = std::env::current_exe().ok()?;
    if args.warmup > 0.0
        && let (Some(load), Some(approach)) = (args.loads.first(), args.approaches.first())
    {
        let mut warm = args.clone();
        warm.seconds = args.warmup;
        let line = warm.cell_command_line(approach, *load, 0);
        let result = run_cell(&exe, line).await?;
        println!(
            "# warmup {approach} {} {} s: {:.0} rec/s, discarded",
            load.id(),
            args.warmup,
            result.records_per_sec
        );
    }
    let mut results = Vec::new();
    for load in &args.loads {
        for run in 0..args.runs {
            for approach in &args.approaches {
                if let Some(stop) = &args.stop_file
                    && stop.exists()
                {
                    println!("# stopped: {} exists", stop.display());
                    return Some(results);
                }
                let line = args.cell_command_line(approach, *load, run);
                let Some(result) = run_cell(&exe, line).await else {
                    eprintln!(
                        "delivery_path: cell {approach} {} run {run} failed",
                        load.id()
                    );
                    return None;
                };
                println!("{}", result.json_line());
                results.push(result);
            }
        }
    }
    Some(results)
}

/// Run one cell in a child process and read back its result.
async fn run_cell(exe: &std::path::Path, line: Vec<String>) -> Option<CellResult> {
    let exe = exe.to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(exe)
            .args(line)
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .output()
    })
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        eprintln!("delivery_path: cell exited {}", output.status);
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json = stdout.lines().find(|l| l.starts_with('{'))?;
    serde_json::from_str::<CellResult>(json).ok()
}

#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
async fn tier_two(args: &mut Args) -> Option<Vec<CellResult>> {
    let stack = tier2::Stack::start(args.broker_cpus.as_deref()).await?;
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
