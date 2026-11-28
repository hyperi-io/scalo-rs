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
//! Each load runs closed-loop first, where a caller offers again on its
//! answer: that measures records per second. `--open-loop 0.5,0.8` then runs
//! it at those fractions of the slowest approach's closed-loop offered rate,
//! with offers at fixed times and latency from the scheduled start, which is
//! where latency is compared. `--retry none|bounded|hold` is the caller's
//! policy for every approach, and `name+policy` sets one approach's own.
//! `--control` adds the first approach a second time as a same-code control.
//! W1g, W2g and W3g send to one gRPC destination and start no broker. A cell
//! serves it from scalo's receive server in a child process of this binary, on
//! the CPUs of `--listener-cpus` with `--listener-workers` threads, and reports
//! that process's CPU per record as `listener_cpu_us_per_record`.
//! Approaches are chosen on tier 2: tier 1 is the fast loop and the regression
//! gate, comparing an approach with its own saved baseline. `--compare` exits
//! 3 when the run cannot be compared: stopped, partial, a cell missing from
//! the saved file, or different settings.
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
mod observe;
mod report;
#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
mod tier2;

use std::collections::BTreeMap;
use std::process::{Command, ExitCode, Stdio};

use scalo::transport::test_support::shaped::PRESETS_CALIBRATED;

use args::{Args, Tier};
use report::{CellResult, Collected, Verdict};

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
    if args.listener {
        return listen(&args);
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

/// Run as the listener process of a gRPC-only load's cell.
#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
fn listen(args: &Args) -> ExitCode {
    if let Some(list) = &args.listener_cpus {
        // Before the runtime starts, so its workers inherit it.
        if let Err(e) = args::cpu_list(list).and_then(|cpus| host::pin(&cpus)) {
            eprintln!("delivery_path listener: pin to CPUs {list}: {e}");
            return ExitCode::from(2);
        }
    }
    listener::serve(args.listener_workers)
}

#[cfg(not(all(feature = "testcontainers", feature = "transport-grpc", unix)))]
fn listen(_args: &Args) -> ExitCode {
    eprintln!(
        "delivery_path: a listener needs --features transport-test-support,testcontainers,transport-grpc on Unix"
    );
    ExitCode::from(2)
}

async fn parent(mut args: Args) -> ExitCode {
    println!(
        "# delivery_path tier {} presets {} workers {} runs {} seconds {} modes {} allocs {} cpus {} allowed_cpus {} broker_cpus {} listener_cpus {} load_avg_start {}",
        args.tier.number(),
        if PRESETS_CALIBRATED {
            "calibrated"
        } else {
            "UNCALIBRATED"
        },
        args.workers,
        args.runs,
        args.seconds,
        args.modes().join(","),
        args.allocs,
        args.cpus.as_deref().unwrap_or("all"),
        host::allowed_cpus().map_or_else(|| "unknown".to_string(), |n| n.to_string()),
        args.broker_cpus.as_deref().unwrap_or("all"),
        args.listener_cpus.as_deref().unwrap_or("all"),
        host::load_average()
    );
    println!(
        "# approaches are chosen on tier 2 only; tier 1 compares an approach with its own saved baseline"
    );

    let outcome = match args.tier {
        Tier::One => cells(&args).await,
        Tier::Two => tier_two(&mut args).await,
    };
    let Some(collected) = outcome else {
        return ExitCode::from(2);
    };
    if collected.cells.is_empty() && !collected.stopped {
        return ExitCode::SUCCESS;
    }

    report::tables(&args, &collected);
    println!("# load_avg_end {}", host::load_average());

    if let Some(path) = &args.save
        && let Err(e) = report::save(path, &args, &collected)
    {
        eprintln!("delivery_path: save {}: {e}", path.display());
        return ExitCode::from(2);
    }
    if let Some(path) = &args.compare {
        return match report::compare(path, &args, &collected) {
            Ok(Verdict::Pass) => ExitCode::SUCCESS,
            Ok(Verdict::Regressed) => ExitCode::from(1),
            Ok(Verdict::Inconclusive(why)) => {
                println!("compare: INCONCLUSIVE: {why}");
                ExitCode::from(args::INCONCLUSIVE)
            }
            Err(e) => {
                eprintln!("delivery_path: compare {}: {e}", path.display());
                ExitCode::from(2)
            }
        };
    }
    if collected.stopped {
        return ExitCode::from(args::INCONCLUSIVE);
    }
    ExitCode::SUCCESS
}

/// Run every cell in a child process of this binary. Each load runs an idle
/// cell, then its closed-loop rounds, then its open-loop rounds at fractions of
/// the slowest approach's closed-loop offered rate. The approach order rotates
/// each round. `--warmup` seconds of a discarded cell come first.
async fn cells(args: &Args) -> Option<Collected> {
    let exe = std::env::current_exe().ok()?;
    let mut collected = Collected {
        cells: Vec::new(),
        idle_rss: BTreeMap::new(),
        stopped: false,
    };
    let (Some(first_load), Some(first_spec)) = (args.loads.first(), args.approaches.first()) else {
        return Some(collected);
    };
    if args.warmup > 0.0 {
        let mut warm = args.clone();
        warm.seconds = args.warmup;
        let line = warm.cell_command_line(first_spec, *first_load, 0, "closed", None);
        let result: CellResult = serde_json::from_str(&run_cell(&exe, line).await?).ok()?;
        println!(
            "# warmup {} {} {} s: {:.0} rec/s, discarded",
            first_spec.label,
            first_load.id(),
            args.warmup,
            result.records_per_sec
        );
    }
    for load in &args.loads {
        let mut idle_line = args.cell_command_line(first_spec, *load, 0, "closed", None);
        idle_line.push("--idle".to_string());
        if let Some(rss) = run_cell(&exe, idle_line)
            .await
            .and_then(|line| serde_json::from_str::<serde_json::Value>(&line).ok())
            .and_then(|v| v["idle_rss_bytes"].as_u64())
        {
            println!("# idle {} rss_bytes {rss}", load.id());
            collected.idle_rss.insert(load.id().to_string(), rss);
        }
        let mut slowest_offered: Option<f64> = None;
        for (mode_index, mode) in args.modes().iter().enumerate() {
            let rate = if mode_index == 0 {
                None
            } else {
                Some(slowest_offered? * args.open_loop[mode_index - 1])
            };
            for run in 0..args.runs {
                let n = args.approaches.len();
                for k in 0..n {
                    let spec = &args.approaches[(k + run) % n];
                    if let Some(stop) = &args.stop_file
                        && stop.exists()
                    {
                        println!("# stopped: {} exists", stop.display());
                        collected.stopped = true;
                        return Some(collected);
                    }
                    let line = args.cell_command_line(spec, *load, run, mode, rate);
                    let Some(json) = run_cell(&exe, line).await else {
                        eprintln!(
                            "delivery_path: cell {} {} {mode} run {run} failed",
                            spec.label,
                            load.id()
                        );
                        return None;
                    };
                    println!("{json}");
                    collected.cells.push(serde_json::from_str(&json).ok()?);
                }
            }
            if mode_index == 0 {
                slowest_offered = args
                    .approaches
                    .iter()
                    .filter_map(|spec| {
                        let mut offered: Vec<f64> = collected
                            .cells
                            .iter()
                            .filter(|c| {
                                c.load == load.id() && c.mode == *mode && c.label == spec.label
                            })
                            .map(|c| c.offered_records_per_sec)
                            .collect();
                        offered.sort_by(f64::total_cmp);
                        offered.get(offered.len() / 2).copied()
                    })
                    .reduce(f64::min);
            }
        }
    }
    Some(collected)
}

/// Run one cell in a child process and return the JSON line it printed.
async fn run_cell(exe: &std::path::Path, line: Vec<String>) -> Option<String> {
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
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find(|l| l.starts_with('{'))
        .map(str::to_string)
}

#[cfg(all(feature = "testcontainers", feature = "transport-grpc", unix))]
async fn tier_two(args: &mut Args) -> Option<Collected> {
    // A gRPC-only load has a listener process and no broker.
    if !(args.calibrate || args.order_check || args.loads.iter().any(|l| !l.grpc_only())) {
        return cells(args).await;
    }
    let stack = tier2::Stack::start(args.broker_cpus.as_deref()).await?;
    let empty = || Collected {
        cells: Vec::new(),
        idle_rss: BTreeMap::new(),
        stopped: false,
    };
    let outcome = if args.calibrate {
        tier2::calibrate(&stack).await;
        Some(empty())
    } else if args.order_check {
        tier2::order_check(&stack).await;
        Some(empty())
    } else {
        args.bootstrap = Some(stack.bootstrap.clone());
        cells(args).await
    };
    drop(stack);
    outcome
}

#[cfg(not(all(feature = "testcontainers", feature = "transport-grpc", unix)))]
#[allow(clippy::unused_async)]
async fn tier_two(_args: &mut Args) -> Option<Collected> {
    eprintln!(
        "delivery_path: tier 2 needs --features transport-test-support,testcontainers,transport-grpc on Unix"
    );
    None
}
