// Project:   scalo
// File:      benches/delivery_path/report.rs
// Purpose:   Per-cell JSON lines, per-load tables, and the saved-baseline gate
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sketches_ddsketch::{Config, DDSketch};

use scalo::transport::test_support::delivery::{AnyApproach, DeliveryApproach, Outcome};
use scalo::transport::test_support::shaped::PRESETS_CALIBRATED;

use crate::args::Args;
use crate::drive::RawRun;
use crate::loads::{Group, Load};

/// One group's figures in one cell.
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupResult {
    pub group: String,
    pub touches_sick: bool,
    pub batches: u64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub sent: u64,
    pub dead_lettered: u64,
    pub backpressured: u64,
    pub failed: u64,
}

/// One (approach, load, run), measured in a process of its own.
#[derive(Debug, Serialize, Deserialize)]
pub struct CellResult {
    pub approach: String,
    pub load: String,
    pub run: usize,
    pub tier: u8,
    pub workers: usize,
    /// CPUs the cell process was allowed to run on.
    pub allowed_cpus: Option<u32>,
    pub presets_calibrated: bool,
    pub elapsed_s: f64,
    pub records_sent: u64,
    pub records_per_sec: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    /// p99 of the callers whose records do not touch the sick destination.
    pub isolated_p99_us: Option<f64>,
    pub peak_held_bytes: u64,
    /// The cell process's `VmHWM`.
    pub peak_rss_bytes: Option<u64>,
    /// The cell process's user plus system CPU while its callers ran.
    pub cpu_us_per_record: Option<f64>,
    pub allocs_per_record: Option<f64>,
    pub alloc_bytes_per_record: Option<f64>,
    /// Confirmations at the destinations beyond what the callers were told
    /// was delivered: copies a caller's retry would send again.
    pub excess_deliveries: Option<i64>,
    pub groups: Vec<GroupResult>,
}

fn per_record(total: Option<u64>, records: u64) -> Option<f64> {
    total
        .filter(|_| records > 0)
        .map(|t| t as f64 / records as f64)
}

impl CellResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        args: &Args,
        load: Load,
        run: usize,
        approach: AnyApproach,
        groups: &[Group],
        raw: &RawRun,
        confirmed: Option<u64>,
        peak_rss_bytes: Option<u64>,
    ) -> Self {
        let sick = load.sick();
        let mut all = DDSketch::new(Config::defaults());
        let mut isolated = DDSketch::new(Config::defaults());
        let mut expected: u64 = 0;
        let mut results = Vec::new();
        for (group, tally) in groups.iter().zip(&raw.groups) {
            let _ = all.merge(&tally.latency_us);
            let touches_sick = sick.is_some_and(|s| group.touches(s));
            if sick.is_some() && !touches_sick {
                let _ = isolated.merge(&tally.latency_us);
            }
            let routes = group.routes.len() as u64;
            expected += tally.count(Outcome::Sent) * routes
                + tally.count(Outcome::DeadLettered) * routes.saturating_sub(1);
            results.push(GroupResult {
                group: group.label.to_string(),
                touches_sick,
                batches: tally.batches,
                p50_us: round(tally.quantile_us(0.50).unwrap_or(0.0)),
                p99_us: round(tally.quantile_us(0.99).unwrap_or(0.0)),
                p999_us: round(tally.quantile_us(0.999).unwrap_or(0.0)),
                sent: tally.count(Outcome::Sent),
                dead_lettered: tally.count(Outcome::DeadLettered),
                backpressured: tally.count(Outcome::Backpressured),
                failed: tally.count(Outcome::Failed),
            });
        }
        let records_sent: u64 = raw.groups.iter().map(|g| g.count(Outcome::Sent)).sum();
        let elapsed_s = raw.elapsed.as_secs_f64();
        let quantile = |s: &DDSketch, q| round(s.quantile(q).ok().flatten().unwrap_or(0.0));
        Self {
            approach: approach.name().to_string(),
            load: load.id().to_string(),
            run,
            tier: args.tier.number(),
            workers: args.workers,
            allowed_cpus: crate::host::allowed_cpus(),
            presets_calibrated: PRESETS_CALIBRATED,
            elapsed_s: round(elapsed_s),
            records_sent,
            records_per_sec: round(records_sent as f64 / elapsed_s.max(f64::EPSILON)),
            p50_us: quantile(&all, 0.50),
            p99_us: quantile(&all, 0.99),
            p999_us: quantile(&all, 0.999),
            isolated_p99_us: (sick.is_some() && isolated.count() > 0)
                .then(|| quantile(&isolated, 0.99)),
            peak_held_bytes: raw.peak_held_bytes,
            peak_rss_bytes,
            cpu_us_per_record: per_record(raw.cpu_ns, records_sent).map(|ns| round(ns / 1000.0)),
            allocs_per_record: per_record(raw.allocs.map(|a| a.0), records_sent).map(round),
            alloc_bytes_per_record: per_record(raw.allocs.map(|a| a.1), records_sent).map(round),
            excess_deliveries: confirmed.map(|c| {
                i64::try_from(c).unwrap_or(i64::MAX) - i64::try_from(expected).unwrap_or(i64::MAX)
            }),
            groups: results,
        }
    }

    pub fn json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

fn round(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// Median and spread of one metric over an approach's runs under one load.
#[derive(Clone, Copy)]
struct Stat {
    median: f64,
    /// (max - min) / median: how far the same code moved between its runs.
    spread: f64,
}

impl Stat {
    fn of(values: &[f64]) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        let mid = median(values.to_vec());
        let max = values.iter().copied().fold(f64::MIN, f64::max);
        let min = values.iter().copied().fold(f64::MAX, f64::min);
        Some(Self {
            median: mid,
            spread: if mid == 0.0 { 0.0 } else { (max - min) / mid },
        })
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[mid - 1], values[mid])
    } else {
        values[mid]
    }
}

struct Summary {
    approach: String,
    runs: usize,
    records_per_sec: Stat,
    p50_us: Stat,
    p99_us: Stat,
    p999_us: Stat,
    isolated_p99_us: Option<Stat>,
    peak_held_bytes: Stat,
    peak_rss_bytes: Option<Stat>,
    cpu_us_per_record: Option<Stat>,
    allocs_per_record: Option<Stat>,
    alloc_bytes_per_record: Option<Stat>,
    excess_deliveries: Option<Stat>,
}

fn summarise(runs: &[&CellResult]) -> Option<Summary> {
    let first = runs.first()?;
    let stat =
        |f: &dyn Fn(&CellResult) -> f64| Stat::of(&runs.iter().map(|r| f(r)).collect::<Vec<f64>>());
    let stat_opt = |f: &dyn Fn(&CellResult) -> Option<f64>| {
        Stat::of(&runs.iter().filter_map(|r| f(r)).collect::<Vec<f64>>())
    };
    Some(Summary {
        approach: first.approach.clone(),
        runs: runs.len(),
        records_per_sec: stat(&|r| r.records_per_sec)?,
        p50_us: stat(&|r| r.p50_us)?,
        p99_us: stat(&|r| r.p99_us)?,
        p999_us: stat(&|r| r.p999_us)?,
        isolated_p99_us: stat_opt(&|r| r.isolated_p99_us),
        peak_held_bytes: stat(&|r| r.peak_held_bytes as f64)?,
        peak_rss_bytes: stat_opt(&|r| r.peak_rss_bytes.map(|b| b as f64)),
        cpu_us_per_record: stat_opt(&|r| r.cpu_us_per_record),
        allocs_per_record: stat_opt(&|r| r.allocs_per_record),
        alloc_bytes_per_record: stat_opt(&|r| r.alloc_bytes_per_record),
        excess_deliveries: stat_opt(&|r| r.excess_deliveries.map(|e| e as f64)),
    })
}

fn summaries(args: &Args, results: &[CellResult], load: Load) -> Vec<Summary> {
    args.approaches
        .iter()
        .filter_map(|name| {
            let runs: Vec<&CellResult> = results
                .iter()
                .filter(|r| r.load == load.id() && &r.approach == name)
                .collect();
            summarise(&runs)
        })
        .collect()
}

/// The difference from the first approach, marked `ns` when it is smaller than
/// either side's spread and so decides nothing.
fn versus(value: Stat, base: Stat) -> String {
    if base.median == 0.0 {
        return "-".to_string();
    }
    let change = (value.median - base.median) / base.median;
    let noise = value.spread.max(base.spread);
    if change.abs() <= noise {
        format!("{:+.1}% ns", change * 100.0)
    } else {
        format!("{:+.1}%", change * 100.0)
    }
}

fn cell(stat: Option<Stat>, scale: f64, precision: usize) -> String {
    stat.map_or_else(
        || "-".to_string(),
        |s| {
            format!(
                "{:.precision$} ({:.0}%)",
                s.median / scale,
                s.spread * 100.0
            )
        },
    )
}

/// One table per load, a row per approach: medians over its runs, each with
/// its spread in brackets.
pub fn tables(args: &Args, results: &[CellResult]) {
    for load in &args.loads {
        let rows = summaries(args, results, *load);
        let Some(base) = rows.first() else {
            continue;
        };
        let mut out = String::new();
        let _ = writeln!(out, "\n## {} -- {}", load.id(), load.describe());
        let _ = writeln!(
            out,
            "| approach | runs | rec/s | vs first | p50 ms | p99 ms | p99 vs first | p999 ms | iso p99 ms | held MiB | rss MiB | cpu us/rec | allocs/rec | alloc B/rec | excess |"
        );
        let _ = writeln!(
            out,
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
        );
        for row in &rows {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                row.approach,
                row.runs,
                cell(Some(row.records_per_sec), 1.0, 0),
                versus(row.records_per_sec, base.records_per_sec),
                cell(Some(row.p50_us), 1000.0, 2),
                cell(Some(row.p99_us), 1000.0, 2),
                versus(row.p99_us, base.p99_us),
                cell(Some(row.p999_us), 1000.0, 2),
                cell(row.isolated_p99_us, 1000.0, 2),
                cell(Some(row.peak_held_bytes), 1_048_576.0, 1),
                cell(row.peak_rss_bytes, 1_048_576.0, 1),
                cell(row.cpu_us_per_record, 1.0, 2),
                cell(row.allocs_per_record, 1.0, 2),
                cell(row.alloc_bytes_per_record, 1.0, 0),
                cell(row.excess_deliveries, 1.0, 0),
            );
        }
        print!("{out}");
    }
    println!(
        "\nEach cell is the median over runs, (spread) is (max - min) / median, and ns marks a difference within the spread."
    );
}

/// Write each (approach, load) median to `path` for a later `--compare`.
pub fn save(path: &Path, args: &Args, results: &[CellResult]) -> std::io::Result<()> {
    let mut rows = Vec::new();
    for load in &args.loads {
        for s in summaries(args, results, *load) {
            rows.push(json!({
                "approach": s.approach,
                "load": load.id(),
                "records_per_sec": s.records_per_sec.median,
                "p99_us": s.p99_us.median,
                "spread_records_per_sec": s.records_per_sec.spread,
                "spread_p99": s.p99_us.spread,
            }));
        }
    }
    let saved = json!({
        "tier": args.tier.number(),
        "workers": args.workers,
        "presets_calibrated": PRESETS_CALIBRATED,
        "results": rows,
    });
    std::fs::write(path, format!("{saved:#}\n"))
}

/// Compare medians with a saved file. False when any (approach, load) lost more
/// than `max` of its records per second or grew its p99 by more than `max`.
pub fn compare(path: &Path, args: &Args, results: &[CellResult]) -> Result<bool, String> {
    let max = args.max_regression;
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let saved: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let rows = saved["results"]
        .as_array()
        .ok_or("no results array in the saved file")?;
    let mut passed = true;
    for load in &args.loads {
        for now in summaries(args, results, *load) {
            let Some(before) = rows
                .iter()
                .find(|r| r["approach"] == now.approach.as_str() && r["load"] == load.id())
            else {
                println!(
                    "compare: {} {}: not in the saved file",
                    now.approach,
                    load.id()
                );
                continue;
            };
            let rps = before["records_per_sec"].as_f64().unwrap_or(0.0);
            let p99 = before["p99_us"].as_f64().unwrap_or(0.0);
            let lost = if rps > 0.0 {
                (rps - now.records_per_sec.median) / rps
            } else {
                0.0
            };
            let grew = if p99 > 0.0 {
                (now.p99_us.median - p99) / p99
            } else {
                0.0
            };
            let ok = lost <= max && grew <= max;
            passed &= ok;
            println!(
                "compare: {} {} {}: rec/s {:+.1}%, p99 {:+.1}% (bound {:.1}%)",
                if ok { "ok" } else { "FAIL" },
                now.approach,
                load.id(),
                -lost * 100.0,
                grew * 100.0,
                max * 100.0
            );
        }
    }
    Ok(passed)
}
