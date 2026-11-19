// Project:   scalo
// File:      benches/delivery_path/report.rs
// Purpose:   Per-run JSON lines, per-load tables, and the saved-baseline gate
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::fmt::Write as _;
use std::path::Path;

use serde_json::{Value, json};
use sketches_ddsketch::{Config, DDSketch};

use scalo::transport::test_support::delivery::{AnyApproach, DeliveryApproach, Outcome};
use scalo::transport::test_support::shaped::PRESETS_CALIBRATED;

use crate::args::Args;
use crate::drive::RawRun;
use crate::loads::{Group, Load};

/// One group's figures in one run.
pub struct GroupResult {
    pub label: &'static str,
    pub touches_sick: bool,
    pub batches: u64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub records: [u64; 4],
}

/// One (approach, load, run).
pub struct RunResult {
    pub approach: &'static str,
    pub load: Load,
    pub run: usize,
    pub tier: u8,
    pub elapsed_s: f64,
    pub records_sent: u64,
    pub records_per_sec: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    /// p99 of the callers whose records do not touch the sick destination.
    pub isolated_p99_us: Option<f64>,
    pub groups: Vec<GroupResult>,
    pub peak_held_bytes: u64,
    pub peak_rss_bytes: Option<u64>,
    pub cpu_us_per_record: Option<f64>,
    /// Confirmations at the destinations beyond what the callers were told
    /// was delivered: copies a caller's retry would send again.
    pub excess_deliveries: Option<i64>,
}

impl RunResult {
    pub fn new(
        args: &Args,
        load: Load,
        run: usize,
        approach: AnyApproach,
        groups: &[Group],
        raw: &RawRun,
        confirmed: Option<u64>,
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
                label: group.label,
                touches_sick,
                batches: tally.batches,
                p50_us: tally.quantile_us(0.50).unwrap_or(0.0),
                p99_us: tally.quantile_us(0.99).unwrap_or(0.0),
                p999_us: tally.quantile_us(0.999).unwrap_or(0.0),
                records: tally.records,
            });
        }
        let records_sent: u64 = raw.groups.iter().map(|g| g.count(Outcome::Sent)).sum();
        let elapsed_s = raw.elapsed.as_secs_f64();
        let quantile = |s: &DDSketch, q| s.quantile(q).ok().flatten().unwrap_or(0.0);
        Self {
            approach: approach.name(),
            load,
            run,
            tier: args.tier.number(),
            elapsed_s,
            records_sent,
            records_per_sec: records_sent as f64 / elapsed_s.max(f64::EPSILON),
            p50_us: quantile(&all, 0.50),
            p99_us: quantile(&all, 0.99),
            p999_us: quantile(&all, 0.999),
            isolated_p99_us: (sick.is_some() && isolated.count() > 0)
                .then(|| quantile(&isolated, 0.99)),
            groups: results,
            peak_held_bytes: raw.peak_held_bytes,
            peak_rss_bytes: raw.peak_rss_bytes,
            cpu_us_per_record: raw
                .cpu_ns
                .filter(|_| records_sent > 0)
                .map(|ns| ns as f64 / 1000.0 / records_sent as f64),
            excess_deliveries: confirmed.map(|c| {
                i64::try_from(c).unwrap_or(i64::MAX) - i64::try_from(expected).unwrap_or(i64::MAX)
            }),
        }
    }

    pub fn json_line(&self) -> String {
        let groups: Vec<Value> = self
            .groups
            .iter()
            .map(|g| {
                json!({
                    "group": g.label,
                    "touches_sick": g.touches_sick,
                    "batches": g.batches,
                    "p50_us": round(g.p50_us),
                    "p99_us": round(g.p99_us),
                    "p999_us": round(g.p999_us),
                    "sent": g.records[Outcome::Sent as usize],
                    "dead_lettered": g.records[Outcome::DeadLettered as usize],
                    "backpressured": g.records[Outcome::Backpressured as usize],
                    "failed": g.records[Outcome::Failed as usize],
                })
            })
            .collect();
        json!({
            "approach": self.approach,
            "load": self.load.id(),
            "run": self.run,
            "tier": self.tier,
            "presets_calibrated": PRESETS_CALIBRATED,
            "elapsed_s": round(self.elapsed_s),
            "records_sent": self.records_sent,
            "records_per_sec": round(self.records_per_sec),
            "p50_us": round(self.p50_us),
            "p99_us": round(self.p99_us),
            "p999_us": round(self.p999_us),
            "isolated_p99_us": self.isolated_p99_us.map(round),
            "peak_held_bytes": self.peak_held_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "cpu_us_per_record": self.cpu_us_per_record.map(round),
            "excess_deliveries": self.excess_deliveries,
            "groups": groups,
        })
        .to_string()
    }
}

fn round(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// Medians of one approach's runs under one load.
struct Summary {
    approach: &'static str,
    runs: usize,
    records_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    isolated_p99_us: Option<f64>,
    peak_held_bytes: f64,
    peak_rss_bytes: Option<f64>,
    cpu_us_per_record: Option<f64>,
    excess_deliveries: Option<f64>,
    spread_records_per_sec: f64,
    spread_p99: f64,
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

/// (max - min) / median: how far the same code moved between its runs.
fn spread(values: &[f64]) -> f64 {
    let max = values.iter().copied().fold(f64::MIN, f64::max);
    let min = values.iter().copied().fold(f64::MAX, f64::min);
    let mid = median(values.to_vec());
    if mid == 0.0 { 0.0 } else { (max - min) / mid }
}

fn summarise(approach: &'static str, runs: &[&RunResult]) -> Summary {
    let pick = |f: &dyn Fn(&RunResult) -> f64| median(runs.iter().map(|r| f(r)).collect());
    let pick_opt = |f: &dyn Fn(&RunResult) -> Option<f64>| {
        let values: Vec<f64> = runs.iter().filter_map(|r| f(r)).collect();
        (!values.is_empty()).then(|| median(values))
    };
    let rps: Vec<f64> = runs.iter().map(|r| r.records_per_sec).collect();
    let p99: Vec<f64> = runs.iter().map(|r| r.p99_us).collect();
    Summary {
        approach,
        runs: runs.len(),
        records_per_sec: pick(&|r| r.records_per_sec),
        p50_us: pick(&|r| r.p50_us),
        p99_us: pick(&|r| r.p99_us),
        p999_us: pick(&|r| r.p999_us),
        isolated_p99_us: pick_opt(&|r| r.isolated_p99_us),
        peak_held_bytes: pick(&|r| r.peak_held_bytes as f64),
        peak_rss_bytes: pick_opt(&|r| r.peak_rss_bytes.map(|b| b as f64)),
        cpu_us_per_record: pick_opt(&|r| r.cpu_us_per_record),
        excess_deliveries: pick_opt(&|r| r.excess_deliveries.map(|e| e as f64)),
        spread_records_per_sec: spread(&rps),
        spread_p99: spread(&p99),
    }
}

fn summaries(args: &Args, results: &[RunResult], load: Load) -> Vec<Summary> {
    let mut out = Vec::new();
    for name in &args.approaches {
        let runs: Vec<&RunResult> = results
            .iter()
            .filter(|r| r.load == load && r.approach == name.as_str())
            .collect();
        if let Some(first) = runs.first() {
            out.push(summarise(first.approach, &runs));
        }
    }
    out
}

fn percent(value: f64, base: f64) -> String {
    if base == 0.0 {
        "-".to_string()
    } else {
        format!("{:+.1}%", (value - base) / base * 100.0)
    }
}

fn ms(us: f64) -> String {
    format!("{:.2}", us / 1000.0)
}

fn mib(bytes: f64) -> String {
    format!("{:.1}", bytes / (1024.0 * 1024.0))
}

/// One table per load, a row per approach, medians over its runs.
pub fn tables(args: &Args, results: &[RunResult]) {
    for load in &args.loads {
        let rows = summaries(args, results, *load);
        let Some(base) = rows.first() else {
            continue;
        };
        let mut out = String::new();
        let _ = writeln!(out, "\n## {} -- {}", load.id(), load.describe());
        let _ = writeln!(
            out,
            "| approach | runs | rec/s | vs first | p50 ms | p99 ms | p999 ms | p99 vs first | iso p99 ms | held MiB | rss MiB | cpu us/rec | excess | spread rec/s | spread p99 |"
        );
        let _ = writeln!(
            out,
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
        );
        for row in &rows {
            let _ = writeln!(
                out,
                "| {} | {} | {:.0} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.1}% | {:.1}% |",
                row.approach,
                row.runs,
                row.records_per_sec,
                percent(row.records_per_sec, base.records_per_sec),
                ms(row.p50_us),
                ms(row.p99_us),
                ms(row.p999_us),
                percent(row.p99_us, base.p99_us),
                row.isolated_p99_us.map_or_else(|| "-".to_string(), ms),
                mib(row.peak_held_bytes),
                row.peak_rss_bytes.map_or_else(|| "-".to_string(), mib),
                row.cpu_us_per_record
                    .map_or_else(|| "-".to_string(), |c| format!("{c:.2}")),
                row.excess_deliveries
                    .map_or_else(|| "-".to_string(), |e| format!("{e:.0}")),
                row.spread_records_per_sec * 100.0,
                row.spread_p99 * 100.0,
            );
        }
        print!("{out}");
    }
}

/// Write each (approach, load) median to `path` for a later `--compare`.
pub fn save(path: &Path, args: &Args, results: &[RunResult]) -> std::io::Result<()> {
    let mut rows = Vec::new();
    for load in &args.loads {
        for s in summaries(args, results, *load) {
            rows.push(json!({
                "approach": s.approach,
                "load": load.id(),
                "records_per_sec": s.records_per_sec,
                "p99_us": s.p99_us,
                "spread_records_per_sec": s.spread_records_per_sec,
                "spread_p99": s.spread_p99,
            }));
        }
    }
    let saved = json!({
        "tier": args.tier.number(),
        "presets_calibrated": PRESETS_CALIBRATED,
        "results": rows,
    });
    std::fs::write(path, format!("{saved:#}\n"))
}

/// Compare medians with a saved file. False when any (approach, load) lost more
/// than `max` of its records per second or grew its p99 by more than `max`.
pub fn compare(path: &Path, results: &[RunResult], max: f64) -> Result<bool, String> {
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let saved: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    let rows = saved["results"]
        .as_array()
        .ok_or("no results array in the saved file")?;
    let mut loads: Vec<Load> = results.iter().map(|r| r.load).collect();
    loads.sort();
    loads.dedup();
    let mut passed = true;
    for load in loads {
        let mut names: Vec<&'static str> = Vec::new();
        for result in results.iter().filter(|r| r.load == load) {
            if !names.contains(&result.approach) {
                names.push(result.approach);
            }
        }
        for name in names {
            let runs: Vec<&RunResult> = results
                .iter()
                .filter(|r| r.load == load && r.approach == name)
                .collect();
            let now = summarise(name, &runs);
            let Some(before) = rows
                .iter()
                .find(|r| r["approach"] == name && r["load"] == load.id())
            else {
                println!("compare: {name} {}: not in the saved file", load.id());
                continue;
            };
            let rps = before["records_per_sec"].as_f64().unwrap_or(0.0);
            let p99 = before["p99_us"].as_f64().unwrap_or(0.0);
            let lost = if rps > 0.0 {
                (rps - now.records_per_sec) / rps
            } else {
                0.0
            };
            let grew = if p99 > 0.0 {
                (now.p99_us - p99) / p99
            } else {
                0.0
            };
            let ok = lost <= max && grew <= max;
            passed &= ok;
            println!(
                "compare: {} {name} {}: rec/s {:+.1}%, p99 {:+.1}% (bound {:.1}%)",
                if ok { "ok" } else { "FAIL" },
                load.id(),
                -lost * 100.0,
                grew * 100.0,
                max * 100.0
            );
        }
    }
    Ok(passed)
}
