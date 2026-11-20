// Project:   scalo
// File:      benches/delivery_path/report.rs
// Purpose:   Per-cell JSON lines, per-load tables, and the saved-baseline gate
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sketches_ddsketch::{Config, DDSketch};

use scalo::transport::test_support::delivery::Outcome;
use scalo::transport::test_support::shaped::PRESETS_CALIBRATED;

use crate::args::{Args, Spec, Tier};
use crate::drive::RawRun;
use crate::loads::{Group, Load};

/// One group's figures in one cell.
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupResult {
    pub group: String,
    pub touches_sick: bool,
    pub batches: u64,
    pub offers: u64,
    pub late_offers: u64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub first_answer_p99_us: f64,
    pub sent: u64,
    pub dead_lettered: u64,
    pub backpressured: u64,
    pub failed: u64,
}

/// The Kafka destination's queue in one cell.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct KafkaQueue {
    pub cap_bytes: u64,
    /// The queue's own peak: tier 1 only.
    pub peak_bytes: Option<u64>,
    /// Offers that waited for room: tier 1 only.
    pub full_waits: Option<u64>,
    /// Offers refused for want of room: tier 1 only.
    pub full_refusals: Option<u64>,
    /// Calls the destination answered `Backpressured`.
    pub backpressured_answers: Option<u64>,
    /// Payload bytes inside calls to the destination at once: an upper bound
    /// on what the queue held, and the only queue figure on tier 2.
    pub caller_side_peak_bytes: Option<u64>,
}

/// One (approach, retry, load, mode, run), measured in a process of its own.
#[derive(Debug, Serialize, Deserialize)]
pub struct CellResult {
    pub label: String,
    pub approach: String,
    pub retry: String,
    pub load: String,
    pub mode: String,
    pub rate: Option<f64>,
    pub run: usize,
    pub tier: u8,
    /// When the cell's callers started and finished, milliseconds since the Unix epoch.
    pub started_unix_ms: u64,
    pub ended_unix_ms: u64,
    pub workers: usize,
    /// CPUs the cell process was allowed to run on.
    pub allowed_cpus: Option<u32>,
    pub presets_calibrated: bool,
    pub elapsed_s: f64,
    pub records_sent: u64,
    pub records_per_sec: f64,
    pub offered_records_per_sec: f64,
    pub late_offers: u64,
    pub resent_records: u64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub first_answer_p99_us: f64,
    /// p99 of the callers whose records do not touch the sick destination.
    pub isolated_p99_us: Option<f64>,
    pub peak_held_bytes: u64,
    /// Most payload bytes waiting in a merge buffer at once, for a merge
    /// approach. Those bytes are inside `peak_held_bytes` already.
    pub peak_queued_bytes: Option<u64>,
    /// The cell process's `VmHWM`: its senders, the approach, the callers and,
    /// on tier 2, the in-process gRPC listeners. The broker is not in it.
    pub peak_rss_bytes: Option<u64>,
    /// The cell process's user plus system CPU while its callers ran.
    pub cpu_us_per_record: Option<f64>,
    pub allocs_per_record: Option<f64>,
    pub alloc_bytes_per_record: Option<f64>,
    /// Confirmations at the destinations beyond what the callers were told
    /// was delivered: copies a caller's retry sends, or would send, again.
    pub excess_deliveries: Option<i64>,
    pub kafka_queue: KafkaQueue,
    /// Why the cell decides nothing, when it does not.
    pub invalid: Option<String>,
    pub groups: Vec<GroupResult>,
    #[serde(skip)]
    expected_deliveries: u64,
}

fn per_record(total: Option<u64>, records: u64) -> Option<f64> {
    total
        .filter(|_| records > 0)
        .map(|t| t as f64 / records as f64)
}

fn quantile(sketch: &DDSketch, q: f64) -> f64 {
    round(sketch.quantile(q).ok().flatten().unwrap_or(0.0))
}

impl CellResult {
    pub fn new(
        args: &Args,
        load: Load,
        spec: &Spec,
        groups: &[Group],
        raw: &RawRun,
        peak_rss_bytes: Option<u64>,
    ) -> Self {
        let sick = load.sick();
        let mut all = DDSketch::new(Config::defaults());
        let mut first = DDSketch::new(Config::defaults());
        let mut isolated = DDSketch::new(Config::defaults());
        let mut expected: u64 = 0;
        let mut offered_records: u64 = 0;
        let mut results = Vec::new();
        for (group, tally) in groups.iter().zip(&raw.groups) {
            let _ = all.merge(&tally.latency_us);
            let _ = first.merge(&tally.first_latency_us);
            let touches_sick = sick.is_some_and(|s| group.touches(s));
            if sick.is_some() && !touches_sick {
                let _ = isolated.merge(&tally.latency_us);
            }
            let routes = group.routes.len() as u64;
            expected += tally.count(Outcome::Sent) * routes
                + tally.count(Outcome::DeadLettered) * routes.saturating_sub(1);
            let per_batch = group.batches.first().map_or(0, |b| b.len()) as u64;
            offered_records += tally.offers * per_batch;
            results.push(GroupResult {
                group: group.label.to_string(),
                touches_sick,
                batches: tally.batches,
                offers: tally.offers,
                late_offers: tally.late,
                p50_us: round(tally.quantile_us(0.50).unwrap_or(0.0)),
                p99_us: round(tally.quantile_us(0.99).unwrap_or(0.0)),
                p999_us: round(tally.quantile_us(0.999).unwrap_or(0.0)),
                first_answer_p99_us: round(tally.first_quantile_us(0.99).unwrap_or(0.0)),
                sent: tally.count(Outcome::Sent),
                dead_lettered: tally.count(Outcome::DeadLettered),
                backpressured: tally.count(Outcome::Backpressured),
                failed: tally.count(Outcome::Failed),
            });
        }
        let records_sent: u64 = raw.groups.iter().map(|g| g.count(Outcome::Sent)).sum();
        let elapsed_s = raw.elapsed.as_secs_f64().max(f64::EPSILON);
        Self {
            label: spec.label.clone(),
            approach: spec.name.clone(),
            retry: spec.retry.name().to_string(),
            load: load.id().to_string(),
            mode: args.mode.clone(),
            rate: args.rate,
            run: args.cell_run.unwrap_or(0),
            tier: args.tier.number(),
            started_unix_ms: raw.started_unix_ms,
            ended_unix_ms: raw.ended_unix_ms,
            workers: args.workers,
            allowed_cpus: crate::host::allowed_cpus(),
            presets_calibrated: PRESETS_CALIBRATED,
            elapsed_s: round(elapsed_s),
            records_sent,
            records_per_sec: round(records_sent as f64 / elapsed_s),
            offered_records_per_sec: round(offered_records as f64 / elapsed_s),
            late_offers: raw.groups.iter().map(|g| g.late).sum(),
            resent_records: raw.groups.iter().map(|g| g.resent).sum(),
            p50_us: quantile(&all, 0.50),
            p99_us: quantile(&all, 0.99),
            p999_us: quantile(&all, 0.999),
            first_answer_p99_us: quantile(&first, 0.99),
            isolated_p99_us: (sick.is_some() && isolated.count() > 0)
                .then(|| quantile(&isolated, 0.99)),
            peak_held_bytes: raw.peak_held_bytes,
            peak_queued_bytes: None,
            peak_rss_bytes,
            cpu_us_per_record: per_record(raw.cpu_ns, records_sent).map(|ns| round(ns / 1000.0)),
            allocs_per_record: per_record(raw.allocs.map(|a| a.0), records_sent).map(round),
            alloc_bytes_per_record: per_record(raw.allocs.map(|a| a.1), records_sent).map(round),
            excess_deliveries: None,
            kafka_queue: KafkaQueue::default(),
            invalid: None,
            groups: results,
            expected_deliveries: expected,
        }
    }

    /// Add what the destinations saw, and mark a W6 cell whose Kafka queue
    /// never filled as deciding nothing.
    pub fn finish(mut self, confirmed: Option<u64>, queue: KafkaQueue) -> Self {
        self.excess_deliveries = confirmed.map(|c| {
            i64::try_from(c).unwrap_or(i64::MAX)
                - i64::try_from(self.expected_deliveries).unwrap_or(i64::MAX)
        });
        if self.load == Load::W6.id() {
            let peak = queue
                .peak_bytes
                .or(queue.caller_side_peak_bytes)
                .unwrap_or(0);
            if peak < queue.cap_bytes {
                self.invalid = Some(format!(
                    "the kafka queue peaked at {peak} bytes, under its {} byte cap",
                    queue.cap_bytes
                ));
            }
        }
        self.kafka_queue = queue;
        self
    }

    pub fn json_line(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

fn round(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// Median and spread of one metric over an approach's runs.
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

/// Everything a run collected.
pub struct Collected {
    pub cells: Vec<CellResult>,
    /// Idle-cell RSS per load id.
    pub idle_rss: BTreeMap<String, u64>,
    /// The run ended early on its stop file.
    pub stopped: bool,
}

fn cells_of<'a>(
    results: &'a [CellResult],
    load: Load,
    mode: &str,
    label: &str,
) -> Vec<&'a CellResult> {
    results
        .iter()
        .filter(|r| r.load == load.id() && r.mode == mode && r.label == label)
        .collect()
}

/// Paired differences from the first approach, round by round: median, min, max.
fn paired(
    results: &[CellResult],
    load: Load,
    mode: &str,
    base: &str,
    label: &str,
    metric: fn(&CellResult) -> f64,
) -> Option<(f64, f64, f64)> {
    let base_cells = cells_of(results, load, mode, base);
    let mut diffs: Vec<f64> = cells_of(results, load, mode, label)
        .iter()
        .filter_map(|cell| {
            let b = base_cells.iter().find(|b| b.run == cell.run)?;
            let base = metric(b);
            (base != 0.0).then(|| (metric(cell) - base) / base)
        })
        .collect();
    if diffs.is_empty() {
        return None;
    }
    diffs.sort_by(f64::total_cmp);
    Some((median(diffs.clone()), diffs[0], diffs[diffs.len() - 1]))
}

fn stat(cells: &[&CellResult], f: impl Fn(&CellResult) -> Option<f64>) -> Option<Stat> {
    Stat::of(&cells.iter().filter_map(|c| f(c)).collect::<Vec<f64>>())
}

fn show(stat: Option<Stat>, scale: f64, precision: usize) -> String {
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

/// The difference from the first approach, marked `ns` when it is within
/// either side's spread, and `indicative` on tier 1, where approaches are not
/// chosen.
fn versus(value: Option<Stat>, base: Option<Stat>, tier: Tier) -> String {
    let (Some(value), Some(base)) = (value, base) else {
        return "-".to_string();
    };
    if base.median == 0.0 {
        return "-".to_string();
    }
    let change = (value.median - base.median) / base.median;
    let mut text = format!("{:+.1}%", change * 100.0);
    if change.abs() <= value.spread.max(base.spread) {
        text.push_str(" ns");
    }
    if tier == Tier::One {
        text.push_str(" indicative");
    }
    text
}

fn show_paired(pair: Option<(f64, f64, f64)>) -> String {
    pair.map_or_else(
        || "-".to_string(),
        |(mid, low, high)| {
            format!(
                "{:+.1}% [{:+.0}, {:+.0}]",
                mid * 100.0,
                low * 100.0,
                high * 100.0
            )
        },
    )
}

/// One table per load and mode, a row per approach: medians over its runs,
/// each with its spread in brackets, then the per-group tails.
pub fn tables(args: &Args, collected: &Collected) {
    let results = &collected.cells;
    for load in &args.loads {
        for mode in args.modes() {
            let Some(base) = args.approaches.first() else {
                continue;
            };
            let base_cells = cells_of(results, *load, &mode, &base.label);
            if base_cells.is_empty() {
                continue;
            }
            let base_rps = stat(&base_cells, |c| Some(c.records_per_sec));
            let base_p99 = stat(&base_cells, |c| Some(c.p99_us));
            let idle = collected.idle_rss.get(load.id()).copied();
            let mut out = String::new();
            let _ = writeln!(out, "\n## {} {mode} -- {}", load.id(), load.describe());
            let _ = writeln!(
                out,
                "| approach | runs | offered rec/s | rec/s | vs first | paired rec/s | p50 ms | p99 ms | vs first | paired p99 | first-answer p99 ms | p999 ms | iso p99 ms | late | resent | held MiB | queued MiB | rss over idle MiB | cpu us/rec | allocs/rec | excess | invalid |"
            );
            let _ = writeln!(out, "|{}", "---|".repeat(22));
            for spec in &args.approaches {
                let cells = cells_of(results, *load, &mode, &spec.label);
                if cells.is_empty() {
                    continue;
                }
                let rps = stat(&cells, |c| Some(c.records_per_sec));
                let p99 = stat(&cells, |c| Some(c.p99_us));
                let invalid = cells.iter().filter(|c| c.invalid.is_some()).count();
                let rss_over_idle = stat(&cells, |c| {
                    c.peak_rss_bytes
                        .zip(idle)
                        .map(|(p, i)| p.saturating_sub(i) as f64)
                });
                let _ = writeln!(
                    out,
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    spec.label,
                    cells.len(),
                    show(stat(&cells, |c| Some(c.offered_records_per_sec)), 1.0, 0),
                    show(rps, 1.0, 0),
                    versus(rps, base_rps, args.tier),
                    show_paired(paired(
                        results,
                        *load,
                        &mode,
                        &base.label,
                        &spec.label,
                        |c| { c.records_per_sec }
                    )),
                    show(stat(&cells, |c| Some(c.p50_us)), 1000.0, 2),
                    show(p99, 1000.0, 2),
                    versus(p99, base_p99, args.tier),
                    show_paired(paired(
                        results,
                        *load,
                        &mode,
                        &base.label,
                        &spec.label,
                        |c| { c.p99_us }
                    )),
                    show(stat(&cells, |c| Some(c.first_answer_p99_us)), 1000.0, 2),
                    show(stat(&cells, |c| Some(c.p999_us)), 1000.0, 2),
                    show(stat(&cells, |c| c.isolated_p99_us), 1000.0, 2),
                    show(stat(&cells, |c| Some(c.late_offers as f64)), 1.0, 0),
                    show(stat(&cells, |c| Some(c.resent_records as f64)), 1.0, 0),
                    show(
                        stat(&cells, |c| Some(c.peak_held_bytes as f64)),
                        1_048_576.0,
                        1
                    ),
                    show(
                        stat(&cells, |c| c.peak_queued_bytes.map(|b| b as f64)),
                        1_048_576.0,
                        1
                    ),
                    show(rss_over_idle, 1_048_576.0, 1),
                    show(stat(&cells, |c| c.cpu_us_per_record), 1.0, 2),
                    show(stat(&cells, |c| c.allocs_per_record), 1.0, 2),
                    show(
                        stat(&cells, |c| c.excess_deliveries.map(|e| e as f64)),
                        1.0,
                        0
                    ),
                    if invalid > 0 {
                        format!("{invalid} of {}", cells.len())
                    } else {
                        "-".to_string()
                    },
                );
            }
            group_tails(&mut out, args, results, *load, &mode);
            if *load == Load::W6 {
                kafka_queue(&mut out, args, results, *load, &mode);
            }
            print!("{out}");
        }
    }
    println!(
        "\nEach figure is the median over runs with (max - min) / median in brackets. ns marks a difference within the spread. Paired figures are the per-round differences from the first approach: median [min, max]. Approach order rotates each round. An open-loop cell offers at a fixed rate and times each offer from its scheduled start. per_record_futures holds at most 1000 record futures per caller batch, as the fetcher does. batch_per_destination sends destinations in turn and never stops at the first failure, since each record carries its own outcome. Queued MiB is the merge buffer's peak, already inside held MiB. A merge window of 0.5 ms waits as long as 1 ms on tokio's millisecond timer. RSS is the cell process's peak over an idle cell's on the same load."
    );
    if args.tier == Tier::One {
        println!(
            "Tier 1 differences between approaches are indicative: approaches are chosen on tier 2, and tier 1 compares an approach with its own saved baseline."
        );
    }
}

fn group_tails(out: &mut String, args: &Args, results: &[CellResult], load: Load, mode: &str) {
    let _ = writeln!(
        out,
        "\n| approach | group | p50 ms | p99 ms | p999 ms | first-answer p99 ms |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|");
    for spec in &args.approaches {
        let cells = cells_of(results, load, mode, &spec.label);
        let groups: Vec<String> = cells
            .first()
            .map(|c| c.groups.iter().map(|g| g.group.clone()).collect())
            .unwrap_or_default();
        for group in groups {
            let pick = |f: fn(&GroupResult) -> f64| {
                stat(&cells, |c| {
                    c.groups.iter().find(|g| g.group == group).map(f)
                })
            };
            let _ = writeln!(
                out,
                "| {} | {group} | {} | {} | {} | {} |",
                spec.label,
                show(pick(|g| g.p50_us), 1000.0, 2),
                show(pick(|g| g.p99_us), 1000.0, 2),
                show(pick(|g| g.p999_us), 1000.0, 2),
                show(pick(|g| g.first_answer_p99_us), 1000.0, 2),
            );
        }
    }
}

fn kafka_queue(out: &mut String, args: &Args, results: &[CellResult], load: Load, mode: &str) {
    let _ = writeln!(
        out,
        "\n| approach | queue cap MiB | queue peak MiB | caller-side peak MiB | full waits | full refusals | backpressured answers |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|");
    for spec in &args.approaches {
        let cells = cells_of(results, load, mode, &spec.label);
        let Some(first) = cells.first() else {
            continue;
        };
        let q = |f: fn(&KafkaQueue) -> Option<u64>| {
            stat(&cells, |c| f(&c.kafka_queue).map(|v| v as f64))
        };
        let _ = writeln!(
            out,
            "| {} | {:.1} | {} | {} | {} | {} | {} |",
            spec.label,
            first.kafka_queue.cap_bytes as f64 / 1_048_576.0,
            show(q(|k| k.peak_bytes), 1_048_576.0, 1),
            show(q(|k| k.caller_side_peak_bytes), 1_048_576.0, 1),
            show(q(|k| k.full_waits), 1.0, 0),
            show(q(|k| k.full_refusals), 1.0, 0),
            show(q(|k| k.backpressured_answers), 1.0, 0),
        );
    }
}

/// The settings a saved baseline was measured under.
fn settings(args: &Args) -> Value {
    json!({
        "tier": args.tier.number(),
        "workers": args.workers,
        "cpus": args.cpus,
        "seconds": args.seconds,
        "runs": args.runs,
        "loads": args.loads.iter().map(|l| l.id()).collect::<Vec<&str>>(),
        "approaches": args.approaches.iter().map(|s| s.label.clone()).collect::<Vec<String>>(),
        "modes": args.modes(),
    })
}

/// Write each (approach, load, mode) median and the run's settings to `path`
/// for a later `--compare`.
pub fn save(path: &Path, args: &Args, collected: &Collected) -> std::io::Result<()> {
    let mut rows = Vec::new();
    for load in &args.loads {
        for mode in args.modes() {
            for spec in &args.approaches {
                let cells = cells_of(&collected.cells, *load, &mode, &spec.label);
                let (Some(rps), Some(p99)) = (
                    stat(&cells, |c| Some(c.records_per_sec)),
                    stat(&cells, |c| Some(c.p99_us)),
                ) else {
                    continue;
                };
                rows.push(json!({
                    "approach": spec.label,
                    "load": load.id(),
                    "mode": mode,
                    "records_per_sec": rps.median,
                    "p99_us": p99.median,
                    "spread_records_per_sec": rps.spread,
                    "spread_p99": p99.spread,
                }));
            }
        }
    }
    let saved = json!({
        "settings": settings(args),
        "presets_calibrated": PRESETS_CALIBRATED,
        "stopped": collected.stopped,
        "results": rows,
    });
    std::fs::write(path, format!("{saved:#}\n"))
}

/// How a comparison with a saved baseline ended.
pub enum Verdict {
    Pass,
    Regressed,
    /// The run or the saved file cannot be compared, and why.
    Inconclusive(String),
}

/// Compare medians with a saved file. Regressed when any (approach, load,
/// mode) lost more than `--max-regression` of its records per second or grew
/// its p99 by more. Inconclusive on a stopped or partial run, a cell missing
/// from the file, or settings that differ.
pub fn compare(path: &Path, args: &Args, collected: &Collected) -> Result<Verdict, String> {
    let max = args.max_regression;
    let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let saved: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if collected.stopped {
        return Ok(Verdict::Inconclusive("the run was stopped".into()));
    }
    if saved["stopped"] == true {
        return Ok(Verdict::Inconclusive("the saved run was stopped".into()));
    }
    if saved["settings"] != settings(args) {
        return Ok(Verdict::Inconclusive(format!(
            "settings differ: saved {}, now {}",
            saved["settings"],
            settings(args)
        )));
    }
    let rows = saved["results"]
        .as_array()
        .ok_or("no results array in the saved file")?;
    let mut regressed = false;
    for load in &args.loads {
        for mode in args.modes() {
            for spec in &args.approaches {
                let cells = cells_of(&collected.cells, *load, &mode, &spec.label);
                if cells.len() != args.runs {
                    return Ok(Verdict::Inconclusive(format!(
                        "{} {} {mode} has {} of {} runs",
                        spec.label,
                        load.id(),
                        cells.len(),
                        args.runs
                    )));
                }
                let Some(before) = rows.iter().find(|r| {
                    r["approach"] == spec.label.as_str()
                        && r["load"] == load.id()
                        && r["mode"] == mode.as_str()
                }) else {
                    return Ok(Verdict::Inconclusive(format!(
                        "{} {} {mode} is not in the saved file",
                        spec.label,
                        load.id()
                    )));
                };
                let now_rps = stat(&cells, |c| Some(c.records_per_sec)).map_or(0.0, |s| s.median);
                let now_p99 = stat(&cells, |c| Some(c.p99_us)).map_or(0.0, |s| s.median);
                let rps = before["records_per_sec"].as_f64().unwrap_or(0.0);
                let p99 = before["p99_us"].as_f64().unwrap_or(0.0);
                let lost = if rps > 0.0 {
                    (rps - now_rps) / rps
                } else {
                    0.0
                };
                let grew = if p99 > 0.0 {
                    (now_p99 - p99) / p99
                } else {
                    0.0
                };
                let ok = lost <= max && grew <= max;
                regressed |= !ok;
                println!(
                    "compare: {} {} {} {mode}: rec/s {:+.1}%, p99 {:+.1}% (bound {:.1}%)",
                    if ok { "ok" } else { "FAIL" },
                    spec.label,
                    load.id(),
                    -lost * 100.0,
                    grew * 100.0,
                    max * 100.0
                );
            }
        }
    }
    Ok(if regressed {
        Verdict::Regressed
    } else {
        Verdict::Pass
    })
}
