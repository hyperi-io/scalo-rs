// Project:   scalo
// File:      benches/rules_benchmark.rs
// Purpose:   Criterion benchmarks for rule set evaluation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Rule set evaluation per record over raw JSON bytes. `scripts/bench-gate.sh`
//! fails a change whose median for any of these is over 5% slower than the
//! saved baseline.

use std::collections::HashSet;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use scalo::rules::{
    Clamp, CompileOptions, JsonPayload, MatchConfig, RuleConfig, RuleSet, RuleSetConfig, SetAction,
};

const PAYLOAD: &[u8] = br#"{"host":"db-17.prod","tags":{"event":{"category":"logs_syslog","org_id":"acme"}},"severity":3,"id":12345,"timestamp":"2026-04-10T12:00:00Z","message":"Sample log event with some realistic padding for benchmarking"}"#;

fn compile(config: &RuleSetConfig) -> RuleSet {
    RuleSet::compile_with(config, &CompileOptions::default()).expect("the bench set compiles")
}

/// Twenty `starts_with` rows on one field, the record matching the last.
fn twenty_rows() -> RuleSet {
    let mut config = RuleSetConfig::new("bench", ["_source"]);
    for i in 0..19 {
        config = config.rule(
            RuleConfig::new(
                format!("r{i}"),
                "_source",
                SetAction::value(format!("s{i}")),
            )
            .matching(MatchConfig::starts_with("host", format!("web-{i}"))),
        );
    }
    compile(
        &config.rule(
            RuleConfig::new("db", "_source", SetAction::value("db"))
                .matching(MatchConfig::starts_with("host", "db-")),
        ),
    )
}

/// A header compatibility shape: a lookup, a fallback, two org copies.
fn two_targets() -> RuleSet {
    compile(
        &RuleSetConfig::new("bench", ["_source", "_org_id"])
            .lookup(
                "aliases",
                [("logs_syslog", "syslog"), ("logs_beats_winlog", "windows")],
            )
            .rule(RuleConfig::new(
                "category",
                "_source",
                SetAction::lookup("aliases", "tags.event.category"),
            ))
            .rule(RuleConfig::new("main", "_source", SetAction::value("main")))
            .rule(
                RuleConfig::new("org-tags", "_org_id", SetAction::from("tags.event.org_id"))
                    .matching(MatchConfig::exists("tags.event.org_id")),
            )
            .rule(RuleConfig::new(
                "org-flat",
                "_org_id",
                SetAction::from("org_id"),
            )),
    )
}

fn bench_native(c: &mut Criterion) {
    let first = compile(
        &RuleSetConfig::new("bench", ["_source"]).rule(
            RuleConfig::new("db", "_source", SetAction::value("db"))
                .matching(MatchConfig::starts_with("host", "db-")),
        ),
    );
    let twenty = twenty_rows();
    let payload = JsonPayload::new(PAYLOAD).expect("depth");

    let mut group = c.benchmark_group("rules_native");
    group.throughput(Throughput::Elements(1));
    group.bench_function("first_row", |b| {
        let mut batch = first.batch();
        b.iter(|| std::hint::black_box(batch.evaluate(&payload).is_empty()));
    });
    group.bench_function("row_20_same_field", |b| {
        let mut batch = twenty.batch();
        b.iter(|| std::hint::black_box(batch.evaluate(&payload).is_empty()));
    });
    group.finish();
}

fn bench_two_targets(c: &mut Criterion) {
    let set = two_targets();
    let org = set.target("_org_id").expect("target");
    let allowed: HashSet<String> = ["acme".to_string(), "default".to_string()].into();
    let clamp = Clamp::new(org, &allowed, "default");
    let payload = JsonPayload::new(PAYLOAD).expect("depth");

    let mut group = c.benchmark_group("rules_two_targets");
    group.throughput(Throughput::Elements(1));
    group.bench_function("lookup_and_copy", |b| {
        let mut batch = set.batch();
        b.iter(|| std::hint::black_box(batch.evaluate(&payload).is_empty()));
    });
    group.bench_function("lookup_and_copy_clamped", |b| {
        let mut batch = set.batch();
        b.iter(|| std::hint::black_box(batch.evaluate_clamped(&payload, &clamp).is_empty()));
    });
    group.finish();
}

fn bench_cel(c: &mut Criterion) {
    let set = compile(
        &RuleSetConfig::new("bench", ["_source"]).rule(
            RuleConfig::new("cel", "_source", SetAction::value("hit"))
                .when(r#"severity > 2 && host.startsWith("db-")"#),
        ),
    );
    let payload = JsonPayload::new(PAYLOAD).expect("depth");

    let mut group = c.benchmark_group("rules_cel");
    group.throughput(Throughput::Elements(1));
    group.bench_function("condition", |b| {
        let mut batch = set.batch();
        b.iter(|| std::hint::black_box(batch.evaluate(&payload).is_empty()));
    });
    group.finish();
}

criterion_group!(benches, bench_native, bench_two_targets, bench_cel);
criterion_main!(benches);
