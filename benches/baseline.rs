// Project:   scalo
// File:      benches/baseline.rs
// Purpose:   scalo-side numbers for Vector baselining (see docs/vector-baselining.md)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! scalo-side baseline benchmarks.
//!
//! These produce the scalo numbers for the Vector BASELINING effort (the goal:
//! the whole picture is better, or at least not worse, than the Vector
//! equivalent -- NOT a competitive bake-off). They run on canonical corpora
//! (fixed small + large JSON log lines) so the per-app benches that compare
//! against the Vector equivalent measure the SAME bytes. See
//! `docs/vector-baselining.md` Section 7 for the full plan; the per-app
//! Vector-equivalent comparisons live in the downstream app repos.
//!
//! Covered here: the [`Record`] wire codec (encode/decode), the serialisation on
//! the spill cache's cold path -- a hot-path-adjacent primitive worth a tracked
//! baseline.

use std::sync::Arc;

use bytes::Bytes;
use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use scalo::transport::{PayloadFormat, Record, RecordMeta, parse};

/// A small (~200 B) canonical JSON log record.
fn small_record() -> Record {
    Record {
        payload: Bytes::from(
            r#"{"_table":"events","host":"web-01","source_type":"syslog","id":42,"level":"info","ts":"2026-04-02T12:00:00Z","message":"request completed in 12ms with status 200"}"#,
        ),
        key: Some(Arc::from("events")),
        headers: vec![("x-scalo-dedup-key".to_string(), b"req-0000000042".to_vec())],
        metadata: RecordMeta {
            timestamp_ms: Some(1_743_580_800_000),
            format: PayloadFormat::Json,
        },
    }
}

/// A large (~4 KiB) canonical JSON record (structured + a padded message).
fn large_record() -> Record {
    let message = "x".repeat(3800);
    Record {
        payload: Bytes::from(format!(
            r#"{{"_table":"events","host":"web-01","source_type":"app","id":42,"trace_id":"abc123def456","span_id":"0011223344","attributes":{{"region":"ap-southeast-2","az":"a","pod":"ingest-7c9","container":"app"}},"message":"{message}"}}"#
        )),
        key: Some(Arc::from("events")),
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: Some(1_743_580_800_000),
            format: PayloadFormat::Json,
        },
    }
}

fn bench_record_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline/record_codec");

    for (name, record) in [
        ("small_200b", small_record()),
        ("large_4kib", large_record()),
    ] {
        let encoded = record.encode();

        // Encode throughput, measured against the payload size.
        group.throughput(Throughput::Bytes(record.payload.len() as u64));
        group.bench_function(format!("encode_{name}"), |b| {
            b.iter(|| black_box(black_box(&record).encode()));
        });

        // Decode throughput, measured against the framed size.
        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_function(format!("decode_{name}"), |b| {
            b.iter(|| black_box(Record::decode(black_box(&encoded)).unwrap()));
        });
    }

    group.finish();
}

/// The ingest decode path: SIMD JSON parse of the payload (scalo's equivalent of
/// a Vector source decoder). The number to baseline against Vector's decode.
fn bench_ingest_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline/ingest_parse");

    for (name, record) in [
        ("small_200b", small_record()),
        ("large_4kib", large_record()),
    ] {
        let payload = record.payload.clone();
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_function(format!("parse_json_{name}"), |b| {
            b.iter(|| black_box(parse(black_box(&payload), PayloadFormat::Json).unwrap()));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_record_codec, bench_ingest_parse);
criterion_main!(benches);
