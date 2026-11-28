// Project:   scalo
// File:      benches/engine_benchmark.rs
// Purpose:   Criterion benchmarks for BatchEngine throughput and overhead
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use scalo::transport::{PayloadFormat, Record, RecordMeta};
use scalo::worker::engine::{BatchEngine, BatchProcessingConfig};
use scalo::worker::{AdaptiveWorkerPool, WorkerPoolConfig};

fn make_messages(n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            payload: Bytes::from(format!(
                r#"{{"_table":"events","host":"web-{i}","source_type":"syslog","id":{i},"timestamp":"2026-04-02T12:00:00Z","message":"Event number {i} with some padding data to make it realistic"}}"#
            )),
            key: Some(Arc::from("partition-key")),
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: Some(1_743_580_800_000),
                format: PayloadFormat::Json,
            },
        })
        .collect()
}

fn bench_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_engine");

    let messages_10k = make_messages(10_000);

    // Benchmark 1: engine mid-tier vs manual serde_json parse
    group.bench_function("engine_mid_tier_10k", |b| {
        let engine = BatchEngine::new(BatchProcessingConfig::default());
        b.iter(|| {
            let results: Vec<Result<(), String>> = engine.process_mid_tier(&messages_10k, |msg| {
                let _ = msg.field("_table");
                Ok(())
            });
            assert_ne!(
                results,
                [] as [std::result::Result<(), std::string::String>; 0]
            );
        });
    });

    group.bench_function("manual_serde_json_10k", |b| {
        b.iter(|| {
            let results: Vec<Result<(), String>> = messages_10k
                .iter()
                .map(|msg| {
                    let v: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
                    let _ = v["_table"].as_str();
                    Ok(())
                })
                .collect();
            assert_ne!(
                results,
                [] as [std::result::Result<(), std::string::String>; 0]
            );
        });
    });

    // Benchmark 2: with vs without pre-route
    group.bench_function("engine_with_pre_route_10k", |b| {
        let config = BatchProcessingConfig {
            routing_field: Some("_table".into()),
            ..BatchProcessingConfig::default()
        };
        let engine = BatchEngine::new(config);
        b.iter(|| {
            let results: Vec<Result<(), String>> =
                engine.process_mid_tier(&messages_10k, |_| Ok(()));
            assert_ne!(
                results,
                [] as [std::result::Result<(), std::string::String>; 0]
            );
        });
    });

    // Benchmark 3: throughput scaling
    for size in [1_000, 5_000, 10_000, 20_000] {
        let msgs = make_messages(size);
        group.bench_with_input(
            BenchmarkId::new("engine_throughput", size),
            &msgs,
            |b, msgs| {
                let engine = BatchEngine::new(BatchProcessingConfig::default());
                b.iter(|| {
                    let results: Vec<Result<(), String>> = engine.process_mid_tier(msgs, |msg| {
                        let _ = msg.field("_table");
                        let _ = msg.field("host");
                        Ok(())
                    });
                    assert_ne!(
                        results,
                        [] as [std::result::Result<(), std::string::String>; 0]
                    );
                });
            },
        );
    }

    // Benchmark 4: process_raw (zero-copy passthrough)
    group.bench_function("engine_raw_10k", |b| {
        let engine = BatchEngine::new(BatchProcessingConfig::default());
        b.iter(|| {
            let results: Vec<Result<usize, String>> =
                engine.process_raw(&messages_10k, |msg| Ok(msg.payload.len()));
            assert_ne!(
                results,
                [] as [std::result::Result<usize, std::string::String>; 0]
            );
        });
    });

    group.finish();
}

// Per-item limiter cost: a no-op map should cost the same per item at 1 and 16 threads.
fn bench_pool_map_owned(c: &mut Criterion) {
    const ITEMS: usize = 100_000;
    let mut group = c.benchmark_group("pool_map_owned_noop");
    group.throughput(Throughput::Elements(ITEMS as u64));
    let items: Vec<u64> = (0..ITEMS as u64).collect();

    for threads in [1, 16] {
        let pool = AdaptiveWorkerPool::new(WorkerPoolConfig {
            min_threads: threads,
            max_threads: threads,
            ..WorkerPoolConfig::default()
        });
        group.bench_with_input(BenchmarkId::new("threads", threads), &items, |b, items| {
            b.iter_batched(
                || items.clone(),
                |batch| pool.map_owned(batch, std::hint::black_box),
                BatchSize::LargeInput,
            );
        });
        // The same map with no limiter, so the gap to `threads` is the limiter's cost.
        group.bench_with_input(
            BenchmarkId::new("rayon_only", threads),
            &items,
            |b, items| {
                b.iter_batched(
                    || items.clone(),
                    |batch| {
                        pool.install(|| {
                            use rayon::prelude::*;
                            batch
                                .into_par_iter()
                                .map(std::hint::black_box)
                                .collect::<Vec<_>>()
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_engine, bench_pool_map_owned);
criterion_main!(benches);
