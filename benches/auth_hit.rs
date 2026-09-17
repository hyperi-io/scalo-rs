// Project:   scalo
// File:      benches/auth_hit.rs
// Purpose:   Criterion benchmark for the credential cache-hit path
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! What a signed request pays for its credential when one is held.
//!
//! The hit is an `ArcSwap` load and a pointer clone: no lock and no park. This
//! is a number for the record, not a CI gate. That it allocates nothing is
//! asserted in `tests/auth_cache_hit_allocation.rs`, because a timing
//! benchmark cannot see an allocation.
//!
//! Run with `cargo bench --bench auth_hit --features auth`.

use std::sync::Arc;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use scalo::auth::{Cached, CredentialSource, Static};

fn bench_cache_hit(c: &mut Criterion) {
    // Timers on: an acquisition bounds the exchange with `tokio::time::timeout`,
    // which panics on a runtime without them.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let source = Arc::new(Cached::new(Static::new("bench-token")));

    // Warm the cache: the measured path is the hit, not the first acquisition.
    runtime
        .block_on(source.credential())
        .expect("a static credential always acquires");

    c.bench_function("auth_credential_cache_hit", |b| {
        b.iter_custom(|iters| {
            runtime.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let credential = source.credential().await.expect("held credential");
                    std::hint::black_box(&credential);
                }
                start.elapsed()
            })
        });
    });
}

criterion_group!(benches, bench_cache_hit);
criterion_main!(benches);
