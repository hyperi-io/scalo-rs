// Project:   scalo
// File:      benches/support/scaling.rs
// Purpose:   Run one operation from several threads at once in a bench
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Thread-scaling groups for state that many workers share.

use std::sync::Barrier;
use std::time::{Duration, Instant};

/// Thread counts each scaling group runs at.
pub const THREADS: [usize; 5] = [1, 2, 4, 8, 16];

/// The slowest thread's time for `iters` calls of `op`, on each of `threads`
/// threads released together, each with its own `setup` state.
///
/// Criterion divides this by `iters`, so a flat line across thread counts
/// means the shared state costs nothing per extra thread.
pub fn concurrent<S>(
    threads: usize,
    iters: u64,
    setup: impl Fn() -> S + Sync,
    op: impl Fn(&mut S) + Sync,
) -> Duration {
    let start = Barrier::new(threads);
    std::thread::scope(|scope| {
        let runs: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut state = setup();
                    start.wait();
                    let began = Instant::now();
                    for _ in 0..iters {
                        op(&mut state);
                    }
                    began.elapsed()
                })
            })
            .collect();
        runs.into_iter()
            .map(|run| run.join().expect("a bench thread panicked"))
            .max()
            .unwrap_or_default()
    })
}
