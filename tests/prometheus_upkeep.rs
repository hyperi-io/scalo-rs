// Project:   scalo
// File:      tests/prometheus_upkeep.rs
// Purpose:   Prove the metrics listener frees recorded histogram samples with no scrape
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service nobody scrapes holds no histogram samples.
//!
//! The Prometheus recorder keeps every histogram sample in a bucket until a
//! render or an upkeep drains it into the series. An idle service with no
//! scraper renders nothing, so without an upkeep the samples pile up for the
//! life of the process. This records a million samples, waits one upkeep
//! interval with no scrape, and reads the heap back.
//!
//! This test owns the global allocator, which is why it is its own binary.
#![cfg(feature = "metrics")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::Duration;

use scalo::metrics::{MetricsConfig, MetricsManager};

/// Samples recorded: about 8 MB of `f64` in the recorder's buckets.
const SAMPLES: usize = 1_000_000;

/// One upkeep interval on the listener, plus margin for a loaded runner.
const UPKEEP_WAIT: Duration = Duration::from_secs(7);

/// Bytes the heap may sit above its baseline once the samples are drained.
const TOLERANCE: isize = 2 * 1024 * 1024;

static LIVE: AtomicIsize = AtomicIsize::new(0);

/// The system allocator, tracking bytes live on the heap.
struct LiveBytes;

// SAFETY: every call forwards to the system allocator with the layout and
// pointer it was given, so the contract is the system allocator's own. The
// counter is an atomic and allocates nothing, so it cannot re-enter.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for LiveBytes {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(size(layout), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(size(layout), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            LIVE.fetch_add(
                isize::try_from(new_size).unwrap_or(isize::MAX) - size(layout),
                Ordering::Relaxed,
            );
        }
        new
    }
}

fn size(layout: Layout) -> isize {
    isize::try_from(layout.size()).unwrap_or(isize::MAX)
}

#[global_allocator]
static ALLOCATOR: LiveBytes = LiveBytes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_samples_are_freed_within_one_upkeep_interval() {
    let mut manager = MetricsManager::with_config(MetricsConfig::offline("upkeep"));
    manager
        .start_server("127.0.0.1:0")
        .await
        .expect("the listener starts");
    let histogram = metrics::histogram!("upkeep_probe_seconds");
    histogram.record(0.0);
    tokio::time::sleep(UPKEEP_WAIT).await;

    let baseline = LIVE.load(Ordering::Relaxed);
    for i in 0..SAMPLES {
        #[allow(clippy::cast_precision_loss)]
        histogram.record(i as f64);
    }
    let recorded = LIVE.load(Ordering::Relaxed);
    assert!(
        recorded - baseline > 4 * 1024 * 1024,
        "the samples were held at all: {} bytes over baseline",
        recorded - baseline
    );

    tokio::time::sleep(UPKEEP_WAIT).await;
    let drained = LIVE.load(Ordering::Relaxed);
    assert!(
        drained - baseline < TOLERANCE,
        "{} bytes still held a full upkeep interval after recording, {} at the peak",
        drained - baseline,
        recorded - baseline
    );
}
