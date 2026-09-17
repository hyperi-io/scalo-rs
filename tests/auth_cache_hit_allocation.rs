// Project:   scalo
// File:      tests/auth_cache_hit_allocation.rs
// Purpose:   Prove the credential cache hit allocates nothing per request
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A held credential costs a signed request no allocation.
//!
//! Every outbound request asks the source for the credential, so the hit is on
//! the hot path of every consumer that signs. It is meant to be an `ArcSwap`
//! load, a refcount bump and an instant comparison, and `benches/auth_hit.rs`
//! measures what that costs in time. Time is not the property that matters
//! here: an allocation per request is what would make the auth layer a
//! per-call cost at transport volumes, and a timing benchmark cannot see one.
//!
//! So this counts them. The allocator is armed only around the measured reads,
//! and the second half is the control: the same counter watching an
//! acquisition, which allocates, so a zero in the first half means the read
//! path is clean rather than the counter being deaf.
//!
//! This test owns the global allocator, which is why it is its own binary.
#![cfg(feature = "auth")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use scalo::auth::{Cached, CredentialSource, Static};

/// Reads taken against a warm cache. Enough that a per-read allocation cannot
/// hide, small enough to stay instant.
const READS: usize = 10_000;

static ARMED: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

/// The system allocator, counting while armed.
struct Counting;

// SAFETY: every call forwards to the system allocator with the layout and
// pointer it was given, so the contract is the system allocator's own. The
// counter is two atomics and allocates nothing, so it cannot re-enter.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Count what `body` allocates. The runtime is current-thread and this binary
/// holds one test, so nothing else is running on another thread to be counted.
fn allocations_during(body: impl FnOnce()) -> usize {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    body();
    ARMED.store(false, Ordering::Relaxed);
    ALLOCATIONS.load(Ordering::Relaxed)
}

#[test]
fn a_held_credential_is_read_without_allocating() {
    // Timers on: an acquisition bounds the exchange with `tokio::time::timeout`,
    // which panics on a runtime without them.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    let source = Arc::new(Cached::new(Static::new("cache-hit-token")));

    // Warm the cache unarmed: the measured path is the hit, not the first
    // acquisition.
    runtime
        .block_on(source.credential())
        .expect("a static credential always acquires");

    let hits = allocations_during(|| {
        runtime.block_on(async {
            for _ in 0..READS {
                let credential = source.credential().await.expect("a held credential");
                std::hint::black_box(&credential);
            }
        });
    });

    assert_eq!(
        hits, 0,
        "a cache hit allocated: {hits} allocations across {READS} reads"
    );

    // The control. Drop the credential and read again, which exchanges: that
    // path builds a credential and wraps it in an `Arc`, so it must allocate.
    // A zero here would mean the counter never sees anything and the assertion
    // above proves nothing.
    source.invalidate();
    let acquisition = allocations_during(|| {
        runtime
            .block_on(source.credential())
            .expect("a static credential always acquires");
    });

    assert!(
        acquisition > 0,
        "the counter recorded nothing for an acquisition, so it is not counting"
    );
}
