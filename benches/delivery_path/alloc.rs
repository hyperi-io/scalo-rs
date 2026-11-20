// Project:   scalo
// File:      benches/delivery_path/alloc.rs
// Purpose:   A counting global allocator for --allocs, in the bench binary only
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Counts heap allocations on every thread while [`start`] is in force, so the
//! runtime's workers are counted too. Off, an allocation costs one relaxed
//! load. Lives in the bench binary so the library keeps `deny(unsafe_code)`.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

/// The system allocator, counting while [`start`] is in force.
pub struct Counting;

fn count(bytes: usize) {
    if COUNTING.load(Ordering::Relaxed) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

// SAFETY: every call forwards to `System` with the caller's arguments unchanged.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        // SAFETY: as `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count(new_size);
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Zero the counters and start counting.
pub fn start() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::SeqCst);
}

/// Stop counting and return (allocations, bytes) since [`start`].
pub fn stop() -> (u64, u64) {
    COUNTING.store(false, Ordering::SeqCst);
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}
