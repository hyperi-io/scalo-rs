// Project:   scalo
// File:      src/memory/mod.rs
// Purpose:   Memory management and OOM prevention
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Memory management and OOM prevention.
//!
//! Provides cgroup-aware memory tracking with backpressure signals
//! for Kubernetes-deployed services. Prevents OOM-kills by applying
//! backpressure before hitting the container memory limit.
//!
//! Both halves of the ratio come from the kernel: [`UsageSource`] reads what
//! the cgroup is charged, [`cgroup`] reads what it is allowed. Neither depends
//! on which allocator the binary installed.
//!
//! # Architecture
//!
//! ```text
//! Layer 1 (opt-in): Cap allocator -- hard limit, last-resort crash instead of OOM-kill
//! Layer 2 (default): MemoryGuard -- cgroup-aware tracking, backpressure signals
//! ```

pub mod cgroup;
pub mod guard;
pub mod usage;

pub use cgroup::{
    detect_memory_high, detect_memory_limit, detect_memory_pressure, detect_memory_stall,
};
pub use guard::{MemoryGuard, MemoryGuardConfig, MemoryPressure, set_heap_source};
pub use usage::UsageSource;
