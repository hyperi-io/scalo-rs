// Project:   scalo
// File:      src/wasm/config.rs
// Purpose:   Limits for the wasm host runtime
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Limits for the wasm host runtime.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Limits every plugin on one [`WasmEngine`](super::WasmEngine) runs under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct WasmConfig {
    /// Plugin instances alive at once across the process. Size it to workers x
    /// plugins x 2, so a reload can overlap the running copy. Default 256.
    pub max_instances: u32,

    /// Linear memory one memory may grow to, in bytes. Growing past it traps.
    /// Default 128 MiB.
    pub max_memory_bytes: usize,

    /// Elements one table may grow to. Growing past it traps. Default 10,000.
    pub max_table_elements: usize,

    /// Guest stack, in bytes. Default 512 KiB.
    pub max_stack_bytes: usize,

    /// CPU time one call may take, in milliseconds, measured in 1 ms epoch
    /// ticks. Default 1,000.
    pub call_timeout_ms: u64,

    /// Host import calls one guest call may make. Default 100,000.
    pub max_host_calls: u64,

    /// Bytes host imports may hand one guest call. Default 64 MiB.
    pub max_host_bytes: u64,

    /// Calls an instance serves before it is replaced. `0` keeps it until a
    /// trap. Default 100,000.
    pub recycle_after_calls: u64,

    /// Linear memory past which an instance is replaced after its call, in
    /// bytes. `0` never replaces it for size. Default 64 MiB.
    pub recycle_above_memory_bytes: usize,

    /// Traps in a row that mark a plugin unhealthy until it is loaded again.
    /// Default 5.
    pub unhealthy_after_traps: u32,

    /// Largest `.wasm` file accepted, in bytes. Default 64 MiB.
    pub max_module_bytes: usize,

    /// A Sigstore `trusted_root.json` that keyless signatures chain to. Unset
    /// uses the public-good root built into scalo, which moves with scalo
    /// releases. Loading never fetches one.
    pub trusted_root_path: Option<PathBuf>,
}

const MIB: usize = 1024 * 1024;

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_instances: 256,
            max_memory_bytes: 128 * MIB,
            max_table_elements: 10_000,
            max_stack_bytes: 512 * 1024,
            call_timeout_ms: 1_000,
            max_host_calls: 100_000,
            max_host_bytes: 64 * MIB as u64,
            recycle_after_calls: 100_000,
            recycle_above_memory_bytes: 64 * MIB,
            unhealthy_after_traps: 5,
            max_module_bytes: 64 * MIB,
            trusted_root_path: None,
        }
    }
}
