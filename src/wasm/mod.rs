// Project:   scalo
// File:      src/wasm/mod.rs
// Purpose:   The wasmtime host runtime for WebAssembly component plugins
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The wasmtime host runtime for WebAssembly component plugins.
//!
//! One [`WasmEngine`] per process compiles plugins and pools their
//! instances. A [`World`] wraps the bindings `bindgen!` generates for a WIT
//! world, [`WasmEngine::load`] checks and links a `.wasm` for it, and each
//! worker calls through its own [`Instance`].
//!
//! Every call runs under a CPU deadline, memory and table caps, a stack cap
//! and an import budget, and a cap hit traps the guest rather than the host.
//! A trap replaces the instance before its next call. A guest can't reach
//! files, sockets, the environment or secrets.
//!
//! wasmtime's own types are part of this API, re-exported as
//! [`wasmtime`](crate::wasm::wasmtime), so a world's crate builds against the
//! same copy. scalo holds wasmtime to its 48 LTS line.

mod config;
mod engine;
mod host;
mod plugin;

#[cfg(all(test, feature = "wasm-test-support"))]
mod tests;

pub use config::WasmConfig;
pub use engine::{EngineError, LoadError, PluginSource, TrustPolicy, WasmEngine};
pub use host::{HostBudget, HostCtx, LimitHit};
pub use plugin::{CallError, Instance, Plugin, World};

pub use wasmtime;
