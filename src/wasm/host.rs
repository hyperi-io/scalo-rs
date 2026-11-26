// Project:   scalo
// File:      src/wasm/host.rs
// Purpose:   Per-store host state: limits, import budget, WASI context
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The data every plugin store holds beside the world's own.

use std::sync::Arc;

use wasmtime::ResourceLimiter;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use super::guest_log::{GuestLog, Stream};

use super::config::WasmConfig;

/// A cap a guest ran into. Carried as the error of the trap it raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LimitHit {
    /// Linear memory grew past `max_memory_bytes`.
    #[error("linear memory cap")]
    Memory,
    /// A table grew past `max_table_elements`.
    #[error("table cap")]
    Table,
    /// Host imports were called more often, or handed more bytes, than one
    /// call's budget allows.
    #[error("host import budget")]
    Budget,
}

/// What host imports may do for one guest call. A world's import charges it
/// before doing the work, and the guest traps once it is spent.
#[derive(Debug)]
pub struct HostBudget {
    max_calls: u64,
    max_bytes: u64,
    calls: u64,
    bytes: u64,
}

impl HostBudget {
    fn new(config: &WasmConfig) -> Self {
        Self {
            max_calls: config.max_host_calls,
            max_bytes: config.max_host_bytes,
            calls: 0,
            bytes: 0,
        }
    }

    /// Charge one import call that hands the guest `bytes`. An `Err` returned
    /// from the import traps the guest with [`LimitHit::Budget`].
    pub fn charge(&mut self, bytes: usize) -> wasmtime::Result<()> {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.calls = self.calls.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        if self.calls > self.max_calls || self.bytes > self.max_bytes {
            return Err(LimitHit::Budget.into());
        }
        Ok(())
    }

    pub(super) fn reset(&mut self) {
        self.calls = 0;
        self.bytes = 0;
    }
}

/// Memory and table caps that raise [`LimitHit`], so a call can say which one.
#[derive(Debug)]
struct Limiter {
    max_memory_bytes: usize,
    max_table_elements: usize,
    peak_memory_bytes: usize,
}

impl ResourceLimiter for Limiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_memory_bytes {
            return Err(LimitHit::Memory.into());
        }
        self.peak_memory_bytes = self.peak_memory_bytes.max(desired);
        Ok(true)
    }

    // The pool's own cap refuses a grow without asking the limiter.
    fn memory_grow_failed(&mut self, _error: wasmtime::Error) -> wasmtime::Result<()> {
        Err(LimitHit::Memory.into())
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_table_elements {
            return Err(LimitHit::Table.into());
        }
        Ok(true)
    }

    fn table_grow_failed(&mut self, _error: wasmtime::Error) -> wasmtime::Result<()> {
        Err(LimitHit::Table.into())
    }
}

/// The data a plugin's [`Store`](wasmtime::Store) holds: the world's `D`, the
/// caps, the import budget and a WASI context that grants nothing.
///
/// A guest sees no files, sockets, environment, arguments or stdin. Its
/// stdout and stderr become log lines with a `plugin` field, cut at 1 KiB and
/// capped at 100 a second. Secrets never enter it: an import resolves a
/// credential name host-side.
pub struct HostCtx<D> {
    data: D,
    plugin: Arc<str>,
    budget: HostBudget,
    limiter: Limiter,
    wasi: WasiCtx,
    table: ResourceTable,
}

impl<D> HostCtx<D> {
    pub(super) fn new(data: D, plugin: Arc<str>, config: &WasmConfig) -> Self {
        let wasi = WasiCtx::builder()
            .stdout(GuestLog::new(Arc::clone(&plugin), Stream::Stdout))
            .stderr(GuestLog::new(Arc::clone(&plugin), Stream::Stderr))
            .build();
        Self {
            data,
            plugin,
            budget: HostBudget::new(config),
            limiter: Limiter {
                max_memory_bytes: config.max_memory_bytes,
                max_table_elements: config.max_table_elements,
                peak_memory_bytes: 0,
            },
            wasi,
            table: ResourceTable::new(),
        }
    }

    /// The world's own data.
    pub fn data(&self) -> &D {
        &self.data
    }

    /// The world's own data, mutably.
    pub fn data_mut(&mut self) -> &mut D {
        &mut self.data
    }

    /// The budget a world's imports charge.
    pub fn budget_mut(&mut self) -> &mut HostBudget {
        &mut self.budget
    }

    /// The plugin's name, for a world's log lines.
    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    /// Resources a world hands the guest, shared with WASI's streams.
    pub fn resources_mut(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    /// The most linear memory this store's guest has grown to, in bytes.
    #[must_use]
    pub fn peak_memory_bytes(&self) -> usize {
        self.limiter.peak_memory_bytes
    }

    pub(super) fn limiter_mut(&mut self) -> &mut dyn ResourceLimiter {
        &mut self.limiter
    }

    pub(super) fn into_data(self) -> D {
        self.data
    }
}

impl<D: Send> WasiView for HostCtx<D> {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_refuses_past_either_cap_and_resets() {
        let config = WasmConfig {
            max_host_calls: 2,
            max_host_bytes: 10,
            ..WasmConfig::default()
        };
        let cases: [(&str, &[usize], bool); 4] = [
            ("within both", &[4, 4], true),
            ("one call too many", &[0, 0, 0], false),
            ("one byte too many", &[10, 1], false),
            ("a single call over the bytes", &[11], false),
        ];
        for (case, charges, ok) in cases {
            let mut budget = HostBudget::new(&config);
            let all_ok = charges.iter().all(|&bytes| budget.charge(bytes).is_ok());
            assert_eq!(all_ok, ok, "{case}");
            budget.reset();
            assert!(
                budget.charge(1).is_ok(),
                "{case}: a reset budget takes a call"
            );
        }
    }

    #[test]
    fn a_spent_budget_names_its_limit() {
        let config = WasmConfig {
            max_host_calls: 0,
            ..WasmConfig::default()
        };
        let err = HostBudget::new(&config).charge(0).unwrap_err();
        assert_eq!(err.downcast_ref::<LimitHit>(), Some(&LimitHit::Budget));
    }
}
