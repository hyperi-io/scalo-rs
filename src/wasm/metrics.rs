// Project:   scalo
// File:      src/wasm/metrics.rs
// Purpose:   Per-plugin call, load and pool metrics for the wasm host
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Per-plugin call, load and pool metrics. Handles are built once per plugin,
//! so a call allocates no label.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram};
use wasmtime::Engine;

/// Every `outcome` a call counts under, in [`CALL_OUTCOMES`] order.
pub(super) const CALL_OUTCOMES: [&str; 9] = [
    "ok",
    "trap",
    "timeout",
    "memory",
    "table",
    "budget",
    "capacity",
    "unhealthy",
    "poisoned",
];

/// One plugin's metric handles.
pub(super) struct PluginMetrics {
    calls: [Counter; CALL_OUTCOMES.len()],
    duration: Histogram,
    instantiations: Counter,
    memory: Gauge,
    peak_memory: AtomicU64,
}

impl PluginMetrics {
    pub(super) fn new(plugin: &str, digest: &str, signer: &str) -> Self {
        ::metrics::gauge!(
            "wasm_plugin_info",
            "plugin" => plugin.to_string(),
            "digest" => digest.to_string(),
            "signer" => signer.to_string()
        )
        .set(1.0);
        Self {
            calls: CALL_OUTCOMES.map(|outcome| {
                ::metrics::counter!(
                    "wasm_plugin_calls_total",
                    "plugin" => plugin.to_string(),
                    "outcome" => outcome
                )
            }),
            duration: ::metrics::histogram!(
                "wasm_plugin_call_duration_seconds",
                "plugin" => plugin.to_string()
            ),
            instantiations: ::metrics::counter!(
                "wasm_plugin_instantiations_total",
                "plugin" => plugin.to_string()
            ),
            memory: ::metrics::gauge!(
                "wasm_plugin_memory_bytes",
                "plugin" => plugin.to_string()
            ),
            peak_memory: AtomicU64::new(0),
        }
    }

    pub(super) fn call(&self, outcome: &str, elapsed: Duration) {
        if let Some(index) = CALL_OUTCOMES.iter().position(|o| *o == outcome) {
            self.calls[index].increment(1);
        }
        self.duration.record(elapsed.as_secs_f64());
    }

    pub(super) fn instantiated(&self) {
        self.instantiations.increment(1);
    }

    /// Raise the memory high-water mark to `bytes` if it is higher.
    pub(super) fn memory(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        if self.peak_memory.fetch_max(bytes, Ordering::Relaxed) < bytes {
            self.memory.set(bytes as f64);
        }
    }
}

/// Count one load by `plugin` under `outcome`.
pub(super) fn load(plugin: &str, outcome: &'static str) {
    ::metrics::counter!(
        "wasm_plugin_loads_total",
        "plugin" => plugin.to_string(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Record how long `plugin` took to compile.
pub(super) fn compiled(plugin: &str, elapsed: Duration) {
    ::metrics::histogram!(
        "wasm_plugin_compile_duration_seconds",
        "plugin" => plugin.to_string()
    )
    .record(elapsed.as_secs_f64());
}

/// Set the pool gauges from `engine`'s pooling allocator.
pub(super) fn pool(engine: &Engine) {
    let Some(pool) = engine.pooling_allocator_metrics() else {
        return;
    };
    ::metrics::gauge!("wasm_pool_component_instances").set(pool.component_instances() as f64);
    ::metrics::gauge!("wasm_pool_core_instances").set(pool.core_instances() as f64);
    ::metrics::gauge!("wasm_pool_memories").set(pool.memories() as f64);
    ::metrics::gauge!("wasm_pool_tables").set(pool.tables() as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_call_error_outcome_has_a_counter() {
        use super::super::host::LimitHit;
        use super::super::plugin::CallError;
        let errors = [
            CallError::Timeout,
            CallError::Limit(LimitHit::Memory),
            CallError::Limit(LimitHit::Table),
            CallError::Limit(LimitHit::Budget),
            CallError::Trap(wasmtime::Trap::UnreachableCodeReached),
            CallError::Capacity,
            CallError::Unhealthy(5),
            CallError::Poisoned,
        ];
        for error in errors {
            assert!(
                CALL_OUTCOMES.contains(&error.outcome()),
                "{} has no counter",
                error.outcome()
            );
        }
    }
}
