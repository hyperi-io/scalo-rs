// Project:   scalo
// File:      src/metrics/process.rs
// Purpose:   Process-level metrics collection
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Process-level metrics collection.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

/// Process metrics collector.
///
/// Emits BARE metric names (e.g. `process_resident_memory_bytes`). Namespacing
/// is applied once by the prefix layer on the global recorder, so process
/// metrics get the same `{namespace}_` prefix as everything else.
#[derive(Debug, Clone)]
pub struct ProcessMetrics {
    system: Arc<std::sync::Mutex<System>>,
    pid: sysinfo::Pid,
    start_time: f64,
}

impl ProcessMetrics {
    /// Create a new process metrics collector.
    ///
    /// The `namespace` argument is accepted for API compatibility but no longer
    /// used to build names -- names are bare and the recorder's prefix layer
    /// adds the namespace.
    #[must_use]
    pub fn new(_namespace: &str) -> Self {
        let pid = sysinfo::Pid::from_u32(std::process::id());
        // Empty: loading every process would hold a /proc file open for each
        // one on the host. `update` refreshes this process alone.
        let system = System::new();

        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());

        // Register metric descriptions (bare names; namespace added by the
        // recorder's prefix layer).
        Self::register_metrics();

        Self {
            system: Arc::new(std::sync::Mutex::new(system)),
            pid,
            start_time,
        }
    }

    /// Register metric descriptions (bare names).
    fn register_metrics() {
        metrics::describe_gauge!(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in seconds"
        );
        metrics::describe_gauge!(
            "process_resident_memory_bytes",
            "Resident memory size in bytes"
        );
        metrics::describe_gauge!(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes"
        );
        metrics::describe_gauge!("process_open_fds", "Number of open file descriptors");
        metrics::describe_gauge!(
            "process_start_time_seconds",
            "Start time of the process since unix epoch in seconds"
        );
    }

    /// Update process metrics.
    pub fn update(&self) {
        // Recover from a poisoned lock rather than panicking: a panic in a
        // prior update must not turn metrics collection into a repeat-panic.
        // Observability degrades, it does not crash.
        let mut system = self.system.lock().unwrap_or_else(|e| e.into_inner());
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );

        if let Some(process) = system.process(self.pid) {
            // sysinfo reports accumulated user + system CPU time in milliseconds.
            let cpu_seconds = process.accumulated_cpu_time() as f64 / 1000.0;
            metrics::gauge!("process_cpu_seconds_total").set(cpu_seconds);

            // Memory
            let rss = process.memory();
            let virtual_mem = process.virtual_memory();
            metrics::gauge!("process_resident_memory_bytes").set(rss as f64);
            metrics::gauge!("process_virtual_memory_bytes").set(virtual_mem as f64);

            // File descriptors (Linux-specific)
            #[cfg(target_os = "linux")]
            {
                if let Ok(fds) = count_open_fds() {
                    metrics::gauge!("process_open_fds").set(fds as f64);
                }
            }

            // Start time
            metrics::gauge!("process_start_time_seconds").set(self.start_time);
        }
    }
}

/// Count open file descriptors (Linux only).
#[cfg(target_os = "linux")]
fn count_open_fds() -> std::io::Result<usize> {
    let fd_dir = format!("/proc/{}/fd", std::process::id());
    std::fs::read_dir(fd_dir).map(|entries| entries.count())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_metrics_new() {
        let pm = ProcessMetrics::new("test");
        assert!(pm.start_time > 0.0);
    }

    #[test]
    fn test_process_metrics_update() {
        let pm = ProcessMetrics::new("test");
        // Should not panic
        pm.update();
    }

    /// The collector tracks this process alone, never the rest of the host.
    #[test]
    fn only_this_process_is_loaded() {
        let pm = ProcessMetrics::new("test");
        pm.update();
        let system = pm.system.lock().unwrap_or_else(|e| e.into_inner());
        let pids: Vec<_> = system.processes().keys().copied().collect();
        assert_eq!(pids, vec![pm.pid], "{pids:?}");
    }

    /// CPU seconds is accumulated time, so it never falls between updates.
    #[test]
    fn cpu_seconds_is_accumulated_time() {
        let pm = ProcessMetrics::new("test");
        pm.update();
        let first = {
            let system = pm.system.lock().unwrap_or_else(|e| e.into_inner());
            system
                .process(pm.pid)
                .map(sysinfo::Process::accumulated_cpu_time)
        };
        let mut spin = 0u64;
        for i in 0..20_000_000u64 {
            spin = spin.wrapping_add(i ^ spin);
        }
        std::hint::black_box(spin);
        pm.update();
        let second = {
            let system = pm.system.lock().unwrap_or_else(|e| e.into_inner());
            system
                .process(pm.pid)
                .map(sysinfo::Process::accumulated_cpu_time)
        };
        assert!(second >= first, "{first:?} then {second:?}");
    }
}
