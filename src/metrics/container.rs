// Project:   scalo
// File:      src/metrics/container.rs
// Purpose:   Container-level metrics from cgroups
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Container metrics from cgroups (v1 and v2).

use std::fs;
use std::path::Path;

/// Container metrics collector.
///
/// Emits BARE metric names (e.g. `container_memory_limit_bytes`). Namespacing
/// is applied once by the prefix layer on the global recorder.
#[derive(Debug, Clone)]
pub struct ContainerMetrics {
    cgroup_version: CgroupVersion,
}

#[derive(Debug, Clone, Copy)]
enum CgroupVersion {
    V1,
    V2,
    Unknown,
}

impl ContainerMetrics {
    /// Create a new container metrics collector.
    ///
    /// The `namespace` argument is accepted for API compatibility but no longer
    /// used to build names -- names are bare and the recorder's prefix layer
    /// adds the namespace.
    #[must_use]
    pub fn new(_namespace: &str) -> Self {
        let cgroup_version = detect_cgroup_version();

        // Register metric descriptions (bare names; namespace added by the
        // recorder's prefix layer).
        Self::register_metrics();

        Self { cgroup_version }
    }

    /// Register metric descriptions (bare names).
    fn register_metrics() {
        metrics::describe_gauge!(
            "container_memory_limit_bytes",
            "Container memory limit in bytes"
        );
        metrics::describe_gauge!(
            "container_memory_usage_bytes",
            "Container memory usage in bytes"
        );
        metrics::describe_gauge!("container_cpu_limit_cores", "Container CPU limit in cores");
    }

    /// Update container metrics.
    pub fn update(&self) {
        // Memory limit
        if let Some(limit) = self.read_memory_limit() {
            metrics::gauge!("container_memory_limit_bytes").set(limit as f64);
        }

        // Memory usage
        if let Some(usage) = self.read_memory_usage() {
            metrics::gauge!("container_memory_usage_bytes").set(usage as f64);
        }

        // CPU limit
        if let Some(cores) = self.read_cpu_limit() {
            metrics::gauge!("container_cpu_limit_cores").set(cores);
        }
    }

    /// Read memory limit from cgroups.
    fn read_memory_limit(&self) -> Option<u64> {
        match self.cgroup_version {
            CgroupVersion::V2 => {
                // cgroup v2: /sys/fs/cgroup/memory.max
                read_cgroup_value("/sys/fs/cgroup/memory.max")
            }
            CgroupVersion::V1 => {
                // cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
                read_cgroup_value("/sys/fs/cgroup/memory/memory.limit_in_bytes")
            }
            CgroupVersion::Unknown => None,
        }
    }

    /// Read memory usage from cgroups.
    fn read_memory_usage(&self) -> Option<u64> {
        match self.cgroup_version {
            CgroupVersion::V2 => {
                // cgroup v2: /sys/fs/cgroup/memory.current
                read_cgroup_value("/sys/fs/cgroup/memory.current")
            }
            CgroupVersion::V1 => {
                // cgroup v1: /sys/fs/cgroup/memory/memory.usage_in_bytes
                read_cgroup_value("/sys/fs/cgroup/memory/memory.usage_in_bytes")
            }
            CgroupVersion::Unknown => None,
        }
    }

    /// Read CPU limit from cgroups (returns cores).
    fn read_cpu_limit(&self) -> Option<f64> {
        match self.cgroup_version {
            CgroupVersion::V2 => {
                // cgroup v2: /sys/fs/cgroup/cpu.max contains "quota period"
                let content = fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
                parse_cpu_max_v2(&content)
            }
            CgroupVersion::V1 => {
                // cgroup v1: quota and period in separate files
                let quota = read_cgroup_value("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?;
                let period = read_cgroup_value("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?;

                if quota == u64::MAX || period == 0 {
                    None
                } else {
                    Some(quota as f64 / period as f64)
                }
            }
            CgroupVersion::Unknown => None,
        }
    }
}

/// Detect which cgroup version is in use.
fn detect_cgroup_version() -> CgroupVersion {
    // cgroup v2 unified hierarchy
    if Path::new("/sys/fs/cgroup/cgroup.controllers").exists() {
        return CgroupVersion::V2;
    }

    // cgroup v1
    if Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes").exists() {
        return CgroupVersion::V1;
    }

    CgroupVersion::Unknown
}

/// Read a numeric value from a cgroup file.
fn read_cgroup_value(path: &str) -> Option<u64> {
    let content = fs::read_to_string(path).ok()?;
    let trimmed = content.trim();

    // Handle "max" (unlimited)
    if trimmed == "max" {
        return Some(u64::MAX);
    }

    trimmed.parse().ok()
}

/// Parse cpu.max format: "quota period" or "max period".
fn parse_cpu_max_v2(content: &str) -> Option<f64> {
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }

    let quota = parts[0];
    let period: u64 = parts[1].parse().ok()?;

    if quota == "max" || period == 0 {
        return None;
    }

    let quota_us: u64 = quota.parse().ok()?;
    Some(quota_us as f64 / period as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_metrics_new() {
        let cm = ContainerMetrics::new("test");
        // Names are bare now; just confirm construction succeeds.
        let _ = cm;
    }

    #[test]
    fn test_parse_cpu_max_v2() {
        assert_eq!(parse_cpu_max_v2("100000 100000"), Some(1.0));
        assert_eq!(parse_cpu_max_v2("50000 100000"), Some(0.5));
        assert_eq!(parse_cpu_max_v2("max 100000"), None);
        assert_eq!(parse_cpu_max_v2("invalid"), None);
    }

    #[test]
    fn test_container_metrics_update() {
        let cm = ContainerMetrics::new("test");
        // Should not panic even if not in a container
        cm.update();
    }
}
