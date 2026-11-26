// Project:   scalo
// File:      src/deployment/memory_profile.rs
// Purpose:   The memory block of the deployment contract, as bench-memory measures it
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The memory a service holds under the standard load.
//!
//! `<app> bench-memory` runs the release binary's own pipeline under a fixed
//! synthetic load, samples its memory over a fixed window and writes a
//! [`MemoryProfile`]. `<app> generate-artefacts --memory-profile <file>` puts
//! it in the contract as [`DeploymentContract::memory`](super::DeploymentContract::memory),
//! which is the only way figures get there.

use serde::{Deserialize, Serialize};

/// Memory a service held over one `bench-memory` run, and the load it ran under.
///
/// A chart can take its memory request from `avg_bytes` and its limit from
/// `max_bytes` plus headroom, below the contract's `resources` and the
/// deployment's values. The contract's `validate()` rejects a profile with a
/// zero figure, or whose min, average and max bytes do not rise in that order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct MemoryProfile {
    /// Lowest sample over the window, in bytes.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub min_bytes: u64,
    /// Mean of the samples, in bytes.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub avg_bytes: u64,
    /// Highest sample, in bytes.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub max_bytes: u64,
    /// The load offered while the samples were taken.
    pub load: BenchLoad,
    /// Seconds the load ran before the first sample.
    pub warmup_seconds: u32,
    /// Seconds the samples span.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub window_seconds: u32,
    /// How many samples the figures come from.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub samples: u32,
    /// What each sample read.
    pub reading: MemoryReading,
}

/// The synthetic load `bench-memory` offers the pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct BenchLoad {
    /// JSON records a second offered to the pipeline's source, across all
    /// producers.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub records_per_second: u64,
    /// Bytes in each record.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub record_bytes: u64,
    /// Producers feeding the source at once, each offering an even share of
    /// the rate.
    #[cfg_attr(feature = "config-schema", schemars(range(min = 1)))]
    pub sources: u32,
}

/// What a memory sample reads, the source the memory guard reads by default.
///
/// Serialised in kebab case: `cgroup-v2`, `proc-status` and so on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum MemoryReading {
    /// The cgroup v2 working set, `memory.current` less `inactive_file`, which
    /// is what kubelet evicts on. It covers every process in the cgroup.
    CgroupV2,
    /// The cgroup v1 working set, `memory.usage_in_bytes` less
    /// `total_inactive_file`.
    CgroupV1,
    /// This process's `VmRSS` from `/proc/self/status`, on a Linux host with
    /// no cgroup memory controller of its own.
    ProcStatus,
    /// This process's working set on Windows.
    WorkingSet,
    /// This process's resident size on macOS.
    TaskInfo,
}

impl MemoryProfile {
    /// The field at fault and why, for a profile no bench run writes.
    pub(crate) fn fault(&self) -> Option<(&'static str, String)> {
        let zero = [
            ("min_bytes", self.min_bytes),
            ("window_seconds", u64::from(self.window_seconds)),
            ("samples", u64::from(self.samples)),
            ("load.records_per_second", self.load.records_per_second),
            ("load.record_bytes", self.load.record_bytes),
            ("load.sources", u64::from(self.load.sources)),
        ]
        .into_iter()
        .find(|(_, value)| *value == 0);
        if let Some((field, _)) = zero {
            return Some((
                field,
                "is 0, and a bench run never writes 0 there".to_string(),
            ));
        }
        if self.avg_bytes < self.min_bytes || self.max_bytes < self.avg_bytes {
            return Some((
                "avg_bytes",
                format!(
                    "min {}, avg {} and max {} must rise in that order",
                    self.min_bytes, self.avg_bytes, self.max_bytes
                ),
            ));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON a bench writes reads back into the same profile, with the
    /// reading in kebab case.
    #[test]
    fn a_profile_reads_back_as_written() {
        let profile = MemoryProfile {
            min_bytes: 40 << 20,
            avg_bytes: 48 << 20,
            max_bytes: 64 << 20,
            load: BenchLoad {
                records_per_second: 5000,
                record_bytes: 1024,
                sources: 1,
            },
            warmup_seconds: 30,
            window_seconds: 120,
            samples: 120,
            reading: MemoryReading::CgroupV2,
        };
        let json = serde_json::to_value(&profile).unwrap();
        assert_eq!(json["reading"], "cgroup-v2");
        assert_eq!(json["load"]["records_per_second"], 5000);
        let back: MemoryProfile = serde_json::from_value(json).unwrap();
        assert_eq!(back, profile);
        for (reading, name) in [
            (MemoryReading::CgroupV1, "cgroup-v1"),
            (MemoryReading::ProcStatus, "proc-status"),
            (MemoryReading::WorkingSet, "working-set"),
            (MemoryReading::TaskInfo, "task-info"),
        ] {
            assert_eq!(serde_json::to_value(reading).unwrap(), name);
        }
        assert_eq!(profile.fault(), None);
    }
}
