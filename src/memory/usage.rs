// Project:   scalo
// File:      src/memory/usage.rs
// Purpose:   Where the memory guard reads this process's memory usage from
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Process memory usage, read from what the kernel charges.
//!
//! The OOM killer acts on the cgroup's `memory.current`, so that is what the
//! guard reads by default. An allocator statistic is not equivalent: it is
//! blind to everything the allocator did not hand out (thread stacks, mmap'd
//! buffers, pages jemalloc retains after a free), and it is unavailable at all
//! unless the application installs a tracking `#[global_allocator]` and
//! registers it with [`set_heap_source`](super::set_heap_source).
//!
//! Four sources, tried in that order of fidelity to the kill decision:
//!
//! - **cgroup v2** `memory.current` -- the number `memory.max` is compared to.
//! - **cgroup v1** `memory.usage_in_bytes` -- the same signal on an older host.
//! - **`/proc/self/status`** `VmRSS` -- this process alone, for a Linux host
//!   running no cgroup memory controller.
//! - **reservations** -- the guard's own outstanding leases, which is all that
//!   is left where the kernel exposes no accounting (non-Linux).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// cgroup v2 mount root, which inside a container namespace is the container's
/// own cgroup. Same root the limit reader uses.
const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// cgroup v1 memory-controller root.
const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";

/// procfs directory for the calling process.
const PROC_SELF: &str = "/proc/self";

/// How long one usage reading is reused before the file is read again.
///
/// The guard is sampled per payload on the receive path, so an uncached read
/// would put a syscall in front of every request. The kernel charges memory in
/// per-CPU batches, so a reading is approximate below this interval anyway.
const CACHE_INTERVAL: Duration = Duration::from_millis(50);

/// Where a [`MemoryGuard`](super::MemoryGuard) reads this process's memory
/// usage from. Resolved once per guard by [`UsageSource::detect`]; pass one
/// explicitly to
/// [`MemoryGuard::with_usage_source`](super::MemoryGuard::with_usage_source)
/// to pin it (a test fixture directory, or a host where detection is wrong).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UsageSource {
    /// cgroup v2 `memory.current` under this root.
    CgroupV2(PathBuf),
    /// cgroup v1 `memory.usage_in_bytes` under this root.
    CgroupV1(PathBuf),
    /// `VmRSS` from the `status` file under this procfs directory.
    ProcStatus(PathBuf),
    /// The guard's own outstanding reservations -- sees only what callers
    /// reserved and released by hand, never the process's real usage.
    Reservations,
}

impl UsageSource {
    /// Resolve the source, preferring the number the OOM killer acts on.
    #[must_use]
    pub fn detect() -> Self {
        Self::detect_in(
            Path::new(CGROUP_V2_ROOT),
            Path::new(CGROUP_V1_MEMORY_ROOT),
            Path::new(PROC_SELF),
        )
    }

    fn detect_in(v2_root: &Path, v1_root: &Path, proc_self: &Path) -> Self {
        if read_u64_file(&v2_root.join("memory.current")).is_some() {
            return Self::CgroupV2(v2_root.to_path_buf());
        }
        if read_u64_file(&v1_root.join("memory.usage_in_bytes")).is_some() {
            return Self::CgroupV1(v1_root.to_path_buf());
        }
        if read_vm_rss(proc_self).is_some() {
            return Self::ProcStatus(proc_self.to_path_buf());
        }
        Self::Reservations
    }

    /// Short name for the init log line.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::CgroupV2(_) => "cgroup-v2",
            Self::CgroupV1(_) => "cgroup-v1",
            Self::ProcStatus(_) => "proc-status",
            Self::Reservations => "reservations",
        }
    }

    /// Current usage in bytes, or `None` for [`Self::Reservations`] and for a
    /// file that has become unreadable since detection.
    #[must_use]
    pub fn read(&self) -> Option<u64> {
        match self {
            Self::CgroupV2(root) => read_u64_file(&root.join("memory.current")),
            Self::CgroupV1(root) => read_u64_file(&root.join("memory.usage_in_bytes")),
            Self::ProcStatus(root) => read_vm_rss(root),
            Self::Reservations => None,
        }
    }
}

fn read_u64_file(path: &Path) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// Parse `VmRSS:    193456 kB` out of a procfs `status` file.
///
/// The kernel reports this field in kB whatever the page size, so there is no
/// page-size constant to get wrong on a 16K or 64K kernel.
fn read_vm_rss(proc_self: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(proc_self.join("status")).ok()?;
    let line = content.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kb.checked_mul(1024)
}

/// A [`UsageSource`] plus the short-interval cache that keeps its file read off
/// the per-payload path.
pub(crate) struct UsageReader {
    source: UsageSource,
    started: Instant,
    cached_bytes: AtomicU64,
    cached_at_nanos: AtomicU64,
    /// False until the first successful read, so a cold reader never serves 0.
    primed: AtomicBool,
}

impl UsageReader {
    pub(crate) fn new(source: UsageSource) -> Self {
        let reader = Self {
            source,
            started: Instant::now(),
            cached_bytes: AtomicU64::new(0),
            cached_at_nanos: AtomicU64::new(0),
            primed: AtomicBool::new(false),
        };
        reader.sample();
        reader
    }

    pub(crate) fn source(&self) -> &UsageSource {
        &self.source
    }

    /// Current usage, re-read at most once per [`CACHE_INTERVAL`].
    pub(crate) fn read(&self) -> Option<u64> {
        if !self.primed.load(Ordering::Relaxed) {
            return self.sample();
        }
        let age = self
            .elapsed_nanos()
            .saturating_sub(self.cached_at_nanos.load(Ordering::Relaxed));
        if Duration::from_nanos(age) < CACHE_INTERVAL {
            return Some(self.cached_bytes.load(Ordering::Relaxed));
        }
        self.sample()
    }

    fn sample(&self) -> Option<u64> {
        match self.source.read() {
            Some(bytes) => {
                self.cached_bytes.store(bytes, Ordering::Relaxed);
                self.cached_at_nanos
                    .store(self.elapsed_nanos(), Ordering::Relaxed);
                self.primed.store(true, Ordering::Relaxed);
                Some(bytes)
            }
            // A read that fails after the source worked keeps serving the last
            // good figure, rather than disarming the guard onto a counter that
            // cannot see the process's real usage.
            None if self.primed.load(Ordering::Relaxed) => {
                Some(self.cached_bytes.load(Ordering::Relaxed))
            }
            None => None,
        }
    }

    fn elapsed_nanos(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write `name`->`contents` files into a fresh temp dir and return it.
    /// Real files on disk -- the readers do real `fs::read_to_string`.
    fn fixture(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for (name, contents) in files {
            std::fs::write(dir.path().join(name), contents).expect("write fixture");
        }
        dir
    }

    #[test]
    fn detect_prefers_cgroup_v2_over_v1_over_proc_status() {
        let v2 = fixture(&[("memory.current", "198299648\n")]);
        let v1 = fixture(&[("memory.usage_in_bytes", "123\n")]);
        let proc_self = fixture(&[("status", "VmRSS:\t  1024 kB\n")]);

        assert_eq!(
            UsageSource::detect_in(v2.path(), v1.path(), proc_self.path()),
            UsageSource::CgroupV2(v2.path().to_path_buf())
        );

        let no_v2 = fixture(&[]);
        assert_eq!(
            UsageSource::detect_in(no_v2.path(), v1.path(), proc_self.path()),
            UsageSource::CgroupV1(v1.path().to_path_buf())
        );

        let no_v1 = fixture(&[]);
        assert_eq!(
            UsageSource::detect_in(no_v2.path(), no_v1.path(), proc_self.path()),
            UsageSource::ProcStatus(proc_self.path().to_path_buf())
        );

        let no_proc = fixture(&[]);
        assert_eq!(
            UsageSource::detect_in(no_v2.path(), no_v1.path(), no_proc.path()),
            UsageSource::Reservations,
            "no kernel accounting readable -> reservations only"
        );
    }

    #[test]
    fn cgroup_sources_read_their_own_file() {
        let v2 = fixture(&[("memory.current", "198299648\n")]);
        assert_eq!(
            UsageSource::CgroupV2(v2.path().to_path_buf()).read(),
            Some(198_299_648)
        );

        let v1 = fixture(&[("memory.usage_in_bytes", "4096\n")]);
        assert_eq!(
            UsageSource::CgroupV1(v1.path().to_path_buf()).read(),
            Some(4096)
        );

        assert_eq!(UsageSource::Reservations.read(), None);
    }

    #[test]
    fn vm_rss_is_parsed_as_kilobytes() {
        let proc_self = fixture(&[(
            "status",
            "Name:\tscalo\nVmPeak:\t  999999 kB\nVmRSS:\t  193456 kB\nThreads:\t8\n",
        )]);
        assert_eq!(
            UsageSource::ProcStatus(proc_self.path().to_path_buf()).read(),
            Some(193_456 * 1024)
        );
    }

    #[test]
    fn proc_status_returns_this_process_resident_size() {
        // The real procfs, not a fixture: the fallback must report the running
        // process, so allocate a known block and watch the figure cover it.
        let source = UsageSource::ProcStatus(PathBuf::from(PROC_SELF));
        let Some(before) = source.read() else {
            return; // not Linux; nothing to assert
        };
        assert!(
            before > 512 * 1024,
            "a running Rust test process holds more than 512 KiB, got {before}"
        );

        let block = vec![7u8; 64 * 1024 * 1024];
        // Touch every page so the kernel actually charges it as resident.
        assert_eq!(
            block.iter().map(|b| u64::from(*b)).sum::<u64>(),
            7 * 64 * 1024 * 1024
        );
        let after = source.read().expect("procfs still readable");
        assert!(
            after >= before + 32 * 1024 * 1024,
            "64 MiB of touched pages must show in VmRSS: {before} -> {after}"
        );
        drop(block);
    }

    #[test]
    fn reader_reuses_a_reading_within_the_cache_interval() {
        let dir = fixture(&[("memory.current", "1000\n")]);
        let reader = UsageReader::new(UsageSource::CgroupV2(dir.path().to_path_buf()));
        assert_eq!(reader.read(), Some(1000));

        std::fs::write(dir.path().join("memory.current"), "2000\n").expect("rewrite");
        assert_eq!(
            reader.read(),
            Some(1000),
            "a fresh reading is served from the cache, not re-read"
        );

        std::thread::sleep(CACHE_INTERVAL + Duration::from_millis(10));
        assert_eq!(reader.read(), Some(2000), "past the interval it re-reads");
    }

    #[test]
    fn reader_keeps_the_last_good_reading_when_the_file_goes_away() {
        let dir = fixture(&[("memory.current", "1000\n")]);
        let reader = UsageReader::new(UsageSource::CgroupV2(dir.path().to_path_buf()));
        assert_eq!(reader.read(), Some(1000));

        std::fs::remove_file(dir.path().join("memory.current")).expect("remove");
        std::thread::sleep(CACHE_INTERVAL + Duration::from_millis(10));
        assert_eq!(
            reader.read(),
            Some(1000),
            "an unreadable file must not disarm the guard"
        );
    }

    #[test]
    fn reader_on_reservations_reads_nothing() {
        let reader = UsageReader::new(UsageSource::Reservations);
        assert_eq!(reader.read(), None);
        assert_eq!(reader.source().name(), "reservations");
    }
}
