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
    /// How long one reading is reused: [`CACHE_INTERVAL`] outside tests.
    interval: Duration,
    started: Instant,
    cached_bytes: AtomicU64,
    cached_at_nanos: AtomicU64,
    /// False until the first successful read, so a cold reader never serves 0.
    primed: AtomicBool,
    /// Net bytes admitted since the cached sample, floored at zero: the kernel
    /// figure is up to [`CACHE_INTERVAL`] stale, so a burst inside one window
    /// would otherwise be admitted against a reading that predates it.
    admitted_since_sample: AtomicU64,
    /// File reads taken, so a test can count them.
    #[cfg(test)]
    samples: std::sync::atomic::AtomicUsize,
}

impl UsageReader {
    pub(crate) fn new(source: UsageSource) -> Self {
        Self::with_interval(source, CACHE_INTERVAL)
    }

    /// A reader that reuses each reading for `interval`, so a test decides
    /// when the next sample is taken rather than the host's scheduler.
    pub(crate) fn with_interval(source: UsageSource, interval: Duration) -> Self {
        let reader = Self {
            source,
            interval,
            started: Instant::now(),
            cached_bytes: AtomicU64::new(0),
            cached_at_nanos: AtomicU64::new(0),
            primed: AtomicBool::new(false),
            admitted_since_sample: AtomicU64::new(0),
            #[cfg(test)]
            samples: std::sync::atomic::AtomicUsize::new(0),
        };
        reader.sample();
        reader
    }

    pub(crate) fn source(&self) -> &UsageSource {
        &self.source
    }

    /// Current usage, re-read at most once per interval.
    ///
    /// When the cached reading goes stale, the one thread whose compare-exchange
    /// moves the timestamp on reads the file and clears the ledger. Every other
    /// thread serves the cached reading for the rest of that window, so a burst
    /// crossing the boundary costs one file read and one ledger clear, not one
    /// each.
    pub(crate) fn read(&self) -> Option<u64> {
        if !self.primed.load(Ordering::Acquire) {
            return self.sample();
        }
        let cached_at = self.cached_at_nanos.load(Ordering::Relaxed);
        let now = self.elapsed_nanos();
        if Duration::from_nanos(now.saturating_sub(cached_at)) < self.interval {
            return Some(self.cached_bytes.load(Ordering::Relaxed));
        }
        let elected = self
            .cached_at_nanos
            .compare_exchange(cached_at, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();
        if elected {
            return self.sample();
        }
        Some(self.cached_bytes.load(Ordering::Relaxed))
    }

    /// Kernel sample plus what has been admitted against it since.
    ///
    /// A reader that sees the reader primed also sees the reading that primed
    /// it (`Release` on the flag, `Acquire` on its loads). The sample and the
    /// ledger are still two atomics, not one pair: a reader landing between a
    /// sampler's ledger clear and its new reading takes the previous reading
    /// against an empty ledger, an under-count that lasts until the reading
    /// lands. Electing one sampler per window keeps that to one gap a window.
    pub(crate) fn estimate(&self) -> Option<u64> {
        let sampled = self.read()?;
        Some(sampled.saturating_add(self.admitted_since_sample.load(Ordering::Relaxed)))
    }

    /// Charge an admission to the ledger, until the next sample sees it.
    pub(crate) fn admit(&self, bytes: u64) {
        self.admitted_since_sample
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Charge an admission and return the ledger total that charge produced,
    /// saturating, so a caller judging the limit judges its own charge in.
    pub(crate) fn admit_and_get(&self, bytes: u64) -> u64 {
        let previous = self
            .admitted_since_sample
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            })
            // Always succeeds (closure always returns Some).
            .unwrap_or_else(|v| v);
        previous.saturating_add(bytes)
    }

    /// Discharge released bytes from the ledger, saturating at zero.
    pub(crate) fn forget(&self, bytes: u64) {
        let _ = self.admitted_since_sample.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(bytes)),
        );
    }

    fn sample(&self) -> Option<u64> {
        #[cfg(test)]
        self.samples.fetch_add(1, Ordering::Relaxed);
        // Cleared before the file read, not after: an admission landing between
        // the two is then double-counted rather than dropped, and over-counting
        // brakes early where an under-count is the overshoot that OOM-kills.
        self.admitted_since_sample.store(0, Ordering::Relaxed);
        match self.source.read() {
            Some(bytes) => {
                self.cached_bytes.store(bytes, Ordering::Relaxed);
                self.cached_at_nanos
                    .store(self.elapsed_nanos(), Ordering::Relaxed);
                // Release: a reader that sees the flag set sees the reading above.
                self.primed.store(true, Ordering::Release);
                Some(bytes)
            }
            // A read that fails after the source worked keeps serving the last
            // good figure, rather than disarming the guard onto a counter that
            // cannot see the process's real usage.
            None if self.primed.load(Ordering::Acquire) => {
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

    /// A burst of readers crossing the interval boundary together elects one
    /// sampler: one file read and one ledger clear for the window, not one each.
    #[test]
    fn a_burst_across_the_boundary_reads_the_file_once() {
        let dir = fixture(&[("memory.current", "1000\n")]);
        let reader = UsageReader::new(UsageSource::CgroupV2(dir.path().to_path_buf()));
        assert_eq!(
            reader.samples.load(Ordering::Relaxed),
            1,
            "the priming read"
        );

        std::fs::write(dir.path().join("memory.current"), "2000\n").expect("rewrite");
        std::thread::sleep(CACHE_INTERVAL + Duration::from_millis(10));

        let threads = 16;
        let barrier = std::sync::Barrier::new(threads);
        let started = Instant::now();
        let readings: Vec<Option<u64>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        reader.read()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        // A burst a loaded host stretches past one window may open a second.
        let windows = 1 + started.elapsed().as_nanos() / CACHE_INTERVAL.as_nanos();

        let burst_reads = reader.samples.load(Ordering::Relaxed) - 1;
        assert!(
            burst_reads >= 1 && burst_reads as u128 <= windows,
            "{burst_reads} file reads across {windows} window(s), from {threads} readers: \
             {readings:?}"
        );
        assert!(
            readings.iter().all(|r| matches!(r, Some(1000 | 2000))),
            "every reader served a real reading: {readings:?}"
        );
        assert_eq!(reader.read(), Some(2000), "the elected read landed");
    }

    #[test]
    fn admit_and_get_returns_the_total_its_charge_made() {
        let reader = UsageReader::new(UsageSource::Reservations);
        assert_eq!(reader.admit_and_get(10), 10);
        assert_eq!(reader.admit_and_get(5), 15);
        reader.forget(15);
        assert_eq!(reader.admit_and_get(u64::MAX), u64::MAX, "saturates");
    }

    #[test]
    fn reader_on_reservations_reads_nothing() {
        let reader = UsageReader::new(UsageSource::Reservations);
        assert_eq!(reader.read(), None);
        assert_eq!(reader.source().name(), "reservations");
    }
}
