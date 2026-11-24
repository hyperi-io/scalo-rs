// Project:   scalo
// File:      src/spool_codec.rs
// Purpose:   Shared on-disk spool helpers: framing, open, sync, quarantine, full disk, metrics
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared on-disk spool helpers, used by BOTH the standalone [`Spool`] primitive
//! and the `TieredSink` cold path so the integrity + recovery behaviour is
//! defined once.
//!
//! - **CRC framing** ([`crc_prefix`] / [`unframe`]): an optional CRC32C in
//!   front of each record's on-disk bytes. The queue parity-checks only the
//!   LENGTH header, so a torn write or bit-rot in the payload would otherwise
//!   be returned silently. The checksum turns that into a [`CorruptionError`].
//! - **Open** ([`open_queue`]): refuses a queue another open holds, replays a
//!   receive position a crash left unusable, and quarantines a queue that will
//!   not open under [`CorruptionPolicy::Quarantine`].
//! - **Sync** ([`sync_segments`]): fsync of the segments written since the
//!   last sync, so a record that was pushed survives the host losing power.
//! - **Full disk** ([`FullDisk`]): refuses writes while the disk is full and
//!   tries again on a backoff, so a full disk backpressures and never panics.
//! - **Quarantine** ([`quarantine`]): move a corrupt queue's files into a
//!   timestamped subdirectory of the spool path (forensics preserved, never
//!   deleted). The spool path itself is never renamed, so it works when the
//!   path is a mount point.
//! - **Metrics** ([`StoreMetrics`]): what a store takes in, writes and holds.
//!
//! [`Spool`]: crate::spool::Spool

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::backoff::Backoff;
use crate::compression::Codec;
use crate::yaque::{self, QueueState};

use serde::{Deserialize, Serialize};

/// The default bytes a segment holds before the next entry starts a new one.
pub(crate) const DEFAULT_SEGMENT_BYTES: u64 = 16 << 20;

/// The default read-ahead buffer.
pub(crate) const DEFAULT_READ_BUFFER_BYTES: usize = 64 << 10;

/// The default time between timed syncs, in milliseconds.
pub(crate) const DEFAULT_SYNC_INTERVAL_MS: u64 = 500;

/// What to do when a corrupt cache is detected (queue won't open, or a CRC check
/// fails on read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CorruptionPolicy {
    /// Move the corrupt queue's files into a `corrupt-YYYYMMDD-HHMMSS-<nanos>`
    /// subdirectory of the spool path (forensics preserved, never deleted) and
    /// start a fresh empty queue. The default -- a spill cache is opt-in,
    /// transient overflow, so continuing beats crashing on a poisoned cache. A
    /// queue another live process holds is refused, never quarantined.
    #[default]
    Quarantine,
    /// Surface the corruption error and do not auto-recover, for callers that
    /// want to handle it explicitly.
    Fail,
}

/// A record's on-disk bytes failed their CRC32C check (torn write / bit-rot), or
/// the framing was too short to hold the header.
#[derive(Debug, Clone)]
pub struct CorruptionError(pub String);

impl std::fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The bytes written in front of `body` when `crc` is on: its CRC32C, little
/// endian. Empty when off, so the entry is `body` alone.
#[must_use]
pub(crate) fn crc_prefix(crc: bool, body: &[u8]) -> ([u8; 4], usize) {
    if crc {
        (crc32c::crc32c(body).to_le_bytes(), 4)
    } else {
        ([0; 4], 0)
    }
}

/// Verifies and strips the CRC32C [`crc_prefix`] wrote, borrowing the body.
///
/// # Errors
/// [`CorruptionError`] if the entry is shorter than the 4-byte CRC or the
/// checksum does not match -- corruption surfaces as an error, never as
/// silently-wrong bytes handed downstream.
pub(crate) fn unframe(crc: bool, raw: &[u8]) -> Result<&[u8], CorruptionError> {
    if !crc {
        return Ok(raw);
    }
    let Some((header, body)) = raw.split_first_chunk::<4>() else {
        return Err(CorruptionError(
            "record shorter than the 4-byte CRC header".to_string(),
        ));
    };
    let expected = u32::from_le_bytes(*header);
    let actual = crc32c::crc32c(body);
    if expected != actual {
        return Err(CorruptionError(format!(
            "CRC32C mismatch (expected {expected:08x}, computed {actual:08x}) -- torn write or bit-rot"
        )));
    }
    Ok(body)
}

/// Name prefix of the subdirectories [`quarantine`] creates inside a spool path.
const QUARANTINE_PREFIX: &str = "corrupt-";

/// What triggered a quarantine, for the counter and the log line.
#[derive(Debug, Clone, Copy)]
pub(crate) enum QuarantineTrigger {
    /// The queue would not open.
    OpenFailed,
    /// A record failed its CRC check on read. Only the standalone spool recovers on read.
    #[cfg(feature = "spool")]
    CrcMismatch,
    /// A header failed its parity check, so no later entry can be found.
    DamagedHeader,
}

impl QuarantineTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::OpenFailed => "open_failed",
            #[cfg(feature = "spool")]
            Self::CrcMismatch => "crc_mismatch",
            Self::DamagedHeader => "damaged_header",
        }
    }
}

/// Why [`open_queue`] could not open a spool directory.
#[derive(Debug)]
pub(crate) enum OpenError {
    /// A live process holds the queue. A second handle would put two writers
    /// and two readers on one queue.
    Locked(String),
    /// The queue would not open, and was not (or could not be) replaced.
    Failed(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Locked(m) => write!(
                f,
                "spool is locked by a live owner, refusing a second open of the same path: {m}"
            ),
            Self::Failed(m) => f.write_str(m),
        }
    }
}

/// Open the queue at `path` for sending and receiving.
///
/// The directory is created owner-only on unix. A queue another open holds,
/// in this process or another, is refused and never treated as corruption:
/// the OS drops a dead process's locks, so no lock is ever stale. A receive
/// position a crash left unusable is replaced by a replay from the first
/// segment. When the queue still will not open, `policy` decides:
/// [`CorruptionPolicy::Quarantine`] moves its files into a `corrupt-*`
/// subdirectory and opens a fresh queue, [`CorruptionPolicy::Fail`] returns the
/// error. Environmental failures (permission, full or read-only disk) are
/// never quarantined, because moving the queue aside would orphan its records.
///
/// # Errors
/// [`OpenError::Locked`] for a live owner, [`OpenError::Failed`] otherwise.
pub(crate) fn open_queue(
    path: &Path,
    policy: CorruptionPolicy,
    options: yaque::Options,
) -> Result<(yaque::Sender, yaque::Receiver), OpenError> {
    create_spool_dir(path).map_err(|e| {
        OpenError::Failed(format!(
            "cannot create spool directory {}: {e}",
            path.display()
        ))
    })?;
    repair_recv_position(path).map_err(|e| {
        OpenError::Failed(format!(
            "cannot repair the receive position in {}: {e}",
            path.display()
        ))
    })?;
    let open_err = match yaque::channel(path, options) {
        Ok(queue) => return Ok(queue),
        Err(e) => e,
    };
    if open_err.kind() == io::ErrorKind::WouldBlock {
        return Err(OpenError::Locked(open_err.to_string()));
    }
    if is_environmental(open_err.kind()) || policy == CorruptionPolicy::Fail {
        return Err(OpenError::Failed(open_err.to_string()));
    }
    quarantine(path, QuarantineTrigger::OpenFailed, &open_err.to_string())
        .map_err(|e| OpenError::Failed(format!("{open_err}; quarantine failed: {e}")))?;
    yaque::channel(path, options)
        .map_err(|e| OpenError::Failed(format!("{open_err}; fresh open failed: {e}")))
}

/// Creates the spool directory, owner-only on unix, where the records it holds
/// are no other user's to read.
fn create_spool_dir(path: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(path)
}

/// Quarantines the queue in `path` because a header in it failed its parity
/// check, and opens a fresh one. The caller has closed both halves.
#[cfg(feature = "tiered-sink")]
///
/// # Errors
/// [`OpenError::Failed`] when the move or the fresh open fails.
pub(crate) fn quarantine_damaged(
    path: &Path,
    cause: &str,
    options: yaque::Options,
) -> Result<(yaque::Sender, yaque::Receiver), OpenError> {
    quarantine(path, QuarantineTrigger::DamagedHeader, cause)
        .map_err(|e| OpenError::Failed(format!("{cause}; quarantine failed: {e}")))?;
    yaque::channel(path, options)
        .map_err(|e| OpenError::Failed(format!("{cause}; fresh open failed: {e}")))
}

/// The segment numbers in `path`, skipping any `.q` file not named for one,
/// which the queue's own open refuses.
fn segment_numbers(path: &Path) -> io::Result<Vec<u64>> {
    match yaque::segments(path) {
        Err(e) if e.kind() == io::ErrorKind::InvalidData => Ok(Vec::new()),
        other => other,
    }
}

/// Replace a receive position the queue cannot use with the start of the
/// queue's first segment.
///
/// A power loss can leave `recv-metadata` short, naming a segment that is
/// gone, or an offset past the end of the segment, the data it counted never
/// having reached the disk. Replaying from the first segment instead delivers
/// each record at least once: the records of that segment already consumed
/// come again, and none is lost.
fn repair_recv_position(path: &Path) -> io::Result<()> {
    let state = path.join(yaque::RECV_METADATA);
    let saved = match std::fs::read(&state) {
        Ok(saved) => saved,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let segments = segment_numbers(path)?;
    if recv_position_is_usable(path, &saved, &segments) {
        return Ok(());
    }
    match segments.first() {
        Some(&segment) => yaque::write_position(
            &state,
            &path.join(format!("{}.tmp", yaque::RECV_METADATA)),
            QueueState {
                segment,
                position: 0,
            },
            true,
        )?,
        None => std::fs::remove_file(&state)?,
    }
    #[cfg(feature = "tracing")]
    tracing::warn!(
        path = %path.display(),
        "the spool's receive position was torn or lost; replaying from its first segment, \
         so records already consumed may be delivered again"
    );
    Ok(())
}

/// Whether `saved` is a position the queue can resume from: 16 bytes naming
/// a segment that exists, at an offset inside it.
fn recv_position_is_usable(path: &Path, saved: &[u8], segments: &[u64]) -> bool {
    let (Some(segment), Some(offset)) = (saved.get(0..8), saved.get(8..16)) else {
        return false;
    };
    if saved.len() != 16 {
        return false;
    }
    let segment = u64::from_be_bytes(segment.try_into().unwrap_or([0; 8]));
    let offset = u64::from_be_bytes(offset.try_into().unwrap_or([0; 8]));
    segments.binary_search(&segment).is_ok()
        && std::fs::metadata(path.join(format!("{segment}.q"))).is_ok_and(|m| offset <= m.len())
}

/// Flush a directory's entries to stable storage, so a file created or renamed
/// in it survives a power loss. Windows has no directory sync: there the
/// segment's own flush is the last step, and this does nothing.
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// The directories a sync flushes: the queue's own, and on the first sync every
/// directory above it up to the root, because opening creates the whole path
/// and any of those entries can be new.
fn directories_to_sync(path: &Path, ancestors: bool) -> Vec<PathBuf> {
    if !ancestors {
        return vec![path.to_path_buf()];
    }
    let absolute = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    absolute
        .ancestors()
        .filter(|dir| !dir.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .collect()
}

/// Flush what was written since the last sync to stable storage, and return
/// the segment that was open, for the next call to start from.
///
/// `last` is the segment open at the last sync, `None` before the first. The
/// open segment is flushed through its own handle. Segments closed since the
/// last sync are flushed by path, and the queue's directory only when a
/// segment was created since, so a steady run of syncs costs one data flush
/// each. The first sync flushes every segment the directory holds, and every
/// directory above the queue up to the root.
///
/// # Errors
/// Propagates a segment's or directory's sync [`io::Error`]. A directory above
/// the queue's parent that this process may not open is skipped, because it
/// is one this process did not create.
pub(crate) fn sync_segments(
    path: &Path,
    last: Option<u64>,
    open: (u64, Arc<std::fs::File>),
) -> io::Result<u64> {
    let (segment, file) = open;
    let closed: Vec<u64> = match last {
        None => segment_numbers(path)?
            .into_iter()
            .filter(|n| *n < segment)
            .collect(),
        Some(last) => (last..segment).collect(),
    };
    for number in closed {
        // Opened for write, though nothing is written, because Windows cannot
        // flush a handle that cannot write.
        match std::fs::OpenOptions::new()
            .write(true)
            .open(path.join(format!("{number}.q")))
        {
            Ok(closed) => closed.sync_data()?,
            // The receiver consumed and deleted it since.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    file.sync_data()?;
    if last != Some(segment) {
        for (index, dir) in directories_to_sync(path, last.is_none()).iter().enumerate() {
            match sync_directory(dir) {
                Ok(()) => {}
                // The queue's directory and its parent are the ones this process
                // creates entries in, so they must flush. Above them, one it may not
                // open is one it did not create.
                Err(e) if index > 1 && e.kind() == io::ErrorKind::PermissionDenied => {}
                // macOS's sealed system volume refuses the full flush its sync_all asks for.
                #[cfg(target_os = "macos")]
                Err(e)
                    if index > 1
                        && matches!(
                            e.kind(),
                            io::ErrorKind::ReadOnlyFilesystem | io::ErrorKind::Unsupported
                        ) => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(segment)
}

/// Errors that describe the host, not the queue: quarantining would not help.
fn is_environmental(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::StorageFull
            | io::ErrorKind::ReadOnlyFilesystem
            | io::ErrorKind::QuotaExceeded
            | io::ErrorKind::OutOfMemory
    )
}

/// Whether a write failed for want of space: a full disk, a quota, or a file
/// size limit.
pub(crate) fn is_full(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded | io::ErrorKind::FileTooLarge
    )
}

/// The wait before writing again after a write found the disk full. It
/// doubles with each retry that finds the disk still full.
const FULL_DISK_RETRY: Backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));

/// Tracks a store's disk filling up. While the disk is full, writes are
/// refused without touching it until the next retry, which the backoff
/// spaces out. The queue's failed write is already cut back, so nothing torn
/// is left, and the drain frees space by deleting consumed segments.
pub(crate) struct FullDisk {
    store: &'static str,
    failures: u32,
    since: Option<Instant>,
    retry_at: Option<Instant>,
    flag: Arc<AtomicBool>,
}

impl FullDisk {
    pub(crate) fn new(store: &'static str) -> Self {
        Self {
            store,
            failures: 0,
            since: None,
            retry_at: None,
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set while the disk is full, for a health check.
    #[cfg(any(test, feature = "health", feature = "tiered-sink"))]
    pub(crate) fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    /// Whether a write may be tried now.
    pub(crate) fn admits(&self, now: Instant) -> bool {
        self.retry_at.is_none_or(|at| now >= at)
    }

    /// Counts `bytes` refused, without a write.
    pub(crate) fn refused(&self, bytes: usize) {
        count_refused(self.store, bytes);
    }

    /// Records a write that found the disk full.
    pub(crate) fn failed(&mut self, now: Instant, bytes: usize, error: &io::Error) {
        self.failures = self.failures.saturating_add(1);
        self.retry_at = Some(now + FULL_DISK_RETRY.delay(self.failures));
        count_refused(self.store, bytes);
        if self.since.is_none() {
            self.since = Some(now);
            self.flag.store(true, Ordering::Relaxed);
            #[cfg(feature = "tracing")]
            tracing::warn!(
                store = self.store,
                error = %error,
                "the spool's disk is full; refusing writes until space frees, retrying on a backoff"
            );
        }
        #[cfg(not(feature = "tracing"))]
        let _ = error;
    }

    /// Records a write that went through, ending a full-disk wait.
    pub(crate) fn wrote(&mut self, now: Instant) {
        if let Some(since) = self.since.take() {
            let waited = now.saturating_duration_since(since);
            #[cfg(feature = "metrics")]
            ::metrics::counter!("spool_disk_full_wait_milliseconds_total", "store" => self.store)
                .increment(u64::try_from(waited.as_millis()).unwrap_or(u64::MAX));
            #[cfg(feature = "tracing")]
            tracing::info!(
                store = self.store,
                waited_ms = waited.as_millis(),
                "the spool's disk has room again"
            );
            let _ = waited;
            self.flag.store(false, Ordering::Relaxed);
        }
        self.failures = 0;
        self.retry_at = None;
    }
}

fn count_refused(store: &'static str, bytes: usize) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("spool_disk_full_refused_bytes_total", "store" => store)
        .increment(bytes as u64);
    #[cfg(not(feature = "metrics"))]
    let _ = (store, bytes);
}

/// The error a store returns for a write its full disk refused.
pub(crate) fn full_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::StorageFull,
        "the spool's disk is full; retry once the drain frees space",
    )
}

/// What one store reports, registered once so a record costs an atomic add.
pub(crate) struct StoreMetrics {
    #[cfg(feature = "metrics")]
    bytes_in: ::metrics::Counter,
    #[cfg(feature = "metrics")]
    bytes_written: ::metrics::Counter,
    #[cfg(feature = "metrics")]
    compress_micros: ::metrics::Counter,
    #[cfg(feature = "metrics")]
    disk_bytes: ::metrics::Gauge,
    #[cfg(feature = "metrics")]
    reserved_bytes: ::metrics::Gauge,
    #[cfg(feature = "metrics")]
    cut_short: ::metrics::Counter,
}

impl StoreMetrics {
    /// The metrics of `store`, writing under `codec`.
    pub(crate) fn new(store: &'static str, codec: Codec) -> Self {
        #[cfg(not(feature = "metrics"))]
        let _ = (store, codec);
        Self {
            #[cfg(feature = "metrics")]
            bytes_in: ::metrics::counter!("spool_bytes_in_total", "store" => store, "codec" => codec.name()),
            #[cfg(feature = "metrics")]
            bytes_written: ::metrics::counter!("spool_bytes_written_total", "store" => store, "codec" => codec.name()),
            #[cfg(feature = "metrics")]
            compress_micros: ::metrics::counter!("spool_codec_microseconds_total", "store" => store, "codec" => codec.name(), "op" => "compress"),
            #[cfg(feature = "metrics")]
            disk_bytes: ::metrics::gauge!("spool_disk_bytes", "store" => store),
            #[cfg(feature = "metrics")]
            reserved_bytes: ::metrics::gauge!("spool_memory_reserved_bytes", "store" => store),
            #[cfg(feature = "metrics")]
            cut_short: ::metrics::counter!("spool_drain_cut_short_total", "store" => store),
        }
    }

    /// A record of `bytes_in` taken in and `written` bytes appended, after
    /// `compressing` spent in the codec.
    pub(crate) fn wrote(&self, bytes_in: usize, written: usize, compressing: Duration) {
        #[cfg(feature = "metrics")]
        {
            self.bytes_in.increment(bytes_in as u64);
            self.bytes_written.increment(written as u64);
            self.compress_micros
                .increment(u64::try_from(compressing.as_micros()).unwrap_or(u64::MAX));
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (self, bytes_in, written, compressing);
    }

    /// The bytes the store's segment files hold.
    pub(crate) fn disk(&self, bytes: u64) {
        #[cfg(feature = "metrics")]
        self.disk_bytes.set(bytes as f64);
        #[cfg(not(feature = "metrics"))]
        let _ = (self, bytes);
    }

    /// The bytes a read holds reserved with the memory guard, and whether the
    /// guard cut the read short.
    pub(crate) fn reserved(&self, bytes: u64, cut_short: bool) {
        #[cfg(feature = "metrics")]
        {
            self.reserved_bytes.set(bytes as f64);
            if cut_short {
                self.cut_short.increment(1);
            }
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (self, bytes, cut_short);
    }
}

/// The open queue's shared half, swapped when a quarantine opens a fresh one.
pub(crate) type QueueCell = Arc<parking_lot::Mutex<Option<Arc<yaque::Shared>>>>;

/// Syncs the queue in `cell` to stable storage on the blocking pool: the
/// segments written since the last sync, the directory when a segment was
/// created since, and the receiver's committed position. Records how long it
/// took in `spool_sync_seconds`.
///
/// # Errors
/// A sync's or the position save's [`io::Error`], or the blocking task's
/// failure as [`io::ErrorKind::Other`].
pub(crate) async fn sync_queue(
    path: PathBuf,
    store: &'static str,
    cell: QueueCell,
) -> io::Result<()> {
    let Some(shared) = cell.lock().clone() else {
        return Ok(());
    };
    tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let synced = sync_now(&path, &shared);
        #[cfg(feature = "metrics")]
        ::metrics::histogram!("spool_sync_seconds", "store" => store)
            .record(started.elapsed().as_secs_f64());
        let _ = (started, store);
        synced
    })
    .await
    .map_err(io::Error::other)?
}

fn sync_now(path: &Path, shared: &yaque::Shared) -> io::Result<()> {
    let open = shared
        .open_segment()
        .ok_or_else(|| io::Error::other("the spool has no open segment"))?;
    // A segment deleted under the open handle would sync to a file no restart finds.
    std::fs::metadata(path.join(format!("{}.q", open.0)))?;
    let segment = sync_segments(path, shared.synced(), open)?;
    shared.mark_synced(segment);
    shared.save_position()
}

/// Syncs a queue every interval, and once more at shutdown, so a power loss
/// loses at most about one interval of writes.
pub(crate) struct SyncTicker {
    pub(crate) path: PathBuf,
    pub(crate) store: &'static str,
    pub(crate) cell: QueueCell,
}

impl crate::concurrency::PeriodicTask for SyncTicker {
    async fn tick(&mut self) -> Result<(), crate::concurrency::TickError> {
        sync_queue(self.path.clone(), self.store, Arc::clone(&self.cell))
            .await
            .map_err(|e| crate::concurrency::TickError::Generic(Box::new(e)))
    }

    async fn shutdown(&mut self) -> Result<(), crate::concurrency::TickError> {
        self.tick().await
    }
}

/// Starts the timed sync, unless `every` is zero.
pub(crate) fn start_sync_ticker(
    ticker: SyncTicker,
    every: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> Option<crate::concurrency::PeriodicWorker> {
    (!every.is_zero()).then(|| crate::concurrency::PeriodicWorker::spawn(ticker, every, shutdown))
}

/// Move every file of the queue at `path` into a new `corrupt-*` subdirectory.
///
/// Earlier quarantine subdirectories stay where they are. Returns the new
/// subdirectory, or `None` when there was nothing to move. Counted as
/// `spool_quarantined_total{trigger}` and logged once with the path.
///
/// # Errors
/// Propagates the subdirectory create or an entry move's [`io::Error`].
pub(crate) fn quarantine(
    path: &Path,
    trigger: QuarantineTrigger,
    cause: &str,
) -> io::Result<Option<PathBuf>> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut to_move = Vec::new();
    for entry in entries {
        let entry = entry?;
        // Earlier quarantines are directories. A file with the prefix is part of
        // the queue like any other.
        let earlier_quarantine = entry.file_type()?.is_dir()
            && entry
                .file_name()
                .to_string_lossy()
                .starts_with(QUARANTINE_PREFIX);
        if !earlier_quarantine {
            to_move.push(entry.path());
        }
    }
    if to_move.is_empty() {
        return Ok(None);
    }
    let dest = create_quarantine_dir(path)?;
    for src in to_move {
        if let Some(name) = src.file_name() {
            std::fs::rename(&src, dest.join(name))?;
        }
    }
    #[cfg(feature = "metrics")]
    ::metrics::counter!("spool_quarantined_total", "trigger" => trigger.label()).increment(1);
    #[cfg(feature = "tracing")]
    tracing::warn!(
        path = %path.display(),
        quarantined = %dest.display(),
        trigger = trigger.label(),
        cause,
        "spool could not be used; moved its files aside and started a fresh queue"
    );
    let _ = (trigger.label(), cause);
    Ok(Some(dest))
}

/// The local-time `YYYYMMDD-HHMMSS-<nanos>` in a quarantine directory's name.
fn quarantine_stamp(now: &jiff::ZonedDateTime) -> String {
    now.strftime("%Y%m%d-%H%M%S-%9f").to_string()
}

/// Create a uniquely named quarantine subdirectory inside `path`.
fn create_quarantine_dir(path: &Path) -> io::Result<PathBuf> {
    let stamp = quarantine_stamp(&jiff::ZonedDateTime::now());
    let mut last = io::Error::from(io::ErrorKind::AlreadyExists);
    for n in 0..100u32 {
        let dest = path.join(format!("{QUARANTINE_PREFIX}{stamp}-{n}"));
        match std::fs::create_dir(&dest) {
            Ok(()) => return Ok(dest),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => last = e,
            Err(e) => return Err(e),
        }
    }
    Err(last)
}

/// Whether an error is the queue reporting a damaged header, which no read
/// can get past.
pub(crate) fn is_damaged_header(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidData
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_framing_round_trips_and_catches_damage() {
        let body = b"the payload bytes";
        let (prefix, len) = crc_prefix(true, body);
        let framed = [&prefix[..len], &body[..]].concat();
        assert_eq!(framed.len(), body.len() + 4, "4-byte CRC header prepended");
        assert_eq!(unframe(true, &framed).unwrap(), body);
        assert_eq!(crc_prefix(false, body).1, 0, "no CRC, no prefix");
        assert_eq!(unframe(false, body).unwrap(), body);
        let mut damaged = framed;
        let last = damaged.len() - 1;
        damaged[last] ^= 0xFF;
        assert!(unframe(true, &damaged).is_err());
        assert!(unframe(true, &[1, 2]).is_err(), "too short for the header");
    }

    #[test]
    fn quarantine_moves_contents_and_keeps_earlier_quarantines() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("spool");
        std::fs::create_dir_all(spool.join("version")).unwrap();
        std::fs::write(spool.join("0.q"), b"seg").unwrap();
        std::fs::write(spool.join("recv-metadata"), b"meta").unwrap();

        let first = quarantine(&spool, QuarantineTrigger::OpenFailed, "test")
            .unwrap()
            .expect("files moved");
        assert!(first.starts_with(&spool), "inside the spool path");
        let name = first.file_name().unwrap().to_str().unwrap();
        let shape: Vec<usize> = name
            .trim_start_matches(QUARANTINE_PREFIX)
            .split('-')
            .map(str::len)
            .collect();
        assert_eq!(shape, [8, 6, 9, 1], "{name}");
        assert!(first.join("0.q").exists());
        assert!(first.join("version").is_dir());
        assert!(!spool.join("0.q").exists());

        std::fs::write(spool.join("1.q"), b"seg").unwrap();
        let second = quarantine(&spool, QuarantineTrigger::DamagedHeader, "test")
            .unwrap()
            .expect("files moved");
        assert_ne!(first, second);
        assert!(
            first.join("0.q").exists(),
            "the earlier quarantine stays put"
        );
        assert!(second.join("1.q").exists());
        assert!(!second.join(first.file_name().unwrap()).exists());

        assert_eq!(
            quarantine(&spool, QuarantineTrigger::OpenFailed, "test").unwrap(),
            None,
            "nothing left to move"
        );
    }

    #[test]
    fn quarantine_stamp_is_the_civil_time_with_nine_fraction_digits() {
        for (instant, stamp) in [
            (
                "2026-03-07T04:05:06.000000789+00:00[UTC]",
                "20260307-040506-000000789",
            ),
            (
                "2026-12-31T23:59:59.5+10:00[+10:00]",
                "20261231-235959-500000000",
            ),
            (
                "2026-01-01T00:00:00-05:00[-05:00]",
                "20260101-000000-000000000",
            ),
        ] {
            let now: jiff::ZonedDateTime = instant.parse().unwrap();
            assert_eq!(quarantine_stamp(&now), stamp, "{instant}");
        }
    }

    fn options() -> yaque::Options {
        yaque::Options {
            segment_bytes: 16,
            read_buffer_bytes: 4096,
            recycle_bytes: 1 << 16,
            whole: None,
            store: "test",
        }
    }

    fn open(sender: &yaque::Sender) -> (u64, Arc<std::fs::File>) {
        sender.shared().open_segment().expect("an open segment")
    }

    /// A sync flushes the open segment through its handle, the segments
    /// closed since by path, and passes over one the receiver deleted.
    #[test]
    fn a_sync_covers_what_was_written_since_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue");
        let (mut sender, _receiver) = open_queue(&path, CorruptionPolicy::Fail, options()).unwrap();
        for _ in 0..3 {
            sender.send(&[], &[0; 20]).unwrap();
        }
        let first = sync_segments(&path, None, open(&sender)).unwrap();
        assert_eq!(first, 2, "the open segment");
        std::fs::remove_file(path.join("1.q")).unwrap();
        let again = sync_segments(&path, Some(1), open(&sender)).unwrap();
        assert_eq!(again, 2);
        assert_eq!(sync_segments(&path, Some(2), open(&sender)).unwrap(), 2);
    }

    /// The first sync flushes each directory up to the root, and later syncs
    /// only the queue's.
    #[test]
    fn the_first_sync_flushes_every_directory_above_the_queue() {
        let root = tempfile::tempdir().unwrap();
        let queue = root
            .path()
            .join("var")
            .join("lib")
            .join("service")
            .join("spool");
        std::fs::create_dir_all(&queue).unwrap();

        let first = directories_to_sync(&queue, false);
        assert_eq!(
            first,
            std::slice::from_ref(&queue),
            "later syncs flush the queue's directory"
        );

        let all = directories_to_sync(&queue, true);
        let real = std::fs::canonicalize(&queue).unwrap();
        assert_eq!(all.first(), Some(&real));
        for level in ["service", "lib", "var"] {
            assert!(
                all.iter().any(|dir| dir.ends_with(level)),
                "{level} is in {all:?}"
            );
        }
        assert_eq!(all.last().map(PathBuf::as_path), Some(Path::new("/")));
    }

    #[cfg(unix)]
    #[test]
    fn the_spool_directory_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh").join("spool");
        drop(open_queue(&path, CorruptionPolicy::Fail, options()).unwrap());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{mode:o}");
    }

    /// A full-disk wait refuses writes until its backoff runs out, and a write
    /// that goes through ends it.
    #[test]
    fn a_full_disk_backs_off_and_recovers() {
        let mut full = FullDisk::new("test");
        let now = Instant::now();
        assert!(full.admits(now));
        full.failed(now, 10, &full_error());
        assert!(full.flag().load(Ordering::Relaxed), "health flag set");
        assert!(!full.admits(now), "refused inside the backoff");
        assert!(
            full.admits(now + Duration::from_secs(6)),
            "retried after it"
        );
        full.wrote(now + Duration::from_secs(6));
        assert!(!full.flag().load(Ordering::Relaxed));
        assert!(full.admits(now));
        for kind in [
            io::ErrorKind::StorageFull,
            io::ErrorKind::QuotaExceeded,
            io::ErrorKind::FileTooLarge,
        ] {
            assert!(is_full(&kind.into()), "{kind:?}");
        }
        assert!(!is_full(&io::ErrorKind::PermissionDenied.into()));
    }

    #[test]
    fn a_permission_error_is_not_quarantined() {
        assert!(is_environmental(io::ErrorKind::PermissionDenied));
        assert!(is_environmental(io::ErrorKind::StorageFull));
        assert!(!is_environmental(io::ErrorKind::UnexpectedEof));
        assert!(!is_environmental(io::ErrorKind::InvalidData));
    }
}
