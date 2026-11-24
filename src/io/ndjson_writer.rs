// Project:   scalo
// File:      src/io/ndjson_writer.rs
// Purpose:   Core NDJSON file writer with rotation and metrics
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Core NDJSON file writer with automatic rotation.
//!
//! Writes `&[u8]` lines to a file rotated by time, by size, or both. This
//! writer knows nothing about DLQ or output semantics -- callers serialise
//! their own types and hand raw bytes to the writer.
//!
//! ## Two APIs
//!
//! - [`NdjsonWriter`] -- synchronous. Acquires a `parking_lot::Mutex` and
//!   writes the file directly. Cheap (~us) but blocks the calling thread.
//!   Safe to call from non-async code and tests.
//! - [`AsyncNdjsonWriter`] -- async wrapper over `Arc<NdjsonWriter>`. Each
//!   call runs the sync work on a `tokio::task::spawn_blocking` thread,
//!   so the tokio runtime is never stalled. Use this from `async fn`
//!   bodies.
//!
//! ## Thread Safety
//!
//! Both wrappers are `Send + Sync`. `NdjsonWriter` holds its file under a
//! `parking_lot::Mutex` so multiple callers can share one writer instance.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use super::config::FileWriterConfig;
use super::rotating::{self, LiveFile, OpenMode, Trigger};

/// Shortest wait between reopen attempts after a failed write.
const REOPEN_BACKOFF_MIN: Duration = Duration::from_millis(250);

/// Longest wait between reopen attempts while writes keep failing.
const REOPEN_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// NDJSON file writer with automatic rotation and metrics.
///
/// Each line written is expected to be a complete JSON object (NDJSON format).
/// The writer rotates by time and size, compresses rotated files, and removes
/// them by age and count. Each write lands whole: a failed one is cut back off
/// the file, and opening a file whose last line has no newline, as a crash
/// mid-write leaves it, cuts that line off before appending.
///
/// After a failed write the writer reopens its target on a later write, with
/// backoff, so a deleted file or a restored directory stops refusing writes
/// before the next rotation. It never recreates a missing directory.
///
/// A write is refused while the path holds no file, or a file other than the
/// one the writer has open, so no write lands in a deleted or renamed file.
/// External rotation of the current file is unsupported: the writer rotates
/// it itself. If something else renames it and puts a new file at the path
/// (logrotate `create`, a second writer), every write until the writer reopens
/// the path, 250 ms or more after the first, returns `Err` and counts as failed.
pub struct NdjsonWriter {
    target: Mutex<Target>,
    config: FileWriterConfig,
    label: String,
    output_path: PathBuf,
    file_path: PathBuf,
    lines_written: AtomicU64,
    write_errors: AtomicU64,
    cut_bytes: AtomicU64,
}

/// The open file plus the schedules that follow a failed write or rotation.
struct Target {
    /// `None` after an open failed, until a reopen succeeds.
    file: Option<LiveFile>,
    reopen: Retry,
    /// After a failed rotation, writes go on into the live file and rotation
    /// waits for this.
    rotate: Retry,
}

/// When a failed step may run again, backing off while it keeps failing.
struct Retry {
    /// `None` while the step is succeeding.
    at: Option<Instant>,
    backoff: Duration,
}

impl Retry {
    const fn new() -> Self {
        Self {
            at: None,
            backoff: REOPEN_BACKOFF_MIN,
        }
    }

    fn record_success(&mut self) {
        self.at = None;
        self.backoff = REOPEN_BACKOFF_MIN;
    }

    fn record_failure(&mut self, now: Instant) {
        if self.at.is_none() {
            self.at = Some(now + self.backoff);
        }
    }

    fn due(&self, now: Instant) -> bool {
        self.at.is_some_and(|at| now >= at)
    }

    fn schedule_next(&mut self, now: Instant) {
        self.backoff = (self.backoff * 2).min(REOPEN_BACKOFF_MAX);
        self.at = Some(now + self.backoff);
    }
}

/// Refuse a directory that is missing, not a directory or cannot be listed.
fn ensure_dir_usable(dir: &Path) -> Result<(), std::io::Error> {
    if !std::fs::metadata(dir)?.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!("{} is not a directory", dir.display()),
        ));
    }
    std::fs::read_dir(dir).map(drop)
}

/// Whether two stats name one file.
#[cfg(unix)]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}

/// Whether two stats name one file.
#[cfg(not(unix))]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    a.created().ok() == b.created().ok()
}

impl std::fmt::Debug for NdjsonWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NdjsonWriter")
            .field("label", &self.label)
            .field("output_path", &self.output_path)
            .field("lines_written", &self.lines_written.load(Ordering::Relaxed))
            .field("write_errors", &self.write_errors.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl NdjsonWriter {
    /// Create a new NDJSON writer.
    ///
    /// Creates `{config.path}/{subdir}/`, each missing directory with
    /// `dir_mode`, and writes to `{filename}` within it. An empty `subdir`
    /// writes in `config.path` itself. The file is opened here, cutting a
    /// partial last line, and a failure to open it is retried at the next
    /// write.
    ///
    /// # Arguments
    ///
    /// * `config` -- Shared file writer settings (path, rotation, compression)
    /// * `subdir` -- Subdirectory under `config.path` (e.g. service name)
    /// * `filename` -- Output filename (e.g. "dlq.ndjson", "events.ndjson")
    /// * `label` -- Human label for log messages (e.g. "dlq", "output")
    ///
    /// # Errors
    ///
    /// Returns `std::io::Error` if the output directory cannot be created
    /// or listed, `filename` names no file (`..`, say), or `compress_rotated`
    /// names a codec rotated files do not take.
    pub fn new(
        config: &FileWriterConfig,
        subdir: &str,
        filename: &str,
        label: &str,
    ) -> Result<Self, std::io::Error> {
        Self::open(config, subdir, filename, label, OpenMode::Append)
    }

    /// As [`Self::new`], dropping whatever the file holds.
    ///
    /// # Errors
    ///
    /// As [`Self::new`].
    pub fn truncating(
        config: &FileWriterConfig,
        subdir: &str,
        filename: &str,
        label: &str,
    ) -> Result<Self, std::io::Error> {
        Self::open(config, subdir, filename, label, OpenMode::Truncate)
    }

    fn open(
        config: &FileWriterConfig,
        subdir: &str,
        filename: &str,
        label: &str,
        mode: OpenMode,
    ) -> Result<Self, std::io::Error> {
        super::ROTATED_ACCEPTS
            .check(config.compress_rotated)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let dir = if subdir.is_empty() {
            config.path.clone()
        } else {
            config.path.join(subdir)
        };
        let file_path = dir.join(filename);
        if filename.is_empty() || file_path.file_name().is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{label} writer filename {filename:?} names no file"),
            ));
        }
        rotating::create_dir(&dir, config.write.dir_mode)?;
        ensure_dir_usable(&dir)?;

        let writer = Self {
            target: Mutex::new(Target {
                file: None,
                reopen: Retry::new(),
                rotate: Retry::new(),
            }),
            config: config.clone(),
            label: label.to_string(),
            output_path: dir,
            file_path,
            lines_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            cut_bytes: AtomicU64::new(0),
        };
        {
            let mut target = writer.target.lock();
            if let Err(e) = writer.open_live(&mut target, mode) {
                warn!(
                    label,
                    path = %writer.file_path.display(),
                    error = %e,
                    "{label} writer could not open its file; retrying at the next write",
                );
                // Due at once, so the first write tries again.
                target.reopen.at = Some(Instant::now());
            }
        }

        info!(
            label,
            path = %writer.file_path.display(),
            rotation = ?config.rotation,
            max_bytes = ?config.write.max_bytes,
            max_files = ?config.write.max_files,
            durable = config.write.durable,
            "{label} writer opened",
        );
        Ok(writer)
    }

    /// Open the live file into `target`, reporting a cut partial line.
    fn open_live(&self, target: &mut Target, mode: OpenMode) -> Result<(), std::io::Error> {
        target.file = None;
        let opened = LiveFile::open(&self.file_path, &self.config, mode)?;
        if opened.cut > 0 {
            self.cut_bytes.fetch_add(opened.cut, Ordering::Relaxed);
            warn!(
                label = %self.label,
                path = %self.file_path.display(),
                cut_bytes = opened.cut,
                "{} writer cut a partial last line left by an interrupted write",
                self.label,
            );
            #[cfg(feature = "metrics")]
            ::metrics::counter!("file_writer_cut_bytes_total", "writer" => self.label.clone())
                .increment(opened.cut);
        }
        target.file = Some(opened.live);
        Ok(())
    }

    /// Replace the file when a reopen is due, dropping the handle the failed
    /// write went to.
    fn reopen_if_due(&self, target: &mut Target) {
        let now = Instant::now();
        if !target.reopen.due(now) {
            return;
        }
        target.reopen.schedule_next(now);
        // A missing directory may be an unmounted volume. Recreating it would
        // write the DLQ onto whatever filesystem sits underneath.
        if let Err(e) = ensure_dir_usable(&self.output_path) {
            warn!(
                label = %self.label,
                path = %self.output_path.display(),
                error = %e,
                "{} writer directory unusable; not reopening",
                self.label,
            );
            target.file = None;
            return;
        }
        match self.open_live(target, OpenMode::Append) {
            Ok(()) => debug!(
                label = %self.label,
                path = %self.file_path.display(),
                "{} writer reopened",
                self.label
            ),
            Err(e) => debug!(
                label = %self.label,
                path = %self.file_path.display(),
                error = %e,
                "{} writer reopen failed",
                self.label
            ),
        }
    }

    /// Refuse a write while the path holds no file, or a file other than the
    /// one open, or one another writer changed the length of, so no write
    /// lands in a deleted or renamed file.
    fn ensure_target_present(&self, live: &LiveFile) -> Result<(), std::io::Error> {
        let at_path = match std::fs::metadata(&self.file_path) {
            Ok(meta) if meta.is_file() => meta,
            Ok(_) => {
                return Err(std::io::Error::other(format!(
                    "{} writer target {} is not a regular file",
                    self.label,
                    self.file_path.display()
                )));
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    e.kind(),
                    format!(
                        "{} writer target {} unusable, write refused: {e}",
                        self.label,
                        self.file_path.display()
                    ),
                ));
            }
        };
        // The length catches a replacement the identity misses: Windows gives
        // a file made under a just-deleted name the old one's creation time.
        if same_file(&live.metadata()?, &at_path) && at_path.len() == live.len() {
            return Ok(());
        }
        Err(std::io::Error::other(format!(
            "{} writer target {} was replaced by another file, write refused",
            self.label,
            self.file_path.display()
        )))
    }

    /// Rename the live file away and open a fresh one. A failed rename
    /// reopens the live file to write on into, and holds rotation off with
    /// backoff.
    fn rotate(&self, target: &mut Target, trigger: Trigger) -> Result<(), std::io::Error> {
        let from_len = target.file.as_ref().map_or(0, LiveFile::len);
        target.file = None;
        let rotated = match rotating::rename_live(&self.file_path) {
            Ok(rotated) => rotated,
            Err(e) => {
                let now = Instant::now();
                target.rotate.record_failure(now);
                warn!(
                    label = %self.label,
                    path = %self.file_path.display(),
                    trigger = trigger.as_str(),
                    error = %e,
                    "{} writer could not rotate its file; writing on into it",
                    self.label,
                );
                return self.open_live(target, OpenMode::Append);
            }
        };
        target.rotate.record_success();
        self.open_live(target, OpenMode::Append)?;
        if self.config.write.durable {
            rotating::sync_dir(&self.output_path)?;
        }
        info!(
            label = %self.label,
            path = %self.file_path.display(),
            rotated_to = %rotated.display(),
            trigger = trigger.as_str(),
            bytes = from_len,
            "{} writer rotated its file",
            self.label,
        );
        #[cfg(feature = "metrics")]
        ::metrics::counter!(
            "file_writer_rotations_total",
            "writer" => self.label.clone(),
            "trigger" => trigger.as_str()
        )
        .increment(1);
        rotating::retain(&self.file_path, &self.config, &self.label);
        Ok(())
    }

    /// Write `bytes` whole, rotating first when a trigger is due.
    fn write_locked(&self, target: &mut Target, bytes: &[u8]) -> Result<(), std::io::Error> {
        let not_open = || {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "{} writer has no open file at {}",
                    self.label,
                    self.file_path.display()
                ),
            )
        };
        let live = target.file.as_ref().ok_or_else(not_open)?;
        self.ensure_target_present(live)?;
        let now = SystemTime::now();
        let due = live.rotation_due(now, bytes.len(), &self.config);
        if let Some(trigger) = due {
            let held = target.rotate.at.is_some();
            if !held || target.rotate.due(Instant::now()) {
                if held {
                    target.rotate.schedule_next(Instant::now());
                }
                self.rotate(target, trigger)?;
            }
        }
        let live = target.file.as_mut().ok_or_else(not_open)?;
        live.append(bytes, self.config.write.durable, now)
    }

    /// Write `bytes` and confirm they reached the target.
    fn write_through(&self, bytes: &[u8]) -> Result<(), std::io::Error> {
        let mut target = self.target.lock();
        self.reopen_if_due(&mut target);
        match self.write_locked(&mut target, bytes) {
            Ok(()) => {
                target.reopen.record_success();
                Ok(())
            }
            Err(e) => {
                target.reopen.record_failure(Instant::now());
                self.write_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Write a single line (must include trailing newline or caller appends it).
    ///
    /// The data is written as-is -- caller is responsible for serialisation
    /// and newline termination.
    pub fn write_line(&self, line: &[u8]) -> Result<(), std::io::Error> {
        self.write_through(line)?;
        self.lines_written.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write a pre-serialised buffer containing multiple newline-delimited lines.
    ///
    /// The buffer should already have newlines between entries. The `count`
    /// parameter is used for metrics tracking. The buffer lands whole or not
    /// at all, and with `durable` set it is on disk when this returns `Ok`.
    pub fn write_buf(&self, buf: &[u8], count: u64) -> Result<(), std::io::Error> {
        self.write_through(buf)?;
        self.lines_written.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    /// Sync the open file's data to disk when `durable` is set. Without it a
    /// write is in the kernel page cache once it returns, and this does
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns the underlying `std::io::Error` if the sync fails. The
    /// internal `write_errors` counter is incremented.
    pub fn flush(&self) -> Result<(), std::io::Error> {
        if !self.config.write.durable {
            return Ok(());
        }
        let target = self.target.lock();
        let Some(live) = target.file.as_ref() else {
            return Ok(());
        };
        if let Err(e) = live.sync() {
            self.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
        Ok(())
    }

    /// Number of lines successfully written.
    pub fn lines_written(&self) -> u64 {
        self.lines_written.load(Ordering::Relaxed)
    }

    /// Number of write errors encountered.
    pub fn write_errors(&self) -> u64 {
        self.write_errors.load(Ordering::Relaxed)
    }

    /// Bytes of partial last lines cut off when the file was opened.
    pub fn cut_bytes(&self) -> u64 {
        self.cut_bytes.load(Ordering::Relaxed)
    }

    /// Human label for this writer (e.g. "dlq", "output").
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Output directory path.
    pub fn output_path(&self) -> &PathBuf {
        &self.output_path
    }

    /// The live file's path.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}

/// Async wrapper around [`NdjsonWriter`] that runs the sync rotate-and-write
/// on `tokio::task::spawn_blocking` to keep the tokio runtime unblocked.
///
/// Use this from `async fn` bodies. For sync code paths, call
/// [`NdjsonWriter`] directly.
///
/// Holds an `Arc<NdjsonWriter>` so multiple async tasks share one writer.
#[derive(Debug, Clone)]
pub struct AsyncNdjsonWriter {
    inner: Arc<NdjsonWriter>,
}

impl AsyncNdjsonWriter {
    /// Wrap an `NdjsonWriter`. Use this when no other task needs the
    /// underlying writer.
    #[must_use]
    pub fn new(writer: NdjsonWriter) -> Self {
        Self {
            inner: Arc::new(writer),
        }
    }

    /// Wrap a shared `Arc<NdjsonWriter>`. Use this when sync code paths
    /// also need access to the same writer.
    #[must_use]
    pub fn from_arc(writer: Arc<NdjsonWriter>) -> Self {
        Self { inner: writer }
    }

    /// Write a single line off-runtime. The closure runs on a blocking
    /// thread. The tokio runtime is free to schedule other tasks.
    ///
    /// # Errors
    ///
    /// Returns the underlying `std::io::Error` from the sync writer, or
    /// an `io::Error::other(JoinError)` if the blocking thread panicked.
    pub async fn write_line(&self, line: Vec<u8>) -> Result<(), std::io::Error> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.write_line(&line))
            .await
            .map_err(std::io::Error::other)?
    }

    /// Write a pre-coalesced buffer of `count` lines off-runtime.
    ///
    /// # Errors
    ///
    /// As [`Self::write_line`].
    pub async fn write_buf(&self, buf: Vec<u8>, count: u64) -> Result<(), std::io::Error> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.write_buf(&buf, count))
            .await
            .map_err(std::io::Error::other)?
    }

    /// Sync the open file to disk off-runtime. See [`NdjsonWriter::flush`].
    ///
    /// # Errors
    ///
    /// As [`Self::write_line`].
    pub async fn flush(&self) -> Result<(), std::io::Error> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || inner.flush())
            .await
            .map_err(std::io::Error::other)?
    }

    /// Number of lines successfully written.
    #[must_use]
    pub fn lines_written(&self) -> u64 {
        self.inner.lines_written()
    }

    /// Number of write errors.
    #[must_use]
    pub fn write_errors(&self) -> u64 {
        self.inner.write_errors()
    }

    /// Human label.
    #[must_use]
    pub fn label(&self) -> &str {
        self.inner.label()
    }

    /// Output directory path.
    #[must_use]
    pub fn output_path(&self) -> &Path {
        self.inner.output_path().as_path()
    }

    /// Shared `Arc<NdjsonWriter>` for code paths that need sync access.
    #[must_use]
    pub fn shared(&self) -> Arc<NdjsonWriter> {
        Arc::clone(&self.inner)
    }
}

#[cfg(test)]
#[path = "ndjson_writer_tests.rs"]
mod tests;
