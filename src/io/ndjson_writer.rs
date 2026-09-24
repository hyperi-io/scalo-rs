// Project:   scalo
// File:      src/io/ndjson_writer.rs
// Purpose:   Core NDJSON file writer with rotation and metrics
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Core NDJSON file writer with automatic rotation.
//!
//! Writes `&[u8]` lines to a rotating file using the `file-rotate` crate.
//! This writer knows nothing about DLQ or output semantics -- callers
//! serialise their own types and hand raw bytes to the writer.
//!
//! ## Two APIs
//!
//! - [`NdjsonWriter`] -- synchronous. Acquires a `parking_lot::Mutex` and
//!   calls `std::io::Write::write_all` directly. Cheap (~us) but blocks
//!   the calling thread. Safe to call from non-async code and tests.
//! - [`AsyncNdjsonWriter`] -- async wrapper over `Arc<NdjsonWriter>`. Each
//!   call runs the sync work on a `tokio::task::spawn_blocking` thread,
//!   so the tokio runtime is never stalled. Use this from `async fn`
//!   bodies.
//!
//! ## Thread Safety
//!
//! Both wrappers are `Send + Sync`. `NdjsonWriter` uses
//! `parking_lot::Mutex<FileRotate>` internally so multiple callers can
//! share one writer instance.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use file_rotate::suffix::AppendTimestamp;
use file_rotate::suffix::FileLimit;
use file_rotate::{ContentLimit, FileRotate, compression::Compression};
use parking_lot::Mutex;
use tracing::{debug, warn};

use super::config::{FileWriterConfig, RotationPeriod};

/// Shortest wait between reopen attempts after a failed write.
const REOPEN_BACKOFF_MIN: Duration = Duration::from_millis(250);

/// Longest wait between reopen attempts while writes keep failing.
const REOPEN_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Retention ceiling in days; `file-rotate`'s age check panics on a
/// `DateTime` underflow past about 95 million.
const MAX_AGE_DAYS_CEILING: u32 = 1_000_000;

/// NDJSON file writer with automatic rotation and metrics.
///
/// Each line written is expected to be a complete JSON object (NDJSON format).
/// The writer handles file rotation, optional compression, and age-based cleanup.
///
/// After a failed write the writer reopens its target on a later write, with
/// backoff, so a deleted file or a restored directory stops refusing writes
/// before the next rotation. It never recreates a missing directory.
///
/// A write is refused, before it reaches `file-rotate`, while the current
/// file is missing: a rotation in that state panics inside `file-rotate`,
/// which aborts a service built with `panic = "abort"`.
///
/// A write `file-rotate` reports `Ok` that did not reach the file, because
/// it could not open the file for writing, returns `Err`.
pub struct NdjsonWriter {
    writer: Mutex<RotatingTarget>,
    config: FileWriterConfig,
    label: String,
    output_path: PathBuf,
    /// Current (non-rotated) output file, probed after each write because
    /// `file-rotate` swallows open failures -- its `write()` returns `Ok`
    /// with no file handle, silently dropping the bytes.
    file_path: PathBuf,
    lines_written: AtomicU64,
    write_errors: AtomicU64,
}

/// The open rotating file plus the reopen schedule that follows a failed write.
struct RotatingTarget {
    file: FileRotate<AppendTimestamp>,
    /// When the next reopen may run; `None` while writes are landing.
    reopen_at: Option<Instant>,
    backoff: Duration,
}

impl RotatingTarget {
    fn record_success(&mut self) {
        self.reopen_at = None;
        self.backoff = REOPEN_BACKOFF_MIN;
    }

    fn record_failure(&mut self, now: Instant) {
        if self.reopen_at.is_none() {
            self.reopen_at = Some(now + self.backoff);
        }
    }

    fn reopen_due(&self, now: Instant) -> bool {
        self.reopen_at.is_some_and(|at| now >= at)
    }

    fn schedule_next_reopen(&mut self, now: Instant) {
        self.backoff = (self.backoff * 2).min(REOPEN_BACKOFF_MAX);
        self.reopen_at = Some(now + self.backoff);
    }
}

fn open_rotating(file_path: &Path, config: &FileWriterConfig) -> FileRotate<AppendTimestamp> {
    let content_limit = match config.rotation {
        RotationPeriod::Hourly => ContentLimit::Time(file_rotate::TimeFrequency::Hourly),
        RotationPeriod::Daily => ContentLimit::Time(file_rotate::TimeFrequency::Daily),
    };

    let max_age = chrono::Duration::days(i64::from(config.max_age_days.min(MAX_AGE_DAYS_CEILING)));
    let suffix_scheme = AppendTimestamp::default(FileLimit::Age(max_age));

    let compression = if config.compress_rotated {
        Compression::OnRotate(6)
    } else {
        Compression::None
    };

    FileRotate::new(file_path, suffix_scheme, content_limit, compression, None)
}

/// Refuse a directory `FileRotate::new` would panic on: it unwraps
/// `create_dir_all` and `read_dir` on the directory it writes into.
fn ensure_dir_usable(dir: &Path) -> Result<(), std::io::Error> {
    if !std::fs::metadata(dir)?.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!("{} is not a directory", dir.display()),
        ));
    }
    std::fs::read_dir(dir).map(drop)
}

/// Whether two stats of the target path are one file, so a rotation between them shows.
#[cfg(unix)]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}

/// Whether two stats of the target path are one file, so a rotation between them shows.
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
    /// Creates `{config.path}/{subdir}/` and writes to `{filename}` within it.
    /// Files are rotated according to the config's rotation period.
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
    /// or listed, or `filename` names no file (`..`, say).
    pub fn new(
        config: &FileWriterConfig,
        subdir: &str,
        filename: &str,
        label: &str,
    ) -> Result<Self, std::io::Error> {
        let dir = config.path.join(subdir);
        let file_path = dir.join(filename);
        // `file-rotate` expects a final path component and panics without one.
        if file_path.file_name().is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{label} writer filename {filename:?} names no file"),
            ));
        }
        std::fs::create_dir_all(&dir)?;
        ensure_dir_usable(&dir)?;
        let file = open_rotating(&file_path, config);

        debug!(
            label = label,
            path = %dir.display(),
            rotation = ?config.rotation,
            "{} writer initialised",
            label,
        );

        Ok(Self {
            writer: Mutex::new(RotatingTarget {
                file,
                reopen_at: None,
                backoff: REOPEN_BACKOFF_MIN,
            }),
            config: config.clone(),
            label: label.to_string(),
            output_path: dir,
            file_path,
            lines_written: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        })
    }

    /// Replace the rotating file when a reopen is due, dropping the handle
    /// the failed write went to.
    fn reopen_if_due(&self, target: &mut RotatingTarget) {
        let now = Instant::now();
        if !target.reopen_due(now) {
            return;
        }
        target.schedule_next_reopen(now);
        // A missing directory may be an unmounted volume; recreating it would
        // write the DLQ onto whatever filesystem sits underneath.
        if let Err(e) = ensure_dir_usable(&self.output_path) {
            warn!(
                label = %self.label,
                path = %self.output_path.display(),
                error = %e,
                "{} writer directory unusable; not reopening",
                self.label,
            );
            return;
        }
        target.file = open_rotating(&self.file_path, &self.config);
        debug!(label = %self.label, path = %self.file_path.display(), "{} writer reopened", self.label);
    }

    /// Refuse a write while the current file is missing: `file-rotate`
    /// panics when a rotation finds its directory gone or cannot create the file.
    fn ensure_target_present(&self) -> Result<std::fs::Metadata, std::io::Error> {
        match std::fs::metadata(&self.file_path) {
            Ok(meta) if meta.is_file() => Ok(meta),
            Ok(_) => Err(std::io::Error::other(format!(
                "{} writer target {} is not a regular file",
                self.label,
                self.file_path.display()
            ))),
            Err(e) => Err(std::io::Error::new(
                e.kind(),
                format!(
                    "{} writer target {} unusable, write refused: {e}",
                    self.label,
                    self.file_path.display()
                ),
            )),
        }
    }

    /// Write `bytes` through the rotating file and confirm they reached the target.
    fn write_through(&self, bytes: &[u8]) -> Result<(), std::io::Error> {
        let mut target = self.writer.lock();
        self.reopen_if_due(&mut target);
        let result = self.ensure_target_present().and_then(|before| {
            target.file.write_all(bytes)?;
            target.file.flush()?;
            self.verify_write_landed(&before, bytes.len())
        });
        match result {
            Ok(()) => {
                target.record_success();
                Ok(())
            }
            Err(e) => {
                target.record_failure(Instant::now());
                self.write_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Detect a write `file-rotate` swallowed. Its `write()` reports `Ok`
    /// with the bytes discarded whenever it holds no open file: the target
    /// could not be opened (no write permission, a read-only filesystem), or
    /// a rotation could not create the next one. Rotation happens under the
    /// mutex the caller holds, so a write that landed either grew the file
    /// `before` describes or replaced it with one holding at least `written`
    /// bytes.
    fn verify_write_landed(
        &self,
        before: &std::fs::Metadata,
        written: usize,
    ) -> Result<(), std::io::Error> {
        let after = match std::fs::metadata(&self.file_path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "{} writer target {} missing after write -- write was silently dropped \
                         (directory removed or read-only filesystem?)",
                        self.label,
                        self.file_path.display()
                    ),
                ));
            }
            Err(e) => return Err(e),
        };
        let written = u64::try_from(written).unwrap_or(u64::MAX);
        let landed = if same_file(before, &after) {
            after.len() >= before.len().saturating_add(written)
        } else {
            after.len() >= written
        };
        if landed {
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            format!(
                "{} writer target {} did not take the {written} bytes written -- write was \
                 silently dropped (file not writable?)",
                self.label,
                self.file_path.display()
            ),
        ))
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
    /// parameter is used for metrics tracking.
    pub fn write_buf(&self, buf: &[u8], count: u64) -> Result<(), std::io::Error> {
        self.write_through(buf)?;
        self.lines_written.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    /// Flush the in-memory buffer through the rotating writer.
    ///
    /// `file-rotate` doesn't expose the inner `File`, so this flushes to
    /// the kernel page cache only -- NOT on-disk durability. Power loss
    /// before write-back can still lose data. Strongest the file backend
    /// can express until `file-rotate` gains a sync hook; for real
    /// durability pair with an `acks=all` Kafka backend.
    ///
    /// # Errors
    ///
    /// Returns the underlying `std::io::Error` if the flush fails. The
    /// internal `write_errors` counter is incremented.
    pub fn flush(&self) -> Result<(), std::io::Error> {
        let mut target = self.writer.lock();
        if let Err(e) = target.file.flush() {
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

    /// Human label for this writer (e.g. "dlq", "output").
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Output directory path.
    pub fn output_path(&self) -> &PathBuf {
        &self.output_path
    }
}

/// Async wrapper around [`NdjsonWriter`] that runs the sync rotate-and-write
/// on `tokio::task::spawn_blocking` to keep the tokio runtime unblocked.
///
/// Use this from `async fn` bodies. For sync code paths, call
/// [`NdjsonWriter`] directly.
///
/// Holds an `Arc<NdjsonWriter>` so multiple async tasks can share one
/// writer without cloning the underlying `parking_lot::Mutex<FileRotate>`.
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
    /// thread; the tokio runtime is free to schedule other tasks.
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

    /// Flush buffered bytes through the rotating writer off-runtime.
    ///
    /// See [`NdjsonWriter::flush`] for durability semantics -- currently
    /// flushes to kernel page cache only (the `file-rotate` crate
    /// doesn't expose the inner `File` for `fsync`).
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
mod tests {
    use super::*;

    fn test_config(dir: &std::path::Path) -> FileWriterConfig {
        FileWriterConfig {
            path: dir.to_path_buf(),
            rotation: RotationPeriod::Daily,
            max_age_days: 1,
            compress_rotated: false,
        }
    }

    #[test]
    fn test_write_single_line() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());

        let writer = NdjsonWriter::new(&config, "test-svc", "out.ndjson", "test").expect("create");
        assert_eq!(writer.lines_written(), 0);
        assert_eq!(writer.write_errors(), 0);

        writer.write_line(b"{\"msg\":\"hello\"}\n").expect("write");
        assert_eq!(writer.lines_written(), 1);

        let content =
            std::fs::read_to_string(dir.path().join("test-svc/out.ndjson")).expect("read");
        assert_eq!(content.trim(), r#"{"msg":"hello"}"#);
    }

    #[test]
    fn test_write_multiple_lines() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());

        let writer =
            NdjsonWriter::new(&config, "multi", "events.ndjson", "output").expect("create");

        for i in 0..3 {
            let line = format!("{{\"n\":{i}}}\n");
            writer.write_line(line.as_bytes()).expect("write");
        }
        assert_eq!(writer.lines_written(), 3);

        let content =
            std::fs::read_to_string(dir.path().join("multi/events.ndjson")).expect("read");
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_write_buf_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());

        let writer = NdjsonWriter::new(&config, "batch", "out.ndjson", "test").expect("create");

        let mut buf = Vec::new();
        for i in 0..5 {
            buf.extend_from_slice(format!("{{\"n\":{i}}}\n").as_bytes());
        }
        writer.write_buf(&buf, 5).expect("write batch");
        assert_eq!(writer.lines_written(), 5);

        let content = std::fs::read_to_string(dir.path().join("batch/out.ndjson")).expect("read");
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 5);
    }

    /// Issue #22 (scalo-rs): `file-rotate` reports `Ok` while writing
    /// nothing when its lazy reopen fails; the writer must surface that
    /// as an error instead of pretending the write happened.
    #[test]
    fn test_swallowed_write_surfaces_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let writer = NdjsonWriter::new(&config, "gone", "out.ndjson", "dlq").expect("create");

        // Replace the output directory with a regular file so the lazy
        // reopen fails and file-rotate would otherwise swallow the write.
        std::fs::remove_dir_all(dir.path().join("gone")).expect("remove dir");
        std::fs::write(dir.path().join("gone"), b"not a directory").expect("plant file");

        let err = writer.write_line(b"{\"msg\":\"lost\"}\n").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotADirectory);
        assert_eq!(writer.lines_written(), 0);
        assert_eq!(writer.write_errors(), 1);
    }

    #[test]
    fn test_writer_reopens_once_its_directory_is_restored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let writer = NdjsonWriter::new(&config, "back", "out.ndjson", "dlq").expect("create");
        writer.write_line(b"{\"n\":1}\n").expect("healthy write");

        std::fs::remove_dir_all(dir.path().join("back")).expect("remove dir");
        writer.write_line(b"{\"n\":2}\n").unwrap_err();
        std::fs::create_dir_all(dir.path().join("back")).expect("restore dir");
        std::thread::sleep(REOPEN_BACKOFF_MIN);

        writer
            .write_line(b"{\"n\":3}\n")
            .expect("write after the directory is restored");
        let content = std::fs::read_to_string(dir.path().join("back/out.ndjson")).expect("read");
        assert_eq!(content, "{\"n\":3}\n");
    }

    #[test]
    fn test_writer_reopens_after_its_file_is_deleted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let writer = NdjsonWriter::new(&config, "del", "out.ndjson", "dlq").expect("create");
        writer.write_line(b"{\"n\":1}\n").expect("healthy write");

        std::fs::remove_file(dir.path().join("del/out.ndjson")).expect("delete file");
        writer.write_line(b"{\"n\":2}\n").unwrap_err();
        std::thread::sleep(REOPEN_BACKOFF_MIN);

        writer
            .write_line(b"{\"n\":3}\n")
            .expect("write after the file is deleted");
        let content = std::fs::read_to_string(dir.path().join("del/out.ndjson")).expect("read");
        assert_eq!(content, "{\"n\":3}\n");
    }

    #[test]
    fn test_writer_does_not_recreate_a_missing_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let writer = NdjsonWriter::new(&config, "gone", "out.ndjson", "dlq").expect("create");

        std::fs::remove_dir_all(dir.path().join("gone")).expect("remove dir");
        writer.write_line(b"{\"n\":1}\n").unwrap_err();
        std::thread::sleep(REOPEN_BACKOFF_MIN);
        writer.write_line(b"{\"n\":2}\n").unwrap_err();

        assert!(!dir.path().join("gone").exists());
        assert_eq!(writer.write_errors(), 2);
    }

    /// Restores a directory's mode on drop, so the tempdir is removed after a panic too.
    #[cfg(unix)]
    struct ModeGuard(std::path::PathBuf);

    #[cfg(unix)]
    impl ModeGuard {
        fn set(path: &std::path::Path, mode: u32) -> Self {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
            Self(path.to_path_buf())
        }
    }

    #[cfg(unix)]
    impl Drop for ModeGuard {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    /// A writer whose current file was last written two days ago, so its next
    /// write crosses the daily rotation boundary.
    fn writer_due_to_rotate(config: &FileWriterConfig, subdir: &str) -> NdjsonWriter {
        let dir = config.path.join(subdir);
        std::fs::create_dir_all(&dir).expect("create dir");
        let file = std::fs::File::create(dir.join("out.ndjson")).expect("create file");
        file.set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 86_400))
            .expect("age the file");
        drop(file);
        NdjsonWriter::new(config, subdir, "out.ndjson", "dlq").expect("create")
    }

    #[test]
    fn test_rotation_into_a_directory_replaced_by_a_file_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = writer_due_to_rotate(&test_config(dir.path()), "swap");

        std::fs::remove_dir_all(dir.path().join("swap")).expect("remove dir");
        std::fs::write(dir.path().join("swap"), b"not a directory").expect("plant file");

        writer.write_line(b"{\"n\":1}\n").unwrap_err();
        writer.write_line(b"{\"n\":2}\n").unwrap_err();
        assert_eq!(writer.write_errors(), 2);
        assert_eq!(writer.lines_written(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn test_rotation_into_a_read_only_directory_is_refused_then_recovers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = writer_due_to_rotate(&test_config(dir.path()), "ro");
        let target = dir.path().join("ro");

        std::fs::remove_file(target.join("out.ndjson")).expect("remove file");
        let guard = ModeGuard::set(&target, 0o555);
        writer.write_line(b"{\"n\":1}\n").unwrap_err();

        drop(guard);
        std::thread::sleep(REOPEN_BACKOFF_MIN);
        writer
            .write_line(b"{\"n\":2}\n")
            .expect("write once the directory is writable again");
        let content = std::fs::read_to_string(target.join("out.ndjson")).expect("read");
        assert_eq!(content, "{\"n\":2}\n");
    }

    #[cfg(unix)]
    #[test]
    fn test_rotation_after_the_directory_and_its_parent_went_away_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = writer_due_to_rotate(&test_config(dir.path()), "parent/child");
        let parent = dir.path().join("parent");

        std::fs::remove_dir_all(parent.join("child")).expect("remove dir");
        let _guard = ModeGuard::set(&parent, 0o555);

        writer.write_line(b"{\"n\":1}\n").unwrap_err();
        assert!(!parent.join("child").exists());
    }

    #[test]
    fn test_rotation_does_not_recreate_a_removed_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let writer = writer_due_to_rotate(&test_config(dir.path()), "gone");

        std::fs::remove_dir_all(dir.path().join("gone")).expect("remove dir");

        writer.write_line(b"{\"n\":1}\n").unwrap_err();
        assert!(
            !dir.path().join("gone").exists(),
            "a rotation must not recreate a directory that may be an unmounted volume"
        );
    }

    #[test]
    fn test_rotation_with_an_unbounded_max_age_does_not_panic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = FileWriterConfig {
            max_age_days: u32::MAX,
            ..test_config(dir.path())
        };
        let writer = writer_due_to_rotate(&config, "forever");

        writer.write_line(b"{\"n\":1}\n").expect("rotate and write");
        let content = std::fs::read_to_string(dir.path().join("forever/out.ndjson")).expect("read");
        assert_eq!(content, "{\"n\":1}\n");
    }

    /// True when this process can list a directory it has no read permission on.
    #[cfg(unix)]
    fn reads_past_permissions(dir: &std::path::Path) -> bool {
        let probe = dir.join("probe");
        std::fs::create_dir(&probe).expect("create probe");
        let _guard = ModeGuard::set(&probe, 0o300);
        std::fs::read_dir(&probe).is_ok()
    }

    #[cfg(unix)]
    #[test]
    fn test_writer_on_an_unreadable_directory_is_refused_at_construction() {
        let dir = tempfile::tempdir().expect("tempdir");
        if reads_past_permissions(dir.path()) {
            eprintln!("skipping: this process reads directories regardless of their mode");
            return;
        }
        let target = dir.path().join("wo");
        std::fs::create_dir(&target).expect("create dir");
        let _guard = ModeGuard::set(&target, 0o300);

        let err = NdjsonWriter::new(&test_config(dir.path()), "wo", "out.ndjson", "dlq")
            .expect_err("an unreadable directory is refused");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[cfg(unix)]
    #[test]
    fn test_writer_does_not_reopen_into_an_unreadable_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        if reads_past_permissions(dir.path()) {
            eprintln!("skipping: this process reads directories regardless of their mode");
            return;
        }
        let config = test_config(dir.path());
        let writer = NdjsonWriter::new(&config, "wo", "out.ndjson", "dlq").expect("create");
        let target = dir.path().join("wo");

        std::fs::remove_file(target.join("out.ndjson")).expect("remove file");
        writer.write_line(b"{\"n\":1}\n").unwrap_err();
        let guard = ModeGuard::set(&target, 0o300);
        std::thread::sleep(REOPEN_BACKOFF_MIN);
        writer.write_line(b"{\"n\":2}\n").unwrap_err();

        drop(guard);
        std::thread::sleep(REOPEN_BACKOFF_MIN * 2);
        writer
            .write_line(b"{\"n\":3}\n")
            .expect("write once the directory is readable again");
    }

    /// True when this process can open a file it has no write permission on.
    #[cfg(unix)]
    fn writes_past_permissions(dir: &std::path::Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        let probe = dir.join("probe.ro");
        std::fs::write(&probe, b"").expect("create probe");
        std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o400)).expect("chmod");
        let writable = std::fs::OpenOptions::new()
            .append(true)
            .open(&probe)
            .is_ok();
        std::fs::remove_file(&probe).expect("remove probe");
        writable
    }

    /// `file-rotate` opens the target without telling us it failed, and then
    /// reports every write `Ok` with the bytes discarded.
    #[cfg(unix)]
    #[test]
    fn test_write_to_a_file_it_cannot_open_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        if writes_past_permissions(dir.path()) {
            eprintln!("skipping: this process writes files regardless of their mode");
            return;
        }
        let target = dir.path().join("ro");
        std::fs::create_dir(&target).expect("create dir");
        std::fs::write(target.join("out.ndjson"), b"").expect("create file");
        std::fs::set_permissions(
            target.join("out.ndjson"),
            std::fs::Permissions::from_mode(0o400),
        )
        .expect("chmod file");
        let writer =
            NdjsonWriter::new(&test_config(dir.path()), "ro", "out.ndjson", "dlq").expect("create");

        writer
            .write_line(b"{\"n\":1}\n")
            .expect_err("a write that reached no file must fail");
        assert_eq!(writer.lines_written(), 0);
        assert_eq!(writer.write_errors(), 1);
        let content = std::fs::read_to_string(target.join("out.ndjson")).expect("read");
        assert!(content.is_empty(), "nothing landed: {content:?}");
    }

    #[test]
    fn test_writer_refuses_a_filename_with_no_final_component() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = NdjsonWriter::new(&test_config(dir.path()), "svc", "..", "dlq")
            .expect_err("a filename of `..` names no file");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_debug_format() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());

        let writer = NdjsonWriter::new(&config, "dbg", "out.ndjson", "dlq").expect("create");
        let debug = format!("{writer:?}");
        assert!(debug.contains("NdjsonWriter"));
        assert!(debug.contains("dlq"));
    }

    #[test]
    fn test_label_and_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());

        let writer = NdjsonWriter::new(&config, "svc", "data.ndjson", "output").expect("create");
        assert_eq!(writer.label(), "output");
        assert_eq!(writer.output_path(), &dir.path().join("svc"));
    }

    // -----------------------------------------------------------------
    // AsyncNdjsonWriter tests -- these prove the async wrapper actually
    // moves the sync work off the runtime thread.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn async_write_line_writes_to_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_config(dir.path());
        let writer = NdjsonWriter::new(&cfg, "async-svc", "out.ndjson", "test").expect("create");
        let async_w = AsyncNdjsonWriter::new(writer);

        async_w
            .write_line(b"{\"k\":\"v\"}\n".to_vec())
            .await
            .expect("write_line");
        assert_eq!(async_w.lines_written(), 1);
        assert_eq!(async_w.write_errors(), 0);
        assert_eq!(async_w.label(), "test");
        assert_eq!(async_w.output_path(), dir.path().join("async-svc"));

        let body = std::fs::read_to_string(dir.path().join("async-svc/out.ndjson")).expect("read");
        assert_eq!(body.trim(), r#"{"k":"v"}"#);
    }

    #[tokio::test]
    async fn async_writer_from_arc_shares_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_config(dir.path());
        let writer = NdjsonWriter::new(&cfg, "share", "out.ndjson", "test").expect("create");
        let shared = Arc::new(writer);
        let a = AsyncNdjsonWriter::from_arc(Arc::clone(&shared));
        let b = AsyncNdjsonWriter::from_arc(Arc::clone(&shared));

        a.write_line(b"{\"a\":1}\n".to_vec()).await.expect("a");
        b.write_line(b"{\"b\":2}\n".to_vec()).await.expect("b");

        // Both views see the shared counter.
        assert_eq!(a.lines_written(), 2);
        assert_eq!(b.lines_written(), 2);
        assert!(Arc::ptr_eq(&a.shared(), &b.shared()));
    }

    #[tokio::test]
    async fn async_write_buf_writes_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_config(dir.path());
        let writer = NdjsonWriter::new(&cfg, "batch", "out.ndjson", "test").expect("create");
        let async_w = AsyncNdjsonWriter::new(writer);

        let mut buf = Vec::new();
        for i in 0..5 {
            buf.extend_from_slice(format!("{{\"n\":{i}}}\n").as_bytes());
        }
        async_w.write_buf(buf, 5).await.expect("write_buf");
        assert_eq!(async_w.lines_written(), 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_writer_does_not_block_runtime() {
        // Prove that concurrent writers + ticker on the same runtime
        // make progress concurrently -- i.e. write_line releases the
        // runtime thread.
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_config(dir.path());
        let writer = NdjsonWriter::new(&cfg, "concurrent", "out.ndjson", "test").expect("create");
        let async_w = AsyncNdjsonWriter::new(writer);

        let ticker_fired = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let tf = ticker_fired.clone();
        let ticker = tokio::spawn(async move {
            let mut t = tokio::time::interval(std::time::Duration::from_millis(2));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t.tick().await; // burn the t=0 tick
            for _ in 0..20 {
                t.tick().await;
                tf.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let mut writers = Vec::new();
        for _ in 0..4 {
            let w = async_w.clone();
            writers.push(tokio::spawn(async move {
                for i in 0..50_u32 {
                    w.write_line(format!("{{\"n\":{i}}}\n").into_bytes())
                        .await
                        .expect("write");
                }
            }));
        }
        for h in writers {
            h.await.expect("writer task");
        }
        ticker.await.expect("ticker task");

        assert_eq!(async_w.lines_written(), 200);
        let ticks = ticker_fired.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            ticks >= 10,
            "ticker fired only {ticks} times -- writers starved the runtime",
        );
    }
}
