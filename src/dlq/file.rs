// Project:   scalo
// File:      src/dlq/file.rs
// Purpose:   File-based DLQ backend using AsyncNdjsonWriter
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! File-based DLQ backend.
//!
//! Writes failed messages as NDJSON to a rotating file via
//! [`AsyncNdjsonWriter`]. The async wrapper runs the sync rotate-and-write
//! on `tokio::task::spawn_blocking` so the runtime thread is never
//! stalled.
//!
//! ## File Layout
//!
//! ```text
//! /var/spool/scalo/dlq/loader/
//! |-- dlq.ndjson              # Current file
//! |-- dlq.ndjson.20260302T140000     # Rotated, local time of the rotation
//! `-- dlq.ndjson.20260302T080000.gz  # Compressed, past the six newest
//! ```

use tracing::debug;

use crate::io::AsyncNdjsonWriter;
use crate::io::NdjsonWriter;

use super::config::FileDlqConfig;
use super::entry::DlqEntry;
use super::error::DlqError;

/// File backend -- internal variant carried by [`super::DlqBackend::File`].
pub struct FileDlqInner {
    writer: AsyncNdjsonWriter,
    service_name: String,
}

impl std::fmt::Debug for FileDlqInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileDlqInner")
            .field("service_name", &self.service_name)
            .field("output_path", &self.writer.output_path())
            .field("lines_written", &self.writer.lines_written())
            .field("write_errors", &self.writer.write_errors())
            .finish_non_exhaustive()
    }
}

impl FileDlqInner {
    /// Create the file backend. Creates the output directory if needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the output directory cannot be created.
    pub fn new(config: &FileDlqConfig, service_name: &str) -> Result<Self, DlqError> {
        let writer_config = config.to_writer_config();
        let writer =
            NdjsonWriter::new(&writer_config, service_name, "dlq.ndjson", "dlq").map_err(|e| {
                DlqError::File(format!(
                    "failed to create DLQ writer for {service_name}: {e}"
                ))
            })?;

        Ok(Self {
            writer: AsyncNdjsonWriter::new(writer),
            service_name: service_name.to_string(),
        })
    }

    /// Send a batch of entries. Serialises each entry to NDJSON, then
    /// hands the buffer to the async writer (which moves the sync I/O
    /// to a blocking thread via `spawn_blocking`).
    pub async fn send_batch(&mut self, batch: &[DlqEntry]) -> Result<(), DlqError> {
        if batch.is_empty() {
            return Ok(());
        }

        let buf = DlqEntry::ndjson(batch)
            .map_err(|e| DlqError::Serialization(format!("DLQ serialise: {e}")))?;

        let count = batch.len() as u64;
        self.writer
            .write_buf(buf, count)
            .await
            .map_err(|e| DlqError::File(format!("DLQ write_buf: {e}")))?;

        #[cfg(feature = "metrics")]
        {
            metrics::counter!("dlq_entries_total").increment(count);
            metrics::gauge!("dlq_entries_written").set(self.writer.lines_written() as f64);
        }

        debug!(
            service = %self.service_name,
            count = batch.len(),
            "DLQ batch written to file"
        );

        Ok(())
    }

    /// Sync the DLQ file's data to disk when `durable` is set. See
    /// [`crate::io::NdjsonWriter::flush`].
    ///
    /// # Errors
    ///
    /// Returns `DlqError::File` if the underlying writer reports an
    /// I/O error.
    pub async fn flush_durable(&mut self) -> Result<(), DlqError> {
        self.writer
            .flush()
            .await
            .map_err(|e| DlqError::File(format!("DLQ file flush: {e}")))
    }

    /// Number of entries successfully written.
    #[must_use]
    pub fn entries_written(&self) -> u64 {
        self.writer.lines_written()
    }

    /// Number of write errors.
    #[must_use]
    pub fn write_errors(&self) -> u64 {
        self.writer.write_errors()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dlq::config::RotationPeriod;
    use crate::dlq::entry::DlqSource;

    fn cfg(dir: &std::path::Path) -> FileDlqConfig {
        FileDlqConfig {
            enabled: true,
            path: dir.to_path_buf(),
            rotation: RotationPeriod::Daily,
            max_age_days: Some(1),
            compress_rotated: crate::compression::Compression::NONE,
            ..FileDlqConfig::default()
        }
    }

    #[tokio::test]
    async fn send_batch_writes_to_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut backend = FileDlqInner::new(&cfg(dir.path()), "svc").expect("create");

        let entries = vec![
            DlqEntry::new("svc", "parse_error", b"a".to_vec())
                .with_destination("acme.auth")
                .with_source(DlqSource::kafka("events", 1, 42)),
            DlqEntry::new("svc", "parse_error", b"b".to_vec()),
        ];

        backend.send_batch(&entries).await.expect("send");
        assert_eq!(backend.entries_written(), 2);

        let body = std::fs::read_to_string(dir.path().join("svc/dlq.ndjson")).expect("read");
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: DlqEntry = serde_json::from_str(lines[0]).expect("parse");
        assert_eq!(parsed.reason, "parse_error");
    }

    /// The file holds each entry as its JSON followed by one newline.
    #[tokio::test]
    async fn send_batch_writes_one_json_line_per_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut backend = FileDlqInner::new(&cfg(dir.path()), "lines").expect("create");
        let entries = [
            DlqEntry::new("svc", "parse_error", b"a".to_vec()),
            DlqEntry::new("svc", "too_large", b"b\n".to_vec()),
        ];
        backend.send_batch(&entries).await.expect("send");
        let mut expected = Vec::new();
        for entry in &entries {
            expected.extend(serde_json::to_vec(entry).expect("serialise"));
            expected.push(b'\n');
        }
        let written = std::fs::read(dir.path().join("lines/dlq.ndjson")).expect("read");
        assert_eq!(written, expected);
    }

    #[tokio::test]
    async fn send_batch_empty_is_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut backend = FileDlqInner::new(&cfg(dir.path()), "empty").expect("create");
        backend.send_batch(&[]).await.expect("empty");
        assert_eq!(backend.entries_written(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_backend_does_not_block_runtime() {
        // Concurrent backends + ticker on the same runtime -- ticker must
        // keep firing while writes are happening.
        let dir = tempfile::tempdir().expect("tempdir");
        let ticks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let tc = ticks.clone();
        let ticker = tokio::spawn(async move {
            let mut t = tokio::time::interval(std::time::Duration::from_millis(2));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t.tick().await;
            for _ in 0..15 {
                t.tick().await;
                tc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let mut backend = FileDlqInner::new(&cfg(dir.path()), "nb").expect("create");
        for _ in 0..30 {
            let batch: Vec<DlqEntry> = (0..10)
                .map(|i| DlqEntry::new("nb", "err", vec![i]))
                .collect();
            backend.send_batch(&batch).await.expect("send");
        }
        ticker.await.expect("ticker");
        let t = ticks.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            t >= 8,
            "ticker fired only {t} times -- file backend starved runtime",
        );
    }
}
