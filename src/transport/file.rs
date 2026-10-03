// Project:   scalo
// File:      src/transport/file.rs
// Purpose:   NDJSON file transport
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # File Transport
//!
//! NDJSON (newline-delimited JSON) file transport for debugging, audit
//! trails, and replay. Wraps async file I/O behind the Transport traits.
//!
//! ## Send
//!
//! Appends one NDJSON line per `send()` call to the configured file path.
//!
//! ## Receive
//!
//! Reads NDJSON lines from the file, tracking byte offset for commit.
//! Position is persisted to a `.pos` sidecar file so reads survive restarts,
//! written by temporary file and rename so a crash cannot truncate it.
//!
//! Only a line that ends in `\n` becomes a record. A trailing line without
//! one is held until the writer finishes it, so a file whose last line has no
//! newline never yields that line.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::file::{FileTransport, FileTransportConfig};
//!
//! let config = FileTransportConfig { path: "/tmp/events.ndjson".into(), append: true, ..Default::default() };
//! let transport = FileTransport::new(&config).await?;
//! transport.send("events", bytes::Bytes::from_static(b"{\"msg\":\"hello\"}")).await;
//! ```

use super::error::{TransportError, TransportResult};
use super::traits::{CommitToken, RecvBatch, TransportBase, TransportReceiver, TransportSender};
use super::types::{Message, PayloadFormat, SendResult};
use super::work_batch::WorkBatch;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

/// Commit token for file transport.
///
/// Contains the byte offset in the file after reading the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileToken {
    /// Byte offset after the line was read.
    pub offset: u64,
}

impl CommitToken for FileToken {}

impl std::fmt::Display for FileToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file:{}", self.offset)
    }
}

/// Configuration for file transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTransportConfig {
    /// File path for read/write.
    pub path: String,

    /// Append mode (default true for send).
    #[serde(default = "default_append")]
    pub append: bool,

    /// Inbound message filters (applied on recv before caller sees messages).
    #[serde(default)]
    pub filters_in: Vec<super::filter::FilterRule>,

    /// Outbound message filters (applied on send before transport dispatches).
    #[serde(default)]
    pub filters_out: Vec<super::filter::FilterRule>,
}

fn default_append() -> bool {
    true
}

impl Default for FileTransportConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            append: true,
            filters_in: Vec::new(),
            filters_out: Vec::new(),
        }
    }
}

impl FileTransportConfig {
    /// Load from the config cascade under the `transport.file` key.
    #[must_use]
    pub fn from_cascade() -> Self {
        <Self as super::traits::FromCascade>::from_cascade_key("transport.file")
    }
}

/// Internal state for the write side.
struct WriteState {
    file: tokio::fs::File,
}

/// Internal state for the read side, kept across `recv` calls so a dropped
/// call loses nothing.
struct ReadState {
    reader: BufReader<tokio::fs::File>,
    /// Byte offset just past the last complete line read.
    offset: u64,
    /// Bytes of a line still waiting for its newline.
    line: Vec<u8>,
    /// Records read but not yet returned.
    pending: Vec<Message<FileToken>>,
}

/// NDJSON file transport.
///
/// Supports both send (append) and receive (sequential read with
/// position tracking). Position is persisted to a `.pos` sidecar
/// file so reads survive process restarts.
pub struct FileTransport {
    config: FileTransportConfig,
    writer: Mutex<Option<WriteState>>,
    reader: Mutex<Option<ReadState>>,
    closed: Arc<AtomicBool>,
    filter_engine: super::filter::TransportFilterEngine,
}

impl FileTransport {
    /// Create a new file transport.
    ///
    /// # Errors
    ///
    /// Returns error if the file path is empty.
    pub async fn new(config: &FileTransportConfig) -> TransportResult<Self> {
        if config.path.is_empty() {
            return Err(TransportError::Config("file path is empty".into()));
        }

        #[cfg(feature = "logger")]
        tracing::info!(path = %config.path, append = config.append, "File transport opened");

        // Fail loud on bad filter config -- silently disabling filters
        // turns a misconfigured `drop` / `dlq` rule into a permanent pass.
        let filter_engine = super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade(),
        )?;

        let closed = Arc::new(AtomicBool::new(false));

        #[cfg(feature = "health")]
        {
            let h = Arc::clone(&closed);
            crate::health::HealthRegistry::register("transport:file", move || {
                if h.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Unhealthy
                } else {
                    crate::health::HealthStatus::Healthy
                }
            });
        }

        Ok(Self {
            config: config.clone(),
            writer: Mutex::new(None),
            reader: Mutex::new(None),
            closed,
            filter_engine,
        })
    }

    /// Path to the `.pos` sidecar file that tracks read position.
    fn pos_path(data_path: &Path) -> PathBuf {
        let mut pos_path = data_path.as_os_str().to_owned();
        pos_path.push(".pos");
        PathBuf::from(pos_path)
    }

    /// Load committed read position from the sidecar file.
    async fn load_position(data_path: &Path) -> u64 {
        let pos_path = Self::pos_path(data_path);
        match tokio::fs::read_to_string(&pos_path).await {
            Ok(content) => content.trim().parse::<u64>().unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Save read position to the sidecar file.
    ///
    /// Written to a temporary sibling, synced and renamed over the sidecar, so
    /// a crash mid-commit leaves the previous position rather than an empty
    /// file that reads back as offset 0.
    async fn save_position(data_path: &Path, offset: u64) -> TransportResult<()> {
        let pos_path = Self::pos_path(data_path);
        let mut tmp_path = pos_path.as_os_str().to_owned();
        tmp_path.push(".tmp");
        let tmp_path = PathBuf::from(tmp_path);
        let commit_err = |e: std::io::Error| {
            TransportError::Commit(format!("failed to write position file: {e}"))
        };

        let mut tmp = tokio::fs::File::create(&tmp_path)
            .await
            .map_err(commit_err)?;
        tmp.write_all(offset.to_string().as_bytes())
            .await
            .map_err(commit_err)?;
        tmp.sync_all().await.map_err(commit_err)?;
        drop(tmp);
        tokio::fs::rename(&tmp_path, &pos_path)
            .await
            .map_err(commit_err)
    }

    /// Lazily open the write file handle.
    async fn ensure_writer(&self) -> TransportResult<()> {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(self.config.append)
                .write(true)
                .open(&self.config.path)
                .await
                .map_err(|e| {
                    TransportError::Connection(format!(
                        "failed to open '{}' for writing: {e}",
                        self.config.path
                    ))
                })?;
            *guard = Some(WriteState { file });
        }
        Ok(())
    }

    /// Lazily open the read file handle and seek to committed position.
    async fn ensure_reader(&self) -> TransportResult<()> {
        let mut guard = self.reader.lock().await;
        if guard.is_none() {
            let path = Path::new(&self.config.path);

            // If the file does not exist yet, there is nothing to read
            if !path.exists() {
                return Err(TransportError::Recv(format!(
                    "file '{}' does not exist",
                    self.config.path
                )));
            }

            let offset = Self::load_position(path).await;
            let mut file = tokio::fs::File::open(&self.config.path)
                .await
                .map_err(|e| {
                    TransportError::Connection(format!(
                        "failed to open '{}' for reading: {e}",
                        self.config.path
                    ))
                })?;

            // Seek to committed position
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(|e| {
                    TransportError::Recv(format!("failed to seek to offset {offset}: {e}"))
                })?;

            *guard = Some(ReadState {
                reader: BufReader::new(file),
                offset,
                line: Vec::with_capacity(4096),
                pending: Vec::new(),
            });
        }
        Ok(())
    }
}

impl TransportBase for FileTransport {
    async fn close(&self) -> TransportResult<()> {
        self.closed.store(true, Ordering::Relaxed);

        // Flush and drop writer
        if let Some(mut state) = self.writer.lock().await.take() {
            let _ = state.file.flush().await;
        }

        // Drop reader
        let _ = self.reader.lock().await.take();

        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.closed.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

impl TransportSender for FileTransport {
    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        // Outbound filter check
        if self.filter_engine.has_outbound_filters() {
            match self.filter_engine.apply_outbound(&payload) {
                super::filter::FilterDisposition::Pass => {}
                super::filter::FilterDisposition::Drop => return SendResult::Ok,
                super::filter::FilterDisposition::Dlq => return SendResult::FilteredDlq,
            }
        }

        if let Err(e) = self.ensure_writer().await {
            return SendResult::Fatal(e);
        }

        let mut guard = self.writer.lock().await;
        let Some(state) = guard.as_mut() else {
            return SendResult::Fatal(TransportError::Internal("writer not initialised".into()));
        };

        // Write payload + newline as a single operation
        if let Err(e) = state.file.write_all(&payload).await {
            #[cfg(feature = "logger")]
            tracing::warn!(error = %e, "File transport: write error");
            return SendResult::Fatal(TransportError::Send(format!("write failed: {e}")));
        }
        if let Err(e) = state.file.write_all(b"\n").await {
            #[cfg(feature = "logger")]
            tracing::warn!(error = %e, "File transport: newline write error");
            return SendResult::Fatal(TransportError::Send(format!("write newline failed: {e}")));
        }
        if let Err(e) = state.file.flush().await {
            #[cfg(feature = "logger")]
            tracing::warn!(error = %e, "File transport: flush error");
            return SendResult::Fatal(TransportError::Send(format!("flush failed: {e}")));
        }

        #[cfg(feature = "logger")]
        tracing::debug!(bytes = payload.len(), "File transport: message sent");

        #[cfg(feature = "metrics")]
        {
            metrics::counter!("transport_sent_total", "transport" => "file").increment(1);
            metrics::counter!("transport_sent_bytes_total", "transport" => "file")
                .increment(payload.len() as u64);
        }

        SendResult::Ok
    }
}

impl TransportReceiver for FileTransport {
    type Token = FileToken;

    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TransportError::Closed);
        }

        self.ensure_reader().await?;

        let mut guard = self.reader.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| TransportError::Internal("reader not initialised".into()))?;

        // `read_until` leaves a partial line in `line` when dropped, and
        // `pending` holds finished records, so a dropped call resumes here.
        let ReadState {
            reader,
            offset,
            line,
            pending,
        } = state;

        for _ in pending.len()..max {
            reader
                .read_until(b'\n', line)
                .await
                .map_err(|e| TransportError::Recv(format!("read failed: {e}")))?;

            // EOF, or a line the writer has not finished: keep the partial
            // bytes in `line` for the next call and leave `offset` before them.
            if line.last() != Some(&b'\n') {
                break;
            }

            // Every byte of the line, across dropped calls too, is in `line`.
            *offset += line.len() as u64;

            // Strip the trailing newline and any carriage returns before it
            let end = line
                .iter()
                .rposition(|&b| b != b'\n' && b != b'\r')
                .map_or(0, |i| i + 1);
            let payload = bytes::Bytes::copy_from_slice(&line[..end]);
            line.clear();
            if payload.is_empty() {
                continue;
            }

            let format = PayloadFormat::detect(&payload);
            let timestamp_ms = chrono::Utc::now().timestamp_millis();

            pending.push(Message {
                key: None,
                payload,
                token: FileToken { offset: *offset },
                timestamp_ms: Some(timestamp_ms),
                format,
            });
        }
        // A call with a smaller `max` than the dropped one leaves the rest queued.
        let rest = pending.split_off(pending.len().min(max));
        let messages = std::mem::replace(pending, rest);
        drop(guard);

        // Apply inbound filters via the shared partition helper; DLQ entries
        // are returned in the RecvBatch for the caller to route onward.
        let batch = self.filter_engine.partition_batch(
            messages,
            |m| m.payload.as_ref(),
            |m| m.key.clone(),
            |m| m.token,
        );
        let messages = batch.messages;
        let dlq_entries = batch.dlq_entries;
        let filtered_tokens = batch.filtered_tokens;

        #[cfg(feature = "logger")]
        if !messages.is_empty() {
            tracing::debug!(lines = messages.len(), "File transport: batch received");
        }

        #[cfg(feature = "metrics")]
        if !messages.is_empty() {
            let bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
            metrics::counter!("transport_received_bytes_total", "transport" => "file")
                .increment(bytes as u64);
            metrics::counter!("transport_received_events_total", "transport" => "file")
                .increment(messages.len() as u64);
        }

        Ok(RecvBatch {
            messages,
            dlq_entries,
            filtered_tokens,
        }
        .into())
    }

    async fn commit(&self, tokens: &[Self::Token]) -> TransportResult<()> {
        if let Some(max_token) = tokens.iter().max_by_key(|t| t.offset) {
            let path = Path::new(&self.config.path);
            Self::save_position(path, max_token.offset).await?;

            #[cfg(feature = "logger")]
            tracing::debug!(
                offset = max_token.offset,
                "File transport: position committed"
            );
        }
        Ok(())
    }
}

impl super::traits::FromCascade for FileTransportConfig {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn make_transport(dir: &TempDir, filename: &str) -> FileTransport {
        let path = dir.path().join(filename);
        let config = FileTransportConfig {
            path: path.to_str().unwrap().to_string(),
            append: true,
            ..Default::default()
        };
        FileTransport::new(&config).await.unwrap()
    }

    #[tokio::test]
    async fn send_and_receive() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.ndjson");
        let path_str = path.to_str().unwrap().to_string();

        // Write messages
        let config = FileTransportConfig {
            path: path_str.clone(),
            append: true,
            ..Default::default()
        };
        let sender = FileTransport::new(&config).await.unwrap();

        let r1 = sender
            .send("key", bytes::Bytes::from_static(b"{\"msg\":\"hello\"}"))
            .await;
        assert!(r1.is_ok());
        let r2 = sender
            .send("key", bytes::Bytes::from_static(b"{\"msg\":\"world\"}"))
            .await;
        assert!(r2.is_ok());
        sender.close().await.unwrap();

        // Read messages back
        let reader_config = FileTransportConfig {
            path: path_str,
            append: true,
            ..Default::default()
        };
        let reader = FileTransport::new(&reader_config).await.unwrap();
        let batch = reader.recv(10).await.unwrap();

        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].payload.as_ref(), b"{\"msg\":\"hello\"}");
        assert_eq!(batch.records[1].payload.as_ref(), b"{\"msg\":\"world\"}");

        // Commit tokens (carried on the batch, in record order) should have
        // increasing offsets.
        assert!(batch.commit_tokens[1].offset > batch.commit_tokens[0].offset);
    }

    #[tokio::test]
    async fn commit_persists_position() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("commit_test.ndjson");
        let path_str = path.to_str().unwrap().to_string();

        // Write 3 messages
        let config = FileTransportConfig {
            path: path_str.clone(),
            append: true,
            ..Default::default()
        };
        let sender = FileTransport::new(&config).await.unwrap();
        sender.send("k", bytes::Bytes::from_static(b"line1")).await;
        sender.send("k", bytes::Bytes::from_static(b"line2")).await;
        sender.send("k", bytes::Bytes::from_static(b"line3")).await;
        sender.close().await.unwrap();

        // Read first 2 messages and commit
        let r1 = FileTransport::new(&FileTransportConfig {
            path: path_str.clone(),
            append: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let batch = r1.recv(2).await.unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].payload.as_ref(), b"line1");
        assert_eq!(batch.records[1].payload.as_ref(), b"line2");

        // Commit up to message 2 via the batch's commit tokens.
        r1.commit(&batch.commit_tokens).await.unwrap();
        r1.close().await.unwrap();

        // Open a new transport -- should resume from committed position
        let r2 = FileTransport::new(&FileTransportConfig {
            path: path_str,
            append: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let remaining = r2.recv(10).await.unwrap().records;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].payload.as_ref(), b"line3");
    }

    #[tokio::test]
    async fn close_prevents_operations() {
        let dir = TempDir::new().unwrap();
        let transport = make_transport(&dir, "close_test.ndjson").await;

        transport.close().await.unwrap();
        assert!(!transport.is_healthy());

        let result = transport
            .send("k", bytes::Bytes::from_static(b"data"))
            .await;
        assert!(result.is_fatal());

        let result = transport.recv(1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn file_token_display() {
        let token = FileToken { offset: 42 };
        assert_eq!(format!("{token}"), "file:42");
    }

    #[tokio::test]
    async fn recv_returns_empty_at_eof() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("eof_test.ndjson");
        let path_str = path.to_str().unwrap().to_string();

        // Write one line
        let config = FileTransportConfig {
            path: path_str.clone(),
            append: true,
            ..Default::default()
        };
        let transport = FileTransport::new(&config).await.unwrap();
        transport
            .send("k", bytes::Bytes::from_static(b"only_line"))
            .await;
        transport.close().await.unwrap();

        // Read all, then read again -- should get empty
        let reader = FileTransport::new(&FileTransportConfig {
            path: path_str,
            append: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let msgs = reader.recv(10).await.unwrap().records;
        assert_eq!(msgs.len(), 1);

        let more = reader.recv(10).await.unwrap().records;
        assert_eq!(more, [] as [crate::transport::work_batch::Record; 0]);
    }

    /// `recv` dropped after a single poll, as a losing `select!` arm is,
    /// across lines that straddle the read buffer's fills: every line still
    /// arrives once, in order, and the last token is the end of the file.
    #[tokio::test]
    async fn a_recv_dropped_mid_read_loses_no_lines() {
        let dir = TempDir::new().unwrap();
        let lines: Vec<String> = (0..2_000)
            .map(|n| format!("{{\"n\":{n},\"pad\":\"{}\"}}", "x".repeat(n % 97)))
            .collect();
        let path = dir.path().join("dropped.ndjson");
        let body: String = lines.iter().flat_map(|l| [l.as_str(), "\n"]).collect();
        std::fs::write(&path, &body).unwrap();
        let transport = make_transport(&dir, "dropped.ndjson").await;

        let mut got = Vec::new();
        let mut last_offset = 0;
        let mut dropped = 0;
        for _ in 0..10_000 {
            // `biased` polls recv exactly once before the ready arm drops it.
            let polled = tokio::select! {
                biased;
                batch = transport.recv(10_000) => Some(batch.unwrap()),
                () = std::future::ready(()) => None,
            };
            if let Some(batch) = polled {
                if batch.records.is_empty() {
                    break;
                }
                got.extend(
                    batch
                        .records
                        .iter()
                        .map(|r| String::from_utf8(r.payload.to_vec()).unwrap()),
                );
                last_offset = batch.commit_tokens.last().unwrap().offset;
            } else {
                dropped += 1;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        assert!(dropped > 0, "recv was never dropped mid-read");
        let first_wrong = got.iter().zip(&lines).position(|(g, l)| g != l);
        assert_eq!(
            first_wrong,
            None,
            "wrong line: got {:?}",
            first_wrong.map(|i| &got[i])
        );
        assert_eq!(got.len(), lines.len(), "lines received");
        assert_eq!(last_offset, body.len() as u64, "last token offset");
    }

    /// A reader racing an appender sees a half-written line as nothing until
    /// its newline lands, then as one record.
    #[tokio::test]
    async fn a_line_without_its_newline_waits_for_the_rest() {
        use std::io::Write as _;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("append.ndjson");
        std::fs::write(&path, b"first\n{\"half\":").unwrap();
        let reader = make_transport(&dir, "append.ndjson").await;

        let batch = reader.recv(10).await.unwrap();
        let payloads: Vec<&[u8]> = batch.records.iter().map(|r| r.payload.as_ref()).collect();
        assert_eq!(payloads, [b"first".as_slice()]);
        assert_eq!(
            batch.commit_tokens[0].offset, 6,
            "the offset stops before the half line"
        );
        assert_eq!(reader.recv(10).await.unwrap().records.len(), 0);

        let mut appender = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        appender.write_all(b"true}\n").unwrap();

        let batch = reader.recv(10).await.unwrap();
        let payloads: Vec<&[u8]> = batch.records.iter().map(|r| r.payload.as_ref()).collect();
        assert_eq!(payloads, [b"{\"half\":true}".as_slice()]);
        assert_eq!(batch.commit_tokens[0].offset, 20);
    }

    /// The position is renamed into place, so a commit leaves the sidecar
    /// holding a whole offset and no temporary file behind it.
    #[tokio::test]
    async fn commit_replaces_the_position_file_whole() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pos.ndjson");
        std::fs::write(&path, b"a\nbb\n").unwrap();
        let reader = make_transport(&dir, "pos.ndjson").await;

        let batch = reader.recv(10).await.unwrap();
        reader.commit(&batch.commit_tokens[..1]).await.unwrap();
        reader.commit(&batch.commit_tokens).await.unwrap();

        let pos = dir.path().join("pos.ndjson.pos");
        assert_eq!(std::fs::read_to_string(&pos).unwrap(), "5");
        assert!(
            !dir.path().join("pos.ndjson.pos.tmp").exists(),
            "the temporary file is renamed over the sidecar"
        );
    }

    #[tokio::test]
    async fn empty_path_is_config_error() {
        let result = FileTransport::new(&FileTransportConfig::default()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn transport_name() {
        let dir = TempDir::new().unwrap();
        let transport = make_transport(&dir, "name_test.ndjson").await;
        assert_eq!(transport.name(), "file");
    }
}
