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
//! Appends one NDJSON line per record through [`crate::io::NdjsonWriter`],
//! a whole `send_batch` block in one write. Rotation by size or time,
//! `durable` syncing and file modes come from the writer's keys. Rotation is
//! off by default, and a reader of the same path follows a rotation only with
//! acknowledgements on.
//!
//! ## Receive
//!
//! Reads NDJSON lines from the file, tracking byte offset for commit.
//! Position is persisted to a `.pos` sidecar file so reads survive restarts,
//! written by temporary file and rename so a crash cannot truncate it.
//!
//! With an `acknowledgements` section that is enabled, the saved position
//! moves only past lines whose release delivered them, in file order, so a
//! later block delivered first never moves it past an earlier one. On Unix
//! the sidecar then names the file's device and inode too, and a reader that
//! finds the path renamed under it by rotation finishes the renamed file,
//! waits for its last line to be released, then reads the new file from its
//! start. A restart whose saved position names a rotated file finds that file
//! by inode and reads its tail first. Without the section the reader saves
//! the highest offset released and does not follow a rotation.
//!
//! Only a line that ends in `\n` becomes a record. A trailing line without
//! one is held until the writer finishes it, so a file whose last line has no
//! newline never yields that line. A line over `max_line_bytes` is never held
//! whole: it comes back as a dead letter carrying its first `max_line_bytes`.
//!
//! At the end of the file `recv` reads again every 10 ms for up to
//! `recv_timeout_ms` (default 100) before it returns empty, so a loop over it
//! does not spin.
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

use super::ack::{AckControl, AckKind, AcknowledgementsConfig, AcknowledgingReceiver, HeldAcks};
use super::core::{Lifecycle, Meter};
use super::error::{TransportError, TransportResult};
use super::finalizer::DeliveryStatus;
use super::traits::{CommitToken, RecvBatch, TransportBase, TransportReceiver, TransportSender};
use super::types::{Message, PayloadFormat, SendResult};
use super::work_batch::WorkBatch;
use crate::compression::Compression;
use crate::framing::{DecodeLimits, Frame, Framing, Lf, StreamDecoder};
use crate::io::{
    FileWriteOptions, FileWriterConfig, NO_AGE_LIMIT, NdjsonWriter, ROTATED_ACCEPTS, RotationPeriod,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader};
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

    /// Append to what the file holds (default). `false` empties the file
    /// when the first send opens it.
    #[serde(default = "default_append")]
    pub append: bool,

    /// Time rotation of the sent file. Default `never`.
    #[serde(default = "default_rotation")]
    pub rotation: RotationPeriod,

    /// Delete rotated files older than this many days, at each rotation.
    /// Unset by default: no age limit.
    #[serde(default)]
    pub max_age_days: Option<u32>,

    /// Codec for rotated files past the six newest: `none` (default), `gzip`
    /// or `zstd`.
    #[serde(default)]
    pub compress_rotated: Compression,

    /// Size rotation, retention by count, `durable` and file modes. An unset
    /// mode is private here: files `0600`, folders `0700`.
    #[serde(flatten)]
    pub write: FileWriteOptions,

    /// Inbound message filters (applied on recv before caller sees messages).
    #[serde(default)]
    pub filters_in: Vec<super::filter::FilterRule>,

    /// Outbound message filters (applied on send before transport dispatches).
    #[serde(default)]
    pub filters_out: Vec<super::filter::FilterRule>,

    /// Longest line read into memory, not counting its newline. Default
    /// 16 MiB. A longer line is read past without being held and returned as
    /// a dead letter carrying its first `max_line_bytes`.
    #[serde(default = "default_max_line_bytes")]
    pub max_line_bytes: usize,

    /// Longest `recv` waits at the end of the file for a new line, in ms,
    /// checking for one every 10 ms, before it returns empty. Default 100. 0
    /// returns empty at once.
    #[serde(default = "default_recv_timeout_ms")]
    pub recv_timeout_ms: u64,
}

fn default_append() -> bool {
    true
}

fn default_recv_timeout_ms() -> u64 {
    100
}

/// How often a `recv` waiting at the end of the file reads it again.
const EOF_POLL: std::time::Duration = std::time::Duration::from_millis(10);

fn default_rotation() -> RotationPeriod {
    RotationPeriod::Never
}

fn default_max_line_bytes() -> usize {
    16 * 1024 * 1024
}

impl Default for FileTransportConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            append: true,
            rotation: default_rotation(),
            max_age_days: None,
            compress_rotated: Compression::NONE,
            write: FileWriteOptions::default(),
            filters_in: Vec::new(),
            filters_out: Vec::new(),
            max_line_bytes: default_max_line_bytes(),
            recv_timeout_ms: default_recv_timeout_ms(),
        }
    }
}

impl FileTransportConfig {
    /// The writer settings and file name for the send side.
    fn writer_config(&self) -> TransportResult<(FileWriterConfig, String)> {
        let path = Path::new(&self.path);
        let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            TransportError::Config(format!("file path {:?} names no file", self.path))
        })?;
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let compress_rotated = ROTATED_ACCEPTS
            .check(self.compress_rotated)
            .map_err(|e| TransportError::Config(e.to_string()))?;
        let config = FileWriterConfig {
            path: dir.to_path_buf(),
            rotation: self.rotation,
            max_age_days: self.max_age_days.unwrap_or(NO_AGE_LIMIT),
            compress_rotated,
            write: self.write.private_by_default(),
        };
        config.check_naming().map_err(TransportError::Config)?;
        Ok((config, name.to_owned()))
    }

    /// Load from the config cascade under the `transport.file` key, or the defaults
    /// when it is absent.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the section is present but does not parse.
    pub fn from_cascade() -> crate::transport::TransportResult<Self> {
        <Self as super::traits::FromCascade>::from_cascade_key("transport.file")
    }
}

/// The device and inode of a file, which a rename keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
}

impl FileId {
    /// The id of the file `meta` describes, `None` off Unix.
    #[cfg_attr(unix, allow(clippy::unnecessary_wraps))]
    fn of(meta: &std::fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            Some(Self {
                dev: meta.dev(),
                ino: meta.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = meta;
            None
        }
    }

    /// The id of the file at `path`, `None` when it is missing or off Unix.
    async fn at(path: &Path) -> Option<Self> {
        Self::of(&tokio::fs::metadata(path).await.ok()?)
    }
}

/// A position saved in the `.pos` sidecar: `<offset>`, or `<offset> <dev>:<ino>`
/// once acknowledgements name the file it is in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SavedPosition {
    offset: u64,
    file: Option<FileId>,
}

impl SavedPosition {
    /// Read back what [`render`](Self::render) wrote. An unreadable sidecar
    /// is offset 0 in the file at the path.
    fn parse(content: &str) -> Self {
        let mut words = content.split_whitespace();
        let offset = words.next().and_then(|w| w.parse().ok()).unwrap_or(0);
        let file = words.next().and_then(|w| {
            let (dev, ino) = w.split_once(':')?;
            Some(FileId {
                dev: dev.parse().ok()?,
                ino: ino.parse().ok()?,
            })
        });
        Self { offset, file }
    }

    fn render(self) -> String {
        match self.file {
            Some(FileId { dev, ino }) => format!("{} {dev}:{ino}", self.offset),
            None => self.offset.to_string(),
        }
    }
}

/// Where a line `recv` handed out stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Line {
    Out,
    Delivered,
    /// Released `Errored`: the saved position never passes it.
    Withheld,
}

/// The lines handed out of the file being read and not yet saved past, by
/// the offset after each.
#[derive(Debug, Default)]
struct HeldLines {
    /// The file the lines are in.
    file: Option<FileId>,
    lines: BTreeMap<u64, Line>,
}

impl HeldLines {
    fn hand_out(&mut self, offsets: impl IntoIterator<Item = u64>) {
        for offset in offsets {
            self.lines.insert(offset, Line::Out);
        }
    }

    /// Mark `offsets` released, and the position the saved one may move to:
    /// past every line from the front that is delivered.
    fn release(&mut self, offsets: impl IntoIterator<Item = u64>, delivered: bool) -> Option<u64> {
        for offset in offsets {
            if let Some(line) = self.lines.get_mut(&offset) {
                *line = if delivered {
                    Line::Delivered
                } else {
                    Line::Withheld
                };
            }
        }
        let mut to = None;
        while let Some(front) = self.lines.first_entry() {
            if *front.get() != Line::Delivered {
                break;
            }
            to = Some(*front.key());
            front.remove();
        }
        to
    }
}

/// The acknowledgement state of a file source built with an `acknowledgements`
/// section.
#[derive(Debug)]
struct FileAcks {
    config: AcknowledgementsConfig,
    armed: AtomicBool,
    held: parking_lot::Mutex<HeldLines>,
    /// The position last saved, which a save never moves back.
    saved: Mutex<SavedPosition>,
}

impl FileAcks {
    fn new(config: AcknowledgementsConfig) -> Self {
        Self {
            config,
            armed: AtomicBool::new(false),
            held: parking_lot::Mutex::new(HeldLines::default()),
            saved: Mutex::new(SavedPosition::default()),
        }
    }
}

impl AckControl for FileAcks {
    fn enabled(&self) -> bool {
        self.config.enabled
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    fn kind(&self) -> AckKind {
        AckKind::Pull
    }

    fn held(&self) -> HeldAcks {
        let held = self.held.lock();
        let count = held
            .lines
            .values()
            .filter(|l| **l != Line::Delivered)
            .count();
        HeldAcks::new(count as u64, 0, None, None)
    }
}

/// Internal state for the read side, kept across `recv` calls so a dropped
/// call loses nothing.
struct ReadState {
    reader: BufReader<tokio::fs::File>,
    /// Byte offset the reader started at.
    base: u64,
    /// The file the reader has open, which rotation may have renamed.
    file: Option<FileId>,
    /// Holds a line still waiting for its newline.
    decoder: StreamDecoder,
    /// Lines read but not yet returned, in file order.
    pending: Vec<ReadLine>,
}

impl ReadState {
    fn new(file: tokio::fs::File, base: u64, id: Option<FileId>, max_line_bytes: usize) -> Self {
        let limits = DecodeLimits {
            max_record_bytes: max_line_bytes,
            ..DecodeLimits::UNBOUNDED
        };
        Self {
            reader: BufReader::new(file),
            base,
            file: id,
            decoder: StreamDecoder::new(Arc::new(Lf), limits),
            pending: Vec::new(),
        }
    }
}

/// A complete line the reader has taken off the file.
enum ReadLine {
    /// A line within `max_line_bytes`.
    Record(Message<FileToken>),
    /// A line over `max_line_bytes`, as a dead letter of its head.
    TooLong(super::filter::FilteredDlqEntry, FileToken),
}

impl ReadLine {
    /// The offset after the line.
    fn offset(&self) -> u64 {
        match self {
            Self::Record(message) => message.token.offset,
            Self::TooLong(_, token) => token.offset,
        }
    }
}

/// NDJSON file transport.
///
/// Supports both send (append) and receive (sequential read with
/// position tracking). Position is persisted to a `.pos` sidecar
/// file so reads survive process restarts.
pub struct FileTransport {
    config: FileTransportConfig,
    /// The send side's writer, opened on the first send.
    writer: Mutex<Option<Arc<NdjsonWriter>>>,
    reader: Mutex<Option<ReadState>>,
    /// Set by an `acknowledgements` section, `None` without one.
    acks: Option<FileAcks>,
    lifecycle: Lifecycle,
    filter_engine: super::filter::TransportFilterEngine,
}

impl FileTransport {
    /// Create a new file transport.
    ///
    /// # Errors
    ///
    /// Returns error if the file path is empty or names no file.
    pub async fn new(config: &FileTransportConfig) -> TransportResult<Self> {
        if config.path.is_empty() {
            return Err(TransportError::Config("file path is empty".into()));
        }
        config.writer_config()?;
        if config.max_line_bytes == 0 {
            return Err(TransportError::Config(
                "max_line_bytes is 0, which turns every line into a dead letter".into(),
            ));
        }

        #[cfg(feature = "logger")]
        tracing::info!(
            path = %config.path,
            append = config.append,
            rotation = ?config.rotation,
            max_bytes = ?config.write.max_bytes,
            durable = config.write.durable,
            "File transport opened"
        );

        // Fail loud on bad filter config -- silently disabling filters
        // turns a misconfigured `drop` / `dlq` rule into a permanent pass.
        let filter_engine = super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade()?,
        )?;

        Ok(Self {
            config: config.clone(),
            writer: Mutex::new(None),
            reader: Mutex::new(None),
            acks: None,
            lifecycle: Lifecycle::new::<Self>(|| true),
            filter_engine,
        })
    }

    /// Apply an `acknowledgements` section. Enabled, the saved position moves
    /// only past lines delivered, in file order, and the reader follows a
    /// rotation once the renamed file's last line is released. Without a
    /// section, or with it disabled, the reader saves the highest offset
    /// released.
    ///
    /// `AnyReceiver::from_config` reads it from `<key>.file.acknowledgements`.
    #[must_use]
    pub fn with_acknowledgements(mut self, acknowledgements: AcknowledgementsConfig) -> Self {
        self.acks = Some(FileAcks::new(acknowledgements));
        self
    }

    /// The acknowledgement state, when a section turned acknowledgements on.
    fn holding(&self) -> Option<&FileAcks> {
        self.acks.as_ref().filter(|a| a.config.enabled)
    }

    /// Path to the `.pos` sidecar file that tracks read position.
    fn pos_path(data_path: &Path) -> PathBuf {
        let mut pos_path = data_path.as_os_str().to_owned();
        pos_path.push(".pos");
        PathBuf::from(pos_path)
    }

    /// Load committed read position from the sidecar file.
    async fn load_position(data_path: &Path) -> SavedPosition {
        let pos_path = Self::pos_path(data_path);
        match tokio::fs::read_to_string(&pos_path).await {
            Ok(content) => SavedPosition::parse(&content),
            Err(_) => SavedPosition::default(),
        }
    }

    /// Save read position to the sidecar file.
    ///
    /// Written to a temporary sibling, synced and renamed over the sidecar, so
    /// a crash mid-commit leaves the previous position rather than an empty
    /// file that reads back as offset 0.
    async fn save_position(data_path: &Path, position: SavedPosition) -> TransportResult<()> {
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
        tmp.write_all(position.render().as_bytes())
            .await
            .map_err(commit_err)?;
        tmp.sync_all().await.map_err(commit_err)?;
        drop(tmp);
        tokio::fs::rename(&tmp_path, &pos_path)
            .await
            .map_err(commit_err)
    }

    /// The writer, opened on the first call. Creates missing directories.
    async fn writer(&self) -> TransportResult<Arc<NdjsonWriter>> {
        let mut guard = self.writer.lock().await;
        if let Some(writer) = guard.as_ref() {
            return Ok(Arc::clone(writer));
        }
        let (config, name) = self.config.writer_config()?;
        let append = self.config.append;
        let opened = tokio::task::spawn_blocking(move || {
            if append {
                NdjsonWriter::new(&config, "", &name, "file transport")
            } else {
                NdjsonWriter::truncating(&config, "", &name, "file transport")
            }
        })
        .await
        .map_err(|e| TransportError::Internal(format!("file writer open panicked: {e}")))?
        .map_err(|e| {
            TransportError::Connection(format!(
                "failed to open '{}' for writing: {e}",
                self.config.path
            ))
        })?;
        let writer = Arc::new(opened);
        *guard = Some(Arc::clone(&writer));
        Ok(writer)
    }

    /// Writes `block`, holding `records` framed lines of `payload_bytes`
    /// before framing, in one write.
    async fn write_block(&self, block: Vec<u8>, records: u64, payload_bytes: u64) -> SendResult {
        let writer = match self.writer().await {
            Ok(writer) => writer,
            Err(e) => return SendResult::Fatal(e),
        };
        let written = tokio::task::spawn_blocking(move || writer.write_buf(&block, records)).await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                #[cfg(feature = "logger")]
                tracing::warn!(error = %e, path = %self.config.path, "File transport: write error");
                return SendResult::Fatal(TransportError::Send(format!("write failed: {e}")));
            }
            Err(e) => {
                return SendResult::Fatal(TransportError::Internal(format!(
                    "file write panicked: {e}"
                )));
            }
        }

        #[cfg(feature = "logger")]
        tracing::debug!(
            records,
            bytes = payload_bytes,
            "File transport: block written"
        );

        Self::count_sent(records, payload_bytes);
        SendResult::Ok
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

            let saved = Self::load_position(path).await;
            let (open_path, start) = match self.holding() {
                Some(_) => self.resume_from(saved).await,
                None => (path.to_path_buf(), saved.offset),
            };
            let mut file = tokio::fs::File::open(&open_path).await.map_err(|e| {
                TransportError::Connection(format!(
                    "failed to open '{}' for reading: {e}",
                    open_path.display()
                ))
            })?;

            // Seek to committed position
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(|e| {
                    TransportError::Recv(format!("failed to seek to offset {start}: {e}"))
                })?;

            let id = FileId::of(&file.metadata().await.map_err(|e| {
                TransportError::Recv(format!("failed to stat '{}': {e}", open_path.display()))
            })?);
            if let Some(acks) = self.holding() {
                let opened = SavedPosition {
                    offset: start,
                    file: id,
                };
                // Saved at once, so a rotation before the first release still
                // leaves a sidecar that names this file.
                if opened != saved {
                    Self::save_position(path, opened).await?;
                }
                acks.held.lock().file = id;
                *acks.saved.lock().await = opened;
            }
            *guard = Some(ReadState::new(file, start, id, self.config.max_line_bytes));
        }
        Ok(())
    }

    /// The file and offset a holding reader resumes from: the saved file, by
    /// inode, when rotation renamed it, else the file at the path.
    async fn resume_from(&self, saved: SavedPosition) -> (PathBuf, u64) {
        let path = Path::new(&self.config.path);
        let Some(want) = saved.file else {
            return (path.to_path_buf(), saved.offset);
        };
        if FileId::at(path).await == Some(want) {
            return (path.to_path_buf(), saved.offset);
        }
        if let Some(rotated) = Self::find_by_id(path, want).await {
            #[cfg(feature = "logger")]
            tracing::info!(
                path = %rotated.display(),
                offset = saved.offset,
                "File transport: reading the rotated file the saved position names first"
            );
            return (rotated, saved.offset);
        }
        #[cfg(feature = "logger")]
        tracing::warn!(
            path = %self.config.path,
            offset = saved.offset,
            "File transport: the rotated file the saved position names is gone, so its unread \
             lines are lost and the live file is read from its start"
        );
        (path.to_path_buf(), 0)
    }

    /// The sibling of `live` named `<live>.<suffix>` whose id is `want`.
    async fn find_by_id(live: &Path, want: FileId) -> Option<PathBuf> {
        let prefix = {
            let mut name = live.file_name()?.to_owned();
            name.push(".");
            name
        };
        let dir = live
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut entries = tokio::fs::read_dir(dir).await.ok()?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            if !name
                .as_encoded_bytes()
                .starts_with(prefix.as_encoded_bytes())
            {
                continue;
            }
            if let Ok(meta) = entry.metadata().await
                && FileId::of(&meta) == Some(want)
            {
                return Some(entry.path());
            }
        }
        None
    }

    /// Read up to `max` lines past those already handed out, following a
    /// rotation once the renamed file is delivered.
    async fn read_pass(&self, state: &mut ReadState, max: usize) -> TransportResult<Vec<ReadLine>> {
        // Seen before the read, so the read ends the renamed file.
        let renamed = match self.holding() {
            Some(_) => FileId::at(Path::new(&self.config.path))
                .await
                .is_some_and(|live| Some(live) != state.file),
            None => false,
        };
        // A dropped read leaves a partial line in the decoder, and `pending`
        // holds finished lines, so a dropped call resumes here.
        let ReadState {
            reader,
            base,
            decoder,
            pending,
            ..
        } = &mut *state;
        let cap = self.config.max_line_bytes;

        for _ in pending.len()..max {
            // EOF, or a line the writer has not finished: the decoder keeps
            // the partial bytes for the next call.
            let Some(frame) = decoder
                .read(reader)
                .await
                .map_err(|e| TransportError::Recv(format!("read failed: {e}")))?
            else {
                break;
            };
            let offset = *base + decoder.taken();
            match frame {
                Frame::Oversize { head, len } => {
                    #[cfg(feature = "logger")]
                    tracing::warn!(
                        offset,
                        line_bytes = len,
                        max_line_bytes = cap,
                        "File transport: a line over max_line_bytes is returned as a dead letter"
                    );
                    Self::count_too_large();
                    pending.push(ReadLine::TooLong(
                        super::filter::FilteredDlqEntry {
                            payload: head,
                            key: None,
                            reason: format!(
                                "line of {len} bytes exceeds max_line_bytes {cap}; the first {cap} \
                                 bytes are kept"
                            ),
                        },
                        FileToken { offset },
                    ));
                }
                Frame::Record(payload) => {
                    let format = PayloadFormat::detect(&payload);
                    let timestamp_ms = crate::clock::epoch_millis();
                    pending.push(ReadLine::Record(Message {
                        key: None,
                        payload,
                        token: FileToken { offset },
                        timestamp_ms: Some(timestamp_ms),
                        format,
                        headers: Vec::new(),
                    }));
                }
            }
        }
        // A call with a smaller `max` than the dropped one leaves the rest queued.
        let rest = pending.split_off(pending.len().min(max));
        let lines = std::mem::replace(pending, rest);
        if let Some(acks) = self.holding() {
            if !lines.is_empty() {
                acks.held
                    .lock()
                    .hand_out(lines.iter().map(ReadLine::offset));
            } else if renamed {
                self.follow_rotation(acks, state).await?;
            }
        }
        Ok(lines)
    }

    /// Move a holding reader at the end of a file rotation renamed onto the
    /// new file at the path, once every line handed out of the old one is
    /// released delivered.
    async fn follow_rotation(&self, acks: &FileAcks, state: &mut ReadState) -> TransportResult<()> {
        let path = Path::new(&self.config.path);
        if !acks.held.lock().lines.is_empty() {
            return Ok(());
        }
        let file = match tokio::fs::File::open(path).await {
            Ok(file) => file,
            // Renamed away and not yet created again.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(TransportError::Connection(format!(
                    "failed to open '{}' for reading: {e}",
                    self.config.path
                )));
            }
        };
        let opened = FileId::of(&file.metadata().await.map_err(|e| {
            TransportError::Recv(format!("failed to stat '{}': {e}", self.config.path))
        })?);
        let start = SavedPosition {
            offset: 0,
            file: opened,
        };
        {
            let mut saved = acks.saved.lock().await;
            Self::save_position(path, start).await?;
            *saved = start;
        }
        acks.held.lock().file = opened;
        *state = ReadState::new(file, 0, opened, self.config.max_line_bytes);
        #[cfg(feature = "logger")]
        tracing::info!(
            path = %self.config.path,
            "File transport: the rotated file is delivered, reading the new one"
        );
        Ok(())
    }

    /// Save `offset` in `file` unless a later position is saved already.
    async fn save_in_order(
        &self,
        acks: &FileAcks,
        file: Option<FileId>,
        offset: u64,
    ) -> TransportResult<()> {
        let mut saved = acks.saved.lock().await;
        if saved.file != file || offset <= saved.offset {
            return Ok(());
        }
        let position = SavedPosition { offset, file };
        Self::save_position(Path::new(&self.config.path), position).await?;
        *saved = position;
        #[cfg(feature = "logger")]
        tracing::debug!(offset, "File transport: position committed");
        Ok(())
    }

    /// Release `tokens` in file order: the saved position moves past every
    /// line from the front that is delivered, and never past one that is not.
    ///
    /// An undelivered release does its work before its first await.
    async fn release_in_order(
        &self,
        acks: &FileAcks,
        tokens: &[FileToken],
        delivered: bool,
    ) -> TransportResult<()> {
        let (file, to) = {
            let mut held = acks.held.lock();
            let to = held.release(tokens.iter().map(|t| t.offset), delivered);
            (held.file, to)
        };
        match to {
            Some(offset) => self.save_in_order(acks, file, offset).await,
            None => Ok(()),
        }
    }
}

impl TransportBase for FileTransport {
    async fn close(&self) -> TransportResult<()> {
        self.lifecycle.close();

        // Sync a durable writer, then drop it.
        if let Some(writer) = self.writer.lock().await.take() {
            let synced = tokio::task::spawn_blocking(move || writer.flush()).await;
            #[cfg(feature = "logger")]
            if let Ok(Err(e)) = synced {
                tracing::warn!(error = %e, path = %self.config.path, "File transport: sync on close failed");
            }
            #[cfg(not(feature = "logger"))]
            drop(synced);
        }

        // Drop reader
        let _ = self.reader.lock().await.take();

        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.lifecycle.is_healthy()
    }

    fn name(&self) -> &'static str {
        Self::TRANSPORT
    }

    fn delivery_guarantee(&self) -> super::DeliveryGuarantee {
        super::DeliveryGuarantee::Written
    }
}

impl Meter for FileTransport {
    const TRANSPORT: &'static str = "file";
}

impl TransportSender for FileTransport {
    /// A record an outbound `dlq` filter matches.
    fn dead_letter_reason(
        &self,
        record: &super::work_batch::Record,
    ) -> Option<super::ack::DeadLetterReason> {
        self.filter_engine
            .outbound_dead_letter(&record.payload)
            .then_some(super::ack::DeadLetterReason::OutboundFilter)
    }

    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        if self.lifecycle.is_closed() {
            return SendResult::Fatal(TransportError::Closed);
        }

        if let Some(settled) = self.filter_engine.outbound_one(&payload) {
            return settled;
        }

        let mut line = Vec::with_capacity(payload.len() + 1);
        let _ = Lf.encode(&[&payload], &mut line);
        self.write_block(line, 1, payload.len() as u64).await
    }

    /// Writes the records the outbound filters pass as one block. A record
    /// they dead-letter leaves the rest to be written and makes the answer
    /// `FilteredDlq`.
    async fn send_batch(&self, records: &[super::work_batch::Record]) -> SendResult {
        if self.lifecycle.is_closed() {
            return SendResult::Fatal(TransportError::Closed);
        }
        let (passed, handled) = self
            .filter_engine
            .partition_outbound(records.iter().map(|r| r.payload.as_ref()), |p| p);
        if !passed.is_empty() {
            let payload_bytes: usize = passed.iter().map(|p| p.len()).sum();
            let mut block = Vec::with_capacity(payload_bytes + passed.len());
            let _ = Lf.encode(&passed, &mut block);
            let written = self
                .write_block(block, passed.len() as u64, payload_bytes as u64)
                .await;
            if !written.is_ok() {
                return written;
            }
        }
        handled
    }
}

impl TransportReceiver for FileTransport {
    type Token = FileToken;

    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
        if self.lifecycle.is_closed() {
            return Err(TransportError::Closed);
        }

        self.ensure_reader().await?;

        let mut guard = self.reader.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| TransportError::Internal("reader not initialised".into()))?;

        let wait_until = (self.config.recv_timeout_ms > 0 && max > 0).then(|| {
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(self.config.recv_timeout_ms)
        });
        let lines = loop {
            let lines = self.read_pass(state, max).await?;
            let now = tokio::time::Instant::now();
            match wait_until {
                Some(until) if lines.is_empty() && now < until && !self.lifecycle.is_closed() => {
                    tokio::time::sleep(EOF_POLL.min(until - now)).await;
                }
                _ => break lines,
            }
        };
        drop(guard);

        let mut messages = Vec::with_capacity(lines.len());
        let mut too_long = Vec::new();
        let mut too_long_tokens = Vec::new();
        for read in lines {
            match read {
                ReadLine::Record(message) => messages.push(message),
                ReadLine::TooLong(entry, token) => {
                    too_long.push(entry);
                    too_long_tokens.push(token);
                }
            }
        }

        // Apply inbound filters via the shared partition helper. DLQ entries
        // are returned in the RecvBatch for the caller to route onward.
        let batch = self.filter_engine.partition_batch(
            messages,
            |m| m.payload.as_ref(),
            |m| m.key.clone(),
            |m| m.token,
        );
        let messages = batch.messages;
        let mut dlq_entries = batch.dlq_entries;
        dlq_entries.extend(too_long);
        let mut filtered_tokens = batch.filtered_tokens;
        filtered_tokens.extend(too_long_tokens);

        #[cfg(feature = "logger")]
        if !messages.is_empty() {
            tracing::debug!(lines = messages.len(), "File transport: batch received");
        }

        Self::count_received_messages(&messages);

        Ok(RecvBatch {
            messages,
            dlq_entries,
            filtered_tokens,
        }
        .into())
    }

    /// Save the read position past `tokens`. With acknowledgements on, this
    /// releases them delivered, in file order.
    async fn commit(&self, tokens: &[Self::Token]) -> TransportResult<()> {
        if let Some(acks) = self.holding() {
            return self.release_in_order(acks, tokens, true).await;
        }
        if let Some(max_token) = tokens.iter().max_by_key(|t| t.offset) {
            let path = Path::new(&self.config.path);
            let position = SavedPosition {
                offset: max_token.offset,
                file: None,
            };
            Self::save_position(path, position).await?;

            #[cfg(feature = "logger")]
            tracing::debug!(
                offset = max_token.offset,
                "File transport: position committed"
            );
        }
        Ok(())
    }

    /// The acknowledgement controls, present once an `acknowledgements`
    /// section was applied.
    fn ack_control(&self) -> Option<&dyn AckControl> {
        self.acks.as_ref().map(|a| a as &dyn AckControl)
    }

    /// With acknowledgements on, move the saved position past every line from
    /// the front released delivered, dropped or dead-lettered, and never past
    /// one released `Errored`. Otherwise commit as `commit` does.
    async fn release(
        &self,
        tokens: &[Self::Token],
        outcome: DeliveryStatus,
    ) -> TransportResult<()> {
        match self.holding() {
            Some(acks) => {
                self.release_in_order(acks, tokens, outcome.should_commit())
                    .await
            }
            None if outcome.should_commit() => self.commit(tokens).await,
            None => Ok(()),
        }
    }
}

impl AcknowledgingReceiver for FileTransport {
    fn acknowledgements(&self) -> AcknowledgementsConfig {
        self.acks
            .as_ref()
            .map_or(AcknowledgementsConfig::new(false), |a| a.config)
    }
}

impl super::traits::FromCascade for FileTransportConfig {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::Codec;
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

    /// A reader of `filename` with acknowledgements on.
    async fn holding(dir: &TempDir, filename: &str) -> FileTransport {
        make_transport(dir, filename)
            .await
            .with_acknowledgements(AcknowledgementsConfig::new(true))
    }

    /// The offset the sidecar of `filename` holds.
    fn saved(dir: &TempDir, filename: &str) -> u64 {
        let pos = dir.path().join(format!("{filename}.pos"));
        SavedPosition::parse(&std::fs::read_to_string(pos).unwrap_or_default()).offset
    }

    /// Lines `0\n` to `{n-1}\n`, as one body.
    fn numbered(n: usize) -> String {
        (0..n).flat_map(|i| [i.to_string(), "\n".into()]).collect()
    }

    /// The payloads of `batch`, as text.
    fn texts(batch: &WorkBatch<FileToken>) -> Vec<String> {
        batch
            .records
            .iter()
            .map(|r| String::from_utf8_lossy(&r.payload).into_owned())
            .collect()
    }

    #[test]
    fn a_saved_position_reads_back_with_or_without_its_file() {
        let named = SavedPosition {
            offset: 42,
            file: Some(FileId { dev: 7, ino: 9 }),
        };
        assert_eq!(named.render(), "42 7:9");
        assert_eq!(SavedPosition::parse(&named.render()), named);
        for (content, offset) in [("42", 42), ("42\n", 42), ("", 0), ("x 1:2", 0)] {
            let parsed = SavedPosition::parse(content);
            assert_eq!(parsed.offset, offset, "{content:?}");
        }
        assert_eq!(SavedPosition::parse("5 bad").file, None);
    }

    /// With acknowledgements on, the saved position moves past a line only
    /// once it and every line before it is released delivered: a later block
    /// released first waits, and an `Errored` block holds it until delivered.
    #[tokio::test]
    async fn the_position_moves_only_past_lines_delivered_in_order() {
        use DeliveryStatus as S;
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("in.ndjson"), numbered(6)).unwrap();
        let reader = holding(&dir, "in.ndjson").await;
        let mut blocks = Vec::new();
        for _ in 0..3 {
            blocks.push(reader.recv(2).await.unwrap().commit_tokens);
        }
        // (block, status, saved position after it): each line is 2 bytes.
        let steps = [
            (2, S::Delivered, 0),
            (0, S::Errored, 0),
            (1, S::Dropped, 0),
            (0, S::Rejected, 12),
        ];
        for (block, status, want) in steps {
            reader.release(&blocks[block], status).await.unwrap();
            assert_eq!(saved(&dir, "in.ndjson"), want, "block {block} {status:?}");
        }
        let control = reader.ack_control().expect("section applied");
        assert_eq!((control.enabled(), control.held().count), (true, 0));
        assert_eq!(control.kind(), AckKind::Pull);

        // Without the section the highest offset released is saved, as before.
        std::fs::write(dir.path().join("plain.ndjson"), numbered(4)).unwrap();
        let plain = make_transport(&dir, "plain.ndjson").await;
        let first = plain.recv(2).await.unwrap().commit_tokens;
        let second = plain.recv(2).await.unwrap().commit_tokens;
        plain.release(&second, S::Delivered).await.unwrap();
        plain.release(&first, S::Errored).await.unwrap();
        assert_eq!(saved(&dir, "plain.ndjson"), 8);
        assert!(plain.ack_control().is_none());
    }

    /// An `Errored` release does its work before its first await, so a block
    /// dropped mid-way is withheld from a drop that polls once.
    #[tokio::test]
    async fn an_errored_release_finishes_in_one_poll() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("in.ndjson"), numbered(2)).unwrap();
        let reader = holding(&dir, "in.ndjson").await;
        let tokens = reader.recv(2).await.unwrap().commit_tokens;
        let release = std::pin::pin!(reader.release(&tokens, DeliveryStatus::Errored));
        let polled = release.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
        assert!(matches!(polled, std::task::Poll::Ready(Ok(()))));
        assert_eq!(reader.ack_control().unwrap().held().count, 2);
    }

    /// A rotation renames the file under the reader: it finishes the renamed
    /// file, reads nothing new until that file's last line is released, then
    /// saves 0 in the new file and reads it from its start.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotation_is_followed_once_the_renamed_file_is_delivered() {
        let dir = TempDir::new().unwrap();
        let live = dir.path().join("in.ndjson");
        std::fs::write(&live, "a\nb\n").unwrap();
        let reader = holding(&dir, "in.ndjson").await;
        let first = reader.recv(1).await.unwrap();
        std::fs::rename(&live, dir.path().join("in.ndjson.1")).unwrap();
        std::fs::write(&live, "c\n").unwrap();

        let tail = reader.recv(10).await.unwrap();
        assert_eq!(texts(&tail), ["b"], "the renamed file is finished first");
        reader
            .release(&tail.commit_tokens, DeliveryStatus::Delivered)
            .await
            .unwrap();
        assert!(reader.recv(10).await.unwrap().is_empty(), "line a is out");
        assert_eq!(saved(&dir, "in.ndjson"), 0, "nothing saved past line a");

        reader
            .release(&first.commit_tokens, DeliveryStatus::Delivered)
            .await
            .unwrap();
        let pos = dir.path().join("in.ndjson.pos");
        assert_eq!(
            std::fs::read_to_string(&pos).unwrap().split(' ').next(),
            Some("4")
        );
        // The waiting recv moves to the new file and reads it in one call.
        let new = reader.recv(10).await.unwrap();
        assert_eq!(texts(&new), ["c"]);
        assert_eq!(saved(&dir, "in.ndjson"), 0, "0 in the new file");
        reader
            .release(&new.commit_tokens, DeliveryStatus::Delivered)
            .await
            .unwrap();
        assert_eq!(
            SavedPosition::parse(&std::fs::read_to_string(&pos).unwrap()).file,
            FileId::at(&live).await,
            "the sidecar names the new file"
        );
    }

    /// A restart whose saved position names a rotated file reads that file's
    /// tail by inode first, then the live file, also when the rotation came
    /// before any release. With the rotated file gone, it reads the live file
    /// from its start.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_restart_finds_the_rotated_file_by_inode() {
        let rows: [(bool, bool, &[&str]); 3] = [
            (true, true, &["b", "c"]),
            (true, false, &["a", "b", "c"]),
            (false, true, &["c"]),
        ];
        for (keep, released, want) in rows {
            let dir = TempDir::new().unwrap();
            let live = dir.path().join("in.ndjson");
            std::fs::write(&live, "a\nb\n").unwrap();
            let first = holding(&dir, "in.ndjson").await;
            let batch = first.recv(1).await.unwrap();
            if released {
                first
                    .release(&batch.commit_tokens, DeliveryStatus::Delivered)
                    .await
                    .unwrap();
            }
            drop(first);
            let rotated = dir.path().join("in.ndjson.20260101T000000");
            std::fs::rename(&live, &rotated).unwrap();
            std::fs::write(&live, "c\n").unwrap();
            if !keep {
                std::fs::remove_file(&rotated).unwrap();
            }

            let restarted = holding(&dir, "in.ndjson").await;
            let mut got = Vec::new();
            for _ in 0..4 {
                let batch = restarted.recv(10).await.unwrap();
                got.extend(texts(&batch));
                restarted
                    .release(&batch.commit_tokens, DeliveryStatus::Delivered)
                    .await
                    .unwrap();
            }
            assert_eq!(got, want, "kept {keep}, released {released}");
        }
    }

    /// A closed and dropped transport leaves the health registry, so a
    /// replacement is not held unready by the one it replaced.
    #[cfg(feature = "health")]
    #[tokio::test]
    async fn a_retired_transport_leaves_the_health_registry() {
        let dir = TempDir::new().unwrap();
        let transport = make_transport(&dir, "health.ndjson").await;
        let id = transport.lifecycle.registration_id();
        assert!(crate::health::HealthRegistry::is_registered(id));

        transport.close().await.unwrap();
        drop(transport);
        assert!(
            !crate::health::HealthRegistry::is_registered(id),
            "the closed transport still reports to readiness"
        );
    }

    /// A line longer than the cap is not held in memory whole: it comes back
    /// as a dead letter carrying the first `max_line_bytes`, its offset is
    /// committed with the block, and the lines either side are records.
    #[tokio::test]
    async fn a_line_over_the_cap_is_dead_lettered_not_buffered() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("long.ndjson");
        let cap = FileTransportConfig::default().max_line_bytes;
        let long = vec![b'x'; cap + 4096];
        let mut body = b"{\"before\":1}\n".to_vec();
        body.extend_from_slice(&long);
        body.extend_from_slice(b"\n{\"after\":1}\n");
        std::fs::write(&path, &body).unwrap();

        let transport = FileTransport::new(&FileTransportConfig {
            path: path.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        let batch = transport.recv(10).await.unwrap();

        let lengths: Vec<usize> = batch.records.iter().map(|r| r.payload.len()).collect();
        assert_eq!(
            lengths,
            [12, 11],
            "record lengths: only the short lines are records"
        );
        assert_eq!(batch.records[0].payload.as_ref(), b"{\"before\":1}");
        assert_eq!(batch.records[1].payload.as_ref(), b"{\"after\":1}");
        assert_eq!(batch.dlq_entries.len(), 1, "the long line is a dead letter");
        assert_eq!(
            batch.dlq_entries[0].payload.len(),
            cap,
            "only its head is kept"
        );
        assert_eq!(
            batch.commit_tokens.iter().map(|t| t.offset).max(),
            Some(body.len() as u64),
            "the block commits past every line it read"
        );
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

    /// Each payload is written followed by a newline, added even when it ends in one.
    #[tokio::test]
    async fn send_writes_each_payload_then_a_newline() {
        let dir = TempDir::new().unwrap();
        let transport = make_transport(&dir, "sent.ndjson").await;
        for payload in [&b"a"[..], b"b\n", b""] {
            let sent = transport
                .send("ignored", bytes::Bytes::copy_from_slice(payload))
                .await;
            assert!(sent.is_ok(), "{sent:?}");
        }
        transport.close().await.unwrap();
        let written = std::fs::read(dir.path().join("sent.ndjson")).unwrap();
        assert_eq!(written, b"a\nb\n\n\n");
    }

    /// A line loses its newline and every carriage return before it, a blank
    /// line is no record but its bytes count toward the next offset, and a
    /// line with no newline yet is held.
    #[tokio::test]
    async fn recv_strips_line_endings_and_skips_blank_lines() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("endings.ndjson"),
            b"a\r\n\r\n\nb\r\r\n c \nlast",
        )
        .unwrap();
        let transport = make_transport(&dir, "endings.ndjson").await;
        let batch = transport.recv(10).await.unwrap();
        let payloads: Vec<&[u8]> = batch.records.iter().map(|r| r.payload.as_ref()).collect();
        assert_eq!(payloads, [&b"a"[..], b"b", b" c "]);
        let offsets: Vec<u64> = batch.commit_tokens.iter().map(|t| t.offset).collect();
        assert_eq!(offsets, [3, 10, 14]);
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

    /// A reader of `filename` whose `recv` waits `recv_timeout_ms` at the end.
    async fn waiting(dir: &TempDir, filename: &str, recv_timeout_ms: u64) -> FileTransport {
        FileTransport::new(&FileTransportConfig {
            path: dir.path().join(filename).display().to_string(),
            recv_timeout_ms,
            ..Default::default()
        })
        .await
        .unwrap()
    }

    /// At the end of the file `recv` waits up to `recv_timeout_ms` for a line,
    /// so a loop over it receives a handful of times in 600 ms rather than
    /// thousands, and returns a line appended while it waits. 0 returns empty
    /// at once.
    #[tokio::test]
    async fn recv_at_the_end_of_the_file_waits_for_a_line() {
        use std::time::{Duration, Instant};
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("eof.ndjson");
        std::fs::write(&path, "a\n").unwrap();
        // (recv_timeout_ms, fewest and most receives in 600 ms)
        for (timeout, fewest, most) in [(200, 2, 4), (0, 100, usize::MAX)] {
            let reader = waiting(&dir, "eof.ndjson", timeout).await;
            assert_eq!(reader.recv(10).await.unwrap().records.len(), 1);
            let started = Instant::now();
            let mut receives = 0;
            while started.elapsed() < Duration::from_millis(600) {
                assert!(reader.recv(10).await.unwrap().is_empty());
                receives += 1;
            }
            assert!(
                (fewest..=most).contains(&receives),
                "timeout {timeout}: {receives} receives in 600 ms at the end of the file"
            );
        }

        let reader = waiting(&dir, "eof.ndjson", 5_000).await;
        assert_eq!(reader.recv(10).await.unwrap().records.len(), 1);
        let appender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            std::io::Write::write_all(&mut file, b"b\n").unwrap();
        });
        let started = Instant::now();
        let batch = reader.recv(10).await.unwrap();
        assert_eq!(texts(&batch), ["b"], "the appended line");
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "returned on the line, not the timeout"
        );
        appender.await.unwrap();
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

    fn record(payload: &'static [u8]) -> crate::transport::work_batch::Record {
        crate::transport::work_batch::Record {
            payload: bytes::Bytes::from_static(payload),
            key: None,
            headers: Vec::new(),
            metadata: crate::transport::work_batch::RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::detect(payload),
            },
        }
    }

    /// A block goes out as one write, and the writer's keys rotate it by size
    /// and keep `max_files` rotated files.
    #[tokio::test]
    async fn send_batch_writes_a_block_and_rotates_by_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("out").join("sent.ndjson");
        let transport = FileTransport::new(&FileTransportConfig {
            path: path.display().to_string(),
            write: FileWriteOptions {
                max_bytes: Some(4),
                max_files: Some(1),
                durable: true,
                ..FileWriteOptions::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();

        for block in [
            [record(b"a"), record(b"b")],
            [record(b"c"), record(b"d")],
            [record(b"e"), record(b"f")],
        ] {
            let sent = transport.send_batch(&block).await;
            assert!(sent.is_ok(), "{sent:?}");
        }
        transport.close().await.unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"e\nf\n");
        let rotated: Vec<String> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n != "sent.ndjson")
            .collect();
        assert_eq!(rotated.len(), 1, "max_files keeps one: {rotated:?}");
        let kept = std::fs::read(path.parent().unwrap().join(&rotated[0])).unwrap();
        assert_eq!(kept, b"c\nd\n", "the newest rotated block");
    }

    /// The crash-cut fixture through the transport: a sent file left ending
    /// mid-line is cut back before the next record, so the two never join and
    /// a reader gets both whole lines.
    #[tokio::test]
    async fn a_half_written_line_is_cut_before_the_next_send() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("crash.ndjson");
        std::fs::write(&path, b"{\"first\":1}\n{\"half\":").unwrap();
        let sender = make_transport(&dir, "crash.ndjson").await;
        assert!(
            sender
                .send("k", bytes::Bytes::from_static(b"{\"next\":2}"))
                .await
                .is_ok()
        );
        sender.close().await.unwrap();

        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"{\"first\":1}\n{\"next\":2}\n"
        );
        let reader = make_transport(&dir, "crash.ndjson").await;
        let batch = reader.recv(10).await.unwrap();
        let payloads: Vec<&[u8]> = batch.records.iter().map(|r| r.payload.as_ref()).collect();
        assert_eq!(payloads, [&b"{\"first\":1}"[..], b"{\"next\":2}"]);
    }

    /// `append: false` empties the file at the first send, and `true` keeps it.
    #[tokio::test]
    async fn append_false_empties_the_file_first() {
        for (append, want) in [(true, &b"old\nnew\n"[..]), (false, b"new\n")] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("f.ndjson");
            std::fs::write(&path, b"old\n").unwrap();
            let transport = FileTransport::new(&FileTransportConfig {
                path: path.display().to_string(),
                append,
                ..Default::default()
            })
            .await
            .unwrap();
            assert!(
                transport
                    .send("k", bytes::Bytes::from_static(b"new"))
                    .await
                    .is_ok()
            );
            assert_eq!(std::fs::read(&path).unwrap(), want, "append {append}");
        }
    }

    /// Outbound filters apply per record in a block: a dropped record is not
    /// written, a dead-lettered one makes the answer `FilteredDlq`, and the
    /// rest are written.
    #[tokio::test]
    async fn send_batch_applies_outbound_filters_per_record() {
        use crate::transport::filter::{FilterAction, FilterRule};
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("filtered.ndjson");
        let rules = vec![
            FilterRule {
                expression: r#"kind == "noise""#.into(),
                action: FilterAction::Drop,
            },
            FilterRule {
                expression: r#"kind == "bad""#.into(),
                action: FilterAction::Dlq,
            },
        ];
        let transport = FileTransport::new(&FileTransportConfig {
            path: path.display().to_string(),
            filters_out: rules,
            ..Default::default()
        })
        .await
        .unwrap();
        let block = [
            record(br#"{"kind":"ok","n":1}"#),
            record(br#"{"kind":"noise"}"#),
            record(br#"{"kind":"bad"}"#),
            record(br#"{"kind":"ok","n":2}"#),
        ];
        assert!(matches!(
            transport.send_batch(&block).await,
            SendResult::FilteredDlq
        ));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"kind\":\"ok\",\"n\":1}\n{\"kind\":\"ok\",\"n\":2}\n"
        );
    }

    #[tokio::test]
    async fn a_path_naming_no_file_is_a_config_error() {
        for path in ["..", "/"] {
            let result = FileTransport::new(&FileTransportConfig {
                path: path.into(),
                ..Default::default()
            })
            .await;
            assert!(matches!(result, Err(TransportError::Config(_))), "{path}");
        }
    }

    /// Every writer key reaches the transport config through the cascade, from
    /// YAML and from the environment, which wins.
    #[cfg(feature = "config")]
    #[test]
    fn writer_keys_reach_the_config_through_the_cascade() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "transport:\n  file:\n    path: /var/log/app/out.ndjson\n    rotation: daily\n    \
             max_age_days: 3\n    compress_rotated: {codec: zstd}\n    max_bytes: 1048576\n    \
             max_files: 4\n    naming: stamped\n    file_mode: \"0640\"\n",
        )
        .unwrap();
        temp_env::with_vars(
            [
                ("FILEKEYS__TRANSPORT__FILE__DURABLE", Some("true")),
                ("FILEKEYS__TRANSPORT__FILE__MAX_FILES", Some("9")),
                ("FILEKEYS__TRANSPORT__FILE__NAMING", Some("numbered")),
                ("FILEKEYS__TRANSPORT__FILE__DIR_MODE", Some("0750")),
            ],
            || {
                crate::config::setup(crate::config::ConfigOptions {
                    env_prefix: "FILEKEYS".to_string(),
                    config_paths: vec![dir.path().to_path_buf()],
                    search_standard_paths: false,
                    load_dotenv: false,
                    ..crate::config::ConfigOptions::default()
                })
                .unwrap();
            },
        );
        let config = FileTransportConfig::from_cascade().unwrap();
        assert_eq!(config.rotation, RotationPeriod::Daily);
        assert_eq!(config.max_age_days, Some(3));
        assert_eq!(config.compress_rotated, Compression::new(Codec::Zstd));
        assert_eq!(config.write.max_bytes, Some(1_048_576));
        assert_eq!(config.write.max_files, Some(9), "env wins");
        assert_eq!(
            config.write.naming,
            crate::io::RotatedNaming::Numbered,
            "env wins"
        );
        assert!(config.write.durable, "from env");
        assert_eq!(
            config.write.file_mode.map(crate::io::FileMode::bits),
            Some(0o640)
        );
        assert_eq!(
            config.write.dir_mode.map(crate::io::FileMode::bits),
            Some(0o750),
            "from env"
        );
    }

    #[test]
    fn writer_keys_default_off_and_private() {
        let config = FileTransportConfig::default();
        assert_eq!(config.rotation, RotationPeriod::Never);
        assert_eq!(config.max_age_days, None);
        assert_eq!(config.compress_rotated, Compression::NONE);
        let lz4 = FileTransportConfig {
            path: "/tmp/out.ndjson".into(),
            compress_rotated: Compression::new(Codec::Lz4),
            ..FileTransportConfig::default()
        };
        assert!(matches!(
            lz4.writer_config(),
            Err(TransportError::Config(_))
        ));
        assert_eq!(config.write, FileWriteOptions::default());

        let numbered = FileTransportConfig {
            path: "/tmp/out.ndjson".into(),
            write: FileWriteOptions {
                naming: crate::io::RotatedNaming::Numbered,
                ..FileWriteOptions::default()
            },
            ..FileTransportConfig::default()
        };
        assert!(numbered.writer_config().is_ok(), "no age limit by default");
        let aged = FileTransportConfig {
            max_age_days: Some(3),
            ..numbered
        };
        assert!(
            matches!(aged.writer_config(), Err(TransportError::Config(e)) if e.contains("max_age_days 3")),
            "numbered refuses an age limit at build"
        );
    }

    /// The sent file and the folders made for it are private by default, and
    /// a set mode is kept.
    #[cfg(unix)]
    #[tokio::test]
    async fn sent_files_are_private_unless_a_mode_is_set() {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let cases = [
            (None, None, 0o600, 0o700),
            (
                crate::io::FileMode::new(0o640).ok(),
                crate::io::FileMode::new(0o750).ok(),
                0o640,
                0o750,
            ),
        ];
        for (file_mode, dir_mode, want_file, want_dir) in cases {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("made").join("sent.ndjson");
            let transport = FileTransport::new(&FileTransportConfig {
                path: path.display().to_string(),
                write: FileWriteOptions {
                    file_mode,
                    dir_mode,
                    ..FileWriteOptions::default()
                },
                ..Default::default()
            })
            .await
            .unwrap();
            assert!(
                transport
                    .send("k", bytes::Bytes::from_static(b"x"))
                    .await
                    .is_ok()
            );
            // A mode with no group or other bits is untouched by any umask.
            if want_file == 0o600 {
                assert_eq!(mode(&path), want_file, "file");
                assert_eq!(mode(path.parent().unwrap()), want_dir, "folder");
            } else {
                assert_eq!(mode(&path) & 0o700, want_file & 0o700, "file");
                assert_eq!(mode(&path) & !want_file, 0, "file adds no bits");
                assert_eq!(
                    mode(path.parent().unwrap()) & !want_dir,
                    0,
                    "folder adds no bits"
                );
            }
        }
    }
}
