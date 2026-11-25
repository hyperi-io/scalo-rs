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
//! - **Framing** ([`prefix`] / [`unframe`]): each entry's codec, write time,
//!   record count, decoded length and CRC32C in front of its body. The queue
//!   parity-checks only the length header, so the checksum is what turns a
//!   torn write or bit-rot in the body into a [`CorruptionError`]. An entry the
//!   format before wrote, with no count or length, still reads as one record.
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

/// An entry's stored bytes failed their CRC32C check, or are too short to hold its prefix.
#[derive(Debug, Clone)]
pub struct CorruptionError(pub String);

impl std::fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Bytes in front of each entry's body: a tag, a CRC32C, the write time, the
/// record count and the decoded length.
pub(crate) const PREFIX_LEN: usize = 21;

/// Bytes in front of an entry of the format before: a tag, a CRC32C and the
/// write time.
pub(crate) const PREVIOUS_PREFIX_LEN: usize = 13;

/// The tag bit set on an entry that carries its CRC.
const CHECKED: u8 = 0x80;

/// The tag bit set on an entry that carries its record count and decoded
/// length. The format before never sets it.
const COUNTED: u8 = 0x40;

/// The most an entry of the format before may decode to, which names no
/// decoded length: the default drain cap.
pub(crate) const PREVIOUS_DECODE_CAP: usize = 4 << 20;

const _: () = assert!(PREFIX_LEN <= crate::yaque::SHAPE_BYTES);

/// The records an entry holds, and the bytes its body decodes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counted {
    pub(crate) records: u32,
    pub(crate) decoded: u32,
}

/// Milliseconds since the Unix epoch, 0 for a clock set before it.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// The prefix of an entry whose `body` holds `counted` and was compressed with
/// `codec` at `written_ms`: the codec's tag, then with `crc` a CRC32C over the
/// tag, the rest of the prefix and the body, then the time, the record count
/// and the decoded length, each little endian.
#[must_use]
pub(crate) fn prefix(
    codec: Codec,
    crc: bool,
    written_ms: u64,
    counted: Counted,
    body: &[u8],
) -> [u8; PREFIX_LEN] {
    tagged(codec.tag(), crc, written_ms, counted, body)
}

/// [`prefix`] for any codec tag, known to this build or not.
#[must_use]
pub(crate) fn tagged(
    tag: u8,
    crc: bool,
    written_ms: u64,
    counted: Counted,
    body: &[u8],
) -> [u8; PREFIX_LEN] {
    let mut prefix = [0; PREFIX_LEN];
    prefix[0] = tag | COUNTED | if crc { CHECKED } else { 0 };
    prefix[5..13].copy_from_slice(&written_ms.to_le_bytes());
    prefix[13..17].copy_from_slice(&counted.records.to_le_bytes());
    prefix[17..21].copy_from_slice(&counted.decoded.to_le_bytes());
    if crc {
        let sum = checksum(prefix[0], &prefix[5..], body);
        prefix[1..5].copy_from_slice(&sum.to_le_bytes());
    }
    prefix
}

/// The prefix the format before wrote: a tag, a CRC32C and the write time.
#[cfg(test)]
pub(crate) fn previous_prefix(
    codec: Codec,
    crc: bool,
    written_ms: u64,
    body: &[u8],
) -> [u8; PREVIOUS_PREFIX_LEN] {
    let mut prefix = [0; PREVIOUS_PREFIX_LEN];
    prefix[0] = codec.tag() | if crc { CHECKED } else { 0 };
    prefix[5..].copy_from_slice(&written_ms.to_le_bytes());
    if crc {
        let sum = checksum(prefix[0], &prefix[5..], body);
        prefix[1..5].copy_from_slice(&sum.to_le_bytes());
    }
    prefix
}

fn checksum(tag: u8, rest: &[u8], body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c_append(crc32c::crc32c(&[tag]), rest), body)
}

/// What an entry's leading bytes say about it, read without its CRC so the
/// queue can count and reserve before reading the body: one record of unknown
/// decoded length for an entry of the format before.
pub(crate) fn shape(leading: &[u8]) -> crate::yaque::Shape {
    match leading {
        [
            tag,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            r0,
            r1,
            r2,
            r3,
            d0,
            d1,
            d2,
            d3,
            ..,
        ] if tag & COUNTED != 0 => crate::yaque::Shape {
            records: u64::from(u32::from_le_bytes([*r0, *r1, *r2, *r3]).max(1)),
            decoded: Some(u64::from(u32::from_le_bytes([*d0, *d1, *d2, *d3]))),
        },
        _ => crate::yaque::Shape::ONE,
    }
}

/// A stored entry, its prefix read and its CRC checked.
#[derive(Debug)]
pub(crate) struct Entry<'a> {
    /// The codec its body was compressed with, or the tag of one this build does not know.
    pub(crate) codec: Result<Codec, u8>,
    /// Its record count and decoded length, `None` for an entry of the format
    /// before, which holds one record and names no length.
    pub(crate) counted: Option<Counted>,
    pub(crate) body: &'a [u8],
}

/// Why a stored entry did not decode.
#[derive(Debug)]
pub(crate) enum Unreadable {
    /// It failed its CRC, or decodes to more or fewer bytes than its prefix
    /// names. Dropped and counted.
    Damaged(CorruptionError),
    /// It names a codec this build lacks, or does not decompress or decode
    /// under its own. Kept.
    Codec(String),
}

/// When an entry was written, in milliseconds since the Unix epoch, read from
/// its prefix without checking the CRC. 0 for one too short to hold it.
#[cfg(feature = "tiered-sink")]
pub(crate) fn written_ms(raw: &[u8]) -> u64 {
    raw.get(5..PREVIOUS_PREFIX_LEN)
        .and_then(|time| time.try_into().ok())
        .map_or(0, u64::from_le_bytes)
}

/// Reads an entry's prefix and checks its CRC, borrowing the body.
///
/// # Errors
/// [`CorruptionError`] when the entry is shorter than its prefix or fails its
/// CRC, so damaged bytes are never handed on.
pub(crate) fn unframe(raw: &[u8]) -> Result<Entry<'_>, CorruptionError> {
    let wanted = match raw.first() {
        Some(tag) if tag & COUNTED != 0 => PREFIX_LEN,
        _ => PREVIOUS_PREFIX_LEN,
    };
    let Some((prefix, body)) = raw.split_at_checked(wanted) else {
        return Err(CorruptionError(format!(
            "entry of {} bytes is shorter than its {wanted}-byte prefix",
            raw.len()
        )));
    };
    let &[tag, c0, c1, c2, c3, ref rest @ ..] = prefix else {
        return Err(CorruptionError("entry prefix is cut short".into()));
    };
    if tag & CHECKED != 0 {
        let expected = u32::from_le_bytes([c0, c1, c2, c3]);
        let actual = checksum(tag, rest, body);
        if expected != actual {
            return Err(CorruptionError(format!(
                "CRC32C mismatch (stored {expected:08x}, computed {actual:08x})"
            )));
        }
    }
    let counted = match rest {
        [_, _, _, _, _, _, _, _, r0, r1, r2, r3, d0, d1, d2, d3] => Some(Counted {
            records: u32::from_le_bytes([*r0, *r1, *r2, *r3]),
            decoded: u32::from_le_bytes([*d0, *d1, *d2, *d3]),
        }),
        _ => None,
    };
    let codec_tag = tag & !(CHECKED | COUNTED);
    Ok(Entry {
        codec: Codec::from_tag(codec_tag).ok_or(codec_tag),
        counted,
        body,
    })
}

/// Whether the last entry of a queue's newest segment is whole: one that fails
/// its CRC there is a torn write, and the open cuts it off.
pub(crate) fn whole(raw: &[u8]) -> bool {
    unframe(raw).is_ok()
}

/// One decompressor per codec the entries name, each made on first use.
#[derive(Default)]
pub(crate) struct Decoders {
    engines: [Option<crate::compression::Compressor>; Codec::ALL.len()],
}

impl Decoders {
    /// Replaces `out` with `entry`'s body decompressed under its own codec,
    /// to exactly the length its prefix names, or to at most `previous_cap`
    /// bytes for an entry of the format before.
    ///
    /// # Errors
    /// [`Unreadable::Codec`] for a codec this build does not know or lacks, or
    /// a body that does not decompress under it. [`Unreadable::Damaged`] for a
    /// body that decodes past its cap or short of its named length.
    pub(crate) fn decode_into(
        &mut self,
        entry: &Entry<'_>,
        out: &mut Vec<u8>,
        previous_cap: usize,
    ) -> Result<(), Unreadable> {
        let codec = entry.codec.map_err(|tag| {
            Unreadable::Codec(format!(
                "the entry names codec tag {tag}, which this build does not know"
            ))
        })?;
        let slot = &mut self.engines[usize::from(codec.tag())];
        if slot.is_none() {
            *slot = Some(
                crate::compression::Compressor::new(crate::compression::Compression::new(codec))
                    .map_err(|e| {
                        Unreadable::Codec(format!("the entry's codec {codec} cannot run here: {e}"))
                    })?,
            );
        }
        let Some(engine) = slot.as_mut() else {
            return Err(Unreadable::Codec(format!("no {codec} decoder")));
        };
        let named = entry
            .counted
            .map(|counted| usize::try_from(counted.decoded).unwrap_or(usize::MAX));
        let cap = named.unwrap_or(previous_cap);
        match engine.decompress_into(entry.body, out, cap) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::FileTooLarge => {
                return Err(Unreadable::Damaged(CorruptionError(format!(
                    "the entry decodes past its {cap}-byte cap"
                ))));
            }
            Err(e) => {
                return Err(Unreadable::Codec(format!(
                    "the entry does not decompress under its codec {codec}: {e}"
                )));
            }
        }
        match named {
            Some(named) if named != out.len() => {
                Err(Unreadable::Damaged(CorruptionError(format!(
                    "the entry decodes to {} bytes, not the {named} its prefix names",
                    out.len()
                ))))
            }
            _ => Ok(()),
        }
    }
}

/// Name prefix of the subdirectories [`quarantine`] creates inside a spool path.
const QUARANTINE_PREFIX: &str = "corrupt-";

/// What triggered a quarantine, for the counter and the log line.
#[derive(Debug, Clone, Copy)]
pub(crate) enum QuarantineTrigger {
    /// The queue would not open.
    OpenFailed,
    /// A header failed its parity check, so no later entry can be found.
    DamagedHeader,
}

impl QuarantineTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::OpenFailed => "open_failed",
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
    // A queue in another format is not damage: its records wait for the release that wrote them.
    if is_environmental(open_err.kind())
        || open_err.kind() == io::ErrorKind::Unsupported
        || policy == CorruptionPolicy::Fail
    {
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
/// in it survives a power loss. Windows opens the directory for write with
/// backup semantics, the only way it hands out a directory handle, and
/// `sync_all` is `FlushFileBuffers` on it.
#[cfg_attr(
    not(any(unix, windows)),
    allow(clippy::unnecessary_wraps, reason = "only the unix and Windows flushes can fail")
)]
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS.0)
            .open(path)?
            .sync_all()?;
    }
    #[cfg(not(any(unix, windows)))]
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
/// spaces out, or until the queue deletes a consumed segment. The queue's
/// failed write is already cut back, so nothing torn is left.
pub(crate) struct FullDisk {
    store: &'static str,
    failures: u32,
    since: Option<Instant>,
    retry_at: Option<Instant>,
    /// The queue's freed bytes when last asked.
    freed: u64,
    flag: Arc<AtomicBool>,
}

impl FullDisk {
    pub(crate) fn new(store: &'static str) -> Self {
        Self {
            store,
            failures: 0,
            since: None,
            retry_at: None,
            freed: 0,
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set while the disk is full, for a health check.
    #[cfg(any(test, feature = "health", feature = "tiered-sink"))]
    pub(crate) fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    /// Whether a write may be tried now: the backoff has run out, or the
    /// queue has deleted segment bytes since, `freed` being its running total.
    pub(crate) fn admits(&mut self, now: Instant, freed: u64) -> bool {
        if freed != self.freed {
            self.freed = freed;
            self.retry_at = None;
        }
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
    #[cfg(feature = "metrics")]
    damaged: ::metrics::Counter,
    #[cfg(feature = "metrics")]
    oldest_age: ::metrics::Gauge,
    store: &'static str,
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
            #[cfg(feature = "metrics")]
            damaged: ::metrics::counter!("spool_damaged_entries_total", "store" => store),
            #[cfg(feature = "metrics")]
            oldest_age: ::metrics::gauge!("spool_oldest_age_seconds", "store" => store),
            store,
        }
    }

    /// An entry that failed its CRC and was dropped: counted, and logged once.
    pub(crate) fn damaged(&self, error: &CorruptionError) {
        #[cfg(feature = "metrics")]
        self.damaged.increment(1);
        tracing::warn!(store = self.store, error = %error, "dropped a spooled entry that failed its CRC");
    }

    /// The age of the oldest entry held, from its write time, 0 for none.
    #[cfg(feature = "tiered-sink")]
    pub(crate) fn oldest(&self, written_ms: u64) {
        #[cfg(feature = "metrics")]
        self.oldest_age
            .set(age(written_ms).map_or(0.0, |age| age.as_secs_f64()));
        #[cfg(not(feature = "metrics"))]
        let _ = (self, written_ms);
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

/// What waits on a store's syncs: pieces of work that report once a sync
/// covers the entry holding them.
pub(crate) trait Waiters: Send + Sync {
    /// The sender's published end, read under the lock a writer holds from
    /// its write to registering the entry's waiters, so every waiter at or
    /// below it is registered.
    fn end(&self, shared: &yaque::Shared) -> Option<QueueState>;

    /// Reports every waiter at or below `end`: durable when `synced`, failed
    /// when the sync that covered it failed.
    fn settle(&self, end: QueueState, synced: bool);
}

/// What one sync did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Synced {
    /// Flushed what was written up to the end it read.
    Flushed,
    /// Nothing was written since the last sync settled the same end, so
    /// nothing was flushed.
    Unchanged,
}

/// Syncs the queue in `cell` to stable storage on the blocking pool: the
/// segments written since the last sync, the directory when a segment was
/// created since, and the receiver's committed position. A sync with nothing
/// written since the last does no flush. Records how long a flush took in
/// `spool_sync_seconds`, and settles `waiters` against the end it covered. A
/// failed flush is not retried: its waiters fail, and the sender starts a new
/// segment, since a flush retried on the same file can report clean with the
/// data gone.
///
/// # Errors
/// A sync's or the position save's [`io::Error`], or the blocking task's
/// failure as [`io::ErrorKind::Other`].
pub(crate) async fn sync_queue(
    path: PathBuf,
    store: &'static str,
    cell: QueueCell,
    waiters: Option<Arc<dyn Waiters>>,
) -> io::Result<Synced> {
    let Some(shared) = cell.lock().clone() else {
        return Ok(Synced::Unchanged);
    };
    tokio::task::spawn_blocking(move || {
        let end = match &waiters {
            Some(waiters) => waiters.end(&shared),
            None => shared.published(),
        };
        let started = Instant::now();
        let synced = sync_now(&path, &shared, end);
        if matches!(synced, Ok(Synced::Flushed) | Err(_)) {
            #[cfg(feature = "metrics")]
            ::metrics::histogram!("spool_sync_seconds", "store" => store)
                .record(started.elapsed().as_secs_f64());
        }
        let _ = (started, store);
        if synced.is_err() {
            shared.request_roll();
        }
        if let (Some(waiters), Some(end)) = (waiters, end) {
            waiters.settle(end, synced.is_ok());
        }
        synced
    })
    .await
    .map_err(io::Error::other)?
}

/// Flushes the queue up to `end`, read before the open segment so a roll
/// between the two cannot mark an unflushed entry durable. An end the last
/// sync settled is not flushed again, and a failure there is reported again.
fn sync_now(path: &Path, shared: &yaque::Shared, end: Option<QueueState>) -> io::Result<Synced> {
    match (end, shared.settled()) {
        (Some(end), Some((settled, flushed))) if end == settled => {
            if !flushed {
                return Err(io::Error::other(
                    "the spool's last sync failed and nothing was spilled since; the next spill \
                     starts a new segment",
                ));
            }
            shared.save_position()?;
            return Ok(Synced::Unchanged);
        }
        _ => {}
    }
    let flushed = (|| {
        let open = shared
            .open_segment()
            .ok_or_else(|| io::Error::other("the spool has no open segment"))?;
        // A segment deleted under the open handle would sync to a file no restart finds.
        std::fs::metadata(path.join(format!("{}.q", open.0)))?;
        let segment = sync_segments(path, shared.synced(), open)?;
        shared.mark_synced(segment);
        shared.save_position()
    })();
    shared.settle(end, flushed.is_ok());
    flushed.map(|()| Synced::Flushed)
}

/// Syncs a queue every interval, and once more at shutdown, so a power loss
/// loses at most about one interval of writes.
pub(crate) struct SyncTicker {
    pub(crate) path: PathBuf,
    pub(crate) store: &'static str,
    pub(crate) cell: QueueCell,
    pub(crate) waiters: Option<Arc<dyn Waiters>>,
}

impl crate::concurrency::PeriodicTask for SyncTicker {
    async fn tick(&mut self) -> Result<(), crate::concurrency::TickError> {
        sync_queue(
            self.path.clone(),
            self.store,
            Arc::clone(&self.cell),
            self.waiters.clone(),
        )
        .await
        .map(|_| ())
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
    quarantined(trigger);
    tracing::warn!(
        path = %path.display(),
        quarantined = %dest.display(),
        trigger = trigger.label(),
        cause,
        "spool could not be used; moved its files aside and started a fresh queue"
    );
    Ok(Some(dest))
}

/// Counts one quarantine in `spool_quarantined_total{trigger}`.
fn quarantined(trigger: QuarantineTrigger) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("spool_quarantined_total", "trigger" => trigger.label()).increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = trigger;
}

/// Copies the newest segment's bytes from a damaged header at `from` to its
/// end into a new `corrupt-*` subdirectory of its queue, synced, before the
/// open cuts them off. No entry past the header can be found, so they are
/// kept for inspection. Counted as `trigger="damaged_header"`, warned once.
///
/// # Errors
/// The copy's, a sync's or the subdirectory create's [`io::Error`]. The open
/// then fails, and cuts nothing.
pub(crate) fn keep_damaged_tail(segment: &Path, from: u64) -> io::Result<()> {
    use std::io::{Seek as _, SeekFrom};
    let (Some(base), Some(name)) = (segment.parent(), segment.file_name()) else {
        return Err(io::Error::other(format!(
            "{} names no segment in a queue directory",
            segment.display()
        )));
    };
    let dest = create_quarantine_dir(base)?;
    let mut source = std::fs::File::open(segment)?;
    source.seek(SeekFrom::Start(from))?;
    let kept = dest.join(name);
    let mut copy = std::fs::File::create(&kept)?;
    let bytes = io::copy(&mut source, &mut copy)?;
    copy.sync_all()?;
    sync_directory(&dest)?;
    sync_directory(base)?;
    quarantined(QuarantineTrigger::DamagedHeader);
    tracing::warn!(
        segment = %segment.display(),
        offset = from,
        bytes,
        quarantined = %kept.display(),
        "a header in the newest spool segment is damaged; moved the segment from it aside"
    );
    Ok(())
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

/// How long ago `written_ms` was, `None` for 0, which stands for no entry.
#[cfg(feature = "tiered-sink")]
pub(crate) fn age(written_ms: u64) -> Option<Duration> {
    (written_ms > 0).then(|| Duration::from_millis(now_ms().saturating_sub(written_ms)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEVEN: Counted = Counted {
        records: 7,
        decoded: 17,
    };

    /// An entry reads back with its codec, time, count and decoded length, and
    /// a flip of any byte its CRC covers is caught. Without the CRC only the
    /// length is checked. An entry of the format before reads as one record
    /// that names no length.
    #[test]
    fn framing_round_trips_and_catches_damage() {
        let body = b"the payload bytes";
        for (codec, crc) in [
            (Codec::Zstd, true),
            (Codec::Lz4, true),
            (Codec::None, false),
        ] {
            let framed = [
                &prefix(codec, crc, 1_700_000_000_123, SEVEN, body)[..],
                body,
            ]
            .concat();
            let entry = unframe(&framed).unwrap();
            assert_eq!(entry.codec, Ok(codec), "{codec}");
            assert_eq!(entry.counted, Some(SEVEN), "{codec}");
            assert_eq!(
                shape(&framed),
                crate::yaque::Shape {
                    records: 7,
                    decoded: Some(17)
                },
                "{codec}"
            );
            #[cfg(feature = "tiered-sink")]
            assert_eq!(written_ms(&framed), 1_700_000_000_123, "{codec}");
            assert_eq!(entry.body, body, "{codec}");
            for at in [0, 1, 6, 14, 18, PREFIX_LEN, framed.len() - 1] {
                let mut damaged = framed.clone();
                damaged[at] ^= 0x01;
                // Bit 0 of the tag turns zstd into none and lz4 into gzip, the CRC catches it.
                assert_eq!(
                    unframe(&damaged).is_err(),
                    crc,
                    "{codec}: byte {at} flipped"
                );
            }

            let before = [
                &previous_prefix(codec, crc, 1_700_000_000_123, body)[..],
                body,
            ]
            .concat();
            let entry = unframe(&before).unwrap();
            assert_eq!(
                (entry.codec, entry.counted, entry.body),
                (Ok(codec), None, &body[..]),
                "{codec}: the format before"
            );
            assert_eq!(shape(&before), crate::yaque::Shape::ONE, "{codec}");
            #[cfg(feature = "tiered-sink")]
            assert_eq!(written_ms(&before), 1_700_000_000_123, "{codec}");
            let mut damaged = before.clone();
            damaged[PREVIOUS_PREFIX_LEN] ^= 0x01;
            assert_eq!(
                unframe(&damaged).is_err(),
                crc,
                "{codec}: the format before"
            );
        }
        assert!(unframe(&[1, 2]).is_err(), "too short for the prefix");
        assert!(
            unframe(&[COUNTED; PREVIOUS_PREFIX_LEN + 2]).is_err(),
            "a counted entry too short for its prefix"
        );
        let unknown = [&tagged(0x0F, true, 1, SEVEN, body)[..], body].concat();
        assert_eq!(
            unframe(&unknown).unwrap().codec,
            Err(0x0F),
            "a codec this build does not know"
        );
    }

    fn packed(codec: Codec, record: &[u8]) -> Vec<u8> {
        let mut packed = Vec::new();
        crate::compression::Compressor::new(crate::compression::Compression::new(codec))
            .unwrap()
            .compress_into(record, &mut packed)
            .unwrap();
        packed
    }

    /// Each entry decodes under the codec it names, whatever the one before it used.
    #[test]
    fn each_entry_decodes_under_its_own_codec() {
        let record = b"a record that compresses a record that compresses".repeat(4);
        let counted = Counted {
            records: 1,
            decoded: u32::try_from(record.len()).unwrap(),
        };
        let mut decoders = Decoders::default();
        for codec in [Codec::Zstd, Codec::None, Codec::Lz4, Codec::Zstd] {
            let packed = packed(codec, &record);
            let framed = [&prefix(codec, true, 1, counted, &packed)[..], &packed].concat();
            let mut out = Vec::new();
            decoders
                .decode_into(&unframe(&framed).unwrap(), &mut out, PREVIOUS_DECODE_CAP)
                .unwrap();
            assert_eq!(out, record, "{codec}");
        }
    }

    /// A decode is held to the length the prefix names, and an entry of the
    /// format before to the cap it is given: more, or fewer, is damage, never
    /// a codec it cannot read.
    #[test]
    fn a_decode_is_held_to_its_named_length_or_the_cap() {
        let record = vec![b'x'; 10_000];
        let mut decoders = Decoders::default();
        for codec in [Codec::Zstd, Codec::None] {
            let packed = packed(codec, &record);
            for (case, named, cap, damaged) in [
                ("named exactly", Some(10_000), 1, false),
                ("named short", Some(9_999), usize::MAX, true),
                ("named long", Some(10_001), usize::MAX, true),
                ("before, under the cap", None, 10_000, false),
                ("before, past the cap", None, 9_999, true),
            ] {
                let framed = match named {
                    Some(decoded) => {
                        let counted = Counted {
                            records: 1,
                            decoded,
                        };
                        [&prefix(codec, true, 1, counted, &packed)[..], &packed].concat()
                    }
                    None => [&previous_prefix(codec, true, 1, &packed)[..], &packed].concat(),
                };
                let mut out = Vec::new();
                let decoded = decoders.decode_into(&unframe(&framed).unwrap(), &mut out, cap);
                assert_eq!(
                    matches!(decoded, Err(Unreadable::Damaged(_))),
                    damaged,
                    "{codec} {case}: {decoded:?}"
                );
                if !damaged {
                    assert_eq!(out, record, "{codec} {case}");
                }
            }
        }
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

    /// The open counts a damaged tail it keeps aside as a quarantine, never
    /// as a torn entry, and leaves the queue readable up to it.
    #[cfg(feature = "metrics")]
    #[test]
    fn a_kept_damaged_tail_is_counted_as_a_quarantine() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q");
        let options = yaque::Options {
            segment_bytes: 1 << 20,
            damaged_tail: Some(keep_damaged_tail),
            ..options()
        };
        {
            let (mut sender, _receiver) = yaque::channel(&path, options).unwrap();
            for record in [&b"kept"[..], b"damaged", b"after"] {
                sender.send(&[], record).unwrap();
            }
        }
        let segment = path.join("0.q");
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes[4 + 4] ^= 0x04;
        std::fs::write(&segment, &bytes).unwrap();

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let rendered = ::metrics::with_local_recorder(&recorder, || {
            let (_sender, mut receiver) = yaque::channel(&path, options).unwrap();
            assert_eq!(receiver.read(10, u64::MAX, None).unwrap().entries, 1);
            assert_eq!(receiver.taken()[0], b"kept");
            recorder.handle().render()
        });
        assert!(
            rendered.contains("spool_quarantined_total{trigger=\"damaged_header\"} 1"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("spool_torn_entries_cut_total"),
            "{rendered}"
        );
        assert_eq!(
            std::fs::metadata(&segment).unwrap().len(),
            8,
            "cut after the first entry"
        );
    }

    fn options() -> yaque::Options {
        yaque::Options {
            segment_bytes: 16,
            read_buffer_bytes: 4096,
            recycle_bytes: 1 << 16,
            whole: None,
            damaged_tail: None,
            shape: None,
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

    /// A sync with nothing written since the last flushes nothing, and one
    /// after a write flushes again. A failed flush is not retried on the same
    /// end, only reported again, and a new segment flushes.
    #[test]
    fn a_sync_flushes_only_what_was_written_since_and_never_retries_a_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue");
        let options = yaque::Options {
            segment_bytes: 1 << 20,
            ..options()
        };
        let (mut sender, _receiver) = open_queue(&path, CorruptionPolicy::Fail, options).unwrap();
        let shared = sender.shared();
        let sync = |shared: &yaque::Shared| sync_now(&path, shared, shared.published());
        assert_eq!(sync(&shared).unwrap(), Synced::Flushed, "the first sync");
        assert_eq!(sync(&shared).unwrap(), Synced::Unchanged, "nothing written");
        sender.send(&[], b"one").unwrap();
        assert_eq!(sync(&shared).unwrap(), Synced::Flushed, "after a write");
        assert_eq!(sync(&shared).unwrap(), Synced::Unchanged);

        sender.send(&[], b"two").unwrap();
        std::fs::remove_file(path.join("0.q")).unwrap();
        let failed = sync(&shared).unwrap_err();
        assert_eq!(
            failed.kind(),
            io::ErrorKind::NotFound,
            "gone from under its handle"
        );
        let again = sync(&shared).unwrap_err();
        assert!(
            again.to_string().contains("last sync failed"),
            "reported again, not flushed again: {again}"
        );
        shared.request_roll();
        sender.send(&[], b"three").unwrap();
        assert_eq!(
            sender.shared().open_segment().map(|(segment, _)| segment),
            Some(1),
            "the next write starts a new segment"
        );
        assert_eq!(
            sync(&shared).unwrap(),
            Synced::Flushed,
            "the new segment flushes"
        );
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
        // `/` on unix, a drive's root such as `\\?\C:\` on Windows.
        assert!(
            all.last().is_some_and(|root| root.parent().is_none()),
            "ends at the root: {all:?}"
        );
    }

    /// A directory flushes on every platform with one: Linux and macOS by
    /// `fsync` on the directory, Windows by `FlushFileBuffers` on a handle
    /// opened with backup semantics.
    #[test]
    fn a_directory_flushes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("0.q"), b"segment").unwrap();
        sync_directory(dir.path()).expect("the directory flushes");
        #[cfg(any(unix, windows))]
        assert!(sync_directory(&dir.path().join("absent")).is_err());
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
        assert!(full.admits(now, 0));
        full.failed(now, 10, &full_error());
        assert!(full.flag().load(Ordering::Relaxed), "health flag set");
        assert!(!full.admits(now, 0), "refused inside the backoff");
        assert!(
            full.admits(now + Duration::from_secs(6), 0),
            "retried after it"
        );
        full.failed(now, 10, &full_error());
        assert!(full.admits(now, 4096), "retried once a segment is deleted");
        assert!(
            full.flag().load(Ordering::Relaxed),
            "still full until a write goes through"
        );
        full.wrote(now + Duration::from_secs(6));
        assert!(!full.flag().load(Ordering::Relaxed));
        assert!(full.admits(now, 4096));
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
