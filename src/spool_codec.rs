// Project:   scalo
// File:      src/spool_codec.rs
// Purpose:   Shared on-disk spool helpers (CRC framing, lock recovery, quarantine)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared on-disk spool helpers, used by BOTH the standalone [`Spool`] primitive
//! and the `TieredSink` cold path so the integrity + recovery behaviour is
//! defined once.
//!
//! - **CRC framing** ([`frame`] / [`unframe`]): an optional CRC32C header in
//!   front of each record's on-disk bytes. The underlying queue (yaque) only
//!   parity-checks the LENGTH header, so a torn write / bit-rot in the payload
//!   would otherwise be returned silently. The checksum turns that into a
//!   detectable [`CorruptionError`].
//! - **Open** ([`open_queue`]): clears lock files a stopped process left behind,
//!   refuses a queue a live process still holds, and quarantines a queue that
//!   will not open under [`CorruptionPolicy::Quarantine`].
//! - **Queue files** ([`open_queue`] checks them): a `version/contents` yaque
//!   cannot read, or a `.q` file that is not a segment, would panic yaque, so
//!   they are refused as an error the open policy handles.
//! - **Receive position** ([`open_queue`] repairs it): a `recv-metadata` yaque
//!   left torn or pointing at a segment that is gone becomes a replay from the
//!   first segment, instead of a queue that will not open.
//! - **Sync** ([`sync_segments`]): fsync of the segment files, which yaque never
//!   does, so a record that was pushed survives the host losing power.
//! - **Count** ([`queued_items_and_bytes`]): what a queue directory still
//!   holds, read once at open so the counters after it move incrementally.
//! - **Quarantine** ([`quarantine`]): move a corrupt queue's files into a
//!   timestamped subdirectory of the spool path (forensics preserved, never
//!   deleted). The spool path itself is never renamed, so it works when the
//!   path is a mount point.
//!
//! [`Spool`]: crate::spool::Spool

use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

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

/// Frame `body` with a CRC32C header when `crc` is enabled.
///
/// Layout when `crc`: `[crc32c(body): u32 LE][body]`. The checksum covers
/// exactly the bytes that land on disk, so it detects a torn write or bit-rot
/// the queue's length-only header would miss. When `crc` is off, `body` is
/// returned unchanged (the on-disk format is byte-identical to pre-CRC).
#[must_use]
pub fn frame(crc: bool, body: Vec<u8>) -> Vec<u8> {
    if !crc {
        return body;
    }
    let sum = crc32c::crc32c(&body);
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&sum.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Verify and strip the CRC32C header (inverse of [`frame`]).
///
/// # Errors
/// [`CorruptionError`] if the record is shorter than the 4-byte header or the
/// checksum does not match -- corruption surfaces as an error, never as
/// silently-wrong bytes handed downstream.
pub fn unframe(crc: bool, raw: Vec<u8>) -> Result<Vec<u8>, CorruptionError> {
    if !crc {
        return Ok(raw);
    }
    if raw.len() < 4 {
        return Err(CorruptionError(
            "record shorter than the 4-byte CRC header".to_string(),
        ));
    }
    let (header, body) = raw.split_at(4);
    let expected = u32::from_le_bytes(header.try_into().unwrap_or([0; 4]));
    let actual = crc32c::crc32c(body);
    if expected != actual {
        return Err(CorruptionError(format!(
            "CRC32C mismatch (expected {expected:08x}, computed {actual:08x}) -- torn write or bit-rot"
        )));
    }
    Ok(body.to_vec())
}

/// Lock files yaque keeps in a queue directory, as `(label, path segments)`.
///
/// `version/lock` guards the version check, which spins without a timeout while
/// the file exists, so a leftover one hangs every open.
const LOCK_FILES: [(&str, &[&str]); 3] = [
    ("send", &["send.lock"]),
    ("recv", &["recv.lock"]),
    ("version", &["version", "lock"]),
];

/// Name prefix of the subdirectories [`quarantine`] creates inside a spool path.
const QUARANTINE_PREFIX: &str = "corrupt-";

/// How long an owner may take between creating a lock file and writing it.
///
/// yaque creates the file and writes `pid=..\ntoken=..` straight after, so an
/// empty lock younger than this may belong to an owner that is mid-write.
const LOCK_WRITE_GRACE: Duration = Duration::from_millis(500);

/// What triggered a quarantine, for the counter and the log line.
#[derive(Debug, Clone, Copy)]
pub(crate) enum QuarantineTrigger {
    /// The queue would not open.
    OpenFailed,
    /// A record failed its CRC check on read. Only the standalone spool recovers on read.
    #[cfg(feature = "spool")]
    CrcMismatch,
}

impl QuarantineTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::OpenFailed => "open_failed",
            #[cfg(feature = "spool")]
            Self::CrcMismatch => "crc_mismatch",
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

/// Open the yaque queue at `path` for sending and receiving.
///
/// Lock files left by a process that no longer exists are removed first, using
/// yaque's pid+token check. An empty or unparseable lock counts as stale. A
/// lock a live process holds refuses the open and is never treated as
/// corruption. When the queue still will not open, `policy` decides:
/// [`CorruptionPolicy::Quarantine`] moves its files into a `corrupt-*`
/// subdirectory and opens a fresh queue, [`CorruptionPolicy::Fail`] returns the
/// error. Environmental failures (permission, full or read-only disk) are
/// never quarantined, because moving the queue aside would orphan its records.
///
/// The lock check reads the process table of this pid namespace only, so two
/// processes in different namespaces sharing one volume cannot see each other.
///
/// # Errors
/// [`OpenError::Locked`] for a live owner, [`OpenError::Failed`] otherwise.
pub(crate) fn open_queue(
    path: &Path,
    policy: CorruptionPolicy,
) -> Result<(yaque::Sender, yaque::Receiver), OpenError> {
    release_stale_locks(path)?;
    let validated = validate_queue_files(path);
    if validated.is_ok() {
        repair_recv_position(path).map_err(|e| {
            OpenError::Failed(format!(
                "cannot repair the receive position in {}: {e}",
                path.display()
            ))
        })?;
    }
    let open_err = match validated.and_then(|()| yaque::channel(path)) {
        Ok(queue) => return Ok(queue),
        Err(e) => e,
    };
    // channel() drops what it acquired on failure, so a lock present now is another owner's.
    if let Some(lock) = held_lock(path) {
        return Err(OpenError::Locked(format!(
            "{} appeared while opening: {open_err}",
            lock.display()
        )));
    }
    if is_environmental(open_err.kind()) || policy == CorruptionPolicy::Fail {
        return Err(OpenError::Failed(open_err.to_string()));
    }
    quarantine(path, QuarantineTrigger::OpenFailed, &open_err.to_string())
        .map_err(|e| OpenError::Failed(format!("{open_err}; quarantine failed: {e}")))?;
    validate_queue_files(path)
        .and_then(|()| yaque::channel(path))
        .map_err(|e| OpenError::Failed(format!("{open_err}; fresh open failed: {e}")))
}

/// The versions of yaque a queue's `version/contents` may name. It is the
/// release line `Cargo.toml` pins yaque to, and a test checks that a queue
/// yaque creates satisfies it.
const YAQUE_QUEUE_VERSIONS: &str = "0.6.*";

/// Refuse a queue directory yaque would panic on, as an error the open policy
/// handles like any queue that will not open.
///
/// yaque panics, where it could return an error, on a `version/contents` that
/// does not parse as a version or names another release line, and on a `.q`
/// file whose name is not a segment number. A panic takes the whole process in
/// a service built with `panic = "abort"`, so the files are read here first.
///
/// # Errors
/// An [`io::ErrorKind::InvalidData`] error naming the file, or the [`io::Error`]
/// of a read that failed.
fn validate_queue_files(path: &Path) -> io::Result<()> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let file = entry?.path();
        if file.extension().and_then(|e| e.to_str()) == Some("q")
            && file
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .is_none()
        {
            return Err(invalid_queue(format!(
                "{} is not a numbered segment",
                file.display()
            )));
        }
    }

    let version = path.join("version").join("contents");
    let contents = match std::fs::read(&version) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    // An empty file is a queue yaque has not stamped yet, and it stamps it. A
    // file of only whitespace is not empty to yaque, which parses it as a
    // version and panics.
    if contents.is_empty() {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&contents);
    let text = text.trim();
    let shown: String = text.chars().take(40).collect();
    let named = semver::Version::parse(text).map_err(|e| {
        invalid_queue(format!(
            "{} does not hold a version ({e}): {shown:?}",
            version.display()
        ))
    })?;
    let accepted = semver::VersionReq::parse(YAQUE_QUEUE_VERSIONS).map_err(|e| {
        invalid_queue(format!(
            "{YAQUE_QUEUE_VERSIONS} is not a version range: {e}"
        ))
    })?;
    if !accepted.matches(&named) {
        return Err(invalid_queue(format!(
            "{} names version {named}, and this build reads {YAQUE_QUEUE_VERSIONS}",
            version.display()
        )));
    }
    Ok(())
}

fn invalid_queue(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// The segment numbers in a yaque queue directory, ascending. Segments are
/// `<n>.q`.
fn segment_numbers(path: &Path) -> io::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let file = entry?.path();
        if file.extension().and_then(|e| e.to_str()) == Some("q")
            && let Some(number) = file
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
        {
            segments.push(number);
        }
    }
    segments.sort_unstable();
    Ok(segments)
}

/// The file yaque keeps the receive position in: the segment, then the byte
/// offset in it, as two big-endian u64.
const RECV_STATE_FILE: &str = "recv-metadata";

/// Replace a receive position yaque cannot use with the start of the queue's
/// first segment.
///
/// yaque saves the position by truncating `recv-metadata` and then writing 16
/// bytes, with no sync. A kill between the two leaves it short, and a power loss
/// can leave it naming a segment that is gone or an offset past the end of the
/// segment, the data it counted never having reached the disk. yaque refuses to
/// open such a queue, and the quarantine policy then moves every spooled record
/// aside. Replaying from the first segment instead delivers each record at
/// least once: the records of that segment already consumed come again, and none
/// is lost.
fn repair_recv_position(path: &Path) -> io::Result<()> {
    let state = path.join(RECV_STATE_FILE);
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
        Some(&first) => write_recv_position(path, first)?,
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

/// Whether `saved` is a position yaque can resume from: 16 bytes naming a
/// segment that exists, at an offset inside it.
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

/// Write the receive position as `segment`, offset 0, through a temporary file
/// and a rename, so a crash leaves the old position or the new one.
fn write_recv_position(path: &Path, segment: u64) -> io::Result<()> {
    let temporary = path.join(format!("{RECV_STATE_FILE}.tmp"));
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&segment.to_be_bytes())?;
    file.write_all(&0_u64.to_be_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, path.join(RECV_STATE_FILE))?;
    sync_directory(path)
}

/// Flush a directory's entries to stable storage, so a file created or renamed
/// in it survives a power loss. Windows has no such call, and does nothing.
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Flush the queue's segment files from `from` upward to stable storage, and
/// the queue directory and its parent with them, so a spool created by its
/// first push is not lost with its directory entry. Returns the newest
/// segment, which the next call passes as its `from`: older segments are full
/// and already synced.
///
/// yaque flushes each send to the operating system and never syncs, so a
/// pushed record survives its process being killed, and survives the host
/// losing power only once this has run. Start `from` at 0 to sync everything the
/// directory holds.
///
/// # Errors
/// Propagates the directory read or a segment's sync [`io::Error`].
#[cfg(feature = "spool")]
pub(crate) fn sync_segments(path: &Path, from: u64) -> io::Result<u64> {
    let mut newest = from;
    for number in segment_numbers(path)?.into_iter().filter(|n| *n >= from) {
        // Opened for write, though nothing is written, because Windows cannot
        // flush a handle that cannot write.
        match std::fs::OpenOptions::new()
            .write(true)
            .open(path.join(format!("{number}.q")))
        {
            Ok(file) => file.sync_data()?,
            // The receiver consumed it and yaque deleted it since the listing.
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
        newest = number;
    }
    sync_directory(path)?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    sync_directory(parent)?;
    Ok(newest)
}

/// Remove every lock file whose owner is gone. Refuse when a live owner holds one.
fn release_stale_locks(path: &Path) -> Result<(), OpenError> {
    for (label, segments) in LOCK_FILES {
        let lock = segments.iter().fold(path.to_path_buf(), |p, s| p.join(s));
        match release_one(&lock) {
            Ok(Some(reason)) => record_stale_lock(path, label, reason),
            Ok(None) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Release one lock file, returning why it was stale, or `None` if it was absent.
fn release_one(lock: &Path) -> Result<Option<&'static str>, OpenError> {
    let Some(contents) = read_lock(lock)? else {
        return Ok(None);
    };
    if parse_lock(&contents).is_none() {
        let age = std::fs::metadata(lock)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .unwrap_or(LOCK_WRITE_GRACE);
        if let Some(wait) = LOCK_WRITE_GRACE.checked_sub(age).filter(|w| !w.is_zero()) {
            std::thread::sleep(wait);
            return release_one(lock);
        }
        return match std::fs::remove_file(lock) {
            Ok(()) => Ok(Some("unparseable")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(OpenError::Failed(format!(
                "cannot remove stale lock {}: {e}",
                lock.display()
            ))),
        };
    }
    match yaque::recovery::unlock(lock) {
        Ok(()) => Ok(Some("dead_owner")),
        // yaque reports a live owner, or this process's own lock, as ErrorKind::Other.
        Err(e) if e.kind() == io::ErrorKind::Other => Err(OpenError::Locked(e.to_string())),
        Err(e) => Err(OpenError::Failed(format!(
            "cannot release lock {}: {e}",
            lock.display()
        ))),
    }
}

/// Read a lock file, `None` when it does not exist. Non-UTF-8 reads as unparseable.
fn read_lock(lock: &Path) -> Result<Option<String>, OpenError> {
    match std::fs::read(lock) {
        Ok(bytes) => Ok(Some(String::from_utf8(bytes).unwrap_or_default())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(OpenError::Failed(format!(
            "cannot read lock {}: {e}",
            lock.display()
        ))),
    }
}

/// Parse a yaque lock the way `yaque::recovery::unlock` does, which panics on a
/// lock it cannot parse, so this must reject everything that would.
fn parse_lock(contents: &str) -> Option<(i32, u64)> {
    fn field<'a>(contents: &'a str, key: &str) -> Option<&'a str> {
        let rest = contents.split(key).nth(1)?;
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        Some(&rest[..end])
    }
    // yaque parses the pid as libc::pid_t, which is i32 on every unix target.
    let pid = field(contents, "pid=")?.parse::<i32>().ok()?;
    let token = field(contents, "token=")?.parse::<u64>().ok()?;
    Some((pid, token))
}

/// The first lock file present in `path`, if any.
fn held_lock(path: &Path) -> Option<PathBuf> {
    LOCK_FILES
        .iter()
        .map(|(_, segments)| segments.iter().fold(path.to_path_buf(), |p, s| p.join(s)))
        .find(|lock| lock.exists())
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

/// Count and log a cleared lock so a recovery after a hard kill is never silent.
fn record_stale_lock(path: &Path, lock: &'static str, reason: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("spool_stale_locks_cleared_total", "lock" => lock, "reason" => reason)
        .increment(1);
    #[cfg(feature = "tracing")]
    tracing::warn!(
        path = %path.display(),
        lock,
        reason,
        "cleared a spool lock left by a process that is no longer running"
    );
    let _ = (path, lock, reason);
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

/// Create a uniquely named quarantine subdirectory inside `path`.
fn create_quarantine_dir(path: &Path) -> io::Result<PathBuf> {
    let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S-%9f").to_string();
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

/// Count the records a yaque queue directory still holds and sum their
/// payload bytes, header excluded.
///
/// yaque stores messages as `[4-byte Hamming header][payload]` in `<n>.q`
/// segments, and the receiver position lives in `recv-metadata` (two
/// big-endian u64: segment, position). Counts from the receiver position to the
/// end of the highest segment. Reads every segment, so it runs off the async
/// runtime, once at open.
pub(crate) fn queued_items_and_bytes(path: &Path) -> io::Result<(u64, u64)> {
    if !path.is_dir() {
        return Ok((0, 0));
    }

    let recv_metadata_path = path.join(RECV_STATE_FILE);
    let (recv_segment, recv_position) = if recv_metadata_path.exists() {
        let data = std::fs::read(&recv_metadata_path)?;
        match (data.get(0..8), data.get(8..16)) {
            (Some(segment), Some(position)) => (
                u64::from_be_bytes(segment.try_into().unwrap_or([0; 8])),
                u64::from_be_bytes(position.try_into().unwrap_or([0; 8])),
            ),
            _ => (0, 0),
        }
    } else {
        (0, 0)
    };

    let segments: Vec<u64> = segment_numbers(path)?
        .into_iter()
        .filter(|&seg_num| seg_num >= recv_segment)
        .collect();

    let header_eof: [u8; 4] = [255, 255, 255, 255];
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    for &seg_num in &segments {
        let file_data = std::fs::read(path.join(format!("{seg_num}.q")))?;
        let mut pos = if seg_num == recv_segment {
            usize::try_from(recv_position).unwrap_or(usize::MAX)
        } else {
            0
        };
        while let Some(header) = pos.checked_add(4).and_then(|end| file_data.get(pos..end)) {
            let header: [u8; 4] = header.try_into().unwrap_or([0; 4]);
            if header == header_eof {
                break;
            }
            // Length in the low 26 bits of the Hamming-encoded header.
            let payload_len = (u32::from_be_bytes(header) & 0x03_FF_FF_FF) as usize;
            pos += 4 + payload_len;
            if pos <= file_data.len() {
                count += 1;
                bytes += payload_len as u64;
            }
        }
    }
    Ok((count, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_unframe_round_trips() {
        let body = b"the payload bytes".to_vec();
        let framed = frame(true, body.clone());
        assert_eq!(framed.len(), body.len() + 4, "4-byte CRC header prepended");
        assert_eq!(unframe(true, framed).unwrap(), body);
    }

    #[test]
    fn frame_off_is_identity() {
        let body = b"x".to_vec();
        assert_eq!(frame(false, body.clone()), body);
        assert_eq!(unframe(false, body.clone()).unwrap(), body);
    }

    #[test]
    fn unframe_detects_corruption() {
        let mut framed = frame(true, b"original".to_vec());
        let last = framed.len() - 1;
        framed[last] ^= 0xFF; // flip a payload byte
        assert!(unframe(true, framed).is_err());
        // Too short to hold the header.
        assert!(unframe(true, vec![1, 2]).is_err());
    }

    #[test]
    fn parse_lock_accepts_only_what_yaque_can_parse() {
        assert_eq!(parse_lock("pid=12\ntoken=34\n"), Some((12, 34)));
        // Every shape here would panic yaque's unlock.
        for bad in [
            "",
            "pid=\ntoken=1",
            "pid=1\n",
            "pid=x\ntoken=1",
            "token=1",
            "pid=99999999999\ntoken=1",
            "pid=1\ntoken=99999999999999999999999",
        ] {
            assert_eq!(parse_lock(bad), None, "{bad:?}");
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
        assert!(first.join("0.q").exists());
        assert!(first.join("version").is_dir());
        assert!(!spool.join("0.q").exists());

        std::fs::write(spool.join("1.q"), b"seg").unwrap();
        let second = quarantine(&spool, QuarantineTrigger::OpenFailed, "test")
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

    /// `sync_segments` reports the newest segment, which the next call starts
    /// from, and passes over one the receiver deleted since the listing.
    #[cfg(feature = "spool")]
    #[test]
    fn sync_segments_reports_the_newest_segment() {
        let dir = tempfile::tempdir().unwrap();
        for number in 0..3 {
            std::fs::write(dir.path().join(format!("{number}.q")), b"segment").unwrap();
        }
        std::fs::write(dir.path().join(RECV_STATE_FILE), b"not a segment").unwrap();

        assert_eq!(sync_segments(dir.path(), 0).unwrap(), 2);
        std::fs::remove_file(dir.path().join("1.q")).unwrap();
        assert_eq!(sync_segments(dir.path(), 1).unwrap(), 2);
        assert_eq!(
            sync_segments(dir.path(), 5).unwrap(),
            5,
            "no segment at or above 5, so the start stays"
        );
    }

    /// A queue yaque stamps names a version `YAQUE_QUEUE_VERSIONS` accepts, so
    /// a bump of the yaque pin that leaves the range behind fails here and not
    /// in a service reopening its spool.
    #[test]
    fn a_queue_yaque_creates_passes_the_version_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh");
        drop(yaque::channel(&path).unwrap());
        let stamped = std::fs::read_to_string(path.join("version").join("contents")).unwrap();
        assert!(!stamped.trim().is_empty(), "yaque stamped no version");
        validate_queue_files(&path).unwrap_or_else(|e| panic!("{stamped:?}: {e}"));
    }

    #[test]
    fn a_permission_error_is_not_quarantined() {
        assert!(is_environmental(io::ErrorKind::PermissionDenied));
        assert!(is_environmental(io::ErrorKind::StorageFull));
        assert!(!is_environmental(io::ErrorKind::UnexpectedEof));
        assert!(!is_environmental(io::ErrorKind::InvalidData));
    }
}
