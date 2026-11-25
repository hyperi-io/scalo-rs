// Copied from yaque 0.6.6, src/sync.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! What the two halves of a queue share: its locks, the end the sender has
//! published, and the segment reader that stops there.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "spool")]
use std::task::Context;

use futures_util::task::AtomicWaker;
use parking_lot::Mutex;

use super::state::{Persistence, QueueState};

/// An exclusive lock on a file in the queue directory. The operating system
/// holds it while the handle is open and drops it when the process dies, so a
/// killed process never leaves the queue locked. The file itself stays.
pub(crate) struct FileGuard {
    _file: File,
}

impl FileGuard {
    /// Locks `path`, creating it. `Ok(None)` when another handle holds it, in
    /// this process or another.
    ///
    /// # Errors
    ///
    /// The open's or the lock's error.
    pub(crate) fn try_lock(path: &Path) -> io::Result<Option<Self>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(e)) => return Err(e),
        }
        // For a person reading the directory. The lock, not this text, decides.
        file.set_len(0)?;
        writeln!(file, "pid={}", std::process::id())?;
        Ok(Some(Self { _file: file }))
    }
}

/// Where the receiver may read up to in one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Limit {
    /// A segment below the sender's: whole, and it will not grow.
    Sealed,
    /// The sender's segment: whole entries end at this offset, and more may follow.
    Open(u64),
    /// A segment the sender has not opened yet.
    NotYet,
}

/// What the two halves of one queue share: what the sender has published,
/// and the receiver's committed position, for a sync on another thread.
#[derive(Default)]
pub(crate) struct Shared {
    wake: AtomicWaker,
    segment: AtomicU64,
    end: AtomicU64,
    disk: AtomicU64,
    /// Bytes of segment files deleted since the queue opened.
    freed: AtomicU64,
    /// The sender's open segment.
    open: Mutex<Option<(u64, Arc<File>)>>,
    /// The segment open at the last sync, `None` before the first.
    synced: Mutex<Option<u64>>,
    position: Mutex<Position>,
}

/// The receiver's committed position, and the last one saved.
#[derive(Default)]
struct Position {
    committed: QueueState,
    saved: Option<QueueState>,
    persistence: Option<Persistence>,
}

impl Shared {
    /// Records the sender's open segment.
    pub(crate) fn set_open(&self, segment: u64, file: Arc<File>) {
        *self.open.lock() = Some((segment, file));
    }

    /// The sender's open segment and its file.
    pub(crate) fn open_segment(&self) -> Option<(u64, Arc<File>)> {
        self.open.lock().clone()
    }

    /// The segment open at the last sync, `None` before the first.
    pub(crate) fn synced(&self) -> Option<u64> {
        *self.synced.lock()
    }

    /// Records a sync that covered every segment up to `segment`.
    pub(crate) fn mark_synced(&self, segment: u64) {
        let mut synced = self.synced.lock();
        *synced = Some(synced.map_or(segment, |was| was.max(segment)));
    }

    /// Where the receiver saves its position, the position it opened at, and
    /// the one the file holds.
    pub(crate) fn set_persistence(
        &self,
        persistence: Persistence,
        opened_at: QueueState,
        saved: Option<QueueState>,
    ) {
        let mut position = self.position.lock();
        position.committed = opened_at;
        position.saved = saved;
        position.persistence = Some(persistence);
    }

    /// Records the receiver's committed position, for the next save.
    pub(crate) fn commit(&self, state: QueueState) {
        self.position.lock().committed = state;
    }

    /// Saves the committed position if it moved since the last save. Saves
    /// run one at a time, so the file never goes back to an older position.
    ///
    /// # Errors
    ///
    /// The save's error.
    pub(crate) fn save_position(&self) -> io::Result<()> {
        let mut position = self.position.lock();
        let committed = position.committed;
        if position.saved == Some(committed) {
            return Ok(());
        }
        if let Some(persistence) = &position.persistence {
            persistence.save(committed)?;
            position.saved = Some(committed);
        }
        Ok(())
    }

    /// Publishes the end of the last whole entry in `segment`, and wakes the receiver.
    ///
    /// The end is stored before the segment, so a receiver that sees a new
    /// segment never reads the previous one's end against it.
    pub(crate) fn publish(&self, segment: u64, end: u64) {
        self.end.store(end, Ordering::Release);
        self.segment.store(segment, Ordering::Release);
        self.wake.wake();
    }

    /// How far `segment` may be read.
    pub(crate) fn limit(&self, segment: u64) -> Limit {
        // A sender moving segments between the loads is read again. Past a few
        // tries the answer is the safe one: nothing more to read yet.
        for _ in 0..4 {
            let published = self.segment.load(Ordering::Acquire);
            let end = self.end.load(Ordering::Acquire);
            if self.segment.load(Ordering::Acquire) == published {
                return match segment.cmp(&published) {
                    std::cmp::Ordering::Less => Limit::Sealed,
                    std::cmp::Ordering::Equal => Limit::Open(end),
                    std::cmp::Ordering::Greater => Limit::NotYet,
                };
            }
        }
        Limit::NotYet
    }

    /// Registers the task to wake on the next publish.
    #[cfg(feature = "spool")]
    pub(crate) fn register(&self, cx: &Context<'_>) {
        self.wake.register(cx.waker());
    }

    /// Bytes the queue's segment files hold.
    pub(crate) fn disk_bytes(&self) -> u64 {
        self.disk.load(Ordering::Relaxed)
    }

    pub(crate) fn add_disk(&self, bytes: u64) {
        self.disk.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn remove_disk(&self, bytes: u64) {
        let _ = self
            .disk
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                Some(held.saturating_sub(bytes))
            });
        self.freed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Bytes of segment files deleted since the queue opened. It only grows.
    pub(crate) fn freed(&self) -> u64 {
        self.freed.load(Ordering::Relaxed)
    }
}

/// Reads one segment through a buffer that never fills past the receiver's
/// limit, so a torn or partly written entry is never read.
pub(crate) struct Follower {
    path: PathBuf,
    file: Option<File>,
    /// The file length, read once, for a sealed segment.
    sealed_len: Option<u64>,
    buf: Vec<u8>,
    start: usize,
    filled: usize,
    /// The file offset of `buf[filled]`.
    offset: u64,
}

impl Follower {
    /// A reader of the segment at `path` from `position`, with a read-ahead
    /// buffer of `buffer` bytes. The file opens on first read.
    pub(crate) fn new(path: PathBuf, position: u64, buffer: usize) -> Self {
        Self {
            path,
            file: None,
            sealed_len: None,
            buf: vec![0; buffer.max(super::MIN_READ_BUFFER)],
            start: 0,
            filled: 0,
            offset: position,
        }
    }

    /// Points the reader at another segment, keeping its buffer.
    pub(crate) fn retarget(&mut self, path: PathBuf, position: u64) {
        self.path = path;
        self.file = None;
        self.sealed_len = None;
        self.start = 0;
        self.filled = 0;
        self.offset = position;
    }

    /// The offset of the next unread byte.
    pub(crate) fn position(&self) -> u64 {
        self.offset - (self.filled - self.start) as u64
    }

    /// Drops what is buffered and reads next from `position`.
    pub(crate) fn seek(&mut self, position: u64) -> io::Result<()> {
        self.start = 0;
        self.filled = 0;
        self.offset = position;
        if let Some(file) = &mut self.file {
            file.seek(SeekFrom::Start(position))?;
        }
        Ok(())
    }

    /// The offset reads may reach under `limit`: the published end for the
    /// sender's segment, the file length for a sealed one.
    pub(crate) fn end(&mut self, limit: Limit) -> io::Result<u64> {
        match limit {
            Limit::NotYet => Ok(self.position()),
            Limit::Open(end) => Ok(end),
            Limit::Sealed => {
                if let Some(len) = self.sealed_len {
                    return Ok(len);
                }
                let len = self.file()?.metadata()?.len();
                self.sealed_len = Some(len);
                Ok(len)
            }
        }
    }

    /// The next 4 bytes, without consuming them. The caller has checked that
    /// they lie before the end.
    pub(crate) fn peek4(&mut self, end: u64) -> io::Result<[u8; 4]> {
        while self.filled - self.start < 4 {
            self.refill(end)?;
        }
        let mut word = [0_u8; 4];
        word.copy_from_slice(&self.buf[self.start..self.start + 4]);
        Ok(word)
    }

    /// Skips `n` buffered bytes, which [`peek4`](Self::peek4) returned.
    pub(crate) fn consume(&mut self, n: usize) {
        self.start += n.min(self.filled - self.start);
    }

    /// Fills `out` from the buffer, then from the file. The caller has checked
    /// that every byte lies before the end.
    pub(crate) fn read_into(&mut self, out: &mut [u8]) -> io::Result<()> {
        let buffered = (self.filled - self.start).min(out.len());
        out[..buffered].copy_from_slice(&self.buf[self.start..self.start + buffered]);
        self.start += buffered;
        let rest = &mut out[buffered..];
        if !rest.is_empty() {
            self.file()?.read_exact(rest)?;
            self.offset += rest.len() as u64;
        }
        Ok(())
    }

    /// Reads more into the buffer, never past `end`.
    fn refill(&mut self, end: u64) -> io::Result<()> {
        if self.start > 0 {
            self.buf.copy_within(self.start..self.filled, 0);
            self.filled -= self.start;
            self.start = 0;
        }
        let room = self.buf.len() - self.filled;
        let wanted = usize::try_from(end.saturating_sub(self.offset))
            .unwrap_or(usize::MAX)
            .min(room);
        if wanted == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{} ends before the queue's published end",
                    self.path.display()
                ),
            ));
        }
        let filled = self.filled;
        self.file()?;
        let Some(file) = self.file.as_mut() else {
            return Err(io::Error::other("segment file was not opened"));
        };
        let read = file.read(&mut self.buf[filled..filled + wanted])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("{} is shorter than its published end", self.path.display()),
            ));
        }
        self.filled += read;
        self.offset += read as u64;
        Ok(())
    }

    fn file(&mut self) -> io::Result<&mut File> {
        if self.file.is_none() {
            let mut file = File::open(&self.path)?;
            file.seek(SeekFrom::Start(self.offset))?;
            self.file = Some(file);
        }
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("segment file was not opened"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One OS lock per file: a second handle, in this process or another, is
    /// refused while the first is open, and a dropped handle frees it.
    #[test]
    fn a_lock_is_held_by_one_handle_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("send.lock");
        let first = FileGuard::try_lock(&path).unwrap().expect("free lock");
        assert!(FileGuard::try_lock(&path).unwrap().is_none(), "held");
        drop(first);
        assert!(FileGuard::try_lock(&path).unwrap().is_some(), "freed");
        assert!(path.exists(), "the file stays for the next owner");
    }

    /// The limit a receiver reads under, as the sender publishes and moves on.
    #[test]
    fn the_limit_follows_what_the_sender_published() {
        let shared = Shared::default();
        shared.publish(3, 100);
        let rows = [
            (2, Limit::Sealed),
            (3, Limit::Open(100)),
            (4, Limit::NotYet),
        ];
        for (segment, want) in rows {
            assert_eq!(shared.limit(segment), want, "segment {segment}");
        }
        shared.publish(4, 0);
        assert_eq!(shared.limit(3), Limit::Sealed);
        assert_eq!(shared.limit(4), Limit::Open(0));
    }

    /// The buffer never reads past the end it is given, so bytes written
    /// after it stay unread until the end moves.
    #[test]
    fn a_follower_reads_no_further_than_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.q");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let mut follower = Follower::new(path, 0, 64);
        assert_eq!(&follower.peek4(4).unwrap(), b"abcd");
        follower.consume(4);
        assert!(
            follower.peek4(6).is_err(),
            "only 2 bytes lie before the end"
        );
        follower.seek(4).unwrap();
        assert_eq!(&follower.peek4(8).unwrap(), b"efgh");
        let mut out = [0_u8; 4];
        follower.read_into(&mut out).unwrap();
        assert_eq!(&out, b"efgh");
        assert_eq!(follower.position(), 8);
    }
}
