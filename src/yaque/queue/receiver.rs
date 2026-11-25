// Copied from yaque 0.6.6, src/queue/receiver.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! The receiving half: reads whole entries in transactions, deleting each
//! segment once every entry in it is committed.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "spool")]
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::{Options, SHAPE_BYTES, Shape, scan, segment_filename, segments};
use crate::yaque::header::{EOF, Header};
use crate::yaque::state::{Persistence, QueueState};
use crate::yaque::sync::{FileGuard, Follower, Limit, Shared};
use crate::yaque::{MemoryGuard, count};

/// The receiver saves its committed position after this many records, an
/// entry counting as the records its shape names...
const SAVE_EVERY_RECORDS: u64 = 250;
/// ...or this long after the last save, whichever comes first. A crash
/// replays at most what was committed since.
const SAVE_EVERY: Duration = Duration::from_millis(350);

/// What is queued after the committed position.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Queued {
    pub(crate) entries: u64,
    /// The records the entries hold, by their shape.
    pub(crate) records: u64,
    /// Their payload bytes.
    pub(crate) bytes: u64,
}

/// What one [`Receiver::read`] took.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Taken {
    /// Entries added to the open transaction.
    pub(crate) entries: usize,
    /// The memory guard refused the next entry, so the read stopped short.
    pub(crate) held_back: bool,
}

/// The receiving half of a queue.
pub(crate) struct Receiver {
    base: PathBuf,
    follower: Follower,
    /// Where the next read starts.
    state: QueueState,
    /// Where the open transaction started.
    committed: QueueState,
    /// The open transaction's entries, the position after each, and the
    /// records each holds.
    taken: Vec<Vec<u8>>,
    ends: Vec<QueueState>,
    records: Vec<u64>,
    /// Bytes the open transaction reserved with the memory guard.
    reserved: u64,
    spare: Spare,
    shape: Option<fn(&[u8]) -> Shape>,
    records_since_save: u64,
    saved_at: Instant,
    shared: Arc<Shared>,
    store: &'static str,
    _lock: FileGuard,
}

impl Receiver {
    /// Opens the receiving half at its saved position.
    pub(crate) fn open(
        base: &Path,
        options: Options,
        shared: Arc<Shared>,
        lock: FileGuard,
    ) -> io::Result<Self> {
        let (persistence, saved) = Persistence::open(base)?;
        let state = start_at(saved.unwrap_or_default(), &shared);
        shared.set_persistence(persistence, state, saved);
        Ok(Self {
            base: base.to_path_buf(),
            follower: Follower::new(
                segment_filename(base, state.segment),
                state.position,
                options.read_buffer_bytes,
            ),
            state,
            committed: state,
            taken: Vec::new(),
            ends: Vec::new(),
            records: Vec::new(),
            reserved: 0,
            spare: Spare {
                buffers: Vec::new(),
                held: 0,
                cap: options.recycle_bytes,
            },
            shape: options.shape,
            records_since_save: 0,
            saved_at: Instant::now(),
            shared,
            store: options.store,
            _lock: lock,
        })
    }

    /// The entries, records and payload bytes queued after the committed
    /// position, read from the segments' headers and each entry's leading
    /// bytes.
    ///
    /// # Errors
    ///
    /// A directory or segment read's error.
    pub(crate) fn queued(&self) -> io::Result<Queued> {
        let mut queued = Queued::default();
        for number in segments(&self.base)? {
            if number < self.committed.segment {
                continue;
            }
            let from = if number == self.committed.segment {
                self.committed.position
            } else {
                0
            };
            match scan(
                &segment_filename(&self.base, number),
                from,
                None,
                self.shape,
            ) {
                Ok(found) => {
                    queued.entries += found.entries;
                    queued.records += found.records;
                    queued.bytes += found.bytes;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(queued)
    }

    /// Reads whole entries into the open transaction while they hold up to
    /// `max_records` records and `max_bytes` of payload, taking the first
    /// whatever it holds. Never waits: an empty queue reads none.
    ///
    /// With a memory guard, each entry's decoded length, or its stored length
    /// where it names none, is reserved before its buffer is filled. A refusal
    /// stops the read after the first entry, never before it, so a pressed
    /// process drains in smaller reads and still drains.
    ///
    /// # Errors
    ///
    /// A damaged header or a read error at the first entry. Past the first,
    /// the read stops there and the next one meets it.
    pub(crate) fn read(
        &mut self,
        max_records: usize,
        max_bytes: u64,
        guard: Option<&MemoryGuard>,
    ) -> io::Result<Taken> {
        let mut taken = Taken::default();
        let mut bytes = 0;
        let mut records = 0_u64;
        let max_records = u64::try_from(max_records).unwrap_or(u64::MAX);
        while records < max_records {
            match self.next(bytes, max_bytes, records, max_records, guard) {
                Ok(Next::Entry(len, held)) => {
                    taken.entries += 1;
                    bytes += len;
                    records += held;
                }
                Ok(Next::Empty) => break,
                Ok(Next::HeldBack) => {
                    taken.held_back = true;
                    break;
                }
                Err(e) => {
                    self.follower.seek(self.state.position)?;
                    if taken.entries == 0 {
                        return Err(e);
                    }
                    break;
                }
            }
        }
        Ok(taken)
    }

    /// The open transaction's entries, oldest first.
    pub(crate) fn taken(&self) -> &[Vec<u8>] {
        &self.taken
    }

    /// Bytes the open transaction holds reserved with the memory guard.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.reserved
    }

    /// Commits the first `entries` of the open transaction and returns the
    /// rest to the queue, so the next read meets them again. `commit(0, ..)`
    /// rolls the whole transaction back. Segments every committed entry has
    /// left are deleted.
    ///
    /// The committed position is saved every few hundred entries, or sooner
    /// once some time has passed. A save that fails is logged: a crash then
    /// replays more, and loses nothing.
    pub(crate) fn commit(&mut self, entries: usize, guard: Option<&MemoryGuard>) {
        let entries = entries.min(self.taken.len());
        let target = match entries.checked_sub(1) {
            Some(last) => self.ends[last],
            None => self.committed,
        };
        let records: u64 = self.records[..entries].iter().sum();
        if let Some(guard) = guard
            && self.reserved > 0
        {
            guard.release(self.reserved);
        }
        self.reserved = 0;
        for buffer in self.taken.drain(..) {
            self.spare.put(buffer);
        }
        self.ends.clear();
        self.records.clear();
        if target != self.state {
            self.go_to(target);
        }
        let from = self.committed.segment;
        self.committed = target;
        self.shared.commit(target);
        if entries == 0 {
            return;
        }
        self.records_since_save += records;
        let crossed = target.segment > from;
        if crossed
            || self.records_since_save >= SAVE_EVERY_RECORDS
            || self.saved_at.elapsed() >= SAVE_EVERY
        {
            self.save();
        }
        if crossed {
            self.remove_segments(from, target.segment);
        }
    }

    /// Crosses any end-of-segment markers at the read position, deleting each
    /// finished segment, while no transaction is open.
    pub(crate) fn sweep(&mut self) -> io::Result<()> {
        while self.taken.is_empty() {
            let limit = self.shared.limit(self.state.segment);
            if limit == Limit::NotYet {
                break;
            }
            let end = self.follower.end(limit)?;
            if end.saturating_sub(self.state.position) < 4 || self.follower.peek4(end)? != EOF {
                break;
            }
            self.end_of_segment(false);
        }
        Ok(())
    }

    /// The position the next read starts from.
    pub(crate) fn position(&self) -> QueueState {
        self.state
    }

    /// Bytes the queue's segment files hold.
    #[cfg(feature = "tiered-sink")]
    pub(crate) fn disk_bytes(&self) -> u64 {
        self.shared.disk_bytes()
    }

    /// Ready once a whole entry or a segment's end lies past the read
    /// position. Registers the task to wake on the sender's next publish.
    #[cfg(feature = "spool")]
    pub(crate) fn poll_ready(&mut self, cx: &Context<'_>) -> Poll<()> {
        self.shared.register(cx);
        let ready = match self.shared.limit(self.state.segment) {
            Limit::NotYet => false,
            Limit::Sealed => true,
            Limit::Open(end) => end > self.state.position,
        };
        if ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// The next whole entry, if one is readable.
    fn next(
        &mut self,
        bytes: u64,
        max_bytes: u64,
        records: u64,
        max_records: u64,
        guard: Option<&MemoryGuard>,
    ) -> io::Result<Next> {
        loop {
            let limit = self.shared.limit(self.state.segment);
            if limit == Limit::NotYet {
                return Ok(Next::Empty);
            }
            let sealed = limit == Limit::Sealed;
            let end = self.follower.end(limit)?;
            let left = end.saturating_sub(self.state.position);
            if left < 4 {
                if !sealed {
                    return Ok(Next::Empty);
                }
                self.end_of_segment(left > 0);
                continue;
            }
            let word = self.follower.peek4(end)?;
            if word == EOF {
                self.end_of_segment(false);
                continue;
            }
            let len = u64::from(Header::decode(word)?.len());
            if left - 4 < len {
                if !sealed {
                    return Ok(Next::Empty);
                }
                self.end_of_segment(true);
                continue;
            }
            let shape = match self.shape {
                Some(read) => {
                    let peek = usize::try_from(len).unwrap_or(usize::MAX).min(SHAPE_BYTES);
                    read(&self.follower.peek(4 + peek, end)?[4..])
                }
                None => Shape::ONE,
            };
            if !self.taken.is_empty()
                && (bytes + len > max_bytes || records + shape.records > max_records)
            {
                return Ok(Next::Empty);
            }
            let reserve = shape.decoded.map_or(len, |decoded| decoded.max(len));
            let reserved = match guard {
                Some(guard) => guard.try_reserve(reserve),
                None => false,
            };
            if guard.is_some() && !reserved && !self.taken.is_empty() {
                return Ok(Next::HeldBack);
            }
            let mut buffer = self.spare.take(len)?;
            self.follower.consume(4);
            if let Err(e) = self.follower.read_into(&mut buffer) {
                if let (Some(guard), true) = (guard, reserved) {
                    guard.release(reserve);
                }
                self.spare.put(buffer);
                return Err(e);
            }
            if reserved {
                self.reserved += reserve;
            }
            self.state.position += 4 + len;
            self.taken.push(buffer);
            self.ends.push(self.state);
            self.records.push(shape.records);
            return Ok(Next::Entry(len, shape.records));
        }
    }

    /// Moves to the next segment. With no transaction open, the finished one
    /// is committed and deleted at once.
    fn end_of_segment(&mut self, torn: bool) {
        if torn {
            tracing::warn!(
                segment = self.state.segment,
                position = self.state.position,
                "a closed spool segment ends without its marker; reading on from the next"
            );
        }
        let finished = self.state.segment;
        let next = self.state.next_segment();
        self.go_to(next);
        if self.taken.is_empty() {
            self.committed = next;
            self.shared.commit(next);
            self.save();
            self.remove_segments(finished, next.segment);
        }
    }

    fn go_to(&mut self, target: QueueState) {
        if target.segment == self.state.segment {
            if let Err(e) = self.follower.seek(target.position) {
                tracing::warn!(error = %e, "cannot seek the spool reader; reopening its segment");
                self.follower.retarget(
                    segment_filename(&self.base, target.segment),
                    target.position,
                );
            }
        } else {
            self.follower.retarget(
                segment_filename(&self.base, target.segment),
                target.position,
            );
        }
        self.state = target;
    }

    /// Saves the committed position now.
    pub(crate) fn save(&mut self) {
        match self.shared.save_position() {
            Ok(()) => {
                self.records_since_save = 0;
                self.saved_at = Instant::now();
            }
            Err(e) => {
                count("spool_position_save_failures_total", self.store);
                tracing::warn!(
                    error = %e,
                    "cannot save the spool's read position; a restart replays from the last one saved"
                );
            }
        }
    }

    fn remove_segments(&mut self, from: u64, to: u64) {
        for number in from..to {
            let path = segment_filename(&self.base, number);
            let size = std::fs::metadata(&path).map_or(0, |m| m.len());
            match std::fs::remove_file(&path) {
                Ok(()) => self.shared.remove_disk(size),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    count("spool_segment_delete_failures_total", self.store);
                    tracing::warn!(
                        segment = %path.display(),
                        error = %e,
                        "cannot delete a consumed spool segment"
                    );
                }
            }
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.save();
    }
}

/// Where to start reading: the saved position, unless it lies past what the
/// sender holds, as after a torn tail was cut off. Everything past the
/// sender's end was consumed or cut.
fn start_at(saved: QueueState, shared: &Shared) -> QueueState {
    match shared.limit(saved.segment) {
        Limit::Open(end) if saved.position > end => {
            tracing::warn!(
                segment = saved.segment,
                saved = saved.position,
                end,
                "the spool's saved read position lies past its end; reading on from the end"
            );
            QueueState {
                position: end,
                ..saved
            }
        }
        _ => saved,
    }
}

enum Next {
    /// An entry of this many payload bytes, holding this many records.
    Entry(u64, u64),
    Empty,
    HeldBack,
}

/// Buffers of committed entries, kept to read the next ones into.
struct Spare {
    buffers: Vec<Vec<u8>>,
    held: usize,
    /// Most bytes of buffers kept.
    cap: usize,
}

impl Spare {
    /// A buffer of `len` zeroed bytes, reusing a kept one.
    fn take(&mut self, len: u64) -> io::Result<Vec<u8>> {
        let len = usize::try_from(len).map_err(io::Error::other)?;
        let mut buffer = match self.buffers.pop() {
            Some(buffer) => {
                self.held -= buffer.capacity();
                buffer
            }
            None => Vec::new(),
        };
        buffer.clear();
        buffer
            .try_reserve_exact(len)
            .map_err(|e| io::Error::new(io::ErrorKind::OutOfMemory, e))?;
        buffer.resize(len, 0);
        Ok(buffer)
    }

    fn put(&mut self, buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        if capacity > 0 && self.held + capacity <= self.cap {
            self.held += capacity;
            self.buffers.push(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yaque::queue::channel;

    /// The position is saved once the records committed reach 250, counting
    /// each entry as the records its shape names, not as one. The timed save
    /// is pushed out of the test's reach.
    #[test]
    fn the_position_save_counts_records() {
        for (shape, saved) in [
            (None, false),
            (
                Some(super::super::tests::first_byte_records as fn(&[u8]) -> Shape),
                true,
            ),
        ] {
            let (_dir, path) = crate::yaque::test_queue_dir();
            let options = Options {
                segment_bytes: 1 << 20,
                read_buffer_bytes: 4096,
                recycle_bytes: 1 << 16,
                whole: None,
                damaged_tail: None,
                shape,
                store: "test",
            };
            let (mut sender, mut receiver) = channel(&path, options).unwrap();
            receiver.saved_at = Instant::now() + Duration::from_secs(3600);
            for _ in 0..3 {
                sender.send(&[100], b"an entry of 100 records").unwrap();
            }
            assert_eq!(receiver.read(1000, u64::MAX, None).unwrap().entries, 3);
            receiver.commit(3, None);
            let on_disk = std::fs::read(path.join(crate::yaque::RECV_METADATA))
                .ok()
                .and_then(|saved| saved.get(8..16).and_then(|at| at.try_into().ok()))
                .map(u64::from_be_bytes);
            assert_eq!(
                on_disk == Some(receiver.position().position),
                saved,
                "shape {}: saved at {on_disk:?}",
                shape.is_some()
            );
        }
    }
}
