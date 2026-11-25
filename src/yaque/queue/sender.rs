// Copied from yaque 0.6.6, src/queue/sender.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! The sending half: appends whole entries to the newest segment.

use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Options, scan, segment_filename, segments};
use crate::yaque::count;
use crate::yaque::header::{EOF, Header, MAX_LEN};
use crate::yaque::state::QueueState;
use crate::yaque::sync::{FileGuard, Shared};

/// The sending half of a queue.
pub(crate) struct Sender {
    base: PathBuf,
    segment_bytes: u64,
    file: Arc<File>,
    state: QueueState,
    /// A failed write's bytes are still in the segment past `state`.
    torn: bool,
    shared: Arc<Shared>,
    _lock: FileGuard,
}

impl Sender {
    /// Opens the newest segment for appending.
    ///
    /// A torn entry at its end, left by a kill or a full disk, is cut off,
    /// and a segment already closed by its end marker is followed by a new one.
    /// From a damaged header on, the bytes go to `options.damaged_tail` first,
    /// since no entry past it can be found.
    pub(crate) fn open(
        base: &Path,
        options: Options,
        shared: Arc<Shared>,
        lock: FileGuard,
    ) -> io::Result<Self> {
        let numbers = segments(base)?;
        let mut disk = 0;
        for &number in &numbers {
            disk += std::fs::metadata(segment_filename(base, number))?.len();
        }
        shared.add_disk(disk);
        let newest = numbers.last().copied().unwrap_or(0);
        let path = segment_filename(base, newest);
        let found = match scan(&path, 0, options.whole) {
            Ok(found) => Some(found),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        let (state, file) = match found {
            Some(found) if found.sealed => {
                let state = QueueState {
                    segment: newest,
                    position: found.end,
                }
                .next_segment();
                (state, open_segment(base, state.segment)?)
            }
            found => {
                let end = found.map_or(0, |f| f.end);
                let file = open_segment(base, newest)?;
                if let Some(found) = found.filter(|f| f.len > f.end) {
                    if let Some(keep) = options.damaged_tail.filter(|_| found.damaged) {
                        keep(&path, end)?;
                    } else {
                        count("spool_torn_entries_cut_total", options.store);
                        tracing::warn!(
                            segment = %path.display(),
                            torn_bytes = found.len - found.end,
                            "cutting a torn entry off the end of the spool"
                        );
                    }
                    file.set_len(end)?;
                    shared.remove_disk(found.len - end);
                }
                let state = QueueState {
                    segment: newest,
                    position: end,
                };
                (state, file)
            }
        };
        (&file).seek(SeekFrom::Start(state.position))?;
        let file = Arc::new(file);
        shared.set_open(state.segment, Arc::clone(&file));
        shared.publish(state.segment, state.position);
        Ok(Self {
            base: base.to_path_buf(),
            segment_bytes: options.segment_bytes.max(1),
            file,
            state,
            torn: false,
            shared,
            _lock: lock,
        })
    }

    /// Appends one entry made of `prefix` then `body`, with one write, and
    /// publishes it to the receiver once it is whole.
    ///
    /// A write that fails is cut back off the segment, so the receiver never
    /// meets a torn entry, and the error is returned.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an entry longer than 64 MiB less one byte, or the
    /// error of the write or of starting the next segment.
    pub(crate) fn send(&mut self, prefix: &[u8], body: &[u8]) -> io::Result<()> {
        let len = prefix.len() + body.len();
        let header = Header::for_len(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("an entry of {len} bytes is longer than the {MAX_LEN} a spool entry holds"),
            )
        })?;
        if self.torn {
            self.cut_back()?;
        }
        if self.state.position >= self.segment_bytes {
            self.roll()?;
        }
        self.append(&mut [
            IoSlice::new(&header.encode()),
            IoSlice::new(prefix),
            IoSlice::new(body),
        ])?;
        let written = 4 + len as u64;
        self.state.position += written;
        self.shared.add_disk(written);
        self.shared.publish(self.state.segment, self.state.position);
        Ok(())
    }

    /// What this queue's halves share, for a sync on another thread.
    pub(crate) fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }

    /// Whether the next send starts a segment, creating a file.
    #[cfg(feature = "tiered-sink")]
    pub(crate) fn rolls_next(&self) -> bool {
        self.torn || self.state.position >= self.segment_bytes
    }

    /// Bytes the queue's segment files hold.
    pub(crate) fn disk_bytes(&self) -> u64 {
        self.shared.disk_bytes()
    }

    /// Ends the open segment when the receiver has read to its end and it
    /// holds a sixteenth of a segment or more, so the receiver can delete it
    /// and the queue stops holding the disk. `Ok(true)` when it did.
    ///
    /// # Errors
    ///
    /// The error of writing the end marker or starting the next segment.
    pub(crate) fn release_drained(&mut self, read_to: QueueState) -> io::Result<bool> {
        if read_to != self.state || self.state.position < self.segment_bytes / 16 {
            return Ok(false);
        }
        self.roll()?;
        Ok(true)
    }

    /// Ends this segment with its marker and starts the next.
    ///
    /// A start that fails leaves the sender in this segment, past the marker,
    /// so the retry writes a second marker no receiver reaches.
    fn roll(&mut self) -> io::Result<()> {
        self.append(&mut [IoSlice::new(&EOF)])?;
        self.state.position += 4;
        self.shared.add_disk(4);
        self.shared.publish(self.state.segment, self.state.position);
        let next = self.state.next_segment();
        let file = Arc::new(open_segment(&self.base, next.segment)?);
        self.shared.set_open(next.segment, Arc::clone(&file));
        self.file = file;
        self.state = next;
        self.shared.publish(next.segment, 0);
        Ok(())
    }

    /// Writes `slices` in full at the end of the segment, or cuts the segment
    /// back to its last whole entry and returns the error.
    fn append(&mut self, slices: &mut [IoSlice<'_>]) -> io::Result<()> {
        let mut rest = slices;
        IoSlice::advance_slices(&mut rest, 0);
        while !rest.is_empty() {
            match (&*self.file).write_vectored(rest) {
                Ok(0) => return Err(self.failed(io::ErrorKind::WriteZero.into())),
                Ok(written) => IoSlice::advance_slices(&mut rest, written),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(self.failed(e)),
            }
        }
        Ok(())
    }

    /// Cuts a failed write off the segment. A cut that fails too is retried
    /// before the next write, so nothing is ever written after a torn entry.
    #[cold]
    fn failed(&mut self, error: io::Error) -> io::Error {
        self.torn = true;
        if let Err(cut) = self.cut_back() {
            tracing::warn!(error = %cut, "cannot cut a failed spool write back; retrying on the next write");
        }
        error
    }

    fn cut_back(&mut self) -> io::Result<()> {
        self.file.set_len(self.state.position)?;
        (&*self.file).seek(SeekFrom::Start(self.state.position))?;
        self.torn = false;
        Ok(())
    }
}

/// Opens segment `number` for writing, creating it, without truncating it.
/// Not append mode, which Windows opens without the rights to cut or sync.
fn open_segment(base: &Path, number: u64) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(segment_filename(base, number))
}
