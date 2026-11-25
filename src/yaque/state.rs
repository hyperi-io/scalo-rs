// Copied from yaque 0.6.6, src/state.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! Where each side of a queue is, and the receiver's saved position.

use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// The file the receiver keeps its position in: the segment, then the byte
/// offset in it, as two big-endian u64.
pub(crate) const RECV_METADATA: &str = "recv-metadata";

/// A position in the queue: a segment and a byte offset in it, ordered as
/// the queue is written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct QueueState {
    pub(crate) segment: u64,
    pub(crate) position: u64,
}

impl QueueState {
    /// The start of the next segment.
    pub(crate) fn next_segment(self) -> Self {
        Self {
            segment: self.segment + 1,
            position: 0,
        }
    }
}

/// The receiver's saved position.
pub(crate) struct Persistence {
    path: PathBuf,
    temporary: PathBuf,
}

impl Persistence {
    /// The position saved in `base`, `None` when none was.
    ///
    /// # Errors
    ///
    /// The read's error, `UnexpectedEof` for a file shorter than 16 bytes.
    pub(crate) fn open(base: &Path) -> io::Result<(Self, Option<QueueState>)> {
        let path = base.join(RECV_METADATA);
        let state = match std::fs::File::open(&path) {
            Ok(mut file) => {
                let mut bytes = [0_u8; 16];
                file.read_exact(&mut bytes)?;
                let (segment, position) = bytes.split_at(8);
                Some(QueueState {
                    segment: u64::from_be_bytes(segment.try_into().unwrap_or_default()),
                    position: u64::from_be_bytes(position.try_into().unwrap_or_default()),
                })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        let temporary = base.join(format!("{RECV_METADATA}.tmp"));
        Ok((Self { path, temporary }, state))
    }

    /// Saves `state` through a temporary file and a rename, so a kill leaves
    /// the old position or the new one, never a torn file. Not synced: a power
    /// loss can lose the latest saves, and the queue then replays from an
    /// older one.
    ///
    /// # Errors
    ///
    /// The write's or the rename's error.
    pub(crate) fn save(&self, state: QueueState) -> io::Result<()> {
        write_position(&self.path, &self.temporary, state, false)
    }
}

/// Writes `state` to `path` through `temporary` and a rename, syncing both
/// and the directory when `sync`.
///
/// # Errors
///
/// The write's, sync's or rename's error.
pub(crate) fn write_position(
    path: &Path,
    temporary: &Path,
    state: QueueState,
    sync: bool,
) -> io::Result<()> {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&state.segment.to_be_bytes());
    bytes[8..].copy_from_slice(&state.position.to_be_bytes());
    if sync {
        let mut file = std::fs::File::create(temporary)?;
        io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
    } else {
        std::fs::write(temporary, bytes)?;
    }
    std::fs::rename(temporary, path)?;
    #[cfg(unix)]
    if sync && let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}
