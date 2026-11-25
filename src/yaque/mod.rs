// Copied from yaque 0.6.6, src/lib.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! The disk queue under the spool and the tiered sink.
//!
//! A queue is a directory of numbered segment files. Each entry is a 4-byte
//! header carrying its length, then the entry. The sender appends to the
//! newest segment and starts the next once one passes its size, ending the
//! old one with a marker. The receiver reads entries in transactions and
//! saves its committed position in `recv-metadata`. A segment goes once every
//! entry in it is committed.
//!
//! The sender and the receiver are opened together and share what the sender
//! has written, so the receiver reads only whole entries and wakes when one
//! lands. Each half holds an OS lock on its lock file, which the system drops
//! with a dead process.

mod header;
mod queue;
mod state;
mod sync;
mod version;

#[cfg(feature = "tiered-sink")]
pub(crate) use queue::Taken;
pub(crate) use queue::{Options, Receiver, Sender, channel, segments};
pub(crate) use state::{QueueState, RECV_METADATA, write_position};
pub(crate) use sync::Shared;

/// The smallest read-ahead buffer: it must hold a header.
pub(crate) const MIN_READ_BUFFER: usize = 4096;

#[cfg(feature = "memory")]
pub(crate) use crate::memory::MemoryGuard;

/// Stands in for the memory guard in a build without the `memory` feature,
/// where no value of it exists.
#[cfg(not(feature = "memory"))]
pub(crate) enum MemoryGuard {}

#[cfg(not(feature = "memory"))]
impl MemoryGuard {
    pub(crate) fn try_reserve(&self, _bytes: u64) -> bool {
        match *self {}
    }

    pub(crate) fn release(&self, _bytes: u64) {
        match *self {}
    }
}

/// Counts one of the queue's failures under its store.
pub(crate) fn count(name: &'static str, store: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(name, "store" => store).increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = (name, store);
}

/// A fresh scratch directory for a test queue, removed when the guard drops.
#[cfg(test)]
pub(crate) fn test_queue_dir() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue");
    (dir, path)
}

/// A seeded xorshift64 stream, so a failing test replays the same data.
#[cfg(test)]
pub(crate) fn test_rng() -> impl FnMut() -> u64 {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    }
}
