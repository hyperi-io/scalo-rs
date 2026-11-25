// Project:   scalo
// File:      src/tiered_sink/error.rs
// Purpose:   TieredSink error types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TieredSink error types.

use std::io;
use thiserror::Error;

/// Errors that can occur in TieredSink operations.
#[derive(Debug, Error)]
pub enum TieredSinkError {
    /// Failed to open or create the spool file.
    #[error("failed to open spool at {path}: {message}")]
    SpoolOpen { path: String, message: String },

    /// Another live sink, in this process or another, holds the spool. It
    /// opens once that owner drops it. `pid` is the one its lock file names,
    /// `None` where the lock file can't be read (Windows).
    #[error(
        "spool at {path} is locked by a live owner{}, refusing a second open of the same path",
        crate::spool_codec::owner_pid(*pid)
    )]
    SpoolLocked { path: String, pid: Option<u32> },

    /// Spool operation failed.
    #[error("spool error: {0}")]
    Spool(String),

    /// Spool is full (max items or size reached).
    #[error("spool is full: {0}")]
    SpoolFull(String),

    /// Compression/decompression failed.
    #[error("codec error: {0}")]
    Codec(#[from] io::Error),

    /// Primary sink returned a fatal error.
    #[error("sink error: {0}")]
    Sink(String),

    /// The sink will never take this record: it names it in
    /// `dead_letter_reason`, or answered `FilteredDlq` for it. Not spooled.
    /// The caller writes it to its DLQ.
    #[error("dead letter: {0}")]
    DeadLetter(String),

    /// Disk is unavailable (full or inaccessible).
    #[error("disk unavailable for spooling")]
    DiskUnavailable,
}
