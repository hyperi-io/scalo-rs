// Project:   scalo
// File:      src/spool/error.rs
// Purpose:   Spool error types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Spool error types.

use std::io;
use thiserror::Error;

/// Errors that can occur during spool operations.
#[derive(Debug, Error)]
pub enum SpoolError {
    /// Failed to open or create the queue file.
    #[error("failed to open spool at {path}: {message}")]
    Open { path: String, message: String },

    /// Another live open, in this process or another, holds the queue. It
    /// opens once that owner drops it. `pid` is the one its lock file names,
    /// `None` where the lock file can't be read (Windows).
    #[error(
        "spool at {path} is locked by a live owner{}, refusing a second open of the same path",
        crate::spool_codec::owner_pid(*pid)
    )]
    Locked { path: String, pid: Option<u32> },

    /// Queue file operation error.
    #[error("spool queue error: {0}")]
    Queue(String),

    /// I/O error during queue operations.
    #[error("spool I/O error: {0}")]
    Io(#[from] io::Error),

    /// Queue has reached its maximum item count.
    #[error("spool is full: maximum {max} items reached")]
    MaxItemsReached { max: usize },

    /// Queue has reached its maximum size.
    #[error("spool is full: maximum size {max_bytes} bytes reached")]
    MaxSizeReached { max_bytes: u64 },

    /// Compression error.
    #[error("compression error: {0}")]
    Compression(String),

    /// Decompression error.
    #[error("decompression error: {0}")]
    Decompression(String),

    /// Queue file is corrupted.
    #[error("spool file is corrupted: {0}")]
    Corrupted(String),

    /// The disk is full. The push was refused and nothing was written. Retry
    /// once the consumer has freed space.
    #[error("spool disk is full: {0}")]
    DiskFull(String),
}
