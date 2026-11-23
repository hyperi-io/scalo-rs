// Project:   scalo
// File:      src/io/mod.rs
// Purpose:   Shared NDJSON file I/O module
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared NDJSON file I/O primitives.
//!
//! Provides [`NdjsonWriter`] -- a rotating file writer for newline-delimited
//! JSON, used by the DLQ file backend, the file output sink and the file
//! transport.
//!
//! ## Design
//!
//! `NdjsonWriter` handles:
//! - Rotation by local time (hourly, daily), by size, or both
//! - Gzip of rotated files, keeping the six newest plain
//! - Removal of rotated files by age and by count
//! - Optional `durable` writes, synced to disk before they return
//! - Private file and directory modes on Unix (`0600`, `0700` by default)
//! - Whole writes: a failed write is cut back off, and a partial last line
//!   left by a crash is cut off when the file is opened
//! - Atomic write counters for metrics
//!
//! It knows nothing about DLQ or output semantics -- callers serialise their
//! own types and hand raw `&[u8]` lines to the writer.

mod config;
mod ndjson_writer;
mod rotating;

pub use config::{
    DEFAULT_SPOOL_ROOT, FileMode, FileWriteOptions, FileWriterConfig, RotationPeriod,
};
pub use ndjson_writer::{AsyncNdjsonWriter, NdjsonWriter};
