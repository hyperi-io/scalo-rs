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
//! - Compression of rotated files (`compress_rotated`, gzip by default), keeping the six newest plain
//! - Removal of rotated files by age and by count
//! - Optional `durable` writes, synced to disk before they return
//! - File and directory modes on Unix, the umask's unless set
//! - Whole writes: a failed write is cut back off, and a partial last line
//!   left by a crash is cut off when the file is opened
//! - Atomic write counters for metrics
//!
//! It knows nothing about DLQ or output semantics -- callers serialise their
//! own types and hand raw `&[u8]` lines to the writer.

mod config;
mod ndjson_writer;
mod rotating;

#[cfg(feature = "transport-file")]
pub(crate) use config::NO_AGE_LIMIT;
#[cfg(feature = "config-schema")]
pub(crate) use config::compress_rotated_schema;
pub use config::{
    DEFAULT_SPOOL_ROOT, FileMode, FileWriteOptions, FileWriterConfig, ROTATED_ACCEPTS,
    RotatedNaming, RotationPeriod,
};
pub use ndjson_writer::{AsyncNdjsonWriter, NdjsonWriter};
