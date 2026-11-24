// Project:   scalo
// File:      src/spool/config.rs
// Purpose:   Spool configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Spool configuration.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::spool_codec::{
    DEFAULT_READ_BUFFER_BYTES, DEFAULT_SEGMENT_BYTES, DEFAULT_SYNC_INTERVAL_MS,
};

/// Configuration for the disk-backed spool.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
pub struct SpoolConfig {
    /// Path to the queue file.
    pub path: PathBuf,

    /// Enable zstd compression. Trades CPU for disk. Default false.
    #[serde(default)]
    pub compress: bool,

    /// Zstd level (1-22, higher = smaller/slower). Default 3.
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,

    /// Item-count cap. `push` fails when reached. Default unlimited.
    #[serde(default)]
    pub max_items: Option<usize>,

    /// Queue file size cap in bytes. `push` fails when reached. Default unlimited.
    #[serde(default)]
    pub max_size_bytes: Option<u64>,

    /// Prepend a CRC32C checksum to each record and verify it on read. Detects
    /// torn writes / bit-rot that the queue's length-only header cannot (a flipped
    /// payload byte would otherwise be returned silently). Default false (the
    /// on-disk format is unchanged unless enabled). Trades 4 bytes/record + a
    /// hardware-accelerated checksum for integrity.
    #[serde(default)]
    pub crc: bool,

    /// What to do when a corrupt cache is detected (queue won't open, or a CRC
    /// check fails on read). Default [`CorruptionPolicy::Quarantine`]: move the
    /// corrupt queue's files into a timestamped `corrupt-*` subdirectory of
    /// `path` and start fresh, so a poisoned spill cache can never stop the
    /// service or silently serve bad data. A cache another live process holds
    /// is refused, never quarantined.
    #[serde(default)]
    pub on_corruption: CorruptionPolicy,

    /// Sync the queue's files to disk before each `push` returns, so a record
    /// that was pushed survives the host losing power as well as the process
    /// being killed. Default false: a push is then flushed to the operating
    /// system, which survives a kill, and [`Spool::sync`](crate::spool::Spool::sync)
    /// makes a batch of pushes durable with one sync. Costs one disk sync per
    /// push, about a millisecond on network storage.
    #[serde(default)]
    pub sync_on_push: bool,

    /// Milliseconds between timed syncs of the spool to disk. 0 turns them off. Default 500.
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,

    /// Bytes a segment file holds before the next record starts a new one. Default 16 MiB.
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: u64,

    /// Read-ahead buffer, and the most record buffers kept to reuse, in bytes. Default 64 KiB.
    #[serde(default = "default_read_buffer_bytes")]
    pub read_buffer_bytes: usize,
}

// The recovery action enum is shared with the TieredSink cold path.
pub use crate::spool_codec::CorruptionPolicy;

fn default_compression_level() -> i32 {
    3
}

fn default_sync_interval_ms() -> u64 {
    DEFAULT_SYNC_INTERVAL_MS
}

fn default_segment_bytes() -> u64 {
    DEFAULT_SEGMENT_BYTES
}

fn default_read_buffer_bytes() -> usize {
    DEFAULT_READ_BUFFER_BYTES
}

impl Default for SpoolConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("spool.queue"),
            compress: false,
            compression_level: default_compression_level(),
            max_items: None,
            max_size_bytes: None,
            crc: false,
            on_corruption: CorruptionPolicy::Quarantine,
            sync_on_push: false,
            sync_interval_ms: default_sync_interval_ms(),
            segment_bytes: default_segment_bytes(),
            read_buffer_bytes: default_read_buffer_bytes(),
        }
    }
}

impl SpoolConfig {
    /// Create a new config with the given path.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    /// Create a config with compression enabled.
    #[must_use]
    pub fn with_compression(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            compress: true,
            ..Default::default()
        }
    }

    /// Set whether compression is enabled.
    #[must_use]
    pub fn compress(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    /// Set the compression level (1-22).
    #[must_use]
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level.clamp(1, 22);
        self
    }

    /// Set the maximum number of items.
    #[must_use]
    pub fn max_items(mut self, max: usize) -> Self {
        self.max_items = Some(max);
        self
    }

    /// Set the maximum queue file size in bytes.
    #[must_use]
    pub fn max_size_bytes(mut self, max: u64) -> Self {
        self.max_size_bytes = Some(max);
        self
    }

    /// Enable per-record CRC32C integrity checking (see [`crc`](Self::crc)).
    #[must_use]
    pub fn crc(mut self, enabled: bool) -> Self {
        self.crc = enabled;
        self
    }

    /// Set the corrupt-cache recovery policy (see [`on_corruption`](Self::on_corruption)).
    #[must_use]
    pub fn on_corruption(mut self, policy: CorruptionPolicy) -> Self {
        self.on_corruption = policy;
        self
    }

    /// Sync to disk before each push returns (see [`sync_on_push`](Self::sync_on_push)).
    #[must_use]
    pub fn sync_on_push(mut self, enabled: bool) -> Self {
        self.sync_on_push = enabled;
        self
    }

    /// Set the time between timed syncs (see [`sync_interval_ms`](Self::sync_interval_ms)).
    #[must_use]
    pub fn sync_interval_ms(mut self, interval_ms: u64) -> Self {
        self.sync_interval_ms = interval_ms;
        self
    }

    /// Set the segment size (see [`segment_bytes`](Self::segment_bytes)).
    #[must_use]
    pub fn segment_bytes(mut self, bytes: u64) -> Self {
        self.segment_bytes = bytes;
        self
    }

    /// Set the read buffer (see [`read_buffer_bytes`](Self::read_buffer_bytes)).
    #[must_use]
    pub fn read_buffer_bytes(mut self, bytes: usize) -> Self {
        self.read_buffer_bytes = bytes;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SpoolConfig::default();
        assert_eq!(config.path, PathBuf::from("spool.queue"));
        assert!(!config.compress);
        assert_eq!(config.compression_level, 3);
        assert!(config.max_items.is_none());
        assert!(config.max_size_bytes.is_none());
        assert_eq!(config.sync_interval_ms, 500);
        assert_eq!(config.segment_bytes, 16 << 20);
        assert_eq!(config.read_buffer_bytes, 64 << 10);
    }

    #[test]
    fn test_new_with_path() {
        let config = SpoolConfig::new("/tmp/test.queue");
        assert_eq!(config.path, PathBuf::from("/tmp/test.queue"));
    }

    #[test]
    fn test_with_compression() {
        let config = SpoolConfig::with_compression("/tmp/test.queue");
        assert!(config.compress);
    }

    #[test]
    fn test_builder_pattern() {
        let config = SpoolConfig::new("/tmp/test.queue")
            .compress(true)
            .compression_level(10)
            .max_items(1000)
            .max_size_bytes(1024 * 1024)
            .sync_interval_ms(0)
            .segment_bytes(1 << 20)
            .read_buffer_bytes(8192);

        assert!(config.compress);
        assert_eq!(config.compression_level, 10);
        assert_eq!(config.max_items, Some(1000));
        assert_eq!(config.max_size_bytes, Some(1024 * 1024));
        assert_eq!(config.sync_interval_ms, 0);
        assert_eq!(config.segment_bytes, 1 << 20);
        assert_eq!(config.read_buffer_bytes, 8192);
    }

    #[test]
    fn test_compression_level_clamped() {
        let config = SpoolConfig::default().compression_level(100);
        assert_eq!(config.compression_level, 22);

        let config = SpoolConfig::default().compression_level(-5);
        assert_eq!(config.compression_level, 1);
    }
}
