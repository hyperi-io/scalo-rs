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

/// Configuration for the disk-backed spool.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// `path` and start fresh, so a poisoned spill cache can never wedge the
    /// service or silently serve bad data. A cache another live process holds
    /// is refused, never quarantined.
    #[serde(default)]
    pub on_corruption: CorruptionPolicy,
}

// The recovery action enum is shared with the TieredSink cold path.
pub use crate::spool_codec::CorruptionPolicy;

fn default_compression_level() -> i32 {
    3
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
            .max_size_bytes(1024 * 1024);

        assert!(config.compress);
        assert_eq!(config.compression_level, 10);
        assert_eq!(config.max_items, Some(1000));
        assert_eq!(config.max_size_bytes, Some(1024 * 1024));
    }

    #[test]
    fn test_compression_level_clamped() {
        let config = SpoolConfig::default().compression_level(100);
        assert_eq!(config.compression_level, 22);

        let config = SpoolConfig::default().compression_level(-5);
        assert_eq!(config.compression_level, 1);
    }
}
