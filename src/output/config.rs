// Project:   scalo
// File:      src/output/config.rs
// Purpose:   File output sink configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration for the file output sink.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::compression::{Codec, Compression};
use crate::io::{DEFAULT_SPOOL_ROOT, FileWriteOptions, FileWriterConfig, RotationPeriod};

/// File output sink configuration.
///
/// Writes raw NDJSON events to rotating files -- used for testing and
/// bare-metal deployments where Kafka is not available.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FileOutputConfig {
    /// Enable the file output sink.
    pub enabled: bool,

    /// Base directory for output files. The service name is appended as a
    /// subdirectory. Default `/var/spool/scalo/output`.
    pub path: PathBuf,

    /// Output filename (e.g. "events.ndjson").
    pub filename: String,

    /// File rotation period.
    pub rotation: RotationPeriod,

    /// Delete rotated files older than this many days, at each rotation.
    /// Unset: 7, or no age limit under `naming: numbered`, which refuses a set
    /// value since its names carry no date.
    pub max_age_days: Option<u32>,

    /// Codec for rotated files past the six newest: `none`, `gzip` or `zstd`.
    /// Default gzip.
    pub compress_rotated: Compression,

    /// Size rotation, retention by count, `durable` and file modes.
    #[serde(flatten)]
    pub write: FileWriteOptions,
}

impl Default for FileOutputConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from(DEFAULT_SPOOL_ROOT).join("output"),
            filename: "events.ndjson".to_string(),
            rotation: RotationPeriod::Hourly,
            max_age_days: None,
            compress_rotated: Compression::new(Codec::Gzip),
            write: FileWriteOptions::default(),
        }
    }
}

/// The age limit of stamped file output with `max_age_days` unset.
const DEFAULT_MAX_AGE_DAYS: u32 = 7;

impl FileOutputConfig {
    /// Convert to the shared `FileWriterConfig` for use with `NdjsonWriter`.
    #[must_use]
    pub fn to_writer_config(&self) -> FileWriterConfig {
        FileWriterConfig {
            path: self.path.clone(),
            rotation: self.rotation,
            max_age_days: crate::io::age_limit(
                self.max_age_days,
                DEFAULT_MAX_AGE_DAYS,
                self.write.naming,
            ),
            compress_rotated: self.compress_rotated,
            write: self.write,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = FileOutputConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.path, PathBuf::from("/var/spool/scalo/output"));
        assert_eq!(config.filename, "events.ndjson");
        assert_eq!(config.rotation, RotationPeriod::Hourly);
        assert_eq!(config.max_age_days, None);
        assert_eq!(config.to_writer_config().max_age_days, 7);
        assert_eq!(config.compress_rotated, Compression::new(Codec::Gzip));
        assert_eq!(config.write, FileWriteOptions::default());
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = FileOutputConfig {
            enabled: true,
            path: "/tmp/test-output".into(),
            filename: "data.ndjson".into(),
            rotation: RotationPeriod::Daily,
            max_age_days: Some(30),
            compress_rotated: crate::compression::Compression::NONE,
            write: FileWriteOptions {
                max_bytes: Some(4096),
                durable: true,
                ..FileWriteOptions::default()
            },
        };
        let json = serde_json::to_string(&config).expect("serialise");
        let parsed: FileOutputConfig = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(parsed.write, config.write);
        assert!(parsed.enabled);
        assert_eq!(parsed.filename, "data.ndjson");
        assert_eq!(parsed.rotation, RotationPeriod::Daily);
        assert_eq!(parsed.max_age_days, Some(30));
    }

    /// Unset, `max_age_days` is 7 under `stamped` and no limit under
    /// `numbered`, so a numbered output opens. A set value is kept, and
    /// numbered refuses it. A config without the key still loads.
    #[test]
    fn numbered_naming_opens_with_no_age_limit() {
        use crate::io::RotatedNaming::{Numbered, Stamped};
        use crate::output::FileOutput;

        let dir = tempfile::tempdir().expect("tempdir");
        let output = |naming, max_age_days| FileOutputConfig {
            enabled: true,
            path: dir.path().to_path_buf(),
            max_age_days,
            compress_rotated: Compression::NONE,
            write: FileWriteOptions {
                naming,
                ..FileWriteOptions::default()
            },
            ..FileOutputConfig::default()
        };
        // (case, config, the writer's age limit with u32::MAX for none, opens)
        let cases = [
            ("stamped-unset", output(Stamped, None), 7, true),
            ("stamped-set", output(Stamped, Some(5)), 5, true),
            ("numbered-unset", output(Numbered, None), u32::MAX, true),
            ("numbered-set", output(Numbered, Some(5)), 5, false),
        ];
        for (case, config, age, opens) in cases {
            assert_eq!(config.to_writer_config().max_age_days, age, "{case}");
            assert_eq!(FileOutput::new(&config, case).is_ok(), opens, "{case}");
        }
        let unset: FileOutputConfig = serde_json::from_str("{}").expect("deserialise");
        assert_eq!(unset.max_age_days, None);
    }
}
