// Project:   scalo
// File:      src/io/config.rs
// Purpose:   Shared file writer configuration types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared configuration for NDJSON file writers.
//!
//! Used by the DLQ file backend, the file output sink and the file transport.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::compression::{Accepts, Codec, Compression};

/// The codecs `compress_rotated` takes. Each writes its own suffix: `.gz`, `.zst`.
pub const ROTATED_ACCEPTS: Accepts =
    Accepts::new("compress_rotated", &[Codec::None, Codec::Gzip, Codec::Zstd]);

/// The schema of a `compress_rotated` key, narrowed to [`ROTATED_ACCEPTS`].
#[cfg(feature = "config-schema")]
pub(crate) fn compress_rotated_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    ROTATED_ACCEPTS.schema()
}

/// Root of the default spool paths: the file writer's, the file output sink's
/// (`output/`) and the file DLQ's (`dlq/`). The output sink and the DLQ append
/// the service name, so apps sharing one volume keep separate directories.
pub const DEFAULT_SPOOL_ROOT: &str = "/var/spool/scalo";

/// File rotation period, by local time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RotationPeriod {
    /// Rotate before the first write in a new hour.
    #[default]
    Hourly,
    /// Rotate before the first write on a new day.
    Daily,
    /// No time rotation. `max_bytes` can still rotate the file.
    Never,
}

/// How rotated files are named.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RotatedNaming {
    /// `<file>.<local YYYYMMDDTHHMMSS>`, adding `.1`, `.2` within one second.
    #[default]
    Stamped,
    /// `<file>.1` newest to `<file>.<max_files>` oldest. Each rotation moves
    /// every number up one. No compression and no age limit: their keys are
    /// refused beside it.
    Numbered,
}

/// A Unix permission mode, written in octal: `"0600"`, `"600"` or `"0o600"`.
///
/// An integer is read by its decimal digits as octal, so `600` from an env var
/// or an unquoted YAML `0600` is mode `0o600`. YAML 1.2 reads `0o600` unquoted
/// as decimal 384, which this refuses, so quote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileMode(u32);

impl FileMode {
    /// Owner read and write, the default for files.
    pub const PRIVATE_FILE: Self = Self(0o600);
    /// Owner read, write and search, the default for directories.
    pub const PRIVATE_DIR: Self = Self(0o700);

    /// A mode from its bits.
    ///
    /// # Errors
    ///
    /// A value above `0o7777`.
    pub fn new(bits: u32) -> Result<Self, String> {
        if bits > 0o7777 {
            return Err(format!("mode {bits:o} is above 7777"));
        }
        Ok(Self(bits))
    }

    /// The permission bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    fn parse(text: &str) -> Result<Self, String> {
        let digits = text.trim();
        let digits = digits.strip_prefix("0o").unwrap_or(digits);
        if digits.is_empty() {
            return Err("a mode needs octal digits, such as \"0600\"".into());
        }
        u32::from_str_radix(digits, 8)
            .map_err(|_| format!("mode {text:?} is not octal, write it as \"0600\""))
            .and_then(Self::new)
    }
}

impl std::fmt::Display for FileMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04o}", self.0)
    }
}

impl Serialize for FileMode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FileMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Text(String),
            Number(u64),
        }
        match Written::deserialize(deserializer)? {
            Written::Text(text) => Self::parse(&text),
            Written::Number(n) => Self::parse(&n.to_string()),
        }
        .map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "config-schema")]
impl schemars::JsonSchema for FileMode {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FileMode".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^(0o)?[0-7]{1,4}$",
            "description": "Unix permission mode in octal, such as \"0600\". Ignored on Windows."
        })
    }
}

/// How a writer rotates by size, keeps rotated files, syncs, and sets
/// permissions. Flattened into each file config, so its keys sit beside
/// `rotation`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct FileWriteOptions {
    /// Rotate before a write that would take the file past this many bytes.
    /// A write into an empty file always lands, however large. Unset: no size
    /// rotation. Works beside the time rotation: either rotates.
    pub max_bytes: Option<u64>,

    /// Rotated files kept, newest first. Older ones are deleted at each
    /// rotation. Unset: no count limit, only the age limit. Under `numbered`,
    /// the highest number kept, and `0` deletes the file at each rotation.
    pub max_files: Option<u32>,

    /// How rotated files are named: `stamped` (default) or `numbered`.
    pub naming: RotatedNaming,

    /// Sync the file's data to disk after each write, and its directory after
    /// the file is created or rotated, so a write that returned `Ok` survives
    /// power loss. Costs a disk flush per batch. Off by default.
    pub durable: bool,

    /// Mode for files the writer creates, before the umask. Unset: `0666`
    /// before the umask, except in the file transport, where it is `0600`.
    /// Unix only: a Windows file takes its folder's ACL. Existing files keep
    /// theirs.
    pub file_mode: Option<FileMode>,

    /// Mode for directories the writer creates, before the umask. Unset:
    /// `0777` before the umask, except in the file transport, where it is
    /// `0700`. Unix only.
    pub dir_mode: Option<FileMode>,
}

impl FileWriteOptions {
    /// These options with an unset mode made private: `0600` for files,
    /// `0700` for directories.
    #[must_use]
    pub fn private_by_default(self) -> Self {
        Self {
            file_mode: self.file_mode.or(Some(FileMode::PRIVATE_FILE)),
            dir_mode: self.dir_mode.or(Some(FileMode::PRIVATE_DIR)),
            ..self
        }
    }
}

/// Shared file writer configuration.
///
/// Contains the core settings for rotating NDJSON file output. Used by
/// [`super::NdjsonWriter`] and consumed by the DLQ, the file output and the
/// file transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileWriterConfig {
    /// Base directory for output files.
    pub path: PathBuf,

    /// File rotation period.
    pub rotation: RotationPeriod,

    /// Delete rotated files older than this many days, at each rotation.
    pub max_age_days: u32,

    /// Codec for rotated files past the six newest: `none`, `gzip` or `zstd`.
    /// Default gzip, which log shippers read.
    pub compress_rotated: Compression,

    /// Size rotation, retention by count, syncing and permissions.
    #[serde(flatten)]
    pub write: FileWriteOptions,
}

impl Default for FileWriterConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_SPOOL_ROOT),
            rotation: RotationPeriod::default(),
            max_age_days: 30,
            compress_rotated: Compression::new(Codec::Gzip),
            write: FileWriteOptions::default(),
        }
    }
}

/// The `max_age_days` that sets no age limit.
pub(crate) const NO_AGE_LIMIT: u32 = u32::MAX;

/// The age limit a writer runs: `set`, else `default` under `stamped` and none
/// under `numbered`, whose names carry no date.
#[cfg(any(feature = "dlq", feature = "output-file"))]
pub(crate) fn age_limit(set: Option<u32>, default: u32, naming: RotatedNaming) -> u32 {
    set.unwrap_or(match naming {
        RotatedNaming::Stamped => default,
        RotatedNaming::Numbered => NO_AGE_LIMIT,
    })
}

impl FileWriterConfig {
    /// Refuse what `naming: numbered` can't apply.
    ///
    /// # Errors
    ///
    /// Under `numbered`, a `compress_rotated` codec, or an age limit, since
    /// numbered names carry no date.
    pub(crate) fn check_naming(&self) -> Result<(), String> {
        if self.write.naming != RotatedNaming::Numbered {
            return Ok(());
        }
        if !self.compress_rotated.is_none() {
            return Err(format!(
                "naming: numbered rotates without compression, so compress_rotated {} \
                 can't apply; set compress_rotated: none",
                self.compress_rotated.codec
            ));
        }
        if self.max_age_days != NO_AGE_LIMIT {
            return Err(format!(
                "naming: numbered names carry no date, so max_age_days {} can't apply; \
                 leave max_age_days unset and keep files by max_files",
                self.max_age_days
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = FileWriterConfig::default();
        assert_eq!(config.path, PathBuf::from("/var/spool/scalo"));
        assert_eq!(config.rotation, RotationPeriod::Hourly);
        assert_eq!(config.max_age_days, 30);
        assert_eq!(config.compress_rotated, Compression::new(Codec::Gzip));
        assert_eq!(config.write, FileWriteOptions::default());
        assert_eq!(config.write.file_mode, None, "the umask decides");
        assert_eq!(config.write.dir_mode, None, "the umask decides");
        let private = config.write.private_by_default();
        assert_eq!(private.file_mode, Some(FileMode::PRIVATE_FILE));
        assert_eq!(private.dir_mode, Some(FileMode::PRIVATE_DIR));
        let set = FileWriteOptions {
            file_mode: FileMode::new(0o640).ok(),
            ..FileWriteOptions::default()
        };
        assert_eq!(
            set.private_by_default().file_mode,
            FileMode::new(0o640).ok(),
            "a set mode stays"
        );
        assert!(!config.write.durable);
    }

    #[cfg(feature = "serde_json")]
    #[test]
    fn test_rotation_period_serde() {
        for (text, period) in [
            ("hourly", RotationPeriod::Hourly),
            ("daily", RotationPeriod::Daily),
            ("never", RotationPeriod::Never),
        ] {
            let parsed: RotationPeriod =
                serde_json::from_str(&format!("{text:?}")).expect("deserialise");
            assert_eq!(parsed, period, "{text}");
        }
        assert!(serde_json::from_str::<RotationPeriod>(r#""weekly""#).is_err());
    }

    #[cfg(feature = "serde_json")]
    #[test]
    fn test_config_serde_roundtrip() {
        let config = FileWriterConfig {
            path: "/tmp/test".into(),
            rotation: RotationPeriod::Daily,
            max_age_days: 7,
            compress_rotated: Compression::new(Codec::Zstd).with_level(9),
            write: FileWriteOptions {
                max_bytes: Some(1024),
                max_files: Some(3),
                naming: RotatedNaming::Numbered,
                durable: true,
                file_mode: FileMode::new(0o640).ok(),
                dir_mode: FileMode::new(0o750).ok(),
            },
        };
        let json = serde_json::to_string(&config).expect("serialise");
        assert!(json.contains(r#""file_mode":"0640""#), "{json}");
        assert!(json.contains(r#""naming":"numbered""#), "{json}");
        let parsed: FileWriterConfig = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(parsed.path, PathBuf::from("/tmp/test"));
        assert_eq!(parsed.rotation, RotationPeriod::Daily);
        assert_eq!(parsed.max_age_days, 7);
        assert_eq!(parsed.compress_rotated, config.compress_rotated);
        assert!(
            json.contains(r#""compress_rotated":{"codec":"zstd","level":9}"#),
            "{json}"
        );
        assert_eq!(parsed.write, config.write);
    }

    /// The write keys are optional and sit beside the others.
    #[cfg(feature = "serde_json")]
    #[test]
    fn test_write_options_are_flat_and_optional() {
        let bare = r#"{"path":"/x","rotation":"never","max_age_days":1,"compress_rotated":{"codec":"none"}}"#;
        let parsed: FileWriterConfig = serde_json::from_str(bare).expect("deserialise");
        assert_eq!(parsed.write, FileWriteOptions::default());

        let flat = r#"{"path":"/x","rotation":"never","max_age_days":1,"compress_rotated":{"codec":"none"},
            "max_bytes":10,"max_files":2,"naming":"numbered","durable":true}"#;
        let parsed: FileWriterConfig = serde_json::from_str(flat).expect("deserialise");
        assert_eq!(parsed.write.max_bytes, Some(10));
        assert_eq!(parsed.write.max_files, Some(2));
        assert_eq!(parsed.write.naming, RotatedNaming::Numbered);
        assert!(parsed.write.durable);
        assert!(serde_json::from_str::<RotatedNaming>(r#""dated""#).is_err());
    }

    /// `numbered` refuses a rotated-file codec and an age limit, naming the
    /// key, and `stamped` takes both.
    #[test]
    fn numbered_naming_refuses_compression_and_an_age_limit() {
        let numbered = FileWriteOptions {
            naming: RotatedNaming::Numbered,
            ..FileWriteOptions::default()
        };
        let plain = FileWriterConfig {
            max_age_days: NO_AGE_LIMIT,
            compress_rotated: Compression::NONE,
            write: numbered,
            ..FileWriterConfig::default()
        };
        let cases = [
            ("numbered, plain, no age limit", plain.clone(), None),
            (
                "numbered with gzip",
                FileWriterConfig {
                    compress_rotated: Compression::new(Codec::Gzip),
                    ..plain.clone()
                },
                Some("compress_rotated gzip"),
            ),
            (
                "numbered with an age limit",
                FileWriterConfig {
                    max_age_days: 30,
                    ..plain.clone()
                },
                Some("max_age_days 30"),
            ),
            ("stamped takes both", FileWriterConfig::default(), None),
        ];
        for (name, config, refused) in cases {
            match (config.check_naming(), refused) {
                (Ok(()), None) => {}
                (Err(e), Some(key)) => {
                    assert!(
                        e.contains("naming: numbered") && e.contains(key),
                        "{name}: {e}"
                    );
                }
                (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[cfg(feature = "serde_json")]
    #[test]
    fn test_file_mode_reads_octal_in_each_form() {
        let cases: [(&str, Option<u32>); 10] = [
            (r#""0600""#, Some(0o600)),
            (r#""600""#, Some(0o600)),
            (r#""0o640""#, Some(0o640)),
            ("600", Some(0o600)),
            ("640", Some(0o640)),
            (r#""7777""#, Some(0o7777)),
            (r#""0800""#, None),
            ("384", None),
            (r#""17777""#, None),
            (r#""""#, None),
        ];
        for (json, want) in cases {
            let got = serde_json::from_str::<FileMode>(json)
                .ok()
                .map(FileMode::bits);
            assert_eq!(got, want, "{json}");
        }
    }
}
