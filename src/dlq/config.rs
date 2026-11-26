// Project:   scalo
// File:      src/dlq/config.rs
// Purpose:   DLQ configuration types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration for the DLQ module.
//!
//! Supports file-based and Kafka-based backends with cascade or fan-out modes.
//!
//! ## Config Cascade Example
//!
//! ```yaml
//! dlq:
//!   mode: cascade
//!   file:
//!     enabled: true
//!     path: /var/spool/scalo/dlq
//!     rotation: hourly
//!     max_age_days: 30               # unset: 30, or none under naming: numbered
//!     compress_rotated: {codec: gzip}
//!   kafka:
//!     enabled: true
//!     routing: per_table
//!     topic_suffix: .dlq
//!     common_topic: errors.dlq     # unset: <service>.dlq
//! ```

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

// Re-export RotationPeriod from the shared io module so existing consumers
// of `dlq::RotationPeriod` continue to work without changes.
use crate::compression::{Codec, Compression};
pub use crate::io::RotationPeriod;
use crate::io::{DEFAULT_SPOOL_ROOT, FileWriteOptions, FileWriterConfig};

/// How backends are used when multiple are enabled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DlqMode {
    /// Try backends in order. Stop on first success.
    /// Default order: Kafka first, file fallback.
    #[default]
    Cascade,

    /// Write every batch to all enabled backends. The write succeeds when at
    /// least one backend takes the whole batch, and fails only when none does.
    FanOut,

    /// File backend only (no Kafka dependency).
    FileOnly,

    /// Kafka backend only (matches a typical consumer's current behaviour).
    KafkaOnly,
}

/// The default [`DlqConfig::max_payload_bytes`], 512 KiB. Base64 grows it to
/// 699,052 bytes, under Kafka's and librdkafka's default `message.max.bytes`
/// (1,048,588 and 1,000,000) with room for the envelope.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 512 * 1024;

/// Top-level DLQ configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct DlqConfig {
    /// Whether DLQ is enabled.
    pub enabled: bool,

    /// Backend routing mode.
    pub mode: DlqMode,

    /// Bounded mpsc capacity. When the queue is full, `try_send` returns
    /// `QueueFull` (overflow=Drop). Sized for failure-burst tolerance.
    /// Default 10_000.
    pub queue_capacity: usize,

    /// Drain coalesces up to this many entries into one backend write.
    /// Default 256.
    pub batch_size: usize,

    /// Flush a partial batch after this duration, even if not full.
    /// Default 100 ms.
    pub flush_interval_ms: u64,

    /// Bytes an entry's payload and copied header values keep together: a
    /// longer payload is cut and the entry marked `truncated` with its
    /// original length, so it lands where the whole record could not, and a
    /// header value that does not fit is left out. `None` keeps every
    /// payload whole. Default [`DEFAULT_MAX_PAYLOAD_BYTES`].
    pub max_payload_bytes: Option<usize>,

    /// File backend configuration.
    pub file: FileDlqConfig,

    /// Kafka backend configuration.
    #[cfg(feature = "dlq-kafka")]
    pub kafka: KafkaDlqConfig,

    /// HTTP backend configuration.
    #[cfg(feature = "dlq-http")]
    pub http: super::http::HttpDlqConfig,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: DlqMode::default(),
            queue_capacity: 10_000,
            batch_size: 256,
            flush_interval_ms: 100,
            max_payload_bytes: Some(DEFAULT_MAX_PAYLOAD_BYTES),
            file: FileDlqConfig::default(),
            #[cfg(feature = "dlq-kafka")]
            kafka: KafkaDlqConfig::default(),
            #[cfg(feature = "dlq-http")]
            http: super::http::HttpDlqConfig::default(),
        }
    }
}

/// File-based DLQ configuration.
///
/// Writes NDJSON files with automatic rotation and cleanup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct FileDlqConfig {
    /// Enable the file backend.
    pub enabled: bool,

    /// Base directory for DLQ files.
    /// Service name is appended as a subdirectory. Default `/var/spool/scalo/dlq`.
    pub path: PathBuf,

    /// File rotation period.
    pub rotation: RotationPeriod,

    /// Delete rotated files older than this many days, at each rotation.
    /// Unset: 30, or no age limit under `naming: numbered`, which refuses a
    /// set value since its names carry no date.
    pub max_age_days: Option<u32>,

    /// Codec for rotated files past the six newest: `none`, `gzip` or `zstd`.
    /// Default gzip.
    #[cfg_attr(
        feature = "config-schema",
        schemars(schema_with = "crate::io::compress_rotated_schema")
    )]
    pub compress_rotated: Compression,

    /// Size rotation, retention by count, `durable` and file modes.
    #[serde(flatten)]
    pub write: FileWriteOptions,
}

impl Default for FileDlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: PathBuf::from(DEFAULT_SPOOL_ROOT).join("dlq"),
            rotation: RotationPeriod::default(),
            max_age_days: None,
            compress_rotated: Compression::new(Codec::Gzip),
            write: FileWriteOptions::default(),
        }
    }
}

/// The age limit of a stamped file DLQ with `max_age_days` unset.
const DEFAULT_MAX_AGE_DAYS: u32 = 30;

impl FileDlqConfig {
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

/// Kafka-based DLQ configuration.
#[cfg(feature = "dlq-kafka")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct KafkaDlqConfig {
    /// Enable the Kafka backend.
    pub enabled: bool,

    /// Topic routing strategy.
    pub routing: DlqRouting,

    /// Suffix appended to destination for per-table routing.
    pub topic_suffix: String,

    /// Common topic when routing is `Common` or destination is unknown. Unset
    /// by default, which names it after the service the DLQ is spawned for:
    /// `<service>.dlq`, like the per-table `<destination>.dlq`, or `dlq` when
    /// the service has no name. A value that is set is used as it is.
    pub common_topic: Option<String>,

    /// How long a `flush` or the shutdown waits for the broker to ack the
    /// entries queued to Kafka, in milliseconds. Entries still unacked then
    /// are purged and counted as dropped. The purge adds a wait of up to 5 s
    /// for the purged entries' delivery reports, so `0` skips the ack wait
    /// but not that one. Default 5000.
    pub send_timeout_ms: u64,
}

#[cfg(feature = "dlq-kafka")]
impl Default for KafkaDlqConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            routing: DlqRouting::default(),
            topic_suffix: ".dlq".to_string(),
            common_topic: None,
            send_timeout_ms: 5000,
        }
    }
}

#[cfg(feature = "dlq-kafka")]
impl KafkaDlqConfig {
    /// The common topic in force for a DLQ spawned for `service_name`: the
    /// configured [`common_topic`](Self::common_topic) when it is set, else
    /// `<service_name>.dlq`, else `dlq` when the service has no name.
    #[must_use]
    pub fn resolved_common_topic(&self, service_name: &str) -> String {
        if let Some(topic) = &self.common_topic {
            return topic.clone();
        }
        let service_name = service_name.trim();
        if service_name.is_empty() {
            "dlq".to_string()
        } else {
            format!("{service_name}.dlq")
        }
    }
}

/// Kafka DLQ topic routing strategy.
#[cfg(feature = "dlq-kafka")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DlqRouting {
    /// Route to topic matching destination with suffix.
    /// e.g. "acme.auth" -> "acme.auth.dlq"
    #[default]
    PerTable,

    /// Route all failures to a single common topic.
    Common,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = DlqConfig::default();
        assert!(config.enabled);
        assert_eq!(config.mode, DlqMode::Cascade);
        assert!(config.file.enabled);
        assert_eq!(config.file.path, PathBuf::from("/var/spool/scalo/dlq"));
        assert_eq!(config.file.max_age_days, None);
        assert_eq!(config.file.to_writer_config().max_age_days, 30);
        assert_eq!(config.file.compress_rotated, Compression::new(Codec::Gzip));
        assert_eq!(config.file.rotation, RotationPeriod::Hourly);
    }

    #[cfg(feature = "dlq-kafka")]
    #[test]
    fn test_kafka_common_topic_defaults_to_the_service_name() {
        let config = KafkaDlqConfig::default();
        assert_eq!(config.common_topic, None);
        assert_eq!(config.topic_suffix, ".dlq");
        assert_eq!(config.resolved_common_topic("loader"), "loader.dlq");
        assert_eq!(config.resolved_common_topic(" loader "), "loader.dlq");
        assert_eq!(config.resolved_common_topic(""), "dlq");
    }

    /// A set topic is used as it is -- the empty string included, which a
    /// caller that routes per destination passes through unchanged.
    #[cfg(feature = "dlq-kafka")]
    #[test]
    fn test_kafka_common_topic_set_is_taken_as_is() {
        for set in ["acme_loader_dlq", ""] {
            let config = KafkaDlqConfig {
                common_topic: Some(set.into()),
                ..KafkaDlqConfig::default()
            };
            assert_eq!(config.resolved_common_topic("loader"), set);
            assert_eq!(config.resolved_common_topic(""), set);
        }
    }

    /// A config that names a topic loads it, and one that leaves it out loads unset.
    #[cfg(feature = "dlq-kafka")]
    #[test]
    fn test_kafka_common_topic_serde() {
        let set: KafkaDlqConfig =
            serde_json::from_str(r#"{ "common_topic": "acme.dlq" }"#).expect("deserialise");
        assert_eq!(set.common_topic.as_deref(), Some("acme.dlq"));
        let unset: KafkaDlqConfig = serde_json::from_str("{}").expect("deserialise");
        assert_eq!(unset.common_topic, None);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = DlqConfig {
            mode: DlqMode::FanOut,
            file: FileDlqConfig {
                enabled: true,
                path: "/tmp/test-dlq".into(),
                rotation: RotationPeriod::Daily,
                max_age_days: Some(7),
                compress_rotated: crate::compression::Compression::NONE,
                write: crate::io::FileWriteOptions {
                    max_bytes: Some(1 << 20),
                    max_files: Some(4),
                    durable: true,
                    ..crate::io::FileWriteOptions::default()
                },
            },
            queue_capacity: 50_000,
            batch_size: 128,
            flush_interval_ms: 250,
            ..DlqConfig::default()
        };
        let json = serde_json::to_string(&config).expect("serialise");
        let parsed: DlqConfig = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(parsed.mode, DlqMode::FanOut);
        assert_eq!(parsed.file.rotation, RotationPeriod::Daily);
        assert_eq!(parsed.file.max_age_days, Some(7));
        assert_eq!(parsed.file.write, config.file.write);
        assert!(
            json.contains(r#""max_files":4"#),
            "the write keys sit in `file`: {json}"
        );
        assert_eq!(parsed.queue_capacity, 50_000);
        assert_eq!(parsed.batch_size, 128);
        assert_eq!(parsed.flush_interval_ms, 250);
    }

    /// Unset, `max_age_days` is 30 under `stamped` and no limit under
    /// `numbered`, so a numbered DLQ opens. A set value is kept, and numbered
    /// refuses it. A config that sets the key still loads.
    #[test]
    fn numbered_naming_opens_with_no_age_limit() {
        use crate::io::NdjsonWriter;
        use crate::io::RotatedNaming::{Numbered, Stamped};

        let dir = tempfile::tempdir().expect("tempdir");
        let file = |naming, max_age_days| FileDlqConfig {
            path: dir.path().to_path_buf(),
            max_age_days,
            compress_rotated: Compression::NONE,
            write: FileWriteOptions {
                naming,
                ..FileWriteOptions::default()
            },
            ..FileDlqConfig::default()
        };
        // (case, config, the writer's age limit with u32::MAX for none, opens)
        let cases = [
            ("stamped-unset", file(Stamped, None), 30, true),
            ("stamped-set", file(Stamped, Some(5)), 5, true),
            ("numbered-unset", file(Numbered, None), u32::MAX, true),
            ("numbered-set", file(Numbered, Some(5)), 5, false),
        ];
        for (case, config, age, opens) in cases {
            let writer = config.to_writer_config();
            assert_eq!(writer.max_age_days, age, "{case}");
            let opened = NdjsonWriter::new(&writer, case, "dlq.ndjson", "dlq");
            assert_eq!(opened.is_ok(), opens, "{case}");
        }
        for (json, want) in [
            (r#"{"max_age_days":30}"#, Some(30)),
            ("{}", None),
            (r#"{"max_age_days":null}"#, None),
        ] {
            let parsed: FileDlqConfig = serde_json::from_str(json).expect("deserialise");
            assert_eq!(parsed.max_age_days, want, "{json}");
        }
    }

    /// `docs/pipeline/dlq.md` documents this default in its config example.
    #[cfg(feature = "dlq-kafka")]
    #[test]
    fn test_kafka_send_timeout_defaults_to_the_documented_5000_ms() {
        assert_eq!(KafkaDlqConfig::default().send_timeout_ms, 5000);
    }

    #[test]
    fn test_dlq_mode_serde() {
        let json = r#""cascade""#;
        let mode: DlqMode = serde_json::from_str(json).expect("deserialise");
        assert_eq!(mode, DlqMode::Cascade);

        let json = r#""fan_out""#;
        let mode: DlqMode = serde_json::from_str(json).expect("deserialise");
        assert_eq!(mode, DlqMode::FanOut);
    }
}
