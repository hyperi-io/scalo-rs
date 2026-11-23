// Project:   scalo
// File:      src/transport/types.rs
// Purpose:   Transport data types and configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use super::error::TransportError;
use super::traits::CommitToken;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Transport type selection, the `type` key of a [`TransportConfig`].
///
/// A name that is not a built-in type parses as [`Custom`](Self::Custom), which
/// the factory looks up among the senders registered with
/// [`register_sender`](super::register_sender).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TransportType {
    /// Apache Kafka (production default).
    #[default]
    Kafka,
    /// Native gRPC (tonic, inter-service mesh).
    Grpc,
    /// In-memory tokio channels (unit tests).
    Memory,
    /// NDJSON file (debugging, audit trails, replay).
    File,
    /// Unix pipe (stdin/stdout, sidecar pattern).
    Pipe,
    /// HTTP/HTTPS (webhook delivery, REST ingest).
    Http,
    /// Syslog over TCP, TLS, UDP or RELP (send only).
    Syslog,
    /// OTLP log records to an OpenTelemetry collector (send only).
    Otlp,
    /// A sender registered under this type name.
    Custom(String),
}

impl TransportType {
    /// The built-in type names, which no custom sender may take.
    pub const BUILT_IN: &'static [&'static str] = &[
        "kafka", "grpc", "memory", "file", "pipe", "http", "syslog", "otlp",
    ];

    /// The config name of this type.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Kafka => "kafka",
            Self::Grpc => "grpc",
            Self::Memory => "memory",
            Self::File => "file",
            Self::Pipe => "pipe",
            Self::Http => "http",
            Self::Syslog => "syslog",
            Self::Otlp => "otlp",
            Self::Custom(name) => name,
        }
    }

    /// The type a config name selects: a built-in type, else `Custom`.
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        match name {
            "kafka" => Self::Kafka,
            "grpc" => Self::Grpc,
            "memory" => Self::Memory,
            "file" => Self::File,
            "pipe" => Self::Pipe,
            "http" => Self::Http,
            "syslog" => Self::Syslog,
            "otlp" => Self::Otlp,
            other => Self::Custom(other.to_string()),
        }
    }
}

impl std::fmt::Display for TransportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for TransportType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TransportType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        if name.is_empty() {
            return Err(serde::de::Error::custom("transport type is empty"));
        }
        Ok(Self::from_name(&name))
    }
}

/// Payload format (auto-detected or explicit).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    /// Auto-detect from payload bytes.
    #[default]
    Auto,
    /// JSON format.
    Json,
    /// MessagePack format.
    MsgPack,
}

impl PayloadFormat {
    /// Detect format from the first payload byte.
    ///
    /// MsgPack map/array lead bytes (0x80-0x9f, 0xdc-0xdf) -> MsgPack;
    /// everything else (incl. empty) -> JSON.
    #[must_use]
    pub fn detect(payload: &[u8]) -> Self {
        if payload.is_empty() {
            return Self::Json;
        }

        // MsgPack: fixmap (0x80-0x8f), map16/32 (0xde/0xdf), fixarray (0x90-0x9f), array16/32 (0xdc/0xdd)
        if matches!(payload[0], 0x80..=0x8f | 0xde | 0xdf | 0x90..=0x9f | 0xdc | 0xdd) {
            Self::MsgPack
        } else {
            // JSON object/array or whitespace-prefixed JSON - default to JSON
            Self::Json
        }
    }
}

/// A received message with transport metadata.
#[derive(Debug, Clone)]
pub struct Message<T: CommitToken> {
    /// Routing key (Kafka topic, gRPC metadata topic).
    pub key: Option<Arc<str>>,

    /// Raw payload bytes -- zero-copy / refcounted. JSON or MsgPack, unchanged.
    pub payload: Bytes,

    /// Transport-specific commit token.
    pub token: T,

    /// Message timestamp from transport layer (milliseconds since epoch).
    pub timestamp_ms: Option<i64>,

    /// Detected payload format.
    pub format: PayloadFormat,

    /// Record headers the source carried, the dedup key among them. Empty for
    /// a source with no headers on the wire.
    pub headers: Vec<(String, Vec<u8>)>,
}

impl<T: CommitToken> Message<T> {
    /// Create a new message with auto-detected format.
    ///
    /// `payload` accepts any `impl Into<Bytes>` -- pass a `Vec<u8>` and it is
    /// moved (zero copy). Pass a `Bytes` slice and the refcount is bumped.
    #[must_use]
    pub fn new(
        key: Option<Arc<str>>,
        payload: impl Into<Bytes>,
        token: T,
        timestamp_ms: Option<i64>,
    ) -> Self {
        let payload = payload.into();
        let format = PayloadFormat::detect(&payload);
        Self {
            key,
            payload,
            token,
            timestamp_ms,
            format,
            headers: Vec::new(),
        }
    }

    /// Returns true if payload is JSON.
    #[must_use]
    pub fn is_json(&self) -> bool {
        self.format == PayloadFormat::Json
    }

    /// Returns true if payload is MsgPack.
    #[must_use]
    pub fn is_msgpack(&self) -> bool {
        self.format == PayloadFormat::MsgPack
    }
}

/// Result of a send operation.
///
/// Four answers besides `Ok` tell the caller what to do with the records:
///
/// | Variant | Decided by | Retry | The caller |
/// |---|---|---|---|
/// | `Backpressured` | the transport or a busy destination | yes, on its own schedule | sends the same records again |
/// | `RetryAfter(wait)` | a busy destination that named a wait | yes, after `wait` | sends the same records again after `wait` |
/// | `Reject(error)` | the destination, for good | never | dead-letters the records, one at a time when the batch names none |
/// | `FilteredDlq` | scalo, before the destination: a filter or the size cap | never | dead-letters what [`dead_letter_reason`](super::TransportSender::dead_letter_reason) names |
///
/// `Fatal` is none of these: the transport itself cannot go on (closed,
/// misconfigured), so the caller stops, or keeps the records until it is fixed.
#[derive(Debug)]
pub enum SendResult {
    /// Message accepted.
    Ok,
    /// Transport is backpressured, retry later.
    Backpressured,
    /// The destination is busy and asked for this wait before the next try.
    RetryAfter(std::time::Duration),
    /// Fatal error, cannot continue.
    Fatal(TransportError),
    /// The destination refused the records permanently. Never retry them: the
    /// same bytes get the same answer.
    Reject(TransportError),
    /// The record was HANDLED rather than sent, and belongs in a dead-letter
    /// queue: it matched an outbound filter with `action: dlq`, or the
    /// transport refused it permanently (a payload over the message-size
    /// ceiling). Caller is responsible for DLQ routing. Never retry it --
    /// the same bytes produce the same outcome.
    FilteredDlq,
}

impl SendResult {
    /// Returns true if send was successful.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Returns true if backpressured (should retry).
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        matches!(self, Self::Backpressured)
    }

    /// Returns true if fatal error.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }

    /// Returns true if filtered for DLQ routing.
    #[must_use]
    pub fn is_filtered_dlq(&self) -> bool {
        matches!(self, Self::FilteredDlq)
    }

    /// Returns true if the destination refused the records for good.
    #[must_use]
    pub fn is_reject(&self) -> bool {
        matches!(self, Self::Reject(_))
    }

    /// Returns true for `Backpressured` and `RetryAfter`: send the same records again.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Backpressured | Self::RetryAfter(_))
    }

    /// The wait the destination asked for, on a `RetryAfter`.
    #[must_use]
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::RetryAfter(wait) => Some(*wait),
            _ => None,
        }
    }
}

/// Folds the results of a block sent in pieces into the block's one result.
///
/// `Ok` goes on. `FilteredDlq` goes on and is remembered, since the rest of
/// the block is still sent. Any other result ends the block.
#[derive(Debug, Default)]
pub(crate) struct BlockOutcome {
    dead_lettered: bool,
}

impl BlockOutcome {
    /// Take one piece's result: `Some` when it ends the block.
    pub(crate) fn add(&mut self, result: SendResult) -> Option<SendResult> {
        match result {
            SendResult::Ok => None,
            SendResult::FilteredDlq => {
                self.dead_lettered = true;
                None
            }
            ended => Some(ended),
        }
    }

    /// The block's result once every piece was taken.
    pub(crate) fn finish(self) -> SendResult {
        if self.dead_lettered {
            SendResult::FilteredDlq
        } else {
            SendResult::Ok
        }
    }
}

/// Top-level transport configuration.
///
/// Used by the transport factory to create the right backend from config.
/// Each transport type has its own optional config section -- only the one
/// matching `transport_type` is read.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Transport type (kafka, grpc, memory, file, pipe, http, syslog, otlp), or the
    /// name a custom sender was registered under.
    #[serde(rename = "type", default)]
    pub transport_type: TransportType,

    /// The weakest delivery guarantee a sender built from this config may
    /// have: `ack` (default), `flush` or `none`. The factory refuses a sender
    /// below it. Receivers ignore it.
    #[serde(default)]
    pub delivery: super::delivery::DeliveryRequirement,

    /// Expected payload format (auto-detect by default).
    #[serde(default)]
    pub payload_format: PayloadFormat,

    /// Kafka-specific configuration.
    #[cfg(feature = "transport-kafka")]
    #[serde(default)]
    pub kafka: Option<super::kafka::KafkaConfig>,

    /// gRPC transport configuration.
    #[cfg(feature = "transport-grpc")]
    #[serde(default)]
    pub grpc: Option<super::grpc::GrpcConfig>,

    /// Memory transport configuration (for tests).
    #[cfg(feature = "transport-memory")]
    #[serde(default)]
    pub memory: Option<super::memory::MemoryConfig>,

    /// Pipe transport configuration (stdin/stdout).
    #[cfg(feature = "transport-pipe")]
    #[serde(default)]
    pub pipe: Option<super::pipe::PipeTransportConfig>,

    /// File transport configuration (NDJSON file I/O).
    #[cfg(feature = "transport-file")]
    #[serde(default)]
    pub file: Option<super::file::FileTransportConfig>,

    /// HTTP transport configuration (webhook delivery, REST ingest).
    #[cfg(feature = "transport-http")]
    #[serde(default)]
    pub http: Option<super::http::HttpTransportConfig>,

    /// Syslog transport configuration (send only).
    #[cfg(feature = "transport-syslog")]
    #[serde(default)]
    pub syslog: Option<super::syslog::SyslogConfig>,

    /// OTLP logs transport configuration (send only).
    #[cfg(feature = "transport-otlp")]
    #[serde(default)]
    pub otlp: Option<super::otlp::OtlpConfig>,

    // Placeholder fields when features are disabled
    #[cfg(not(feature = "transport-kafka"))]
    #[serde(default, skip)]
    pub kafka: Option<()>,

    #[cfg(not(feature = "transport-grpc"))]
    #[serde(default, skip)]
    pub grpc: Option<()>,

    #[cfg(not(feature = "transport-memory"))]
    #[serde(default, skip)]
    pub memory: Option<()>,

    #[cfg(not(feature = "transport-pipe"))]
    #[serde(default, skip)]
    pub pipe: Option<()>,

    #[cfg(not(feature = "transport-file"))]
    #[serde(default, skip)]
    pub file: Option<()>,

    #[cfg(not(feature = "transport-http"))]
    #[serde(default, skip)]
    pub http: Option<()>,

    #[cfg(not(feature = "transport-syslog"))]
    #[serde(default, skip)]
    pub syslog: Option<()>,
    #[cfg(not(feature = "transport-otlp"))]
    #[serde(default, skip)]
    pub otlp: Option<()>,

    /// Every other key, by name. A custom sender reads its section from the
    /// key named after its type.
    #[serde(flatten, default)]
    pub custom: std::collections::BTreeMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_json_object() {
        assert_eq!(PayloadFormat::detect(b"{\"foo\":1}"), PayloadFormat::Json);
    }

    #[test]
    fn detect_json_array() {
        assert_eq!(PayloadFormat::detect(b"[1,2,3]"), PayloadFormat::Json);
    }

    #[test]
    fn detect_msgpack_fixmap() {
        // fixmap with 1 element: 0x81
        assert_eq!(PayloadFormat::detect(&[0x81, 0xa3]), PayloadFormat::MsgPack);
    }

    #[test]
    fn detect_msgpack_map16() {
        assert_eq!(
            PayloadFormat::detect(&[0xde, 0x00, 0x10]),
            PayloadFormat::MsgPack
        );
    }

    #[test]
    fn detect_empty_defaults_json() {
        assert_eq!(PayloadFormat::detect(&[]), PayloadFormat::Json);
    }

    #[test]
    fn each_send_result_says_whether_to_retry_and_after_what() {
        use std::time::Duration;
        let wait = Duration::from_secs(3);
        // (result, ok, retryable, reject, dead-letter, fatal, asked wait)
        let cases = [
            (SendResult::Ok, true, false, false, false, false, None),
            (
                SendResult::Backpressured,
                false,
                true,
                false,
                false,
                false,
                None,
            ),
            (
                SendResult::RetryAfter(wait),
                false,
                true,
                false,
                false,
                false,
                Some(wait),
            ),
            (
                SendResult::Reject(TransportError::Send("400".into())),
                false,
                false,
                true,
                false,
                false,
                None,
            ),
            (
                SendResult::FilteredDlq,
                false,
                false,
                false,
                true,
                false,
                None,
            ),
            (
                SendResult::Fatal(TransportError::Closed),
                false,
                false,
                false,
                false,
                true,
                None,
            ),
        ];
        for (result, ok, retryable, reject, dlq, fatal, asked) in cases {
            let got = (
                result.is_ok(),
                result.is_retryable(),
                result.is_reject(),
                result.is_filtered_dlq(),
                result.is_fatal(),
                result.retry_after(),
            );
            assert_eq!(
                got,
                (ok, retryable, reject, dlq, fatal, asked),
                "{result:?}"
            );
        }
    }

    #[test]
    fn a_block_ends_on_the_first_result_that_is_not_handled() {
        use std::time::Duration;
        let reject = || SendResult::Reject(TransportError::Send("400".into()));
        // (case, results in order, the block's result as text)
        let cases: Vec<(&str, Vec<SendResult>, &str)> = vec![
            ("empty", vec![], "Ok"),
            ("all sent", vec![SendResult::Ok, SendResult::Ok], "Ok"),
            (
                "one dead letter",
                vec![SendResult::Ok, SendResult::FilteredDlq],
                "FilteredDlq",
            ),
            (
                "reject ends it",
                vec![SendResult::FilteredDlq, reject(), SendResult::Backpressured],
                "Reject",
            ),
            (
                "wait ends it",
                vec![
                    SendResult::Ok,
                    SendResult::RetryAfter(Duration::from_secs(1)),
                    reject(),
                ],
                "RetryAfter",
            ),
            (
                "backpressure ends it",
                vec![SendResult::Backpressured, reject()],
                "Backpressured",
            ),
            (
                "fatal ends it",
                vec![SendResult::Ok, SendResult::Fatal(TransportError::Closed)],
                "Fatal",
            ),
        ];
        for (case, results, want) in cases {
            let mut outcome = BlockOutcome::default();
            let ended = results.into_iter().find_map(|r| outcome.add(r));
            let got = ended.unwrap_or_else(|| outcome.finish());
            assert!(format!("{got:?}").starts_with(want), "{case}: {got:?}");
        }
    }

    #[test]
    fn a_type_name_parses_as_a_built_in_or_a_custom_type() {
        // (config value, type)
        let cases = [
            ("kafka", TransportType::Kafka),
            ("grpc", TransportType::Grpc),
            ("memory", TransportType::Memory),
            ("file", TransportType::File),
            ("pipe", TransportType::Pipe),
            ("http", TransportType::Http),
            ("syslog", TransportType::Syslog),
            ("otlp", TransportType::Otlp),
            ("audit_log", TransportType::Custom("audit_log".into())),
            ("Kafka", TransportType::Custom("Kafka".into())),
        ];
        for (text, want) in cases {
            let parsed: TransportType =
                serde_json::from_value(serde_json::Value::String(text.into())).unwrap();
            assert_eq!(parsed, want, "{text}");
            assert_eq!(
                serde_json::to_value(&parsed).unwrap(),
                text,
                "{text} round trip"
            );
        }
        for name in TransportType::BUILT_IN {
            assert_eq!(TransportType::from_name(name).as_str(), *name);
            assert!(!matches!(
                TransportType::from_name(name),
                TransportType::Custom(_)
            ));
        }
        assert!(
            serde_json::from_value::<TransportType>(serde_json::Value::String(String::new()))
                .is_err()
        );
        assert!(serde_json::from_value::<TransportType>(serde_json::json!(5)).is_err());
    }

    #[test]
    fn a_config_keeps_delivery_and_the_custom_sections() {
        let config: TransportConfig = serde_json::from_value(serde_json::json!({
            "type": "audit_log",
            "delivery": "flush",
            "audit_log": {"path": "/var/audit", "batch": 10},
        }))
        .unwrap();
        assert_eq!(
            config.transport_type,
            TransportType::Custom("audit_log".into())
        );
        assert_eq!(
            config.delivery,
            crate::transport::DeliveryRequirement::Flush
        );
        assert_eq!(
            config.custom.get("audit_log"),
            Some(&serde_json::json!({"path": "/var/audit", "batch": 10}))
        );

        let plain: TransportConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(plain.transport_type, TransportType::Kafka);
        assert_eq!(plain.delivery, crate::transport::DeliveryRequirement::Ack);
        assert!(plain.custom.is_empty());

        let bad =
            serde_json::from_value::<TransportConfig>(serde_json::json!({"delivery": "maybe"}));
        assert!(bad.is_err(), "an unknown delivery value is refused");
    }
}
