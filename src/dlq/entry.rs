// Project:   scalo
// File:      src/dlq/entry.rs
// Purpose:   Shared DLQ entry envelope format
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared DLQ entry types used by all backends.

use base64::Engine;
use serde::{Deserialize, Serialize};

/// A failed message routed to the dead letter queue.
///
/// This envelope is backend-agnostic -- it carries the original payload
/// plus metadata about why and where the failure occurred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// When the failure occurred (epoch milliseconds).
    pub timestamp: i64,

    /// Service that generated this entry (e.g. "loader", "receiver").
    pub service: String,

    /// Why the message failed processing.
    pub reason: String,

    /// How many times the message was retried before being sent to DLQ.
    pub attempts: u32,

    /// Original message payload (base64-encoded in JSON), or its first bytes
    /// when `truncated`.
    #[serde(
        serialize_with = "serialize_payload",
        deserialize_with = "deserialize_payload"
    )]
    pub payload: Vec<u8>,

    /// Whether `payload` holds only the start of the original.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,

    /// The original payload's length in bytes, when `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_bytes: Option<usize>,

    /// Intended destination (e.g. "db.table" or topic name).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,

    /// Where the message originated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<DlqSource>,
}

/// Source metadata for a DLQ entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqSource {
    /// Original Kafka topic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    /// Original Kafka partition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,

    /// Original Kafka offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
}

impl DlqEntry {
    /// Create a new DLQ entry with the current timestamp.
    pub fn new(service: impl Into<String>, reason: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            timestamp: chrono::Utc::now().timestamp_millis(),
            service: service.into(),
            reason: reason.into(),
            attempts: 0,
            payload,
            truncated: false,
            original_bytes: None,
            destination: None,
            source: None,
        }
    }

    /// Set the intended destination.
    #[must_use]
    pub fn with_destination(mut self, dest: impl Into<String>) -> Self {
        self.destination = Some(dest.into());
        self
    }

    /// Set the source metadata.
    #[must_use]
    pub fn with_source(mut self, source: DlqSource) -> Self {
        self.source = Some(source);
        self
    }

    /// Set the retry attempt count.
    #[must_use]
    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Keep the first `max_bytes` of a longer payload, marking the entry
    /// `truncated` with the original length. A payload within it is kept
    /// whole.
    #[must_use]
    pub fn truncate_to(mut self, max_bytes: usize) -> Self {
        let len = self.payload.len();
        if len > max_bytes {
            self.payload.truncate(max_bytes);
            self.payload.shrink_to_fit();
            self.truncated = true;
            self.original_bytes = Some(self.original_bytes.unwrap_or(len));
        }
        self
    }
}

impl DlqSource {
    /// Create source metadata from Kafka coordinates.
    pub fn kafka(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: Some(topic.into()),
            partition: Some(partition),
            offset: Some(offset),
        }
    }
}

fn serialize_payload<S>(payload: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
    serializer.serialize_str(&encoded)
}

fn deserialize_payload<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    base64::engine::general_purpose::STANDARD
        .decode(&s)
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_new() {
        let entry = DlqEntry::new("loader", "schema_mismatch", b"test payload".to_vec());
        assert_eq!(entry.service, "loader");
        assert_eq!(entry.reason, "schema_mismatch");
        assert_eq!(entry.payload, b"test payload");
        assert_eq!(entry.attempts, 0);
        assert!(entry.destination.is_none());
        assert!(entry.source.is_none());
        assert!(entry.timestamp > 0);
    }

    #[test]
    fn test_entry_builders() {
        let entry = DlqEntry::new("receiver", "parse_error", b"bad json".to_vec())
            .with_destination("acme.auth")
            .with_source(DlqSource::kafka("events", 3, 12345))
            .with_attempts(2);

        assert_eq!(entry.destination.as_deref(), Some("acme.auth"));
        assert_eq!(entry.attempts, 2);
        let src = entry.source.as_ref().expect("source should be set");
        assert_eq!(src.topic.as_deref(), Some("events"));
        assert_eq!(src.partition, Some(3));
        assert_eq!(src.offset, Some(12345));
    }

    #[test]
    fn truncate_to_keeps_the_start_and_says_how_long_it_was() {
        let entry = |len: usize| DlqEntry::new("svc", "r", vec![b'x'; len]);
        // (case, payload length, cap, kept, original_bytes)
        let cases = [
            ("under the cap", 3, 4, 3, None),
            ("at the cap", 4, 4, 4, None),
            ("one byte over", 5, 4, 4, Some(5)),
            ("a zero cap", 5, 0, 0, Some(5)),
        ];
        for (name, len, cap, kept, original) in cases {
            let capped = entry(len).truncate_to(cap);
            assert_eq!(capped.payload.len(), kept, "{name}");
            assert_eq!(capped.original_bytes, original, "{name}");
            assert_eq!(capped.truncated, original.is_some(), "{name}");
        }
        let twice = entry(10).truncate_to(6).truncate_to(2);
        assert_eq!(
            twice.original_bytes,
            Some(10),
            "a second cut keeps the first length"
        );
    }

    #[test]
    fn only_a_truncated_entry_carries_the_truncation_fields() {
        let whole = serde_json::to_value(DlqEntry::new("svc", "r", b"abc".to_vec())).expect("json");
        assert!(whole.get("truncated").is_none() && whole.get("original_bytes").is_none());

        let cut = DlqEntry::new("svc", "r", b"abcdef".to_vec()).truncate_to(2);
        let json = serde_json::to_string(&cut).expect("serialise");
        let value: serde_json::Value = serde_json::from_str(&json).expect("json");
        assert_eq!(value["truncated"], true);
        assert_eq!(value["original_bytes"], 6);
        let parsed: DlqEntry = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(
            (parsed.payload, parsed.truncated, parsed.original_bytes),
            (b"ab".to_vec(), true, Some(6))
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        let entry = DlqEntry::new("loader", "type_error", b"\x00\x01\x02\xff".to_vec())
            .with_destination("db.table")
            .with_source(DlqSource::kafka("topic", 0, 999));

        let json = serde_json::to_string(&entry).expect("serialise");
        let parsed: DlqEntry = serde_json::from_str(&json).expect("deserialise");

        assert_eq!(parsed.service, entry.service);
        assert_eq!(parsed.reason, entry.reason);
        assert_eq!(parsed.payload, entry.payload);
        assert_eq!(parsed.destination, entry.destination);
        assert_eq!(parsed.attempts, entry.attempts);
    }

    #[test]
    fn test_payload_base64_encoding() {
        let entry = DlqEntry::new("test", "reason", b"hello world".to_vec());
        let json = serde_json::to_string(&entry).expect("serialise");
        // base64 of "hello world" is "aGVsbG8gd29ybGQ="
        assert!(json.contains("aGVsbG8gd29ybGQ="));
    }
}
