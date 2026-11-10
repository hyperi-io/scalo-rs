// Project:   scalo
// File:      src/dlq/entry.rs
// Purpose:   Shared DLQ entry envelope format
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared DLQ entry types used by all backends.

use std::borrow::Cow;

use base64::Engine;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

/// A failed message routed to the dead letter queue.
///
/// This envelope is backend-agnostic -- it carries the original payload
/// plus metadata about why and where the failure occurred.
///
/// It is written as a DFE event. A payload that is one whole JSON object goes under `_json` as itself, with its `_org_id`, `_source`, `_timestamp_received` and `_tags` copied beside it. Any other payload goes under `_raw`: valid UTF-8 as text, binary or truncated bytes as base64 with `_raw_encoding: "base64"`.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    /// When the failure occurred (epoch milliseconds).
    pub timestamp: i64,

    /// Service that generated this entry (e.g. "loader", "receiver").
    pub service: String,

    /// Why the message failed processing.
    pub reason: String,

    /// How many times the message was retried before being sent to DLQ.
    pub attempts: u32,

    /// Original message payload, or its first bytes when `truncated`.
    pub payload: Vec<u8>,

    /// Whether `payload` holds only the start of the original.
    pub truncated: bool,

    /// The original payload's length in bytes, when `truncated`.
    pub original_bytes: Option<usize>,

    /// Intended destination (e.g. "db.table" or topic name).
    pub destination: Option<String>,

    /// Where the message originated.
    pub source: Option<DlqSource>,

    /// The original's common-header fields, read before `truncate_to` cut it.
    header: Option<Header>,
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

/// The DFE common-header fields an entry copies from its original event, as written there.
#[derive(Debug, Clone, Default, Deserialize)]
struct Header {
    #[serde(default, rename = "_org_id")]
    org_id: Option<Box<RawValue>>,
    #[serde(default, rename = "_source")]
    source: Option<Box<RawValue>>,
    #[serde(default, rename = "_timestamp_received")]
    timestamp_received: Option<Box<RawValue>>,
    #[serde(default, rename = "_tags")]
    tags: Option<Box<RawValue>>,
}

/// How `_raw` holds the payload when it is not plain text.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawEncoding {
    Base64,
}

/// An entry as written. `RawValue` cannot sit under `#[serde(flatten)]`, so the header fields are listed here.
#[derive(Serialize)]
struct WireOut<'a> {
    timestamp: i64,
    service: &'a str,
    reason: &'a str,
    attempts: u32,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<&'a DlqSource>,
    #[serde(rename = "_org_id", skip_serializing_if = "Option::is_none")]
    org_id: Option<&'a RawValue>,
    #[serde(rename = "_source", skip_serializing_if = "Option::is_none")]
    origin: Option<&'a RawValue>,
    #[serde(
        rename = "_timestamp_received",
        skip_serializing_if = "Option::is_none"
    )]
    timestamp_received: Option<&'a RawValue>,
    #[serde(rename = "_tags", skip_serializing_if = "Option::is_none")]
    tags: Option<&'a RawValue>,
    #[serde(rename = "_json", skip_serializing_if = "Option::is_none")]
    json: Option<&'a RawValue>,
    #[serde(rename = "_raw", skip_serializing_if = "Option::is_none")]
    raw: Option<Cow<'a, str>>,
    #[serde(rename = "_raw_encoding", skip_serializing_if = "Option::is_none")]
    raw_encoding: Option<RawEncoding>,
}

/// An entry as read back.
#[derive(Deserialize)]
struct WireIn {
    timestamp: i64,
    service: String,
    reason: String,
    #[serde(default)]
    attempts: u32,
    #[serde(default)]
    truncated: bool,
    #[serde(default)]
    original_bytes: Option<usize>,
    #[serde(default)]
    destination: Option<String>,
    #[serde(default)]
    source: Option<DlqSource>,
    #[serde(default, rename = "_org_id")]
    org_id: Option<Box<RawValue>>,
    #[serde(default, rename = "_source")]
    origin: Option<Box<RawValue>>,
    #[serde(default, rename = "_timestamp_received")]
    timestamp_received: Option<Box<RawValue>>,
    #[serde(default, rename = "_tags")]
    tags: Option<Box<RawValue>>,
    #[serde(default, rename = "_json")]
    json: Option<Box<RawValue>>,
    #[serde(default, rename = "_raw")]
    raw: Option<String>,
    #[serde(default, rename = "_raw_encoding")]
    raw_encoding: Option<RawEncoding>,
}

impl Header {
    fn is_empty(&self) -> bool {
        self.org_id.is_none()
            && self.source.is_none()
            && self.timestamp_received.is_none()
            && self.tags.is_none()
    }
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
            header: None,
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
    ///
    /// The original's common-header fields are read first, so the entry still carries them.
    #[must_use]
    pub fn truncate_to(mut self, max_bytes: usize) -> Self {
        let len = self.payload.len();
        if len > max_bytes {
            if self.header.is_none() {
                self.header = object_text(&self.payload)
                    .and_then(|text| serde_json::from_str::<Header>(&text).ok());
            }
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

impl Serialize for DlqEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let object = if self.truncated {
            None
        } else {
            json_object(&self.payload)
        };
        let text = match (&object, self.truncated) {
            (None, false) => std::str::from_utf8(&self.payload).ok(),
            _ => None,
        };
        let header = match &object {
            Some((header, _)) => Some(header),
            None => self.header.as_ref(),
        };
        let (raw, raw_encoding) = match (&object, text) {
            (Some(_), _) => (None, None),
            (None, Some(text)) => (Some(Cow::Borrowed(text)), None),
            (None, None) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(&self.payload);
                (Some(Cow::Owned(encoded)), Some(RawEncoding::Base64))
            }
        };
        WireOut {
            timestamp: self.timestamp,
            service: &self.service,
            reason: &self.reason,
            attempts: self.attempts,
            truncated: self.truncated,
            original_bytes: self.original_bytes,
            destination: self.destination.as_deref(),
            source: self.source.as_ref(),
            org_id: header.and_then(|h| h.org_id.as_deref()),
            origin: header.and_then(|h| h.source.as_deref()),
            timestamp_received: header.and_then(|h| h.timestamp_received.as_deref()),
            tags: header.and_then(|h| h.tags.as_deref()),
            json: object.as_ref().map(|(_, json)| json.as_ref()),
            raw,
            raw_encoding,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DlqEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireIn::deserialize(deserializer)?;
        let header = Header {
            org_id: wire.org_id,
            source: wire.origin,
            timestamp_received: wire.timestamp_received,
            tags: wire.tags,
        };
        let payload = match (wire.json, wire.raw, wire.raw_encoding) {
            (Some(json), None, None) => json.get().as_bytes().to_vec(),
            (None, Some(raw), None) => raw.into_bytes(),
            (None, Some(raw), Some(RawEncoding::Base64)) => {
                base64::engine::general_purpose::STANDARD
                    .decode(raw)
                    .map_err(D::Error::custom)?
            }
            _ => {
                return Err(D::Error::custom(
                    "a DLQ entry holds `_json`, or `_raw` with an optional `_raw_encoding`",
                ));
            }
        };
        Ok(Self {
            timestamp: wire.timestamp,
            service: wire.service,
            reason: wire.reason,
            attempts: wire.attempts,
            payload,
            truncated: wire.truncated,
            original_bytes: wire.original_bytes,
            destination: wire.destination,
            source: wire.source,
            header: (!header.is_empty()).then_some(header),
        })
    }
}

/// The payload as one JSON object, with its common-header fields.
fn json_object(payload: &[u8]) -> Option<(Header, Box<RawValue>)> {
    let text = object_text(payload)?;
    let header: Header = serde_json::from_str(&text).ok()?;
    let json = RawValue::from_string(text).ok()?;
    Some((header, json))
}

/// The payload's text when it opens a JSON object, with raw newlines as spaces.
///
/// Valid JSON holds a raw newline only between tokens, so the change keeps its meaning and a file line holds one entry.
fn object_text(payload: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(payload).ok()?.trim_ascii();
    text.starts_with('{')
        .then(|| text.replace(['\n', '\r'], " "))
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
    fn the_original_lands_under_json_or_raw_by_what_it_is() {
        let event = br#"{"_org_id":"acme","_source":"aws-cloudtrail","_timestamp_received":"2026-11-10T00:00:00Z","_tags":{"env":"dev"},"eventVersion":"1.11"}"#;
        let header = r#""_org_id":"acme","_source":"aws-cloudtrail","_timestamp_received":"2026-11-10T00:00:00Z","_tags":{"env":"dev"}"#;
        // (case, payload, what follows `"attempts":0,` as written, payload read back)
        let cases: [(&str, &[u8], String, &[u8]); 9] = [
            (
                "event",
                event,
                format!(r#"{header},"_json":{}}}"#, String::from_utf8_lossy(event)),
                event,
            ),
            (
                "object without a header",
                br#"{"a":1}"#,
                r#""_json":{"a":1}}"#.to_owned(),
                br#"{"a":1}"#,
            ),
            (
                "pretty-printed object",
                b"{\n  \"a\": 1\r\n}\n",
                r#""_json":{   "a": 1  }}"#.to_owned(),
                br#"{   "a": 1  }"#,
            ),
            ("array", b"[1,2]", r#""_raw":"[1,2]"}"#.to_owned(), b"[1,2]"),
            (
                "text",
                b"hello\nworld",
                r#""_raw":"hello\nworld"}"#.to_owned(),
                b"hello\nworld",
            ),
            ("empty", b"", r#""_raw":""}"#.to_owned(), b""),
            (
                "cut object",
                br#"{"a":"#,
                r#""_raw":"{\"a\":"}"#.to_owned(),
                br#"{"a":"#,
            ),
            (
                "object with trailing text",
                br#"{"a":1} x"#,
                r#""_raw":"{\"a\":1} x"}"#.to_owned(),
                br#"{"a":1} x"#,
            ),
            (
                "binary",
                b"\x00\x01\xff",
                r#""_raw":"AAH/","_raw_encoding":"base64"}"#.to_owned(),
                b"\x00\x01\xff",
            ),
        ];
        for (name, payload, written, read_back) in cases {
            let entry = DlqEntry {
                timestamp: 1,
                ..DlqEntry::new("svc", "r", payload.to_vec())
            };
            let json = serde_json::to_string(&entry).expect("serialise");
            let expected =
                format!(r#"{{"timestamp":1,"service":"svc","reason":"r","attempts":0,{written}"#);
            assert_eq!(json, expected, "{name}");
            assert!(!json.contains('\n'), "{name}: one entry per line");
            let parsed: DlqEntry = serde_json::from_str(&json).expect("deserialise");
            assert_eq!(parsed.payload, read_back, "{name}");
        }
    }

    #[test]
    fn a_truncated_event_keeps_its_header_and_goes_under_raw_as_base64() {
        let event = br#"{"_org_id":"acme","_tags":{},"eventVersion":"1.11"}"#;
        let cut = DlqEntry::new("svc", "r", event.to_vec()).truncate_to(8);

        let json = serde_json::to_string(&cut).expect("serialise");
        let value: serde_json::Value = serde_json::from_str(&json).expect("json");
        assert_eq!(value["_org_id"], "acme");
        assert_eq!(value["_tags"], serde_json::json!({}));
        assert_eq!(value["_raw_encoding"], "base64");
        assert!(value.get("_json").is_none());

        // Read back and written again, the header survives
        let parsed: DlqEntry = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(parsed.payload, &event[..8]);
        let again: serde_json::Value = serde_json::to_value(&parsed).expect("json");
        assert_eq!(again, value);
    }

    #[test]
    fn an_entry_without_one_payload_field_is_refused() {
        let base = r#""timestamp":1,"service":"s","reason":"r""#;
        let cases = [
            ("neither", format!("{{{base}}}")),
            ("both", format!(r#"{{{base},"_json":{{}},"_raw":""}}"#)),
            (
                "encoding on json",
                format!(r#"{{{base},"_json":{{}},"_raw_encoding":"base64"}}"#),
            ),
            (
                "unknown encoding",
                format!(r#"{{{base},"_raw":"","_raw_encoding":"hex"}}"#),
            ),
            (
                "bad base64",
                format!(r#"{{{base},"_raw":"!","_raw_encoding":"base64"}}"#),
            ),
        ];
        for (name, json) in cases {
            assert!(serde_json::from_str::<DlqEntry>(&json).is_err(), "{name}");
        }
    }
}
