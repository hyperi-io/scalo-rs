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
use serde::de::{Error as _, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use crate::parse_guard::{MAX_PARSE_DEPTH, json_depth_within};

/// Deepest original filed under `_json`: the entry wraps it one level deeper, which a loader's parse still holds.
const JSON_DEPTH: usize = MAX_PARSE_DEPTH - 1;

/// The whitespace JSON allows around a value.
const JSON_WHITESPACE: [char; 4] = [' ', '\t', '\n', '\r'];

/// A failed message routed to the dead letter queue.
///
/// This envelope is backend-agnostic -- it carries the original payload
/// plus metadata about why and where the failure occurred.
///
/// It is written as a DFE event. A payload that is one whole JSON object, nested no deeper than 63 levels, goes under `_json` as itself, with its `_org_id`, `_source`, `_timestamp_received` and `_tags` copied beside it. Any other payload goes under `_raw`: valid UTF-8 as text unless escaping would make it longer than base64, binary or truncated bytes as base64 with `_raw_encoding: "base64"`. A deeper object keeps its header and adds `_raw_reason: "depth"`.
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
    /// The fields left out for the cap or the depth limit, written as `header_dropped`.
    #[serde(skip)]
    dropped: Vec<String>,
}

/// How `_raw` holds the payload when it is not plain text.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawEncoding {
    Base64,
}

/// Why a JSON object went under `_raw`.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum RawReason {
    /// It nests deeper than [`JSON_DEPTH`].
    Depth,
}

/// Where an entry files a payload it holds whole.
enum Filed {
    /// One whole JSON object, under `_json` with its header beside it.
    Json(Header, Box<RawValue>),
    /// A JSON object nested deeper than [`JSON_DEPTH`], under `_raw` with its header beside it.
    TooDeep(Header),
    /// Anything else, under `_raw`.
    Raw,
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
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    header_dropped: &'a [String],
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
    #[serde(rename = "_raw_reason", skip_serializing_if = "Option::is_none")]
    raw_reason: Option<RawReason>,
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
    header_dropped: Vec<String>,
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
    /// The header of an object's text, without the values nested too deep to copy.
    fn read(text: &str) -> Option<Self> {
        let mut header: Self = serde_json::from_str(text).ok()?;
        header.keep(|value| json_depth_within(value.get().as_bytes(), JSON_DEPTH));
        Some(header)
    }

    fn is_empty(&self) -> bool {
        self.org_id.is_none()
            && self.source.is_none()
            && self.timestamp_received.is_none()
            && self.tags.is_none()
            && self.dropped.is_empty()
    }

    /// Leaves out each value `fits` refuses, in header order, and notes its name.
    fn keep(&mut self, mut fits: impl FnMut(&RawValue) -> bool) {
        for (name, value) in [
            ("_org_id", &mut self.org_id),
            ("_source", &mut self.source),
            ("_timestamp_received", &mut self.timestamp_received),
            ("_tags", &mut self.tags),
        ] {
            if value.as_deref().is_some_and(|raw| !fits(raw)) {
                *value = None;
                self.dropped.push(name.to_owned());
            }
        }
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

    /// Keep the start of a payload longer than `max_bytes`, marking the entry
    /// `truncated` with the original length. A payload within it is kept
    /// whole.
    ///
    /// The original's common-header fields are read first, so the entry still carries them. They share `max_bytes` with the kept payload: a value that would go over it is left out and named in `header_dropped`.
    #[must_use]
    pub fn truncate_to(mut self, max_bytes: usize) -> Self {
        let len = self.payload.len();
        if len > max_bytes {
            if self.header.is_none() {
                self.header = object_text(&self.payload).and_then(|text| Header::read(&text));
            }
            let mut header_bytes = 0;
            if let Some(header) = &mut self.header {
                header.keep(|value| {
                    let len = value.get().len();
                    let fits = header_bytes + len <= max_bytes;
                    if fits {
                        header_bytes += len;
                    }
                    fits
                });
            }
            self.payload.truncate(max_bytes - header_bytes);
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
        let filed = if self.truncated {
            Filed::Raw
        } else {
            file(&self.payload)
        };
        let (header, json, raw_reason) = match &filed {
            Filed::Json(header, json) => (Some(header), Some(json.as_ref()), None),
            Filed::TooDeep(header) => (Some(header), None, Some(RawReason::Depth)),
            Filed::Raw => (self.header.as_ref(), None, None),
        };
        let (raw, raw_encoding) = if json.is_some() {
            (None, None)
        } else {
            match std::str::from_utf8(&self.payload) {
                Ok(text) if !self.truncated && fits_as_text(text) => {
                    (Some(Cow::Borrowed(text)), None)
                }
                _ => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&self.payload);
                    (Some(Cow::Owned(encoded)), Some(RawEncoding::Base64))
                }
            }
        };
        WireOut {
            timestamp: self.timestamp,
            service: &self.service,
            reason: &self.reason,
            attempts: self.attempts,
            truncated: self.truncated,
            original_bytes: self.original_bytes,
            header_dropped: header.map_or(&[][..], |h| h.dropped.as_slice()),
            destination: self.destination.as_deref(),
            source: self.source.as_ref(),
            org_id: header.and_then(|h| h.org_id.as_deref()),
            origin: header.and_then(|h| h.source.as_deref()),
            timestamp_received: header.and_then(|h| h.timestamp_received.as_deref()),
            tags: header.and_then(|h| h.tags.as_deref()),
            json,
            raw,
            raw_encoding,
            raw_reason,
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
            dropped: wire.header_dropped,
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

/// Where a whole payload goes.
fn file(payload: &[u8]) -> Filed {
    let Some(text) = object_text(payload) else {
        return Filed::Raw;
    };
    // Parsing the header validates the whole object.
    let Some(header) = Header::read(&text) else {
        return Filed::Raw;
    };
    if !json_depth_within(text.as_bytes(), JSON_DEPTH) {
        return Filed::TooDeep(header);
    }
    RawValue::from_string(text.into_owned()).map_or(Filed::Raw, |json| Filed::Json(header, json))
}

/// The payload's text when it opens a JSON object, trimmed, with its line breaks as spaces so a file line holds one entry.
///
/// A line break inside a string is invalid JSON that a space would hide, so text holding one is validated before the change.
fn object_text(payload: &[u8]) -> Option<Cow<'_, str>> {
    let text = std::str::from_utf8(payload)
        .ok()?
        .trim_matches(JSON_WHITESPACE);
    if !text.starts_with('{') {
        return None;
    }
    if !text.contains(['\n', '\r']) {
        return Some(Cow::Borrowed(text));
    }
    serde_json::from_str::<IgnoredAny>(text).ok()?;
    Some(Cow::Owned(text.replace(['\n', '\r'], " ")))
}

/// Whether `text` as a JSON string is no longer than its base64, which keeps `_raw` within the base64 bound.
fn fits_as_text(text: &str) -> bool {
    let escaped: usize = text.bytes().map(escaped_len).sum();
    base64::encoded_len(text.len(), true).is_none_or(|encoded| escaped <= encoded)
}

/// Bytes one byte of a JSON string takes as serde_json escapes it.
fn escaped_len(byte: u8) -> usize {
    match byte {
        b'"' | b'\\' | 0x08 | b'\t' | b'\n' | 0x0c | b'\r' => 2,
        0x00..=0x1f => 6,
        _ => 1,
    }
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
        let nested = |depth: usize| format!("{}1{}", r#"{"key":"#.repeat(depth), "}".repeat(depth));
        let (deep_63, deep_64, deep_66) = (nested(63), nested(64), nested(66));
        let deep_tags = format!(r#"{{"_org_id":"acme","_tags":{deep_64}}}"#);
        let as_string = |text: &str| serde_json::to_string(text).expect("a JSON string");
        // (case, payload, what follows `"attempts":0,` as written, payload read back)
        let cases: [(&str, &[u8], String, &[u8]); 17] = [
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
            (
                "line break inside a string",
                b"{\"a\":\"x\ny\"}",
                r#""_raw":"{\"a\":\"x\ny\"}"}"#.to_owned(),
                b"{\"a\":\"x\ny\"}",
            ),
            (
                "multi-line object with an escaped line break",
                b"{\n\"a\": \"x\\ny\"\n}",
                r#""_json":{ "a": "x\ny" }}"#.to_owned(),
                br#"{ "a": "x\ny" }"#,
            ),
            (
                "form feed before an object",
                b"\x0c{}",
                r#""_raw":"\f{}"}"#.to_owned(),
                b"\x0c{}",
            ),
            (
                "text its escapes would grow past base64",
                b"\x01\x02\x03",
                r#""_raw":"AQID","_raw_encoding":"base64"}"#.to_owned(),
                b"\x01\x02\x03",
            ),
            (
                "63 deep",
                deep_63.as_bytes(),
                format!(r#""_json":{deep_63}}}"#),
                deep_63.as_bytes(),
            ),
            (
                "64 deep",
                deep_64.as_bytes(),
                format!(r#""_raw":{},"_raw_reason":"depth"}}"#, as_string(&deep_64)),
                deep_64.as_bytes(),
            ),
            (
                "66 deep",
                deep_66.as_bytes(),
                format!(r#""_raw":{},"_raw_reason":"depth"}}"#, as_string(&deep_66)),
                deep_66.as_bytes(),
            ),
            (
                "too deep a _tags",
                deep_tags.as_bytes(),
                format!(
                    r#""header_dropped":["_tags"],"_org_id":"acme","_raw":{},"_raw_reason":"depth"}}"#,
                    as_string(&deep_tags)
                ),
                deep_tags.as_bytes(),
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
            assert!(
                json_depth_within(json.as_bytes(), MAX_PARSE_DEPTH),
                "{name}: within a loader's parse depth"
            );
            let parsed: DlqEntry = serde_json::from_str(&json).expect("deserialise");
            assert_eq!(parsed.payload, read_back, "{name}");
        }
    }

    #[test]
    fn a_truncated_event_keeps_the_header_values_its_cap_holds() {
        let small = br#"{"_org_id":"acme","_tags":{},"eventVersion":"1.11"}"#.to_vec();
        let big = format!(
            r#"{{"_org_id":"acme","_tags":{{"k":"{}"}},"eventVersion":"1.11"}}"#,
            "x".repeat(64)
        )
        .into_bytes();
        // `"acme"` takes 6 bytes, `{}` 2 and the big `_tags` 72, out of the same cap as the kept payload.
        // (case, event, cap, payload kept, header_dropped)
        let cases: [(&str, &[u8], usize, usize, &[&str]); 4] = [
            ("small header", &small, 16, 8, &[]),
            ("oversized _tags", &big, 16, 10, &["_tags"]),
            ("header fills the cap", &big, 78, 0, &[]),
            ("cap under every value", &big, 4, 4, &["_org_id", "_tags"]),
        ];
        for (name, event, cap, kept, dropped) in cases {
            let cut = DlqEntry::new("svc", "r", event.to_vec()).truncate_to(cap);

            let json = serde_json::to_string(&cut).expect("serialise");
            let value: serde_json::Value = serde_json::from_str(&json).expect("json");
            assert_eq!(value["_raw_encoding"], "base64", "{name}");
            assert!(value.get("_json").is_none(), "{name}");
            assert_eq!(
                value.get("header_dropped"),
                (!dropped.is_empty())
                    .then(|| serde_json::json!(dropped))
                    .as_ref(),
                "{name}"
            );
            let header_bytes: usize = ["_org_id", "_tags"]
                .iter()
                .filter_map(|field| value.get(*field))
                .map(|field| field.to_string().len())
                .sum();
            assert!(header_bytes + kept <= cap, "{name}");

            // Read back and written again, the header and its note survive
            let parsed: DlqEntry = serde_json::from_str(&json).expect("deserialise");
            assert_eq!(parsed.payload, &event[..kept], "{name}");
            let again: serde_json::Value = serde_json::to_value(&parsed).expect("json");
            assert_eq!(again, value, "{name}");
        }
    }

    /// `_raw` never takes more than the base64 of the default cap, 699,052 bytes.
    #[test]
    fn raw_stays_within_the_base64_of_the_cap() {
        let cap = crate::dlq::DEFAULT_MAX_PAYLOAD_BYTES;
        // (case, payload)
        let cases = [
            ("binary over the cap", vec![0xff; cap + 1]),
            ("control text at the cap", vec![0x01; cap]),
            ("quotes at the cap", vec![b'"'; cap]),
            ("plain text at the cap", vec![b'x'; cap]),
        ];
        for (name, payload) in cases {
            let entry = DlqEntry::new("svc", "r", payload).truncate_to(cap);
            let value = serde_json::to_value(&entry).expect("json");
            let raw = value["_raw"].as_str().expect("a `_raw` string");
            let written = serde_json::to_string(raw).expect("a JSON string").len() - 2;
            assert!(written <= 699_052, "{name}: {written} bytes");
        }
    }

    #[test]
    fn escaping_counts_what_serde_json_writes() {
        let texts: Vec<String> = (0..=0x7f_u8)
            .map(|byte| char::from(byte).to_string())
            .chain(["\u{e9}".to_owned(), "a\"b\\c\u{1f}".to_owned()])
            .collect();
        for text in texts {
            let written = serde_json::to_string(&text).expect("a JSON string").len() - 2;
            assert_eq!(
                text.bytes().map(escaped_len).sum::<usize>(),
                written,
                "{text:?}"
            );
        }
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
