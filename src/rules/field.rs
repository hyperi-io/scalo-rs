// Project:   scalo
// File:      src/rules/field.rs
// Purpose:   How a rule set reads the fields of a record
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A record is anything that implements [`FieldSource`]. scalo provides two:
//! [`JsonPayload`], raw JSON bytes read lazily with no DOM built, and
//! `serde_json::Value`, for a record already parsed.

use std::borrow::Cow;

/// A dotted field path, split once when the rule set compiles.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath {
    text: Box<str>,
    segments: Box<[String]>,
}

impl FieldPath {
    /// Split `text` on `.`. `None` when it is empty or has an empty segment.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        if text.is_empty() || text.split('.').any(str::is_empty) {
            return None;
        }
        Some(Self {
            text: text.into(),
            segments: text.split('.').map(str::to_string).collect(),
        })
    }

    /// The path as written.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The path's segments, outermost first.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.segments
    }
}

impl std::fmt::Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

/// What a record holds at a path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lookup<'a> {
    /// Nothing is at the path.
    Missing,
    /// JSON null.
    Null,
    /// A string as written, or a number or boolean as its JSON text.
    Text(Cow<'a, str>),
    /// An object or an array.
    Composite,
}

impl<'a> Lookup<'a> {
    /// The text, when it is non-empty: what `exists` accepts and an action copies.
    #[must_use]
    pub fn present_text(self) -> Option<Cow<'a, str>> {
        match self {
            Self::Text(text) if !text.is_empty() => Some(text),
            Self::Missing | Self::Null | Self::Text(_) | Self::Composite => None,
        }
    }
}

/// A record a rule set reads fields from.
///
/// `Sync`, because a CEL condition reads the record through a resolver the
/// `cel` crate requires to be `Send + Sync`.
pub trait FieldSource: Sync {
    /// What the record holds at `path`.
    fn lookup(&self, path: &FieldPath) -> Lookup<'_>;

    /// The top-level field `name` as a CEL value, for a condition that names it.
    fn cel_value(&self, name: &str) -> Option<cel::Value>;
}

/// A JSON object payload, read lazily: each lookup walks the bytes to the path
/// and builds no DOM.
#[derive(Debug, Clone, Copy)]
pub struct JsonPayload<'a> {
    bytes: &'a [u8],
}

impl<'a> JsonPayload<'a> {
    /// Wrap `bytes` after the depth check the lazy reader needs.
    ///
    /// `None` when they nest deeper than [`MAX_PARSE_DEPTH`](crate::parse_guard::MAX_PARSE_DEPTH):
    /// the lazy reader has no depth limit of its own, and a deep enough body
    /// overflows the stack.
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        crate::parse_guard::json_depth_within(bytes, crate::parse_guard::MAX_PARSE_DEPTH)
            .then_some(Self { bytes })
    }

    /// Wrap bytes the caller has already passed through
    /// [`json_depth_within`](crate::parse_guard::json_depth_within), or a parser
    /// with its own depth limit.
    #[must_use]
    pub fn trusted(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// The wrapped bytes.
    #[must_use]
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

impl FieldSource for JsonPayload<'_> {
    fn lookup(&self, path: &FieldPath) -> Lookup<'_> {
        use sonic_rs::{JsonType, JsonValueTrait};
        let Ok(lazy) = sonic_rs::get_from_slice(self.bytes, path.segments()) else {
            return Lookup::Missing;
        };
        match lazy.get_type() {
            JsonType::Null => Lookup::Null,
            JsonType::Object | JsonType::Array => Lookup::Composite,
            JsonType::Number | JsonType::Boolean => Lookup::Text(lazy.as_raw_cow()),
            JsonType::String => match lazy.as_raw_cow() {
                // No escape, so the bytes between the quotes are the text.
                Cow::Borrowed(raw) if memchr::memchr(b'\\', raw.as_bytes()).is_none() => {
                    Lookup::Text(Cow::Borrowed(&raw[1..raw.len() - 1]))
                }
                _ => lazy.as_str().map_or(Lookup::Missing, |text| {
                    Lookup::Text(Cow::Owned(text.to_owned()))
                }),
            },
        }
    }

    fn cel_value(&self, name: &str) -> Option<cel::Value> {
        let lazy = sonic_rs::get_from_slice(self.bytes, [name]).ok()?;
        let value: serde_json::Value = sonic_rs::from_str(lazy.as_raw_str()).ok()?;
        Some(crate::expression::evaluator::json_to_cel(&value))
    }
}

impl FieldSource for serde_json::Value {
    fn lookup(&self, path: &FieldPath) -> Lookup<'_> {
        let mut node = self;
        for segment in path.segments() {
            match node.get(segment.as_str()) {
                Some(next) => node = next,
                None => return Lookup::Missing,
            }
        }
        match node {
            serde_json::Value::Null => Lookup::Null,
            serde_json::Value::Bool(b) => {
                Lookup::Text(Cow::Borrowed(if *b { "true" } else { "false" }))
            }
            serde_json::Value::Number(n) => Lookup::Text(Cow::Owned(n.to_string())),
            serde_json::Value::String(s) => Lookup::Text(Cow::Borrowed(s)),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Lookup::Composite,
        }
    }

    fn cel_value(&self, name: &str) -> Option<cel::Value> {
        self.get(name)
            .map(crate::expression::evaluator::json_to_cel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(text: &str) -> FieldPath {
        FieldPath::parse(text).unwrap()
    }

    #[test]
    fn a_path_with_an_empty_segment_is_refused() {
        for bad in ["", ".", "a.", ".a", "a..b"] {
            assert!(FieldPath::parse(bad).is_none(), "{bad:?}");
        }
        assert_eq!(path("a.b.c").segments(), ["a", "b", "c"]);
    }

    /// Both sources read every JSON kind the same way, so a rule set behaves the
    /// same over raw bytes and a parsed value.
    #[test]
    fn json_bytes_and_a_parsed_value_agree_on_every_kind() {
        let text = r#"{"s":"web-1","e":"","n":42,"f":1.5,"b":true,"z":null,"o":{"k":"v"},"a":[1],"esc":"a\"b"}"#;
        let bytes = JsonPayload::new(text.as_bytes()).unwrap();
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        let cases = [
            ("s", Lookup::Text(Cow::Borrowed("web-1"))),
            ("e", Lookup::Text(Cow::Borrowed(""))),
            ("n", Lookup::Text(Cow::Borrowed("42"))),
            ("f", Lookup::Text(Cow::Borrowed("1.5"))),
            ("b", Lookup::Text(Cow::Borrowed("true"))),
            ("z", Lookup::Null),
            ("o", Lookup::Composite),
            ("a", Lookup::Composite),
            ("o.k", Lookup::Text(Cow::Borrowed("v"))),
            ("esc", Lookup::Text(Cow::Borrowed("a\"b"))),
            ("absent", Lookup::Missing),
            ("s.deeper", Lookup::Missing),
        ];
        for (field, expected) in cases {
            assert_eq!(bytes.lookup(&path(field)), expected, "bytes {field}");
            assert_eq!(value.lookup(&path(field)), expected, "value {field}");
        }
    }

    #[test]
    fn an_unescaped_string_is_borrowed_from_the_payload() {
        let text = br#"{"host":"web-1"}"#;
        let payload = JsonPayload::new(text).unwrap();
        assert!(matches!(
            payload.lookup(&path("host")),
            Lookup::Text(Cow::Borrowed("web-1"))
        ));
    }

    #[test]
    fn a_payload_past_the_depth_bound_is_refused() {
        let deep = format!(
            "{}{}",
            "[".repeat(crate::parse_guard::MAX_PARSE_DEPTH + 1),
            "]".repeat(crate::parse_guard::MAX_PARSE_DEPTH + 1)
        );
        assert!(JsonPayload::new(deep.as_bytes()).is_none());
    }

    #[test]
    fn present_text_refuses_empty_null_and_composite() {
        assert_eq!(
            Lookup::Text(Cow::Borrowed("x")).present_text().as_deref(),
            Some("x")
        );
        for absent in [
            Lookup::Text(Cow::Borrowed("")),
            Lookup::Null,
            Lookup::Missing,
            Lookup::Composite,
        ] {
            assert_eq!(absent.present_text(), None);
        }
    }

    #[test]
    fn a_cel_value_reads_a_top_level_field_from_either_source() {
        let text = r#"{"n":3,"o":{"k":"v"}}"#;
        let bytes = JsonPayload::new(text.as_bytes()).unwrap();
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(bytes.cel_value("n"), Some(cel::Value::Int(3)));
        assert_eq!(value.cel_value("n"), Some(cel::Value::Int(3)));
        assert!(bytes.cel_value("o").is_some());
        assert_eq!(bytes.cel_value("absent"), None);
        assert_eq!(value.cel_value("absent"), None);
    }
}
