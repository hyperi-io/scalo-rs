// Project:   scalo
// File:      src/framing.rs
// Purpose:   Batch framing: many encoded records in one unit, and back
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Where one encoded record ends and the next begins, in a unit that holds
//! many: a request body, a file, a message, a byte stream.
//!
//! A [`Framing`] takes records the record codec already turned into bytes and
//! never looks inside one. Each framing says whether its units append (two
//! units back to back decode as the records of both, so a file can grow) and
//! whether it decodes as a stream (a [`StreamDecoder`] hands records out before
//! the input ends).
//!
//! | Name | One unit | Appends | Streams |
//! |---|---|---|---|
//! | `ndjson` | each record then `\n`, unless it ends in one | yes | yes |
//! | `lf` | each record then `\n` | yes | yes |
//! | `octet_counting` | each record as `<length> <record>`, RFC 6587 | yes | yes |
//! | `json_concat` | the records back to back, each an object or array | yes | no |
//! | `json_array` | `[r1,r2]` | no | no |
//! | `json_wrapped` | `{"<key>":[r1,r2]}` | no | no |
//! | `single` | one record as it is | no | no |
//!
//! [`FramingConfig`] builds one by name, and [`register_framing`] adds an app's
//! own under a name config can then select. The guide is `docs/transport/framing.md`.
//!
//! ```
//! use scalo::framing::{DecodeLimits, Framing, Ndjson};
//!
//! let records: [&[u8]; 2] = [br#"{"a":1}"#, br#"{"b":2}"#];
//! let mut unit = Vec::new();
//! assert_eq!(Ndjson.encode(&records, &mut unit), 2);
//! assert_eq!(unit, b"{\"a\":1}\n{\"b\":2}\n");
//!
//! let decoded = Ndjson.decode(&unit, &DecodeLimits::UNBOUNDED).unwrap();
//! assert_eq!(decoded.records, records);
//! ```

use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::registry::{Registry, RegistryError};

/// Why a unit did not decode.
///
/// Structure only: a JSON framing finds where each value ends but never
/// parses one, so `[1,nul]` frames as two records and the parser downstream
/// refuses the second.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum FramingError {
    /// The unit did not start, after whitespace, with `[`.
    #[error("json-array framing: expected opening '[', found {0}")]
    NotAnArray(String),

    /// The unit did not start, after whitespace, with `{`.
    #[error("json framing: expected opening '{{', found {0}")]
    NotAnObject(String),

    /// The wrapping object has no member of this name.
    #[error("json framing: no '{0}' member holds the records")]
    MissingKey(String),

    /// The unit ended inside an array, a string, or a frame its framing could not end.
    #[error("framing: unexpected end of input (unterminated array, string or frame)")]
    UnexpectedEof,

    /// A value position held nothing (leading, trailing, or doubled comma).
    #[error("json framing: empty element at byte offset {0} (stray comma)")]
    EmptyElement(usize),

    /// A container closed that was never opened.
    #[error("json framing: unbalanced closing bracket at byte offset {0}")]
    Unbalanced(usize),

    /// Bytes other than whitespace followed the last value.
    #[error("json framing: trailing garbage after the closing bracket at byte offset {0}")]
    TrailingGarbage(usize),

    /// A byte the structure has no place for.
    #[error("json framing: unexpected byte at offset {0}")]
    UnexpectedByte(usize),

    /// The unit is over [`DecodeLimits::max_decoded_bytes`].
    #[error("framing: a unit of {len} bytes is over max_decoded_bytes {max}")]
    TooLarge {
        /// The unit's length.
        len: usize,
        /// The limit.
        max: usize,
    },

    /// [`StreamDecoder`] was given a framing that decodes whole units only.
    #[error("framing: this framing decodes whole units, not a stream")]
    NotAStream,

    /// [`FramingConfig::build`] was given a name nothing is registered under.
    #[error(
        "framing: no framing is named '{name}': built-in are {builtin}, registered are [{registered}]"
    )]
    Unknown {
        /// The name asked for.
        name: String,
        /// The built-in names.
        builtin: String,
        /// The names apps registered.
        registered: String,
    },
}

/// Bounds on what decoding takes in.
///
/// There is no default: set them from config for input this process did not
/// write, or use [`UNBOUNDED`](Self::UNBOUNDED) for input it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Largest unit [`Framing::decode`] takes, counted after any content
    /// decoding: a gzip body is held to this once inflated, and to the
    /// transport's own body limit as it arrives.
    pub max_decoded_bytes: usize,
    /// Largest record. A longer one is left out of the records and reported
    /// as oversize. An LF line counts the carriage returns before its `\n`.
    pub max_record_bytes: usize,
    /// Largest octet count taken as a frame. A frame between
    /// `max_record_bytes` and this is skipped whole. A count over it is not
    /// trusted, and the bytes from it to the next `\n` are read as an LF line.
    /// Read as at least `max_record_bytes`.
    pub max_frame_bytes: usize,
}

impl DecodeLimits {
    /// No bound, for bytes this process wrote itself.
    pub const UNBOUNDED: Self = Self {
        max_decoded_bytes: usize::MAX,
        max_record_bytes: usize::MAX,
        max_frame_bytes: usize::MAX,
    };

    /// Refuse a unit over `max_decoded_bytes`.
    ///
    /// # Errors
    ///
    /// [`FramingError::TooLarge`].
    pub fn check(&self, unit: &[u8]) -> Result<(), FramingError> {
        if unit.len() > self.max_decoded_bytes {
            return Err(FramingError::TooLarge {
                len: unit.len(),
                max: self.max_decoded_bytes,
            });
        }
        Ok(())
    }
}

/// The records of one unit, borrowed from it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Decoded<'a> {
    /// The records, in order.
    pub records: Vec<&'a [u8]>,
    /// Records over `max_record_bytes`, whole and in order, left out of `records`.
    pub oversize: Vec<&'a [u8]>,
}

impl<'a> Decoded<'a> {
    /// `records` sorted into those within `limits.max_record_bytes` and those over it.
    #[must_use]
    pub fn from_records(
        records: impl IntoIterator<Item = &'a [u8]>,
        limits: &DecodeLimits,
    ) -> Self {
        let mut decoded = Self::default();
        for record in records {
            if record.len() > limits.max_record_bytes {
                decoded.oversize.push(record);
            } else {
                decoded.records.push(record);
            }
        }
        decoded
    }
}

/// The frame at the front of a stream buffer, as [`Framing::split`] finds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Split {
    /// `len` bytes are one frame, and `record` is where its record sits.
    Record {
        /// The record, as a range of the buffer.
        record: Range<usize>,
        /// The frame's bytes, from the front of the buffer.
        len: usize,
    },
    /// `len` bytes are a frame with no record, such as a blank line.
    Skip {
        /// The frame's bytes, from the front of the buffer.
        len: usize,
    },
    /// `len` bytes start a frame whose record is over `max_record_bytes`.
    Oversize {
        /// The record, or as much of it as the buffer holds, as a range of it.
        record: Range<usize>,
        /// The frame's bytes in the buffer, from its front.
        len: usize,
        /// What of the frame is still to come, `None` when it ends in the buffer.
        rest: Option<Rest>,
    },
    /// The buffer holds no whole frame yet.
    More,
}

/// The part of an oversize frame still to come.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rest {
    /// This many bytes more.
    Bytes(u64),
    /// Everything to the next `\n`, inclusive, or to the end of input.
    Line,
}

/// Puts many encoded records in one unit, and splits a unit back into them.
///
/// An app's own framing implements [`encode`](Self::encode), and either
/// [`decode`](Self::decode) for whole units, or [`streams`](Self::streams) and
/// [`split`](Self::split) for a stream, which the default `decode` then uses.
/// [`register_framing`] has a worked example.
pub trait Framing: Send + Sync + std::fmt::Debug {
    /// The MIME type of a unit, for an HTTP `Content-Type`.
    fn content_type(&self) -> &'static str;

    /// Whether a unit appended to another decodes as the records of both.
    fn appends(&self) -> bool;

    /// Whether [`split`](Self::split) takes frames off the front of a stream.
    fn streams(&self) -> bool {
        false
    }

    /// Frame the leading records of `records` as one unit, appended to `out`,
    /// and return how many it took: all of them, unless the framing holds
    /// fewer in a unit, and never none of a non-empty slice.
    #[must_use]
    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize;

    /// The records of one whole unit. The default splits it with
    /// [`split`](Self::split), the end of the unit ending its last frame.
    ///
    /// # Errors
    ///
    /// [`FramingError`] when the unit is over `limits.max_decoded_bytes` or
    /// is not well framed.
    fn decode<'a>(
        &self,
        unit: &'a [u8],
        limits: &DecodeLimits,
    ) -> Result<Decoded<'a>, FramingError> {
        limits.check(unit)?;
        let mut decoded = Decoded::default();
        let mut at = 0;
        while let Some(rest) = unit.get(at..).filter(|rest| !rest.is_empty()) {
            let len = match self.split(rest, 0, limits, true)? {
                Split::Record { record, len } => {
                    decoded.records.push(&rest[record]);
                    len
                }
                Split::Oversize {
                    record,
                    len,
                    rest: None,
                } => {
                    decoded.oversize.push(&rest[record]);
                    len
                }
                Split::Skip { len } => len,
                Split::Oversize { rest: Some(_), .. } | Split::More => 0,
            };
            if len == 0 {
                return Err(FramingError::UnexpectedEof);
            }
            at += len;
        }
        Ok(decoded)
    }

    /// Find the frame at the front of `buf`. `seen` bytes of it were there at
    /// the last call, which answered [`Split::More`], so a search for a
    /// delimiter can start there. `at_end` says nothing follows `buf`.
    ///
    /// # Errors
    ///
    /// [`FramingError::NotAStream`], the default, from a framing that does
    /// not stream. A stream framing errs only at the end of input, on a frame
    /// it cannot end.
    fn split(
        &self,
        buf: &[u8],
        seen: usize,
        limits: &DecodeLimits,
        at_end: bool,
    ) -> Result<Split, FramingError> {
        let _ = (buf, seen, limits, at_end);
        Err(FramingError::NotAStream)
    }
}

/// Newline-delimited JSON: each record then `\n`, unless it already ends in
/// one. Decoding trims ASCII whitespace from each line and skips blank ones.
#[derive(Debug, Clone, Copy, Default)]
pub struct Ndjson;

impl Framing for Ndjson {
    fn content_type(&self) -> &'static str {
        "application/x-ndjson"
    }

    fn appends(&self) -> bool {
        true
    }

    fn streams(&self) -> bool {
        true
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        for record in records {
            out.extend_from_slice(record);
            if record.last() != Some(&b'\n') {
                out.push(b'\n');
            }
        }
        records.len()
    }

    fn split(
        &self,
        buf: &[u8],
        seen: usize,
        limits: &DecodeLimits,
        at_end: bool,
    ) -> Result<Split, FramingError> {
        Ok(split_line(buf, seen, limits, at_end, |line, _| {
            without_whitespace(line)
        }))
    }
}

/// Lines: each record then `\n`. Decoding drops the `\n` and every `\r`
/// before it, skips empty lines, and reads a last line cut off by the end of
/// input as a record.
#[derive(Debug, Clone, Copy, Default)]
pub struct Lf;

impl Framing for Lf {
    fn content_type(&self) -> &'static str {
        "text/plain"
    }

    fn appends(&self) -> bool {
        true
    }

    fn streams(&self) -> bool {
        true
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        for record in records {
            out.extend_from_slice(record);
            out.push(b'\n');
        }
        records.len()
    }

    fn split(
        &self,
        buf: &[u8],
        seen: usize,
        limits: &DecodeLimits,
        at_end: bool,
    ) -> Result<Split, FramingError> {
        Ok(split_line(buf, seen, limits, at_end, |line, _| {
            without_carriage_returns(line)
        }))
    }
}

/// RFC 6587 octet counting: each syslog record as its length in ASCII digits,
/// a space, then the record, which starts with its `<PRI>`.
///
/// Decoding reads a frame as counted only when it is a count starting 1 to 9,
/// one space, then `<`. Anything else is an LF line, so a stream can mix the
/// two, and a line such as `2026 Oct 11 ...` never swallows what follows it.
/// A record that is empty or does not start with `<` encodes, but decodes as
/// an LF line. A counted frame over `max_record_bytes` but within
/// `max_frame_bytes` is skipped whole. A count over `max_frame_bytes` is not
/// trusted: the bytes to the next `\n` are read as an LF line and counting
/// resumes after it.
///
/// At the end of input a counted frame cut short is dropped, or reported as
/// oversize when its count is over `max_record_bytes`. A last line with no
/// `\n` loses its ASCII whitespace, and is dropped when nothing is left.
#[derive(Debug, Clone, Copy, Default)]
pub struct OctetCounting;

impl Framing for OctetCounting {
    fn content_type(&self) -> &'static str {
        "text/plain"
    }

    fn appends(&self) -> bool {
        true
    }

    fn streams(&self) -> bool {
        true
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        for record in records {
            out.extend_from_slice(record.len().to_string().as_bytes());
            out.push(b' ');
            out.extend_from_slice(record);
        }
        records.len()
    }

    fn split(
        &self,
        buf: &[u8],
        seen: usize,
        limits: &DecodeLimits,
        at_end: bool,
    ) -> Result<Split, FramingError> {
        if let Some(split) = split_counted(buf, limits, at_end) {
            return Ok(split);
        }
        Ok(split_line(buf, seen, limits, at_end, |line, ended| {
            if ended {
                without_carriage_returns(line)
            } else {
                without_whitespace(line)
            }
        }))
    }
}

/// JSON values back to back, each an object or an array, with optional
/// whitespace between them.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonConcat;

impl Framing for JsonConcat {
    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn appends(&self) -> bool {
        true
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        for record in records {
            out.extend_from_slice(record);
        }
        records.len()
    }

    fn decode<'a>(
        &self,
        unit: &'a [u8],
        limits: &DecodeLimits,
    ) -> Result<Decoded<'a>, FramingError> {
        limits.check(unit)?;
        let mut values = Vec::new();
        let mut at = skip_whitespace(unit, 0);
        while let Some(&first) = unit.get(at) {
            if first != b'{' && first != b'[' {
                return Err(FramingError::UnexpectedByte(at));
            }
            let end = container_end(unit, at)?;
            values.push(&unit[at..end]);
            at = skip_whitespace(unit, end);
        }
        Ok(Decoded::from_records(values, limits))
    }
}

/// A JSON array of the records: `[r1,r2]`.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonArray;

impl Framing for JsonArray {
    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn appends(&self) -> bool {
        false
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        encode_array(records, out);
        records.len()
    }

    fn decode<'a>(
        &self,
        unit: &'a [u8],
        limits: &DecodeLimits,
    ) -> Result<Decoded<'a>, FramingError> {
        limits.check(unit)?;
        let open = skip_whitespace(unit, 0);
        if unit.get(open) != Some(&b'[') {
            return Err(FramingError::NotAnArray(found(unit, open)));
        }
        let (elements, after) = array_elements(unit, open)?;
        end_of_unit(unit, after)?;
        Ok(Decoded::from_records(
            elements.into_iter().map(|range| &unit[range]),
            limits,
        ))
    }
}

/// A JSON object holding the records' array under one key: `{"key":[r1,r2]}`.
/// Decoding ignores the object's other members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonWrapped {
    /// The key, JSON-escaped.
    key: String,
}

impl JsonWrapped {
    /// Wrap the array under `key`.
    #[must_use]
    pub fn new(key: &str) -> Self {
        let mut escaped = String::with_capacity(key.len());
        for c in key.chars() {
            match c {
                '"' => escaped.push_str("\\\""),
                '\\' => escaped.push_str("\\\\"),
                c if u32::from(c) < 0x20 => {
                    let _ = write!(escaped, "\\u{:04x}", u32::from(c));
                }
                c => escaped.push(c),
            }
        }
        Self { key: escaped }
    }

    /// The members of the object whose `{` is at `open`, the elements of the
    /// array under the key, and the index past its `}`.
    fn members(
        &self,
        unit: &[u8],
        open: usize,
    ) -> Result<(Option<Vec<Range<usize>>>, usize), FramingError> {
        let mut elements = None;
        let mut at = skip_whitespace(unit, open + 1);
        if unit.get(at) == Some(&b'}') {
            return Ok((elements, at + 1));
        }
        loop {
            expect(unit, at, b'"')?;
            let key_end = string_end(unit, at)?;
            let key = &unit[at + 1..key_end - 1];
            at = skip_whitespace(unit, key_end);
            expect(unit, at, b':')?;
            at = skip_whitespace(unit, at + 1);
            if key == self.key.as_bytes() && elements.is_none() {
                if unit.get(at) != Some(&b'[') {
                    return Err(FramingError::NotAnArray(found(unit, at)));
                }
                let (array, after) = array_elements(unit, at)?;
                elements = Some(array);
                at = skip_whitespace(unit, after);
            } else {
                let end = value_end(unit, at, b'}')?;
                if end == at {
                    return Err(FramingError::EmptyElement(at));
                }
                at = end;
            }
            match unit.get(at) {
                Some(b',') => at = skip_whitespace(unit, at + 1),
                Some(b'}') => return Ok((elements, at + 1)),
                Some(_) => return Err(FramingError::UnexpectedByte(at)),
                None => return Err(FramingError::UnexpectedEof),
            }
        }
    }
}

impl Framing for JsonWrapped {
    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn appends(&self) -> bool {
        false
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        out.extend_from_slice(b"{\"");
        out.extend_from_slice(self.key.as_bytes());
        out.extend_from_slice(b"\":");
        encode_array(records, out);
        out.push(b'}');
        records.len()
    }

    fn decode<'a>(
        &self,
        unit: &'a [u8],
        limits: &DecodeLimits,
    ) -> Result<Decoded<'a>, FramingError> {
        limits.check(unit)?;
        let open = skip_whitespace(unit, 0);
        if unit.get(open) != Some(&b'{') {
            return Err(FramingError::NotAnObject(found(unit, open)));
        }
        let (elements, after) = self.members(unit, open)?;
        end_of_unit(unit, after)?;
        let elements = elements.ok_or_else(|| FramingError::MissingKey(self.key.clone()))?;
        Ok(Decoded::from_records(
            elements.into_iter().map(|range| &unit[range]),
            limits,
        ))
    }
}

/// One record per unit, as it is: an HTTP request, a message, a datagram.
#[derive(Debug, Clone, Copy, Default)]
pub struct Single;

impl Framing for Single {
    fn content_type(&self) -> &'static str {
        "application/octet-stream"
    }

    fn appends(&self) -> bool {
        false
    }

    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        let Some(record) = records.first() else {
            return 0;
        };
        out.extend_from_slice(record);
        1
    }

    fn decode<'a>(
        &self,
        unit: &'a [u8],
        limits: &DecodeLimits,
    ) -> Result<Decoded<'a>, FramingError> {
        limits.check(unit)?;
        Ok(Decoded::from_records([unit], limits))
    }
}

/// The framings scalo builds by name.
pub const BUILT_IN: &[&str] = &[
    "ndjson",
    "lf",
    "octet_counting",
    "json_concat",
    "json_array",
    "json_wrapped",
    "single",
];

/// Framings apps registered by name.
static FRAMINGS: Registry<dyn Framing> = Registry::new("framing", BUILT_IN);

/// Register `framing` under `name`, so a [`FramingConfig`] naming it builds it.
///
/// Register at startup, before building from config. The name is lowercase
/// letters, digits and `_`, starting with a letter, and not a built-in one.
///
/// A framing of records each ended by the ASCII record separator, 0x1e:
///
/// ```
/// use std::sync::Arc;
/// use scalo::framing::{
///     DecodeLimits, Decoded, Framing, FramingConfig, FramingError, register_framing,
/// };
///
/// #[derive(Debug)]
/// struct RecordSeparated;
///
/// impl Framing for RecordSeparated {
///     fn content_type(&self) -> &'static str {
///         "application/octet-stream"
///     }
///     fn appends(&self) -> bool {
///         true
///     }
///     fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
///         for record in records {
///             out.extend_from_slice(record);
///             out.push(0x1e);
///         }
///         records.len()
///     }
///     fn decode<'a>(
///         &self,
///         unit: &'a [u8],
///         limits: &DecodeLimits,
///     ) -> Result<Decoded<'a>, FramingError> {
///         limits.check(unit)?;
///         let records = unit.split(|&b| b == 0x1e).filter(|r| !r.is_empty());
///         Ok(Decoded::from_records(records, limits))
///     }
/// }
///
/// register_framing("record_separated", Arc::new(RecordSeparated))?;
///
/// let config = FramingConfig {
///     framing_type: "record_separated".into(),
///     ..FramingConfig::default()
/// };
/// let framing = config.build()?;
/// let records: [&[u8]; 2] = [b"one", b"two"];
/// let mut unit = Vec::new();
/// assert_eq!(framing.encode(&records, &mut unit), 2);
/// let decoded = framing.decode(&unit, &DecodeLimits::UNBOUNDED)?;
/// assert_eq!(decoded.records, records);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// [`RegistryError`] when the name is malformed, built in, or taken.
pub fn register_framing(name: &str, framing: Arc<dyn Framing>) -> Result<(), RegistryError> {
    FRAMINGS.register(name, framing)
}

/// Which framing to use, as config names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct FramingConfig {
    /// A built-in framing, or one an app registered. Default `ndjson`.
    #[serde(rename = "type")]
    pub framing_type: String,
    /// The key `json_wrapped` puts the records under. Default `records`.
    pub key: String,
}

impl Default for FramingConfig {
    fn default() -> Self {
        Self {
            framing_type: "ndjson".to_string(),
            key: "records".to_string(),
        }
    }
}

impl FramingConfig {
    /// The framing this config names.
    ///
    /// # Errors
    ///
    /// [`FramingError::Unknown`] when nothing is registered under the name.
    pub fn build(&self) -> Result<Arc<dyn Framing>, FramingError> {
        Ok(match self.framing_type.as_str() {
            "ndjson" => Arc::new(Ndjson),
            "lf" => Arc::new(Lf),
            "octet_counting" => Arc::new(OctetCounting),
            "json_concat" => Arc::new(JsonConcat),
            "json_array" => Arc::new(JsonArray),
            "json_wrapped" => Arc::new(JsonWrapped::new(&self.key)),
            "single" => Arc::new(Single),
            name => FRAMINGS.get(name).ok_or_else(|| FramingError::Unknown {
                name: name.to_string(),
                builtin: BUILT_IN.join(", "),
                registered: FRAMINGS.names().join(", "),
            })?,
        })
    }
}

/// A frame taken off a stream by a [`StreamDecoder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// A record.
    Record(Vec<u8>),
    /// A record over `max_record_bytes`, read past without being held.
    Oversize {
        /// Its first `max_record_bytes` or fewer.
        head: Vec<u8>,
        /// The frame's length in the stream, its header and delimiter included.
        len: u64,
    },
}

/// Takes records off a byte stream as it arrives, through a framing that
/// [streams](Framing::streams).
///
/// [`push`](Self::push) the bytes read, then call [`next_frame`](Self::next_frame)
/// until it answers `None`. It holds the part of a frame still waiting for its end,
/// never more than `max_record_bytes` of one, plus what was pushed last. Once
/// the input ends, [`finish`](Self::finish) lets `next_frame` take a last line that
/// has no `\n`.
#[derive(Debug)]
pub struct StreamDecoder {
    framing: Arc<dyn Framing>,
    limits: DecodeLimits,
    buf: Vec<u8>,
    /// Where the next frame starts in `buf`.
    start: usize,
    /// Bytes of that frame the last split saw, when it answered `More`.
    seen: usize,
    /// The oversize frame being read past.
    skipping: Option<Skipping>,
    taken: u64,
    at_end: bool,
}

/// An oversize frame partly read past.
#[derive(Debug)]
struct Skipping {
    head: Vec<u8>,
    len: u64,
    rest: Rest,
}

impl StreamDecoder {
    /// A decoder of `framing` under `limits`. Given a framing that does not
    /// stream, [`next_frame`](Self::next_frame) answers [`FramingError::NotAStream`].
    #[must_use]
    pub fn new(framing: Arc<dyn Framing>, limits: DecodeLimits) -> Self {
        Self {
            framing,
            limits,
            buf: Vec::new(),
            start: 0,
            seen: 0,
            skipping: None,
            taken: 0,
            at_end: false,
        }
    }

    /// Add bytes read from the stream.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Mark the end of the input.
    pub fn finish(&mut self) {
        self.at_end = true;
    }

    /// Stream bytes in the frames taken so far, blank lines and oversize
    /// frames included: the offset just past the last frame.
    #[must_use]
    pub fn taken(&self) -> u64 {
        self.taken
    }

    /// The next frame, or `None` until more bytes arrive.
    ///
    /// # Errors
    ///
    /// [`FramingError`] from the framing, and [`FramingError::UnexpectedEof`]
    /// when the input ended inside a frame the framing could not end.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, FramingError> {
        loop {
            if self.skipping.is_some() {
                let frame = self.skip();
                if frame.is_none() {
                    self.compact();
                }
                return Ok(frame);
            }
            let pending = &self.buf[self.start..];
            if pending.is_empty() {
                self.compact();
                return Ok(None);
            }
            match self
                .framing
                .split(pending, self.seen, &self.limits, self.at_end)?
            {
                Split::More if self.at_end => return Err(FramingError::UnexpectedEof),
                Split::More => {
                    self.seen = pending.len();
                    self.compact();
                    return Ok(None);
                }
                Split::Skip { len } => self.advance(len),
                Split::Record { record, len } => {
                    let record = pending[record].to_vec();
                    self.advance(len);
                    return Ok(Some(Frame::Record(record)));
                }
                Split::Oversize { record, len, rest } => {
                    let record = &pending[record];
                    let head = record[..record.len().min(self.limits.max_record_bytes)].to_vec();
                    self.advance(len);
                    let Some(rest) = rest else {
                        return Ok(Some(Frame::Oversize {
                            head,
                            len: len as u64,
                        }));
                    };
                    self.skipping = Some(Skipping {
                        head,
                        len: len as u64,
                        rest,
                    });
                }
            }
        }
    }

    /// Read on past the oversize frame: `Some` once it ends or the input does,
    /// `None` when the buffer is spent first.
    fn skip(&mut self) -> Option<Frame> {
        let pending = &self.buf[self.start..];
        let skipping = self.skipping.as_mut()?;
        // (frame bytes taken, record bytes among them, whether the frame ends)
        let (len, record, done) = match skipping.rest {
            Rest::Bytes(more) => {
                let len =
                    usize::try_from(more).map_or(pending.len(), |more| more.min(pending.len()));
                skipping.rest = Rest::Bytes(more - len as u64);
                (len, len, len as u64 == more)
            }
            Rest::Line => newline(pending)
                .map_or((pending.len(), pending.len(), self.at_end), |at| {
                    (at + 1, at, true)
                }),
        };
        let room = self
            .limits
            .max_record_bytes
            .saturating_sub(skipping.head.len());
        skipping
            .head
            .extend_from_slice(&pending[..record.min(room)]);
        skipping.len += len as u64;
        self.advance(len);
        // At the end of input a frame cut short still reports as oversize.
        if done || self.at_end {
            return self
                .skipping
                .take()
                .map(|Skipping { head, len, .. }| Frame::Oversize { head, len });
        }
        None
    }

    /// The next frame from `reader`, `None` once it has no more bytes and
    /// the decoder no whole frame. Cancel-safe: bytes read stay in the decoder.
    ///
    /// # Errors
    ///
    /// The reader's error, or a [`FramingError`] as `InvalidData`.
    #[cfg(feature = "tokio")]
    pub async fn read<R>(&mut self, reader: &mut R) -> std::io::Result<Option<Frame>>
    where
        R: tokio::io::AsyncBufRead + Unpin,
    {
        use tokio::io::AsyncBufReadExt as _;
        loop {
            let frame = self
                .next_frame()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            if frame.is_some() || self.at_end {
                return Ok(frame);
            }
            let chunk = reader.fill_buf().await?;
            if chunk.is_empty() {
                return Ok(None);
            }
            let read = chunk.len();
            self.push(chunk);
            reader.consume(read);
        }
    }

    fn advance(&mut self, len: usize) {
        self.start += len;
        self.taken += len as u64;
        self.seen = 0;
    }

    /// Drop the frames already taken from the front of the buffer.
    fn compact(&mut self) {
        self.buf.drain(..self.start);
        self.start = 0;
    }
}

/// The line at the front of `buf`, the record in it being `record(line,
/// ended)`, where `ended` is false for a last line the end of input cut off.
fn split_line(
    buf: &[u8],
    seen: usize,
    limits: &DecodeLimits,
    at_end: bool,
    record: impl Fn(&[u8], bool) -> Range<usize>,
) -> Split {
    let (line, len, ended) = match newline(buf.get(seen..).unwrap_or_default()) {
        Some(at) => (&buf[..seen + at], seen + at + 1, true),
        None if at_end => (buf, buf.len(), false),
        None if buf.len() > limits.max_record_bytes => {
            return Split::Oversize {
                record: 0..buf.len(),
                len: buf.len(),
                rest: Some(Rest::Line),
            };
        }
        None => return Split::More,
    };
    if line.len() > limits.max_record_bytes {
        return Split::Oversize {
            record: 0..line.len(),
            len,
            rest: None,
        };
    }
    let record = record(line, ended);
    if record.is_empty() {
        Split::Skip { len }
    } else {
        Split::Record { record, len }
    }
}

/// The line without the carriage returns that end it.
fn without_carriage_returns(line: &[u8]) -> Range<usize> {
    0..line.iter().rposition(|&b| b != b'\r').map_or(0, |i| i + 1)
}

/// The line without the ASCII whitespace around it.
fn without_whitespace(line: &[u8]) -> Range<usize> {
    let end = line
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(0, |i| i + 1);
    let start = line[..end]
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(end);
    start..end
}

/// The counted frame at the front of `buf`, or `None` when it does not start
/// with a count to trust.
fn split_counted(buf: &[u8], limits: &DecodeLimits, at_end: bool) -> Option<Split> {
    if !matches!(buf.first(), Some(b'1'..=b'9')) {
        return None;
    }
    let digits = buf.iter().take_while(|b| b.is_ascii_digit()).count();
    let count = buf[..digits].iter().try_fold(0_usize, |count, &digit| {
        count
            .checked_mul(10)
            .and_then(|count| count.checked_add(usize::from(digit - b'0')))
    });
    let frame_limit = limits.max_frame_bytes.max(limits.max_record_bytes);
    let count = count.filter(|&count| count <= frame_limit)?;
    let start = digits + 1;
    match (buf.get(digits), buf.get(start)) {
        (Some(b' '), Some(b'<')) => {}
        (Some(b' '), None) | (None, _) if !at_end => return Some(Split::More),
        _ => return None,
    }
    let held = buf.len() - start;
    // A frame the end of input cut short holds what arrived of it.
    let end = buf.len().min(start + count);
    if count > limits.max_record_bytes {
        let rest = (held < count && !at_end).then(|| Rest::Bytes((count - held) as u64));
        return Some(Split::Oversize {
            record: start..end,
            len: end,
            rest,
        });
    }
    Some(match (held >= count, at_end) {
        (true, _) => Split::Record {
            record: start..end,
            len: end,
        },
        (false, true) => Split::Skip { len: end },
        (false, false) => Split::More,
    })
}

/// The index of the first `\n` in `bytes`.
fn newline(bytes: &[u8]) -> Option<usize> {
    #[cfg(feature = "memchr")]
    return memchr::memchr(b'\n', bytes);
    #[cfg(not(feature = "memchr"))]
    bytes.iter().position(|&b| b == b'\n')
}

fn encode_array(records: &[&[u8]], out: &mut Vec<u8>) {
    out.push(b'[');
    for (index, record) in records.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        out.extend_from_slice(record);
    }
    out.push(b']');
}

fn skip_whitespace(bytes: &[u8], mut at: usize) -> usize {
    while bytes.get(at).is_some_and(u8::is_ascii_whitespace) {
        at += 1;
    }
    at
}

/// What sits at `at`, for an error message.
fn found(bytes: &[u8], at: usize) -> String {
    bytes.get(at).map_or_else(
        || "end of input".to_string(),
        |&b| format!("byte {b:#04x} ('{}')", char::from(b)),
    )
}

/// Refuse anything but `byte` at `at`.
fn expect(bytes: &[u8], at: usize, byte: u8) -> Result<(), FramingError> {
    match bytes.get(at) {
        Some(&b) if b == byte => Ok(()),
        Some(_) => Err(FramingError::UnexpectedByte(at)),
        None => Err(FramingError::UnexpectedEof),
    }
}

/// Refuse anything but whitespace from `at` on.
fn end_of_unit(bytes: &[u8], at: usize) -> Result<(), FramingError> {
    let at = skip_whitespace(bytes, at);
    if at < bytes.len() {
        return Err(FramingError::TrailingGarbage(at));
    }
    Ok(())
}

/// The elements of the array whose `[` is at `open`, without the whitespace
/// around them, and the index past its `]`.
fn array_elements(bytes: &[u8], open: usize) -> Result<(Vec<Range<usize>>, usize), FramingError> {
    let mut elements = Vec::new();
    let mut at = open + 1;
    loop {
        at = skip_whitespace(bytes, at);
        match bytes.get(at) {
            None => return Err(FramingError::UnexpectedEof),
            Some(b']') if elements.is_empty() => return Ok((elements, at + 1)),
            Some(b']' | b',') => return Err(FramingError::EmptyElement(at)),
            Some(_) => {}
        }
        let end = value_end(bytes, at, b']')?;
        let trimmed = bytes[at..end]
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            .map_or(at, |i| at + i + 1);
        elements.push(at..trimmed);
        if bytes[end] == b']' {
            return Ok((elements, end + 1));
        }
        at = end + 1;
    }
}

/// The index of the `,` or `close` that ends the value starting at `at`,
/// skipping strings and nested containers.
fn value_end(bytes: &[u8], mut at: usize, close: u8) -> Result<usize, FramingError> {
    let mut depth = 0_usize;
    while let Some(&byte) = bytes.get(at) {
        match byte {
            b'"' => {
                at = string_end(bytes, at)?;
                continue;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' if depth > 0 => depth -= 1,
            b',' if depth == 0 => return Ok(at),
            byte if byte == close => return Ok(at),
            b'}' | b']' => return Err(FramingError::Unbalanced(at)),
            _ => {}
        }
        at += 1;
    }
    Err(FramingError::UnexpectedEof)
}

/// The index past the container that opens at `open`.
fn container_end(bytes: &[u8], open: usize) -> Result<usize, FramingError> {
    let mut depth = 0_usize;
    let mut at = open;
    while let Some(&byte) = bytes.get(at) {
        match byte {
            b'"' => {
                at = string_end(bytes, at)?;
                continue;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(at + 1);
                }
            }
            _ => {}
        }
        at += 1;
    }
    Err(FramingError::UnexpectedEof)
}

/// The index past the `"` that closes the string opening at `open`.
fn string_end(bytes: &[u8], open: usize) -> Result<usize, FramingError> {
    let mut escaped = false;
    for (offset, &byte) in bytes[open + 1..].iter().enumerate() {
        match byte {
            _ if escaped => escaped = false,
            b'\\' => escaped = true,
            b'"' => return Ok(open + offset + 2),
            _ => {}
        }
    }
    Err(FramingError::UnexpectedEof)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapped() -> JsonWrapped {
        JsonWrapped::new("records")
    }

    /// Records over 4 bytes are oversize, and counts over 8 are not trusted.
    const SMALL: DecodeLimits = DecodeLimits {
        max_decoded_bytes: usize::MAX,
        max_record_bytes: 4,
        max_frame_bytes: 8,
    };

    const ANY: DecodeLimits = DecodeLimits::UNBOUNDED;

    #[test]
    fn each_framing_encodes_these_bytes() {
        let quoted_key = JsonWrapped::new("a\"b");
        // (framing, records, unit, records taken)
        let cases: [(&dyn Framing, &[&[u8]], &[u8], usize); 11] = [
            (&Ndjson, &[b"a", b"b\n", b""], b"a\nb\n\n", 3),
            (&Lf, &[b"a", b"b\n", b""], b"a\nb\n\n\n", 3),
            (&OctetCounting, &[b"<1>a", b"<2>bc"], b"4 <1>a5 <2>bc", 2),
            (&JsonConcat, &[br#"{"a":1}"#, b"[2]"], br#"{"a":1}[2]"#, 2),
            (&JsonArray, &[b"1", b"{}"], b"[1,{}]", 2),
            (&JsonArray, &[], b"[]", 0),
            (&wrapped(), &[b"1", b"2"], br#"{"records":[1,2]}"#, 2),
            (&quoted_key, &[b"1"], br#"{"a\"b":[1]}"#, 1),
            (&Single, &[b"x", b"y"], b"x", 1),
            (&Single, &[], b"", 0),
            (&Ndjson, &[], b"", 0),
        ];
        for (framing, records, unit, taken) in cases {
            let mut out = b"kept".to_vec();
            assert_eq!(framing.encode(records, &mut out), taken, "{framing:?}");
            assert_eq!(&out[..4], b"kept", "{framing:?} appends to out");
            assert_eq!(
                String::from_utf8_lossy(&out[4..]),
                String::from_utf8_lossy(unit),
                "{framing:?}"
            );
        }
    }

    #[test]
    fn each_framing_decodes_these_records() {
        // (framing, unit, records)
        let cases: [(&dyn Framing, &[u8], &[&[u8]]); 29] = [
            (
                &Ndjson,
                b"{\"a\":1}\r\n\n   \n  {\"b\": 2}  \n\t\r\nlast",
                &[br#"{"a":1}"#, br#"{"b": 2}"#, b"last"],
            ),
            (&Ndjson, b"", &[]),
            (
                &Lf,
                b"a\r\n\r\n\nb\r\r\n c \nlast",
                &[b"a", b"b", b" c ", b"last"],
            ),
            (
                &OctetCounting,
                b"4 <1>a5 <2>bc\n6 <3>def<4>lf line\r\n",
                &[b"<1>a", b"<2>bc", b"<3>def", b"<4>lf line"],
            ),
            // A count starts 1 to 9: a leading 0 makes the frame an LF line.
            (&OctetCounting, b"05 <1>ab\n0 \n", &[b"05 <1>ab", b"0 "]),
            // A count needs a space then `<` after it, or the frame is an LF line.
            (
                &OctetCounting,
                b"2026 Oct 11 10:00:00 host msg\n4 <1>a",
                &[b"2026 Oct 11 10:00:00 host msg", b"<1>a"],
            ),
            // The input ends after the space: an LF line, its whitespace trimmed.
            (&OctetCounting, b"4 ", &[b"4"]),
            // A line keeps its spaces, and the last one with no \n loses them.
            (&OctetCounting, b"<1>a \n  x  ", &[b"<1>a ", b"x"]),
            // Whitespace left at the end of input is no record.
            (&OctetCounting, b"4 <1>a \t ", &[b"<1>a"]),
            // A counted frame the end of input cut short is dropped.
            (&OctetCounting, b"<1>a\n10 <short", &[b"<1>a"]),
            // lf keeps the spaces of its last line, as of every other.
            (&Lf, b"a\n \t ", &[b"a", b" \t "]),
            (&OctetCounting, b"", &[]),
            (
                &JsonConcat,
                b" {\"a\":\"}\"} [1,2]\n{}",
                &[br#"{"a":"}"}"#, b"[1,2]", b"{}"],
            ),
            (&JsonArray, b"[]", &[]),
            (&JsonArray, b"  [  ]  ", &[]),
            (&JsonArray, b"[1, 2, 3]", &[b"1", b"2", b"3"]),
            (
                &JsonArray,
                b"\n\t [\n  {\"a\":1}  ,  {\"b\":2}\n] \n",
                &[br#"{"a":1}"#, br#"{"b":2}"#],
            ),
            (
                &JsonArray,
                br#"["a,b[c]{d}", "plain"]"#,
                &[br#""a,b[c]{d}""#, br#""plain""#],
            ),
            (
                &JsonArray,
                br#"["he said \"hi\", then left", 7]"#,
                &[br#""he said \"hi\", then left""#, b"7"],
            ),
            (&JsonArray, br#"["path\\", 1]"#, &[br#""path\\""#, b"1"]),
            (
                &JsonArray,
                br#"[[1,2],[3],{"k":[4,5]}]"#,
                &[b"[1,2]", b"[3]", br#"{"k":[4,5]}"#],
            ),
            (
                &JsonArray,
                br#"[{"a":{"b":{"c":[1,{"d":2}]}}}]"#,
                &[br#"{"a":{"b":{"c":[1,{"d":2}]}}}"#],
            ),
            (
                &JsonArray,
                "[\"caf\u{e9}\", \"na\u{ef}ve\"]".as_bytes(),
                &["\"caf\u{e9}\"".as_bytes(), "\"na\u{ef}ve\"".as_bytes()],
            ),
            // Framing, not parsing: an element runs to its comma or bracket.
            (&JsonArray, b"[1 2]", &[b"1 2"]),
            (
                &wrapped(),
                br#"{"count": 2, "records": [1, {"a":[2]}], "more": "x"}"#,
                &[b"1", br#"{"a":[2]}"#],
            ),
            (&wrapped(), br#" { "records" : [] } "#, &[]),
            (&JsonWrapped::new("a\"b"), br#"{"a\"b":[1]}"#, &[b"1"]),
            (&Single, b"any \n bytes", &[b"any \n bytes"]),
            (&Single, b"", &[b""]),
        ];
        for (framing, unit, records) in cases {
            assert_eq!(
                framing.decode(unit, &ANY),
                Ok(Decoded {
                    records: records.to_vec(),
                    oversize: Vec::new()
                }),
                "{framing:?} {:?}",
                String::from_utf8_lossy(unit)
            );
        }
    }

    #[test]
    fn malformed_units_are_refused() {
        let tiny = DecodeLimits {
            max_decoded_bytes: 3,
            ..ANY
        };
        let not_an_array = |found: &str| FramingError::NotAnArray(found.to_string());
        let eof = FramingError::UnexpectedEof;
        // (framing, unit, limits, error)
        let cases: [(&dyn Framing, &[u8], DecodeLimits, FramingError); 20] = [
            (
                &JsonArray,
                br#"{"a":1}"#,
                ANY,
                not_an_array("byte 0x7b ('{')"),
            ),
            (&JsonArray, b"", ANY, not_an_array("end of input")),
            (&JsonArray, b" \n\t ", ANY, not_an_array("end of input")),
            (&JsonArray, b"[1, 2", ANY, eof.clone()),
            (&JsonArray, br#"["unclosed]"#, ANY, eof.clone()),
            (&JsonArray, b"[", ANY, eof.clone()),
            (
                &JsonArray,
                b"[1, 2] junk",
                ANY,
                FramingError::TrailingGarbage(7),
            ),
            (&JsonArray, b"[1]]", ANY, FramingError::TrailingGarbage(3)),
            (&JsonArray, b"[1, 2, ]", ANY, FramingError::EmptyElement(7)),
            (&JsonArray, b"[, 1]", ANY, FramingError::EmptyElement(1)),
            (&JsonArray, b"[1,, 2]", ANY, FramingError::EmptyElement(3)),
            (&JsonArray, b"[1}]", ANY, FramingError::Unbalanced(2)),
            (
                &wrapped(),
                b"[1]",
                ANY,
                FramingError::NotAnObject("byte 0x5b ('[')".to_string()),
            ),
            (
                &wrapped(),
                br#"{"other":[1]}"#,
                ANY,
                FramingError::MissingKey("records".to_string()),
            ),
            (
                &wrapped(),
                br#"{"records": 5}"#,
                ANY,
                not_an_array("byte 0x35 ('5')"),
            ),
            (&wrapped(), br#"{"records":[1]"#, ANY, eof.clone()),
            (
                &wrapped(),
                b"{records:[1]}",
                ANY,
                FramingError::UnexpectedByte(1),
            ),
            (&wrapped(), br#"{"a":}"#, ANY, FramingError::EmptyElement(5)),
            (
                &JsonConcat,
                br#"{"a":1} 2"#,
                ANY,
                FramingError::UnexpectedByte(8),
            ),
            (
                &Ndjson,
                b"1234",
                tiny,
                FramingError::TooLarge { len: 4, max: 3 },
            ),
        ];
        for (framing, unit, limits, error) in cases {
            assert_eq!(
                framing.decode(unit, &limits),
                Err(error),
                "{framing:?} {:?}",
                String::from_utf8_lossy(unit)
            );
        }
        assert_ne!(eof.to_string(), "");
    }

    #[test]
    fn records_over_the_limit_are_reported_not_returned() {
        // (framing, unit, records, oversize)
        type Records = &'static [&'static [u8]];
        let cases: [(&dyn Framing, &[u8], Records, Records); 7] = [
            (
                &Ndjson,
                b"abc\n abcdef \nxy",
                &[b"abc", b"xy"],
                &[b" abcdef "],
            ),
            (&Lf, b"abc\nabcd\r\nxy", &[b"abc", b"xy"], &[b"abcd\r"]),
            (
                &OctetCounting,
                b"3 <ab6 <abcde2 <x",
                &[b"<ab", b"<x"],
                &[b"<abcde"],
            ),
            // A frame over the limit the end of input cut short reports as oversize.
            (&OctetCounting, b"2 <x6 <ab", &[b"<x"], &[b"<ab"]),
            // A count over max_frame_bytes reads as an LF line, and counting resumes after it.
            (
                &OctetCounting,
                b"20 <abcdefghi\n2 <o",
                &[b"<o"],
                &[b"20 <abcdefghi"],
            ),
            (
                &JsonArray,
                br#"[1,"long",2]"#,
                &[b"1", b"2"],
                &[br#""long""#],
            ),
            (&Single, b"abcde", &[], &[b"abcde"]),
        ];
        for (framing, unit, records, oversize) in cases {
            assert_eq!(
                framing.decode(unit, &SMALL),
                Ok(Decoded {
                    records: records.to_vec(),
                    oversize: oversize.to_vec()
                }),
                "{framing:?} {:?}",
                String::from_utf8_lossy(unit)
            );
        }
    }

    /// Records, then the oversize heads, with lengths left out.
    fn sorted(frames: Vec<Frame>) -> Vec<Frame> {
        let (mut records, oversize): (Vec<Frame>, Vec<Frame>) = frames
            .into_iter()
            .partition(|frame| matches!(frame, Frame::Record(_)));
        records.extend(oversize.into_iter().map(|frame| match frame {
            Frame::Oversize { head, .. } => Frame::Oversize { head, len: 0 },
            record @ Frame::Record(_) => record,
        }));
        records
    }

    /// Fed in pieces of every size, a stream gives the records and oversize
    /// heads a whole-unit decode does, and accounts for every byte.
    #[test]
    fn a_stream_in_any_pieces_decodes_as_the_whole_unit() {
        let cases: [(Arc<dyn Framing>, &[u8]); 3] = [
            (Arc::new(Ndjson), b"ab\r\n\n  cd \nabcdefgh\nxy"),
            (Arc::new(Lf), b"ab\r\n\ncdefghij\n\nk"),
            (
                Arc::new(OctetCounting),
                b"2 <a6 <abcde\n20 <abcdefghi\n1 <<5>lf\n3 ab\n0 x\n4 <1>a6 <ab",
            ),
        ];
        for (framing, unit) in cases {
            let whole = framing.decode(unit, &SMALL).unwrap();
            let expected = sorted(
                whole
                    .records
                    .iter()
                    .map(|record| Frame::Record(record.to_vec()))
                    .chain(whole.oversize.iter().map(|record| Frame::Oversize {
                        head: record[..record.len().min(4)].to_vec(),
                        len: 0,
                    }))
                    .collect(),
            );
            for piece in 1..=unit.len() {
                let mut decoder = StreamDecoder::new(Arc::clone(&framing), SMALL);
                let mut frames = Vec::new();
                for chunk in unit.chunks(piece) {
                    decoder.push(chunk);
                    while let Some(frame) = decoder.next_frame().unwrap() {
                        frames.push(frame);
                    }
                }
                decoder.finish();
                while let Some(frame) = decoder.next_frame().unwrap() {
                    frames.push(frame);
                }
                assert_eq!(
                    decoder.taken(),
                    unit.len() as u64,
                    "{framing:?} in {piece}s"
                );
                assert_eq!(sorted(frames), expected, "{framing:?} in pieces of {piece}");
            }
        }
    }

    #[test]
    fn a_stream_decoder_reports_cut_frames_and_unit_framings() {
        let mut lines = StreamDecoder::new(Arc::new(Lf), ANY);
        lines.push(b"held");
        assert_eq!(lines.next_frame(), Ok(None), "a line waits for its newline");
        lines.finish();
        assert_eq!(
            lines.next_frame(),
            Ok(Some(Frame::Record(b"held".to_vec())))
        );
        assert_eq!(lines.taken(), 4);

        // At the end of input a cut oversize frame reports, and a cut frame
        // within the limit is dropped, its bytes counted as taken.
        let mut counted = StreamDecoder::new(Arc::new(OctetCounting), SMALL);
        counted.push(b"6 <ab");
        assert_eq!(counted.next_frame(), Ok(None));
        counted.finish();
        let head = b"<ab".to_vec();
        assert_eq!(
            counted.next_frame(),
            Ok(Some(Frame::Oversize { head, len: 5 }))
        );
        let mut cut = StreamDecoder::new(Arc::new(OctetCounting), ANY);
        cut.push(b"10 <short");
        assert_eq!(cut.next_frame(), Ok(None));
        cut.finish();
        assert_eq!((cut.next_frame(), cut.taken()), (Ok(None), 9));

        // A space at the end of what arrived waits for the byte after it.
        let mut split = StreamDecoder::new(Arc::new(OctetCounting), ANY);
        let record = |bytes: &[u8]| -> Result<Option<Frame>, FramingError> {
            Ok(Some(Frame::Record(bytes.to_vec())))
        };
        split.push(b"4 ");
        assert_eq!(split.next_frame(), Ok(None));
        split.push(b"<1>a3 ");
        assert_eq!(split.next_frame(), record(b"<1>a"));
        assert_eq!(split.next_frame(), Ok(None));
        split.push(b"x\n4 ");
        assert_eq!(split.next_frame(), record(b"3 x"), "no `<`: an LF line");
        assert_eq!(split.next_frame(), Ok(None));
        split.finish();
        assert_eq!(
            split.next_frame(),
            record(b"4"),
            "the input ended after the space"
        );

        let mut whole = StreamDecoder::new(Arc::new(JsonArray), SMALL);
        whole.push(b"[1]");
        assert_eq!(whole.next_frame(), Err(FramingError::NotAStream));
    }

    /// Each built-in name builds its framing. Two units of one that appends,
    /// end to end, decode as the records of both.
    #[test]
    fn built_in_names_build_framings_that_append_as_they_say() {
        for name in BUILT_IN {
            let records: [&[u8]; 2] = if *name == "octet_counting" {
                [b"<1>a", b"<2>b"]
            } else {
                [br#"{"a":1}"#, b"[2]"]
            };
            let mut twice = records.to_vec();
            twice.extend(records);
            let framing = FramingConfig {
                framing_type: (*name).to_string(),
                ..FramingConfig::default()
            }
            .build()
            .unwrap();
            let mut unit = Vec::new();
            let _ = framing.encode(&records, &mut unit);
            let _ = framing.encode(&records, &mut unit);
            let appended = framing.decode(&unit, &ANY);
            assert_eq!(
                appended.is_ok_and(|decoded| decoded.records == twice),
                framing.appends(),
                "{name}"
            );
            let streams = StreamDecoder::new(framing.clone(), ANY)
                .next_frame()
                .is_ok();
            assert!(streams || !framing.streams(), "{name}");
        }
    }

    #[test]
    fn config_names_a_framing_an_app_registered() {
        #[derive(Debug)]
        struct Reversed;
        impl Framing for Reversed {
            fn content_type(&self) -> &'static str {
                "text/plain"
            }
            fn appends(&self) -> bool {
                false
            }
            fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
                out.extend(records.iter().rev().flat_map(|r| r.iter()));
                records.len()
            }
            fn decode<'a>(
                &self,
                unit: &'a [u8],
                limits: &DecodeLimits,
            ) -> Result<Decoded<'a>, FramingError> {
                Ok(Decoded::from_records([unit], limits))
            }
        }

        let named = |name: &str| FramingConfig {
            framing_type: name.to_string(),
            ..FramingConfig::default()
        };
        assert!(matches!(
            named("framing_test_reversed").build(),
            Err(FramingError::Unknown { .. })
        ));
        register_framing("framing_test_reversed", Arc::new(Reversed)).unwrap();
        let framing = named("framing_test_reversed").build().unwrap();
        let mut unit = Vec::new();
        assert_eq!(framing.encode(&[b"a", b"b"], &mut unit), 2);
        assert_eq!(unit, b"ba");

        assert!(matches!(
            register_framing("ndjson", Arc::new(Reversed)),
            Err(RegistryError::Reserved { .. })
        ));
        let unknown = named("nope").build().unwrap_err().to_string();
        assert!(
            unknown.contains("ndjson") && unknown.contains("framing_test_reversed"),
            "{unknown}"
        );

        let events = FramingConfig {
            framing_type: "json_wrapped".to_string(),
            key: "events".to_string(),
        };
        let mut unit = Vec::new();
        let _ = events.build().unwrap().encode(&[b"1"], &mut unit);
        assert_eq!(unit, br#"{"events":[1]}"#);
    }

    #[cfg(feature = "serde_json")]
    #[test]
    fn framing_config_reads_its_type_and_key() {
        let read = |json: &str| serde_json::from_str::<FramingConfig>(json).unwrap();
        let defaults = read("{}");
        assert_eq!(defaults, FramingConfig::default());
        assert_eq!(
            (defaults.framing_type.as_str(), defaults.key.as_str()),
            ("ndjson", "records")
        );
        let set = read(r#"{"type":"json_wrapped","key":"events"}"#);
        assert_eq!(
            (set.framing_type.as_str(), set.key.as_str()),
            ("json_wrapped", "events")
        );
    }
}
