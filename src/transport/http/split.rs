// Project:   scalo
// File:      src/transport/http/split.rs
// Purpose:   HTTP receive side: one request body into its records, by the framing config
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! How the receive side splits a request body into records, by the
//! transport's `framing`.
//!
//! Each record is the body's own bytes, never parsed and written back, so an
//! integer wider than `u64` arrives as sent. A body holding more than
//! [`DecodeLimits::DEFAULT_MAX_RECORDS`] is refused whole, and any other body
//! that does not frame is one record, for the consumer's parser to refuse.
//! `json` is read here and nowhere else: it takes a JSON array or NDJSON on one
//! path, which a sender can't frame.

use std::sync::Arc;

use bytes::Bytes;

use crate::framing::{DecodeLimits, Framing, FramingConfig, FramingError, JsonArray, Ndjson};
use crate::parse_guard::{MAX_PARSE_DEPTH, json_depth_within};
use crate::transport::error::{TransportError, TransportResult};

/// Bounds on a request body's records. `max_body_bytes` has bounded its bytes.
const LIMITS: DecodeLimits = DecodeLimits {
    max_records: DecodeLimits::DEFAULT_MAX_RECORDS,
    ..DecodeLimits::UNBOUNDED
};

/// Deepest a JSON array body nests: its records, then the array around them.
const ARRAY_DEPTH: usize = MAX_PARSE_DEPTH + 1;

/// The `framing` a request body is split by.
#[derive(Debug, Clone)]
pub(super) enum BodySplit {
    /// One record per body.
    Single,
    /// One record per element of a top-level JSON array, when every element parses.
    JsonArray,
    /// One record per line, when the first line is a JSON value.
    Ndjson,
    /// [`JsonArray`](Self::JsonArray), else [`Ndjson`](Self::Ndjson) for a
    /// body that does not split as an array.
    Json,
    /// Any other framing, an app's own included, as it decodes.
    Framed(Arc<dyn Framing>),
}

/// A request body's records.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BodyParts<'a> {
    /// The whole body is one record.
    Whole,
    /// These records, each a slice of the body.
    Each(Vec<&'a [u8]>),
    /// The body holds more than this many records.
    TooMany(usize),
}

impl BodySplit {
    /// The split `config` names.
    pub(super) fn new(config: &FramingConfig) -> TransportResult<Self> {
        Ok(match config.framing_type.as_str() {
            "single" => Self::Single,
            "json_array" => Self::JsonArray,
            "ndjson" => Self::Ndjson,
            super::JSON_FRAMING => Self::Json,
            _ => Self::Framed(
                config
                    .build()
                    .map_err(|e| TransportError::Config(format!("HTTP body framing: {e}")))?,
            ),
        })
    }

    /// `body`'s records.
    pub(super) fn parts<'a>(&self, body: &'a [u8]) -> BodyParts<'a> {
        match self {
            Self::Single => BodyParts::Whole,
            Self::JsonArray => array_parts(body),
            Self::Ndjson => ndjson_parts(body),
            Self::Json => match array_parts(body) {
                BodyParts::Whole => ndjson_parts(body),
                parts => parts,
            },
            Self::Framed(framing) => match framing.decode(body, &LIMITS) {
                Err(FramingError::TooManyRecords { max }) => BodyParts::TooMany(max),
                Ok(decoded) if decoded.oversize.is_empty() => BodyParts::Each(decoded.records),
                Ok(_) | Err(_) => BodyParts::Whole,
            },
        }
    }
}

/// A JSON array body's elements. A body too deep to split, not one array, or
/// holding an element that does not parse is one record.
fn array_parts(body: &[u8]) -> BodyParts<'_> {
    if body.trim_ascii_start().first() != Some(&b'[') {
        return BodyParts::Whole;
    }
    // Too deep to split stays whole, refused once rather than per element.
    if !json_depth_within(body, ARRAY_DEPTH) {
        return BodyParts::Whole;
    }
    match JsonArray.decode(body, &LIMITS) {
        Err(FramingError::TooManyRecords { max }) => BodyParts::TooMany(max),
        // Framing finds where each element ends without parsing it.
        Ok(decoded) if parses(body) => BodyParts::Each(decoded.records),
        Ok(_) | Err(_) => BodyParts::Whole,
    }
}

/// An NDJSON body's lines, when there are two or more and the first is one
/// whole JSON value, which a pretty-printed object's `{` line is not.
fn ndjson_parts(body: &[u8]) -> BodyParts<'_> {
    let first = body
        .split(|&b| b == b'\n')
        .map(<[u8]>::trim_ascii)
        .find(|line| !line.is_empty());
    if !first.is_some_and(|first| json_depth_within(first, MAX_PARSE_DEPTH) && parses(first)) {
        return BodyParts::Whole;
    }
    match Ndjson.decode(body, &LIMITS) {
        Err(FramingError::TooManyRecords { max }) => BodyParts::TooMany(max),
        Ok(decoded) if decoded.records.len() >= 2 => BodyParts::Each(decoded.records),
        // One line is one record, sent as it came.
        Ok(_) | Err(_) => BodyParts::Whole,
    }
}

/// Whether `json` is one whole JSON value. The parse recurses per level, so
/// the caller has bounded its depth.
fn parses(json: &[u8]) -> bool {
    sonic_rs::from_slice::<sonic_rs::LazyValue<'_>>(json).is_ok()
}

/// `record` as a slice of `body`, or a copy when a framing handed back bytes
/// it did not borrow from the body.
pub(super) fn slice_of(body: &Bytes, record: &[u8]) -> Bytes {
    let start = record.as_ptr().addr().wrapping_sub(body.as_ptr().addr());
    match start.checked_add(record.len()) {
        Some(end) if end <= body.len() => body.slice(start..end),
        _ => Bytes::copy_from_slice(record),
    }
}
