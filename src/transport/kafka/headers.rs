// Project:   scalo
// File:      src/transport/kafka/headers.rs
// Purpose:   Record headers across a Kafka hop, bounded in count and bytes
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Record headers on the Kafka wire.
//!
//! A send puts every record header on the message as a Kafka header (name to
//! key, bytes to value), and a receive reads every Kafka header back onto the
//! record. Both directions stop at [`MAX_HEADERS`] headers or
//! [`MAX_HEADER_BYTES`] of header names and values, whichever comes first, and
//! leave off the rest. A header past a cap is counted in
//! `transport_dropped_total{transport="kafka",reason="headers_over_cap"}`,
//! one count per header, and logged once per direction. A received header whose
//! key is not UTF-8 is skipped and counted under `reason="header_key_not_utf8"`.

use std::sync::atomic::{AtomicBool, Ordering};

use rdkafka::message::{Header, Headers, OwnedHeaders};

use crate::transport::propagation::TRACEPARENT_HEADER;

/// A received header whose key is not UTF-8.
pub(super) struct BadKey;

/// Header `idx` of `headers`, `Ok(None)` past the end.
///
/// rdkafka unwraps the key's UTF-8 conversion, so any producer can panic the
/// receive with one header. That panic is caught here and read as [`BadKey`]. A
/// `panic = "abort"` build still aborts.
pub(super) fn header_at<H: Headers>(
    headers: &H,
    idx: usize,
) -> Result<Option<Header<'_, &[u8]>>, BadKey> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| headers.try_get(idx)))
        .map_err(|_| BadKey)
}

/// Most headers one record carries across a hop.
pub(super) const MAX_HEADERS: usize = 64;

/// Most bytes of header names and values one record carries across a hop.
pub(super) const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Which way a record is crossing, for the once-only log line.
#[derive(Clone, Copy)]
enum Direction {
    Send,
    Receive,
}

impl Direction {
    fn logged(self) -> &'static AtomicBool {
        static SEND: AtomicBool = AtomicBool::new(false);
        static RECEIVE: AtomicBool = AtomicBool::new(false);
        match self {
            Self::Send => &SEND,
            Self::Receive => &RECEIVE,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::Receive => "receive",
        }
    }
}

/// What is left of the caps. Once a header is refused every later one is too,
/// so a record keeps its leading headers and never a gappy selection.
struct Budget {
    count: usize,
    bytes: usize,
    full: bool,
}

impl Budget {
    const fn new() -> Self {
        Self {
            count: 0,
            bytes: 0,
            full: false,
        }
    }

    /// Take a header of `name` and `value` bytes, or refuse it and every one after.
    fn admit(&mut self, name: usize, value: usize) -> bool {
        let size = name.saturating_add(value);
        if self.full
            || self.count >= MAX_HEADERS
            || self.bytes.saturating_add(size) > MAX_HEADER_BYTES
        {
            self.full = true;
            return false;
        }
        self.count += 1;
        self.bytes += size;
        true
    }
}

/// The Kafka headers for a record with `headers`, and how many the caps left off.
///
/// `traceparent`, the active span's, goes first and counts toward the caps,
/// and it replaces any record header of that name, so a record read from one
/// hop and sent on does not carry two. `None` when there is nothing to send.
fn bounded_send(
    headers: &[(String, Vec<u8>)],
    traceparent: Option<&str>,
) -> (Option<OwnedHeaders>, usize) {
    let mut budget = Budget::new();
    let mut out = OwnedHeaders::new_with_capacity(headers.len().min(MAX_HEADERS) + 1);
    let mut kept = 0_usize;
    let mut dropped = 0_usize;
    if let Some(tp) = traceparent
        && budget.admit(TRACEPARENT_HEADER.len(), tp.len())
    {
        out = out.insert(Header {
            key: TRACEPARENT_HEADER,
            value: Some(tp),
        });
        kept += 1;
    }
    for (name, value) in headers {
        if traceparent.is_some() && name == TRACEPARENT_HEADER {
            continue;
        }
        if budget.admit(name.len(), value.len()) {
            out = out.insert(Header {
                key: name.as_str(),
                value: Some(value.as_slice()),
            });
            kept += 1;
        } else {
            dropped += 1;
        }
    }
    ((kept > 0).then_some(out), dropped)
}

/// Most bytes librdkafka adds to a header for its name and value lengths.
const HEADER_FRAMING: usize = 6;

/// The bytes `headers` add to a record's size on the wire, counting only the
/// headers the caps let through. An active `traceparent` is left to the
/// record's framing allowance.
pub(super) fn wire_len(headers: &[(String, Vec<u8>)]) -> usize {
    let mut budget = Budget::new();
    headers
        .iter()
        .take_while(|(name, value)| budget.admit(name.len(), value.len()))
        .map(|(name, value)| name.len() + value.len() + HEADER_FRAMING)
        .sum()
}

/// The Kafka headers to put on a record, counting and logging any left off.
pub(super) fn for_send(
    headers: &[(String, Vec<u8>)],
    traceparent: Option<&str>,
) -> Option<OwnedHeaders> {
    let (out, dropped) = bounded_send(headers, traceparent);
    report(Direction::Send, dropped);
    out
}

/// Headers a receive left off: past a cap, or with a key that is not UTF-8.
#[derive(Debug, Default, PartialEq, Eq)]
struct Dropped {
    over_cap: usize,
    bad_key: usize,
}

/// The record headers for a message's Kafka `headers`, and what was left off.
/// A header with no value reads as empty.
fn bounded_receive<H: Headers>(headers: &H) -> (Vec<(String, Vec<u8>)>, Dropped) {
    let total = headers.count();
    let mut budget = Budget::new();
    let mut out = Vec::with_capacity(total.min(MAX_HEADERS));
    let mut dropped = Dropped::default();
    for idx in 0..total {
        match header_at(headers, idx) {
            Ok(Some(header)) => {
                let value = header.value.unwrap_or_default();
                if budget.admit(header.key.len(), value.len()) {
                    out.push((header.key.to_owned(), value.to_vec()));
                } else {
                    dropped.over_cap += 1;
                }
            }
            Ok(None) => break,
            Err(BadKey) => dropped.bad_key += 1,
        }
    }
    (out, dropped)
}

/// The record headers for a message's Kafka `headers`, counting and logging
/// any left off.
pub(super) fn from_message<H: Headers>(headers: Option<&H>) -> Vec<(String, Vec<u8>)> {
    let Some(headers) = headers else {
        return Vec::new();
    };
    let (out, dropped) = bounded_receive(headers);
    report(Direction::Receive, dropped.over_cap);
    report_bad_keys(dropped.bad_key);
    out
}

/// Count `dropped` headers with a key that is not UTF-8, logging the first time.
fn report_bad_keys(dropped: usize) {
    static LOGGED: AtomicBool = AtomicBool::new(false);
    if dropped == 0 {
        return;
    }
    #[cfg(feature = "metrics")]
    ::metrics::counter!(
        "transport_dropped_total",
        "transport" => "kafka",
        "reason" => "header_key_not_utf8"
    )
    .increment(dropped as u64);
    if !LOGGED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            dropped,
            "kafka: a received record has a header key that is not UTF-8, so that header is \
             left off; counted in transport_dropped_total{{reason=\"header_key_not_utf8\"}}, \
             logged once"
        );
    }
}

/// Count `dropped` headers and log the first time in this direction.
fn report(direction: Direction, dropped: usize) {
    if dropped == 0 {
        return;
    }
    #[cfg(feature = "metrics")]
    ::metrics::counter!(
        "transport_dropped_total",
        "transport" => "kafka",
        "reason" => "headers_over_cap"
    )
    .increment(dropped as u64);
    if !direction.logged().swap(true, Ordering::Relaxed) {
        tracing::warn!(
            direction = direction.as_str(),
            dropped,
            max_headers = MAX_HEADERS,
            max_header_bytes = MAX_HEADER_BYTES,
            "kafka: a record has more headers than the caps allow, so the rest are left off; \
             counted in transport_dropped_total{{reason=\"headers_over_cap\"}}, logged once"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Pairs = Vec<(String, Vec<u8>)>;

    fn pairs(n: usize, value_len: usize) -> Vec<(String, Vec<u8>)> {
        (0..n)
            .map(|i| (format!("h{i}"), vec![b'x'; value_len]))
            .collect()
    }

    /// What `bounded_send` puts on the wire, read back as `(name, value)`.
    fn wire(out: Option<&OwnedHeaders>) -> Pairs {
        let Some(out) = out else {
            return Vec::new();
        };
        out.iter()
            .map(|h| (h.key.to_owned(), h.value.unwrap_or_default().to_vec()))
            .collect()
    }

    #[test]
    fn send_keeps_what_the_caps_allow() {
        let non_utf8 = vec![("bin".to_string(), vec![0xff, 0xfe, 0x00, 0x80])];
        let cases: Vec<(&str, Pairs, Option<&str>, usize, usize)> = vec![
            ("none", vec![], None, 0, 0),
            ("several", pairs(5, 10), None, 5, 0),
            (
                "at the count cap",
                pairs(MAX_HEADERS, 1),
                None,
                MAX_HEADERS,
                0,
            ),
            (
                "over the count cap",
                pairs(MAX_HEADERS + 6, 1),
                None,
                MAX_HEADERS,
                6,
            ),
            // h0..h2 are 2 + 30000 bytes each, so the third passes 64 KiB.
            ("over the byte cap", pairs(4, 30_000), None, 2, 2),
            // The first header is past the cap, so the small one after it goes too.
            (
                "a header past the cap ends the list",
                {
                    let mut v = pairs(1, MAX_HEADER_BYTES);
                    v.push(("small".to_string(), vec![1]));
                    v
                },
                None,
                0,
                2,
            ),
            ("a value that is not UTF-8", non_utf8, None, 1, 0),
            ("trace header alone", vec![], Some("00-a-b-01"), 1, 0),
            (
                "trace header takes a place in the count cap",
                pairs(MAX_HEADERS, 1),
                Some("00-a-b-01"),
                MAX_HEADERS,
                1,
            ),
        ];
        for (case, headers, traceparent, kept, dropped) in cases {
            let (out, got_dropped) = bounded_send(&headers, traceparent);
            assert_eq!(wire(out.as_ref()).len(), kept, "{case}: kept");
            assert_eq!(got_dropped, dropped, "{case}: dropped");
            assert_eq!(out.is_none(), kept == 0, "{case}: none when empty");
        }
    }

    #[test]
    fn send_keeps_names_values_and_order_and_the_non_utf8_bytes() {
        let headers = vec![
            ("b".to_string(), vec![0xff, 0x00]),
            ("a".to_string(), vec![]),
            ("b".to_string(), b"again".to_vec()),
        ];
        let (out, dropped) = bounded_send(&headers, None);
        assert_eq!(dropped, 0);
        assert_eq!(wire(out.as_ref()), headers);
    }

    #[test]
    fn the_active_traceparent_replaces_a_record_header_of_that_name() {
        let headers = vec![
            (TRACEPARENT_HEADER.to_string(), b"00-old-old-01".to_vec()),
            ("tenant".to_string(), b"t1".to_vec()),
        ];
        let (out, dropped) = bounded_send(&headers, Some("00-new-new-01"));
        assert_eq!(dropped, 0);
        assert_eq!(
            wire(out.as_ref()),
            vec![
                (TRACEPARENT_HEADER.to_string(), b"00-new-new-01".to_vec()),
                ("tenant".to_string(), b"t1".to_vec()),
            ]
        );
        // With no active span the record's own traceparent goes on as it is.
        let (out, _) = bounded_send(&headers, None);
        assert_eq!(wire(out.as_ref()), headers);
    }

    #[test]
    fn receive_keeps_what_the_caps_allow() {
        let non_utf8 = vec![("bin".to_string(), vec![0xff, 0xfe, 0x00, 0x80])];
        let cases: Vec<(&str, Pairs, usize, usize)> = vec![
            ("none", vec![], 0, 0),
            ("several", pairs(5, 10), 5, 0),
            (
                "over the count cap",
                pairs(MAX_HEADERS + 6, 1),
                MAX_HEADERS,
                6,
            ),
            ("over the byte cap", pairs(4, 30_000), 2, 2),
            ("a value that is not UTF-8", non_utf8, 1, 0),
        ];
        for (case, headers, kept, dropped) in cases {
            // Built without the send-side caps, as a foreign producer writes it.
            let mut raw = OwnedHeaders::new();
            for (name, value) in &headers {
                raw = raw.insert(Header {
                    key: name.as_str(),
                    value: Some(value.as_slice()),
                });
            }
            let (out, got_dropped) = bounded_receive(&raw);
            assert_eq!(out.len(), kept, "{case}: kept");
            assert_eq!(got_dropped.over_cap, dropped, "{case}: dropped");
            assert_eq!(got_dropped.bad_key, 0, "{case}: bad keys");
            assert_eq!(out, headers[..kept], "{case}: the leading headers, as sent");
        }
    }

    /// Headers whose key at `bad` panics on read, as rdkafka does for a key
    /// that is not UTF-8.
    struct BadKeyAt {
        inner: OwnedHeaders,
        bad: usize,
    }

    impl Headers for BadKeyAt {
        fn count(&self) -> usize {
            self.inner.count()
        }

        fn try_get(&self, idx: usize) -> Option<Header<'_, &[u8]>> {
            assert_ne!(idx, self.bad, "header key is not UTF-8");
            self.inner.try_get(idx)
        }
    }

    #[test]
    fn receive_skips_a_header_whose_key_panics_and_keeps_the_rest() {
        let mut inner = OwnedHeaders::new();
        for (name, value) in pairs(3, 1) {
            inner = inner.insert(Header {
                key: name.as_str(),
                value: Some(value.as_slice()),
            });
        }
        let (out, dropped) = bounded_receive(&BadKeyAt { inner, bad: 1 });
        assert_eq!(
            dropped,
            Dropped {
                over_cap: 0,
                bad_key: 1
            }
        );
        let names: Vec<&str> = out.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["h0", "h2"]);
    }

    #[test]
    fn receive_reads_a_header_with_no_value_as_empty() {
        let raw = OwnedHeaders::new().insert(Header::<&[u8]> {
            key: "null",
            value: None,
        });
        let (out, dropped) = bounded_receive(&raw);
        assert_eq!(dropped, Dropped::default());
        assert_eq!(out, vec![("null".to_string(), Vec::new())]);
    }

    #[test]
    fn wire_len_counts_the_headers_the_caps_let_through() {
        let cases: Vec<(&str, Pairs, usize)> = vec![
            ("none", vec![], 0),
            ("two", pairs(2, 10), 2 * (2 + 10 + HEADER_FRAMING)),
            (
                "over the count cap",
                pairs(MAX_HEADERS + 6, 1),
                10 * (2 + 1 + HEADER_FRAMING) + (MAX_HEADERS - 10) * (3 + 1 + HEADER_FRAMING),
            ),
            (
                "over the byte cap",
                pairs(4, 30_000),
                2 * (2 + 30_000 + HEADER_FRAMING),
            ),
        ];
        for (case, headers, want) in cases {
            assert_eq!(wire_len(&headers), want, "{case}");
        }
    }

    #[test]
    fn a_message_with_no_headers_reads_as_none() {
        assert_eq!(from_message::<OwnedHeaders>(None), Pairs::new());
    }
}
