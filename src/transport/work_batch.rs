// Project:   scalo
// File:      src/transport/work_batch.rs
// Purpose:   Canonical data-plane contract: Record + WorkBatch
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Canonical data-plane contract
//!
//! The single currency that flows through `get -> process -> send -> commit`:
//!
//! - [`Record`] -- one work record (payload + routing + lean metadata). It
//!   carries **no** commit token. Tokens live on the batch, not the record;
//!   that separation is what makes 1->N fan-out safe (a transform can grow or
//!   shrink the record count without disturbing the source acks).
//! - [`RecordMeta`] -- lean per-record metadata (timestamp + payload format).
//!   Deliberately leaner than the engine's `MessageMetadata`, which carries a
//!   type-erased commit token. Here tokens move to the batch.
//! - [`WorkBatch`] -- the canonical zero-copy block of records, the source
//!   commit tokens for the whole block, and any inline-DLQ entries carried
//!   forward (the no-silent-drop contract preserved from the older
//!   `RecvBatch`).
//!
//! `commit_tokens.len()` is NOT tied to `records.len()`. After a fan-out
//! transform the record count may change while the commit tokens stay equal to
//! the input source acks -- they are fired only after the WHOLE block is sent
//! (at-least-once delivery).
//!
//! The block flows `get -> process -> send -> commit` through the unified
//! engine driver (`crate::worker::engine::driver`). Fields are parsed on
//! demand via the [`codec`](super::codec). For the wider picture see
//! `docs/backpressure.md` (commit-token + brake contract),
//! `docs/self-regulation.md`, and `docs/migrations.md` (the
//! `Message`/`RawMessage`/`RecvBatch` -> `WorkBatch` collapse).

use super::filter::FilteredDlqEntry;
use super::traits::CommitToken;
use super::types::PayloadFormat;
use crate::framing::{DecodeLimits, Framing, FramingError, JsonArray, Ndjson};
use bytes::Bytes;
use std::sync::Arc;
use thiserror::Error;

/// Lean per-record metadata.
///
/// Unlike the engine's `MessageMetadata`, this carries **no** commit token --
/// tokens live on the [`WorkBatch`], not the record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMeta {
    /// Source timestamp (milliseconds since epoch), if the transport provided one.
    pub timestamp_ms: Option<i64>,

    /// Detected or declared payload format.
    pub format: PayloadFormat,
}

/// One work record: payload + routing + metadata, with **no** commit token.
///
/// The payload is held as [`bytes::Bytes`] so cloning a record (or fanning one
/// record out into many) is a refcount bump, not a deep copy.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Raw payload bytes -- zero-copy / refcounted.
    pub payload: Bytes,

    /// Routing destination (Kafka topic, gRPC routing key, ...).
    ///
    /// NOT a Kafka message/partition key -- scalo does not model per-record
    /// partition keys at this layer. The field stays named `key` because
    /// renaming a `pub` field breaks every consumer (human-only call).
    pub key: Option<Arc<str>>,

    /// Transport / application headers.
    pub headers: Vec<(String, Vec<u8>)>,

    /// Lean per-record metadata.
    pub metadata: RecordMeta,
}

/// Header name carrying an optional idempotency / dedup key.
///
/// Set by an upstream (e.g. a content hash or the source offset) and read by an
/// idempotent downstream (e.g. a dedup-on-key table, or a deterministic object
/// key) to drop duplicates on replay. This is the second half of
/// effectively-once delivery: the idempotent producer removes producer-retry
/// duplicates on the wire, the dedup key lets the sink remove replay duplicates.
/// Carried as a header (not a struct field) so it stays optional and does not
/// touch the zero-copy record layout.
pub const DEDUP_KEY_HEADER: &str = "x-scalo-dedup-key";

impl Record {
    /// The idempotency / dedup key for this record, if an upstream set one (via
    /// the `DEDUP_KEY_HEADER` header). `None` when no key was attached.
    #[must_use]
    pub fn dedup_key(&self) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(k, _)| k.as_str() == DEDUP_KEY_HEADER)
            .map(|(_, v)| v.as_slice())
    }

    /// Attach (or replace) the dedup key header, builder-style.
    #[must_use]
    pub fn with_dedup_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.headers.retain(|(k, _)| k.as_str() != DEDUP_KEY_HEADER);
        self.headers
            .push((DEDUP_KEY_HEADER.to_string(), key.into()));
        self
    }

    /// Serialise the WHOLE record (payload + key + headers + metadata) to a
    /// compact self-describing byte frame.
    ///
    /// This is what the send-side spill cache persists: spilling only the
    /// payload bytes would lose the routing key, headers, and the dedup key, so a
    /// drained replay could not address the downstream or dedupe correctly. The
    /// frame is little-endian and length-prefixed:
    ///
    /// ```text
    /// version:u8=1 | payload(len:u32, bytes) | key(present:u8, len:u32?, utf8?)
    ///   | headers(count:u32, [klen:u32,k, vlen:u32,v]*) | ts(present:u8, i64?)
    ///   | format:u8 (0=auto,1=json,2=msgpack)
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.payload.len() + 32);
        buf.push(1u8); // frame version

        put_u32(
            &mut buf,
            u32::try_from(self.payload.len()).unwrap_or(u32::MAX),
        );
        buf.extend_from_slice(&self.payload);

        match &self.key {
            Some(k) => {
                buf.push(1);
                put_u32(&mut buf, u32::try_from(k.len()).unwrap_or(u32::MAX));
                buf.extend_from_slice(k.as_bytes());
            }
            None => buf.push(0),
        }

        put_u32(
            &mut buf,
            u32::try_from(self.headers.len()).unwrap_or(u32::MAX),
        );
        for (k, v) in &self.headers {
            put_u32(&mut buf, u32::try_from(k.len()).unwrap_or(u32::MAX));
            buf.extend_from_slice(k.as_bytes());
            put_u32(&mut buf, u32::try_from(v.len()).unwrap_or(u32::MAX));
            buf.extend_from_slice(v);
        }

        match self.metadata.timestamp_ms {
            Some(ts) => {
                buf.push(1);
                buf.extend_from_slice(&ts.to_le_bytes());
            }
            None => buf.push(0),
        }

        buf.push(match self.metadata.format {
            PayloadFormat::Auto => 0,
            PayloadFormat::Json => 1,
            PayloadFormat::MsgPack => 2,
        });
        buf
    }

    /// Reconstruct a record from a frame produced by [`encode`](Self::encode).
    ///
    /// # Errors
    /// [`RecordCodecError`] on a truncated/malformed frame, an unknown version
    /// or format byte, or invalid UTF-8 in the key/header names. Bounds are
    /// checked on every field, so a corrupt frame fails fast rather than
    /// over-reading or over-allocating.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, RecordCodecError> {
        let mut r = FrameReader { buf: bytes, pos: 0 };
        let version = r.u8()?;
        if version != 1 {
            return Err(RecordCodecError::Version(version));
        }
        let payload = Bytes::copy_from_slice(r.bytes()?);
        let key = if r.u8()? == 1 {
            let raw = r.bytes()?;
            let s = std::str::from_utf8(raw).map_err(|_| RecordCodecError::Utf8)?;
            Some(Arc::from(s))
        } else {
            None
        };
        let n_headers = r.u32()? as usize;
        // Cap the pre-allocation -- a corrupt count cannot force a huge alloc.
        // The per-header reads are still bounds-checked and fail fast on EOF.
        let mut headers = Vec::with_capacity(n_headers.min(64));
        for _ in 0..n_headers {
            let k = std::str::from_utf8(r.bytes()?)
                .map_err(|_| RecordCodecError::Utf8)?
                .to_string();
            let v = r.bytes()?.to_vec();
            headers.push((k, v));
        }
        let timestamp_ms = if r.u8()? == 1 { Some(r.i64()?) } else { None };
        let format = match r.u8()? {
            0 => PayloadFormat::Auto,
            1 => PayloadFormat::Json,
            2 => PayloadFormat::MsgPack,
            other => return Err(RecordCodecError::Format(other)),
        };
        Ok(Self {
            payload,
            key,
            headers,
            metadata: RecordMeta {
                timestamp_ms,
                format,
            },
        })
    }
}

/// Append a little-endian `u32` length prefix.
fn put_u32(buf: &mut Vec<u8>, n: u32) {
    buf.extend_from_slice(&n.to_le_bytes());
}

/// Bounds-checked cursor over an encoded [`Record`] frame.
struct FrameReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> FrameReader<'a> {
    fn take(&mut self, n: usize) -> std::result::Result<&'a [u8], RecordCodecError> {
        let end = self.pos.checked_add(n).ok_or(RecordCodecError::Eof)?;
        if end > self.buf.len() {
            return Err(RecordCodecError::Eof);
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> std::result::Result<u8, RecordCodecError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> std::result::Result<u32, RecordCodecError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().unwrap_or([0; 4]),
        ))
    }
    fn i64(&mut self) -> std::result::Result<i64, RecordCodecError> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().unwrap_or([0; 8]),
        ))
    }
    /// Read a `u32`-length-prefixed byte slice.
    fn bytes(&mut self) -> std::result::Result<&'a [u8], RecordCodecError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

/// Failure modes for [`Record::decode`].
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecordCodecError {
    /// The frame ended before a field could be fully read (truncated/corrupt).
    #[error("record codec: unexpected end of frame")]
    Eof,
    /// The frame's version byte is not supported by this build.
    #[error("record codec: unsupported frame version {0}")]
    Version(u8),
    /// A key or header name was not valid UTF-8.
    #[error("record codec: invalid UTF-8 in key/header name")]
    Utf8,
    /// The payload-format byte was not a known variant.
    #[error("record codec: unknown payload format {0}")]
    Format(u8),
}

/// The canonical zero-copy block of work records.
///
/// One `WorkBatch` is the single currency through `get -> process -> send ->
/// commit`. The `commit_tokens` are the INPUT source acks -- fired once after
/// the WHOLE block is sent (at-least-once). `T` generalises the source ack: a
/// Kafka offset, a file position, or a sequence number for a push source
/// (gRPC, HTTP). A push source answers its sender when the record is queued,
/// or, once armed, when the token is released
/// ([`TransportReceiver::release`](super::TransportReceiver::release)).
///
/// `commit_tokens.len()` is intentionally decoupled from `records.len()`: a
/// fan-out transform may grow or shrink the record count while the commit
/// tokens stay equal to the input acks.
#[derive(Debug)]
pub struct WorkBatch<T: CommitToken> {
    /// The work records in this block.
    pub records: Vec<Record>,

    /// Source acks for the whole block. Fired after the block is fully sent.
    /// Length is NOT tied to `records.len()`.
    pub commit_tokens: Vec<T>,

    /// Inline-DLQ entries carried forward (no-silent-drop contract).
    pub dlq_entries: Vec<FilteredDlqEntry>,
}

impl<T: CommitToken> WorkBatch<T> {
    /// An empty batch (no records, no tokens, no DLQ entries).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            records: Vec::new(),
            commit_tokens: Vec::new(),
            dlq_entries: Vec::new(),
        }
    }

    /// A batch of records with no commit tokens and no DLQ entries.
    ///
    /// Useful for records that were generated downstream (e.g. a transform that
    /// emits new records) and do not themselves carry a source ack.
    #[must_use]
    pub fn from_records(records: Vec<Record>) -> Self {
        Self {
            records,
            commit_tokens: Vec::new(),
            dlq_entries: Vec::new(),
        }
    }

    /// A batch of records plus their source commit tokens (no DLQ entries).
    #[must_use]
    pub fn new(records: Vec<Record>, commit_tokens: Vec<T>) -> Self {
        Self {
            records,
            commit_tokens,
            dlq_entries: Vec::new(),
        }
    }

    /// Attach inline-DLQ entries to this batch, consuming and returning it.
    #[must_use]
    pub fn with_dlq_entries(mut self, dlq_entries: Vec<FilteredDlqEntry>) -> Self {
        self.dlq_entries = dlq_entries;
        self
    }

    /// Whether the batch has no records (DLQ entries may still be present).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Number of records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Number of records (explicit alias for [`WorkBatch::len`]).
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Total payload bytes across all records.
    ///
    /// Used by the byte-budget governor to size a block against a memory budget.
    /// Saturating sum: a pathological block clamps at `usize::MAX` rather than
    /// wrapping.
    #[must_use]
    pub fn total_payload_bytes(&self) -> usize {
        self.records
            .iter()
            .map(|r| r.payload.len())
            .fold(0usize, usize::saturating_add)
    }

    /// Transform the records while PRESERVING the commit tokens and DLQ entries.
    ///
    /// This is the safe shape for the future `process` handler: a transform may
    /// grow, shrink, or rewrite the records (fan-out / fan-in), but the source
    /// acks and inline-DLQ entries flow through untouched.
    #[must_use]
    pub fn map_records(mut self, f: impl FnOnce(Vec<Record>) -> Vec<Record>) -> Self {
        self.records = f(self.records);
        self
    }

    // ---- Zero-copy ingestion / framing helpers ----------------------------
    //
    // These slice ONE inbound `Bytes` blob into per-record views so the WHOLE
    // batch shares ONE allocation: `record.payload = blob.slice(start..end)` is
    // a refcounted view, never a payload copy. They do NOT parse / deserialise
    // and do NOT re-serialise -- framing only. Commit tokens and DLQ entries are
    // left EMPTY. The source attaches its acks afterwards.

    /// One record holding the WHOLE blob (no framing).
    ///
    /// The single record's payload is the blob itself (zero-copy move of the
    /// `Bytes` handle), `key` is `None`, `headers` empty, and the metadata
    /// format is auto-detected via [`PayloadFormat::detect`].
    #[must_use]
    pub fn single(blob: Bytes) -> Self {
        let format = PayloadFormat::detect(&blob);
        let record = Record {
            payload: blob,
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format,
            },
        };
        Self {
            records: vec![record],
            commit_tokens: Vec::new(),
            dlq_entries: Vec::new(),
        }
    }

    /// One record per line of an NDJSON blob, each a zero-copy view of it.
    ///
    /// Each line is trimmed of ASCII whitespace, a trailing `\r` included, and
    /// blank lines are skipped, as [`Ndjson`] decodes. A line that is not JSON
    /// is still a record: the parser downstream refuses it. The format is
    /// [`PayloadFormat::Json`].
    #[must_use]
    pub fn from_ndjson(blob: Bytes) -> Self {
        let records = Ndjson
            .decode(&blob, &DecodeLimits::UNBOUNDED)
            .map_or_default(|decoded| decoded.records);
        Self::json_records(&blob, records)
    }

    /// One record per element of a top-level JSON array blob, each a zero-copy
    /// view of it. Elements are framed, not parsed, as [`JsonArray`] decodes,
    /// and `[]` yields no records.
    ///
    /// # Errors
    ///
    /// [`FramingError`] when the blob is not one well-framed array.
    pub fn from_json_array(blob: Bytes) -> Result<Self, FramingError> {
        let records = JsonArray.decode(&blob, &DecodeLimits::UNBOUNDED)?.records;
        Ok(Self::json_records(&blob, records))
    }

    /// `records`, slices of `blob`, as JSON records sharing its allocation.
    fn json_records(blob: &Bytes, records: Vec<&[u8]>) -> Self {
        let records = records
            .into_iter()
            .map(|record| Record {
                payload: blob.slice_ref(record),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            })
            .collect();
        Self {
            records,
            commit_tokens: Vec::new(),
            dlq_entries: Vec::new(),
        }
    }
}

impl<T: CommitToken> From<crate::Message<T>> for WorkBatch<T> {
    /// Collapse a single [`crate::Message`] into a one-record `WorkBatch`.
    ///
    /// The message payload is already [`Bytes`] -- this is a move, not a copy.
    fn from(msg: crate::Message<T>) -> Self {
        let record = Record {
            payload: msg.payload,
            key: msg.key,
            headers: msg.headers,
            metadata: RecordMeta {
                timestamp_ms: msg.timestamp_ms,
                format: msg.format,
            },
        };
        Self {
            records: vec![record],
            commit_tokens: vec![msg.token],
            dlq_entries: Vec::new(),
        }
    }
}

impl<T: CommitToken> From<crate::transport::traits::RecvBatch<T>> for WorkBatch<T> {
    /// Collapse a [`RecvBatch`](crate::transport::traits::RecvBatch) into a
    /// `WorkBatch`.
    ///
    /// Each `Message<T>` becomes a [`Record`]; its `token` is collected into
    /// `commit_tokens` (preserving order). `dlq_entries` carry straight across.
    /// `filtered_tokens` (acks of records removed by inbound drop/dlq filters)
    /// are appended to `commit_tokens` so the block commit advances the source
    /// past them -- the commit-token count is deliberately NOT tied to
    /// `records.len()`. Payloads are already [`Bytes`] -- each is a move.
    fn from(batch: crate::transport::traits::RecvBatch<T>) -> Self {
        let token_capacity = batch.messages.len() + batch.filtered_tokens.len();
        let mut records = Vec::with_capacity(batch.messages.len());
        let mut commit_tokens = Vec::with_capacity(token_capacity);
        for msg in batch.messages {
            commit_tokens.push(msg.token);
            records.push(Record {
                payload: msg.payload,
                key: msg.key,
                headers: msg.headers,
                metadata: RecordMeta {
                    timestamp_ms: msg.timestamp_ms,
                    format: msg.format,
                },
            });
        }
        // Acks of filtered-out records: no Record, but the source still needs
        // committing once the block (incl. DLQ routing) is handled.
        commit_tokens.extend(batch.filtered_tokens);
        Self {
            records,
            commit_tokens,
            dlq_entries: batch.dlq_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;
    use crate::transport::traits::RecvBatch;

    /// Minimal commit token for tests.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestToken(u64);

    impl std::fmt::Display for TestToken {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "tok-{}", self.0)
        }
    }

    impl CommitToken for TestToken {}

    fn record(payload: &'static [u8]) -> Record {
        Record {
            payload: Bytes::from_static(payload),
            key: Some(Arc::from("events")),
            headers: vec![("h".to_string(), b"v".to_vec())],
            metadata: RecordMeta {
                timestamp_ms: Some(42),
                format: PayloadFormat::Json,
            },
        }
    }

    #[test]
    fn encode_decode_round_trips_full_record() {
        let original = record(b"hello world").with_dedup_key("dedup-42");
        let bytes = original.encode();
        let decoded = Record::decode(&bytes).unwrap();
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.headers, original.headers);
        assert_eq!(decoded.metadata, original.metadata);
        // The dedup key (a header) survives the round-trip -- effectively-once.
        assert_eq!(decoded.dedup_key(), Some(b"dedup-42".as_slice()));
    }

    #[test]
    fn encode_decode_round_trips_minimal_record() {
        // No key, no headers, no timestamp, msgpack format.
        let original = Record {
            payload: Bytes::from_static(&[0x81, 0xa3]),
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::MsgPack,
            },
        };
        let decoded = Record::decode(&original.encode()).unwrap();
        assert_eq!(decoded.payload, original.payload);
        assert!(decoded.key.is_none());
        assert_eq!(
            decoded.headers,
            [] as [(std::string::String, std::vec::Vec<u8>); 0]
        );
        assert_eq!(decoded.metadata, original.metadata);
    }

    #[test]
    fn encode_decode_handles_empty_payload_and_binary_headers() {
        let original = Record {
            payload: Bytes::new(),
            key: Some(Arc::from("k")),
            headers: vec![("bin".to_string(), vec![0, 255, 1, 254])],
            metadata: RecordMeta {
                timestamp_ms: Some(-5),
                format: PayloadFormat::Auto,
            },
        };
        let decoded = Record::decode(&original.encode()).unwrap();
        assert_eq!(decoded.payload.len(), 0);
        assert_eq!(decoded.headers, original.headers);
        assert_eq!(decoded.metadata.timestamp_ms, Some(-5));
    }

    #[test]
    fn decode_rejects_truncated_frame() {
        let bytes = record(b"some payload").encode();
        // Every proper prefix shorter than the whole frame must error, never
        // panic or over-read.
        for cut in 0..bytes.len() {
            assert!(
                Record::decode(&bytes[..cut]).is_err(),
                "truncated-to-{cut} frame must be rejected"
            );
        }
        // The whole frame decodes.
        assert!(Record::decode(&bytes).is_ok());
    }

    #[test]
    fn decode_rejects_bad_version() {
        let mut bytes = record(b"x").encode();
        bytes[0] = 99; // corrupt the version byte
        assert_eq!(Record::decode(&bytes), Err(RecordCodecError::Version(99)));
    }

    #[test]
    fn dedup_key_round_trips_via_header() {
        // No key by default.
        assert!(record(b"x").dedup_key().is_none());
        // Set then read back.
        let r = record(b"x").with_dedup_key("abc123");
        assert_eq!(r.dedup_key(), Some(b"abc123".as_slice()));
        // Setting again replaces, never duplicates the header.
        let r2 = r.with_dedup_key("def456");
        assert_eq!(r2.dedup_key(), Some(b"def456".as_slice()));
        assert_eq!(
            r2.headers
                .iter()
                .filter(|(k, _)| k == DEDUP_KEY_HEADER)
                .count(),
            1,
            "dedup key header must be unique"
        );
        // Pre-existing unrelated headers are preserved.
        assert!(r2.headers.iter().any(|(k, _)| k == "h"));
    }

    #[test]
    fn empty_has_no_records_tokens_or_dlq() {
        let b = WorkBatch::<TestToken>::empty();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        assert_eq!(b.record_count(), 0);
        assert_eq!(
            b.commit_tokens,
            [] as [crate::transport::work_batch::tests::TestToken; 0]
        );
        assert!(b.dlq_entries.is_empty());
        assert_eq!(b.total_payload_bytes(), 0);
    }

    #[test]
    fn from_records_has_no_tokens() {
        let b = WorkBatch::<TestToken>::from_records(vec![record(b"{}"), record(b"[]")]);
        assert_eq!(b.len(), 2);
        assert!(!b.is_empty());
        assert_eq!(
            b.commit_tokens,
            [] as [crate::transport::work_batch::tests::TestToken; 0]
        );
    }

    #[test]
    fn new_carries_records_and_tokens() {
        let b = WorkBatch::new(vec![record(b"{}")], vec![TestToken(1), TestToken(2)]);
        assert_eq!(b.record_count(), 1);
        assert_eq!(b.commit_tokens.len(), 2);
    }

    #[test]
    fn with_dlq_entries_attaches_entries() {
        let entry = FilteredDlqEntry {
            payload: b"bad".to_vec(),
            key: None,
            reason: "filter".to_string(),
        };
        let b =
            WorkBatch::<TestToken>::from_records(vec![record(b"{}")]).with_dlq_entries(vec![entry]);
        assert_eq!(b.dlq_entries.len(), 1);
        assert_eq!(b.dlq_entries[0].reason, "filter");
    }

    #[test]
    fn total_payload_bytes_sums_payloads() {
        let b = WorkBatch::<TestToken>::from_records(vec![
            record(b"abc"), // 3
            record(b"de"),  // 2
            record(b"f"),   // 1
        ]);
        assert_eq!(b.total_payload_bytes(), 6);
    }

    #[test]
    fn map_records_preserves_tokens_and_dlq() {
        let entry = FilteredDlqEntry {
            payload: b"bad".to_vec(),
            key: None,
            reason: "filter".to_string(),
        };
        let b =
            WorkBatch::new(vec![record(b"{}")], vec![TestToken(7)]).with_dlq_entries(vec![entry]);

        let b = b.map_records(|recs| {
            // identity-ish transform that mutates payload but keeps count
            recs.into_iter()
                .map(|mut r| {
                    r.payload = Bytes::from_static(b"changed");
                    r
                })
                .collect()
        });

        assert_eq!(b.record_count(), 1);
        assert_eq!(b.records[0].payload.as_ref(), b"changed");
        // tokens + dlq preserved across the transform
        assert_eq!(b.commit_tokens, vec![TestToken(7)]);
        assert_eq!(b.dlq_entries.len(), 1);
    }

    #[test]
    fn map_records_fan_out_keeps_tokens_intact() {
        // One input record + one source ack...
        let b = WorkBatch::new(vec![record(b"{}")], vec![TestToken(99)]);
        assert_eq!(b.record_count(), 1);
        assert_eq!(b.commit_tokens.len(), 1);

        // ...fans out to three records, but the source ack must stay singular.
        let b = b.map_records(|recs| {
            let mut out = Vec::new();
            for r in recs {
                out.push(r.clone());
                out.push(r.clone());
                out.push(r);
            }
            out
        });

        assert_eq!(b.record_count(), 3);
        assert_eq!(b.commit_tokens, vec![TestToken(99)]);
    }

    #[test]
    fn from_message_yields_single_record_batch() {
        let msg = Message::new(
            Some(Arc::from("topic")),
            b"{\"a\":1}".to_vec(),
            TestToken(5),
            Some(11),
        );
        let b: WorkBatch<TestToken> = msg.into();

        assert_eq!(b.record_count(), 1);
        assert_eq!(b.commit_tokens, vec![TestToken(5)]);
        assert_eq!(b.records[0].payload.as_ref(), b"{\"a\":1}");
        assert_eq!(b.records[0].key.as_deref(), Some("topic"));
        assert_eq!(b.records[0].metadata.timestamp_ms, Some(11));
        assert_eq!(b.records[0].metadata.format, PayloadFormat::Json);
        assert!(b.dlq_entries.is_empty());
    }

    #[test]
    fn from_recv_batch_collapses_and_preserves_order() {
        let entry = FilteredDlqEntry {
            payload: b"bad".to_vec(),
            key: None,
            reason: "drop-it".to_string(),
        };
        let recv = RecvBatch {
            messages: vec![
                Message::new(Some(Arc::from("a")), b"{}".to_vec(), TestToken(1), None),
                Message::new(Some(Arc::from("b")), b"[]".to_vec(), TestToken(2), None),
                Message::new(None, b"{}".to_vec(), TestToken(3), None),
            ],
            dlq_entries: vec![entry],
            filtered_tokens: Vec::new(),
        };

        let b: WorkBatch<TestToken> = recv.into();

        assert_eq!(b.record_count(), 3);
        // commit tokens preserved in order
        assert_eq!(
            b.commit_tokens,
            vec![TestToken(1), TestToken(2), TestToken(3)]
        );
        // dlq entries carried straight across
        assert_eq!(b.dlq_entries.len(), 1);
        assert_eq!(b.dlq_entries[0].reason, "drop-it");
        // record payloads + keys preserved
        assert_eq!(b.records[0].key.as_deref(), Some("a"));
        assert_eq!(b.records[2].key, None);
    }

    #[test]
    fn from_recv_batch_appends_filtered_tokens_to_commit_tokens() {
        // Acks of filtered-out records (no passing Record) must still reach
        // commit_tokens so the block commit advances the source past them.
        let recv = RecvBatch {
            messages: vec![Message::new(
                Some(Arc::from("a")),
                b"{}".to_vec(),
                TestToken(1),
                None,
            )],
            dlq_entries: Vec::new(),
            filtered_tokens: vec![TestToken(7), TestToken(8)],
        };
        let b: WorkBatch<TestToken> = recv.into();
        // One passing record, but THREE acks to commit (1 passing + 2 filtered).
        assert_eq!(b.record_count(), 1);
        assert_eq!(
            b.commit_tokens,
            vec![TestToken(1), TestToken(7), TestToken(8)]
        );
    }

    #[test]
    fn from_recv_batch_all_filtered_is_records_empty_tokens_present() {
        // Every record filtered: no Records, but the acks survive so the source
        // is not stalled behind an all-filtered stretch.
        let recv: RecvBatch<TestToken> = RecvBatch {
            messages: Vec::new(),
            dlq_entries: Vec::new(),
            filtered_tokens: vec![TestToken(5), TestToken(6)],
        };
        let b: WorkBatch<TestToken> = recv.into();
        assert_eq!(b.record_count(), 0);
        assert_eq!(b.commit_tokens, vec![TestToken(5), TestToken(6)]);
    }

    #[test]
    fn from_message_moves_payload_without_copying() {
        // Build a heap-backed payload and remember its allocation pointer.
        // The vec is moved via `impl Into<Bytes>` -- no copy at Message::new.
        let payload = b"zero-copy-please".to_vec();
        let payload_ptr = payload.as_ptr();

        let msg = Message::new(Some(Arc::from("topic")), payload, TestToken(1), None);
        // Message.payload is Bytes built from the vec. Same allocation.
        assert_eq!(msg.payload.as_ptr(), payload_ptr);

        let wb: WorkBatch<TestToken> = msg.into();

        // From<Message> moves the Bytes handle -- the record payload must point
        // at the SAME allocation. A regression to copy_from_slice would fail here.
        assert_eq!(wb.records[0].payload.as_ptr(), payload_ptr);
    }

    /// Capability test: `Message::new` with an already-allocated
    /// `Bytes` payload travels through `WorkBatch` with ZERO copies.
    ///
    /// This proves the headline win of the migration: an upstream `Bytes` (e.g.
    /// the axum body) reaches the `Record` without ever calling
    /// `copy_from_slice`. The allocation pointer must be identical at every hop.
    #[test]
    fn bytes_payload_travels_zero_copy_through_workbatch() {
        // Simulate a caller that already holds a `Bytes` (e.g. the axum ingest
        // handler after the body.to_vec() removal).
        let raw = b"bytes-zero-copy-payload-test".to_vec();
        let src: Bytes = raw.into();
        let src_ptr = src.as_ptr();

        // Pass the Bytes directly into Message::new (accepted via impl Into<Bytes>).
        let msg = Message::new(Some(Arc::from("k")), src, TestToken(42), Some(99));
        // The Bytes handle is MOVED -- same backing allocation.
        assert_eq!(msg.payload.as_ptr(), src_ptr, "copy at Message::new");

        // Convert to WorkBatch: From<Message> moves msg.payload (already Bytes).
        let wb: WorkBatch<TestToken> = msg.into();
        assert_eq!(
            wb.records[0].payload.as_ptr(),
            src_ptr,
            "copy at From<Message> for WorkBatch"
        );

        // Clone the record payload (fan-out): Bytes::clone is a refcount bump.
        let cloned = wb.records[0].payload.clone();
        assert_eq!(
            cloned.as_ptr(),
            src_ptr,
            "clone allocated a new buffer instead of bumping refcount"
        );
    }

    #[test]
    fn from_recv_batch_moves_payloads_without_copying() {
        let p0 = b"first-buffer".to_vec();
        let p1 = b"second-buffer".to_vec();
        let p0_ptr = p0.as_ptr();
        let p1_ptr = p1.as_ptr();

        let recv = RecvBatch {
            messages: vec![
                Message::new(Some(Arc::from("a")), p0, TestToken(1), None),
                Message::new(Some(Arc::from("b")), p1, TestToken(2), None),
            ],
            dlq_entries: Vec::new(),
            filtered_tokens: Vec::new(),
        };

        let wb: WorkBatch<TestToken> = recv.into();

        // Each payload Vec is MOVED into Bytes -- same allocations, no copy.
        assert_eq!(wb.records[0].payload.as_ptr(), p0_ptr);
        assert_eq!(wb.records[1].payload.as_ptr(), p1_ptr);
    }

    #[test]
    fn payload_is_bytes_and_clone_is_zero_copy() {
        // Bytes::clone shares the same underlying allocation (refcount bump).
        let r = record(b"shared-buffer");
        let p1 = r.payload.clone();
        let r2 = r.clone();
        // same backing buffer pointer => zero-copy clone
        assert_eq!(p1.as_ptr(), r2.payload.as_ptr());
        assert_eq!(r2.payload.as_ref(), b"shared-buffer");
    }

    // ---- Zero-copy framing helpers -----------------------------------------

    /// Assert that `slice` is a zero-copy view INTO `blob` (a refcounted slice,
    /// not a fresh allocation): its byte range must fall within `blob`'s range.
    fn assert_within(slice: &Bytes, blob: &Bytes) {
        let blob_start = blob.as_ptr() as usize;
        let blob_end = blob_start + blob.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        assert!(
            slice_start >= blob_start && slice_end <= blob_end,
            "slice [{slice_start:#x}, {slice_end:#x}) is not within blob \
             [{blob_start:#x}, {blob_end:#x}) -- it is a copy, not a view"
        );
    }

    // --- single() -----------------------------------------------------------

    #[test]
    fn single_holds_whole_blob_as_one_record() {
        let blob = Bytes::from_static(b"{\"a\":1}");
        let b = WorkBatch::<TestToken>::single(blob.clone());
        assert_eq!(b.record_count(), 1);
        assert_eq!(
            b.commit_tokens,
            [] as [crate::transport::work_batch::tests::TestToken; 0]
        );
        assert!(b.dlq_entries.is_empty());
        assert_eq!(b.records[0].payload, blob);
        assert_eq!(b.records[0].key, None);
        assert_eq!(
            b.records[0].headers,
            [] as [(std::string::String, std::vec::Vec<u8>); 0]
        );
        // whole blob is the same allocation (zero-copy)
        assert_eq!(b.records[0].payload.as_ptr(), blob.as_ptr());
    }

    #[test]
    fn single_detects_format_json_object() {
        let b = WorkBatch::<TestToken>::single(Bytes::from_static(b"{\"a\":1}"));
        assert_eq!(b.records[0].metadata.format, PayloadFormat::Json);
    }

    #[test]
    fn single_detects_format_msgpack() {
        // fixmap with one entry (0x81) -> MsgPack
        let b = WorkBatch::<TestToken>::single(Bytes::from_static(&[0x81, 0xa1, 0x61]));
        assert_eq!(b.records[0].metadata.format, PayloadFormat::MsgPack);
    }

    // --- from_ndjson() and from_json_array() -------------------------------
    // How each blob splits is tested with the framings in crate::framing.

    #[test]
    fn framed_blobs_become_json_records_sharing_the_blob() {
        let ndjson = Bytes::from_static(b"{\"a\":1}\r\n\n  {\"b\":2}  ");
        let array = Bytes::from_static(b"[{\"a\":1}, {\"b\":2}]");
        let batches = [
            (ndjson.clone(), WorkBatch::<TestToken>::from_ndjson(ndjson)),
            (array.clone(), WorkBatch::from_json_array(array).unwrap()),
        ];
        for (blob, batch) in batches {
            let payloads: Vec<&[u8]> = batch.records.iter().map(|r| r.payload.as_ref()).collect();
            assert_eq!(payloads, [&b"{\"a\":1}"[..], b"{\"b\":2}"]);
            for record in &batch.records {
                assert_eq!(record.metadata.format, PayloadFormat::Json);
                assert_within(&record.payload, &blob);
            }
            assert!(batch.commit_tokens.is_empty() && batch.dlq_entries.is_empty());
        }
        assert_eq!(
            WorkBatch::<TestToken>::from_ndjson(Bytes::new()).record_count(),
            0
        );
    }

    #[test]
    fn a_blob_that_is_not_an_array_is_refused() {
        let refused = WorkBatch::<TestToken>::from_json_array(Bytes::from_static(b"nope"));
        assert!(matches!(refused, Err(FramingError::NotAnArray(_))));
    }
}
