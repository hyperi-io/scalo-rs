# Framing

A framing says where one encoded record ends and the next begins in a unit that holds many: an HTTP body, a file, a message, a TCP stream. It sits after the record codec. Records reach it as bytes, and it never looks inside one.

Everything in scalo that puts many records in one unit goes through `scalo::framing`: the file and pipe transports, the file output, the DLQ's file and HTTP backends, and `WorkBatch::from_ndjson` and `from_json_array`.

| `type` | One unit | Content type | Appends | Streams |
| --- | --- | --- | --- | --- |
| `ndjson` | each record then `\n`, unless it ends in one | `application/x-ndjson` | yes | yes |
| `lf` | each record then `\n` | `text/plain` | yes | yes |
| `octet_counting` | `<length> <record>` per record, RFC 6587 | `text/plain` | yes | yes |
| `json_concat` | records back to back, each an object or array | `application/json` | yes | no |
| `json_array` | `[r1,r2]` | `application/json` | no | no |
| `json_wrapped` | `{"<key>":[r1,r2]}` | `application/json` | no | no |
| `single` | one record as it is | `application/octet-stream` | no | no |

Appends means two units back to back decode as the records of both, so a file can grow by appending. Streams means `StreamDecoder` hands records out before the input ends.

`encode` returns how many records it framed. `single` frames one per call, so a sender loops until every record is out. The JSON framings find where each value ends and never parse one: `[1,nul]` frames as two records, and the parser downstream refuses the second.

---

## Decoding

`DecodeLimits` has no default. Set it from config for input this process did not write, or use `DecodeLimits::UNBOUNDED` for input it did.

| Field | Bounds |
| --- | --- |
| `max_decoded_bytes` | the unit `decode` takes, after content decoding. A gzip body meets the transport's own limit as it arrives and this one once inflated |
| `max_record_bytes` | one record. A longer one goes to `Decoded::oversize`, or arrives from a stream as `Frame::Oversize` with its first `max_record_bytes`, and is never held whole |
| `max_frame_bytes` | an octet count. A frame over `max_record_bytes` and within this is skipped whole. A larger count is not trusted: the bytes from it to the next `\n` are read as an LF line, and counting resumes after it |

`octet_counting` is for syslog. A frame is counted only when it is a count starting 1 to 9, one space, then the `<` of the PRI. Anything else is an LF line, so one stream can carry both, and a line such as `2026 Oct 11 ...` never swallows what follows it. A space at the end of the input so far waits for the next byte, and at the end of input it is a line.

A framing that streams decodes a byte stream as it arrives:

```rust
use std::sync::Arc;
use scalo::framing::{DecodeLimits, Frame, OctetCounting, StreamDecoder};

let limits = DecodeLimits { max_decoded_bytes: usize::MAX, max_record_bytes: 64 * 1024, max_frame_bytes: 1024 * 1024 };
let mut decoder = StreamDecoder::new(Arc::new(OctetCounting), limits);
while let Some(frame) = decoder.read(&mut reader).await? {
    match frame {
        Frame::Record(record) => handle(record),
        Frame::Oversize { head, len } => dead_letter(head, len),
    }
}
```

`read` takes any `AsyncBufRead` and is cancel-safe. At the end of input it answers `None` and keeps a last line with no `\n`: `finish()` then lets it out, as the pipe transport does at EOF. The file transport never calls `finish`, so a line still being written waits for its newline. `taken()` is the offset just past the last frame.

---

## Config and your own framing

```yaml
framing:
  type: json_wrapped
  key: events
```

`FramingConfig { type, key }` builds a framing by name. `type` defaults to `ndjson` and `key`, read only by `json_wrapped`, to `records`.

An app's own framing implements `Framing` and registers under a name before config is built from, the way a [custom sender](custom.md) does:

```rust
use std::sync::Arc;
use scalo::framing::{DecodeLimits, Decoded, Framing, FramingError, register_framing};

/// Each record ended by the ASCII record separator, 0x1e.
#[derive(Debug)]
struct RecordSeparated;

impl Framing for RecordSeparated {
    fn content_type(&self) -> &'static str { "application/octet-stream" }
    fn appends(&self) -> bool { true }
    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        for record in records {
            out.extend_from_slice(record);
            out.push(0x1e);
        }
        records.len()
    }
    fn decode<'a>(&self, unit: &'a [u8], limits: &DecodeLimits) -> Result<Decoded<'a>, FramingError> {
        limits.check(unit)?;
        let records = unit.split(|&b| b == 0x1e).filter(|r| !r.is_empty());
        Ok(Decoded::from_records(records, limits))
    }
}

register_framing("record_separated", Arc::new(RecordSeparated))?;
```

`type: record_separated` now builds it. A framing that decodes as a stream answers `streams() -> true` and implements `split`, which finds the frame at the front of a buffer, and the default `decode` then splits whole units with it.
