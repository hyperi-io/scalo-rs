// Project:   scalo
// File:      src/geoip_download/mmdb.rs
// Purpose:   Structural check of a downloaded MMDB database
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Structural check of a downloaded MMDB database.
//!
//! An MMDB file ends in a metadata map that opens with a fixed marker within
//! its last 128 KiB. Reading that map, and checking the search tree it
//! describes fits in front of it, refuses an error page, a truncated transfer
//! or a file of another format before it replaces a working database. It does
//! not prove the provider published those bytes.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::GeoIpDownloadError;

/// Bytes that open the metadata section.
const MARKER: &[u8] = b"\xAB\xCD\xEFMaxMind.com";

/// The metadata section starts within this many bytes of the end of the file.
const METADATA_SPAN: u64 = 128 * 1024;

/// Zero bytes between the search tree and the data section.
const DATA_SECTION_SEPARATOR: u128 = 16;

/// Nesting a metadata value may reach, so a hostile file cannot exhaust the stack.
const MAX_DEPTH: usize = 8;

/// The metadata fields the check reads.
#[derive(Debug, Default)]
struct Metadata {
    major_version: Option<u64>,
    node_count: Option<u64>,
    record_size: Option<u64>,
    ip_version: Option<u64>,
    has_database_type: bool,
}

/// A decoded metadata value, reduced to what the check needs.
enum Field<'a> {
    Uint(u64),
    Text(&'a str),
    Other,
}

/// Refuse `path` unless it reads as an MMDB database.
///
/// # Errors
///
/// [`GeoIpDownloadError::NotMmdb`] naming the first fault found, or
/// [`GeoIpDownloadError::Io`] when the file cannot be read.
pub(super) fn verify(path: &Path) -> Result<(), GeoIpDownloadError> {
    let mut file = fs::File::open(path)?;
    let len = file.metadata()?.len();
    let tail_len = len.min(METADATA_SPAN + MARKER.len() as u64);
    file.seek(SeekFrom::Start(len - tail_len))?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail)?;
    check(&tail, len - tail_len)
}

/// [`verify`] over the file's last bytes, `tail`, which start at `tail_at`.
fn check(tail: &[u8], tail_at: u64) -> Result<(), GeoIpDownloadError> {
    let not = |reason| Err(GeoIpDownloadError::NotMmdb { reason });
    let Some(marker_at) = tail.windows(MARKER.len()).rposition(|w| w == MARKER) else {
        return not("no metadata marker in the last 128 KiB");
    };
    let Some(metadata) = read_metadata(&tail[marker_at + MARKER.len()..]) else {
        return not("the metadata map does not decode");
    };
    if metadata.major_version != Some(2) {
        return not("binary_format_major_version is not 2");
    }
    let Some(record_size) = metadata.record_size.filter(|&r| matches!(r, 24 | 28 | 32)) else {
        return not("record_size is not 24, 28 or 32");
    };
    if !matches!(metadata.ip_version, Some(4 | 6)) {
        return not("ip_version is not 4 or 6");
    }
    if !metadata.has_database_type {
        return not("database_type is missing");
    }
    let Some(node_count) = metadata.node_count.filter(|&n| n > 0) else {
        return not("node_count is missing");
    };
    // Each node holds two records, so the tree is node_count * record_size / 4 bytes.
    let tree_end = u128::from(node_count) * u128::from(record_size) / 4 + DATA_SECTION_SEPARATOR;
    if tree_end > u128::from(tail_at) + marker_at as u128 {
        return not("the search tree runs past the metadata");
    }
    Ok(())
}

/// The fields of the metadata map at the start of `bytes`.
fn read_metadata(bytes: &[u8]) -> Option<Metadata> {
    let (kind, entries, mut at) = header(bytes, 0)?;
    if kind != 7 {
        return None;
    }
    let mut metadata = Metadata::default();
    for _ in 0..entries {
        let (Field::Text(key), after_key) = value(bytes, at, 1)? else {
            return None;
        };
        let (field, after_value) = value(bytes, after_key, 1)?;
        let uint = match field {
            Field::Uint(n) => Some(n),
            _ => None,
        };
        match key {
            "binary_format_major_version" => metadata.major_version = uint,
            "node_count" => metadata.node_count = uint,
            "record_size" => metadata.record_size = uint,
            "ip_version" => metadata.ip_version = uint,
            "database_type" => {
                metadata.has_database_type = matches!(field, Field::Text(t) if !t.is_empty());
            }
            _ => {}
        }
        at = after_value;
    }
    Some(metadata)
}

/// The type, size and payload offset of the non-pointer value at `at`.
fn header(bytes: &[u8], at: usize) -> Option<(u8, usize, usize)> {
    let control = *bytes.get(at)?;
    let mut next = at + 1;
    let mut kind = control >> 5;
    if kind == 0 {
        kind = bytes.get(next)?.checked_add(7)?;
        next += 1;
    }
    let low = usize::from(control & 0x1f);
    let (base, extra) = match low {
        0..=28 => (low, 0),
        29 => (29, 1),
        30 => (285, 2),
        _ => (65_821, 3),
    };
    let size = base + be_uint(bytes.get(next..next + extra)?);
    Some((kind, size, next + extra))
}

/// Decode the value at `at`, returning it and the offset after it.
fn value(bytes: &[u8], at: usize, depth: usize) -> Option<(Field<'_>, usize)> {
    if depth > MAX_DEPTH {
        return None;
    }
    let control = *bytes.get(at)?;
    if control >> 5 == 1 {
        // A pointer's target is an offset into the metadata section.
        let width = usize::from((control >> 3) & 0x3) + 1;
        let raw = bytes.get(at + 1..at + 1 + width)?;
        let high = if width == 4 {
            0
        } else {
            usize::from(control & 0x7)
        };
        let bias = [0, 2_048, 526_336, 0][width - 1];
        let target = (high << (8 * width)) + be_uint(raw) + bias;
        let (field, _) = value(bytes, target, depth + 1)?;
        return Some((field, at + 1 + width));
    }
    let (kind, size, payload) = header(bytes, at)?;
    let end = payload.checked_add(size)?;
    let field = match kind {
        // UTF-8 string.
        2 => Field::Text(std::str::from_utf8(bytes.get(payload..end)?).ok()?),
        // uint16, uint32, uint64.
        5 | 6 | 9 if size <= 8 => {
            Field::Uint(u64::try_from(be_uint(bytes.get(payload..end)?)).ok()?)
        }
        // Map: `size` key and value pairs.
        7 => {
            let mut next = payload;
            for _ in 0..size.checked_mul(2)? {
                next = value(bytes, next, depth + 1)?.1;
            }
            return Some((Field::Other, next));
        }
        // Array: `size` values.
        11 => {
            let mut next = payload;
            for _ in 0..size {
                next = value(bytes, next, depth + 1)?.1;
            }
            return Some((Field::Other, next));
        }
        // Boolean: the size is the value and there is no payload.
        14 => return Some((Field::Other, payload)),
        // double, bytes, int32, uint128, float.
        3 | 4 | 8 | 10 | 15 => {
            bytes.get(payload..end)?;
            Field::Other
        }
        _ => return None,
    };
    Some((field, end))
}

/// `bytes` read as a big-endian unsigned integer, saturating past `usize`.
fn be_uint(bytes: &[u8]) -> usize {
    bytes.iter().fold(0usize, |n, &b| {
        n.saturating_mul(256).saturating_add(usize::from(b))
    })
}

/// A small well-formed database, for tests that need one on disk.
#[cfg(test)]
pub(super) fn sample() -> Vec<u8> {
    tests::database(100, &tests::metadata(2, 100))
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    /// A metadata value in the MMDB data format, for building test files.
    enum Encode<'a> {
        Text(&'a str),
        Uint(u8, u64),
        Map(Vec<(&'a str, Encode<'a>)>),
        Array(Vec<Encode<'a>>),
    }

    fn control(kind: u8, size: usize, out: &mut Vec<u8>) {
        let size = u8::try_from(size).expect("test values stay under 29 bytes");
        assert!(size < 29);
        if kind > 7 {
            out.push(size);
            out.push(kind - 7);
        } else {
            out.push((kind << 5) | size);
        }
    }

    fn encode(value: &Encode<'_>, out: &mut Vec<u8>) {
        match value {
            Encode::Text(text) => {
                control(2, text.len(), out);
                out.extend_from_slice(text.as_bytes());
            }
            Encode::Uint(kind, n) => {
                let bytes: Vec<u8> = n
                    .to_be_bytes()
                    .into_iter()
                    .skip_while(|&b| b == 0)
                    .collect();
                control(*kind, bytes.len(), out);
                out.extend_from_slice(&bytes);
            }
            Encode::Map(entries) => {
                control(7, entries.len(), out);
                for (key, value) in entries {
                    encode(&Encode::Text(key), out);
                    encode(value, out);
                }
            }
            Encode::Array(items) => {
                control(11, items.len(), out);
                for item in items {
                    encode(item, out);
                }
            }
        }
    }

    /// Metadata a real MMDB writer emits, with `major` and `node_count` as given.
    pub(super) fn metadata(major: u64, node_count: u64) -> Vec<u8> {
        let map = Encode::Map(vec![
            ("binary_format_major_version", Encode::Uint(5, major)),
            ("binary_format_minor_version", Encode::Uint(5, 0)),
            ("build_epoch", Encode::Uint(9, 1_700_000_000)),
            ("database_type", Encode::Text("DBIP-City-Lite")),
            (
                "description",
                Encode::Map(vec![("en", Encode::Text("test database"))]),
            ),
            ("ip_version", Encode::Uint(5, 6)),
            ("languages", Encode::Array(vec![Encode::Text("en")])),
            ("node_count", Encode::Uint(6, node_count)),
            ("record_size", Encode::Uint(5, 24)),
        ]);
        let mut out = MARKER.to_vec();
        encode(&map, &mut out);
        out
    }

    /// A file with a search tree of `node_count` 24-bit nodes and the given
    /// metadata after it.
    pub(super) fn database(node_count: u64, metadata: &[u8]) -> Vec<u8> {
        let tree = usize::try_from(node_count * 6).expect("a small tree");
        let mut file = vec![0u8; tree + 16];
        file.extend_from_slice(b"data section");
        file.extend_from_slice(metadata);
        file
    }

    #[test]
    fn a_well_formed_database_passes() {
        let file = database(100, &metadata(2, 100));
        assert!(check(&file, 0).is_ok(), "{:?}", check(&file, 0));
    }

    #[test]
    fn a_file_that_is_not_a_database_is_refused() {
        for (bytes, reason) in [
            (&b"<html><body>quota exceeded</body></html>"[..], "marker"),
            (&b""[..], "marker"),
            (MARKER, "decode"),
        ] {
            match check(bytes, 0) {
                Err(GeoIpDownloadError::NotMmdb { reason: why }) => {
                    assert!(why.contains(reason), "{why}");
                }
                other => panic!("{bytes:?} was not refused: {other:?}"),
            }
        }
    }

    /// A transfer cut short loses the metadata at the end of the file.
    #[test]
    fn a_truncated_database_is_refused() {
        let file = database(100, &metadata(2, 100));
        let cut = &file[..file.len() - 20];
        assert!(matches!(
            check(cut, 0),
            Err(GeoIpDownloadError::NotMmdb { .. })
        ));
    }

    #[test]
    fn metadata_outside_the_format_is_refused() {
        let wrong_major = database(100, &metadata(3, 100));
        let tree_too_big = database(100, &metadata(2, 1_000_000));
        for (file, reason) in [
            (wrong_major, "binary_format_major_version"),
            (tree_too_big, "search tree"),
        ] {
            match check(&file, 0) {
                Err(GeoIpDownloadError::NotMmdb { reason: why }) => {
                    assert!(why.contains(reason), "{why}");
                }
                other => panic!("not refused for {reason}: {other:?}"),
            }
        }
    }

    /// The tail's offset in the file counts towards the room the tree has.
    #[test]
    fn the_tree_check_counts_the_bytes_before_the_tail() {
        let meta = metadata(2, 1_000);
        assert!(check(&meta, 0).is_err(), "no room for 1,000 nodes");
        assert!(
            check(&meta, 6_016).is_ok(),
            "6,000 tree bytes and the separator fit"
        );
    }

    /// A hostile map that nests past the depth bound is refused, not followed.
    #[test]
    fn deep_nesting_is_refused() {
        let mut nested = Encode::Text("x");
        for _ in 0..MAX_DEPTH + 2 {
            nested = Encode::Array(vec![nested]);
        }
        let mut bytes = MARKER.to_vec();
        encode(&Encode::Map(vec![("languages", nested)]), &mut bytes);
        assert!(matches!(
            check(&bytes, 0),
            Err(GeoIpDownloadError::NotMmdb { reason }) if reason.contains("decode")
        ));
    }

    /// A pointer in the metadata resolves against the metadata section.
    #[test]
    fn a_pointer_resolves_within_the_metadata() {
        // The map's database_type points at the string stored after the map.
        let mut body = Vec::new();
        encode(
            &Encode::Map(vec![
                ("binary_format_major_version", Encode::Uint(5, 2)),
                ("ip_version", Encode::Uint(5, 4)),
                ("node_count", Encode::Uint(6, 1)),
                ("record_size", Encode::Uint(5, 32)),
            ]),
            &mut body,
        );
        // Rewrite the entry count to five and append the pointer entry.
        body[0] = (7 << 5) | 5;
        encode(&Encode::Text("database_type"), &mut body);
        let target = body.len() + 2;
        body.push((1 << 5) | u8::try_from(target >> 8).expect("small offset"));
        body.push(u8::try_from(target & 0xff).expect("one byte"));
        encode(&Encode::Text("GeoLite2-ASN"), &mut body);
        let mut bytes = MARKER.to_vec();
        bytes.extend_from_slice(&body);
        let file = database(1, &bytes);
        assert!(check(&file, 0).is_ok(), "{:?}", check(&file, 0));
    }

    #[test]
    fn verify_reads_the_tail_of_a_file_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.mmdb");
        // More than the metadata span ahead of the metadata, so only the tail is read.
        let mut file = vec![0u8; 200 * 1024];
        file.extend_from_slice(&metadata(2, 100));
        fs::write(&path, &file).unwrap();
        assert!(verify(&path).is_ok(), "{:?}", verify(&path));

        fs::write(&path, b"not a database").unwrap();
        assert!(matches!(
            verify(&path),
            Err(GeoIpDownloadError::NotMmdb { .. })
        ));
    }

    #[test]
    fn a_missing_file_is_an_io_error() {
        let err = verify(Path::new("/nonexistent/db.mmdb")).unwrap_err();
        assert!(
            matches!(&err, GeoIpDownloadError::Io(e) if e.kind() == io::ErrorKind::NotFound),
            "{err:?}"
        );
    }
}
