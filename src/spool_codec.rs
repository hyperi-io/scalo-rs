// Project:   scalo
// File:      src/spool_codec.rs
// Purpose:   Shared on-disk spool helpers (CRC framing + corrupt-cache quarantine)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Shared on-disk spool helpers, used by BOTH the standalone [`Spool`] primitive
//! and the `TieredSink` cold path so the integrity + recovery behaviour is
//! defined once.
//!
//! - **CRC framing** ([`frame`] / [`unframe`]): an optional CRC32C header in
//!   front of each record's on-disk bytes. The underlying queue (yaque) only
//!   parity-checks the LENGTH header, so a torn write / bit-rot in the payload
//!   would otherwise be returned silently; the checksum turns that into a
//!   detectable [`CorruptionError`].
//! - **Quarantine** ([`quarantine_dir`]): rename a corrupt cache directory aside
//!   with a timestamp (forensics preserved, never deleted) so a poisoned cache
//!   can be replaced with a fresh one rather than wedging the service.
//!
//! [`Spool`]: crate::spool::Spool

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// What to do when a corrupt cache is detected (queue won't open, or a CRC check
/// fails on read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CorruptionPolicy {
    /// Rename the corrupt cache directory aside to
    /// `<path>.corrupt-YYYYMMDD-HHMMSS` (forensics preserved, never deleted) and
    /// start a fresh empty queue. The default -- a spill cache is opt-in,
    /// transient overflow, so continuing beats crashing on a poisoned cache.
    #[default]
    Quarantine,
    /// Surface the corruption error and do not auto-recover, for callers that
    /// want to handle it explicitly.
    Fail,
}

/// A record's on-disk bytes failed their CRC32C check (torn write / bit-rot), or
/// the framing was too short to hold the header.
#[derive(Debug, Clone)]
pub struct CorruptionError(pub String);

impl std::fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Frame `body` with a CRC32C header when `crc` is enabled.
///
/// Layout when `crc`: `[crc32c(body): u32 LE][body]`. The checksum covers
/// exactly the bytes that land on disk, so it detects a torn write or bit-rot
/// the queue's length-only header would miss. When `crc` is off, `body` is
/// returned unchanged (the on-disk format is byte-identical to pre-CRC).
#[must_use]
pub fn frame(crc: bool, body: Vec<u8>) -> Vec<u8> {
    if !crc {
        return body;
    }
    let sum = crc32c::crc32c(&body);
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&sum.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Verify and strip the CRC32C header (inverse of [`frame`]).
///
/// # Errors
/// [`CorruptionError`] if the record is shorter than the 4-byte header or the
/// checksum does not match -- corruption surfaces as an error, never as
/// silently-wrong bytes handed downstream.
pub fn unframe(crc: bool, raw: Vec<u8>) -> Result<Vec<u8>, CorruptionError> {
    if !crc {
        return Ok(raw);
    }
    if raw.len() < 4 {
        return Err(CorruptionError(
            "record shorter than the 4-byte CRC header".to_string(),
        ));
    }
    let (header, body) = raw.split_at(4);
    let expected = u32::from_le_bytes(header.try_into().unwrap_or([0; 4]));
    let actual = crc32c::crc32c(body);
    if expected != actual {
        return Err(CorruptionError(format!(
            "CRC32C mismatch (expected {expected:08x}, computed {actual:08x}) -- torn write or bit-rot"
        )));
    }
    Ok(body.to_vec())
}

/// Rename a corrupt cache directory aside to `<name>.corrupt-YYYYMMDD-HHMMSS`,
/// preserving it for forensics. Returns the new path, or `None` if there was
/// nothing to move. The caller then opens a fresh queue at the original path.
///
/// # Errors
/// Propagates the rename's [`std::io::Error`].
pub fn quarantine_dir(path: &Path) -> std::io::Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("spool");
    let dest = path.with_file_name(format!("{name}.corrupt-{stamp}"));
    std::fs::rename(path, &dest)?;
    Ok(Some(dest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_unframe_round_trips() {
        let body = b"the payload bytes".to_vec();
        let framed = frame(true, body.clone());
        assert_eq!(framed.len(), body.len() + 4, "4-byte CRC header prepended");
        assert_eq!(unframe(true, framed).unwrap(), body);
    }

    #[test]
    fn frame_off_is_identity() {
        let body = b"x".to_vec();
        assert_eq!(frame(false, body.clone()), body);
        assert_eq!(unframe(false, body.clone()).unwrap(), body);
    }

    #[test]
    fn unframe_detects_corruption() {
        let mut framed = frame(true, b"original".to_vec());
        let last = framed.len() - 1;
        framed[last] ^= 0xFF; // flip a payload byte
        assert!(unframe(true, framed).is_err());
        // Too short to hold the header.
        assert!(unframe(true, vec![1, 2]).is_err());
    }
}
