// Project:   scalo
// File:      src/compression/lz4_codec.rs
// Purpose:   LZ4 blocks with a 4-byte length in front and a reused hash table
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io;

use lz4_flex::block;

use super::engine::{invalid, over_limit};

/// Bytes of the little-endian length `lz4_flex::compress_prepend_size` writes.
const LEN: usize = 4;

/// The hash table, kept between blocks.
#[derive(Default)]
pub(super) struct Block {
    table: block::CompressTable,
}

impl Block {
    /// The format of `lz4_flex::compress_prepend_size`.
    pub(super) fn compress_into(&mut self, src: &[u8], dst: &mut Vec<u8>) -> io::Result<()> {
        let len = u32::try_from(src.len())
            .map_err(|_| io::Error::other("an lz4 block holds at most 4 GiB"))?;
        let bound = LEN + block::get_maximum_output_size(src.len());
        dst.try_reserve(bound).map_err(io::Error::other)?;
        dst.extend_from_slice(&len.to_le_bytes());
        dst.resize(bound, 0);
        let written = block::compress_into_with_table(src, &mut dst[LEN..], &mut self.table)
            .map_err(io::Error::other)?;
        dst.truncate(LEN + written);
        Ok(())
    }
}

/// A block [`Block::compress_into`] wrote. Decompression keeps no state.
pub(super) fn decompress_into(src: &[u8], dst: &mut Vec<u8>, limit: usize) -> io::Result<()> {
    let (size, body) = block::uncompressed_size(src).map_err(invalid)?;
    if size > limit {
        return Err(over_limit(limit));
    }
    // LZ4 expands at most 255 to 1, so a larger claim is not an LZ4 block,
    // and is refused before `dst` is zero-filled to it.
    if size > body.len().saturating_mul(255).saturating_add(16) {
        return Err(invalid(format!(
            "lz4 block of {} bytes cannot hold the {size} its header says",
            body.len()
        )));
    }
    dst.try_reserve(size).map_err(io::Error::other)?;
    dst.resize(size, 0);
    let written = block::decompress_into(body, dst).map_err(invalid)?;
    if written != size {
        return Err(invalid(format!(
            "lz4 block holds {written} bytes, its header says {size}"
        )));
    }
    Ok(())
}
