// Project:   scalo
// File:      src/compression/zstd_codec.rs
// Purpose:   Zstandard blocks with a reused context
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io;

use ::zstd::bulk;

use super::engine::{invalid, over_limit, read_limited};

/// A level and the contexts, each made on first use.
pub(super) struct Block {
    level: i32,
    compressor: Option<bulk::Compressor<'static>>,
    decompressor: Option<bulk::Decompressor<'static>>,
}

impl Block {
    pub(super) fn new(level: i32) -> Self {
        Self {
            level,
            compressor: None,
            decompressor: None,
        }
    }

    pub(super) fn compress_into(&mut self, src: &[u8], dst: &mut Vec<u8>) -> io::Result<()> {
        let compressor = match &mut self.compressor {
            Some(compressor) => compressor,
            empty => empty.insert(bulk::Compressor::new(self.level)?),
        };
        dst.try_reserve(::zstd::zstd_safe::compress_bound(src.len()))
            .map_err(io::Error::other)?;
        compressor.compress_to_buffer(src, dst).map(drop)
    }

    /// A frame that records its size is read in one call into a buffer of
    /// that size. One that does not is streamed.
    pub(super) fn decompress_into(
        &mut self,
        src: &[u8],
        dst: &mut Vec<u8>,
        limit: usize,
    ) -> io::Result<()> {
        let Some(size) = bulk::Decompressor::upper_bound(src) else {
            let decoder = ::zstd::stream::read::Decoder::with_buffer(src).map_err(invalid)?;
            return read_limited(decoder, dst, limit);
        };
        if size > limit {
            return Err(over_limit(limit));
        }
        let decompressor = match &mut self.decompressor {
            Some(decompressor) => decompressor,
            empty => empty.insert(bulk::Decompressor::new()?),
        };
        dst.try_reserve(size).map_err(io::Error::other)?;
        decompressor
            .decompress_to_buffer(src, dst)
            .map(drop)
            .map_err(invalid)
    }
}
