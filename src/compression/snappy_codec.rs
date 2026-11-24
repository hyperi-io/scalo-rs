// Project:   scalo
// File:      src/compression/snappy_codec.rs
// Purpose:   Raw snappy blocks with a kept encoder and decoder
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io;

use snap::raw;

use super::engine::{invalid, over_limit};

/// The encoder's hash table and the decoder, kept between blocks.
pub(super) struct Block {
    encoder: raw::Encoder,
    decoder: raw::Decoder,
}

impl Block {
    pub(super) fn new() -> Self {
        Self {
            encoder: raw::Encoder::new(),
            decoder: raw::Decoder::new(),
        }
    }

    pub(super) fn compress_into(&mut self, src: &[u8], dst: &mut Vec<u8>) -> io::Result<()> {
        let bound = raw::max_compress_len(src.len());
        if bound == 0 && !src.is_empty() {
            return Err(io::Error::other("input too large for a snappy block"));
        }
        dst.try_reserve(bound).map_err(io::Error::other)?;
        dst.resize(bound, 0);
        let written = self.encoder.compress(src, dst).map_err(io::Error::other)?;
        dst.truncate(written);
        Ok(())
    }

    pub(super) fn decompress_into(
        &mut self,
        src: &[u8],
        dst: &mut Vec<u8>,
        limit: usize,
    ) -> io::Result<()> {
        let size = raw::decompress_len(src).map_err(invalid)?;
        if size > limit {
            return Err(over_limit(limit));
        }
        // Snappy expands at most about 22 to 1, so a larger claim is not a
        // snappy block, and is refused before `dst` is zero-filled to it.
        if size > src.len().saturating_mul(32).saturating_add(16) {
            return Err(invalid(format!(
                "snappy block of {} bytes cannot hold the {size} its header says",
                src.len()
            )));
        }
        dst.try_reserve(size).map_err(io::Error::other)?;
        dst.resize(size, 0);
        let written = self.decoder.decompress(src, dst).map_err(invalid)?;
        dst.truncate(written);
        Ok(())
    }
}
