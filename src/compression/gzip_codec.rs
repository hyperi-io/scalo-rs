// Project:   scalo
// File:      src/compression/gzip_codec.rs
// Purpose:   gzip blocks over a reused deflate context, and the gzip stream encoder
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io::{self, Write};

use flate2::{FlushCompress, Status};

use super::engine::read_limited;

/// The RFC 1952 member header: deflate, no flags, no time, unknown OS.
const HEADER: [u8; 10] = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];

/// A raw deflate context, reset for each block, so a block allocates no
/// compressor state. flate2's `GzEncoder` cannot be reset.
pub(super) struct Block {
    deflate: flate2::Compress,
}

impl Block {
    pub(super) fn new(level: i32) -> Self {
        Self {
            deflate: flate2::Compress::new(flate_level(level), false),
        }
    }

    /// One gzip member: header, deflate, then CRC-32 and length mod 2^32.
    pub(super) fn compress_into(&mut self, src: &[u8], dst: &mut Vec<u8>) -> io::Result<()> {
        self.deflate.reset();
        // Deflate grows incompressible input by about 5 bytes per 16 KiB block.
        let estimate = HEADER.len() + src.len() + src.len() / 1024 + 64;
        dst.try_reserve(estimate).map_err(io::Error::other)?;
        dst.extend_from_slice(&HEADER);
        loop {
            let consumed = usize::try_from(self.deflate.total_in()).unwrap_or(src.len());
            if dst.len() == dst.capacity() {
                dst.try_reserve(1024).map_err(io::Error::other)?;
            }
            let status = self
                .deflate
                .compress_vec(&src[consumed..], dst, FlushCompress::Finish)
                .map_err(io::Error::other)?;
            if status == Status::StreamEnd {
                break;
            }
        }
        let mut crc = flate2::Crc::new();
        crc.update(src);
        dst.extend_from_slice(&crc.sum().to_le_bytes());
        dst.extend_from_slice(&crc.amount().to_le_bytes());
        Ok(())
    }
}

/// Every member of `src`, into `dst`, refusing more than `limit` bytes.
pub(super) fn decompress_into(src: &[u8], dst: &mut Vec<u8>, limit: usize) -> io::Result<()> {
    read_limited(flate2::read::MultiGzDecoder::new(src), dst, limit)
}

/// A gzip stream writer at `level`.
pub(super) fn encoder<W: Write>(inner: W, level: i32) -> flate2::write::GzEncoder<W> {
    flate2::write::GzEncoder::new(inner, flate_level(level))
}

/// flate2's level, from one [`Compression::validate`](super::Compression::validate) passed.
fn flate_level(level: i32) -> flate2::Compression {
    flate2::Compression::new(u32::try_from(level).unwrap_or(6))
}
