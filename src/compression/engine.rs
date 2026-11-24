// Project:   scalo
// File:      src/compression/engine.rs
// Purpose:   Block compressor and stream encoder/decoder over every codec
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The engines: [`Compressor`] for blocks, [`Encoder`] and [`Decoder`] for streams.

use std::io::{self, Read, Write};
use std::sync::{Mutex, PoisonError};

use super::{Codec, Compression, CompressionError, gzip_codec, lz4_codec, zstd_codec};

#[cfg(feature = "compression-snappy")]
use super::snappy_codec;

/// Compresses and decompresses blocks, reusing its codec context between
/// calls, so a loop of blocks allocates nothing beyond what `dst` grows by.
///
/// ```
/// use scalo::compression::{Codec, Compression, Compressor};
///
/// let mut compressor = Compressor::new(Compression::new(Codec::Zstd))?;
/// let (mut packed, mut unpacked) = (Vec::new(), Vec::new());
/// compressor.compress_into(b"aaaaaaaaaaaaaaaaaaaaaaaa", &mut packed)?;
/// compressor.decompress_into(&packed, &mut unpacked, 1024)?;
/// assert_eq!(unpacked, b"aaaaaaaaaaaaaaaaaaaaaaaa");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Compressor {
    compression: Compression,
    state: State,
}

enum State {
    None,
    Zstd(zstd_codec::Block),
    Lz4(lz4_codec::Block),
    Gzip(gzip_codec::Block),
    #[cfg(feature = "compression-snappy")]
    Snappy(Box<snappy_codec::Block>),
}

impl Compressor {
    /// A compressor for `compression`.
    ///
    /// # Errors
    ///
    /// A level the codec does not take, or a codec this build lacks.
    pub fn new(compression: Compression) -> Result<Self, CompressionError> {
        let compression = compression.validate()?;
        let level = compression.effective_level().unwrap_or_default();
        let state = match compression.codec {
            Codec::None => State::None,
            Codec::Zstd => State::Zstd(zstd_codec::Block::new(level)),
            Codec::Lz4 => State::Lz4(lz4_codec::Block::default()),
            Codec::Gzip => State::Gzip(gzip_codec::Block::new(level)),
            #[cfg(feature = "compression-snappy")]
            Codec::Snappy => State::Snappy(Box::new(snappy_codec::Block::new())),
            #[cfg(not(feature = "compression-snappy"))]
            Codec::Snappy => return Err(not_compiled(Codec::Snappy)),
        };
        Ok(Self { compression, state })
    }

    /// The setting it runs.
    #[must_use]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Replaces `dst` with `src` compressed.
    ///
    /// # Errors
    ///
    /// The codec's own error, or a block too large for its format.
    pub fn compress_into(&mut self, src: &[u8], dst: &mut Vec<u8>) -> io::Result<()> {
        dst.clear();
        match &mut self.state {
            State::None => {
                dst.try_reserve(src.len()).map_err(io::Error::other)?;
                dst.extend_from_slice(src);
                Ok(())
            }
            State::Zstd(block) => block.compress_into(src, dst),
            State::Lz4(block) => block.compress_into(src, dst),
            State::Gzip(block) => block.compress_into(src, dst),
            #[cfg(feature = "compression-snappy")]
            State::Snappy(block) => block.compress_into(src, dst),
        }
    }

    /// Replaces `dst` with `src` decompressed, refusing more than `limit`
    /// bytes of output.
    ///
    /// # Errors
    ///
    /// `InvalidData` for bytes that are not this codec's block or that
    /// decompress past `limit`.
    pub fn decompress_into(
        &mut self,
        src: &[u8],
        dst: &mut Vec<u8>,
        limit: usize,
    ) -> io::Result<()> {
        dst.clear();
        match &mut self.state {
            State::None => {
                if src.len() > limit {
                    return Err(over_limit(limit));
                }
                dst.try_reserve(src.len()).map_err(io::Error::other)?;
                dst.extend_from_slice(src);
                Ok(())
            }
            State::Zstd(block) => block.decompress_into(src, dst, limit),
            State::Lz4(_) => lz4_codec::decompress_into(src, dst, limit),
            State::Gzip(_) => gzip_codec::decompress_into(src, dst, limit),
            #[cfg(feature = "compression-snappy")]
            State::Snappy(block) => block.decompress_into(src, dst, limit),
        }
    }
}

impl std::fmt::Debug for Compressor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compressor")
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}

/// Compressors for callers that compress at the same time. Each call takes an
/// idle one, or makes one when every one is busy, and gives it back, so the
/// pool holds at most as many as were ever in use at once.
///
/// ```
/// use scalo::compression::{Codec, Compression, CompressorPool};
///
/// let pool = CompressorPool::new(Compression::new(Codec::Gzip))?;
/// let mut packed = Vec::new();
/// pool.compress_into(b"aaaaaaaaaaaaaaaaaaaaaaaa", &mut packed)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct CompressorPool {
    compression: Compression,
    idle: Mutex<Vec<Compressor>>,
}

impl CompressorPool {
    /// A pool for `compression`, holding one idle compressor.
    ///
    /// # Errors
    ///
    /// As [`Compressor::new`].
    pub fn new(compression: Compression) -> Result<Self, CompressionError> {
        let first = Compressor::new(compression)?;
        Ok(Self {
            compression: first.compression(),
            idle: Mutex::new(vec![first]),
        })
    }

    /// The setting it runs.
    #[must_use]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// [`Compressor::compress_into`] on an idle compressor.
    ///
    /// # Errors
    ///
    /// As [`Compressor::compress_into`].
    pub fn compress_into(&self, src: &[u8], dst: &mut Vec<u8>) -> io::Result<()> {
        self.with(|c| c.compress_into(src, dst))
    }

    /// [`Compressor::decompress_into`] on an idle compressor.
    ///
    /// # Errors
    ///
    /// As [`Compressor::decompress_into`].
    pub fn decompress_into(&self, src: &[u8], dst: &mut Vec<u8>, limit: usize) -> io::Result<()> {
        self.with(|c| c.decompress_into(src, dst, limit))
    }

    /// Idle compressors, for tests.
    #[cfg(test)]
    fn idle(&self) -> usize {
        self.idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn with(&self, run: impl FnOnce(&mut Compressor) -> io::Result<()>) -> io::Result<()> {
        let taken = self
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let mut compressor = match taken {
            Some(compressor) => compressor,
            None => Compressor::new(self.compression).map_err(io::Error::other)?,
        };
        let result = run(&mut compressor);
        self.idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(compressor);
        result
    }
}

impl std::fmt::Debug for CompressorPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressorPool")
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}

impl Compression {
    /// `src` compressed in one call, with a context made for it.
    ///
    /// A loop of blocks holds a [`Compressor`] instead.
    ///
    /// # Errors
    ///
    /// As [`Compressor::new`] and [`Compressor::compress_into`].
    pub fn compress(self, src: &[u8]) -> io::Result<Vec<u8>> {
        let mut dst = Vec::new();
        Compressor::new(self)
            .map_err(io::Error::other)?
            .compress_into(src, &mut dst)?;
        Ok(dst)
    }

    /// `src` decompressed in one call, refusing more than `limit` bytes.
    ///
    /// # Errors
    ///
    /// As [`Compressor::new`] and [`Compressor::decompress_into`].
    pub fn decompress(self, src: &[u8], limit: usize) -> io::Result<Vec<u8>> {
        let mut dst = Vec::new();
        Compressor::new(self)
            .map_err(io::Error::other)?
            .decompress_into(src, &mut dst, limit)?;
        Ok(dst)
    }

    /// A stream that writes `inner` compressed, in the codec's framed format.
    /// Call [`Encoder::finish`] to write the trailer.
    ///
    /// # Errors
    ///
    /// As [`Compressor::new`], or the codec failing to start.
    pub fn encoder<W: Write>(self, inner: W) -> io::Result<Encoder<W>> {
        let compression = self.validate().map_err(io::Error::other)?;
        let level = compression.effective_level().unwrap_or_default();
        Ok(Encoder(match compression.codec {
            Codec::None => EncoderKind::None(inner),
            Codec::Zstd => EncoderKind::Zstd(::zstd::stream::write::Encoder::new(inner, level)?),
            Codec::Lz4 => EncoderKind::Lz4(lz4_flex::frame::FrameEncoder::new(inner)),
            Codec::Gzip => EncoderKind::Gzip(gzip_codec::encoder(inner, level)),
            #[cfg(feature = "compression-snappy")]
            Codec::Snappy => EncoderKind::Snappy(Box::new(snap::write::FrameEncoder::new(inner))),
            #[cfg(not(feature = "compression-snappy"))]
            Codec::Snappy => return Err(io::Error::other(not_compiled(Codec::Snappy))),
        }))
    }

    /// A stream that reads `inner` decompressed, from the codec's framed
    /// format. Wrap it in [`Read::take`] to bound what untrusted input inflates to.
    ///
    /// # Errors
    ///
    /// A codec this build lacks, or the codec failing to start.
    pub fn decoder<R: Read>(self, inner: R) -> io::Result<Decoder<R>> {
        Ok(Decoder(match self.codec {
            Codec::None => DecoderKind::None(inner),
            Codec::Zstd => DecoderKind::Zstd(::zstd::stream::read::Decoder::new(inner)?),
            Codec::Lz4 => DecoderKind::Lz4(lz4_flex::frame::FrameDecoder::new(inner)),
            Codec::Gzip => DecoderKind::Gzip(flate2::read::MultiGzDecoder::new(inner)),
            #[cfg(feature = "compression-snappy")]
            Codec::Snappy => DecoderKind::Snappy(snap::read::FrameDecoder::new(inner)),
            #[cfg(not(feature = "compression-snappy"))]
            Codec::Snappy => return Err(io::Error::other(not_compiled(Codec::Snappy))),
        }))
    }
}

/// Writes compressed into an inner writer. [`finish`](Self::finish) writes
/// the trailer and returns the writer: dropping it unfinished leaves a
/// truncated stream.
pub struct Encoder<W: Write>(EncoderKind<W>);

enum EncoderKind<W: Write> {
    None(W),
    Zstd(::zstd::stream::write::Encoder<'static, W>),
    Lz4(lz4_flex::frame::FrameEncoder<W>),
    Gzip(flate2::write::GzEncoder<W>),
    #[cfg(feature = "compression-snappy")]
    Snappy(Box<snap::write::FrameEncoder<W>>),
}

impl<W: Write> Encoder<W> {
    /// Writes the trailer and returns the inner writer.
    ///
    /// # Errors
    ///
    /// The inner writer's error.
    pub fn finish(self) -> io::Result<W> {
        match self.0 {
            EncoderKind::None(w) => Ok(w),
            EncoderKind::Zstd(e) => e.finish(),
            EncoderKind::Lz4(e) => e.finish().map_err(io::Error::other),
            EncoderKind::Gzip(e) => e.finish(),
            #[cfg(feature = "compression-snappy")]
            EncoderKind::Snappy(e) => e
                .into_inner()
                .map_err(|e| io::Error::new(e.error().kind(), e.error().to_string())),
        }
    }
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.0 {
            EncoderKind::None(w) => w.write(buf),
            EncoderKind::Zstd(e) => e.write(buf),
            EncoderKind::Lz4(e) => e.write(buf),
            EncoderKind::Gzip(e) => e.write(buf),
            #[cfg(feature = "compression-snappy")]
            EncoderKind::Snappy(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.0 {
            EncoderKind::None(w) => w.flush(),
            EncoderKind::Zstd(e) => e.flush(),
            EncoderKind::Lz4(e) => e.flush(),
            EncoderKind::Gzip(e) => e.flush(),
            #[cfg(feature = "compression-snappy")]
            EncoderKind::Snappy(e) => e.flush(),
        }
    }
}

/// Reads decompressed from an inner reader.
pub struct Decoder<R: Read>(DecoderKind<R>);

enum DecoderKind<R: Read> {
    None(R),
    Zstd(::zstd::stream::read::Decoder<'static, io::BufReader<R>>),
    Lz4(lz4_flex::frame::FrameDecoder<R>),
    Gzip(flate2::read::MultiGzDecoder<R>),
    #[cfg(feature = "compression-snappy")]
    Snappy(snap::read::FrameDecoder<R>),
}

impl<R: Read> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.0 {
            DecoderKind::None(r) => r.read(buf),
            DecoderKind::Zstd(d) => d.read(buf),
            DecoderKind::Lz4(d) => d.read(buf),
            DecoderKind::Gzip(d) => d.read(buf),
            #[cfg(feature = "compression-snappy")]
            DecoderKind::Snappy(d) => d.read(buf),
        }
    }
}

/// The error for output past a decompression limit.
pub(super) fn over_limit(limit: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("decompressed past the {limit}-byte limit"),
    )
}

/// A codec error as `InvalidData`.
pub(super) fn invalid(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Reads `reader` into `dst`, refusing more than `limit` bytes.
pub(super) fn read_limited(reader: impl Read, dst: &mut Vec<u8>, limit: usize) -> io::Result<()> {
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    reader.take(cap).read_to_end(dst)?;
    if dst.len() > limit {
        return Err(over_limit(limit));
    }
    Ok(())
}

#[cfg(not(feature = "compression-snappy"))]
fn not_compiled(codec: Codec) -> CompressionError {
    CompressionError::NotCompiled {
        surface: "compression",
        codec,
        feature: codec.feature().unwrap_or("compression"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compiled() -> Vec<Codec> {
        Codec::ALL.into_iter().filter(|c| c.is_compiled()).collect()
    }

    fn sample() -> Vec<u8> {
        br#"{"host":"web-01","level":"info","msg":"GET /index.html 200"}"#.repeat(64)
    }

    /// Bytes no codec shrinks (xorshift output), so gzip outgrows its first
    /// reservation.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    /// Block and stream round trips, for every compiled codec and level edge,
    /// with one reused compressor across sizes.
    #[test]
    fn every_codec_round_trips_block_and_stream() {
        let data = sample();
        for codec in compiled() {
            let levels: Vec<Option<i32>> = match codec.levels() {
                Some(range) => vec![None, Some(*range.start()), Some(*range.end())],
                None => vec![None],
            };
            for level in levels {
                let compression = Compression { codec, level };
                let mut compressor = Compressor::new(compression).unwrap();
                let (mut packed, mut unpacked) = (Vec::new(), Vec::new());
                let noise = incompressible(70_000);
                for input in [&b""[..], b"x", &data[..], &noise[..], &data[..10]] {
                    compressor.compress_into(input, &mut packed).unwrap();
                    compressor
                        .decompress_into(&packed, &mut unpacked, input.len())
                        .unwrap();
                    assert_eq!(unpacked, input, "{compression} block");
                }
                if codec != Codec::None {
                    compressor.compress_into(&data, &mut packed).unwrap();
                    assert!(packed.len() < data.len() / 4, "{compression} shrinks");
                }

                let mut encoder = compression.encoder(Vec::new()).unwrap();
                encoder.write_all(&data).unwrap();
                let framed = encoder.finish().unwrap();
                let mut read = Vec::new();
                compression
                    .decoder(&framed[..])
                    .unwrap()
                    .read_to_end(&mut read)
                    .unwrap();
                assert_eq!(read, data, "{compression} stream");
            }
        }
    }

    /// The one-shot forms, and zstd and gzip blocks reading as their streams.
    #[test]
    fn one_shot_and_block_stream_agree() {
        let data = sample();
        for codec in [Codec::Zstd, Codec::Gzip] {
            let packed = Compression::new(codec).compress(&data).unwrap();
            let mut read = Vec::new();
            Compression::new(codec)
                .decoder(&packed[..])
                .unwrap()
                .read_to_end(&mut read)
                .unwrap();
            assert_eq!(read, data, "{codec}");
            assert_eq!(
                Compression::new(codec)
                    .decompress(&packed, data.len())
                    .unwrap(),
                data,
                "{codec}"
            );
        }
        let mut gz = Vec::new();
        flate2::read::GzDecoder::new(&Compression::new(Codec::Gzip).compress(&data).unwrap()[..])
            .read_to_end(&mut gz)
            .unwrap();
        assert_eq!(gz, data, "flate2 reads our gzip block");
    }

    /// Output past the limit, and bytes of the wrong codec, are refused.
    #[test]
    fn limit_and_garbage_are_refused() {
        let data = sample();
        for codec in compiled() {
            let mut compressor = Compressor::new(Compression::new(codec)).unwrap();
            let (mut packed, mut unpacked) = (Vec::new(), Vec::new());
            compressor.compress_into(&data, &mut packed).unwrap();
            let err = compressor
                .decompress_into(&packed, &mut unpacked, data.len() - 1)
                .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{codec}: {err}");
            if codec != Codec::None {
                assert!(
                    compressor
                        .decompress_into(b"\x00not a block at all", &mut unpacked, 1 << 20)
                        .is_err(),
                    "{codec}"
                );
            }
        }
    }

    /// Overlapping callers each get a compressor, and the pool keeps them all
    /// for the next calls rather than making one per call.
    #[test]
    fn a_pool_reuses_compressors_across_overlapping_calls() {
        let pool = CompressorPool::new(Compression::new(Codec::Gzip)).unwrap();
        let data = sample();
        let mut packed = Vec::new();
        pool.with(|outer| {
            pool.compress_into(&data, &mut packed).unwrap();
            outer.compress_into(&data, &mut Vec::new())
        })
        .unwrap();
        assert_eq!(pool.idle(), 2, "the overlapping call's compressor is kept");
        for _ in 0..5 {
            pool.compress_into(&data, &mut packed).unwrap();
        }
        assert_eq!(pool.idle(), 2, "serial calls make none");
        let mut unpacked = Vec::new();
        pool.decompress_into(&packed, &mut unpacked, data.len())
            .unwrap();
        assert_eq!(unpacked, data);
        assert!(CompressorPool::new(Compression::new(Codec::Gzip).with_level(0)).is_err());
    }

    #[test]
    fn an_invalid_setting_builds_nothing() {
        assert!(Compressor::new(Compression::new(Codec::Zstd).with_level(40)).is_err());
        assert!(
            Compression::new(Codec::Lz4)
                .with_level(2)
                .encoder(Vec::new())
                .is_err()
        );
        let snappy = Compressor::new(Compression::new(Codec::Snappy));
        assert_eq!(snappy.is_ok(), cfg!(feature = "compression-snappy"));
    }
}
