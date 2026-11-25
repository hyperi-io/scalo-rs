// Copied from yaque 0.6.6, src/header.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! The 4-byte header in front of every entry: a 26-bit length and six parity
//! bits over it, so a damaged header reads as an error, never as a length.
//!
//! The parity masks come from this Python:
//! ```python
//! pow2 = {1, 2, 4, 8, 16, 32}
//!
//! for p in ( 2 ** k for k in range(6) ):
//!     for i in list(range(1, 27))[::-1]: # arabic numerals...
//!         if i not in pow2:
//!             print(1 if i & p else 0, end="")
//!     print()
//! ```
//! See [this section in Wikipedia](https://en.wikipedia.org/wiki/Hamming_code#General_algorithm).

use std::io;

/// The parity masks, in the order their bits sit above the length.
const MASKS: [u32; 6] = [
    0b11_1111_1111_1111_1111_1111_1111,
    0b10_1010_1010_1010_1101_0101_1011,
    0b11_0011_0011_0011_0110_0110_1101,
    0b11_1100_0011_1100_0111_1000_1110,
    0b11_1111_1100_0000_0111_1111_0000,
    0b11_1111_1111_1111_1000_0000_0000,
];

/// The longest entry a header can describe: 64 MiB less one byte.
pub(crate) const MAX_LEN: u32 = 0x03FF_FFFF;

/// The marker that ends a segment, in place of a header.
pub(crate) const EOF: [u8; 4] = [255; 4];

/// An entry's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    len: u32,
}

impl Header {
    /// The header of an entry of `len` bytes, or `None` past [`MAX_LEN`].
    pub(crate) fn for_len(len: usize) -> Option<Self> {
        u32::try_from(len)
            .ok()
            .filter(|len| *len <= MAX_LEN)
            .map(|len| Self { len })
    }

    /// The length of the entry's payload.
    pub(crate) fn len(self) -> u32 {
        self.len
    }

    /// The header as it is written.
    pub(crate) fn encode(self) -> [u8; 4] {
        (self.len | parity(self.len) << 26).to_be_bytes()
    }

    /// Reads a header as it was written.
    ///
    /// # Errors
    ///
    /// `InvalidData` when a parity bit does not match the length.
    pub(crate) fn decode(bytes: [u8; 4]) -> io::Result<Self> {
        let encoded = u32::from_be_bytes(bytes);
        let len = encoded & MAX_LEN;
        if encoded >> 26 != parity(len) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("entry header {bytes:02x?} fails its parity check"),
            ));
        }
        Ok(Self { len })
    }
}

/// The six parity bits of `len`, lowest first.
fn parity(len: u32) -> u32 {
    MASKS
        .iter()
        .rev()
        .fold(0, |bits, mask| bits << 1 | ((len & mask).count_ones() & 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every length round-trips, and every single-bit flip of a header is caught.
    #[test]
    fn a_header_round_trips_and_any_flipped_bit_is_an_error() {
        let mut rng = crate::yaque::test_rng();
        let lengths = [0, 1, 4, 255, 4096, MAX_LEN - 1, MAX_LEN]
            .into_iter()
            .chain((0..10_000).map(|_| u32::try_from(rng() & u64::from(MAX_LEN)).unwrap()));
        for len in lengths {
            let header = Header::for_len(usize::try_from(len).unwrap()).unwrap();
            let bytes = header.encode();
            assert_eq!(Header::decode(bytes).unwrap(), header, "{len}");
            let word = u32::from_be_bytes(bytes);
            for bit in 0..32 {
                let flipped = (word ^ 1 << bit).to_be_bytes();
                assert!(
                    Header::decode(flipped).is_err(),
                    "{len} with bit {bit} flipped"
                );
            }
        }
    }

    #[test]
    fn lengths_past_the_header_and_foreign_bytes_are_refused() {
        let max = usize::try_from(MAX_LEN).unwrap();
        for (len, fits) in [(max, true), (max + 1, false), (usize::MAX, false)] {
            assert_eq!(Header::for_len(len).is_some(), fits, "{len}");
        }
        for bytes in [*b"Asbt", EOF] {
            let err = Header::decode(bytes).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{bytes:?}");
        }
    }
}
