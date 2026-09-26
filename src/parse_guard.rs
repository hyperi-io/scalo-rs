// Project:   scalo
// File:      src/parse_guard.rs
// Purpose:   Stack-safe nesting-depth guard for the JSON/MsgPack parse paths
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Parse-path depth guard against stack-exhaustion DoS.
//!
//! JSON (`sonic_rs`) and MsgPack (`rmpv`) recurse per nesting level; a
//! deeply-nested payload can blow the worker stack. We reject above
//! [`MAX_PARSE_DEPTH`] -- JSON via the iterative [`json_depth_within`] (run
//! before the recursive parser), MsgPack via `read_value_with_max_depth`.
//!
//! A consumer that parses untrusted JSON itself -- a lazy `sonic_rs` read, a
//! parse into `sonic_rs::Value` -- runs [`json_depth_within`] first, with the
//! same bound, rather than carrying its own copy of the guard.

use wide::u8x32;

/// Max accepted JSON/MsgPack nesting depth. Data-plane payloads are shallow; 64 is
/// well above real data and under a stack hazard.
pub const MAX_PARSE_DEPTH: usize = 64;

/// Bytes classified per step, one bit each in a `u64` mask.
const BLOCK: usize = 64;

/// Every odd bit position, which tells an odd run of backslashes from an even one.
const ODD_BITS: u64 = 0xAAAA_AAAA_AAAA_AAAA;

/// `true` if the JSON payload nests no deeper than `max`.
///
/// Counts `{` and `[` outside strings, honouring `\` escapes inside strings; a
/// close with nothing open saturates at zero. Not a validator -- a pre-filter
/// ahead of `sonic_rs`: malformed JSON, unbalanced brackets, unterminated
/// strings and non-UTF-8 bytes all get an answer, and the parser that runs next
/// rejects what is malformed.
///
/// Classifies 64 bytes per step into bitmasks and walks only the brackets
/// outside strings. Iterative, so stack-safe at any depth, and it returns at the
/// first block that passes `max`, so a pathological payload costs `O(max)`, not
/// `O(len)`. A backslash outside a string escapes nothing, which the masks
/// cannot express, so the rest of such a payload is read a byte at a time.
///
/// # Examples
///
/// ```
/// use scalo::parse_guard::{MAX_PARSE_DEPTH, json_depth_within};
///
/// let record = br#"{"user":{"name":"a \"[{\" b"},"tags":["x","y"]}"#;
/// assert!(json_depth_within(record, MAX_PARSE_DEPTH));
///
/// // 100,000 unclosed arrays: refused after the first two 64-byte blocks.
/// let hostile = vec![b'['; 100_000];
/// assert!(!json_depth_within(&hostile, MAX_PARSE_DEPTH));
/// ```
#[must_use]
pub fn json_depth_within(payload: &[u8], max: usize) -> bool {
    let (blocks, tail) = payload.as_chunks::<BLOCK>();
    let mut scan = Scan::default();
    for (i, block) in blocks.iter().enumerate() {
        match scan.block(Classes::of(block), max) {
            Step::Within => {}
            Step::TooDeep => return false,
            Step::Irregular => return scan.bytes(&payload[i * BLOCK..], max),
        }
    }
    if tail.is_empty() {
        return true;
    }
    // Re-read the last 64 bytes and shift out the ones already scanned.
    let classes = if let Some(last) = payload.last_chunk::<BLOCK>() {
        Classes::of(last).skip(BLOCK - tail.len())
    } else {
        // A zero byte is in no class, so the padding changes nothing.
        let mut padded = [0; BLOCK];
        padded[..tail.len()].copy_from_slice(tail);
        Classes::of(&padded)
    };
    match scan.block(classes, max) {
        Step::Within => true,
        Step::TooDeep => false,
        Step::Irregular => scan.bytes(tail, max),
    }
}

/// One bit per byte of a block for each byte class the scan reads.
#[derive(Clone, Copy)]
struct Classes {
    quotes: u64,
    backslashes: u64,
    opens: u64,
    closes: u64,
}

impl Classes {
    #[inline]
    fn of(block: &[u8; BLOCK]) -> Self {
        let quote = u8x32::splat(b'"');
        let backslash = u8x32::splat(b'\\');
        // `[` and `{` differ only in bit 5, as do `]` and `}`.
        let fold = u8x32::splat(0x20);
        let open = u8x32::splat(b'{');
        let close = u8x32::splat(b'}');
        let mut classes = Self {
            quotes: 0,
            backslashes: 0,
            opens: 0,
            closes: 0,
        };
        let (halves, _) = block.as_chunks::<32>();
        for (half, shift) in halves.iter().zip([0_u32, 32]) {
            let bytes = u8x32::new(*half);
            let folded = bytes | fold;
            classes.quotes |= u64::from(bytes.simd_eq(quote).to_bitmask()) << shift;
            classes.backslashes |= u64::from(bytes.simd_eq(backslash).to_bitmask()) << shift;
            classes.opens |= u64::from(folded.simd_eq(open).to_bitmask()) << shift;
            classes.closes |= u64::from(folded.simd_eq(close).to_bitmask()) << shift;
        }
        classes
    }

    /// Drops the first `seen` bytes, which an earlier block already scanned.
    #[inline]
    fn skip(self, seen: usize) -> Self {
        Self {
            quotes: self.quotes >> seen,
            backslashes: self.backslashes >> seen,
            opens: self.opens >> seen,
            closes: self.closes >> seen,
        }
    }
}

/// Scan state carried from one block to the next.
#[derive(Default)]
struct Scan {
    depth: usize,
    /// All ones while a string is open across a block boundary.
    in_string: u64,
    /// 1 when a backslash ending the last block escapes this block's first byte.
    escaped: u64,
}

/// What one block did to the scan.
enum Step {
    Within,
    TooDeep,
    /// A backslash outside a string: the byte scan takes over from this block.
    Irregular,
}

impl Scan {
    #[inline]
    fn block(&mut self, classes: Classes, max: usize) -> Step {
        let Classes {
            quotes,
            backslashes,
            opens,
            closes,
        } = classes;
        // A byte is escaped when an odd run of backslashes precedes it (simdjson's escape scanner).
        let (escaped, next_escaped) = if backslashes == 0 {
            (self.escaped, 0)
        } else {
            let potential = backslashes & !self.escaped;
            let codes = ((potential << 1) | ODD_BITS).wrapping_sub(potential) ^ ODD_BITS;
            (
                codes ^ (backslashes | self.escaped),
                (codes & backslashes) >> 63,
            )
        };
        // Set from each opening quote up to, not including, its closing quote.
        let strings = prefix_xor(quotes & !escaped) ^ self.in_string;
        if backslashes & !strings != 0 {
            return Step::Irregular;
        }
        if !self.walk(opens & !strings, closes & !strings, max) {
            return Step::TooDeep;
        }
        self.in_string = 0_u64.wrapping_sub(strings >> 63);
        self.escaped = next_escaped;
        Step::Within
    }

    /// Applies one block's brackets in order; `false` once the depth passes `max`.
    #[inline]
    fn walk(&mut self, opens: u64, closes: u64, max: usize) -> bool {
        let n_open = opens.count_ones() as usize;
        let n_close = closes.count_ones() as usize;
        // No close can reach zero and no open can pass `max`, so the counts alone are exact.
        if n_close <= self.depth && n_open <= max - self.depth {
            self.depth = self.depth + n_open - n_close;
            return true;
        }
        let mut marks = opens | closes;
        while marks != 0 {
            if (opens >> marks.trailing_zeros()) & 1 == 0 {
                self.depth = self.depth.saturating_sub(1);
            } else {
                self.depth += 1;
                if self.depth > max {
                    return false;
                }
            }
            marks &= marks - 1;
        }
        true
    }

    /// The byte-at-a-time scan, resumed from this state.
    fn bytes(&self, rest: &[u8], max: usize) -> bool {
        let mut depth = self.depth;
        let mut in_string = self.in_string != 0;
        let mut escaped = self.escaped != 0;
        for &b in rest {
            if in_string {
                if escaped {
                    escaped = false;
                } else if b == b'\\' {
                    escaped = true;
                } else if b == b'"' {
                    in_string = false;
                }
                continue;
            }
            match b {
                b'"' => in_string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > max {
                        return false;
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        true
    }
}

/// Bit `i` of the result is the parity of the set bits at or below `i`.
#[inline]
fn prefix_xor(mut bits: u64) -> u64 {
    bits ^= bits << 1;
    bits ^= bits << 2;
    bits ^= bits << 4;
    bits ^= bits << 8;
    bits ^= bits << 16;
    bits ^= bits << 32;
    bits
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn flat_and_shallow_pass() {
        assert!(json_depth_within(br"{}", 64));
        assert!(json_depth_within(br#"{"a":1,"b":[1,2,3]}"#, 64));
        assert!(json_depth_within(br#"{"a":{"b":{"c":1}}}"#, 64));
    }

    #[test]
    fn exactly_at_bound_passes_one_over_fails() {
        // depth 3 with max 3 is OK; depth 4 with max 3 is rejected.
        assert!(json_depth_within(br"[[[1]]]", 3));
        assert!(!json_depth_within(br"[[[[1]]]]", 3));
    }

    #[test]
    fn braces_inside_strings_do_not_count() {
        // The string value contains many braces but real depth is 1.
        assert!(json_depth_within(br#"{"k":"{{{{{{{{[[[[["}"#, 2));
    }

    #[test]
    fn escaped_quote_keeps_string_open() {
        // The escaped quote does not close the string, so the trailing braces
        // stay inside it and do not add depth.
        assert!(json_depth_within(br#"{"k":"a\"{{{{{"}"#, 2));
    }

    #[test]
    fn pathological_depth_is_rejected_cheaply() {
        // 5000 nested arrays -> rejected well before the end (O(max), no panic,
        // no recursion).
        let mut deep = vec![b'['; 5000];
        deep.extend_from_slice(b"1");
        deep.extend(std::iter::repeat_n(b']', 5000));
        assert!(!json_depth_within(&deep, MAX_PARSE_DEPTH));
    }

    fn nested(open: &str, close: &str, depth: usize) -> Vec<u8> {
        let mut payload = open.repeat(depth).into_bytes();
        payload.extend_from_slice(b"1");
        payload.extend_from_slice(close.repeat(depth).as_bytes());
        payload
    }

    /// `n` filler bytes, then `tail`.
    fn after(n: usize, tail: &[u8]) -> Vec<u8> {
        let mut payload = vec![b'x'; n];
        payload.extend_from_slice(tail);
        payload
    }

    #[test]
    fn empty_payload_passes() {
        assert!(json_depth_within(b"", 0));
        assert!(json_depth_within(b"", MAX_PARSE_DEPTH));
    }

    #[test]
    fn object_nesting_at_the_default_bound() {
        assert!(json_depth_within(
            &nested("{\"a\":", "}", MAX_PARSE_DEPTH),
            MAX_PARSE_DEPTH
        ));
        assert!(!json_depth_within(
            &nested("{\"a\":", "}", MAX_PARSE_DEPTH + 1),
            MAX_PARSE_DEPTH
        ));
    }

    #[test]
    fn sibling_containers_do_not_add_up() {
        // Depth is how far down the payload goes, not how many containers it has.
        let wide = format!("[{}]", vec!["[[1]]"; 1000].join(","));
        assert!(json_depth_within(wide.as_bytes(), 3));
    }

    #[test]
    fn an_escaped_backslash_closes_the_string() {
        // `\\` is one literal backslash, so the quote after it ends the string
        // and the brackets that follow are structure.
        assert!(!json_depth_within(br#"["\\"[[[1]]]]"#, 3));
    }

    #[test]
    fn pathological_depth_is_refused() {
        for depth in [5_000, 20_000, 100_000] {
            assert!(!json_depth_within(
                &nested("[", "]", depth),
                MAX_PARSE_DEPTH
            ));
            assert!(!json_depth_within(
                &nested("{\"a\":", "}", depth),
                MAX_PARSE_DEPTH
            ));
        }
    }

    #[test]
    fn a_backslash_outside_a_string_escapes_nothing() {
        // Outside a string the backslash is skipped, so the quote opens a string that hides the brackets.
        let payload = format!("[\\\"{}", "[".repeat(200));
        assert!(json_depth_within(payload.as_bytes(), 1));
        assert!(reference(payload.as_bytes(), 1));
    }

    #[test]
    fn a_close_with_nothing_open_does_not_go_below_zero() {
        // Extra closes saturate at zero, so they cannot bank headroom for later opens.
        let payload = format!("{}{}", "]".repeat(70), "[".repeat(4));
        assert!(!json_depth_within(payload.as_bytes(), 3));
        assert!(!reference(payload.as_bytes(), 3));
    }

    #[test]
    fn a_quote_ending_a_lane_or_block_opens_and_closes_a_string() {
        for edge in [15, 31, 63, 127] {
            // Opening quote is the last byte of the lane or block: the brackets after it are string.
            let opened = after(edge, br#""[[[["#);
            assert!(json_depth_within(&opened, 0), "edge {edge}");
            // Closing quote is the last byte: the brackets after it are structure.
            let mut closed = vec![b'"'];
            closed.extend(std::iter::repeat_n(b'a', edge - 1));
            closed.extend_from_slice(br#""[["#);
            assert!(json_depth_within(&closed, 2), "edge {edge}");
            assert!(!json_depth_within(&closed, 1), "edge {edge}");
            for max in 0..4 {
                agrees(&opened, max);
                agrees(&closed, max);
            }
        }
    }

    #[test]
    fn a_backslash_ending_a_lane_or_block_escapes_the_next_byte() {
        for edge in [15, 31, 63, 127] {
            // The string opens at byte 0 and the backslash is the last byte before the edge.
            let mut one = vec![b'"'];
            one.extend(std::iter::repeat_n(b'a', edge - 1));
            one.extend_from_slice(br#"\"[[["#);
            assert!(json_depth_within(&one, 0), "edge {edge}");
            // Two backslashes end the edge: the quote after them closes the string.
            let mut two = vec![b'"'];
            two.extend(std::iter::repeat_n(b'a', edge - 2));
            two.extend_from_slice(br#"\\"[[["#);
            assert!(!json_depth_within(&two, 2), "edge {edge}");
            assert!(json_depth_within(&two, 3), "edge {edge}");
            for max in 0..4 {
                agrees(&one, max);
                agrees(&two, max);
            }
        }
    }

    #[test]
    fn backslash_runs_across_a_block_edge_keep_their_parity() {
        for run in 1..=9_usize {
            for before_edge in 1..=run {
                // The run starts `before_edge` bytes before byte 64, so it ends on or past the edge.
                let mut payload = vec![b'"'];
                payload.extend(std::iter::repeat_n(b'a', 64 - before_edge - 1));
                payload.extend(std::iter::repeat_n(b'\\', run));
                payload.extend_from_slice(br#""[["#);
                // An odd run escapes the quote, which keeps the brackets inside the string.
                assert_eq!(
                    json_depth_within(&payload, 1),
                    run % 2 == 1,
                    "run {run}, {before_edge} before the edge"
                );
                for max in 0..4 {
                    agrees(&payload, max);
                }
            }
        }
    }

    #[test]
    fn an_unterminated_string_hides_everything_after_it() {
        assert!(json_depth_within(br#"{"a":"[[[[[[[["#, 1));
        let long = format!(r#"{{"a":"{}"#, "[{".repeat(200));
        assert!(json_depth_within(long.as_bytes(), 1));
        let trailing_escape = format!(r#"{{"a":"{}\"#, "x".repeat(126));
        assert!(json_depth_within(trailing_escape.as_bytes(), 1));
        for payload in [long.as_bytes(), trailing_escape.as_bytes()] {
            for max in 0..4 {
                agrees(payload, max);
            }
        }
    }

    #[test]
    fn only_open_brackets_add_depth_for_every_byte_value() {
        for b in 0_u8..=u8::MAX {
            // A run of one byte value, long enough to fill two blocks and a tail.
            let payload = vec![b; 150];
            assert_eq!(
                json_depth_within(&payload, 0),
                b != b'[' && b != b'{',
                "byte {b:#04x}"
            );
            for max in 0..4 {
                agrees(&payload, max);
            }
        }
    }

    #[test]
    fn bytes_past_ascii_are_filler_inside_and_outside_strings() {
        for b in 0x80_u8..=u8::MAX {
            let mut outside = vec![b'['];
            outside.extend(std::iter::repeat_n(b, 100));
            outside.push(b']');
            assert!(json_depth_within(&outside, 1), "byte {b:#04x}");
            assert!(!json_depth_within(&outside, 0), "byte {b:#04x}");
            let mut inside = b"[\"".to_vec();
            inside.extend(std::iter::repeat_n(b, 100));
            inside.extend_from_slice(b"\"[]]");
            assert!(json_depth_within(&inside, 2), "byte {b:#04x}");
            assert!(!json_depth_within(&inside, 1), "byte {b:#04x}");
        }
    }

    /// The byte-at-a-time guard whose verdicts the block scan must reproduce.
    fn reference(payload: &[u8], max: usize) -> bool {
        let mut depth: usize = 0;
        let mut in_string = false;
        let mut escaped = false;
        for &b in payload {
            if in_string {
                if escaped {
                    escaped = false;
                } else if b == b'\\' {
                    escaped = true;
                } else if b == b'"' {
                    in_string = false;
                }
                continue;
            }
            match b {
                b'"' => in_string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > max {
                        return false;
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        true
    }

    fn agrees(payload: &[u8], max: usize) {
        assert_eq!(
            json_depth_within(payload, max),
            reference(payload, max),
            "max {max}, payload {:?}",
            String::from_utf8_lossy(payload)
        );
    }

    /// Every byte the scan reads specially, plus filler, so short strings hit every transition.
    const STRUCTURAL: &[u8] = b"{}[]\"\\x:, 1";

    /// The structural bytes with backslashes weighted up and three bytes past ASCII.
    const EDGE_ALPHABET: &[u8] = b"{}[]\"\\\\\\x \x80\xdb\xfb";

    /// Lengths either side of the 16-byte lane, the 32-byte half and the 64-byte block edges.
    const EDGE_LENGTHS: &[usize] = &[
        0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 95, 96, 97, 127, 128, 129, 191, 192, 193,
    ];

    /// Cases per differential property.
    const CASES: u32 = 4096;

    /// Cases for generated JSON, whose recursive generator costs far more per case than the scan.
    const JSON_CASES: u32 = 1024;

    /// A bound: the edges 0, 1 and 2, the default, or anywhere up to twice the default.
    fn bound() -> impl Strategy<Value = usize> {
        prop_oneof![
            Just(0_usize),
            Just(1_usize),
            Just(2_usize),
            Just(MAX_PARSE_DEPTH),
            0_usize..130,
        ]
    }

    /// A structural byte, a run of one to nine backslashes, a byte past ASCII, or filler.
    fn token() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            4 => prop::sample::select(b"{}[]\"".as_slice()).prop_map(|b| vec![b]),
            3 => (1_usize..10).prop_map(|n| vec![b'\\'; n]),
            1 => (0x80_u8..=u8::MAX).prop_map(|b| vec![b]),
            2 => prop::sample::select(b"x:, 1".as_slice()).prop_map(|b| vec![b]),
        ]
    }

    /// JSON of random shape and depth, with brackets, quotes and backslashes inside strings.
    fn json_value() -> impl Strategy<Value = String> {
        let string = "[a-z{}\\[\\]\"\\\\]{0,12}".prop_map(|s| {
            let mut out = String::from("\"");
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        });
        let leaf = prop_oneof![Just("1".to_string()), Just("null".to_string()), string];
        leaf.prop_recursive(120, 4000, 6, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6)
                    .prop_map(|items| format!("[{}]", items.join(","))),
                prop::collection::vec(("[a-z\\[{\"\\\\]{0,6}", inner), 0..6).prop_map(|fields| {
                    let body: Vec<String> = fields
                        .into_iter()
                        .map(|(key, value)| format!("{key:?}:{value}"))
                        .collect();
                    format!("{{{}}}", body.join(","))
                }),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(CASES))]

        #[test]
        fn block_scan_agrees_on_arbitrary_bytes(
            payload in prop::collection::vec(any::<u8>(), 0..700),
            max in bound(),
        ) {
            agrees(&payload, max);
        }

        #[test]
        fn block_scan_agrees_on_structural_bytes(
            payload in prop::collection::vec(prop::sample::select(STRUCTURAL), 0..700),
            max in bound(),
        ) {
            agrees(&payload, max);
        }

        #[test]
        fn block_scan_agrees_on_backslash_runs_and_structure(
            tokens in prop::collection::vec(token(), 0..160),
            max in bound(),
        ) {
            agrees(&tokens.concat(), max);
        }

        #[test]
        fn block_scan_agrees_either_side_of_lane_and_block_edges(
            payload in prop::sample::select(EDGE_LENGTHS)
                .prop_flat_map(|len| prop::collection::vec(prop::sample::select(EDGE_ALPHABET), len)),
            max in bound(),
        ) {
            agrees(&payload, max);
        }

        #[test]
        fn block_scan_agrees_on_deep_and_mismatched_nesting(
            depth in 0_usize..400,
            offset in 0_usize..130,
            max in 0_usize..300,
            opens in prop::sample::select(vec!["[", "{\"a\":", "{"]),
            closes in prop::sample::select(vec!["]", "}"]),
        ) {
            let payload = format!("{}{}1{}", " ".repeat(offset), opens.repeat(depth), closes.repeat(depth));
            agrees(payload.as_bytes(), max);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(JSON_CASES))]

        #[test]
        fn block_scan_agrees_on_generated_json(json in json_value(), max in bound()) {
            agrees(json.as_bytes(), max);
        }
    }

    #[test]
    fn block_scan_agrees_on_every_short_string_across_lane_and_block_edges() {
        let alphabet = b"{]\"\\x[";
        for len in 0..=5_u32 {
            for n in 0..alphabet.len().pow(len) {
                let mut s = Vec::new();
                let mut k = n;
                for _ in 0..len {
                    s.push(alphabet[k % alphabet.len()]);
                    k /= alphabet.len();
                }
                for offset in [0, 15, 16, 31, 32, 33, 59, 62, 63, 64, 65, 123, 127, 128] {
                    let payload = after(offset, &s);
                    for max in 0..4 {
                        agrees(&payload, max);
                    }
                }
            }
        }
    }
}
