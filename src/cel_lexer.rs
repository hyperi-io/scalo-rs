// Project:   scalo
// File:      src/cel_lexer.rs
// Purpose:   Read CEL text the way cel's lexer does, without parsing it
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Read CEL text the way cel's lexer does (cel 0.14.5, `CEL.g4`), for code
//! that needs the names an expression reads and calls without parsing it.
//!
//! String and bytes literals (one-line, triple-quoted and raw, with the `r`,
//! `R`, `b`, `B`, `br`, `bR`, `Br` and `BR` prefixes), `//` comments and
//! backtick-quoted identifiers are opaque, so a name inside one is never read.
//! Whitespace and comments between a name and its `(` still make it a call.
//! scalo-py's `expression/profile.py` reads the same forms.

/// What a token is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An identifier followed, past any whitespace and comments, by `(`.
    Call,
    /// Any other identifier.
    Name,
    /// A literal, a comment or a backtick-quoted identifier.
    Opaque,
}

/// One token, the byte range `start..end` of the text it was read from.
///
/// Both ends of every token fall on ASCII bytes, so they are char boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) kind: Kind,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// The identifiers and opaque spans of `expr`, in order. Bytes in neither are
/// operators, digits, punctuation and whitespace.
pub(crate) fn tokens(expr: &str) -> Tokens<'_> {
    Tokens {
        bytes: expr.as_bytes(),
        i: 0,
    }
}

/// `expr` with every byte of every opaque span replaced by a space, so a
/// pattern run over it sees only code, at the same byte offsets.
#[cfg(feature = "transport")]
pub(crate) fn code_only(expr: &str) -> String {
    let mut code = String::with_capacity(expr.len());
    let mut copied = 0;
    for token in tokens(expr).filter(|t| t.kind == Kind::Opaque) {
        code.push_str(&expr[copied..token.start]);
        code.extend(std::iter::repeat_n(' ', token.end - token.start));
        copied = token.end;
    }
    code.push_str(&expr[copied..]);
    code
}

/// Bytes between a function name and its `(` that still leave it a call.
const CALL_GAP: &[u8] = b" \t\r\n\x0c\x0b";

/// Whole names that open a string or bytes literal when a quote follows directly.
const LITERAL_PREFIXES: &[&[u8]] = &[b"r", b"R", b"b", b"B", b"br", b"bR", b"Br", b"BR"];

/// Bytes cel's lexer accepts inside a backtick-quoted identifier.
const ESCAPED_IDENT_BYTES: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-/ ";

/// The iterator [`tokens`] returns.
pub(crate) struct Tokens<'a> {
    bytes: &'a [u8],
    i: usize,
}

impl Iterator for Tokens<'_> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        let bytes = self.bytes;
        while self.i < bytes.len() {
            let start = self.i;
            let byte = bytes[start];
            if is_quote(byte) {
                self.i = string_end(bytes, start, false);
                return Some(opaque(start, self.i));
            }
            if bytes[start..].starts_with(b"//") {
                self.i = comment_end(bytes, start);
                return Some(opaque(start, self.i));
            }
            if byte == b'`' {
                self.i = escaped_ident_end(bytes, start);
                if self.i > start + 1 {
                    return Some(opaque(start, self.i));
                }
                continue;
            }
            if !is_ident_start(byte) {
                self.i += 1;
                continue;
            }
            while self.i < bytes.len() && is_ident_char(bytes[self.i]) {
                self.i += 1;
            }
            let word = &bytes[start..self.i];
            if self.i < bytes.len() && is_quote(bytes[self.i]) && LITERAL_PREFIXES.contains(&word) {
                let raw = word.iter().any(|b| b.eq_ignore_ascii_case(&b'r'));
                self.i = string_end(bytes, self.i, raw);
                return Some(opaque(start, self.i));
            }
            let kind = if bytes.get(gap_end(bytes, self.i)) == Some(&b'(') {
                Kind::Call
            } else {
                Kind::Name
            };
            return Some(Token {
                kind,
                start,
                end: self.i,
            });
        }
        None
    }
}

fn opaque(start: usize, end: usize) -> Token {
    Token {
        kind: Kind::Opaque,
        start,
        end,
    }
}

/// The index after the string literal whose opening quote is at `quote_at`.
///
/// Three quotes open a literal that runs to the next three, a raw literal has
/// no escapes, and a one-line literal ends at a CR or LF, which cel refuses.
fn string_end(bytes: &[u8], quote_at: usize, raw: bool) -> usize {
    let quote = bytes[quote_at];
    let step = |b: u8| if !raw && b == b'\\' { 2 } else { 1 };
    let triple = [quote; 3];
    if bytes[quote_at..].starts_with(&triple) {
        let mut i = quote_at + 3;
        while i < bytes.len() {
            if bytes[i..].starts_with(&triple) {
                return i + 3;
            }
            i += step(bytes[i]);
        }
        // With no closing triple, cel reads an empty literal and opens a new one
        // at the third quote.
        return quote_at + 2;
    }
    let mut i = quote_at + 1;
    while i < bytes.len() {
        let b = bytes[i];
        if b == quote {
            return i + 1;
        }
        if b == b'\n' || b == b'\r' {
            return i;
        }
        i += step(b);
    }
    bytes.len()
}

/// The index of the LF that ends the `//` comment at `start`, or the end.
fn comment_end(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map_or(bytes.len(), |n| start + n)
}

/// The index after the whitespace and `//` comments starting at `i`.
fn gap_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        if CALL_GAP.contains(&bytes[i]) {
            i += 1;
        } else if bytes[i..].starts_with(b"//") {
            i = comment_end(bytes, i);
        } else {
            break;
        }
    }
    i
}

/// The index after the backtick-quoted identifier at `start`, or after the
/// backtick alone when none closes.
fn escaped_ident_end(bytes: &[u8], start: usize) -> usize {
    let body = bytes[start + 1..]
        .iter()
        .take_while(|b| ESCAPED_IDENT_BYTES.contains(b))
        .count();
    let close = start + 1 + body;
    if body > 0 && bytes.get(close) == Some(&b'`') {
        close + 1
    } else {
        start + 1
    }
}

fn is_quote(b: u8) -> bool {
    b == b'"' || b == b'\''
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

/// Whether `b` may continue a CEL identifier.
pub(crate) fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(expr: &str) -> Vec<(Kind, &str)> {
        tokens(expr)
            .map(|t| (t.kind, &expr[t.start..t.end]))
            .collect()
    }

    #[test]
    fn names_calls_and_opaque_spans_come_out_in_order() {
        assert_eq!(
            read("a.b(r\"x\") // c\n || `d.e` == 'f'"),
            [
                (Kind::Name, "a"),
                (Kind::Call, "b"),
                (Kind::Opaque, "r\"x\""),
                (Kind::Opaque, "// c"),
                (Kind::Opaque, "`d.e`"),
                (Kind::Opaque, "'f'"),
            ]
        );
    }

    #[test]
    fn a_gap_of_whitespace_and_comments_still_makes_a_call() {
        assert_eq!(
            read("m // c\n\t("),
            [(Kind::Call, "m"), (Kind::Opaque, "// c")]
        );
        assert_eq!(read("m;("), [(Kind::Name, "m")]);
    }

    /// Opaque spans become spaces and the offsets of everything else hold.
    #[cfg(feature = "transport")]
    #[test]
    fn code_only_blanks_every_opaque_span() {
        let expr = "x == \"é(\" && y.m(br'z') // q\n";
        let code = code_only(expr);
        assert_eq!(code.len(), expr.len());
        let blank = |n: usize| " ".repeat(n);
        assert_eq!(
            code,
            format!("x == {} && y.m({}) {}\n", blank(5), blank(5), blank(4))
        );
        // A lone backtick is code, so it is kept.
        assert_eq!(code_only("` a"), "` a");
    }
}
