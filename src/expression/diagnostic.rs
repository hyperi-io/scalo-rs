// Project:   scalo
// File:      src/expression/diagnostic.rs
// Purpose:   Located problems found in an expression's text
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A problem in an expression, with the byte range it covers.
//!
//! [`diagnose`](super::diagnose) returns these where [`validate`](super::validate)
//! returns bare strings, so an editor can underline the call or token at fault.

use serde::Serialize;

/// A byte range in an expression's text, `end` exclusive: `&expr[start..end]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct Span {
    /// First byte of the range.
    pub start: usize,
    /// One past the last byte of the range.
    pub end: usize,
}

impl Span {
    /// The range `start..end`.
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl std::fmt::Display for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

/// One problem found in an expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Diagnostic {
    /// What is wrong, without the position.
    pub message: String,
    /// Where it is, when the scanner or parser knows.
    pub span: Option<Span>,
}

impl Diagnostic {
    pub(crate) fn new(message: impl Into<String>, span: Option<Span>) -> Self {
        Self {
            message: message.into(),
            span,
        }
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.span {
            Some(span) => write!(f, "{} (at {span})", self.message),
            None => f.write_str(&self.message),
        }
    }
}

/// The byte span of the character at a parser position: `line` from 1, `column`
/// from 1, counted in characters. `None` for a position the parser left unset.
pub(crate) fn span_at(expr: &str, line: isize, column: isize) -> Option<Span> {
    let line = usize::try_from(line).ok().filter(|&l| l >= 1)?;
    let column = usize::try_from(column).ok().filter(|&c| c >= 1)?;
    let mut line_start = 0usize;
    for (index, text) in expr.split_inclusive('\n').enumerate() {
        if index + 1 == line {
            let found = text.char_indices().nth(column - 1);
            return Some(match found {
                Some((offset, ch)) => {
                    Span::new(line_start + offset, line_start + offset + ch.len_utf8())
                }
                None => Span::new(line_start + text.len(), line_start + text.len()),
            });
        }
        line_start += text.len();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_position_on_the_first_line_maps_to_its_byte() {
        assert_eq!(span_at("abc def", 1, 5), Some(Span::new(4, 5)));
    }

    #[test]
    fn a_position_on_a_later_line_counts_the_lines_before_it() {
        let expr = "a ==\n  b &&";
        assert_eq!(span_at(expr, 2, 3), Some(Span::new(7, 8)));
        assert_eq!(&expr[7..8], "b");
    }

    #[test]
    fn a_column_counts_characters_not_bytes() {
        let expr = "x == \"\u{e9}\" &&";
        let span = span_at(expr, 1, 10).expect("in range");
        assert_eq!(span, Span::new(10, 11));
        assert_eq!(&expr[span.start..span.end], "&");
    }

    #[test]
    fn a_position_past_the_line_points_at_its_end() {
        assert_eq!(span_at("ab", 1, 9), Some(Span::new(2, 2)));
    }

    #[test]
    fn an_unset_position_has_no_span() {
        assert_eq!(span_at("ab", 0, 0), None);
        assert_eq!(span_at("ab", 3, 1), None);
    }
}
