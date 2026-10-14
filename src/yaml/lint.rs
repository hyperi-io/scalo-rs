// Project:   scalo
// File:      src/yaml/lint.rs
// Purpose:   Warning passes run after a successful YAML parse
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Warnings that do not stop a parse: odd indentation, and plain scalars an
//! untyped reader takes as numbers when they were likely meant as text.

use serde_saphyr::granit_parser::{Event, Parser, ScalarStyle};

use super::Profile;

/// A warning about YAML that parsed, located at a line and column.
///
/// The message never quotes the value, so a lint is safe to log.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lint {
    /// 1-based line.
    pub line: u64,
    /// 1-based column.
    pub column: u64,
    /// What looked wrong and how to fix it.
    pub message: String,
}

impl std::fmt::Display for Lint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "line {}, column {}: {}",
            self.line, self.column, self.message
        )
    }
}

/// The lints `profile` asks for, over text that has already parsed.
pub(super) fn run(text: &str, profile: Profile) -> Vec<Lint> {
    let mut lints = Vec::new();
    match profile {
        Profile::Config => {
            odd_indentation(text, &mut lints);
            number_shapes(text, &mut lints);
        }
        Profile::Data => number_shapes(text, &mut lints),
        Profile::Trusted => {}
    }
    lints
}

/// Flag the first indentation that is not a multiple of two.
fn odd_indentation(text: &str, lints: &mut Vec<Lint>) {
    let options = serde_saphyr::options! {
        budget: None,
        emit_comments: false,
        with_snippet: false,
        require_indent: serde_saphyr::RequireIndent::Even,
    };
    let Err(error) = serde_saphyr::from_str_with_options::<serde::de::IgnoredAny>(text, options)
    else {
        return;
    };
    if let serde_saphyr::Error::IndentationError { actual, .. } = error.without_snippet()
        && let Some(at) = error.location()
    {
        lints.push(Lint {
            line: at.line(),
            column: at.column(),
            message: format!(
                "indented {actual} spaces, not a multiple of two -- check this line sits under the key it belongs to"
            ),
        });
    }
}

/// Where the parser is inside one collection.
enum Frame {
    Sequence,
    /// `true` while the next node is a key.
    Mapping(bool),
}

/// Flag plain scalar values shaped like a leading-zero or `_`-separated number.
fn number_shapes(text: &str, lints: &mut Vec<Lint>) {
    let mut options = serde_saphyr::granit_parser::Options::default();
    options.emit_comments = false;
    let mut frames: Vec<Frame> = Vec::new();

    for item in Parser::new_from_str_with_options(text, options) {
        // The text already parsed once, so a scan error here is unreachable.
        // Stopping is the safe answer if it is not.
        let Ok((event, span)) = item else { return };
        match event {
            Event::Scalar(value, style, _, tag) => {
                let is_key = take_node(&mut frames);
                if !is_key
                    && style == ScalarStyle::Plain
                    && tag.is_none()
                    && let Some(shape) = number_shape(&value)
                {
                    lints.push(Lint {
                        line: span.start.line() as u64,
                        column: span.start.col() as u64 + 1,
                        message: shape.message().to_string(),
                    });
                }
            }
            Event::Alias(_) => {
                take_node(&mut frames);
            }
            Event::SequenceStart(..) => {
                take_node(&mut frames);
                frames.push(Frame::Sequence);
            }
            Event::MappingStart(..) => {
                take_node(&mut frames);
                frames.push(Frame::Mapping(true));
            }
            Event::SequenceEnd | Event::MappingEnd => {
                frames.pop();
            }
            _ => {}
        }
    }
}

/// Account for one node in the enclosing collection, returning `true` for a key.
fn take_node(frames: &mut [Frame]) -> bool {
    match frames.last_mut() {
        Some(Frame::Mapping(expect_key)) => {
            let is_key = *expect_key;
            *expect_key = !is_key;
            is_key
        }
        Some(Frame::Sequence) | None => false,
    }
}

/// A number shape a typed `String` field keeps as text but an untyped reader
/// does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    LeadingZero,
    Underscore,
}

impl Shape {
    fn message(self) -> &'static str {
        match self {
            Self::LeadingZero => {
                "plain number with a leading zero: an untyped reader takes it as a number and drops the zero -- quote it to keep it as text"
            }
            Self::Underscore => {
                "plain number with `_` separators: an untyped reader takes it as a number -- quote it to keep it as text"
            }
        }
    }
}

/// The shape of `scalar` when it is a number an untyped reader would change.
fn number_shape(scalar: &str) -> Option<Shape> {
    let body = scalar.strip_prefix(['+', '-']).unwrap_or(scalar);
    if !body.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let digits: String = body.chars().filter(|&c| c != '_').collect();
    let numeric = digits.parse::<i128>().is_ok()
        || (digits.parse::<f64>().is_ok()
            && digits
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-')));
    if !numeric {
        return None;
    }
    let bytes = body.as_bytes();
    if bytes.len() > 1 && bytes[0] == b'0' && bytes[1].is_ascii_digit() {
        Some(Shape::LeadingZero)
    } else if body.contains('_') {
        Some(Shape::Underscore)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_shape_flags_leading_zero_and_underscore_only() {
        assert_eq!(number_shape("0123"), Some(Shape::LeadingZero));
        assert_eq!(number_shape("-0123"), Some(Shape::LeadingZero));
        assert_eq!(number_shape("00"), Some(Shape::LeadingZero));
        assert_eq!(number_shape("0800.5"), Some(Shape::LeadingZero));
        assert_eq!(number_shape("1_000"), Some(Shape::Underscore));
        assert_eq!(number_shape("0_1"), Some(Shape::Underscore));
        for plain in [
            "0",
            "123",
            "0.5",
            "1e3",
            "0x1A",
            "0o17",
            "2026-01-01",
            "12:30:00",
            "0123abc",
            "_1",
            "v1_0",
            "1.2.3",
            "",
        ] {
            assert_eq!(number_shape(plain), None, "{plain:?}");
        }
    }

    #[test]
    fn number_shapes_skips_keys_quoted_and_tagged_values() {
        let text = "0123: key\nquoted: '0123'\ntagged: !!str 0123\nlist:\n  - 0123\nnested:\n  1_000: k\n  v: 1_000\n";
        let mut lints = Vec::new();
        number_shapes(text, &mut lints);
        let flagged: Vec<u64> = lints.iter().map(|l| l.line).collect();
        assert_eq!(flagged, [5, 8], "{lints:?}");
        assert_eq!(lints[0].column, 5);
    }

    #[test]
    fn odd_indentation_reports_line_and_spaces() {
        let mut lints = Vec::new();
        odd_indentation("a:\n   b: 1\n", &mut lints);
        assert_eq!(lints.len(), 1, "{lints:?}");
        assert_eq!(lints[0].line, 2);
        assert!(
            lints[0].message.contains("3 spaces"),
            "{}",
            lints[0].message
        );

        let mut none = Vec::new();
        odd_indentation("a:\n  b: 1\n", &mut none);
        assert_eq!(none, [] as [Lint; 0]);
    }

    #[test]
    fn profiles_choose_their_passes() {
        let text = "a:\n   b: 0123\n";
        assert_eq!(run(text, Profile::Config).len(), 2);
        assert_eq!(run(text, Profile::Data).len(), 1);
        assert_eq!(run(text, Profile::Trusted), [] as [Lint; 0]);
    }

    #[test]
    fn lint_messages_never_quote_the_value() {
        let lints = run("pin: 0123987\n", Profile::Config);
        assert_eq!(lints.len(), 1);
        assert!(!lints[0].message.contains("0123987"));
        assert!(!lints[0].to_string().contains("0123987"));
    }
}
