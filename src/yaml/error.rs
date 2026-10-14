// Project:   scalo
// File:      src/yaml/error.rs
// Purpose:   Located YAML error with a secret-masking snippet renderer
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`YamlError`]: where a parse or emit failed, and why.

use std::fmt::Write as _;

/// Placeholder written over a masked value in [`YamlError::render_snippet`].
const MASK: &str = "***REDACTED***";

/// A YAML parse or emit failure, located when the parser could say where.
///
/// `Display` is `<origin>: <message>`, and the message carries the line and
/// column. It quotes no line of the input, so it is safe to log.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
#[error("{origin}: {message}")]
pub struct YamlError {
    /// The file path or label the YAML came from.
    pub origin: String,
    /// 1-based line of the failure, when known.
    pub line: Option<u64>,
    /// 1-based column of the failure, when known.
    pub column: Option<u64>,
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl YamlError {
    /// A parse or deserialise failure from serde-saphyr.
    pub(super) fn parse(origin: &str, error: serde_saphyr::Error) -> Self {
        let location = error.location().filter(|at| at.line() > 0);
        Self {
            origin: origin.to_string(),
            line: location.map(|at| at.line()),
            column: location.map(|at| at.column()).filter(|&column| column > 0),
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }

    /// The input could not be read.
    pub(super) fn io(origin: &str, error: std::io::Error) -> Self {
        Self {
            origin: origin.to_string(),
            line: None,
            column: None,
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }

    /// The input is over the profile's byte limit.
    pub(super) fn too_large(origin: &str, len: usize, max: usize) -> Self {
        Self {
            origin: origin.to_string(),
            line: None,
            column: None,
            message: format!("{len} bytes is over the {max}-byte limit for this input"),
            source: None,
        }
    }

    /// The value could not be serialised.
    pub(super) fn emit(
        origin: &str,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            origin: origin.to_string(),
            line: None,
            column: None,
            message: format!("cannot serialise the value: {error}"),
            source: Some(Box::new(error)),
        }
    }

    /// The emitted text did not read back to the value it was emitted from.
    pub(super) fn round_trip(origin: &str) -> Self {
        Self {
            origin: origin.to_string(),
            line: None,
            column: None,
            message: "the YAML emitted for this value reads back as a different value".to_string(),
            source: None,
        }
    }

    /// The failure, without the origin.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The failing line of `source`, with a caret under the column.
    ///
    /// A value under a key whose name looks secret (`password`, `token`,
    /// `key`, `auth` and the rest of scalo's sensitive-name list), or under a
    /// parent key that does, is replaced with `***REDACTED***`. Only the one
    /// line is shown. Empty when the error has no line.
    ///
    /// Meant for a person at a terminal, such as `config-check`. Logs take
    /// the `Display` form, which quotes nothing.
    #[must_use]
    pub fn render_snippet(&self, source: &str) -> String {
        let Some(line_no) = self.line else {
            return String::new();
        };
        let lines: Vec<&str> = source.lines().collect();
        let Some(index) = usize::try_from(line_no - 1)
            .ok()
            .filter(|&i| i < lines.len())
        else {
            return String::new();
        };
        let shown = if under_sensitive_key(&lines, index) {
            masked(lines[index])
        } else {
            printable(lines[index])
        };

        let gutter = line_no.to_string();
        let pad = " ".repeat(gutter.len());
        let mut out = format!("{pad}--> {}:{line_no}", self.origin);
        if let Some(column) = self.column {
            let _ = write!(out, ":{column}");
        }
        let _ = write!(out, "\n{pad} |\n{gutter} | {shown}");
        if let Some(column) = self.column {
            let indent = " ".repeat(usize::try_from(column - 1).unwrap_or(0));
            let _ = write!(out, "\n{pad} | {indent}^");
        }
        out
    }
}

/// Whether line `index`, or any key it sits under, names a secret.
fn under_sensitive_key(lines: &[&str], index: usize) -> bool {
    if crate::sensitive::is_sensitive_name(lines[index]) {
        return true;
    }
    let mut indent = indentation(lines[index]);
    for line in lines[..index].iter().rev() {
        let trimmed = line.trim_start();
        if indent == 0 {
            break;
        }
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let parent_indent = indentation(line);
        if parent_indent < indent {
            if crate::sensitive::is_sensitive_name(key_of(trimmed)) {
                return true;
            }
            indent = parent_indent;
        }
    }
    false
}

/// Leading spaces on a line.
fn indentation(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// The key of a `key: value` line, with any `- ` item marker removed.
fn key_of(trimmed: &str) -> &str {
    let item = trimmed.trim_start_matches(['-', ' ']);
    item.split_once(':').map_or(item, |(key, _)| key)
}

/// The line with everything after its key replaced by the mask.
fn masked(line: &str) -> String {
    let lead = &line[..indentation(line)];
    match line.split_once(':') {
        Some((key, _)) => format!("{}: {MASK}", printable(key)),
        None => format!("{lead}{MASK}"),
    }
}

/// The line with control characters escaped, so it cannot drive a terminal.
fn printable(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    for c in line.chars() {
        if c.is_control() && c != '\t' {
            out.extend(c.escape_debug());
        } else {
            out.push(c);
        }
    }
    out
}
