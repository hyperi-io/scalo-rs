// Project:   scalo
// File:      src/source_census.rs
// Purpose:   Test-only scan of the crate's source for the metric series it writes
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Test-only scans of `src` for the metric series the crate writes.
//!
//! The transport fixture test and the manifest coverage test read the same
//! source, so they share the file walk and the macro argument parser.

#![allow(
    dead_code,
    reason = "the transport and manifest censuses each use part of this"
)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The Rust files of `dir` and below, test modules cut, test files left out,
/// and any file named in `skip` left out.
pub(crate) fn sources(dir: &Path, skip: &[&str], out: &mut Vec<(PathBuf, String)>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if path.is_dir() && name != "test_support" {
            sources(&path, skip, out);
        } else if path.extension().is_some_and(|e| e == "rs")
            && !name.contains("test")
            && !skip.contains(&name.as_str())
        {
            let text = std::fs::read_to_string(&path).unwrap();
            let cut = text.find("#[cfg(test)]\nmod tests {").unwrap_or(text.len());
            out.push((path, text[..cut].to_string()));
        }
    }
}

/// `text`, which follows a `(`, up to its matching `)`.
pub(crate) fn parenthesised(text: &str) -> &str {
    let mut depth = 1;
    let end = text.char_indices().find(|&(_, c)| {
        depth += i32::from(c == '(') - i32::from(c == ')');
        depth == 0
    });
    &text[..end.map_or(text.len(), |(i, _)| i)]
}

/// Top-level comma-separated arguments.
pub(crate) fn arguments(args: &str) -> Vec<String> {
    let (mut out, mut depth, mut current) = (Vec::new(), 0, String::new());
    for c in args.chars() {
        depth += i32::from("([{".contains(c)) - i32::from(")]}".contains(c));
        if c == ',' && depth == 0 {
            out.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
    }
    out.push(current);
    out.into_iter()
        .map(|a| a.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|a| !a.is_empty())
        .collect()
}

/// A metric site's first argument: a literal name, or an expression that
/// names the series at run time.
fn name_of(first: &str) -> Result<String, String> {
    match first
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        Some(literal) if !literal.contains('"') => Ok(literal.to_string()),
        _ => Err(first.to_string()),
    }
}

/// Where the text before `at` is not part of a longer identifier.
fn starts_a_call(text: &str, at: usize) -> bool {
    text[..at]
        .chars()
        .next_back()
        .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
}

/// `text` as code alone: its comments blanked and its test module cut.
pub(crate) fn code_of(text: &str) -> String {
    let cut = text.find("\nmod tests {").unwrap_or(text.len());
    text[..cut]
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("//") {
                ""
            } else {
                line
            }
        })
        .collect::<Vec<&str>>()
        .join("\n")
}

/// The literal names the source hands to a describer, as `(file under src,
/// name)`: `Series::counter("name"` and its `gauge` and `histogram` forms, and
/// the same calls on a `manager`.
pub(crate) fn described_names() -> Vec<(String, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&src, &["source_census.rs"], &mut files);
    let mut out = Vec::new();
    for (path, text) in &files {
        let rel = path
            .strip_prefix(&src)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let code = code_of(text);
        for kind in ["counter", "gauge", "histogram"] {
            let mut calls = vec![format!("Series::{kind}(")];
            for suffix in ["", "_with_labels", "_with_buckets"] {
                calls.push(format!("manager.{kind}{suffix}("));
            }
            for call in calls {
                for (at, _) in code.match_indices(&call) {
                    let args = arguments(parenthesised(&code[at + call.len()..]));
                    if let Some(Ok(name)) = args.first().map(|a| name_of(a)) {
                        out.push((rel.clone(), name));
                    }
                }
            }
        }
    }
    out
}

/// What the crate's source writes: the literal series names, and each site
/// that names its series at run time, as `(file under src, expression)`.
#[derive(Debug, Default)]
pub(crate) struct Census {
    pub literal: BTreeSet<String>,
    pub dynamic: BTreeSet<(String, String)>,
}

/// Every metric name the non-test source under `src` writes, through
/// `metrics::counter!`, `gauge!` and `histogram!`, `Key::from_static_parts`,
/// and the manager's and clients' `.counter("name"`, `.gauge("name"` and
/// `.histogram("name"` calls with their `_with_labels` and `_with_buckets`
/// forms. Comments and the `describe_*!` macros are not writes.
pub(crate) fn metric_names() -> Census {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&src, &["source_census.rs"], &mut files);
    let mut census = Census::default();
    for (path, text) in &files {
        let rel = path
            .strip_prefix(&src)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let code = code_of(text);
        let mut firsts: Vec<String> = Vec::new();
        let mut method_names: Vec<String> = Vec::new();
        for kind in ["counter", "gauge", "histogram"] {
            let call = format!("{kind}!(");
            for (at, _) in code.match_indices(&call) {
                if starts_a_call(&code, at) {
                    let args = arguments(parenthesised(&code[at + call.len()..]));
                    firsts.push(args[0].clone());
                }
            }
            for suffix in ["", "_with_labels", "_with_buckets"] {
                let call = format!(".{kind}{suffix}(");
                for (at, _) in code.match_indices(&call) {
                    let args = arguments(parenthesised(&code[at + call.len()..]));
                    // A method call on a non-literal name is a site the macros
                    // above already list.
                    if let Some(Ok(name)) = args.first().map(|a| name_of(a)) {
                        method_names.push(name);
                    }
                }
            }
        }
        let call = "Key::from_static_parts(";
        for (at, _) in code.match_indices(call) {
            let args = arguments(parenthesised(&code[at + call.len()..]));
            firsts.push(args[0].clone());
        }
        for first in firsts {
            match name_of(&first) {
                Ok(name) => {
                    census.literal.insert(name);
                }
                Err(expr) => {
                    census.dynamic.insert((rel.clone(), expr));
                }
            }
        }
        census.literal.extend(method_names);
    }
    census
}
