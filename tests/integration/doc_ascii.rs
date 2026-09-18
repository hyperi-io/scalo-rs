// Project:   scalo
// File:      tests/integration/doc_ascii.rs
// Purpose:   Keep every doc comment under src/ ASCII
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Doc comments ship in rustdoc and in every config schema derived from
//! them, so they stay ASCII.

use std::path::{Path, PathBuf};

/// Collect every `.rs` file under `dir`, recursively.
fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|e| panic!("cannot list {}: {e}", dir.display()))
            .path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn doc_comments_under_src_are_ascii() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_sources(&root.join("src"), &mut files);
    files.sort();
    assert!(!files.is_empty(), "no .rs files found under src/");

    let mut offenders = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
        for (idx, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if (trimmed.starts_with("///") || trimmed.starts_with("//!")) && !line.is_ascii() {
                let shown = file.strip_prefix(root).unwrap_or(file);
                offenders.push(format!("{}:{}", shown.display(), idx + 1));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "non-ASCII in doc comments (use ->, --, ..., <=, >=, us):\n{}",
        offenders.join("\n")
    );
}
