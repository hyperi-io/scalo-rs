// Project:   scalo
// File:      tests/common/mod.rs
// Purpose:   Shared test fixtures and utilities
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(dead_code)]

//! Shared test fixtures and utilities.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// The file DLQ's current file name; a rotation appends a timestamp to it.
const DLQ_FILE: &str = "dlq.ndjson";

/// Every line a file DLQ wrote under `path` for `service`, oldest first: the
/// files a rotation moved aside, then the current file. A daily rotation can
/// fall between two writes, so the current file alone can miss entries. The
/// unit tests' copy is `scalo::dlq::test_files::written_lines`, which an
/// integration test cannot reach.
pub fn dlq_file_lines(path: &Path, service: &str) -> Vec<String> {
    let Ok(listing) = std::fs::read_dir(path.join(service)) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = listing
        .map(|entry| entry.expect("list the DLQ directory").path())
        .filter(|file| {
            file.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix(DLQ_FILE))
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
        })
        .collect();
    files.sort_by_key(|file| {
        (
            file.file_name().is_some_and(|name| name == DLQ_FILE),
            file.clone(),
        )
    });
    files
        .iter()
        .flat_map(|file| {
            let body = std::fs::read_to_string(file).expect("read a DLQ file");
            body.lines().map(str::to_owned).collect::<Vec<_>>()
        })
        .collect()
}

/// Create a temporary directory with config files for testing.
#[allow(dead_code)]
pub fn create_test_config_dir() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let path = dir.path().to_path_buf();

    // Create defaults.yaml
    std::fs::write(
        path.join("defaults.yaml"),
        r#"
log_level: debug
database:
  host: localhost
  port: 5432
"#,
    )
    .expect("failed to write defaults.yaml");

    // Create settings.yaml
    std::fs::write(
        path.join("settings.yaml"),
        r#"
app_name: test_app
database:
  username: testuser
"#,
    )
    .expect("failed to write settings.yaml");

    // Create settings.development.yaml
    std::fs::write(
        path.join("settings.development.yaml"),
        r#"
debug: true
database:
  password: devpassword
"#,
    )
    .expect("failed to write settings.development.yaml");

    (dir, path)
}
