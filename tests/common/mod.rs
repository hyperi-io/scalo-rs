// Project:   scalo
// File:      tests/common/mod.rs
// Purpose:   Shared test fixtures and utilities
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(dead_code)]

//! Shared test fixtures and utilities.

use std::path::PathBuf;
use tempfile::TempDir;

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
