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
use std::process::Command;
use std::sync::OnceLock;
use tempfile::TempDir;

#[cfg(all(feature = "secrets-vault", feature = "deployment-test-support", unix))]
pub mod openbao;

#[cfg(all(feature = "transport-kafka-test-support", unix))]
pub mod kafka;

/// This suite, for container names, labels, the run log and the
/// `HYPERI_TEST_*` lookup.
#[cfg(all(feature = "deployment-test-support", unix))]
pub const SUITE: scalo::deployment::test_support::shared_service::Suite =
    scalo::deployment::test_support::shared_service::Suite {
        repo: "scalo-rs",
        manifest_dir: env!("CARGO_MANIFEST_DIR"),
        tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
    };

/// `name` made unique to this test process, for a topic, consumer group or
/// secret path on a service other tests share.
pub fn unique(name: &str) -> String {
    format!("{name}-p{}", std::process::id())
}

/// Fail in CI rather than skip: a skipped container test reports green while
/// exercising nothing, and CI does provide a container runtime.
pub fn skip_or_fail(reason: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "{reason} in CI, which provides a container runtime. This test must RUN here."
    );
    eprintln!("skipping: {reason}");
}

/// Whether a Docker daemon answers `docker info`, asked once per test binary.
pub fn docker_answers() -> bool {
    static ANSWERS: OnceLock<bool> = OnceLock::new();
    *ANSWERS.get_or_init(|| {
        Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

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
