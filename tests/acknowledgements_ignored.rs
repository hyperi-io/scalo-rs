// Project:   scalo
// File:      tests/acknowledgements_ignored.rs
// Purpose:   An acknowledgements section warns once where it is ignored, and applies under HTTP
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `AnyReceiver::from_config` warns once per backend when
//! `<key>.<type>.acknowledgements` sits under a memory, pipe or file source,
//! none of which holds its acknowledgement, and builds the receiver anyway.
//! Under an HTTP source the section is applied, with no warning.
//!
//! The global config installs once per process, and each warning fires once
//! per process, so this file owns both.

#![cfg(all(
    feature = "config",
    feature = "logger",
    any(
        feature = "transport-memory",
        feature = "transport-pipe",
        feature = "transport-file",
        feature = "transport-http"
    )
))]

use std::sync::{Arc, Mutex};

use scalo::transport::{AnyReceiver, TransportBase};
use tracing_subscriber::layer::SubscriberExt as _;

/// A `tracing` writer into a shared buffer.
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Build the receiver at `key` twice: the second build must not warn again.
async fn build_twice(key: &str) {
    for _ in 0..2 {
        let receiver = AnyReceiver::from_config(key)
            .await
            .unwrap_or_else(|e| panic!("{key} builds despite the section: {e}"));
        let _ = receiver.close().await;
    }
}

/// The section reaches an HTTP receive server built armed: on by default and
/// armed before it listens, off where the section says so.
#[cfg(all(feature = "transport-http", feature = "http-server"))]
async fn http_section_applies() {
    for (key, enabled) in [("transport.http_in", true), ("transport.http_off", false)] {
        let receiver = AnyReceiver::from_config_armed(key)
            .await
            .unwrap_or_else(|e| panic!("{key}: {e}"));
        let control = scalo::transport::TransportReceiver::ack_control(&receiver)
            .expect("an HTTP server can hold");
        assert_eq!(
            (control.enabled(), control.is_armed()),
            (enabled, true),
            "{key}"
        );
        let _ = receiver.close().await;
    }
}

#[cfg(all(feature = "transport-http", not(feature = "http-server")))]
async fn http_section_applies() {}

/// The logged lines that carry the warning for `section`.
fn warnings_for<'a>(out: &'a str, section: &str) -> Vec<&'a str> {
    let field = format!("section={section}");
    out.lines().filter(|line| line.contains(&field)).collect()
}

#[tokio::test]
async fn a_section_under_a_source_that_does_not_hold_it_warns_once() {
    let dir = tempfile::tempdir().expect("config tempdir");
    let file_in = dir.path().join("in.ndjson");
    let file_plain = dir.path().join("plain.ndjson");
    std::fs::write(
        dir.path().join("settings.yaml"),
        format!(
            "transport:\n  \
               memory_in:\n    type: memory\n    memory:\n      \
                 acknowledgements:\n        enabled: true\n  \
               pipe_in:\n    type: pipe\n    pipe:\n      \
                 acknowledgements:\n        enabled: true\n  \
               file_in:\n    type: file\n    file:\n      path: \"{}\"\n      \
                 acknowledgements:\n        enabled: true\n  \
               file_plain:\n    type: file\n    file:\n      path: \"{}\"\n  \
               http_in:\n    type: http\n    http:\n      listen: \"127.0.0.1:0\"\n      \
                 acknowledgements:\n        enabled: true\n  \
               http_off:\n    type: http\n    http:\n      listen: \"127.0.0.1:0\"\n      \
                 acknowledgements:\n        enabled: false\n",
            file_in.display(),
            file_plain.display(),
        ),
    )
    .expect("write settings.yaml");
    scalo::config::setup(scalo::config::ConfigOptions {
        config_paths: vec![dir.path().to_path_buf()],
        load_dotenv: false,
        ..scalo::config::ConfigOptions::default()
    })
    .expect("config setup");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::WARN)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || Capture(Arc::clone(&sink))),
        );
    let guard = tracing::subscriber::set_default(subscriber);

    #[cfg(feature = "transport-memory")]
    build_twice("transport.memory_in").await;
    #[cfg(feature = "transport-pipe")]
    build_twice("transport.pipe_in").await;
    #[cfg(feature = "transport-file")]
    {
        build_twice("transport.file_in").await;
        build_twice("transport.file_plain").await;
    }
    #[cfg(feature = "transport-http")]
    {
        build_twice("transport.http_in").await;
        http_section_applies().await;
    }

    drop(guard);
    let out = String::from_utf8_lossy(&captured.lock().unwrap()).into_owned();

    #[cfg(feature = "transport-memory")]
    {
        let lines = warnings_for(&out, "transport.memory_in.memory.acknowledgements");
        assert_eq!(lines.len(), 1, "memory warns once: {out}");
        assert!(
            lines[0].contains("this source has no acknowledgement to hold"),
            "{out}"
        );
    }
    #[cfg(feature = "transport-pipe")]
    {
        let lines = warnings_for(&out, "transport.pipe_in.pipe.acknowledgements");
        assert_eq!(lines.len(), 1, "pipe warns once: {out}");
        assert!(
            lines[0].contains("this source has no acknowledgement to hold"),
            "{out}"
        );
    }
    #[cfg(feature = "transport-file")]
    {
        let lines = warnings_for(&out, "transport.file_in.file.acknowledgements");
        assert_eq!(lines.len(), 1, "file warns once: {out}");
        assert!(
            lines[0].contains("not supported on a file source yet")
                && lines[0].contains("a block released Errored is skipped"),
            "{out}"
        );
        assert!(
            !out.contains("transport.file_plain"),
            "a file source with no section does not warn: {out}"
        );
    }
    #[cfg(feature = "transport-http")]
    {
        assert!(
            warnings_for(&out, "transport.http_in.http.acknowledgements").is_empty(),
            "an HTTP source applies the section without a warning: {out}"
        );
    }
    assert!(out.contains("WARN"), "{out}");
}
