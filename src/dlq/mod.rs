// Project:   scalo
// File:      src/dlq/mod.rs
// Purpose:   Unified dead letter queue with pluggable backends
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Unified dead letter queue (DLQ) with pluggable backends.
//!
//! Provides a shared DLQ abstraction for all data-plane services. Failed messages
//! are routed to one or more backends (file, Kafka, or custom) using
//! configurable cascade or fan-out modes.
//!
//! ## Backends
//!
//! - **File**: NDJSON files with automatic rotation and cleanup. Always
//!   available, no external dependencies.
//! - **Kafka**: Routes to Kafka topics with per-table or common
//!   routing. Requires the `dlq-kafka` feature.
//! - **HTTP**: POSTs entries as NDJSON. Requires the `dlq-http` feature.
//!
//! Backends are selected and configured via [`DlqConfig`]; consumers
//! never construct backend types directly. To add a new backend, extend
//! the [`DlqBackend`] enum in scalo itself.
//!
//! ## Modes
//!
//! - **Cascade** (default): Try backends in order, stop on first success.
//! - **Fan-out**: Write to all backends, succeed if any succeed.
//! - **FileOnly**: File backend only (no Kafka dependency).
//! - **KafkaOnly**: Kafka backend only.
//!
//! ## Example
//!
//! ```rust,no_run
//! use scalo::dlq::{Dlq, DlqConfig, DlqEntry, DlqSource};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = DlqConfig::default();
//! let shutdown = CancellationToken::new();
//! let dlq = Dlq::spawn(&config, "my-service", None, shutdown.clone())?;
//!
//! let entry = DlqEntry::new("my-service", "parse_error", b"bad data".to_vec())
//!     .with_destination("acme.auth")
//!     .with_source(DlqSource::kafka("events", 1, 42));
//!
//! dlq.send(entry).await?;       // queued (non-blocking)
//! dlq.flush().await?;           // wait for durable write
//! shutdown.cancel();
//! dlq.shutdown().await?;        // drain + exit
//! # Ok(())
//! # }
//! ```

mod backend;
mod config;
mod entry;
mod error;
mod file;
mod orchestrator;

#[cfg(feature = "dlq-kafka")]
mod kafka;

#[cfg(feature = "dlq-http")]
mod http;

// Core types (always available with `dlq` feature)
pub use backend::DlqBackend;
pub use config::{DlqConfig, DlqMode, FileDlqConfig, RotationPeriod};
pub use entry::{DlqEntry, DlqSource};
pub use error::DlqError;
pub use orchestrator::Dlq;

// Kafka types (only with `dlq-kafka` feature)
#[cfg(feature = "dlq-kafka")]
pub use config::{DlqRouting, KafkaDlqConfig};

// HTTP types (only with `dlq-http` feature)
#[cfg(feature = "dlq-http")]
pub use http::HttpDlqConfig;

/// Result type for DLQ operations.
pub type Result<T> = std::result::Result<T, DlqError>;

/// Reads what the file backend wrote, for tests in any module.
#[cfg(test)]
pub(crate) mod test_files {
    use std::path::{Path, PathBuf};

    /// The file backend's current file name; a rotation appends a timestamp to it.
    const CURRENT: &str = "dlq.ndjson";

    /// Every line the file backend wrote under `path` for `service`, oldest
    /// first: the files a rotation moved aside, then the current file. A daily
    /// rotation can fall between two writes, so the current file alone can miss
    /// entries. Empty when the service directory cannot be listed.
    pub(crate) fn written_lines(path: &Path, service: &str) -> Vec<String> {
        let Ok(listing) = std::fs::read_dir(path.join(service)) else {
            return Vec::new();
        };
        let mut files: Vec<PathBuf> = listing
            .map(|entry| entry.expect("list the DLQ directory").path())
            .filter(|file| {
                file.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_prefix(CURRENT))
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
            })
            .collect();
        files.sort_by_key(|file| {
            (
                file.file_name().is_some_and(|name| name == CURRENT),
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
}
