// Project:   scalo
// File:      tests/metrics_without_a_runtime.rs
// Purpose:   MetricsManager must build outside a Tokio runtime
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Building a `MetricsManager` from a synchronous context must not panic.
//!
//! Consumers construct one in plain `#[test]` functions to assert on their own
//! counters. Composing the OTLP exporter into the `metrics` feature made that
//! a panic -- the exporter builds a hyper connector, and hyper panics with
//! "there is no reactor running" when there is no Tokio runtime. Ten
//! dfe-loader tests died on it.
//!
//! Export cannot work without a runtime anyway, so it is skipped and the
//! Prometheus recorder installs on its own.

#![cfg(feature = "metrics")]

use metrics::counter;
use scalo::metrics::{MetricsConfig, MetricsManager};

#[test]
fn a_manager_builds_and_records_with_no_tokio_runtime() {
    // Deliberately NOT #[tokio::test]: the absence of a runtime is the point.
    let manager = MetricsManager::with_config(MetricsConfig {
        namespace: String::new(),
        enable_process_metrics: false,
        enable_container_metrics: false,
        ..MetricsConfig::default()
    });

    counter!("sync_context_probe").increment(3);

    let rendered = manager
        .render_handle()
        .expect("Prometheus recorder must install without a runtime")
        .render();
    assert!(
        rendered.contains("sync_context_probe"),
        "scrape endpoint must still work without a runtime:\n{rendered}"
    );
}

#[test]
fn export_reports_itself_inactive_without_a_runtime() {
    #[cfg(feature = "otel-metrics")]
    {
        let config = scalo::metrics::OtelMetricsConfig::default();
        assert!(
            !config.is_active(),
            "export must stand down with no runtime rather than panicking"
        );
    }
}

#[tokio::test]
async fn export_is_active_again_once_a_runtime_exists() {
    #[cfg(feature = "otel-metrics")]
    {
        let config = scalo::metrics::OtelMetricsConfig::default();
        assert!(
            config.is_active(),
            "the runtime guard must not switch export off where it CAN work"
        );
    }
}
