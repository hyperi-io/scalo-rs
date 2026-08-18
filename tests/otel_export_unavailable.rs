// Project:   scalo
// File:      tests/otel_export_unavailable.rs
// Purpose:   OTLP export against a receiver that is not there
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! What happens when the collector is simply absent.
//!
//! On-by-default export means most processes will, at some point, run with
//! nothing listening on the endpoint -- a laptop, a CI runner, a namespace
//! whose collector has not been deployed yet. That has to cost telemetry and
//! nothing else: the service starts, keeps serving `/metrics`, stays
//! responsive under sustained emission, and does not accumulate the
//! measurements it cannot send.
//!
//! No Docker needed: the point is the ABSENCE of a receiver, and a closed
//! port is a truer absence than a stopped container.

#![cfg(feature = "otel-metrics")]

use std::time::{Duration, Instant};

use metrics::counter;
use scalo::metrics::{MetricsConfig, MetricsManager, OtelMetricsConfig};

/// A port with nothing on it: bind, read the port, drop the listener.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn resident_bytes() -> u64 {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
    let pid = sysinfo::get_current_pid().expect("current pid");
    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    sys.process(pid).map_or(0, sysinfo::Process::memory)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_collector_costs_telemetry_and_nothing_else() {
    let port = closed_port();
    let manager = MetricsManager::with_config(MetricsConfig {
        namespace: String::new(),
        enable_process_metrics: false,
        enable_container_metrics: false,
        otel: OtelMetricsConfig {
            enabled: true,
            endpoint: format!("http://127.0.0.1:{port}"),
            export_interval_secs: 1,
            export_timeout_secs: 1,
            service_name: "scalo-otel-absent-test".to_string(),
            ..OtelMetricsConfig::default()
        },
        ..MetricsConfig::default()
    });

    // Startup must have completed at all -- an unreachable endpoint is not a
    // construction error, because the exporter connects lazily.
    let handle = manager
        .render_handle()
        .expect("Prometheus handle must exist even with no collector");

    // Sustained emission across several export cycles, so the exporter has
    // had many chances to fail and to back off.
    let before = resident_bytes();
    let started = Instant::now();
    for i in 0..50_000_u64 {
        counter!("otel_absent_probe").increment(1);
        if i % 10_000 == 0 {
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
    }
    let elapsed = started.elapsed();
    let growth = resident_bytes().saturating_sub(before);

    assert!(
        elapsed < Duration::from_secs(30),
        "emission stalled behind a dead collector: took {elapsed:?}"
    );
    assert!(
        handle.render().contains("otel_absent_probe"),
        "/metrics stopped serving while the collector was absent"
    );
    // One counter is fixed cardinality, so anything held back would be
    // un-exported batches. The bound is deliberately loose -- it is there to
    // catch accumulation, not to measure the allocator.
    assert!(
        growth < 128 * 1024 * 1024,
        "resident memory grew {growth} bytes with no collector -- exports are accumulating"
    );
}

#[test]
fn export_is_off_when_disabled_or_unaddressed() {
    let default = OtelMetricsConfig::default();
    assert!(
        default.is_active(),
        "export is on by default -- a service should be a good citizen without being told"
    );

    let disabled = OtelMetricsConfig {
        enabled: false,
        ..OtelMetricsConfig::default()
    };
    assert!(!disabled.is_active(), "enabled=false must switch it off");

    let blank = OtelMetricsConfig {
        endpoint: String::new(),
        ..OtelMetricsConfig::default()
    };
    assert!(!blank.is_active(), "a blank endpoint must switch it off");

    let whitespace = OtelMetricsConfig {
        endpoint: "   ".to_string(),
        ..OtelMetricsConfig::default()
    };
    assert!(
        !whitespace.is_active(),
        "a whitespace endpoint reads as off, not as an unparseable URI"
    );
}

#[test]
fn the_env_var_can_switch_export_off() {
    temp_env::with_var("OTEL_EXPORTER_OTLP_ENDPOINT", Some(""), || {
        assert!(
            !OtelMetricsConfig::default().is_active(),
            "blanking the endpoint via env must switch export off"
        );
    });
    temp_env::with_var(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        Some("http://collector.example:4317"),
        || {
            assert!(
                OtelMetricsConfig::default().is_active(),
                "an endpoint from env must switch export on"
            );
        },
    );
}
