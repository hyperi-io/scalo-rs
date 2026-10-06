// Project:   scalo
// File:      tests/otel_export.rs
// Purpose:   Real-collector OTLP export tests -- arrival, outage, recovery
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OTLP metric export against a REAL OpenTelemetry Collector container.
//!
//! No mocks and no happy-path-only asserts: the collector receives what scalo
//! pushes, then the container is paused to take the receiver away mid-run and
//! unpaused to prove export comes back on its own. Between those, the service
//! has to stay up and keep serving its Prometheus endpoint.
//!
//! `#[ignore]` because they need a running Docker daemon:
//!
//! ```text
//! cargo test --features metrics --test otel_export -- --ignored --nocapture
//! ```
//!
//! The global metrics recorder installs once per process, so this file owns
//! one `MetricsManager` and every phase shares it.

#![cfg(feature = "otel-metrics")]

use std::time::Duration;

use metrics::counter;
use scalo::metrics::{MetricsConfig, MetricsManager, OtelMetricsConfig};
use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Collector to test against.
///
/// Pinned in our own source so dependency review can see it: an image tag
/// buried in a crate default is invisible to Renovate, which only reads
/// Cargo.toml.
// renovate: datasource=docker depName=otel/opentelemetry-collector
const COLLECTOR_TAG: &str = "0.162.0";

const OTLP_GRPC_PORT: u16 = 4317;

/// Collector config: OTLP in, straight back out to its own log.
///
/// The debug exporter is what makes the assertion possible -- received metric
/// names land in the container's stderr, so the test reads the collector's
/// own account of what arrived rather than trusting the sender.
const COLLECTOR_CONFIG: &str = r"
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug]
  telemetry:
    logs:
      level: info
";

async fn start_collector() -> (ContainerAsync<GenericImage>, u16) {
    let node = GenericImage::new("otel/opentelemetry-collector", COLLECTOR_TAG)
        .with_exposed_port(ContainerPort::Tcp(OTLP_GRPC_PORT))
        .with_wait_for(WaitFor::message_on_stderr("Everything is ready"))
        .with_env_var("OTELCOL_CONFIG", COLLECTOR_CONFIG)
        .with_cmd(["--config=env:OTELCOL_CONFIG"])
        .start()
        .await
        .expect("start otel collector container");
    let port = node
        .get_host_port_ipv4(OTLP_GRPC_PORT)
        .await
        .expect("collector host port");
    (node, port)
}

/// Everything the collector has logged so far.
async fn collector_log(node: &ContainerAsync<GenericImage>) -> String {
    let out = node.stderr_to_vec().await.unwrap_or_default();
    String::from_utf8_lossy(&out).into_owned()
}

/// Poll the collector log until `needle` shows up or the budget runs out.
///
/// Polling rather than one long sleep so a slow container fails on evidence
/// instead of on a fixed guess.
async fn wait_for_log(node: &ContainerAsync<GenericImage>, needle: &str, budget: Duration) -> bool {
    let deadline = std::time::Instant::now() + budget;
    loop {
        if collector_log(node).await.contains(needle) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn manager_for(port: u16) -> MetricsManager {
    MetricsManager::with_config(MetricsConfig {
        namespace: String::new(),
        enable_process_metrics: false,
        enable_container_metrics: false,
        otel: OtelMetricsConfig {
            enabled: true,
            endpoint: format!("http://127.0.0.1:{port}"),
            // Fast enough to keep the test honest about elapsed time, and it
            // doubles as the backoff base.
            export_interval_secs: 1,
            // A paused container accepts the connection and never answers, so
            // without a short deadline the outage phase would just block.
            export_timeout_secs: 2,
            service_name: "scalo-otel-export-test".to_string(),
            ..OtelMetricsConfig::default()
        },
        ..MetricsConfig::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Docker daemon; run with --ignored"]
async fn export_arrives_survives_an_outage_and_recovers() {
    let (collector, port) = start_collector().await;
    let manager = manager_for(port);

    // --- Phase 1: the collector actually receives what we push ---
    counter!("otel_probe_before_outage").increment(1);
    assert!(
        wait_for_log(
            &collector,
            "otel_probe_before_outage",
            Duration::from_secs(30)
        )
        .await,
        "collector never logged the metric; it did not arrive:\n{}",
        collector_log(&collector).await
    );

    // --- Phase 2: take the receiver away mid-run ---
    collector.pause().await.expect("pause collector");
    counter!("otel_probe_during_outage").increment(1);
    tokio::time::sleep(Duration::from_secs(8)).await;

    // The service must not have died with it, and the scrape endpoint is
    // independent of the push path.
    let rendered = manager
        .render_handle()
        .expect("Prometheus handle must survive an OTLP outage")
        .render();
    assert!(
        rendered.contains("otel_probe_before_outage"),
        "/metrics stopped serving during an OTLP outage:\n{rendered}"
    );
    counter!("otel_probe_still_alive").increment(1);

    // --- Phase 3: give it back, export resumes without intervention ---
    collector.unpause().await.expect("unpause collector");
    counter!("otel_probe_after_recovery").increment(1);
    assert!(
        wait_for_log(
            &collector,
            "otel_probe_after_recovery",
            Duration::from_secs(90)
        )
        .await,
        "export did not resume after the collector came back:\n{}",
        collector_log(&collector).await
    );
}
