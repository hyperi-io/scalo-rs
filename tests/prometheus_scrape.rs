// Project:   scalo
// File:      tests/prometheus_scrape.rs
// Purpose:   Real-Prometheus scrape test against the metrics endpoint
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Prometheus half of "good citizen by default", proven by a REAL
//! Prometheus container scraping a REAL metrics server.
//!
//! The assertion is what Prometheus STORED, read back through its own query
//! API -- not what scalo rendered. A scrape endpoint that renders correctly
//! but cannot be scraped (wrong bind address, wrong content type, malformed
//! exposition) passes the render check and fails this one.
//!
//! `#[ignore]` because it needs a running Docker daemon:
//!
//! ```text
//! cargo test --features metrics --test prometheus_scrape -- --ignored --nocapture
//! ```

#![cfg(feature = "metrics")]

use std::time::Duration;

use metrics::counter;
use scalo::metrics::{MetricsConfig, MetricsManager};
use testcontainers_modules::testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Prometheus to test against.
///
/// Pinned in our own source so dependency review can see it.
// renovate: datasource=docker depName=prom/prometheus
const PROMETHEUS_TAG: &str = "v3.15.0";

const PROMETHEUS_PORT: u16 = 9090;

/// The series the test asserts on.
const PROBE_METRIC: &str = "prom_scrape_probe";

/// A free port, found by binding and letting go.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Scrape config aimed back at the test process across the container boundary.
fn prometheus_config(target_port: u16) -> String {
    format!(
        "global:\n  \
           scrape_interval: 1s\n\
         scrape_configs:\n  \
           - job_name: scalo\n    \
             static_configs:\n      \
               - targets: ['host.docker.internal:{target_port}']\n"
    )
}

async fn start_prometheus(target_port: u16) -> (ContainerAsync<GenericImage>, u16) {
    let node = GenericImage::new("prom/prometheus", PROMETHEUS_TAG)
        .with_exposed_port(ContainerPort::Tcp(PROMETHEUS_PORT))
        .with_wait_for(WaitFor::message_on_stderr("Server is ready to receive"))
        // The scrape target is the test process on the host, which is only
        // addressable from inside the container through the gateway alias.
        .with_host("host.docker.internal", Host::HostGateway)
        .with_cmd([
            "--config.file=/etc/prometheus/generated.yml",
            "--storage.tsdb.retention.time=1h",
        ])
        .with_copy_to(
            "/etc/prometheus/generated.yml",
            prometheus_config(target_port).into_bytes(),
        )
        .start()
        .await
        .expect("start prometheus container");
    let port = node
        .get_host_port_ipv4(PROMETHEUS_PORT)
        .await
        .expect("prometheus host port");
    (node, port)
}

/// Ask Prometheus whether it holds the series yet.
async fn query_has_result(client: &reqwest::Client, prom_port: u16, query: &str) -> bool {
    let url = format!("http://127.0.0.1:{prom_port}/api/v1/query?query={query}");
    let Ok(resp) = client.get(&url).send().await else {
        return false;
    };
    let Ok(body) = resp.json::<serde_json::Value>().await else {
        return false;
    };
    body["data"]["result"]
        .as_array()
        .is_some_and(|r| !r.is_empty())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Docker daemon; run with --ignored"]
async fn prometheus_scrapes_the_metrics_endpoint() {
    let metrics_port = free_port();

    let mut manager = MetricsManager::with_config(MetricsConfig {
        namespace: String::new(),
        enable_process_metrics: false,
        enable_container_metrics: false,
        // Scrape is the subject here; push has its own test.
        #[cfg(feature = "otel-metrics")]
        otel: scalo::metrics::OtelMetricsConfig {
            enabled: false,
            ..scalo::metrics::OtelMetricsConfig::default()
        },
        ..MetricsConfig::default()
    });

    // Bound to all interfaces: the scraper reaches in from a container, so
    // a loopback-only endpoint would render fine and never be scraped.
    manager
        .start_server(&format!("0.0.0.0:{metrics_port}"))
        .await
        .expect("start metrics server");

    counter!(PROBE_METRIC).increment(7);

    let (_prometheus, prom_port) = start_prometheus(metrics_port).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build http client");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut stored = false;
    while std::time::Instant::now() < deadline {
        if query_has_result(&client, prom_port, PROBE_METRIC).await {
            stored = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        stored,
        "Prometheus never stored {PROBE_METRIC} -- the endpoint was not scrapeable"
    );

    // The target itself must be healthy, not merely reachable: a scrape that
    // errors still leaves the series absent for the reason above.
    assert!(
        query_has_result(&client, prom_port, "up{job=\"scalo\"}").await,
        "Prometheus has no `up` sample for the scalo job"
    );
}
