// Project:   scalo
// File:      tests/otel_tracing_export.rs
// Purpose:   Real-collector span export via the auto-wired logger
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Span export against a REAL OpenTelemetry Collector container.
//!
//! The subject is the AUTO-WIRE: an app calls `logger::setup` and nothing
//! else, and its `tracing` spans have to reach the collector as OTel spans.
//! Asserting on the collector's own record of what arrived is the only way to
//! prove the layer was actually composed into the subscriber -- a span that is
//! created but never exported looks identical from inside the process.
//!
//! `#[ignore]` because it needs a running Docker daemon:
//!
//! ```text
//! cargo test --features metrics,logger --test otel_tracing_export -- --ignored --nocapture
//! ```
//!
//! The global subscriber installs once per process, so this file owns one
//! `logger::setup` call.

#![cfg(all(feature = "otel-tracing", feature = "logger"))]

use std::time::Duration;

use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

// renovate: datasource=docker depName=otel/opentelemetry-collector
const COLLECTOR_TAG: &str = "0.162.0";

const OTLP_GRPC_PORT: u16 = 4317;

const PROBE_SPAN: &str = "scalo_probe_span";

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
    traces:
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

async fn collector_log(node: &ContainerAsync<GenericImage>) -> String {
    let out = node.stderr_to_vec().await.unwrap_or_default();
    String::from_utf8_lossy(&out).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Docker daemon; run with --ignored"]
async fn logger_setup_alone_exports_spans_to_the_collector() {
    let (collector, port) = start_collector().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    temp_env::async_with_vars(
        [
            ("OTEL_EXPORTER_OTLP_ENDPOINT", Some(endpoint.as_str())),
            // Sample everything: the default ratio is low on purpose, and one
            // probe span would usually fall outside it.
            ("OTEL_TRACES_SAMPLER_ARG", Some("1.0")),
        ],
        async {
            // The whole wiring an app is expected to do.
            scalo::logger::setup(scalo::logger::LoggerOptions {
                service_name: Some("scalo-span-export-test".to_string()),
                ..scalo::logger::LoggerOptions::default()
            })
            .expect("logger setup");

            tracing::info_span!(PROBE_SPAN).in_scope(|| {
                tracing::info!("inside the probe span");
            });

            // Batches leave on a scheduled delay, so give it several.
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            let mut arrived = false;
            while std::time::Instant::now() < deadline {
                if collector_log(&collector).await.contains(PROBE_SPAN) {
                    arrived = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(
                arrived,
                "collector never logged {PROBE_SPAN} -- the tracing layer was not wired in:\n{}",
                collector_log(&collector).await
            );

            // The resource has to carry the service name, or every service's
            // spans arrive indistinguishable.
            assert!(
                collector_log(&collector)
                    .await
                    .contains("scalo-span-export-test"),
                "exported spans carry no service.name"
            );
        },
    )
    .await;
}
