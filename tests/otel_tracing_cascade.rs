// Project:   scalo
// File:      tests/otel_tracing_cascade.rs
// Purpose:   Span export configured from the config cascade, not env vars
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Span export aimed by a settings file, proven against a REAL collector.
//!
//! The logger used to initialise before the config cascade existed, which left
//! `OTEL_EXPORTER_OTLP_ENDPOINT` as the only way to aim span export. This test
//! deliberately sets NO environment variable: the endpoint comes from
//! `settings.yaml` alone, so it fails if the ordering ever goes back.
//!
//! `#[ignore]` because it needs a running Docker daemon:
//!
//! ```text
//! cargo test --features metrics,logger --test otel_tracing_cascade -- --ignored --nocapture
//! ```
//!
//! Config and logger both install once per process, so this file owns one of
//! each.

#![cfg(all(feature = "otel-tracing", feature = "logger", feature = "config"))]

use std::time::Duration;

use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

// renovate: datasource=docker depName=otel/opentelemetry-collector
const COLLECTOR_TAG: &str = "0.162.0";

const OTLP_GRPC_PORT: u16 = 4317;

const PROBE_SPAN: &str = "scalo_cascade_probe_span";

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
async fn settings_yaml_aims_span_export_without_any_env_var() {
    let (collector, port) = start_collector().await;

    let dir = tempfile::tempdir().expect("config tempdir");
    std::fs::write(
        dir.path().join("settings.yaml"),
        format!(
            "otel_tracing:\n  \
               endpoint: http://127.0.0.1:{port}\n  \
               sample_ratio: 1.0\n"
        ),
    )
    .expect("write settings.yaml");

    // No OTEL_* variables: if the cascade is not consulted, export goes to the
    // default localhost:4317 and nothing reaches this collector.
    temp_env::async_with_vars(
        [
            ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", None::<&str>),
            ("OTEL_TRACES_SAMPLER_ARG", None::<&str>),
            ("OTEL_SERVICE_NAME", None::<&str>),
        ],
        async {
            // The order under test: config first, then the logger.
            scalo::config::setup(scalo::config::ConfigOptions {
                config_paths: vec![dir.path().to_path_buf()],
                ..scalo::config::ConfigOptions::default()
            })
            .expect("config setup");

            scalo::logger::setup(scalo::logger::LoggerOptions {
                service_name: Some("scalo-cascade-test".to_string()),
                ..scalo::logger::LoggerOptions::default()
            })
            .expect("logger setup");

            tracing::info_span!(PROBE_SPAN).in_scope(|| {
                tracing::info!("inside the cascade probe span");
            });

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
                "span never reached the collector named in settings.yaml -- \
                 the logger is being built before the config cascade:\n{}",
                collector_log(&collector).await
            );
        },
    )
    .await;
}
