// Project:   scalo
// File:      tests/otel_http_signal_path.rs
// Purpose:   OTLP/HTTP export reaches the per-signal path on a real listener
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OTLP/HTTP export against a real HTTP listener that records request paths.
//!
//! scalo's `endpoint` is a base URL, the way the OTel spec defines
//! `OTEL_EXPORTER_OTLP_ENDPOINT`, and each signal appends its own path to it.
//! The listener answers 404 everywhere else, as a collector does, so an export
//! that lands on the root fails exactly the way it would in production. The
//! assertion is the path that crossed the wire.
//!
//! The global metrics recorder installs once per process, so this file owns one
//! `MetricsManager`.

#![cfg(all(feature = "otel-tracing", feature = "otel-metrics"))]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use metrics::counter;
use opentelemetry::trace::{Tracer as _, TracerProvider as _};
use scalo::metrics::{MetricsConfig, MetricsManager, OtelMetricsConfig, OtelProtocol};
use scalo::otel_tracing::{OtelTracingConfig, OtelTracingProtocol, build_tracer_layer};

/// Env vars that would override the endpoint or protocol under test.
const OTEL_ENV: [(&str, Option<&str>); 4] = [
    ("OTEL_EXPORTER_OTLP_ENDPOINT", None),
    ("OTEL_EXPORTER_OTLP_PROTOCOL", None),
    ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None),
    ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", None),
];

/// The header the metrics export sends, whose value `Debug` must not show.
const API_KEY: (&str, &str) = ("x-api-key", "metrics-key-value");

/// Request paths the listener received, in arrival order, each with its
/// `API_KEY` header value.
type Paths = Arc<Mutex<Vec<(String, Option<String>)>>>;

async fn record(State(paths): State<Paths>, uri: Uri, headers: HeaderMap) -> StatusCode {
    let path = uri.path().to_string();
    let known = path == "/v1/traces" || path == "/v1/metrics";
    let key = headers
        .get(API_KEY.0)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    paths.lock().unwrap().push((path, key));
    if known {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// Bind, hold the listener, serve. The address is only handed out after the
/// server owns the socket.
async fn collector() -> (SocketAddr, Paths) {
    let paths: Paths = Arc::default();
    let app = Router::new()
        .fallback(record)
        .with_state(Arc::clone(&paths));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, paths)
}

fn received(paths: &Paths) -> Vec<String> {
    paths
        .lock()
        .unwrap()
        .iter()
        .map(|(path, _)| path.clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn http_span_export_posts_to_the_traces_path() {
    let (addr, paths) = collector().await;
    temp_env::async_with_vars(OTEL_ENV, async {
        let config = OtelTracingConfig {
            endpoint: format!("http://{addr}"),
            protocol: OtelTracingProtocol::Http,
            service_name: "scalo-http-path-test".to_string(),
            sample_ratio: 1.0,
            ..OtelTracingConfig::default()
        };
        let (_layer, provider) =
            build_tracer_layer::<tracing_subscriber::Registry>(&config).unwrap();
        provider.tracer("http-path-test").in_span("probe", |_| {});
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();
    })
    .await;

    assert_eq!(
        received(&paths),
        ["/v1/traces"],
        "a base endpoint must carry the traces signal path"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn http_metric_export_posts_to_the_metrics_path() {
    let (addr, paths) = collector().await;
    temp_env::async_with_vars(OTEL_ENV, async {
        let otel = OtelMetricsConfig {
            enabled: true,
            endpoint: format!("http://{addr}/"),
            protocol: OtelProtocol::Http,
            export_interval_secs: 1,
            export_timeout_secs: 2,
            service_name: "scalo-http-path-test".to_string(),
            headers: [(API_KEY.0.to_string(), API_KEY.1.into())]
                .into_iter()
                .collect(),
            ..OtelMetricsConfig::default()
        };
        assert!(
            !format!("{otel:?}").contains(API_KEY.1),
            "Debug redacts header values"
        );
        let mut manager = MetricsManager::with_config(MetricsConfig {
            namespace: String::new(),
            enable_process_metrics: false,
            enable_container_metrics: false,
            otel,
            ..MetricsConfig::default()
        });
        counter!("otel_http_path_probe").increment(1);
        // Shutdown runs the final collect and export.
        manager.shutdown_otel();
    })
    .await;

    let seen = received(&paths);
    assert!(!seen.is_empty(), "no metric export reached the listener");
    assert!(
        seen.iter().all(|path| path == "/v1/metrics"),
        "a base endpoint with a trailing slash must carry the metrics signal path, got {seen:?}"
    );
    assert!(
        paths
            .lock()
            .unwrap()
            .iter()
            .all(|(_, key)| key.as_deref() == Some(API_KEY.1)),
        "every export carries the real header value"
    );
}
