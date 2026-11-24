// Project:   scalo
// File:      tests/otel_metrics_transport.rs
// Purpose:   OTLP metric push through the OTLP transport, with no /metrics server
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `MetricsManager::with_otlp_transport` pushes through the OTLP transport to
//! an in-process gRPC collector, the SDK's reader exporting from its own
//! thread, with no `/metrics` server started. The process gauges it pushes are
//! set by the manager's own refresh: with no server, nothing else sets them. A
//! counter set with `absolute` exports the value set, not a running sum of them.
//!
//! The global metrics recorder installs once per process, so this file owns
//! one `MetricsManager`.

#![cfg(all(feature = "otel-metrics", feature = "transport-otlp"))]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as Number;
use scalo::metrics::{MetricsConfig, MetricsManager, OtelMetricsConfig};
use scalo::transport::{OtlpConfig, OtlpTransport};

type Received = Arc<Mutex<Vec<ExportMetricsServiceRequest>>>;

/// Takes every metric export and keeps it.
struct Collector(Received);

#[tonic::async_trait]
impl MetricsService for Collector {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.0.lock().unwrap().push(request.into_inner());
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

/// Every value `name` was exported with, in order, as floats.
fn values(received: &[ExportMetricsServiceRequest], name: &str) -> Vec<f64> {
    received
        .iter()
        .flat_map(|request| &request.resource_metrics)
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .filter(|metric| metric.name == name)
        .filter_map(|metric| match &metric.data {
            Some(Data::Gauge(gauge)) => gauge.data_points.last(),
            Some(Data::Sum(sum)) => sum.data_points.last(),
            _ => None,
        })
        .filter_map(|point| point.value)
        .map(|value| match value {
            Number::AsDouble(v) => v,
            #[allow(clippy::cast_precision_loss, reason = "test values are small")]
            Number::AsInt(v) => v as f64,
        })
        .collect()
}

/// The last value `name` was exported with.
fn last_value(received: &[ExportMetricsServiceRequest], name: &str) -> Option<f64> {
    values(received, name).last().copied()
}

/// Wait for the collector to have `name` at `want`, or the deadline: a bound
/// on a hang, not a timing assertion, since exports land every second.
async fn wait_for(received: &Received, name: &str, want: impl Fn(f64) -> bool) -> Option<f64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let last = last_value(&received.lock().unwrap(), name);
        if last.is_some_and(&want) || Instant::now() > deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The resource attribute `key` on the first export, as text.
fn attribute(received: &[ExportMetricsServiceRequest], key: &str) -> Option<String> {
    received
        .first()?
        .resource_metrics
        .first()?
        .resource
        .as_ref()?
        .attributes
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.clone()?.value)
        .and_then(|value| match value {
            Value::StringValue(text) => Some(text),
            _ => None,
        })
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_push_through_the_transport_with_no_server() {
    let received = Received::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(Collector(Arc::clone(&received))))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );

    let transport =
        Arc::new(OtlpTransport::new(&OtlpConfig::grpc(&format!("http://{addr}"))).unwrap());
    let manager = MetricsManager::with_otlp_transport(
        MetricsConfig {
            enable_process_metrics: true,
            enable_container_metrics: false,
            update_interval: Duration::from_millis(200),
            otel: OtelMetricsConfig {
                service_name: "scalo-transport-push".to_string(),
                export_interval_secs: 1,
                ..OtelMetricsConfig::default()
            },
            ..MetricsConfig::default()
        },
        Arc::clone(&transport),
    );
    let absolute = metrics::counter!("transport_push_absolute_total");
    absolute.absolute(10);

    let rss = wait_for(&received, "process_resident_memory_bytes", |bytes| {
        bytes > 0.0
    })
    .await;
    assert!(
        rss.is_some_and(|bytes| bytes > 0.0),
        "the process gauge was refreshed with no server: {rss:?}"
    );
    let first = wait_for(&received, "transport_push_absolute_total", |v| {
        (v - 10.0).abs() < f64::EPSILON
    })
    .await;
    assert_eq!(
        first,
        Some(10.0),
        "the absolute counter reached the collector"
    );
    absolute.absolute(25);
    let second = wait_for(&received, "transport_push_absolute_total", |v| {
        (v - 25.0).abs() < f64::EPSILON
    })
    .await;
    assert_eq!(second, Some(25.0), "the second absolute value, not 10 + 25");

    let received = received.lock().unwrap();
    assert!(
        values(&received, "transport_push_absolute_total")
            .iter()
            .all(|v| [10.0, 25.0].contains(v)),
        "no export summed the absolute values"
    );
    assert_eq!(
        attribute(&received, "service.name").as_deref(),
        Some("scalo-transport-push"),
        "the metrics resource, not the transport's"
    );
    for key in ["service.instance.id", "os.type", "host.name"] {
        assert!(attribute(&received, key).is_some(), "{key} on the resource");
    }
    drop(manager);
}
