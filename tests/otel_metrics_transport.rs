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
//! set by the manager's own refresh: with no server, nothing else sets them.
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

/// The last value `name` was exported with, as a float.
fn last_value(received: &[ExportMetricsServiceRequest], name: &str) -> Option<f64> {
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
        .next_back()
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
    metrics::counter!("transport_push_probe_total").increment(1);

    // A bound on a hang, not a timing assertion: exports land every second.
    let deadline = Instant::now() + Duration::from_secs(30);
    let (rss, probe) = loop {
        let (rss, probe) = {
            let received = received.lock().unwrap();
            (
                last_value(&received, "process_resident_memory_bytes"),
                last_value(&received, "transport_push_probe_total"),
            )
        };
        if (rss.is_some() && probe.is_some()) || Instant::now() > deadline {
            break (rss, probe);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(probe, Some(1.0), "the counter reached the collector");
    assert!(
        rss.is_some_and(|bytes| bytes > 0.0),
        "the process gauge was refreshed with no server: {rss:?}"
    );

    let received = received.lock().unwrap();
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
