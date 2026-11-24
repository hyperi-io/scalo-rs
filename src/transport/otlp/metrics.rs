// Project:   scalo
// File:      src/transport/otlp/metrics.rs
// Purpose:   OpenTelemetry SDK metric exporter over the OTLP transport
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

use super::OtlpTransport;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::types::SendResult;

/// An OpenTelemetry SDK push exporter that sends metrics through an
/// [`OtlpTransport`], on its connection, TLS, proxy and headers.
///
/// Each export is one `MetricsService/Export` call or `POST /v1/metrics`,
/// bounded by the transport's `timeout_ms`, carrying the resource the meter
/// provider gives it. Temporality is cumulative, so the next export that lands
/// carries what a failed one held, and a failed export is not retried. Shutting
/// the exporter down leaves the transport open for the logs it sends.
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
/// use scalo::transport::{OtlpConfig, OtlpMetricExporter, OtlpTransport};
///
/// let transport = Arc::new(OtlpTransport::new(&OtlpConfig::from_cascade()?)?);
/// let exporter = OtlpMetricExporter::new(Arc::clone(&transport))?;
/// let provider = SdkMeterProvider::builder()
///     .with_reader(PeriodicReader::builder(exporter).build())
///     .build();
/// ```
pub struct OtlpMetricExporter {
    transport: Arc<OtlpTransport>,
    /// The runtime each export runs on: the SDK's reader exports from a thread of its own.
    runtime: tokio::runtime::Handle,
    shut_down: AtomicBool,
}

impl OtlpMetricExporter {
    /// An exporter over `transport`, exporting on the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when built outside a Tokio runtime.
    pub fn new(transport: Arc<OtlpTransport>) -> TransportResult<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            TransportError::Config(
                "OTLP: a metric exporter is built inside a Tokio runtime".to_string(),
            )
        })?;
        Ok(Self {
            transport,
            runtime,
            shut_down: AtomicBool::new(false),
        })
    }
}

impl std::fmt::Debug for OtlpMetricExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpMetricExporter")
            .field("endpoint", &self.transport.endpoint)
            .field("shut_down", &self.shut_down.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl PushMetricExporter for OtlpMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        if self.shut_down.load(Ordering::Acquire) {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        let request = ExportMetricsServiceRequest::from(metrics);
        let transport = Arc::clone(&self.transport);
        let answered = self
            .runtime
            .spawn(async move { transport.export_metrics(request).await })
            .await
            .map_err(|e| OTelSdkError::InternalFailure(format!("OTLP metric export: {e}")))?;
        let failure = match answered {
            SendResult::Ok => return Ok(()),
            SendResult::Backpressured => {
                "the collector was unavailable or did not answer in time".to_string()
            }
            SendResult::RetryAfter(wait) => format!("the collector asked for a wait of {wait:?}"),
            SendResult::Reject(error) | SendResult::Fatal(error) => error.to_string(),
            SendResult::FilteredDlq => {
                unreachable!("invariant: no outbound filter runs on a metric export")
            }
        };
        Err(OTelSdkError::InternalFailure(format!(
            "OTLP metric export: {failure}"
        )))
    }

    /// Nothing is held between exports.
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    /// Refuse further exports. The transport stays open.
    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}
