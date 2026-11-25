// Project:   scalo
// File:      src/metrics/otel_types.rs
// Purpose:   Configuration types for OTel metrics backend
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration types for the OpenTelemetry metrics backend.
//!
//! These types extend `MetricsConfig` with OTel-specific options
//! when the `otel-metrics` feature is enabled.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// OTLP transport protocol.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtelProtocol {
    /// gRPC (default, port 4317)
    #[default]
    Grpc,
    /// HTTP/protobuf (port 4318)
    Http,
}

impl OtelProtocol {
    /// Default endpoint for this protocol: `http://localhost` with its port.
    #[must_use]
    pub fn default_endpoint(self) -> String {
        crate::otel_endpoint::endpoint_for_protocol(
            crate::otel_endpoint::DEFAULT_ENDPOINT,
            self == Self::Http,
        )
        .into_owned()
    }
}

/// OTel-specific configuration for the metrics backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtelMetricsConfig {
    /// Master switch for OTLP push, on by default.
    ///
    /// Set `false`, or blank the [`endpoint`](Self::endpoint), to export
    /// nothing. Both routes leave the Prometheus scrape endpoint untouched.
    pub enabled: bool,

    /// OTLP endpoint, `http://localhost` by default. An `http://` endpoint
    /// naming no port takes 4317 under gRPC and 4318 under HTTP. A named port,
    /// or `https://` naming none (443), is used as given.
    ///
    /// Override via `OTEL_EXPORTER_OTLP_ENDPOINT` env var. An empty endpoint
    /// disables export -- it is the "off" value for an operator who cannot
    /// reach the `enabled` flag.
    pub endpoint: String,

    /// OTLP transport protocol.
    ///
    /// Override via `OTEL_EXPORTER_OTLP_PROTOCOL` env var.
    pub protocol: OtelProtocol,

    /// Service name reported in OTel resource.
    ///
    /// Override via `OTEL_SERVICE_NAME` env var.
    pub service_name: String,

    /// Additional OTLP headers, commonly a backend API key. Each value is a
    /// [`SensitiveString`](crate::sensitive::SensitiveString), so `Debug` and
    /// logs show it redacted, and the map is never serialised back out.
    #[serde(skip_serializing)]
    pub headers: HashMap<String, crate::sensitive::SensitiveString>,

    /// Additional resource attributes.
    pub resource_attributes: HashMap<String, String>,

    /// Metric export interval in seconds (default: 60).
    ///
    /// Override via `OTEL_METRIC_EXPORT_INTERVAL` env var (in milliseconds).
    pub export_interval_secs: u64,

    /// Per-export deadline in seconds (default: 10).
    ///
    /// Bounds one export attempt against an endpoint that accepts the
    /// connection and never answers. Without it a black-holed collector holds
    /// the exporter open until the next interval and the attempts overlap.
    pub export_timeout_secs: u64,

    /// Push through the OTLP transport that `transport.otlp` configures, on
    /// its connection, TLS, proxy and headers, instead of
    /// [`endpoint`](Self::endpoint), [`protocol`](Self::protocol),
    /// [`headers`](Self::headers) and the export timeout, which are then
    /// unused. Off by default. Needs scalo's `transport-otlp` feature: without
    /// it push stays off.
    pub via_transport: bool,
}

impl Default for OtelMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: crate::otel_endpoint::DEFAULT_ENDPOINT.to_string(),
            protocol: OtelProtocol::default(),
            service_name: String::new(),
            headers: HashMap::new(),
            resource_attributes: HashMap::new(),
            export_interval_secs: 60,
            export_timeout_secs: 10,
            via_transport: false,
        }
    }
}

impl OtelMetricsConfig {
    /// Whether OTLP push should be wired up, after env-var resolution.
    ///
    /// False when [`enabled`](Self::enabled) is off, there is nowhere to push
    /// (a blank effective endpoint, or [`via_transport`](Self::via_transport)
    /// without the `transport-otlp` feature), or there is no Tokio runtime to
    /// export on. Callers install the Prometheus recorder alone.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.enabled && self.has_destination() && runtime_available()
    }

    /// Whether there is somewhere to push: the transport, or a non-blank endpoint.
    fn has_destination(&self) -> bool {
        if self.via_transport {
            cfg!(feature = "transport-otlp")
        } else {
            !resolved_endpoint(&self.endpoint).is_empty()
        }
    }
}

/// Why export is off, for the log line that says so.
pub(crate) fn inactive_reason(config: &OtelMetricsConfig) -> &'static str {
    if !config.enabled {
        "metrics.otel.enabled = false"
    } else if config.via_transport && !config.has_destination() {
        "metrics.otel.via_transport needs scalo's transport-otlp feature"
    } else if !config.has_destination() {
        "endpoint is blank"
    } else if !runtime_available() {
        "no Tokio runtime on this thread"
    } else {
        "active"
    }
}

/// Whether a Tokio runtime is available to the exporter.
///
/// The OTLP exporter builds a hyper connector, which panics with "there is no
/// reactor running" when constructed outside one. A `MetricsManager` built in
/// a plain `#[test]` or any synchronous path would take the whole process
/// down, so export is skipped instead -- it could not have worked there.
pub(crate) fn runtime_available() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

/// The endpoint actually used, with the OTel env var taking precedence.
///
/// Whitespace-only values resolve to empty so `OTEL_EXPORTER_OTLP_ENDPOINT=" "`
/// reads as "off" rather than as an unparseable URI.
pub(crate) fn resolved_endpoint(configured: &str) -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| configured.to_string())
        .trim()
        .to_string()
}
