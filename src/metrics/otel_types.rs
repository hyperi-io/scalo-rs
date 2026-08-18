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
    /// Default endpoint for this protocol.
    #[must_use]
    pub fn default_endpoint(self) -> &'static str {
        match self {
            Self::Grpc => "http://localhost:4317",
            Self::Http => "http://localhost:4318",
        }
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

    /// OTLP endpoint (default: protocol-dependent).
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

    /// Additional OTLP headers, commonly a backend API key.
    ///
    /// Never serialised back out: this section is reachable through the admin
    /// `/config` endpoint, and the redaction there matches on field NAMES, so
    /// a token sitting in a map value would be printed in clear.
    #[serde(skip_serializing)]
    pub headers: HashMap<String, String>,

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
}

impl Default for OtelMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: OtelProtocol::Grpc.default_endpoint().to_string(),
            protocol: OtelProtocol::default(),
            service_name: String::new(),
            headers: HashMap::new(),
            resource_attributes: HashMap::new(),
            export_interval_secs: 60,
            export_timeout_secs: 10,
        }
    }
}

impl OtelMetricsConfig {
    /// Whether OTLP push should be wired up, after env-var resolution.
    ///
    /// False when [`enabled`](Self::enabled) is off or the effective endpoint
    /// is blank; callers install the Prometheus recorder alone.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.enabled && !resolved_endpoint(&self.endpoint).is_empty()
    }
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
