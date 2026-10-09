// Project:   scalo
// File:      src/metrics/otel.rs
// Purpose:   OTel MeterProvider setup and recorder installation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OpenTelemetry metrics setup.
//!
//! Configures an OTel `SdkMeterProvider` with an OTLP periodic exporter
//! via the `metrics-exporter-opentelemetry` crate. When both the `metrics`
//! (Prometheus) and `otel-metrics` features are enabled, a `Fanout` recorder
//! sends each measurement to both backends.

use std::time::Duration;

use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};

use super::MetricsError;
use super::otel_types::{OtelMetricsConfig, OtelProtocol};

/// Resolved OTLP configuration after applying env var overrides.
struct ResolvedOtelConfig {
    endpoint: String,
    protocol: OtelProtocol,
    export_interval: Duration,
    export_timeout: Duration,
    headers: std::collections::HashMap<String, String>,
    service_name: String,
    /// Deployment environment (dev/staging/prod), tagged on the OTel resource
    /// as the stable semconv attribute `deployment.environment.name`. Sourced
    /// from scalo's env detection ([`crate::env::get_app_env`]).
    deployment_environment: String,
    /// Consumer-configured extra resource attributes, applied LAST so they can
    /// override the auto-set ones (e.g. a custom `deployment.environment.name`).
    resource_attributes: std::collections::HashMap<String, String>,
}

/// Resolve OTel config with env var overrides.
fn resolve_config(config: &OtelMetricsConfig) -> ResolvedOtelConfig {
    let endpoint = super::otel_types::resolved_endpoint(&config.endpoint);

    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .ok()
        .and_then(|p| match p.as_str() {
            "grpc" => Some(OtelProtocol::Grpc),
            "http/protobuf" | "http" => Some(OtelProtocol::Http),
            _ => None,
        })
        .unwrap_or(config.protocol);

    let export_interval = std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or_else(
            || Duration::from_secs(config.export_interval_secs),
            Duration::from_millis,
        );

    let export_timeout = std::env::var("OTEL_METRIC_EXPORT_TIMEOUT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or_else(
            || Duration::from_secs(config.export_timeout_secs),
            Duration::from_millis,
        );

    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| config.service_name.clone());

    // Deployment environment for the OTel resource. Sourced from scalo's own
    // env detection (APP_ENV/ENVIRONMENT/ENV -> "development"), so traces and
    // metrics are tagged with the tier without per-app wiring. A consumer can
    // override via `resource_attributes["deployment.environment.name"]`.
    let deployment_environment = crate::env::get_app_env();

    ResolvedOtelConfig {
        endpoint,
        protocol,
        export_interval,
        export_timeout,
        headers: config.headers.clone(),
        service_name,
        deployment_environment,
        resource_attributes: config.resource_attributes.clone(),
    }
}

/// Build the OTLP metric exporter for the given protocol and endpoint.
///
/// The timeout is the per-attempt deadline, so an endpoint that accepts and
/// then stalls fails the attempt instead of holding the exporter open.
fn build_otlp_exporter(
    resolved: &ResolvedOtelConfig,
) -> Result<opentelemetry_otlp::MetricExporter, MetricsError> {
    match resolved.protocol {
        OtelProtocol::Grpc => {
            if !resolved.headers.is_empty() {
                tracing::warn!(
                    "metrics.otel.headers are only applied on the http protocol; \
                     set OTEL_EXPORTER_OTLP_HEADERS for grpc"
                );
            }
            opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&resolved.endpoint)
                .with_timeout(resolved.export_timeout)
                .build()
                .map_err(|e| MetricsError::BuildError(format!("OTel gRPC exporter: {e}")))
        }
        OtelProtocol::Http => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(&resolved.endpoint)
            .with_timeout(resolved.export_timeout)
            .with_headers(resolved.headers.clone())
            .build()
            .map_err(|e| MetricsError::BuildError(format!("OTel HTTP exporter: {e}"))),
    }
}

/// Build the OTel recorder (without installing it globally).
///
/// Uses `metrics-exporter-opentelemetry` to create a `metrics::Recorder`
/// backed by an OTel `SdkMeterProvider` with an OTLP periodic exporter.
///
/// The recorder is returned but NOT installed -- the caller decides whether
/// to install it directly or compose it with Prometheus via Fanout.
pub(crate) fn build_otel_recorder(
    scope_name: &str,
    config: &OtelMetricsConfig,
) -> Result<
    (
        metrics_exporter_opentelemetry::Recorder,
        opentelemetry_sdk::metrics::SdkMeterProvider,
    ),
    MetricsError,
> {
    let resolved = resolve_config(config);

    let exporter = build_otlp_exporter(&resolved)?;
    // Backoff starts at one export interval so the first retry is the next
    // scheduled tick, then doubles while the collector stays unreachable.
    let exporter =
        crate::otel_backoff::GatedMetricExporter::new(exporter, resolved.export_interval);
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(resolved.export_interval)
        .build();

    // Build resource: service.name + deployment.environment.name + any
    // consumer-configured attributes. `deployment.environment.name` is the
    // STABLE OTel semconv key (the bare `deployment.environment` is deprecated).
    let mut resource_builder = opentelemetry_sdk::Resource::builder();
    if !resolved.service_name.is_empty() {
        resource_builder = resource_builder.with_service_name(resolved.service_name);
    }
    if !resolved.deployment_environment.is_empty() {
        resource_builder = resource_builder.with_attribute(opentelemetry::KeyValue::new(
            "deployment.environment.name",
            resolved.deployment_environment,
        ));
    }
    // Consumer attributes applied LAST so they win over the auto-set ones.
    for (k, v) in resolved.resource_attributes {
        resource_builder = resource_builder.with_attribute(opentelemetry::KeyValue::new(k, v));
    }
    let resource = resource_builder.build();

    // Use with_meter_provider to attach our OTLP reader to the internal provider
    let reader_for_closure = reader;
    let resource_for_closure = resource;

    // build() returns (SdkMeterProvider, Recorder) -- provider first
    let (provider, recorder) =
        metrics_exporter_opentelemetry::Recorder::builder(scope_name.to_string())
            .with_meter_provider(move |mpb| {
                mpb.with_reader(reader_for_closure)
                    .with_resource(resource_for_closure)
            })
            .build();

    tracing::info!(
        endpoint = %resolved.endpoint,
        protocol = ?resolved.protocol,
        export_interval_secs = resolved.export_interval.as_secs(),
        export_timeout_secs = resolved.export_timeout.as_secs(),
        "OTLP metric push enabled"
    );

    Ok((recorder, provider))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deploy-env OTel: the deployment environment must be sourced from scalo's
    // env detection (APP_ENV) and configured resource_attributes must pass
    // through to the resolved config (they were previously dropped). Mirrors
    // the temp_env pattern used by env.rs tests.
    #[test]
    fn resolve_sets_deployment_environment_and_keeps_resource_attributes() {
        temp_env::with_vars(
            [
                ("APP_ENV", Some("staging")),
                ("ENVIRONMENT", None::<&str>),
                ("ENV", None::<&str>),
                ("OTEL_SERVICE_NAME", None::<&str>),
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
            ],
            || {
                let mut cfg = OtelMetricsConfig::default();
                cfg.resource_attributes
                    .insert("team".to_string(), "data-plane".to_string());
                let resolved = resolve_config(&cfg);
                assert_eq!(
                    resolved.deployment_environment, "staging",
                    "deployment environment must come from APP_ENV"
                );
                assert_eq!(
                    resolved.resource_attributes.get("team").map(String::as_str),
                    Some("data-plane"),
                    "configured resource attributes must survive resolution"
                );
            },
        );
    }

    #[test]
    fn resolve_deployment_environment_defaults_to_development() {
        temp_env::with_vars(
            [
                ("APP_ENV", None::<&str>),
                ("ENVIRONMENT", None::<&str>),
                ("ENV", None::<&str>),
            ],
            || {
                let resolved = resolve_config(&OtelMetricsConfig::default());
                assert_eq!(resolved.deployment_environment, "development");
            },
        );
    }
}
