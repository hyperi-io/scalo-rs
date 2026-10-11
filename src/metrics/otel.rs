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
    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .ok()
        .and_then(|p| match p.as_str() {
            "grpc" => Some(OtelProtocol::Grpc),
            "http/protobuf" | "http" => Some(OtelProtocol::Http),
            _ => None,
        })
        .unwrap_or(config.protocol);

    // Resolved after the protocol, as the default endpoint depends on it.
    let endpoint =
        super::otel_types::resolved_endpoint(crate::otel_backoff::endpoint_for_protocol(
            &config.endpoint,
            protocol == OtelProtocol::Http,
        ));

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
        // The builder POSTs to the endpoint verbatim, so the base URL needs the signal path.
        OtelProtocol::Http => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(crate::otel_backoff::append_signal_path(
                &resolved.endpoint,
                "v1/metrics",
            ))
            .with_timeout(resolved.export_timeout)
            .with_headers(resolved.headers.clone())
            .build()
            .map_err(|e| MetricsError::BuildError(format!("OTel HTTP exporter: {e}"))),
    }
}

/// The OTLP resource every exported metric carries: `service.name`,
/// `deployment.environment.name`, the replica's identity, and the consumer's
/// attributes, which win over all three.
///
/// `deployment.environment.name` is the stable semconv key; the bare
/// `deployment.environment` is deprecated.
fn resource(resolved: &ResolvedOtelConfig) -> opentelemetry_sdk::Resource {
    let mut builder = opentelemetry_sdk::Resource::builder();
    if !resolved.service_name.is_empty() {
        builder = builder.with_service_name(resolved.service_name.clone());
    }
    if !resolved.deployment_environment.is_empty() {
        builder = builder.with_attribute(opentelemetry::KeyValue::new(
            "deployment.environment.name",
            resolved.deployment_environment.clone(),
        ));
    }
    builder = builder.with_attributes(crate::otel_resource::instance_attributes());
    for (k, v) in &resolved.resource_attributes {
        builder = builder.with_attribute(opentelemetry::KeyValue::new(k.clone(), v.clone()));
    }
    builder.build()
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

    let resource = resource(&resolved);

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
                ("APP_ENV", Some("production")),
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
                    resolved.deployment_environment, "production",
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

    fn attribute(resource: &opentelemetry_sdk::Resource, key: &'static str) -> Option<String> {
        resource
            .get(&opentelemetry::Key::from_static_str(key))
            .map(|value| value.to_string())
    }

    /// Every replica pushing one series makes the pushed copy useless for rates.
    #[test]
    fn the_metrics_resource_names_the_replica() {
        temp_env::with_vars(
            [
                ("OTEL_RESOURCE_ATTRIBUTES", None::<&str>),
                ("POD_NAME", Some("loader-7d9f-abcde")),
                ("HOSTNAME", Some("node-host")),
            ],
            || {
                let resource = resource(&resolve_config(&OtelMetricsConfig::default()));
                assert_eq!(
                    attribute(&resource, "service.instance.id").as_deref(),
                    Some("loader-7d9f-abcde")
                );
                assert_eq!(
                    attribute(&resource, "k8s.pod.name").as_deref(),
                    Some("loader-7d9f-abcde")
                );
            },
        );
    }

    #[test]
    fn configured_resource_attributes_win_over_the_replica_identity() {
        temp_env::with_vars(
            [
                ("OTEL_RESOURCE_ATTRIBUTES", None::<&str>),
                ("POD_NAME", Some("loader-7d9f-abcde")),
            ],
            || {
                let mut cfg = OtelMetricsConfig::default();
                cfg.resource_attributes
                    .insert("service.instance.id".to_string(), "from-config".to_string());
                let resource = resource(&resolve_config(&cfg));
                assert_eq!(
                    attribute(&resource, "service.instance.id").as_deref(),
                    Some("from-config")
                );
            },
        );
    }

    #[test]
    fn the_default_endpoint_follows_the_protocol() {
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", None::<&str>),
            ],
            || {
                assert_eq!(
                    resolve_config(&OtelMetricsConfig::default()).endpoint,
                    "http://localhost:4317"
                );
                let http = OtelMetricsConfig {
                    protocol: OtelProtocol::Http,
                    ..OtelMetricsConfig::default()
                };
                assert_eq!(resolve_config(&http).endpoint, "http://localhost:4318");
            },
        );
    }

    #[test]
    fn the_protocol_env_var_moves_the_default_endpoint() {
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", Some("http/protobuf")),
            ],
            || {
                let resolved = resolve_config(&OtelMetricsConfig::default());
                assert_eq!(resolved.protocol, OtelProtocol::Http);
                assert_eq!(resolved.endpoint, "http://localhost:4318");
            },
        );
    }

    #[test]
    fn a_configured_or_env_endpoint_is_used_as_given() {
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", None::<&str>),
            ],
            || {
                let cfg = OtelMetricsConfig {
                    protocol: OtelProtocol::Http,
                    endpoint: "http://collector:4317".to_string(),
                    ..OtelMetricsConfig::default()
                };
                assert_eq!(resolve_config(&cfg).endpoint, "http://collector:4317");
            },
        );
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", Some("http://localhost:4317")),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", Some("http/protobuf")),
            ],
            || {
                let resolved = resolve_config(&OtelMetricsConfig::default());
                assert_eq!(resolved.endpoint, "http://localhost:4317");
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
