// Project:   scalo
// File:      src/otel_tracing/mod.rs
// Purpose:   OpenTelemetry trace span exporter (OTLP) + tracing-subscriber bridge
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OpenTelemetry distributed tracing -- span export via OTLP.
//!
//! Bridges the `tracing` ecosystem (`tracing::span!`, `#[instrument]`,
//! `tracing::info_span!`) to OpenTelemetry spans that get exported via
//! OTLP to a collector or backend (Tempo, Jaeger, Honeycomb, etc.).
//!
//! Closes the loop on the framework's W3C traceparent propagation:
//! [`crate::transport::propagation`] reads the current OTel context
//! (set externally) and propagates it across transport boundaries.
//! Without this module wired up, internal `tracing::span!`s never become
//! OTel spans, leaving distributed traces with broken segments.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use scalo::otel_tracing::{OtelTracingConfig, build_tracer_layer};
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::util::SubscriberInitExt;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = OtelTracingConfig {
//!     service_name: "dfe-loader".into(),
//!     endpoint: "http://otel-collector:4317".into(),
//!     ..Default::default()
//! };
//! let (otel_layer, _provider) = build_tracer_layer(&config)?;
//!
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::fmt::layer())
//!     .with(otel_layer)
//!     .init();
//!
//! tracing::info_span!("startup").in_scope(|| {
//!     tracing::info!("application booted");
//! });
//! # Ok(())
//! # }
//! ```
//!
//! # Why a separate module from `otel-metrics`
//!
//! Metrics and traces have independent lifecycles, samplers, and exporters.
//! Mixing them under one feature gate forced consumers who only want one
//! to pull in the other. They share `OtelProtocol` and the OTLP endpoint
//! discipline but otherwise operate independently.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Layer as _;

/// OTLP transport protocol (mirrors [`crate::metrics::OtelProtocol`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OtelTracingProtocol {
    /// gRPC with tonic (default; OTLP-native, lowest overhead).
    #[default]
    Grpc,
    /// HTTP with protobuf body.
    Http,
}

/// OpenTelemetry tracing configuration.
///
/// Resolves env-var overrides at build time:
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` overrides `endpoint`
/// - `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc` | `http/protobuf` | `http`) overrides `protocol`
/// - `OTEL_SERVICE_NAME` overrides `service_name`
/// - `OTEL_TRACES_SAMPLER_ARG` overrides `sample_ratio`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtelTracingConfig {
    /// Master switch for span export, on by default.
    ///
    /// Set `false`, or blank the [`endpoint`](Self::endpoint), to export
    /// nothing. Either way the `tracing` logger is untouched.
    pub enabled: bool,
    /// OTLP endpoint (default `http://localhost:4317` for gRPC).
    pub endpoint: String,
    /// Wire protocol.
    pub protocol: OtelTracingProtocol,
    /// `service.name` resource attribute.
    pub service_name: String,
    /// Fraction of new traces to sample, 0.0 to 1.0.
    ///
    /// Applied under a parent-based sampler, so a request already sampled
    /// upstream is always kept and distributed traces stay whole. The default
    /// is well below 1.0 because a data-plane service creates spans at request
    /// rate and exporting all of them costs more than the traces are worth.
    pub sample_ratio: f64,
    /// Batch exporter scheduled-delay (milliseconds).
    pub batch_scheduled_delay_ms: u64,
    /// Batch exporter max queue size.
    ///
    /// The queue is the memory ceiling for un-exported spans: once full, new
    /// spans are dropped rather than buffered, so an unreachable collector
    /// costs spans instead of growing without bound.
    pub batch_max_queue_size: usize,
    /// Maximum spans per export request.
    pub batch_max_export_batch_size: usize,
    /// Per-export deadline (milliseconds).
    pub export_timeout_ms: u64,
}

impl Default for OtelTracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "http://localhost:4317".into(),
            protocol: OtelTracingProtocol::Grpc,
            service_name: String::new(),
            sample_ratio: 0.05,
            batch_scheduled_delay_ms: 5_000,
            batch_max_queue_size: 2_048,
            batch_max_export_batch_size: 512,
            export_timeout_ms: 10_000,
        }
    }
}

impl OtelTracingConfig {
    /// Load from the config cascade under the `otel_tracing` key.
    ///
    /// Falls back to defaults when config is not initialised or the key is
    /// absent.
    #[must_use]
    pub fn from_cascade() -> Self {
        #[cfg(feature = "config")]
        {
            if let Some(cfg) = crate::config::try_get()
                && let Ok(settings) = cfg.unmarshal_key_registered::<Self>("otel_tracing")
            {
                return settings;
            }
        }
        Self::default()
    }

    /// Whether span export should be wired up, after env-var resolution.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.enabled && !resolve(self).endpoint.is_empty()
    }
}

/// Errors when building the OTel tracer.
#[derive(Debug, thiserror::Error)]
pub enum OtelTracingError {
    /// OTLP exporter construction failed.
    #[error("OTLP {protocol:?} span exporter: {source}")]
    ExporterBuild {
        /// The protocol attempted.
        protocol: OtelTracingProtocol,
        /// Underlying error.
        source: opentelemetry_otlp::ExporterBuildError,
    },
}

fn resolve(config: &OtelTracingConfig) -> OtelTracingConfig {
    let mut resolved = config.clone();
    if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        resolved.endpoint = v;
    }
    resolved.endpoint = resolved.endpoint.trim().to_string();
    if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL") {
        resolved.protocol = match v.as_str() {
            "http/protobuf" | "http" => OtelTracingProtocol::Http,
            _ => OtelTracingProtocol::Grpc,
        };
    }
    if let Ok(v) = std::env::var("OTEL_SERVICE_NAME") {
        resolved.service_name = v;
    }
    // The spec's sampler knob, so an operator can turn sampling up on a
    // service without a config change.
    if let Ok(v) = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        && let Ok(ratio) = v.parse::<f64>()
    {
        resolved.sample_ratio = ratio;
    }
    resolved
}

fn build_span_exporter(
    config: &OtelTracingConfig,
) -> Result<opentelemetry_otlp::SpanExporter, OtelTracingError> {
    let timeout = std::time::Duration::from_millis(config.export_timeout_ms);
    let result = match config.protocol {
        OtelTracingProtocol::Grpc => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .with_timeout(timeout)
            .build(),
        OtelTracingProtocol::Http => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(&config.endpoint)
            .with_timeout(timeout)
            .build(),
    };
    result.map_err(|source| OtelTracingError::ExporterBuild {
        protocol: config.protocol,
        source,
    })
}

/// Build an OTel tracer + tracing-subscriber layer ready for composition.
///
/// Sets the resulting [`SdkTracerProvider`] as the **global** tracer
/// provider (so [`crate::transport::propagation`] picks it up). The
/// returned layer should be added to a `tracing_subscriber::Registry`.
///
/// The provider is also returned so callers can `.shutdown()` it on
/// graceful exit (otherwise the batch exporter loses queued spans).
///
/// # Errors
///
/// Returns [`OtelTracingError::ExporterBuild`] if the OTLP exporter
/// cannot be initialised (typically endpoint format / TLS setup issues).
pub fn build_tracer_layer<S>(
    config: &OtelTracingConfig,
) -> Result<
    (
        OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
        SdkTracerProvider,
    ),
    OtelTracingError,
>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let resolved = resolve(config);

    let exporter = build_span_exporter(&resolved)?;

    let resource = Resource::builder()
        .with_service_name(resolved.service_name.clone())
        .build();

    // Bounded queue: spans are dropped once it fills, so an unreachable
    // collector costs telemetry and never memory.
    let batch_config = opentelemetry_sdk::trace::BatchConfigBuilder::default()
        .with_max_queue_size(resolved.batch_max_queue_size)
        .with_max_export_batch_size(resolved.batch_max_export_batch_size)
        .with_scheduled_delay(std::time::Duration::from_millis(
            resolved.batch_scheduled_delay_ms,
        ))
        .build();
    // Backoff starts at one scheduled delay so the first retry is the next
    // batch, then doubles while the collector stays unreachable.
    let exporter = crate::otel_backoff::GatedSpanExporter::new(
        exporter,
        std::time::Duration::from_millis(resolved.batch_scheduled_delay_ms),
    );
    let processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();

    // Parent-based so an upstream sampling decision is honoured; the ratio
    // only governs traces that start here.
    let sampler = opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
        opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(resolved.sample_ratio),
    ));

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("scalo");

    // Install as global so propagation.rs picks up the active context.
    opentelemetry::global::set_tracer_provider(provider.clone());

    let layer = tracing_opentelemetry::layer().with_tracer(tracer);

    Ok((layer, provider))
}

/// Crates on the export path itself, whose spans must never be exported.
///
/// Sending a batch over OTLP runs through tonic, hyper and h2, all of which
/// emit `tracing` spans. Feeding those to the exporter makes every export
/// generate the spans for the next one, and the queue climbs until it is
/// dropping data -- worse the harder the collector is to reach.
const SELF_TELEMETRY_TARGETS: [&str; 8] = [
    "opentelemetry",
    "opentelemetry_sdk",
    "h2",
    "hyper",
    "hyper_util",
    "tonic",
    "tower",
    "reqwest",
];

/// Whether an event comes from the export path.
///
/// Matches a target exactly or as a module prefix (`hyper::client::...`),
/// never a bare `starts_with`, which would also swallow an app's own
/// `hyperion` target.
fn is_self_telemetry(target: &str) -> bool {
    SELF_TELEMETRY_TARGETS.iter().any(|crate_name| {
        target
            .strip_prefix(crate_name)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
    })
}

/// Filter keeping the export path's own spans out of the exporter.
///
/// Applied to the OTel layer alone, so these targets still reach the console
/// logger when `RUST_LOG` asks for them.
type SelfTelemetryFilter = tracing_subscriber::filter::FilterFn<fn(&tracing::Metadata<'_>) -> bool>;

fn keep_out_of_export(meta: &tracing::Metadata<'_>) -> bool {
    !is_self_telemetry(meta.target())
}

fn self_telemetry_filter() -> SelfTelemetryFilter {
    tracing_subscriber::filter::FilterFn::new(
        keep_out_of_export as fn(&tracing::Metadata<'_>) -> bool,
    )
}

/// Tracer provider retained for the flush on shutdown.
static TRACER_PROVIDER: std::sync::OnceLock<SdkTracerProvider> = std::sync::OnceLock::new();

/// What happened during [`layer_if_active`], reported once the logger exists.
static INIT_STATUS: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Build the span-export layer when config asks for it.
///
/// Returns `None` when export is switched off or the exporter cannot be
/// built -- a collector that is missing or misconfigured degrades telemetry
/// and never stops the service starting. The provider is retained for
/// [`shutdown`].
///
/// Called during logger setup, before any subscriber exists, so the outcome
/// is recorded for [`log_init_status`] rather than logged here.
pub fn layer_if_active<S>(
    config: &OtelTracingConfig,
) -> Option<
    tracing_subscriber::filter::Filtered<
        OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
        SelfTelemetryFilter,
        S,
    >,
>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if !config.is_active() {
        let _ = INIT_STATUS.set(
            "OTLP span export disabled (otel_tracing.enabled=false or blank endpoint)".to_string(),
        );
        return None;
    }

    match build_tracer_layer(config) {
        Ok((layer, provider)) => {
            let _ = TRACER_PROVIDER.set(provider);
            let resolved = resolve(config);
            let _ = INIT_STATUS.set(format!(
                "OTLP span export enabled -> {} (sample_ratio {}, max_queue {})",
                resolved.endpoint, resolved.sample_ratio, resolved.batch_max_queue_size
            ));
            Some(layer.with_filter(self_telemetry_filter()))
        }
        Err(e) => {
            let _ = INIT_STATUS.set(format!(
                "OTLP span export unavailable, continuing without it: {e}"
            ));
            None
        }
    }
}

/// Emit whatever [`layer_if_active`] decided, now that a subscriber exists.
pub fn log_init_status() {
    if let Some(status) = INIT_STATUS.get() {
        tracing::info!("{status}");
    }
}

/// Flush and stop the tracer provider, if one was built.
///
/// Queued spans are dropped rather than waited on when the collector is
/// unreachable, so this cannot hold up a shutdown.
pub fn shutdown() {
    if let Some(provider) = TRACER_PROVIDER.get()
        && let Err(e) = provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT)
    {
        tracing::debug!(error = %e, "OTel tracer provider shutdown");
    }
}

/// Flush deadline on exit, short enough to stay well inside a Kubernetes
/// termination grace period when the collector is unreachable.
const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_round_trip() {
        let cfg = OtelTracingConfig::default();
        assert_eq!(cfg.protocol, OtelTracingProtocol::Grpc);
        assert!(!cfg.endpoint.is_empty());
        assert!(cfg.enabled, "span export is on by default");
        // Empty on purpose: the caller supplies the app's name. A crate-name
        // default would report every service in the fleet as "scalo".
        assert!(cfg.service_name.is_empty());
    }

    #[test]
    fn export_is_off_when_disabled_or_unaddressed() {
        assert!(OtelTracingConfig::default().is_active());
        assert!(
            !OtelTracingConfig {
                enabled: false,
                ..OtelTracingConfig::default()
            }
            .is_active(),
            "enabled=false must switch it off"
        );
        assert!(
            !OtelTracingConfig {
                endpoint: "  ".to_string(),
                ..OtelTracingConfig::default()
            }
            .is_active(),
            "a blank endpoint must switch it off"
        );
    }

    #[test]
    fn batch_defaults_bound_what_can_be_held() {
        let cfg = OtelTracingConfig::default();
        assert!(
            cfg.batch_max_queue_size > 0,
            "an unbounded queue would let a dead collector grow memory"
        );
        assert!(
            cfg.batch_max_export_batch_size <= cfg.batch_max_queue_size,
            "the SDK rejects a batch size above the queue size"
        );
        assert!(
            cfg.sample_ratio > 0.0 && cfg.sample_ratio <= 1.0,
            "sample ratio {} is outside 0..1",
            cfg.sample_ratio
        );
    }

    #[test]
    fn the_export_path_is_kept_out_of_the_export() {
        for target in [
            "hyper",
            "hyper::client::conn",
            "h2::codec",
            "tonic::transport::channel",
            "opentelemetry_sdk::trace::span_processor",
            "tower::buffer",
        ] {
            assert!(
                is_self_telemetry(target),
                "{target} is on the export path and must not be exported"
            );
        }
    }

    #[test]
    fn app_targets_that_merely_share_a_prefix_are_still_exported() {
        // A bare `starts_with` would swallow all of these.
        for target in [
            "hyperion",
            "hyperi_thing::worker",
            "towerbridge",
            "h2o",
            "reqwest_middleware_of_ours",
            "dfe_receiver::ingest",
        ] {
            assert!(
                !is_self_telemetry(target),
                "{target} is application telemetry and must still be exported"
            );
        }
    }

    #[test]
    fn the_sampler_arg_env_var_overrides_the_ratio() {
        temp_env::with_var("OTEL_TRACES_SAMPLER_ARG", Some("1.0"), || {
            let r = resolve(&OtelTracingConfig::default());
            assert!(
                (r.sample_ratio - 1.0).abs() < f64::EPSILON,
                "OTEL_TRACES_SAMPLER_ARG must win over the configured ratio"
            );
        });
    }

    #[test]
    fn resolve_picks_up_env_overrides() {
        // SAFETY: temp_env handles cleanup; env mutations are scoped.
        temp_env::with_vars(
            [
                (
                    "OTEL_EXPORTER_OTLP_ENDPOINT",
                    Some("http://my-collector:4317"),
                ),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", Some("http/protobuf")),
                ("OTEL_SERVICE_NAME", Some("test-service")),
            ],
            || {
                let r = resolve(&OtelTracingConfig::default());
                assert_eq!(r.endpoint, "http://my-collector:4317");
                assert_eq!(r.protocol, OtelTracingProtocol::Http);
                assert_eq!(r.service_name, "test-service");
            },
        );
    }
}
