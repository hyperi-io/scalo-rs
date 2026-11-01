// Project:   scalo
// File:      src/cascade.rs
// Purpose:   Read one config section from the cascade, or its defaults
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Read one config section from the cascade, or its defaults.
//!
//! Every module's `from_cascade()` reads through [`section`]: an absent
//! section, or a cascade that was never set up, takes the type's defaults,
//! and a present section that does not deserialise is a [`CascadeError`]
//! naming the key. [`check_runtime_sections`] reads every section scalo's own
//! runtime reads, so `run_app` refuses a bad one at startup.

/// A config section that is present in the cascade but does not deserialise.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("config section `{key}` does not parse: {reason}")]
pub struct CascadeError {
    /// The cascade key of the section.
    pub key: String,
    /// Why it does not deserialise.
    pub reason: String,
}

/// The section at `key`, registered in the config registry, or `T::default()`
/// when it is absent or the cascade is not set up.
///
/// # Errors
///
/// [`CascadeError`] when the section is present but does not deserialise.
#[cfg(feature = "config")]
pub fn section<T>(key: &str) -> Result<T, CascadeError>
where
    T: serde::de::DeserializeOwned + serde::Serialize + Default + 'static,
{
    match crate::config::try_get() {
        Some(cfg) if cfg.contains(key) => {
            cfg.unmarshal_key_registered::<T>(key)
                .map_err(|e| CascadeError {
                    key: key.to_string(),
                    reason: e.to_string(),
                })
        }
        _ => Ok(T::default()),
    }
}

/// Without the `config` feature there is no cascade: always the defaults.
///
/// # Errors
///
/// None.
#[cfg(not(feature = "config"))]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the same signature as the config-feature build"
)]
pub fn section<T: Default>(key: &str) -> Result<T, CascadeError> {
    let _ = key;
    Ok(T::default())
}

/// Read every section scalo's runtime reads under the enabled features, and
/// return every one that does not parse.
///
/// The logger, the CLI's level and format resolution and the metrics manifest
/// read their sections where no error can be returned, so `run_app` runs this
/// once the cascade is set up and refuses to start on a bad section.
#[must_use]
pub fn check_runtime_sections() -> Vec<CascadeError> {
    let checks: &[fn() -> Result<(), CascadeError>] = &[
        #[cfg(feature = "logger")]
        || crate::logger::LoggerSettings::from_cascade().map(drop),
        #[cfg(feature = "metrics")]
        || crate::metrics::MetricsSettings::from_cascade().map(drop),
        #[cfg(feature = "memory")]
        || crate::memory::MemoryGuardConfig::from_cascade().map(drop),
        #[cfg(feature = "governor")]
        || crate::governor::SelfRegulationConfig::from_cascade().map(drop),
        #[cfg(feature = "scaling")]
        || crate::scaling::ScalingPressureConfig::from_cascade().map(drop),
        #[cfg(feature = "otel-tracing")]
        || crate::otel_tracing::OtelTracingConfig::from_cascade().map(drop),
        #[cfg(feature = "transport")]
        || crate::transport::filter::TransportFilterTierConfig::from_cascade().map(drop),
        #[cfg(feature = "expression")]
        || crate::expression::evaluator::profile_from_cascade().map(drop),
        #[cfg(feature = "sink-stack")]
        || section::<crate::sink_stack::SinkStackConfig>("sink_stack").map(drop),
    ];
    checks.iter().filter_map(|check| check().err()).collect()
}

#[cfg(all(
    test,
    feature = "config",
    feature = "http",
    feature = "otel-tracing",
    feature = "secrets",
    feature = "geoip-download",
    feature = "metrics",
    feature = "scaling",
    feature = "transport",
    feature = "logger",
    feature = "memory",
    feature = "governor",
    feature = "rules",
    feature = "worker-pool",
    feature = "sink-stack"
))]
mod tests {
    /// Each section as a string where a map belongs, so none deserialises.
    const UNPARSABLE: &str = "\
http_client: nope
otel_tracing: nope
secrets: nope
geoip: nope
metrics: nope
scaling: nope
transport:
  filter_tiers: nope
logger: nope
memory: nope
self_regulation: nope
expression: nope
worker_pool: nope
sink_stack: nope
";

    /// Every `from_cascade()` refuses its section when it does not parse,
    /// instead of answering with the defaults, `check_runtime_sections`
    /// names each section the runtime reads, and an absent one still takes
    /// the defaults.
    #[test]
    fn every_from_cascade_refuses_a_section_that_does_not_parse() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("settings.yaml"), UNPARSABLE).unwrap();
        crate::config::setup(crate::config::ConfigOptions {
            config_paths: vec![dir.path().to_path_buf()],
            ..crate::config::ConfigOptions::default()
        })
        .unwrap();

        let refused: [(&str, bool); 12] = [
            (
                "http_client",
                crate::http_client::HttpClientConfig::from_cascade().is_err(),
            ),
            (
                "otel_tracing",
                crate::otel_tracing::OtelTracingConfig::from_cascade().is_err(),
            ),
            (
                "secrets",
                crate::secrets::SecretsConfig::from_cascade().is_err(),
            ),
            (
                "geoip",
                crate::geoip_download::GeoIpConfig::from_cascade().is_err(),
            ),
            (
                "metrics",
                crate::metrics::MetricsSettings::from_cascade().is_err(),
            ),
            (
                "scaling",
                crate::scaling::ScalingPressureConfig::from_cascade().is_err(),
            ),
            (
                "transport.filter_tiers",
                crate::transport::filter::TransportFilterTierConfig::from_cascade().is_err(),
            ),
            (
                "logger",
                crate::logger::LoggerSettings::from_cascade().is_err(),
            ),
            (
                "memory",
                crate::memory::MemoryGuardConfig::from_cascade().is_err(),
            ),
            (
                "self_regulation",
                crate::governor::SelfRegulationConfig::from_cascade().is_err(),
            ),
            (
                "expression",
                crate::rules::CompileOptions::from_cascade().is_err(),
            ),
            (
                "worker_pool",
                crate::worker::WorkerPoolConfig::from_cascade("worker_pool").is_err(),
            ),
        ];
        let swallowed: Vec<&str> = refused
            .iter()
            .filter(|(_, refused)| !refused)
            .map(|(key, _)| *key)
            .collect();
        assert_eq!(swallowed, Vec::<&str>::new(), "sections read as defaults");

        let mut named: Vec<String> = super::check_runtime_sections()
            .into_iter()
            .map(|e| e.key)
            .collect();
        named.sort();
        assert_eq!(
            named,
            [
                "expression",
                "logger",
                "memory",
                "metrics",
                "otel_tracing",
                "scaling",
                "self_regulation",
                "sink_stack",
                "transport.filter_tiers"
            ]
        );

        let absent: crate::metrics::MetricsSettings = super::section("absent.metrics").unwrap();
        assert_eq!(absent.namespace, "");
    }
}
