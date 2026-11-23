// Project:   scalo
// File:      src/otel_endpoint.rs
// Purpose:   OTLP default endpoints and per-signal HTTP paths
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The default OTLP endpoint per protocol and the OTLP/HTTP signal path, so
//! metrics, tracing and the OTLP logs transport build an export URL one way.

/// Default OTLP gRPC endpoint, the one every config struct starts from.
pub(crate) const DEFAULT_GRPC_ENDPOINT: &str = "http://localhost:4317";

/// Default OTLP/HTTP endpoint.
pub(crate) const DEFAULT_HTTP_ENDPOINT: &str = "http://localhost:4318";

/// The configured endpoint for a protocol, with the gRPC default moved to the
/// HTTP port when the protocol is HTTP.
///
/// The config structs default `endpoint` to the gRPC address, so that value is
/// the only sign none was configured. Any other endpoint is used as given.
pub(crate) fn endpoint_for_protocol(configured: &str, protocol_is_http: bool) -> &str {
    if protocol_is_http && configured.trim() == DEFAULT_GRPC_ENDPOINT {
        DEFAULT_HTTP_ENDPOINT
    } else {
        configured
    }
}

/// Append an OTLP signal path to a base endpoint, for the HTTP protocol.
///
/// scalo's `endpoint` and `OTEL_EXPORTER_OTLP_ENDPOINT` are base URLs that each
/// signal appends its own path to, and the exporter POSTs to whatever endpoint
/// its builder is handed, unchanged. An endpoint already ending in the signal
/// path is returned as it is, so a full signal URL is never doubled.
///
/// gRPC has no per-signal path, so this applies to the HTTP protocol only.
pub(crate) fn append_signal_path(endpoint: &str, signal_path: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    let names_the_signal = trimmed
        .strip_suffix(signal_path)
        .is_some_and(|base| base.ends_with('/'));
    if names_the_signal {
        trimmed.to_string()
    } else {
        format!("{trimmed}/{signal_path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_base_endpoint_gets_the_signal_path() {
        assert_eq!(
            append_signal_path("http://collector:4318", "v1/traces"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            append_signal_path("http://collector:4318/", "v1/metrics"),
            "http://collector:4318/v1/metrics"
        );
    }

    #[test]
    fn a_base_path_prefix_is_kept_and_extended() {
        // The spec appends the signal path to a base URL that carries a path of its own.
        assert_eq!(
            append_signal_path("https://gateway.example/otlp/", "v1/traces"),
            "https://gateway.example/otlp/v1/traces"
        );
    }

    #[test]
    fn an_endpoint_naming_the_signal_is_used_as_given() {
        assert_eq!(
            append_signal_path("http://collector:4318/v1/traces", "v1/traces"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            append_signal_path("http://collector:4318/v1/metrics/", "v1/metrics"),
            "http://collector:4318/v1/metrics"
        );
    }

    #[test]
    fn a_lookalike_suffix_is_not_the_signal_path() {
        assert_eq!(
            append_signal_path("http://collector:4318/xv1/traces", "v1/traces"),
            "http://collector:4318/xv1/traces/v1/traces"
        );
        assert_eq!(
            append_signal_path("http://collector:4318/v1/traces", "v1/metrics"),
            "http://collector:4318/v1/traces/v1/metrics"
        );
    }
}
