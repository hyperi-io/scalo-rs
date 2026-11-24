// Project:   scalo
// File:      src/otel_endpoint.rs
// Purpose:   OTLP default endpoint, protocol ports and per-signal HTTP paths
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The default OTLP endpoint, the port each protocol adds to it, and the
//! OTLP/HTTP signal path, so metrics, tracing and the OTLP transport build an
//! export URL one way.

use std::borrow::Cow;

/// Default OTLP endpoint, the one every config struct starts from. It names no
/// port, so the protocol's port applies.
pub(crate) const DEFAULT_ENDPOINT: &str = "http://localhost";

/// The OTLP gRPC port.
const GRPC_PORT: u16 = 4317;

/// The OTLP/HTTP port.
const HTTP_PORT: u16 = 4318;

/// The endpoint a protocol exports to: an `http://` endpoint naming no port
/// takes 4317 under gRPC and 4318 under HTTP.
///
/// An endpoint naming a port is used as given, and an `https://` one naming
/// none keeps 443, where hosted collectors behind a TLS gateway listen.
pub(crate) fn endpoint_for_protocol(configured: &str, protocol_is_http: bool) -> Cow<'_, str> {
    let endpoint = configured.trim();
    let Some(rest) = endpoint
        .get(..7)
        .filter(|scheme| scheme.eq_ignore_ascii_case("http://"))
        .and(endpoint.get(7..))
    else {
        return Cow::Borrowed(endpoint);
    };
    let (authority, path) = rest.split_at(rest.find(['/', '?', '#']).unwrap_or(rest.len()));
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    // An IPv6 host is bracketed, so only a colon after the bracket starts a port.
    let names_a_port = host
        .rfind(']')
        .map_or(host.contains(':'), |end| host[end..].contains(':'));
    if host.is_empty() || names_a_port {
        return Cow::Borrowed(endpoint);
    }
    let port = if protocol_is_http {
        HTTP_PORT
    } else {
        GRPC_PORT
    };
    Cow::Owned(format!("{}{authority}:{port}{path}", &endpoint[..7]))
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
    if strip_signal_path(trimmed, signal_path).is_some() {
        trimmed.to_string()
    } else {
        format!("{trimmed}/{signal_path}")
    }
}

/// `endpoint` up to its trailing `signal_path`, or `None` when it does not end
/// in that path: the base another signal appends its own path to.
pub(crate) fn strip_signal_path<'a>(endpoint: &'a str, signal_path: &str) -> Option<&'a str> {
    endpoint
        .trim_end_matches('/')
        .strip_suffix(signal_path)
        .filter(|base| base.ends_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Four rules per protocol: the default and an `http://` endpoint naming no
    /// port take the protocol's port, a named port is kept, and `https://`
    /// naming none keeps 443.
    #[test]
    fn each_endpoint_takes_its_port_by_protocol() {
        // (case, endpoint, under gRPC, under HTTP)
        let cases = [
            (
                "the default",
                DEFAULT_ENDPOINT,
                "http://localhost:4317",
                "http://localhost:4318",
            ),
            (
                "http naming no port",
                "http://collector",
                "http://collector:4317",
                "http://collector:4318",
            ),
            (
                "http naming no port, with a path",
                "HTTP://gateway/otlp/",
                "HTTP://gateway:4317/otlp/",
                "HTTP://gateway:4318/otlp/",
            ),
            (
                "an IPv6 host naming no port",
                "http://[::1]",
                "http://[::1]:4317",
                "http://[::1]:4318",
            ),
            (
                "a user and password naming no port",
                " http://user:pw@collector ",
                "http://user:pw@collector:4317",
                "http://user:pw@collector:4318",
            ),
            (
                "a named port, even the other protocol's",
                "http://localhost:4317",
                "http://localhost:4317",
                "http://localhost:4317",
            ),
            (
                "a named port on IPv6",
                "http://[::1]:9000",
                "http://[::1]:9000",
                "http://[::1]:9000",
            ),
            (
                "https naming no port keeps 443",
                "https://gateway.example/otlp",
                "https://gateway.example/otlp",
                "https://gateway.example/otlp",
            ),
            (
                "https naming a port",
                "https://collector:4317",
                "https://collector:4317",
                "https://collector:4317",
            ),
            ("blank", "  ", "", ""),
            ("no host", "http://", "http://", "http://"),
        ];
        for (case, endpoint, grpc, http) in cases {
            assert_eq!(endpoint_for_protocol(endpoint, false), grpc, "{case}: grpc");
            assert_eq!(endpoint_for_protocol(endpoint, true), http, "{case}: http");
        }
    }

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
