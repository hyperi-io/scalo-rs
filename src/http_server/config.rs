// Project:   scalo
// File:      src/http_server/config.rs
// Purpose:   HTTP server configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! HTTP server configuration.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// HTTP server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct HttpServerConfig {
    /// Address to bind to (e.g., "0.0.0.0:8080").
    pub bind_address: String,

    /// Request timeout in milliseconds.
    /// Defaults to 30 seconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Keep-alive timeout in ms. Default 75 s.
    ///
    /// **Not wired.** axum 0.8 `serve()` doesn't surface keep-alive;
    /// needs hyper builder. Follow-up.
    #[serde(default = "default_keep_alive_timeout_ms")]
    pub keep_alive_timeout_ms: u64,

    /// In-flight request cap. Default 10,000. Enforced via
    /// `tower::limit::ConcurrencyLimitLayer`. Excess requests queue.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Largest request body any route accepts, in bytes. Default 16 MiB, the
    /// same as the HTTP transport's `max_body_bytes`. A larger declared
    /// `Content-Length` is answered 413 before the handler runs, and a chunked
    /// body is cut off at the limit. `0` turns the limit off, for a server whose
    /// routes stream bodies of their own size.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,

    /// Mount /livez + /readyz. Default true.
    #[serde(default = "default_true")]
    pub enable_health_endpoints: bool,

    /// Mount /metrics.
    ///
    /// **Not wired here.** MetricsManager owns its own /metrics
    /// listener (often a separate admin port). Forward-compat for
    /// framework-managed wiring.
    #[serde(default)]
    pub enable_metrics_endpoint: bool,

    /// Mount /config (redacted effective config). Default false.
    #[serde(default)]
    pub enable_config_endpoint: bool,

    /// HTTP/2 (also required for gRPC).
    ///
    /// **Not wired.** axum 0.8 `serve()` always negotiates HTTP/1
    /// and HTTP/2 on cleartext. Disabling needs hyper builder.
    #[serde(default = "default_true")]
    pub enable_http2: bool,

    /// Server TLS, the [`ServerTlsConfig`](crate::tls_config::ServerTlsConfig)
    /// every server reads: profile, certificate and key from files or secrets,
    /// client CAs, client auth and reload. Needs the `tls` feature. Unset, the
    /// server is plain HTTP.
    #[serde(default)]
    pub tls: Option<crate::tls_config::ServerTlsConfig>,

    /// Graceful drain budget in ms. Default 30 s. Caps drain time
    /// to fit under K8s `terminationGracePeriodSeconds`.
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
}

fn default_request_timeout_ms() -> u64 {
    30_000
}

fn default_keep_alive_timeout_ms() -> u64 {
    75_000
}

fn default_max_connections() -> usize {
    10_000
}

fn default_max_body_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_shutdown_timeout_ms() -> u64 {
    30_000
}

fn default_true() -> bool {
    true
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            request_timeout_ms: default_request_timeout_ms(),
            keep_alive_timeout_ms: default_keep_alive_timeout_ms(),
            max_connections: default_max_connections(),
            max_body_bytes: default_max_body_bytes(),
            enable_health_endpoints: true,
            enable_metrics_endpoint: false,
            enable_config_endpoint: false,
            enable_http2: true,
            tls: None,
            shutdown_timeout_ms: default_shutdown_timeout_ms(),
        }
    }
}

impl HttpServerConfig {
    /// Create a new config with the given bind address.
    #[must_use]
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            bind_address: bind_address.into(),
            ..Default::default()
        }
    }

    /// Get request timeout as Duration.
    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    /// Get keep-alive timeout as Duration.
    #[must_use]
    pub fn keep_alive_timeout(&self) -> Duration {
        Duration::from_millis(self.keep_alive_timeout_ms)
    }

    /// Get shutdown timeout as Duration.
    #[must_use]
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }

    /// Whether a `tls` section is set.
    #[must_use]
    pub fn is_tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    /// Refuse a TLS setting the server would not honour, so it never binds
    /// cleartext while the config asks for TLS. `serve` calls this first, and
    /// loading the section checks its sources.
    ///
    /// # Errors
    ///
    /// A `tls` section in a build without the `tls` feature.
    pub fn validate(&self) -> Result<(), String> {
        if self.tls.is_some() && !cfg!(feature = "tls") {
            return Err(
                "http_server: a tls section needs scalo's `tls` feature, which this build \
                 does not enable"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HttpServerConfig::default();
        assert_eq!(config.bind_address, "0.0.0.0:8080");
        assert_eq!(config.request_timeout_ms, 30_000);
        assert_eq!(config.keep_alive_timeout_ms, 75_000);
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.max_body_bytes, 16 * 1024 * 1024);
        assert!(config.enable_health_endpoints);
        assert!(!config.enable_metrics_endpoint);
        assert!(config.enable_http2);
        assert!(!config.is_tls_enabled());
    }

    #[test]
    fn test_new_with_address() {
        let config = HttpServerConfig::new("127.0.0.1:3000");
        assert_eq!(config.bind_address, "127.0.0.1:3000");
    }

    /// A `tls` section is refused in a build that cannot terminate TLS, rather
    /// than served in clear text.
    #[test]
    fn validate_refuses_tls_it_would_not_serve() {
        let tls = HttpServerConfig {
            tls: Some(crate::tls_config::ServerTlsConfig::from_files(
                "/c.pem", "/k.pem",
            )),
            ..HttpServerConfig::default()
        };
        assert!(tls.is_tls_enabled());
        assert_eq!(tls.validate().is_ok(), cfg!(feature = "tls"));
        assert!(HttpServerConfig::default().validate().is_ok());
    }

    #[test]
    fn test_duration_conversions() {
        let config = HttpServerConfig::default();
        assert_eq!(config.request_timeout(), Duration::from_secs(30));
        assert_eq!(config.keep_alive_timeout(), Duration::from_secs(75));
        assert_eq!(config.shutdown_timeout(), Duration::from_secs(30));
    }
}
