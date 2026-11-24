// Project:   scalo
// File:      src/transport/otlp/config.rs
// Purpose:   OTLP logs transport configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::compression::{Accepts, Codec, Compression};
use crate::otel_endpoint::{DEFAULT_GRPC_ENDPOINT, append_signal_path, endpoint_for_protocol};
use crate::sensitive::SensitiveString;
use crate::tls::{ClientTls, ClientTlsConfig};
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::filter::FilterRule;

/// The OTLP/HTTP path for logs.
const LOGS_PATH: &str = "v1/logs";

/// How the transport reaches the collector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    /// `LogsService/Export` over gRPC.
    #[default]
    Grpc,
    /// `POST /v1/logs` with a protobuf body. `http/protobuf`, the
    /// `OTEL_EXPORTER_OTLP_PROTOCOL` spelling, is read as this too.
    #[serde(alias = "http/protobuf")]
    Http,
}

impl OtlpProtocol {
    /// The config name of this protocol.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Http => "http",
        }
    }
}

/// The codecs an export request takes: `grpc-encoding` on gRPC,
/// `Content-Encoding` on HTTP. tonic fixes the gRPC level, so no level is
/// taken on either. tonic and flate2 apply it, not scalo's engine.
pub const OTLP_ACCEPTS: Accepts =
    Accepts::new("transport.otlp.compression", &[Codec::None, Codec::Gzip])
        .without_level()
        .external();

/// OTLP logs transport configuration, read from `transport.otlp` or from the
/// `otlp` section of a [`TransportConfig`](crate::transport::TransportConfig).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtlpConfig {
    /// Collector base URL. Left at the gRPC default, `http://localhost:4317`,
    /// it is `http://localhost:4318` under `http`. Under `http`, `/v1/logs`
    /// is appended unless the URL already ends in it.
    pub endpoint: String,

    /// `grpc` (default) or `http`.
    pub protocol: OtlpProtocol,

    /// Headers sent with every request: gRPC metadata or HTTP headers,
    /// commonly an API key. Each value is a [`SensitiveString`], so `Debug`
    /// and logs show it redacted, and the map is never serialised back out.
    #[serde(skip_serializing)]
    pub headers: BTreeMap<String, SensitiveString>,

    /// Limit on each export, connect and TLS handshake included, in
    /// milliseconds. Default 10000. A send past it is `Backpressured`.
    pub timeout_ms: u64,

    /// `{codec: none}` (default) or `{codec: gzip}`. No level.
    pub compression: Compression,

    /// The longest wait a collector's `Retry-After` or `RetryInfo` is taken
    /// at, in milliseconds. Default 300000 (5 min), so a collector cannot hold
    /// the sender indefinitely.
    pub max_retry_delay_ms: u64,

    /// `service.name` on the resource. Unset, the resource takes
    /// `OTEL_SERVICE_NAME`, else `unknown_service`.
    pub service_name: Option<String>,

    /// Resource attributes, set over the replica identity and
    /// `OTEL_RESOURCE_ATTRIBUTES`.
    pub resource_attributes: BTreeMap<String, String>,

    /// Client TLS for an `https://` endpoint, in the [`ClientTlsConfig`] keys.
    /// Without it an `https://` endpoint is verified against the OS root
    /// store. With it, the endpoint must be `https://`. gRPC takes `native` or
    /// `bundle` trust.
    pub tls: Option<ClientTlsConfig>,

    /// `none` (default), `system` (the machine's proxy) or a proxy URL, as the
    /// other transports read it. See
    /// [`ProxyConfig`](crate::transport::ProxyConfig).
    pub proxy: crate::transport::ProxyConfig,

    /// Outbound message filters, applied to each record's payload before the
    /// request is built.
    pub filters_out: Vec<FilterRule>,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_GRPC_ENDPOINT.to_string(),
            protocol: OtlpProtocol::Grpc,
            headers: BTreeMap::new(),
            timeout_ms: 10_000,
            compression: Compression::NONE,
            max_retry_delay_ms: 300_000,
            service_name: None,
            resource_attributes: BTreeMap::new(),
            tls: None,
            proxy: crate::transport::ProxyConfig::None,
            filters_out: Vec::new(),
        }
    }
}

impl crate::transport::traits::FromCascade for OtlpConfig {}

impl OtlpConfig {
    /// Load from the config cascade under `transport.otlp`, or the defaults
    /// when it is absent.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the section is present but does not parse.
    pub fn from_cascade() -> TransportResult<Self> {
        <Self as crate::transport::traits::FromCascade>::from_cascade_key("transport.otlp")
    }

    /// Send over gRPC to `endpoint`.
    #[must_use]
    pub fn grpc(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            ..Self::default()
        }
    }

    /// Send over HTTP/protobuf to `endpoint`, a base URL.
    #[must_use]
    pub fn http(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            protocol: OtlpProtocol::Http,
            ..Self::default()
        }
    }

    /// The URL each request goes to: the endpoint for gRPC, and the endpoint
    /// with `/v1/logs` for HTTP.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the endpoint is blank.
    pub(super) fn url(&self) -> TransportResult<String> {
        let is_http = self.protocol == OtlpProtocol::Http;
        let endpoint = endpoint_for_protocol(&self.endpoint, is_http).trim();
        if endpoint.is_empty() {
            return Err(TransportError::Config(
                "OTLP: endpoint is empty".to_string(),
            ));
        }
        Ok(if is_http {
            append_signal_path(endpoint, LOGS_PATH)
        } else {
            endpoint.to_string()
        })
    }

    /// The client TLS for `url`: the `tls` section, or the OS root store when
    /// there is none, for `https://`, and `None` for `http://`.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for a scheme other than `http` or `https`, a
    /// `tls` section beside an `http://` URL, or half a client identity.
    pub(super) fn client_tls(&self, url: &str) -> TransportResult<Option<ClientTls>> {
        let scheme = url
            .split_once("://")
            .map(|(scheme, _)| scheme.to_ascii_lowercase());
        match (scheme.as_deref(), &self.tls) {
            (Some("https"), Some(tls)) => tls
                .client_tls()
                .map(Some)
                .map_err(|e| TransportError::Config(format!("OTLP {e}"))),
            (Some("https"), None) => Ok(Some(ClientTls::default())),
            (Some("http"), None) => Ok(None),
            (Some("http"), Some(_)) => Err(TransportError::Config(
                "OTLP: a tls section needs an https:// endpoint".to_string(),
            )),
            _ => Err(TransportError::Config(
                "OTLP: the endpoint must start with http:// or https://".to_string(),
            )),
        }
    }

    /// Refuse a config no transport can be built from.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for a `timeout_ms` of 0, or a `compression`
    /// [`OTLP_ACCEPTS`] refuses.
    pub(super) fn validate(&self) -> TransportResult<()> {
        if self.timeout_ms == 0 {
            return Err(TransportError::Config(
                "OTLP: timeout_ms must be above 0".to_string(),
            ));
        }
        OTLP_ACCEPTS
            .check(self.compression)
            .map_err(|e| TransportError::Config(format!("OTLP: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::tls::TrustChoice;

    /// Each key reaches the config through the cascade, from YAML and from
    /// the environment, which wins.
    #[cfg(feature = "config")]
    #[test]
    fn every_key_reaches_the_config_through_the_cascade() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "transport:\n  otlp:\n    endpoint: https://collector.example:4318\n    \
             protocol: grpc\n    headers:\n      authorization: Bearer yaml\n      \
             x-tenant: blue\n    timeout_ms: 2500\n    compression: {codec: none}\n    \
             max_retry_delay_ms: 60000\n    service_name: from-yaml\n    \
             resource_attributes:\n      team: data\n    tls:\n      trust: bundle\n      \
             ca_path: /etc/tls/ca.pem\n      client_cert_path: /etc/tls/client.pem\n      \
             client_key_path: /etc/tls/yaml.key\n    proxy: http://proxy.example:3128\n    \
             filters_out:\n      \
             - expression: 'has(_debug)'\n        action: drop\n",
        )
        .unwrap();
        temp_env::with_vars(
            [
                ("OTLPCFG__TRANSPORT__OTLP__PROTOCOL", Some("http")),
                ("OTLPCFG__TRANSPORT__OTLP__COMPRESSION__CODEC", Some("gzip")),
                (
                    "OTLPCFG__TRANSPORT__OTLP__HEADERS__AUTHORIZATION",
                    Some("Bearer env"),
                ),
                ("OTLPCFG__TRANSPORT__OTLP__SERVICE_NAME", Some("from-env")),
                (
                    "OTLPCFG__TRANSPORT__OTLP__TLS__CLIENT_KEY_PATH",
                    Some("/etc/tls/env.key"),
                ),
            ],
            || {
                crate::config::setup(crate::config::ConfigOptions {
                    env_prefix: "OTLPCFG".to_string(),
                    config_paths: vec![dir.path().to_path_buf()],
                    search_standard_paths: false,
                    load_dotenv: false,
                    ..crate::config::ConfigOptions::default()
                })
                .unwrap();
            },
        );
        let config = OtlpConfig::from_cascade().unwrap();
        assert_eq!(config.endpoint, "https://collector.example:4318");
        assert_eq!(config.protocol, OtlpProtocol::Http);
        assert_eq!(
            config
                .headers
                .get("authorization")
                .map(SensitiveString::expose),
            Some("Bearer env")
        );
        assert_eq!(
            config.headers.get("x-tenant").map(SensitiveString::expose),
            Some("blue")
        );
        assert_eq!(config.timeout_ms, 2500);
        assert_eq!(config.compression, Compression::new(Codec::Gzip));
        assert_eq!(config.max_retry_delay_ms, 60_000);
        assert_eq!(config.service_name.as_deref(), Some("from-env"));
        assert_eq!(
            config.resource_attributes.get("team").map(String::as_str),
            Some("data")
        );
        assert_eq!(
            config.tls,
            Some(ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_path: Some(PathBuf::from("/etc/tls/ca.pem")),
                client_cert_path: Some(PathBuf::from("/etc/tls/client.pem")),
                client_key_path: Some(PathBuf::from("/etc/tls/env.key")),
                ..ClientTlsConfig::default()
            })
        );
        assert_eq!(
            config.proxy,
            crate::transport::ProxyConfig::Url(crate::sensitive::SensitiveString::new(
                "http://proxy.example:3128"
            ))
        );
        assert_eq!(config.filters_out.len(), 1);
        assert!(
            !serde_json::to_string(&config).unwrap().contains("Bearer"),
            "headers are never serialised back out"
        );
        assert!(
            !format!("{config:?}").contains("Bearer"),
            "Debug redacts header values"
        );
    }

    /// The URL and client TLS each endpoint and section give, and the
    /// settings refused before anything is dialled.
    #[test]
    fn the_endpoint_and_tls_section_give_the_url_and_trust() {
        type Want = Result<(&'static str, Option<ClientTls>), &'static str>;
        let tls = |trust, cert: Option<&str>, key: Option<&str>| ClientTlsConfig {
            trust,
            ca_path: Some(PathBuf::from("ca.pem")),
            client_cert_path: cert.map(PathBuf::from),
            client_key_path: key.map(PathBuf::from),
            ..ClientTlsConfig::default()
        };
        // (case, config, url and client TLS, or the error text)
        let cases: Vec<(&str, OtlpConfig, Want)> = vec![
            (
                "default grpc",
                OtlpConfig::default(),
                Ok(("http://localhost:4317", None)),
            ),
            (
                "default endpoint under http moves to 4318",
                OtlpConfig {
                    protocol: OtlpProtocol::Http,
                    ..OtlpConfig::default()
                },
                Ok(("http://localhost:4318/v1/logs", None)),
            ),
            (
                "http base url gets the logs path",
                OtlpConfig::http("http://collector:4318/"),
                Ok(("http://collector:4318/v1/logs", None)),
            ),
            (
                "https with no section trusts the OS store",
                OtlpConfig::grpc("https://collector:4317"),
                Ok(("https://collector:4317", Some(ClientTls::default()))),
            ),
            (
                "https with mutual TLS",
                OtlpConfig {
                    tls: Some(tls(TrustChoice::Bundle, Some("c.pem"), Some("k.pem"))),
                    ..OtlpConfig::http("https://collector:4318/v1/logs")
                },
                Ok((
                    "https://collector:4318/v1/logs",
                    Some(ClientTls::pinned("ca.pem").with_identity("c.pem", "k.pem")),
                )),
            ),
            (
                "half an identity",
                OtlpConfig {
                    tls: Some(tls(TrustChoice::Native, Some("c.pem"), None)),
                    ..OtlpConfig::grpc("https://collector:4317")
                },
                Err("needs both"),
            ),
            (
                "tls beside http",
                OtlpConfig {
                    tls: Some(tls(TrustChoice::Bundle, None, None)),
                    ..OtlpConfig::grpc("http://collector:4317")
                },
                Err("needs an https:// endpoint"),
            ),
            (
                "no scheme",
                OtlpConfig::grpc("collector:4317"),
                Err("must start with http:// or https://"),
            ),
            ("blank", OtlpConfig::grpc("  "), Err("endpoint is empty")),
        ];
        for (case, config, want) in cases {
            let got = config
                .url()
                .and_then(|url| config.client_tls(&url).map(|tls| (url, tls)))
                .map_err(|e| e.to_string());
            match (got, want) {
                (Ok((url, tls)), Ok((want_url, want_tls))) => {
                    assert_eq!((url.as_str(), tls), (want_url, want_tls), "{case}");
                }
                (Err(e), Err(text)) => assert!(e.contains(text), "{case}: {e}"),
                (got, want) => panic!("{case}: got {got:?}, want {want:?}"),
            }
        }
        assert!(
            OtlpConfig {
                timeout_ms: 0,
                ..OtlpConfig::default()
            }
            .validate()
            .is_err(),
            "a timeout of 0 is refused"
        );
        for (compression, ok) in [
            (Compression::new(Codec::Gzip), true),
            (Compression::new(Codec::Zstd), false),
            (Compression::new(Codec::Gzip).with_level(9), false),
        ] {
            let config = OtlpConfig {
                compression,
                ..OtlpConfig::default()
            };
            assert_eq!(config.validate().is_ok(), ok, "{compression}");
        }
        assert!(OtlpConfig::default().validate().is_ok());
    }
}
