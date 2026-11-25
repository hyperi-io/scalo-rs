// Project:   scalo
// File:      src/transport/otlp/config.rs
// Purpose:   OTLP transport configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::compression::{Accepts, Codec, Compression};
use crate::otel_endpoint::{
    DEFAULT_ENDPOINT, append_signal_path, endpoint_for_protocol, strip_signal_path,
};
use crate::sensitive::SensitiveString;
use crate::tls::{ClientTls, ClientTlsConfig};
use crate::transport::connect_to::ConnectTo;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::filter::FilterRule;

use super::Signal;

/// How the transport reaches the collector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    /// `LogsService/Export` and `MetricsService/Export` over gRPC.
    #[default]
    Grpc,
    /// `POST /v1/logs` and `POST /v1/metrics` with a protobuf body.
    /// `http/protobuf`, the `OTEL_EXPORTER_OTLP_PROTOCOL` spelling, is read as
    /// this too.
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

/// OTLP transport configuration, read from `transport.otlp` or from the
/// `otlp` section of a [`TransportConfig`](crate::transport::TransportConfig).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtlpConfig {
    /// Collector base URL, `http://localhost` by default. An `http://` URL
    /// naming no port takes 4317 under `grpc` and 4318 under `http`. A named
    /// port, or `https://` naming none (443), is used as given. Under `http`,
    /// `/v1/logs` and `/v1/metrics` are appended to it, and a URL ending in
    /// either is read as the base before it.
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

    /// An IP address, or `IP:port`, to dial in place of the endpoint's host
    /// and port. TLS still checks the endpoint's host name, and requests name
    /// the endpoint's host and port. The endpoint must name its host, and
    /// `proxy` must be `none`, since a proxy looks the name up itself.
    pub connect_to: Option<String>,

    /// Outbound message filters, applied to each record's payload before the
    /// request is built.
    pub filters_out: Vec<FilterRule>,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.to_string(),
            protocol: OtlpProtocol::Grpc,
            headers: BTreeMap::new(),
            timeout_ms: 10_000,
            compression: Compression::NONE,
            max_retry_delay_ms: 300_000,
            service_name: None,
            resource_attributes: BTreeMap::new(),
            tls: None,
            proxy: crate::transport::ProxyConfig::None,
            connect_to: None,
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

    /// The URL a `signal`'s requests go to: the endpoint for gRPC, and for
    /// HTTP the endpoint's base with the signal's path.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the endpoint is blank.
    pub(super) fn url(&self, signal: Signal) -> TransportResult<String> {
        let is_http = self.protocol == OtlpProtocol::Http;
        let endpoint = endpoint_for_protocol(&self.endpoint, is_http);
        if endpoint.is_empty() {
            return Err(TransportError::Config(
                "OTLP: endpoint is empty".to_string(),
            ));
        }
        Ok(if is_http {
            let base = [Signal::Logs, Signal::Metrics]
                .into_iter()
                .find_map(|named| strip_signal_path(&endpoint, named.path()))
                .unwrap_or(&endpoint);
            append_signal_path(base, signal.path())
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

    /// The address to dial in place of the endpoint's, `None` when unset or blank.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for a value that is not `IP` or `IP:port`, or
    /// one set beside a proxy.
    pub(super) fn connect_to(&self) -> TransportResult<Option<ConnectTo>> {
        let Some(text) = self.connect_to.as_deref().filter(|t| !t.trim().is_empty()) else {
            return Ok(None);
        };
        if self.proxy != crate::transport::ProxyConfig::None {
            return Err(TransportError::Config(
                "OTLP: connect_to cannot be set with a proxy, which looks the endpoint's name up \
                 itself and so would not dial connect_to"
                    .to_string(),
            ));
        }
        ConnectTo::parse(text)
            .map(Some)
            .map_err(|e| TransportError::Config(format!("OTLP: {e}")))
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
             connect_to: 10.0.0.5:4317\n    \
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
        assert_eq!(config.connect_to.as_deref(), Some("10.0.0.5:4317"));
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

    /// The logs and metrics URLs and client TLS each endpoint and section
    /// give, and the settings refused before anything is dialled.
    #[test]
    fn the_endpoint_and_tls_section_give_the_url_and_trust() {
        type Want = Result<(&'static str, &'static str, Option<ClientTls>), &'static str>;
        let tls = |trust, cert: Option<&str>, key: Option<&str>| ClientTlsConfig {
            trust,
            ca_path: Some(PathBuf::from("ca.pem")),
            client_cert_path: cert.map(PathBuf::from),
            client_key_path: key.map(PathBuf::from),
            ..ClientTlsConfig::default()
        };
        // (case, config, logs and metrics URLs and client TLS, or the error text)
        let cases: Vec<(&str, OtlpConfig, Want)> = vec![
            (
                "default grpc",
                OtlpConfig::default(),
                Ok(("http://localhost:4317", "http://localhost:4317", None)),
            ),
            (
                "default endpoint under http moves to 4318",
                OtlpConfig {
                    protocol: OtlpProtocol::Http,
                    ..OtlpConfig::default()
                },
                Ok((
                    "http://localhost:4318/v1/logs",
                    "http://localhost:4318/v1/metrics",
                    None,
                )),
            ),
            (
                "http base url gets each signal's path",
                OtlpConfig::http("http://collector:4318/"),
                Ok((
                    "http://collector:4318/v1/logs",
                    "http://collector:4318/v1/metrics",
                    None,
                )),
            ),
            (
                "an http url naming the metrics path is read as the base",
                OtlpConfig::http("http://gateway:4320/otlp/v1/metrics"),
                Ok((
                    "http://gateway:4320/otlp/v1/logs",
                    "http://gateway:4320/otlp/v1/metrics",
                    None,
                )),
            ),
            (
                "https with no section trusts the OS store",
                OtlpConfig::grpc("https://collector:4317"),
                Ok((
                    "https://collector:4317",
                    "https://collector:4317",
                    Some(ClientTls::default()),
                )),
            ),
            (
                "https with mutual TLS, the url naming the logs path",
                OtlpConfig {
                    tls: Some(tls(TrustChoice::Bundle, Some("c.pem"), Some("k.pem"))),
                    ..OtlpConfig::http("https://collector:4318/v1/logs")
                },
                Ok((
                    "https://collector:4318/v1/logs",
                    "https://collector:4318/v1/metrics",
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
            (
                "an http url naming the gRPC port keeps it",
                OtlpConfig::http("http://localhost:4317"),
                Ok((
                    "http://localhost:4317/v1/logs",
                    "http://localhost:4317/v1/metrics",
                    None,
                )),
            ),
            (
                "an http url naming no port takes 4318",
                OtlpConfig::http("http://collector"),
                Ok((
                    "http://collector:4318/v1/logs",
                    "http://collector:4318/v1/metrics",
                    None,
                )),
            ),
            (
                "an https url naming no port keeps 443",
                OtlpConfig::http("https://gateway.example/otlp"),
                Ok((
                    "https://gateway.example/otlp/v1/logs",
                    "https://gateway.example/otlp/v1/metrics",
                    Some(ClientTls::default()),
                )),
            ),
            ("blank", OtlpConfig::grpc("  "), Err("endpoint is empty")),
        ];
        for (case, config, want) in cases {
            let got = config
                .url(Signal::Logs)
                .and_then(|logs| Ok((logs, config.url(Signal::Metrics)?)))
                .and_then(|(logs, metrics)| {
                    config.client_tls(&logs).map(|tls| (logs, metrics, tls))
                })
                .map_err(|e| e.to_string());
            match (got, want) {
                (Ok((logs, metrics, tls)), Ok(want)) => {
                    assert_eq!((logs.as_str(), metrics.as_str(), tls), want, "{case}");
                }
                (Err(e), Err(text)) => assert!(e.contains(text), "{case}: {e}"),
                (got, want) => panic!("{case}: got {got:?}, want {want:?}"),
            }
        }
    }

    /// A zero timeout, and a codec or level the transport cannot send, are
    /// refused before anything is built.
    #[test]
    fn a_zero_timeout_and_an_unsent_codec_are_refused() {
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
