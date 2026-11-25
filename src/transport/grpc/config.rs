// Project:   scalo
// File:      src/transport/grpc/config.rs
// Purpose:   gRPC transport configuration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use serde::{Deserialize, Serialize};

use crate::compression::{Accepts, Compression};
use crate::tls_config::Removed;
use crate::transport::tonic_channel::{DEFAULT_SEND_TIMEOUT_MS, TONIC_CODECS};

/// The codecs a gRPC client sends with: none, gzip, and zstd with the
/// `compression` feature. tonic applies them and fixes the level, so none is
/// taken and scalo's engine is not needed.
pub const GRPC_ACCEPTS: Accepts = Accepts::new("transport.grpc.compression", TONIC_CODECS)
    .without_level()
    .external();

#[cfg(feature = "config-schema")]
fn compression_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    GRPC_ACCEPTS.schema()
}

/// gRPC transport configuration.
///
/// Supports client mode (sending), server mode (receiving), or both.
///
/// # Client mode
///
/// Set `endpoint` to connect to a remote gRPC server.
///
/// # Server mode
///
/// Set `listen` to accept incoming Push RPCs.
///
/// # Both
///
/// Set both for bidirectional communication (e.g., a relay node).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct GrpcConfig {
    /// Server listen address (e.g., "0.0.0.0:6000").
    /// When set, the transport accepts incoming Push RPCs.
    pub listen: Option<String>,

    /// Client endpoint URI (e.g., "http://loader:6000"). When set, the
    /// transport can send messages to a remote server, over TLS when it is
    /// `https://`.
    pub endpoint: Option<String>,

    /// Receive buffer size (messages buffered from incoming RPCs).
    ///
    /// A `RouteBatch` with more records than this is held whole beside the
    /// buffer, one batch at a time.
    pub recv_buffer_size: usize,

    /// Receive timeout in milliseconds (0 = non-blocking).
    pub recv_timeout_ms: u64,

    /// Send deadline in milliseconds (0 = no deadline). Default 30s.
    ///
    /// Bounds each `send`, and each request of a `send_batch`, end to end --
    /// DNS, TCP connect, TLS handshake and the RPC -- so a server that is down
    /// or never answers returns `Backpressured` at the limit. A dial still
    /// unfinished at nine tenths of it is abandoned, so the next send dials
    /// afresh. Also sent as the `grpc-timeout` header, which an armed receiver
    /// holds its answer within.
    ///
    /// A connection that has read nothing for this long is sent an HTTP/2
    /// PING, and closed if the PING goes unanswered for as long again (30s
    /// each when 0), so a server that stays connected but stops answering is
    /// dropped and the next send dials afresh.
    pub send_timeout_ms: u64,

    /// Maximum message size in bytes (both send and receive).
    pub max_message_size: usize,

    /// Codec the client sends requests with: `none` (default), `gzip` or
    /// `zstd` with the `compression` feature, with no level. The server reads
    /// what the build compiles on every connection.
    #[cfg_attr(
        feature = "config-schema",
        schemars(schema_with = "compression_schema")
    )]
    pub compression: Compression,

    /// Client TLS for an `https://` endpoint, the
    /// [`ClientTlsConfig`](crate::tls::ClientTlsConfig) every transport reads.
    /// An `https://` endpoint with no section takes the defaults: `strong`,
    /// the OS root store, the endpoint's host as the name. A section beside an
    /// `http://` endpoint is refused.
    pub tls: Option<crate::tls::ClientTlsConfig>,

    /// Proxy the `endpoint` connection goes through: `none` (default),
    /// `system` (the machine's proxy) or a proxy URL. The connection is an
    /// HTTP CONNECT tunnel, and TLS runs inside it. See
    /// [`ProxyConfig`](crate::transport::ProxyConfig).
    pub proxy: crate::transport::ProxyConfig,

    /// Enable Vector wire protocol compatibility on the same server.
    /// When true, the server also accepts `/vector.Vector/PushEvents` RPCs
    /// from legacy Vector sinks.
    /// Requires the `transport-grpc-vector-compat` feature.
    #[cfg(feature = "transport-grpc-vector-compat")]
    pub vector_compat: bool,

    /// Inbound message filters (applied on recv before caller sees messages).
    pub filters_in: Vec<crate::transport::filter::FilterRule>,

    /// Outbound message filters (applied on send before transport dispatches).
    pub filters_out: Vec<crate::transport::filter::FilterRule>,

    /// Joining small `send_batch` blocks for `endpoint` into one `RouteBatch`
    /// call.
    pub coalesce: CoalesceConfig,

    /// Removed: an `https://` endpoint runs TLS.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_tls_enabled")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub tls_enabled: Removed,

    /// Removed: `tls.ca_paths` with `tls.trust: bundle`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_tls_ca_path")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub tls_ca_path: Removed,

    /// Removed: `tls.server_name`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_tls_domain")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub tls_domain: Removed,

    /// Removed: `tls.client_cert_path`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_tls_client_cert_path")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub tls_client_cert_path: Removed,

    /// Removed: `tls.client_key_path`.
    #[doc(hidden)]
    #[serde(skip_serializing, deserialize_with = "removed_tls_client_key_path")]
    #[cfg_attr(feature = "config-schema", schemars(skip))]
    pub tls_client_key_path: Removed,
}

crate::tls_config::removed_keys! {
    removed_tls_enabled: "tls_enabled" => "an https:// endpoint, which runs TLS";
    removed_tls_ca_path: "tls_ca_path" => "tls.ca_paths with tls.trust: bundle";
    removed_tls_domain: "tls_domain" => "tls.server_name";
    removed_tls_client_cert_path: "tls_client_cert_path" => "tls.client_cert_path";
    removed_tls_client_key_path: "tls_client_key_path" => "tls.client_key_path";
}

/// How `send_batch` joins small blocks for one endpoint into one `RouteBatch`
/// call.
///
/// When a call slot is free, the next queued block goes out with every block
/// queued behind it, up to `max_batch_bytes` and `max_message_size` encoded,
/// with no timer. A block at that cap or over it is sent at once on its own.
/// `send_timeout_ms` bounds the wait for a slot too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct CoalesceConfig {
    /// Join blocks. `false` sends every `send_batch` as its own call.
    pub enabled: bool,

    /// Most bytes one joined call carries, encoded.
    pub max_batch_bytes: u64,

    /// Calls in flight to the endpoint at once, joined calls only.
    pub in_flight: usize,
}

impl Default for CoalesceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_batch_bytes: 65_536,
            in_flight: 4,
        }
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen: None,
            endpoint: None,
            recv_buffer_size: 10_000,
            recv_timeout_ms: 100,
            send_timeout_ms: DEFAULT_SEND_TIMEOUT_MS,
            max_message_size: 16 * 1024 * 1024, // 16 MB
            compression: Compression::NONE,
            tls: None,
            proxy: crate::transport::ProxyConfig::None,
            #[cfg(feature = "transport-grpc-vector-compat")]
            vector_compat: false,
            filters_in: Vec::new(),
            filters_out: Vec::new(),
            coalesce: CoalesceConfig::default(),
            tls_enabled: Removed,
            tls_ca_path: Removed,
            tls_domain: Removed,
            tls_client_cert_path: Removed,
            tls_client_key_path: Removed,
        }
    }
}

impl GrpcConfig {
    /// Load from the config cascade under the `grpc` key, or the defaults
    /// when it is absent.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the section is present but does not parse.
    pub fn from_cascade() -> crate::transport::TransportResult<Self> {
        <Self as crate::transport::traits::FromCascade>::from_cascade_key("grpc")
    }

    /// Create a server-only config.
    #[must_use]
    pub fn server(listen: &str) -> Self {
        Self {
            listen: Some(listen.to_string()),
            ..Default::default()
        }
    }

    /// Create a client-only config.
    #[must_use]
    pub fn client(endpoint: &str) -> Self {
        Self {
            endpoint: Some(endpoint.to_string()),
            ..Default::default()
        }
    }

    /// Send requests compressed with `compression`.
    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// `compression`, if [`GRPC_ACCEPTS`] takes it.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` naming the codecs gRPC takes, or a level.
    pub fn check_compression(&self) -> crate::transport::TransportResult<()> {
        GRPC_ACCEPTS
            .check(self.compression)
            .map(drop)
            .map_err(|e| crate::transport::TransportError::Config(e.to_string()))
    }

    /// Set max message size.
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Enable Vector wire protocol compatibility (feature-gated).
    #[cfg(feature = "transport-grpc-vector-compat")]
    #[must_use]
    pub fn with_vector_compat(mut self) -> Self {
        self.vector_compat = true;
        self
    }
}

impl crate::transport::traits::FromCascade for GrpcConfig {}

#[cfg(test)]
mod tests {
    use super::*;

    /// A removed TLS key fails the load, naming its replacement, where the
    /// client would dial with the key dropped.
    #[cfg(all(feature = "yaml", feature = "figment"))]
    #[test]
    fn a_removed_tls_key_fails_the_load() {
        crate::tls_config::assert_removed::<GrpcConfig>(&[
            (r#"{"tls_enabled": true}"#, "https://"),
            (r#"{"tls_ca_path": "/ca.pem"}"#, "tls.ca_paths"),
            (r#"{"tls_domain": "loader"}"#, "tls.server_name"),
            (
                r#"{"tls_client_cert_path": "/c.pem"}"#,
                "tls.client_cert_path",
            ),
            (
                r#"{"tls_client_key_path": "/k.pem"}"#,
                "tls.client_key_path",
            ),
        ]);
    }

    /// The `tls` section reaches the config through the cascade, from YAML
    /// and from the environment, which wins.
    #[cfg(feature = "config")]
    #[test]
    fn the_tls_section_reaches_the_config_through_the_cascade() {
        use crate::tls::{ClientTlsConfig, CryptoProfile, KeyShare, TrustChoice};

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "grpc:\n  endpoint: https://loader:6000\n  tls:\n    profile: cnsa2\n    \
             trust: bundle\n    ca_paths: /etc/tls/ca.pem\n    server_name: loader\n",
        )
        .unwrap();
        temp_env::with_var("GRPCTLS__GRPC__TLS__KEY_SHARE", Some("classical"), || {
            crate::config::setup(crate::config::ConfigOptions {
                env_prefix: "GRPCTLS".to_string(),
                config_paths: vec![dir.path().to_path_buf()],
                search_standard_paths: false,
                load_dotenv: false,
                ..crate::config::ConfigOptions::default()
            })
            .unwrap();
        });
        let config = GrpcConfig::from_cascade().unwrap();
        assert_eq!(
            config.tls,
            Some(ClientTlsConfig {
                profile: CryptoProfile::Cnsa2,
                trust: TrustChoice::Bundle,
                ca_paths: vec!["/etc/tls/ca.pem".into()],
                server_name: Some("loader".to_string()),
                key_share: KeyShare::Classical,
                ..ClientTlsConfig::default()
            })
        );
    }
}
