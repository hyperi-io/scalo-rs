// Project:   scalo
// File:      src/transport/http.rs
// Purpose:   HTTP/HTTPS transport (send via POST, receive via embedded server)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # HTTP Transport
//!
//! HTTP/HTTPS transport for webhook delivery and REST ingest.
//!
//! ## Send
//!
//! POSTs each run of records with the same key to `{endpoint}/{key}` as one
//! body, framed by `framing` and compressed by `compression`. `failover`
//! endpoints are tried in order after `endpoint`. Redirects are refused unless
//! `follow_redirects` is set, and no proxy is used unless `proxy` names one.
//! The guide is `docs/transport/http.md`.
//!
//! ## Receive (requires `http-server` feature)
//!
//! Starts an embedded axum HTTP server that accepts POST requests on a
//! configurable path. Each body is split into records by `framing`, and its
//! records are queued together into a bounded `tokio::sync::mpsc` channel.
//! `recv()` drains from this channel.
//!
//! ## Shutdown
//!
//! The server answers 200 once a body's records are queued for `recv`, so a receiving
//! service shuts down in this order: `close()`, then `recv` until it returns
//! `TransportError::Closed`, then its final flush. `close()` answers new POSTs
//! with 503, which senders retry.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::http::{HttpTransport, HttpTransportConfig};
//!
//! // Send-only
//! let config = HttpTransportConfig {
//!     endpoint: Some("http://loader:8080/ingest".into()),
//!     ..Default::default()
//! };
//! let transport = HttpTransport::new(&config).await?;
//! transport.send("events", bytes::Bytes::from_static(b"{\"msg\":\"hello\"}")).await;
//! ```

mod send;
#[cfg(test)]
mod send_tests;
#[cfg(feature = "http-server")]
mod split;

pub use send::{HttpAuth, HttpEndpoint};

/// The receive-only `framing`: a JSON array or NDJSON, told apart by the body.
const JSON_FRAMING: &str = "json";

/// The codecs an HTTP request body takes.
pub const HTTP_ACCEPTS: Accepts = Accepts::new(
    "transport.http.compression",
    &[Codec::None, Codec::Gzip, Codec::Zstd],
);

use super::core::{Lifecycle, Meter};
use super::error::{TransportError, TransportResult};
#[cfg(feature = "http-server")]
use super::traits::RecvBatch;
use super::traits::{CommitToken, TransportBase, TransportReceiver, TransportSender};
#[cfg(feature = "http-server")]
use super::types::Message;
#[cfg(feature = "http-server")]
use super::types::PayloadFormat;
use super::types::SendResult;
use super::work_batch::WorkBatch;
use crate::compression::{Accepts, Codec, Compression};
use crate::tls_config::Removed;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[cfg(feature = "http-server")]
use std::sync::atomic::{AtomicU64, Ordering};

/// The spawned axum serve loop.
#[cfg(feature = "http-server")]
type ServerTask = tokio::task::JoinHandle<std::io::Result<()>>;

/// Commit token for HTTP transport.
///
/// HTTP is fire-and-forget from the receiver's perspective, so commit
/// is a no-op. The token provides sequence tracking and optional
/// client address for observability.
#[derive(Debug, Clone)]
pub struct HttpToken {
    /// Local sequence number (monotonically increasing per transport instance).
    pub seq: u64,

    /// Source client address (if available from the HTTP request).
    pub source_addr: Option<String>,
}

impl HttpToken {
    /// Create a new token with sequence number.
    #[must_use]
    pub fn new(seq: u64) -> Self {
        Self {
            seq,
            source_addr: None,
        }
    }

    /// Create a new token with sequence number and source address.
    #[must_use]
    pub fn with_source(seq: u64, addr: String) -> Self {
        Self {
            seq,
            source_addr: Some(addr),
        }
    }
}

impl CommitToken for HttpToken {}

impl std::fmt::Display for HttpToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source_addr {
            Some(addr) => write!(f, "http:{}:{}", addr, self.seq),
            None => write!(f, "http:{}", self.seq),
        }
    }
}

fn default_recv_path() -> String {
    "/ingest".to_string()
}

fn default_buffer_size() -> usize {
    10_000
}

fn default_recv_timeout_ms() -> u64 {
    100
}

fn default_connect_timeout_ms() -> u64 {
    5_000
}

fn default_send_timeout_ms() -> u64 {
    30_000
}

fn default_max_body_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_max_in_flight_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_framing() -> crate::framing::FramingConfig {
    crate::framing::FramingConfig {
        framing_type: "single".to_string(),
        ..crate::framing::FramingConfig::default()
    }
}

fn default_max_retry_after_ms() -> u64 {
    60_000
}

/// Configuration for HTTP transport.
#[derive(Clone, Serialize, Deserialize)]
pub struct HttpTransportConfig {
    /// Endpoint URL for sending (e.g., "http://loader:8080/ingest"). None = send disabled.
    /// `user:password@` in it is basic auth, and is never logged.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Endpoints tried in order after `endpoint`, each with its own `auth` or
    /// the transport's. Every request starts at `endpoint`.
    #[serde(default)]
    pub failover: Vec<HttpEndpoint>,

    /// Credentials for `endpoint`, and for each `failover` endpoint without its own.
    #[serde(default)]
    pub auth: HttpAuth,

    /// Headers sent on every request. Values are secrets: never logged.
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, crate::sensitive::SensitiveString>,

    /// `User-Agent` header. Unset sends none.
    #[serde(default)]
    pub user_agent: Option<String>,

    /// How records share a request body, sent and received. Default `single`,
    /// one record per request. Any [`crate::framing`] name, an app's own
    /// included, or `json`, which receives a JSON array or NDJSON and can't
    /// send. The receive side refuses a body of more than
    /// [`DEFAULT_MAX_RECORDS`](crate::framing::DecodeLimits::DEFAULT_MAX_RECORDS)
    /// records with 413, and takes one that does not frame as one record.
    #[serde(default = "default_framing")]
    pub framing: crate::framing::FramingConfig,

    /// Request body codec, sent as `Content-Encoding`: `none` (default),
    /// `gzip` or `zstd`. The receiver must decode it.
    #[serde(default)]
    pub compression: Compression,

    /// Statuses that mean the body is stored. Empty, the default, is any 2xx.
    #[serde(default)]
    pub success_status: Vec<u16>,

    /// `none` (default), `system` (the machine's proxy) or a proxy URL. See
    /// [`ProxyConfig`](crate::transport::ProxyConfig).
    #[serde(default)]
    pub proxy: crate::transport::ProxyConfig,

    /// Follow same-origin redirects, up to 5. Default off: a 3xx is the
    /// endpoint's fault and holds the records.
    #[serde(default)]
    pub follow_redirects: bool,

    /// Longest `Retry-After` wait taken, in ms. Default 60000. 0 ignores the header.
    #[serde(default = "default_max_retry_after_ms")]
    pub max_retry_after_ms: u64,

    /// Listen address for receiving (e.g., "0.0.0.0:8080"). None = receive disabled.
    /// Requires the `http-server` feature.
    #[serde(default)]
    pub listen: Option<String>,

    /// Path to accept POSTs on for receive mode. Default: "/ingest".
    #[serde(default = "default_recv_path")]
    pub recv_path: String,

    /// Requests queued for `recv` at once, each with all its records
    /// (bounded channel capacity). Default: 10000.
    #[serde(default = "default_buffer_size")]
    pub recv_buffer_size: usize,

    /// Receive timeout in milliseconds. Default: 100.
    #[serde(default = "default_recv_timeout_ms")]
    pub recv_timeout_ms: u64,

    /// Inbound message filters (applied on recv before caller sees messages).
    #[serde(default)]
    pub filters_in: Vec<super::filter::FilterRule>,

    /// Outbound message filters (applied on send before transport dispatches).
    #[serde(default)]
    pub filters_out: Vec<super::filter::FilterRule>,

    /// Connect timeout (ms) for the send-side HTTP client. Default 5000.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Total request timeout (ms) per send. Default 30000. Prevents a hung
    /// send from consuming worker capacity indefinitely.
    #[serde(default = "default_send_timeout_ms")]
    pub send_timeout_ms: u64,

    /// Maximum accepted request body size in bytes (receive side). Default
    /// 16 MiB. Oversized POSTs are rejected with 413 before buffering.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,

    /// Body bytes the receive side holds at once, read and queued for `recv`
    /// together. Default 64 MiB. A request is admitted only when its declared
    /// `Content-Length`, or `max_body_bytes` without one, fits what is left,
    /// and is answered 503 before its body is read otherwise. Must be at
    /// least `max_body_bytes`.
    #[serde(default = "default_max_in_flight_bytes")]
    pub max_in_flight_bytes: usize,

    /// Client TLS for `https://` endpoints, the [`ClientTlsConfig`] every
    /// transport reads: profile, trust, CA files, client certificate and key
    /// pins. The endpoint's host is the name checked, so `server_name` is
    /// refused.
    ///
    /// [`ClientTlsConfig`]: crate::tls::ClientTlsConfig
    #[serde(default)]
    pub tls: crate::tls::ClientTlsConfig,

    /// Removed: `tls.ca_paths`.
    #[doc(hidden)]
    #[serde(default, skip_serializing, deserialize_with = "removed_tls_ca_path")]
    pub tls_ca_path: Removed,

    /// Removed: `tls.trust`.
    #[doc(hidden)]
    #[serde(default, skip_serializing, deserialize_with = "removed_tls_trust")]
    pub tls_trust: Removed,

    /// Removed: `tls.client_cert_path`.
    #[doc(hidden)]
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "removed_tls_client_cert_path"
    )]
    pub tls_client_cert_path: Removed,

    /// Removed: `tls.client_key_path`.
    #[doc(hidden)]
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "removed_tls_client_key_path"
    )]
    pub tls_client_key_path: Removed,

    /// Removed: `tls.pin_sha256`.
    #[doc(hidden)]
    #[serde(default, skip_serializing, deserialize_with = "removed_tls_pin_sha256")]
    pub tls_pin_sha256: Removed,
}

crate::tls_config::removed_keys! {
    removed_tls_ca_path: "tls_ca_path" => "tls.ca_paths";
    removed_tls_trust: "tls_trust" => "tls.trust";
    removed_tls_client_cert_path: "tls_client_cert_path" => "tls.client_cert_path";
    removed_tls_client_key_path: "tls_client_key_path" => "tls.client_key_path";
    removed_tls_pin_sha256: "tls_pin_sha256" => "tls.pin_sha256";
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            failover: Vec::new(),
            auth: HttpAuth::None,
            headers: std::collections::BTreeMap::new(),
            user_agent: None,
            framing: default_framing(),
            compression: Compression::NONE,
            success_status: Vec::new(),
            proxy: crate::transport::ProxyConfig::None,
            follow_redirects: false,
            max_retry_after_ms: default_max_retry_after_ms(),
            listen: None,
            recv_path: default_recv_path(),
            recv_buffer_size: default_buffer_size(),
            recv_timeout_ms: default_recv_timeout_ms(),
            filters_in: Vec::new(),
            filters_out: Vec::new(),
            connect_timeout_ms: default_connect_timeout_ms(),
            send_timeout_ms: default_send_timeout_ms(),
            max_body_bytes: default_max_body_bytes(),
            max_in_flight_bytes: default_max_in_flight_bytes(),
            tls: crate::tls::ClientTlsConfig::default(),
            tls_ca_path: Removed,
            tls_trust: Removed,
            tls_client_cert_path: Removed,
            tls_client_key_path: Removed,
            tls_pin_sha256: Removed,
        }
    }
}

/// Written out so `endpoint` shows without its user name and password.
impl std::fmt::Debug for HttpTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransportConfig")
            .field("endpoint", &self.endpoint.as_deref().map(send::shown_text))
            .field("failover", &self.failover)
            .field("auth", &self.auth)
            .field("headers", &self.headers)
            .field("user_agent", &self.user_agent)
            .field("framing", &self.framing)
            .field("compression", &self.compression)
            .field("success_status", &self.success_status)
            .field("proxy", &self.proxy)
            .field("follow_redirects", &self.follow_redirects)
            .field("max_retry_after_ms", &self.max_retry_after_ms)
            .field("listen", &self.listen)
            .field("recv_path", &self.recv_path)
            .field("recv_buffer_size", &self.recv_buffer_size)
            .field("recv_timeout_ms", &self.recv_timeout_ms)
            .field("filters_in", &self.filters_in)
            .field("filters_out", &self.filters_out)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("send_timeout_ms", &self.send_timeout_ms)
            .field("max_body_bytes", &self.max_body_bytes)
            .field("max_in_flight_bytes", &self.max_in_flight_bytes)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl HttpTransportConfig {
    /// Load from the config cascade under the `transport.http` key, or the defaults
    /// when it is absent.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the section is present but does not parse.
    pub fn from_cascade() -> crate::transport::TransportResult<Self> {
        <Self as super::traits::FromCascade>::from_cascade_key("transport.http")
    }

    /// The client TLS the `tls` section names.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for half a client identity, a `server_name`,
    /// or refused profile settings.
    fn client_tls(&self) -> TransportResult<crate::tls::ClientTls> {
        let fail = |e: &dyn std::fmt::Display| TransportError::Config(format!("HTTP client {e}"));
        if self.tls.server_name.is_some() {
            return Err(fail(
                &"TLS: tls.server_name is not taken: the endpoint's host is the name checked",
            ));
        }
        self.tls.client_tls().map_err(|e| fail(&e))
    }

    /// Create a send-only config pointing at the given endpoint URL.
    #[must_use]
    pub fn sender(endpoint: &str) -> Self {
        Self {
            endpoint: Some(endpoint.to_string()),
            ..Default::default()
        }
    }

    /// Create a receive-only config listening on the given address.
    #[must_use]
    pub fn receiver(listen: &str) -> Self {
        Self {
            listen: Some(listen.to_string()),
            ..Default::default()
        }
    }
}

/// HTTP/HTTPS transport.
///
/// Supports send (POST to endpoint) and receive (embedded axum server).
/// The receive side requires the `http-server` feature for axum.
pub struct HttpTransport {
    /// The send side, `None` when no `endpoint` is set.
    sender: Option<send::Sender>,

    /// Receiver channel populated by the embedded HTTP server.
    /// Only available when `http-server` feature is enabled AND `listen` is configured.
    #[cfg(feature = "http-server")]
    receiver: Option<tokio::sync::Mutex<Inbox>>,

    /// Address the embedded server bound, with any port-0 request resolved.
    #[cfg(feature = "http-server")]
    local_addr: Option<std::net::SocketAddr>,

    /// One permit per body byte read or queued. `recv` returns a record's
    /// permits as it hands the record over.
    #[cfg(feature = "http-server")]
    in_flight: Option<Arc<tokio::sync::Semaphore>>,

    /// Graceful-shutdown signal for the server task. Behind a
    /// `Mutex<Option<..>>` so `close(&self)` can take and fire it.
    #[cfg(feature = "http-server")]
    shutdown_tx: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,

    /// Server task. `close()` awaits it and `Drop` aborts it: a dropped
    /// `JoinHandle` only detaches the task.
    #[cfg(feature = "http-server")]
    server_task: parking_lot::Mutex<Option<ServerTask>>,

    /// Closed, or refused by the proxy: either fails readiness, and a refusal
    /// lasts until a send goes through.
    lifecycle: Lifecycle,

    /// Receive timeout in milliseconds (used by receive side).
    #[cfg(feature = "http-server")]
    recv_timeout_ms: u64,

    /// Transport-level message filter engine.
    filter_engine: super::filter::TransportFilterEngine,
}

/// Builds an [`HttpTransport`] with what [`HttpTransportConfig`] does not
/// carry: the pressure governor and a connect resolver.
///
/// ```rust,ignore
/// let transport = HttpTransport::builder(&config)
///     .resolver(Arc::new(proven_routes))
///     .start()
///     .await?;
/// ```
#[must_use = "the transport starts only when `start` is awaited"]
pub struct HttpTransportBuilder<'a> {
    config: &'a HttpTransportConfig,
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
    resolver: Option<Arc<dyn super::ConnectResolver>>,
}

impl std::fmt::Debug for HttpTransportBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransportBuilder")
            .field("config", self.config)
            .field("resolver", &self.resolver.is_some())
            .finish_non_exhaustive()
    }
}

impl HttpTransportBuilder<'_> {
    /// Answer 503 on the receive side while `pressure` holds (`governor`
    /// feature), as [`HttpTransport::with_pressure`] does.
    #[cfg(feature = "governor")]
    pub fn pressure(mut self, pressure: Arc<crate::governor::UnifiedPressure>) -> Self {
        self.pressure = Some(pressure);
        self
    }

    /// Dial each endpoint where `resolver` says, asked at every connect. The
    /// send side then keeps no idle connection, so a changed answer takes
    /// the next request, and a `proxy` other than `none` fails
    /// [`start`](Self::start). See [`ConnectResolver`](super::ConnectResolver).
    pub fn resolver(mut self, resolver: Arc<dyn super::ConnectResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Bind the receive server and build the client, as configured.
    ///
    /// # Errors
    ///
    /// As [`HttpTransport::new`], and a resolver beside a proxy.
    pub async fn start(self) -> TransportResult<HttpTransport> {
        HttpTransport::new_inner(self).await
    }
}

impl HttpTransport {
    /// Create a new HTTP transport.
    ///
    /// - Set `config.endpoint` to enable sending (POST to URL).
    /// - Set `config.listen` to enable receiving (embedded HTTP server, requires `http-server` feature).
    ///
    /// # Errors
    ///
    /// Returns error if the listen address is invalid or the server fails to bind.
    pub async fn new(config: &HttpTransportConfig) -> TransportResult<Self> {
        Self::builder(config).start().await
    }

    /// A builder for the settings `config` does not carry.
    pub fn builder(config: &HttpTransportConfig) -> HttpTransportBuilder<'_> {
        HttpTransportBuilder {
            config,
            #[cfg(feature = "governor")]
            pressure: None,
            resolver: None,
        }
    }

    /// Create an HTTP transport bound to a pressure governor (`governor`
    /// feature).
    ///
    /// Identical to [`new`](Self::new) except the embedded receive server
    /// consults `pressure` BEFORE enqueuing each request: while
    /// [`UnifiedPressure::should_hold`](crate::governor::UnifiedPressure::should_hold)
    /// holds, the handler returns 503 (SERVICE_UNAVAILABLE) -- the same status
    /// the existing channel-full backpressure path uses. Passing `None` is
    /// exactly equivalent to [`new`](Self::new).
    ///
    /// # Errors
    ///
    /// Same as [`new`](Self::new).
    #[cfg(feature = "governor")]
    pub async fn with_pressure(
        config: &HttpTransportConfig,
        pressure: Option<Arc<crate::governor::UnifiedPressure>>,
    ) -> TransportResult<Self> {
        let builder = Self::builder(config);
        match pressure {
            Some(pressure) => builder.pressure(pressure),
            None => builder,
        }
        .start()
        .await
    }

    async fn new_inner(options: HttpTransportBuilder<'_>) -> TransportResult<Self> {
        let config = options.config;
        #[cfg(feature = "governor")]
        let pressure = options.pressure;
        let mut client_builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(config.connect_timeout_ms))
            .timeout(std::time::Duration::from_millis(config.send_timeout_ms));

        let sender = match config.endpoint.as_deref() {
            Some(endpoint) => {
                // A kept connection would outlive a moved answer.
                if let Some(resolver) = options.resolver {
                    super::resolver::refuse_proxy(&config.proxy, "HTTP")?;
                    client_builder = client_builder
                        .dns_resolver(super::resolver::ReqwestResolver(resolver))
                        .pool_max_idle_per_host(0);
                }
                let tls = config.client_tls()?;
                let settings = tls
                    .profile_settings()
                    .map_err(|e| TransportError::Config(format!("HTTP client {e}")))?;
                // Plain endpoints need no root store, which a scratch image may lack.
                let mut urls = std::iter::once(endpoint)
                    .chain(config.failover.iter().map(|failover| failover.url.as_str()));
                if urls.any(crate::tls::is_https) {
                    let tls_cfg = crate::tls::build_client_tls(&tls)
                        .map_err(|e| TransportError::Config(format!("HTTP client {e}")))?;
                    client_builder = client_builder.use_preconfigured_tls((*tls_cfg).clone());
                }
                Some(send::Sender::new(
                    config,
                    endpoint,
                    client_builder,
                    settings,
                )?)
            }
            None => None,
        };
        if sender.is_none() && !config.failover.is_empty() {
            return Err(TransportError::Config(
                "HTTP failover endpoints need an endpoint to fail over from".into(),
            ));
        }

        #[cfg(feature = "http-server")]
        let (receiver, local_addr, in_flight, shutdown_tx, server_handle) = if let Some(listen) =
            &config.listen
        {
            let addr: std::net::SocketAddr = listen
                .parse()
                .map_err(|e| TransportError::Config(format!("invalid listen address: {e}")))?;
            let in_flight = Arc::new(in_flight_budget(config)?);
            let split = split::BodySplit::new(&config.framing)?;

            let (tx, rx) = tokio::sync::mpsc::channel(config.recv_buffer_size);
            let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();

            let sequence = Arc::new(AtomicU64::new(0));
            let recv_path = config.recv_path.clone();

            let app = build_receiver_router(
                ReceiverState {
                    sender: tx,
                    sequence,
                    in_flight: Arc::clone(&in_flight),
                    max_body_bytes: config.max_body_bytes,
                    split,
                    decoders: Arc::new(BodyDecoders::new()?),
                    #[cfg(feature = "governor")]
                    pressure: pressure.clone(),
                },
                &recv_path,
            );

            let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
                TransportError::Connection(format!("failed to bind to {addr}: {e}"))
            })?;
            let bound = listener.local_addr().map_err(|e| {
                TransportError::Connection(format!("failed to read bound address for {addr}: {e}"))
            })?;

            let handle = tokio::spawn(async move {
                axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .with_graceful_shutdown(async {
                    sd_rx.await.ok();
                })
                .await
            });

            (
                Some(tokio::sync::Mutex::new(Inbox {
                    queue: rx,
                    partial: None,
                })),
                Some(bound),
                Some(in_flight),
                Some(sd_tx),
                Some(handle),
            )
        } else {
            (None, None, None, None, None)
        };

        // When the governor feature is on but http-server is off there is no
        // receive server to attach the pressure governor to. Consume it so the
        // signature stays uniform without an unused-variable warning.
        #[cfg(all(feature = "governor", not(feature = "http-server")))]
        let _ = pressure;

        #[cfg(feature = "logger")]
        tracing::info!(
            send = sender.is_some(),
            listen = ?config.listen,
            "HTTP transport opened"
        );

        // Fail loud on bad filter config -- silently disabling filters
        // turns a misconfigured `drop` / `dlq` rule into a permanent pass.
        let filter_engine = super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade()?,
        )?;

        let refusals = sender.as_ref().map_or_default(send::Sender::refusals);

        Ok(Self {
            sender,
            #[cfg(feature = "http-server")]
            receiver,
            #[cfg(feature = "http-server")]
            local_addr,
            #[cfg(feature = "http-server")]
            in_flight,
            #[cfg(feature = "http-server")]
            shutdown_tx: parking_lot::Mutex::new(shutdown_tx),
            #[cfg(feature = "http-server")]
            server_task: parking_lot::Mutex::new(server_handle),
            lifecycle: Lifecycle::new::<Self>(move || !refusals.is_refused()),
            #[cfg(feature = "http-server")]
            recv_timeout_ms: config.recv_timeout_ms,
            filter_engine,
        })
    }

    /// Address the embedded receive server is listening on, or `None` when
    /// `listen` is unset. Configure `listen` with port 0 to take any free port.
    #[cfg(feature = "http-server")]
    #[must_use]
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.local_addr
    }

    /// Stop the receive server without waiting on its clients.
    ///
    /// The graceful signal has every open connection finish its in-flight
    /// request in axum's own per-connection tasks, which outlive the serve task.
    /// Aborting the serve task frees the listener at once, so a client that
    /// never completes its request cannot hold `close()` open.
    #[cfg(feature = "http-server")]
    async fn stop_server(&self) {
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }
        let Some(task) = self.server_task.lock().take() else {
            return;
        };
        task.abort();
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "HTTP: server ended with an error"),
            Err(e) if e.is_cancelled() => {}
            Err(e) => tracing::warn!(error = %e, "HTTP: server task panicked"),
        }
    }
}

/// The receive side's body-byte budget, refusing a `max_in_flight_bytes` that
/// could never admit a `max_body_bytes` request.
#[cfg(feature = "http-server")]
fn in_flight_budget(config: &HttpTransportConfig) -> TransportResult<tokio::sync::Semaphore> {
    if u32::try_from(config.max_body_bytes).is_err() {
        return Err(TransportError::Config(format!(
            "max_body_bytes {} is over the {} a request can reserve",
            config.max_body_bytes,
            u32::MAX
        )));
    }
    if config.max_in_flight_bytes < config.max_body_bytes {
        return Err(TransportError::Config(format!(
            "max_in_flight_bytes {} is below max_body_bytes {}, so a request of the largest \
             accepted size could never be admitted",
            config.max_in_flight_bytes, config.max_body_bytes
        )));
    }
    if config.max_in_flight_bytes > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(TransportError::Config(format!(
            "max_in_flight_bytes {} is over {}",
            config.max_in_flight_bytes,
            tokio::sync::Semaphore::MAX_PERMITS
        )));
    }
    Ok(tokio::sync::Semaphore::new(config.max_in_flight_bytes))
}

/// Build the axum router for the receive side.
#[cfg(feature = "http-server")]
fn build_receiver_router(state: ReceiverState, recv_path: &str) -> axum::Router {
    use axum::routing::post;

    let max_body_bytes = state.max_body_bytes;
    axum::Router::new()
        .route(recv_path, post(ingest_handler))
        // Reject oversized bodies with 413 before the handler buffers them.
        .layer(axum::extract::DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
}

/// The receive queue, and the rest of the request `recv` last took part of.
#[cfg(feature = "http-server")]
struct Inbox {
    queue: tokio::sync::mpsc::Receiver<Queued>,
    partial: Option<Queued>,
}

/// One request's records, queued as one so a body is taken whole or not at all.
#[cfg(feature = "http-server")]
struct Queued {
    messages: std::vec::IntoIter<Message<HttpToken>>,
    /// In-flight bytes the request holds, returned as `recv` takes its last record.
    held: usize,
}

/// Shared state for the receive handler.
#[cfg(feature = "http-server")]
#[derive(Clone)]
struct ReceiverState {
    sender: tokio::sync::mpsc::Sender<Queued>,
    sequence: Arc<AtomicU64>,
    /// Body bytes read or queued, see [`HttpTransportConfig::max_in_flight_bytes`].
    in_flight: Arc<tokio::sync::Semaphore>,
    max_body_bytes: usize,
    /// How a body splits into records, from `framing`.
    split: split::BodySplit,
    /// Decoding contexts for a compressed request body, kept between requests.
    decoders: Arc<BodyDecoders>,
    /// Optional pressure governor (`governor` feature). `None` by default
    /// -> the handler never consults it. When `Some`, the handler rejects with
    /// 503 while `UnifiedPressure::should_hold` holds -- pressure-driven
    /// shedding ON TOP of the channel-full and byte-budget 503s.
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
}

/// One decoder pool per `Content-Encoding` the receive side reads.
#[cfg(feature = "http-server")]
struct BodyDecoders {
    gzip: crate::compression::CompressorPool,
    zstd: crate::compression::CompressorPool,
}

#[cfg(feature = "http-server")]
impl BodyDecoders {
    fn new() -> TransportResult<Self> {
        let pool = |codec| {
            crate::compression::CompressorPool::new(Compression::new(codec))
                .map_err(|e| TransportError::Config(e.to_string()))
        };
        Ok(Self {
            gzip: pool(Codec::Gzip)?,
            zstd: pool(Codec::Zstd)?,
        })
    }

    fn get(&self, codec: Codec) -> Option<&crate::compression::CompressorPool> {
        match codec {
            Codec::Gzip => Some(&self.gzip),
            Codec::Zstd => Some(&self.zstd),
            _ => None,
        }
    }
}

/// The codec a request's `Content-Encoding` names: none when absent or
/// `identity`, gzip or zstd. Anything else, a list of codings included, is
/// `None`, answered 415.
#[cfg(feature = "http-server")]
fn body_codec(headers: &axum::http::HeaderMap) -> Option<Codec> {
    let mut values = headers.get_all(axum::http::header::CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return Some(Codec::None);
    };
    if values.next().is_some() {
        return None;
    }
    match value.to_str().ok()?.trim().to_ascii_lowercase().as_str() {
        "" | "identity" => Some(Codec::None),
        "gzip" | "x-gzip" => Some(Codec::Gzip),
        "zstd" => Some(Codec::Zstd),
        _ => None,
    }
}

/// `body` decoded, held to `limit` bytes, on the blocking pool.
#[cfg(feature = "http-server")]
async fn decode_body(
    decoders: Arc<BodyDecoders>,
    codec: Codec,
    body: bytes::Bytes,
    limit: usize,
) -> std::io::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let pool = decoders
            .get(codec)
            .ok_or_else(|| std::io::Error::other(format!("no decoder for {codec}")))?;
        let mut decoded = Vec::new();
        pool.decompress_into(&body, &mut decoded, limit)?;
        Ok(decoded)
    })
    .await
    .map_err(std::io::Error::other)?
}

/// POST handler that accepts raw bytes and queues their records into the mpsc channel.
///
/// A `Content-Encoding` of gzip or zstd is decoded first, to at most
/// `max_body_bytes`, and the decoded bytes are what the in-flight budget
/// counts. Any other coding is a 415, answered before the body is read.
///
/// The pressure check and the byte reservation come before the body is read,
/// so a held governor or a spent budget costs no body buffer. Answers 200 once
/// the records are queued. A full queue, a held inbound gate, a spent byte budget
/// and a closed receiver answer 503, which a sender retries.
#[cfg(feature = "http-server")]
async fn ingest_handler(
    axum::extract::State(state): axum::extract::State<ReceiverState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::extract::FromRequest as _;
    use axum::response::IntoResponse as _;

    // Pressure-driven shedding (governor feature, opt-in), before any body
    // byte is read -- consistent with the channel-full 503 below (NOT 429).
    #[cfg(feature = "governor")]
    if let Some(pressure) = &state.pressure
        && pressure.should_hold()
    {
        #[cfg(feature = "metrics")]
        metrics::counter!("transport_backpressured_total", "transport" => "http", "reason" => "pressure")
            .increment(1);
        return shed_503();
    }

    let Some(codec) = body_codec(request.headers()) else {
        return axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    };

    let declared = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    match declared {
        Some(0) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
        Some(n) if n > state.max_body_bytes => {
            return axum::http::StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
        _ => {}
    }
    // A body with no Content-Length can run to max_body_bytes.
    let reserved = declared.unwrap_or(state.max_body_bytes);
    let Some(mut permit) = reserve(&state.in_flight, reserved) else {
        return budget_spent();
    };

    // Extract W3C traceparent from incoming HTTP headers for distributed tracing
    #[cfg(feature = "transport-trace")]
    if let Some(tp) = request
        .headers()
        .get(super::propagation::TRACEPARENT_HEADER)
        .and_then(|v| v.to_str().ok())
        && super::propagation::is_valid_traceparent(tp)
    {
        tracing::Span::current().record("traceparent", tp);
    }

    // Under the DefaultBodyLimit layer, a body over max_body_bytes is a 413.
    let body = match axum::body::Bytes::from_request(request, &()).await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };
    if body.is_empty() {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    // Keep exactly the bytes read. A short body returns the rest of its reservation.
    drop(permit.split(permit.num_permits().saturating_sub(body.len())));

    let body = if codec == Codec::None {
        body
    } else {
        // Decoding holds the body and up to `max_body_bytes` decoded at once.
        let Some(decoding) = reserve(&state.in_flight, state.max_body_bytes) else {
            return budget_spent();
        };
        permit.merge(decoding);
        let decoded = match decode_body(
            Arc::clone(&state.decoders),
            codec,
            body,
            state.max_body_bytes,
        )
        .await
        {
            Ok(decoded) => decoded,
            Err(e) if e.kind() == std::io::ErrorKind::FileTooLarge => {
                return axum::http::StatusCode::PAYLOAD_TOO_LARGE.into_response();
            }
            Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
        };
        if decoded.is_empty() {
            return axum::http::StatusCode::BAD_REQUEST.into_response();
        }
        // The compressed body is gone: keep exactly the decoded bytes.
        drop(permit.split(permit.num_permits().saturating_sub(decoded.len())));
        bytes::Bytes::from(decoded)
    };

    queue(&state, permit, body, addr)
}

/// Split `body` into its records by `framing` and queue them together,
/// answering 200 once they are queued. `permit` holds the body's bytes of the
/// in-flight budget.
///
/// A body over the record cap is 413, naming the cap, and nothing of it is
/// queued, so its sender can retry it smaller.
#[cfg(feature = "http-server")]
fn queue(
    state: &ReceiverState,
    mut permit: tokio::sync::OwnedSemaphorePermit,
    body: bytes::Bytes,
    addr: std::net::SocketAddr,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let source = addr.to_string();
    let timestamp_ms = Some(crate::clock::epoch_millis());
    let message = |payload: bytes::Bytes, seq: u64| Message {
        key: None,
        format: PayloadFormat::detect(&payload),
        payload,
        token: HttpToken::with_source(seq, source.clone()),
        timestamp_ms,
        headers: Vec::new(),
    };
    let body_len = body.len();
    let messages: Vec<Message<HttpToken>> = match state.split.parts(&body) {
        split::BodyParts::Whole => {
            vec![message(
                body,
                state.sequence.fetch_add(1, Ordering::Relaxed),
            )]
        }
        // An empty array: nothing to queue, and its bytes return as the permit drops.
        split::BodyParts::Each(records) if records.is_empty() => {
            return axum::http::StatusCode::OK.into_response();
        }
        split::BodyParts::Each(records) => {
            // Each record's message and source address are memory the body's bytes do not count.
            let overhead = records
                .len()
                .saturating_mul(size_of::<Message<HttpToken>>() + source.len());
            let Some(more) = reserve(&state.in_flight, overhead) else {
                return budget_spent();
            };
            permit.merge(more);
            let first = state
                .sequence
                .fetch_add(records.len() as u64, Ordering::Relaxed);
            records
                .iter()
                .zip(first..)
                .map(|(record, seq)| message(split::slice_of(&body, record), seq))
                .collect()
        }
        split::BodyParts::TooMany(max) => {
            let answer = format!("the body holds more than {max} records: send fewer per request");
            return (axum::http::StatusCode::PAYLOAD_TOO_LARGE, answer).into_response();
        }
    };

    let records = messages.len();
    let queued = Queued {
        messages: messages.into_iter(),
        held: permit.num_permits(),
    };
    match state.sender.try_send(queued) {
        Ok(()) => {
            // The queued records hold their bytes until `recv` takes the last of them.
            permit.forget();
            HttpTransport::count_received(records as u64, body_len as u64);
            axum::http::StatusCode::OK.into_response()
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            HttpTransport::count_backpressured();
            shed_503()
        }
        // A closed receiver is shutting down: 503 so the sender retries, not drops.
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            HttpTransport::count_refused();
            shed_503()
        }
    }
}

/// `bytes` more of the in-flight budget, or `None` when they do not fit.
#[cfg(feature = "http-server")]
fn reserve(
    in_flight: &Arc<tokio::sync::Semaphore>,
    bytes: usize,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    u32::try_from(bytes)
        .ok()
        .and_then(|n| Arc::clone(in_flight).try_acquire_many_owned(n).ok())
}

/// The 503 for a spent in-flight budget, counted.
#[cfg(feature = "http-server")]
fn budget_spent() -> axum::response::Response {
    #[cfg(feature = "metrics")]
    metrics::counter!("transport_backpressured_total", "transport" => "http", "reason" => "in_flight_bytes")
        .increment(1);
    shed_503()
}

/// 503 shed response with a `Retry-After: 1` hint, so a well-behaved sender
/// backs off briefly instead of hot-retrying into a pod that is already holding.
#[cfg(feature = "http-server")]
fn shed_503() -> axum::response::Response {
    use axum::response::IntoResponse as _;
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::RETRY_AFTER, "1")],
    )
        .into_response()
}

impl TransportBase for HttpTransport {
    /// Stop sending and receiving, keeping every record already acknowledged.
    ///
    /// A POST that arrives from here on is answered 503, which a sender
    /// retries. Records the server already answered 200 for stay queued: call
    /// [`recv`](TransportReceiver::recv) until it returns
    /// [`TransportError::Closed`] or they are lost. Open connections finish
    /// their in-flight requests on their own, and the listener is free when this
    /// returns. Waits for a `recv` in progress (at most `recv_timeout_ms`).
    /// Idempotent.
    async fn close(&self) -> TransportResult<()> {
        self.lifecycle.close();

        #[cfg(feature = "http-server")]
        {
            if let Some(receiver) = &self.receiver {
                receiver.lock().await.queue.close();
            }
            self.stop_server().await;
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.lifecycle.is_healthy()
    }

    fn name(&self) -> &'static str {
        Self::TRANSPORT
    }

    fn delivery_guarantee(&self) -> super::DeliveryGuarantee {
        super::DeliveryGuarantee::Acknowledged
    }
}

impl HttpTransport {
    /// Send `records`, the block answering by `deadline` when there is one,
    /// and each body by `send_timeout_ms` when there is not.
    async fn send_block(
        &self,
        records: impl Iterator<Item = (&str, &bytes::Bytes)>,
        deadline: Option<std::time::Instant>,
    ) -> SendResult {
        if self.lifecycle.is_closed() {
            return SendResult::Fatal(TransportError::Closed);
        }
        let (kept, handled) = self.filter_engine.partition_outbound(
            records.map(|(destination, payload)| (destination, payload.as_ref())),
            |(_, payload)| payload,
        );
        if kept.is_empty() {
            return handled;
        }
        let Some(sender) = &self.sender else {
            return SendResult::Fatal(TransportError::Config(
                "no endpoint configured for sending".into(),
            ));
        };
        let sent = match deadline {
            None => sender.send(&kept, None).await,
            Some(deadline) => {
                let until = tokio::time::Instant::from_std(deadline);
                tokio::time::timeout_at(until, sender.send(&kept, Some(deadline)))
                    .await
                    .unwrap_or_else(|_elapsed| {
                        tracing::warn!(
                            records = kept.len(),
                            "HTTP send deadline elapsed; the records may have arrived and are \
                             sent again"
                        );
                        SendResult::Backpressured
                    })
            }
        };
        if sent.is_ok() { handled } else { sent }
    }
}

impl Meter for HttpTransport {
    const TRANSPORT: &'static str = "http";
}

impl TransportSender for HttpTransport {
    /// A record an outbound `dlq` filter matches.
    fn dead_letter_reason(
        &self,
        record: &super::work_batch::Record,
    ) -> Option<super::ack::DeadLetterReason> {
        self.filter_engine
            .outbound_dead_letter(&record.payload)
            .then_some(super::ack::DeadLetterReason::OutboundFilter)
    }

    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        self.send_block(std::iter::once((destination, &payload)), None)
            .await
    }

    /// One body per run of records with the same key, each by `send_timeout_ms`.
    async fn send_batch(&self, records: &[super::work_batch::Record]) -> SendResult {
        self.send_block(keyed(records), None).await
    }

    /// One body per run of records with the same key. Each request's timeout
    /// is its share of the time left, so failover endpoints all get a turn.
    async fn send_batch_by(
        &self,
        records: &[super::work_batch::Record],
        deadline: std::time::Instant,
    ) -> SendResult {
        self.send_block(keyed(records), Some(deadline)).await
    }
}

/// Each record's destination, its key or none, beside its payload.
fn keyed(records: &[super::work_batch::Record]) -> impl Iterator<Item = (&str, &bytes::Bytes)> {
    records
        .iter()
        .map(|r| (r.key.as_deref().unwrap_or(""), &r.payload))
}

impl TransportReceiver for HttpTransport {
    type Token = HttpToken;

    /// Receive up to `max` records the server has acked.
    ///
    /// After [`close`](TransportBase::close) this keeps returning the records
    /// still queued, then [`TransportError::Closed`] once none are left.
    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
        #[cfg(feature = "http-server")]
        {
            use tokio::sync::mpsc::error::TryRecvError;

            let Some(receiver) = &self.receiver else {
                if self.lifecycle.is_closed() {
                    return Err(TransportError::Closed);
                }
                return Err(TransportError::Config(
                    "no listen address configured for receiving".into(),
                ));
            };

            let mut inbox = receiver.lock().await;
            let inbox = &mut *inbox;
            let mut messages = Vec::with_capacity(max.min(100));

            while messages.len() < max {
                let mut queued = match inbox.partial.take() {
                    Some(queued) => queued,
                    // The first record waits up to recv_timeout_ms. The rest only
                    // take what is already queued.
                    None if self.recv_timeout_ms == 0 || !messages.is_empty() => {
                        match inbox.queue.try_recv() {
                            Ok(queued) => queued,
                            Err(TryRecvError::Disconnected) if messages.is_empty() => {
                                return Err(TransportError::Closed);
                            }
                            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                        }
                    }
                    None => match tokio::time::timeout(
                        std::time::Duration::from_millis(self.recv_timeout_ms),
                        inbox.queue.recv(),
                    )
                    .await
                    {
                        Ok(Some(queued)) => queued,
                        Ok(None) => return Err(TransportError::Closed),
                        Err(_elapsed) => break,
                    },
                };
                messages.extend(queued.messages.by_ref().take(max - messages.len()));
                if !queued.messages.as_slice().is_empty() {
                    inbox.partial = Some(queued);
                } else if let Some(in_flight) = &self.in_flight {
                    in_flight.add_permits(queued.held);
                }
            }

            // Apply inbound filters via the shared partition helper. DLQ
            // entries are returned in the RecvBatch for the caller to route.
            let batch = self.filter_engine.partition_batch(
                messages,
                |m| m.payload.as_ref(),
                |m| m.key.clone(),
                |m| m.token.clone(),
            );
            let messages = batch.messages;
            let dlq_entries = batch.dlq_entries;
            let filtered_tokens = batch.filtered_tokens;

            #[cfg(feature = "logger")]
            if !messages.is_empty() {
                tracing::debug!(messages = messages.len(), "HTTP transport: batch received");
            }

            Ok(RecvBatch {
                messages,
                dlq_entries,
                filtered_tokens,
            }
            .into())
        }

        #[cfg(not(feature = "http-server"))]
        {
            let _ = max;
            if self.lifecycle.is_closed() {
                return Err(TransportError::Closed);
            }
            Err(TransportError::Config(
                "HTTP receive requires the 'http-server' feature".into(),
            ))
        }
    }

    async fn commit(&self, _tokens: &[Self::Token]) -> TransportResult<()> {
        // HTTP is fire-and-forget -- commit is a no-op.
        Ok(())
    }
}

impl Drop for HttpTransport {
    fn drop(&mut self) {
        // Abort explicitly: dropping the handle would detach the serve task.
        #[cfg(feature = "http-server")]
        if let Some(task) = self.server_task.get_mut().take() {
            task.abort();
        }
    }
}

impl super::traits::FromCascade for HttpTransportConfig {}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "http-server")]
    use std::sync::atomic::AtomicBool;

    #[test]
    fn http_token_display() {
        let token = HttpToken::new(42);
        assert_eq!(format!("{token}"), "http:42");
    }

    #[test]
    fn http_token_display_with_source() {
        let token = HttpToken::with_source(7, "192.168.1.1:54321".to_string());
        assert_eq!(format!("{token}"), "http:192.168.1.1:54321:7");
    }

    #[test]
    fn config_defaults() {
        let config = HttpTransportConfig::default();
        assert!(config.endpoint.is_none());
        assert!(config.listen.is_none());
        assert_eq!(config.recv_path, "/ingest");
        assert_eq!(config.recv_buffer_size, 10_000);
        assert_eq!(config.recv_timeout_ms, 100);
    }

    #[test]
    fn config_sender_helper() {
        let config = HttpTransportConfig::sender("http://localhost:8080/ingest");
        assert_eq!(
            config.endpoint.as_deref(),
            Some("http://localhost:8080/ingest")
        );
        assert!(config.listen.is_none());
    }

    /// Every `tls` key reaches the config through the cascade, from YAML and
    /// from the environment, which wins, and a single CA path reads as a list.
    #[cfg(feature = "config")]
    #[test]
    fn tls_keys_reach_the_config_through_the_cascade() {
        use crate::tls::{ClientTlsConfig, CryptoProfile, KeyShare, TrustChoice};

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "transport:\n  http:\n    tls:\n      profile: commercial\n      trust: bundle\n      \
             ca_paths: /etc/tls/ca.pem\n      client_cert_path: /etc/tls/client.pem\n      \
             client_key_path: /etc/tls/yaml.key\n      key_share: classical\n      \
             pin_sha256: [\"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\"]\n",
        )
        .unwrap();
        temp_env::with_vars(
            [
                ("HTTPTLS__TRANSPORT__HTTP__TLS__TRUST", Some("platform")),
                (
                    "HTTPTLS__TRANSPORT__HTTP__TLS__CLIENT_KEY_PATH",
                    Some("/etc/tls/env.key"),
                ),
            ],
            || {
                crate::config::setup(crate::config::ConfigOptions {
                    env_prefix: "HTTPTLS".to_string(),
                    config_paths: vec![dir.path().to_path_buf()],
                    search_standard_paths: false,
                    load_dotenv: false,
                    ..crate::config::ConfigOptions::default()
                })
                .unwrap();
            },
        );
        let config = HttpTransportConfig::from_cascade().unwrap();
        assert_eq!(
            config.tls,
            ClientTlsConfig {
                profile: CryptoProfile::Commercial,
                trust: TrustChoice::Platform,
                ca_paths: vec!["/etc/tls/ca.pem".into()],
                client_cert_path: Some("/etc/tls/client.pem".into()),
                client_key_path: Some("/etc/tls/env.key".into()),
                pin_sha256: vec![
                    "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
                        .parse()
                        .unwrap()
                ],
                server_name: None,
                key_share: KeyShare::Classical,
                ..ClientTlsConfig::default()
            }
        );
    }

    /// A sink with only `http://` endpoints builds no TLS, so it starts where
    /// no root store can be read, and an `https://` endpoint, failover
    /// included, still needs one.
    #[tokio::test]
    async fn only_an_https_endpoint_builds_tls() {
        let no_roots = crate::tls::ClientTlsConfig {
            trust: crate::tls::TrustChoice::Bundle,
            ..crate::tls::ClientTlsConfig::default()
        };
        let config = |endpoint: &str, failover: &[&str]| HttpTransportConfig {
            endpoint: Some(endpoint.to_string()),
            failover: failover
                .iter()
                .map(|url| HttpEndpoint {
                    url: (*url).to_string(),
                    auth: None,
                })
                .collect(),
            tls: no_roots.clone(),
            ..HttpTransportConfig::default()
        };
        let cases = [
            (
                "plain",
                config("http://127.0.0.1:9", &["http://127.0.0.1:10"]),
                None,
            ),
            (
                "https",
                config("HTTPS://127.0.0.1:9", &[]),
                Some("no trusted root"),
            ),
            (
                "https failover",
                config("http://127.0.0.1:9", &["https://127.0.0.1:10"]),
                Some("no trusted root"),
            ),
        ];
        for (name, config, refused) in cases {
            let built = HttpTransport::new(&config).await;
            match (refused, built) {
                (None, Ok(_)) => {}
                (Some(want), Err(TransportError::Config(message))) => {
                    assert!(message.contains(want), "{name}: {message}");
                }
                (_, other) => panic!("{name}: {:?}", other.err()),
            }
        }
    }

    /// A removed TLS key fails the load, naming its replacement, where the
    /// sink would send with the key dropped.
    #[cfg(all(feature = "yaml", feature = "figment"))]
    #[test]
    fn a_removed_tls_key_fails_the_load() {
        crate::tls_config::assert_removed::<HttpTransportConfig>(&[
            (r#"{"tls_ca_path": "/ca.pem"}"#, "tls.ca_paths"),
            (r#"{"tls_trust": "bundle"}"#, "tls.trust"),
            (
                r#"{"tls_client_cert_path": "/c.pem"}"#,
                "tls.client_cert_path",
            ),
            (
                r#"{"tls_client_key_path": "/k.pem"}"#,
                "tls.client_key_path",
            ),
            (r#"{"tls_pin_sha256": ["x"]}"#, "tls.pin_sha256"),
        ]);
    }

    /// The `tls` section refuses what HTTP cannot honour, and a key the
    /// section does not have, at load.
    #[test]
    fn the_tls_section_refuses_what_http_cannot_honour() {
        let refused = [
            (
                "a server name",
                serde_json::json!({"tls": {"server_name": "other.example"}}),
                "tls.server_name is not taken",
            ),
            (
                "half an identity",
                serde_json::json!({"tls": {"client_cert_path": "c.pem"}}),
                "needs both client_cert_path and client_key_path",
            ),
            (
                "a malformed pin",
                serde_json::json!({"tls": {"pin_sha256": ["not a pin"]}}),
                "\"not a pin\" is not a SHA-256",
            ),
            (
                "the old one-file ca_path key",
                serde_json::json!({"tls": {"ca_path": "ca.pem"}}),
                "`ca_path` is removed: set ca_paths",
            ),
            (
                "the fips profile",
                serde_json::json!({"tls": {"profile": "fips"}}),
                "needs a FIPS 140-3 build",
            ),
        ];
        for (name, value, want) in refused {
            let got = serde_json::from_value::<HttpTransportConfig>(value)
                .map_err(|e| e.to_string())
                .and_then(|config| config.client_tls().map(drop).map_err(|e| e.to_string()));
            assert!(
                got.as_ref().is_err_and(|e| e.contains(want)),
                "{name}: {got:?}"
            );
        }
    }

    /// A send over HTTPS to a server that asks for a client certificate, under
    /// each trust choice, with and without the identity.
    #[tokio::test]
    async fn https_send_uses_the_trust_and_identity_configured() {
        use crate::tls::TrustChoice;
        use crate::tls::test_pki::TestCa;

        let ca = TestCa::new("http transport test CA");
        let server_leaf = ca.server("http server");
        let client_leaf = ca.client("http client");
        let dir = tempfile::tempdir().unwrap();
        let file = |name: &str, text: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, text).unwrap();
            path.to_string_lossy().into_owned()
        };
        let ca_path: std::path::PathBuf = file("ca.pem", &ca.pem()).into();
        let cert: std::path::PathBuf = file("client.pem", &client_leaf.cert_pem).into();
        let key: std::path::PathBuf = file("client.key", &client_leaf.key_pem).into();
        let addr = crate::tls::test_pki::mtls_ok_server(&ca, &server_leaf).await;
        let endpoint = format!("https://localhost:{}", addr.port());

        let cases = [
            ("bundle with the identity", TrustChoice::Bundle, true, true),
            ("native with the identity", TrustChoice::Native, true, true),
            (
                "platform with the identity",
                TrustChoice::Platform,
                true,
                true,
            ),
            (
                "bundle without the identity",
                TrustChoice::Bundle,
                false,
                false,
            ),
        ];
        for (name, trust, identity, delivered) in cases {
            let config = HttpTransportConfig {
                tls: crate::tls::ClientTlsConfig {
                    trust,
                    ca_paths: vec![ca_path.clone()],
                    client_cert_path: identity.then(|| cert.clone()),
                    client_key_path: identity.then(|| key.clone()),
                    ..crate::tls::ClientTlsConfig::default()
                },
                ..HttpTransportConfig::sender(&endpoint)
            };
            let transport = HttpTransport::new(&config).await.unwrap();
            let result = transport
                .send("ingest", bytes::Bytes::from_static(b"{}"))
                .await;
            assert_eq!(result.is_ok(), delivered, "{name}: {result:?}");
        }

        let missing = HttpTransportConfig {
            tls: crate::tls::ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_paths: vec![dir.path().join("gone.pem")],
                ..crate::tls::ClientTlsConfig::default()
            },
            ..HttpTransportConfig::sender(&endpoint)
        };
        let error = HttpTransport::new(&missing)
            .await
            .err()
            .map(|e| e.to_string());
        assert!(
            error.as_deref().is_some_and(|e| e.contains("gone.pem")),
            "a missing CA file fails the build: {error:?}"
        );
    }

    #[test]
    fn config_receiver_helper() {
        let config = HttpTransportConfig::receiver("0.0.0.0:8080");
        assert!(config.endpoint.is_none());
        assert_eq!(config.listen.as_deref(), Some("0.0.0.0:8080"));
    }

    #[tokio::test]
    async fn send_only_transport() {
        // Send-only config (no endpoint = send disabled, but transport creates fine)
        let config = HttpTransportConfig::default();
        let transport = HttpTransport::new(&config).await.unwrap();

        assert!(transport.is_healthy());
        assert_eq!(transport.name(), "http");

        // Send without endpoint should fail
        let result = transport
            .send("test", bytes::Bytes::from_static(b"payload"))
            .await;
        assert!(result.is_fatal());

        // Commit is always ok
        transport.commit(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn close_prevents_send() {
        let config = HttpTransportConfig::sender("http://localhost:19999/test");
        let transport = HttpTransport::new(&config).await.unwrap();

        transport.close().await.unwrap();
        assert!(!transport.is_healthy());

        let result = transport
            .send("test", bytes::Bytes::from_static(b"data"))
            .await;
        assert!(result.is_fatal());
    }

    /// An endpoint that is down is waited out, not treated as a dead transport.
    #[tokio::test]
    async fn a_refused_connection_is_backpressure_not_fatal() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("bind an ephemeral port")
            .port();
        // The listener is dropped above, so the port now refuses connections.
        let transport = HttpTransport::new(&HttpTransportConfig::sender(&format!(
            "http://127.0.0.1:{port}"
        )))
        .await
        .unwrap();
        let result = transport
            .send("ingest", bytes::Bytes::from_static(b"{}"))
            .await;
        assert!(
            result.is_backpressured(),
            "a refused connection must be retried, got {result:?}"
        );
    }

    /// A record spooled while the endpoint answers 500 stays spooled through
    /// the 500s and is delivered once the endpoint recovers.
    #[cfg(feature = "tiered-sink")]
    #[tokio::test]
    async fn a_spooled_record_survives_a_server_error_and_is_delivered() {
        use crate::tiered_sink::{TieredSink, TieredSinkConfig};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU16, Ordering};

        let status = Arc::new(AtomicU16::new(500));
        let addr = switchable_endpoint(Arc::clone(&status)).await;
        let sender = HttpTransport::new(&HttpTransportConfig::sender(&format!("http://{addr}")))
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut config = TieredSinkConfig::new(dir.path().join("q"));
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 20;
        config.drain_interval_ms = 5;
        let tiered = TieredSink::new(sender, config).await.unwrap();

        let record = crate::transport::Record {
            payload: bytes::Bytes::from_static(b"{\"keep\":1}"),
            key: None,
            headers: Vec::new(),
            metadata: crate::transport::RecordMeta {
                timestamp_ms: None,
                format: crate::transport::PayloadFormat::Json,
            },
        };
        tiered.send(&record).await.expect("spooled on the 500");
        assert_eq!(tiered.spool_len().await, 1, "the 500 spools the record");

        // The drainer offers it to the failing endpoint many times over.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            tiered.spool_len().await,
            1,
            "a server error must not remove the spooled record"
        );

        status.store(200, Ordering::SeqCst);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while tiered.spool_len().await > 0 {
            assert!(std::time::Instant::now() < deadline, "never drained");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tiered.shutdown().await;
    }

    /// A loopback endpoint that answers every request with whatever `status`
    /// holds at the time.
    #[cfg(feature = "tiered-sink")]
    async fn switchable_endpoint(
        status: std::sync::Arc<std::sync::atomic::AtomicU16>,
    ) -> std::net::SocketAddr {
        use std::sync::atomic::Ordering;
        send_tests::Server::start(move |_| send_tests::reply(status.load(Ordering::SeqCst), &[]))
            .await
            .addr
    }

    #[tokio::test]
    async fn close_prevents_recv() {
        let config = HttpTransportConfig::default();
        let transport = HttpTransport::new(&config).await.unwrap();

        transport.close().await.unwrap();
        let result = transport.recv(1).await;
        assert!(result.is_err());
    }

    /// Full send + receive round-trip test.
    /// Requires both `transport-http` and `http-server` features.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn send_and_receive_roundtrip() {
        // Port 0 binds a free port atomically. A probe-then-rebind races
        // parallel tests for the same port.
        let recv_config = HttpTransportConfig {
            listen: Some("127.0.0.1:0".to_string()),
            recv_path: "/ingest".to_string(),
            recv_buffer_size: 100,
            recv_timeout_ms: 1000,
            ..Default::default()
        };
        let receiver = HttpTransport::new(&recv_config).await.unwrap();
        let addr = receiver.local_addr().expect("receiver bound");

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send a message via a separate sender transport
        let send_config =
            HttpTransportConfig::sender(&format!("http://127.0.0.1:{}/ingest", addr.port()));
        let sender = HttpTransport::new(&send_config).await.unwrap();

        let result = sender
            .send("", bytes::Bytes::from_static(b"{\"msg\":\"hello\"}"))
            .await;
        assert!(result.is_ok(), "send failed: {result:?}");

        // Receive it
        let batch = receiver.recv(10).await.unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].payload.as_ref(), b"{\"msg\":\"hello\"}");
        // The source address rides on the batch commit token, not the record.
        assert!(batch.commit_tokens[0].source_addr.is_some());

        // Cleanup
        sender.close().await.unwrap();
        receiver.close().await.unwrap();
    }

    /// Test that the receiver rejects empty bodies.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn receive_rejects_empty_body() {
        let recv_config = HttpTransportConfig {
            listen: Some("127.0.0.1:0".to_string()),
            recv_timeout_ms: 200,
            ..Default::default()
        };
        let receiver = HttpTransport::new(&recv_config).await.unwrap();
        let addr = receiver.local_addr().expect("receiver bound");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send empty body
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/ingest", addr.port()))
            .body(Vec::<u8>::new())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

        // recv should timeout with no messages
        let records = receiver.recv(10).await.unwrap().records;
        assert_eq!(records, [] as [crate::transport::work_batch::Record; 0]);

        receiver.close().await.unwrap();
    }

    /// Oversized bodies are rejected with 413 before the handler buffers them.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn receive_rejects_oversized_body() {
        let recv_config = HttpTransportConfig {
            listen: Some("127.0.0.1:0".to_string()),
            recv_timeout_ms: 200,
            max_body_bytes: 1024, // 1 KiB cap
            ..Default::default()
        };
        let receiver = HttpTransport::new(&recv_config).await.unwrap();
        let addr = receiver.local_addr().expect("receiver bound");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // POST 8 KiB -- over the 1 KiB cap.
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{}/ingest", addr.port()))
            .body(vec![b'x'; 8 * 1024])
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);

        // The oversized POST never reached the queue.
        let records = receiver.recv(10).await.unwrap().records;
        assert!(records.is_empty(), "oversized body must not be queued");

        receiver.close().await.unwrap();
    }

    /// A scalo sender's gzip and zstd bodies arrive decoded. Any other
    /// `Content-Encoding` is a 415, a body that decodes past `max_body_bytes`
    /// a 413 and one that does not decode a 400, and none of those queues
    /// anything.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn compressed_bodies_decode_and_refusals_queue_nothing() {
        use crate::compression::{Codec, Compression};

        enum Post {
            Scalo(Codec),
            Raw(&'static str, Vec<u8>, u16),
        }
        let receiver = HttpTransport::new(&HttpTransportConfig {
            listen: Some("127.0.0.1:0".to_string()),
            recv_timeout_ms: 200,
            max_body_bytes: 1024,
            ..Default::default()
        })
        .await
        .unwrap();
        let addr = receiver.local_addr().expect("receiver bound");
        let bomb = Compression::new(Codec::Gzip)
            .compress(&[b'x'; 4096])
            .unwrap();
        assert!(bomb.len() < 1024, "the body itself is under the cap");
        let rows = [
            ("scalo gzip", Post::Scalo(Codec::Gzip)),
            ("scalo zstd", Post::Scalo(Codec::Zstd)),
            ("scalo none", Post::Scalo(Codec::None)),
            ("brotli", Post::Raw("br", b"{}".to_vec(), 415)),
            ("two codings", Post::Raw("gzip, zstd", b"{}".to_vec(), 415)),
            ("decodes past the cap", Post::Raw("gzip", bomb, 413)),
            ("not gzip", Post::Raw("gzip", b"{\"a\":1}".to_vec(), 400)),
        ];
        let client = reqwest::Client::new();
        for (case, post) in rows {
            match post {
                Post::Scalo(codec) => {
                    let sender = HttpTransport::new(&HttpTransportConfig {
                        compression: Compression::new(codec),
                        ..HttpTransportConfig::sender(&format!("http://{addr}"))
                    })
                    .await
                    .unwrap();
                    let payload = format!(r#"{{"case":"{case}","pad":"{}"}}"#, "a".repeat(200));
                    let sent = sender
                        .send("ingest", bytes::Bytes::from(payload.clone()))
                        .await;
                    assert!(sent.is_ok(), "{case}: {sent:?}");
                    let records = receiver.recv(10).await.unwrap().records;
                    assert_eq!(records.len(), 1, "{case}");
                    assert_eq!(records[0].payload.as_ref(), payload.as_bytes(), "{case}");
                    sender.close().await.unwrap();
                }
                Post::Raw(encoding, body, want) => {
                    let status = client
                        .post(format!("http://{addr}/ingest"))
                        .header(reqwest::header::CONTENT_ENCODING, encoding)
                        .body(body)
                        .send()
                        .await
                        .unwrap()
                        .status();
                    assert_eq!(status.as_u16(), want, "{case}");
                    let records = receiver.recv(10).await.unwrap().records;
                    assert!(records.is_empty(), "{case}: nothing queued");
                }
            }
        }
        receiver.close().await.unwrap();
    }

    /// What a POST in a split row is answered, and what `recv` then takes.
    #[cfg(feature = "http-server")]
    enum SplitWant {
        /// 200, and these records.
        Records(Vec<String>),
        /// 200, and the body as one record.
        Whole,
        /// 413 naming the cap, and nothing queued.
        Refused,
    }

    /// A split row: (case, framing, body, want).
    #[cfg(feature = "http-server")]
    type SplitRow = (&'static str, &'static str, String, SplitWant);

    /// `1` nested in `depth` arrays.
    #[cfg(feature = "http-server")]
    fn nested(depth: usize) -> String {
        format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
    }

    /// `n` ones with `between` between them.
    #[cfg(feature = "http-server")]
    fn ones(n: usize, between: &str) -> String {
        vec!["1"; n].join(between)
    }

    /// 200, and `items` as the records.
    #[cfg(feature = "http-server")]
    fn split_records(items: &[&str]) -> SplitWant {
        SplitWant::Records(items.iter().map(|i| (*i).into()).collect())
    }

    /// Rows for `json_array` and `single`.
    #[cfg(feature = "http-server")]
    fn array_split_rows() -> Vec<SplitRow> {
        use crate::parse_guard::MAX_PARSE_DEPTH;
        use SplitWant::{Records, Refused, Whole};

        let cap = crate::framing::DecodeLimits::DEFAULT_MAX_RECORDS;
        let wide = r#"{"id":123456789012345678901234}"#;
        vec![
            (
                "array elements as sent",
                "json_array",
                format!(r#"[{wide}, {{"n":1.0e2}} ,"a,]"]"#),
                split_records(&[wide, r#"{"n":1.0e2}"#, r#""a,]""#]),
            ),
            (
                "the deepest array that splits",
                "json_array",
                format!("[{}]", nested(MAX_PARSE_DEPTH)),
                split_records(&[&nested(MAX_PARSE_DEPTH)]),
            ),
            (
                "an array too deep to split",
                "json_array",
                format!("[{}]", nested(MAX_PARSE_DEPTH + 1)),
                Whole,
            ),
            (
                "an element that does not parse",
                "json_array",
                r#"[{"a":1},{"b":}]"#.into(),
                Whole,
            ),
            (
                "bytes after the closing bracket",
                "json_array",
                r#"[{"a":1}] and then some"#.into(),
                Whole,
            ),
            (
                "a second array after the first",
                "json_array",
                "[1][2]".into(),
                Whole,
            ),
            (
                "an object, not an array",
                "json_array",
                r#"{"a":1}"#.into(),
                Whole,
            ),
            (
                "an array cut short",
                "json_array",
                r#"[{"a":1},"#.into(),
                Whole,
            ),
            (
                "an empty array",
                "json_array",
                " [ ] ".into(),
                split_records(&[]),
            ),
            (
                "cap - 1 elements",
                "json_array",
                format!("[{}]", ones(cap - 1, ",")),
                Records(vec!["1".into(); cap - 1]),
            ),
            (
                "cap elements",
                "json_array",
                format!("[{}]", ones(cap, ",")),
                Records(vec!["1".into(); cap]),
            ),
            (
                "cap + 1 elements",
                "json_array",
                format!("[{}]", ones(cap + 1, ",")),
                Refused,
            ),
            (
                "single keeps an array whole",
                "single",
                "[1,2]".into(),
                Whole,
            ),
        ]
    }

    /// Rows for `ndjson`, and for `json`, which takes an array or NDJSON.
    #[cfg(feature = "http-server")]
    fn line_split_rows() -> Vec<SplitRow> {
        use crate::parse_guard::MAX_PARSE_DEPTH;
        use SplitWant::{Records, Refused, Whole};

        let cap = crate::framing::DecodeLimits::DEFAULT_MAX_RECORDS;
        let wide = r#"{"id":123456789012345678901234}"#;
        let pretty = "{\n  \"a\": 1\n}";
        vec![
            (
                "ndjson lines as sent, a bad later line included",
                "ndjson",
                format!("{{\"a\":1}}\r\n\n  {wide}  \nnot json\n"),
                split_records(&[r#"{"a":1}"#, wide, "not json"]),
            ),
            ("a pretty-printed object", "ndjson", pretty.into(), Whole),
            (
                "a first line that is not JSON",
                "ndjson",
                "not json\n{\"a\":1}".into(),
                Whole,
            ),
            (
                "a first line too deep to parse",
                "ndjson",
                format!("{}\n1", nested(MAX_PARSE_DEPTH + 1)),
                Whole,
            ),
            ("one line", "ndjson", "{\"a\":1}\n".into(), Whole),
            (
                "cap lines",
                "ndjson",
                ones(cap, "\n"),
                Records(vec!["1".into(); cap]),
            ),
            ("cap + 1 lines", "ndjson", ones(cap + 1, "\n"), Refused),
            (
                "json: an array",
                "json",
                format!(" [{wide},{{\"a\":2}}]"),
                split_records(&[wide, r#"{"a":2}"#]),
            ),
            (
                "json: ndjson",
                "json",
                format!("{wide}\n{{\"a\":2}}\n"),
                split_records(&[wide, r#"{"a":2}"#]),
            ),
            (
                "json: ndjson of arrays",
                "json",
                "[1]\n[2,3]".into(),
                split_records(&["[1]", "[2,3]"]),
            ),
            (
                "json: an element that does not parse",
                "json",
                r#"[{"a":1},{"b":}]"#.into(),
                Whole,
            ),
            (
                "json: a pretty-printed object",
                "json",
                pretty.into(),
                Whole,
            ),
            (
                "json: cap + 1 elements",
                "json",
                format!("[{}]", ones(cap + 1, ",")),
                Refused,
            ),
        ]
    }

    /// A body splits by `framing` into records holding its own bytes. An array
    /// too deep to split, holding an element that does not parse, or with bytes
    /// after its `]` stays one record, as does NDJSON whose first line is not a
    /// JSON value. `json` takes either shape on one path. A body of more than
    /// 100000 records is refused whole with 413 naming the cap. The in-flight
    /// budget is whole again once `recv` has taken every record.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn a_body_splits_by_framing_and_one_over_the_cap_is_refused_whole() {
        let cap = crate::framing::DecodeLimits::DEFAULT_MAX_RECORDS.to_string();
        for (case, framing, body, want) in array_split_rows().into_iter().chain(line_split_rows()) {
            let (status, answer, got, budget_whole) = post_split(framing, &body).await;
            let (want_status, want): (u16, Vec<&[u8]>) = match &want {
                SplitWant::Records(records) => {
                    (200, records.iter().map(String::as_bytes).collect())
                }
                SplitWant::Whole => (200, vec![body.as_bytes()]),
                SplitWant::Refused => {
                    assert!(answer.contains(&cap), "{case}: {answer:?}");
                    (413, Vec::new())
                }
            };
            assert_eq!(status, want_status, "{case}: {answer:?}");
            assert!(
                got.iter().map(AsRef::as_ref).eq(want.iter().copied()),
                "{case}: {} records, want {}, first different at {:?}",
                got.len(),
                want.len(),
                got.iter().zip(&want).position(|(g, w)| g.as_ref() != *w),
            );
            assert!(budget_whole, "{case}: in-flight bytes not returned");
        }
    }

    /// POST `body` to a receiver splitting by `framing`. Returns the answer's
    /// status and text, every record `recv` then takes, over several calls so
    /// a request is taken across them, and whether the in-flight budget is
    /// whole again.
    #[cfg(feature = "http-server")]
    async fn post_split(framing: &str, body: &str) -> (u16, String, Vec<bytes::Bytes>, bool) {
        let config = HttpTransportConfig {
            framing: crate::framing::FramingConfig {
                framing_type: framing.into(),
                ..crate::framing::FramingConfig::default()
            },
            ..receiver_config(50)
        };
        let receiver = HttpTransport::new(&config).await.unwrap();
        let addr = receiver.local_addr().expect("receiver bound");
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/ingest"))
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let answer = response.text().await.unwrap();
        let mut got = Vec::new();
        loop {
            let batch = receiver.recv(40_000).await.unwrap();
            if batch.records.is_empty() {
                break;
            }
            got.extend(batch.records.into_iter().map(|r| r.payload));
        }
        let in_flight = receiver.in_flight.as_ref().expect("receiving");
        let budget_whole = in_flight.available_permits() == config.max_in_flight_bytes;
        receiver.close().await.unwrap();
        (status, answer, got, budget_whole)
    }

    /// Test recv without listen returns config error.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn recv_without_listen_returns_error() {
        let config = HttpTransportConfig::sender("http://localhost:9999");
        let transport = HttpTransport::new(&config).await.unwrap();

        let result = transport.recv(10).await;
        assert!(result.is_err());
    }

    /// With a pressure governor pinned HIGH, the ingest handler sheds with
    /// 503 (SERVICE_UNAVAILABLE) -- the same status as the channel-full path.
    /// With `None` (the default `new`), POST is accepted (200) as before.
    #[cfg(all(feature = "http-server", feature = "governor"))]
    #[tokio::test]
    async fn pressure_high_sheds_with_503_and_none_is_normal() {
        use crate::governor::{Hysteresis, MemoryPressureSource, PressureSource, UnifiedPressure};
        use crate::memory::{MemoryGuard, MemoryGuardConfig};

        // --- None default: POST accepted (200) ---
        {
            let cfg = HttpTransportConfig {
                listen: Some("127.0.0.1:0".to_string()),
                recv_timeout_ms: 200,
                ..Default::default()
            };
            let receiver = HttpTransport::new(&cfg).await.unwrap();
            let addr = receiver.local_addr().expect("receiver bound");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let resp = client
                .post(format!("http://127.0.0.1:{}/ingest", addr.port()))
                .body(b"{\"msg\":\"ok\"}".to_vec())
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "no governor -> accepted"
            );
            receiver.close().await.unwrap();
        }

        // --- Governor pinned HIGH (HARD memory source at 95%): 503 ---
        {
            // Pinned to the reservation counter so 950/1000 is the ratio, not
            // the host's own memory usage.
            let guard = Arc::new(MemoryGuard::with_usage_source(
                MemoryGuardConfig {
                    limit_bytes: 1000,
                    pressure_threshold: 0.80,
                    ..Default::default()
                },
                crate::memory::UsageSource::Reservations,
            ));
            guard.add_bytes(950); // 95% -> well above pause_above
            let src = MemoryPressureSource::new(Arc::clone(&guard));
            let pressure = Arc::new(UnifiedPressure::new(
                vec![Arc::new(src) as Arc<dyn PressureSource>],
                Hysteresis::new(0.80, 0.65).expect("valid band"),
            ));
            // Sanity: the latch is armed.
            assert!(pressure.should_hold(), "pinned-high governor must hold");

            let cfg = HttpTransportConfig {
                listen: Some("127.0.0.1:0".to_string()),
                recv_timeout_ms: 200,
                ..Default::default()
            };
            let receiver = HttpTransport::with_pressure(&cfg, Some(Arc::clone(&pressure)))
                .await
                .unwrap();
            let addr = receiver.local_addr().expect("receiver bound");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let client = reqwest::Client::new();
            let resp = client
                .post(format!("http://127.0.0.1:{}/ingest", addr.port()))
                .body(b"{\"msg\":\"shed\"}".to_vec())
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "pinned-high governor must shed with 503"
            );
            // The 503 carries a Retry-After hint so a well-behaved sender backs
            // off instead of hot-retrying into a holding pod.
            assert_eq!(
                resp.headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some("1"),
                "503 shed must carry Retry-After"
            );

            // A held gate answers before the body is read: the client has
            // sent only the head.
            let mut stream = post_head(addr, 64).await;
            let status = status_line(&mut stream).await;
            assert!(
                status.starts_with("HTTP/1.1 503"),
                "a held gate must refuse before the body, got {status:?}"
            );

            // The shed POST never reached the queue.
            let records = receiver.recv(10).await.unwrap().records;
            assert!(records.is_empty(), "shed request must not be queued");
            receiver.close().await.unwrap();
        }
    }

    /// A receive-only config on a free loopback port.
    #[cfg(feature = "http-server")]
    fn receiver_config(recv_timeout_ms: u64) -> HttpTransportConfig {
        HttpTransportConfig {
            listen: Some("127.0.0.1:0".to_string()),
            recv_timeout_ms,
            ..Default::default()
        }
    }

    /// A sender posting to `receiver`'s ingest path.
    #[cfg(feature = "http-server")]
    async fn sender_to(receiver: &HttpTransport) -> HttpTransport {
        let addr = receiver.local_addr().expect("receiver bound");
        HttpTransport::new(&HttpTransportConfig::sender(&format!(
            "http://{addr}/ingest"
        )))
        .await
        .expect("sender")
    }

    /// Receive until the transport reports `Closed`, counting records. Bounded,
    /// so a recv that never ends fails the test instead of hanging it.
    #[cfg(feature = "http-server")]
    async fn drain_until_closed(receiver: &HttpTransport) -> Result<usize, String> {
        let mut delivered = 0;
        for _ in 0..1000 {
            match receiver.recv(100).await {
                Ok(batch) => delivered += batch.records.len(),
                Err(TransportError::Closed) => return Ok(delivered),
                Err(e) => return Err(format!("recv failed after {delivered} records: {e}")),
            }
        }
        Err(format!(
            "recv never reported Closed; {delivered} records so far"
        ))
    }

    /// Send the head of a POST and part of its body, holding the request open.
    #[cfg(feature = "http-server")]
    async fn partial_post(
        addr: std::net::SocketAddr,
        body: &[u8],
        sent: usize,
    ) -> tokio::net::TcpStream {
        use tokio::io::AsyncWriteExt;

        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let head = format!(
            "POST /ingest HTTP/1.1\r\nhost: {addr}\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .expect("write request head");
        stream
            .write_all(&body[..sent])
            .await
            .expect("write part of the body");
        stream
    }

    /// Read a response's status line, bounded so a silent server fails the test.
    #[cfg(feature = "http-server")]
    async fn status_line(stream: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();
        let mut chunk = [0_u8; 1024];
        let read = async {
            while !buf.windows(2).any(|w| w == b"\r\n") {
                let n = stream.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), read)
            .await
            .expect("the server answered within 5 s");
        let text = String::from_utf8_lossy(&buf);
        text.lines().next().unwrap_or_default().to_string()
    }

    /// Every record the server acked reaches `recv`, even when `close()` comes
    /// before the consumer read it -- with a blocking and a non-blocking `recv`.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn acked_records_reach_recv_after_close() {
        for recv_timeout_ms in [100, 0] {
            let receiver = HttpTransport::new(&receiver_config(recv_timeout_ms))
                .await
                .expect("receiver");
            let sender = sender_to(&receiver).await;
            for seq in 0..3 {
                let sent = sender
                    .send("", bytes::Bytes::from(format!("{{\"seq\":{seq}}}")))
                    .await;
                assert!(sent.is_ok(), "{sent:?}");
            }

            receiver.close().await.expect("close");
            let delivered = drain_until_closed(&receiver).await;
            assert_eq!(
                delivered,
                Ok(3),
                "recv_timeout_ms={recv_timeout_ms}: the server acked 3 records and recv \
                 returned {delivered:?} of them after close()"
            );
            assert!(
                matches!(receiver.recv(10).await, Err(TransportError::Closed)),
                "Closed must stay terminal once drained"
            );
            let _ = sender.close().await;
        }
    }

    /// A POST whose body is still arriving when `close()` runs is answered 503,
    /// which the sender retries, never 200: nothing is left to deliver it.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn a_request_in_flight_at_close_is_refused_not_acked() {
        use tokio::io::AsyncWriteExt;

        let receiver = HttpTransport::new(&receiver_config(100))
            .await
            .expect("receiver");
        let addr = receiver.local_addr().expect("receiver bound");
        let body = br#"{"late":1}"#;
        let mut stream = partial_post(addr, body, 4).await;
        // Let the server read the head and start waiting on the body.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        receiver.close().await.expect("close");
        stream.write_all(&body[4..]).await.expect("finish the body");
        let status = status_line(&mut stream).await;

        assert!(
            status.starts_with("HTTP/1.1 503"),
            "a request that completes after close() must be refused with 503, got {status:?}"
        );
        assert_eq!(
            drain_until_closed(&receiver).await,
            Ok(0),
            "nothing sent after close() may be queued"
        );
    }

    /// Send a POST head declaring `content_length` and none of its body.
    #[cfg(feature = "http-server")]
    async fn post_head(addr: std::net::SocketAddr, content_length: usize) -> tokio::net::TcpStream {
        use tokio::io::AsyncWriteExt;

        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        let head = format!(
            "POST /ingest HTTP/1.1\r\nhost: {addr}\r\ncontent-length: {content_length}\r\n\r\n"
        );
        stream
            .write_all(head.as_bytes())
            .await
            .expect("write request head");
        stream
    }

    /// POST `body` until the answer is `want`, bounded so a server that never
    /// answers it fails the test.
    #[cfg(feature = "http-server")]
    async fn post_until(addr: std::net::SocketAddr, body: &'static [u8], want: u16) {
        let client = reqwest::Client::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut last = None;
        while std::time::Instant::now() < deadline {
            let status = client
                .post(format!("http://{addr}/ingest"))
                .body(body)
                .send()
                .await
                .map(|r| r.status().as_u16());
            if matches!(status, Ok(s) if s == want) {
                return;
            }
            last = Some(status);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("never answered {want} within 5 s, last {last:?}");
    }

    /// Requests are admitted against `max_in_flight_bytes` by their declared
    /// length before their bodies are read: four that declare the largest
    /// body take the default budget and the next is refused at once, and the
    /// budget comes back as those requests end and as `recv` takes records.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn admission_reserves_declared_bytes_before_reading_the_body() {
        let receiver = HttpTransport::new(&receiver_config(100))
            .await
            .expect("receiver");
        let addr = receiver.local_addr().expect("receiver bound");
        let largest = HttpTransportConfig::default().max_body_bytes;
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(post_head(addr, largest).await);
        }
        post_until(addr, b"{}", 503).await;

        drop(held);
        post_until(addr, b"{}", 200).await;
        let records = receiver.recv(10).await.expect("recv").records;
        assert!(
            records.iter().all(|r| r.payload.as_ref() == b"{}"),
            "only the small POSTs were queued: {records:?}"
        );
        receiver.close().await.expect("close");
    }

    /// A budget below the largest accepted body could never admit one.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn an_in_flight_budget_below_the_largest_body_is_refused() {
        let config = HttpTransportConfig {
            max_body_bytes: 2048,
            max_in_flight_bytes: 1024,
            ..receiver_config(100)
        };
        assert!(
            matches!(
                HttpTransport::new(&config).await,
                Err(TransportError::Config(m)) if m.contains("max_in_flight_bytes")
            ),
            "a budget under max_body_bytes must be refused"
        );
    }

    /// A handler that finds the receive queue closed answers 503 with a
    /// `Retry-After`, the status a sender retries, not 410.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn a_closed_queue_answers_retryable_503() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        rx.close();
        let state = ReceiverState {
            sender: tx,
            sequence: Arc::new(AtomicU64::new(0)),
            in_flight: Arc::new(tokio::sync::Semaphore::new(1024)),
            max_body_bytes: 1024,
            split: split::BodySplit::Single,
            decoders: Arc::new(BodyDecoders::new().expect("decoders")),
            #[cfg(feature = "governor")]
            pressure: None,
        };

        let response = ingest_handler(
            axum::extract::State(state),
            axum::extract::ConnectInfo("127.0.0.1:9".parse().expect("addr")),
            axum::http::Request::post("/ingest")
                .body(axum::body::Body::from("{}"))
                .expect("request"),
        )
        .await;

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    /// `close()` stops the server even while a client holds a request open,
    /// without waiting on that client, and the listener is free when it returns.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn close_stops_the_server_while_a_client_holds_a_request_open() {
        let receiver = HttpTransport::new(&receiver_config(100))
            .await
            .expect("receiver");
        let addr = receiver.local_addr().expect("receiver bound");
        let _stalled = partial_post(addr, br#"{"stalled":1}"#, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let started = std::time::Instant::now();
        let closing =
            tokio::time::timeout(std::time::Duration::from_secs(5), receiver.close()).await;
        let took = started.elapsed();
        assert!(
            matches!(closing, Ok(Ok(()))),
            "close() waited on the stalled client, got {closing:?}"
        );
        assert!(
            took < std::time::Duration::from_secs(1),
            "close() took {took:?}"
        );
        let rebind = HttpTransportConfig {
            listen: Some(addr.to_string()),
            ..Default::default()
        };
        assert!(
            HttpTransport::new(&rebind).await.is_ok(),
            "{addr} still bound when close() returned -- the server outlived close()"
        );
    }

    /// Dropping the transport without `close()` stops the server too.
    #[cfg(feature = "http-server")]
    #[tokio::test]
    async fn drop_stops_the_server_while_a_client_holds_a_request_open() {
        let receiver = HttpTransport::new(&receiver_config(100))
            .await
            .expect("receiver");
        let addr = receiver.local_addr().expect("receiver bound");
        let _stalled = partial_post(addr, br#"{"stalled":1}"#, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        drop(receiver);
        let rebind = HttpTransportConfig {
            listen: Some(addr.to_string()),
            ..Default::default()
        };
        let mut free = false;
        for _ in 0..40 {
            if HttpTransport::new(&rebind).await.is_ok() {
                free = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(free, "{addr} still bound 2 s after drop");
    }

    /// Senders still posting while the server closes: every record acked to
    /// them is one `recv` returns, whichever side of `close()` it landed on.
    #[cfg(feature = "http-server")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_acked_record_is_delivered_when_close_races_the_senders() {
        use std::sync::atomic::AtomicUsize;

        /// Acks left unread before `close()`, as behind a consumer that stopped calling recv.
        const BACKLOG: usize = 20;

        let receiver = HttpTransport::new(&receiver_config(100))
            .await
            .expect("receiver");
        let sender = Arc::new(sender_to(&receiver).await);
        let stop = Arc::new(AtomicBool::new(false));
        let acked = Arc::new(AtomicUsize::new(0));

        let mut senders = tokio::task::JoinSet::new();
        for task in 0..4_u32 {
            let sender = Arc::clone(&sender);
            let stop = Arc::clone(&stop);
            let acked = Arc::clone(&acked);
            senders.spawn(async move {
                let mut seq = task * 1_000_000;
                while !stop.load(Ordering::Relaxed) {
                    let payload = bytes::Bytes::from(format!("{{\"seq\":{seq}}}"));
                    if sender.send("", payload).await.is_ok() {
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                    seq += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            });
        }

        // Close on a backlog, not a timer: a loaded host can take longer than any sleep to ack.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while acked.load(Ordering::Relaxed) < BACKLOG {
            assert!(
                std::time::Instant::now() < deadline,
                "the senders got {} of {BACKLOG} acks in 10 s",
                acked.load(Ordering::Relaxed)
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        receiver.close().await.expect("close");
        let delivered = drain_until_closed(&receiver).await.expect("drain");
        stop.store(true, Ordering::Relaxed);

        while let Some(joined) = senders.join_next().await {
            joined.expect("sender task");
        }
        let acked = acked.load(Ordering::Relaxed);
        assert_eq!(
            delivered, acked,
            "{acked} records acked to the senders, {delivered} returned by recv"
        );
    }

    /// Accepted HTTP records count as received, never as sent: in one process
    /// the sender's counts are the only sends.
    #[cfg(all(feature = "http-server", feature = "metrics"))]
    #[tokio::test]
    async fn receipts_count_as_received_not_sent() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // Current-thread runtime: the server's tasks run on this thread and see it.
        let _local = metrics::set_default_local_recorder(&recorder);

        let receiver = HttpTransport::new(&receiver_config(1000))
            .await
            .expect("receiver");
        let sender = sender_to(&receiver).await;
        for seq in 0..2 {
            let sent = sender
                .send("", bytes::Bytes::from(format!("{{\"seq\":{seq}}}")))
                .await;
            assert!(sent.is_ok(), "{sent:?}");
        }
        assert_eq!(receiver.recv(10).await.expect("recv").records.len(), 2);

        let rendered = handle.render();
        let value = |name: &str| -> Option<f64> {
            rendered
                .lines()
                .find(|line| line.starts_with(&format!("{name}{{transport=\"http\"}}")))
                .and_then(|line| line.rsplit(' ').next()?.parse().ok())
        };
        assert_eq!(
            value("transport_sent_total"),
            Some(2.0),
            "two POSTs were sent:\n{rendered}"
        );
        assert_eq!(
            value("transport_received_events_total"),
            Some(2.0),
            "two records were received:\n{rendered}"
        );

        let _ = sender.close().await;
        let _ = receiver.close().await;
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = HttpTransportConfig {
            endpoint: Some("http://example.com/ingest".into()),
            listen: Some("0.0.0.0:8080".into()),
            recv_path: "/custom".into(),
            recv_buffer_size: 5000,
            recv_timeout_ms: 250,
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: HttpTransportConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.endpoint, config.endpoint);
        assert_eq!(parsed.listen, config.listen);
        assert_eq!(parsed.recv_path, config.recv_path);
        assert_eq!(parsed.recv_buffer_size, config.recv_buffer_size);
        assert_eq!(parsed.recv_timeout_ms, config.recv_timeout_ms);
    }
}
