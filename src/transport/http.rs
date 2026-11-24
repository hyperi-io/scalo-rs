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
//! configurable path. Incoming payloads are queued into a bounded
//! `tokio::sync::mpsc` channel. `recv()` drains from this channel.
//!
//! ## Shutdown
//!
//! The server answers 200 once a record is queued for `recv`, so a receiving
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

pub use send::{HttpAuth, HttpEndpoint};

/// The codecs an HTTP request body takes.
pub const HTTP_ACCEPTS: Accepts = Accepts::new(
    "transport.http.compression",
    &[Codec::None, Codec::Gzip, Codec::Zstd],
);

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
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[cfg(feature = "http-server")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};

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

    /// How records share a request body. Default `single`, one record per
    /// request. Any [`crate::framing`] name, an app's own included.
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

    /// Receive buffer size (bounded channel capacity). Default: 10000.
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

    /// CA PEM file the send-side HTTPS client trusts, beside the OS store
    /// under `native` and `platform`, and alone under `bundle`.
    #[serde(default)]
    pub tls_ca_path: Option<String>,

    /// Which certificates the send-side client accepts the server's from:
    /// `native`, `platform` or `bundle` (see [`crate::tls::TrustChoice`]).
    /// Unset is `native` when any other `tls_*` key is set, and the HTTP
    /// client library's own verifier, the OS one, when none is.
    #[serde(default)]
    pub tls_trust: Option<crate::tls::TrustChoice>,

    /// Client certificate PEM for mutual TLS, with `tls_client_key_path`.
    #[serde(default)]
    pub tls_client_cert_path: Option<String>,

    /// Client private key PEM for mutual TLS, with `tls_client_cert_path`.
    /// May name the same file when it holds both.
    #[serde(default)]
    pub tls_client_key_path: Option<String>,

    /// SHA-256 pins on the server's public key, in base64 or hex (see
    /// [`crate::tls::SpkiPin`]). Once the chain verifies, the key must match
    /// one. Empty (the default) checks no key.
    #[serde(default)]
    pub tls_pin_sha256: Vec<crate::tls::SpkiPin>,
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
            tls_ca_path: None,
            tls_trust: None,
            tls_client_cert_path: None,
            tls_client_key_path: None,
            tls_pin_sha256: Vec::new(),
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
            .field("tls_ca_path", &self.tls_ca_path)
            .field("tls_trust", &self.tls_trust)
            .field("tls_client_cert_path", &self.tls_client_cert_path)
            .field("tls_client_key_path", &self.tls_client_key_path)
            .field("tls_pin_sha256", &self.tls_pin_sha256)
            .finish()
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

    /// The client TLS the `tls_*` keys name, or `None` when none is set and
    /// the HTTP client keeps its own verifier.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when only one of the client certificate and
    /// key is set.
    fn client_tls(&self) -> TransportResult<Option<crate::tls::ClientTls>> {
        let identity = match (&self.tls_client_cert_path, &self.tls_client_key_path) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => {
                return Err(TransportError::Config(
                    "HTTP client TLS: mutual TLS needs both tls_client_cert_path and \
                     tls_client_key_path"
                        .to_string(),
                ));
            }
        };
        if self.tls_trust.is_none()
            && self.tls_ca_path.is_none()
            && identity.is_none()
            && self.tls_pin_sha256.is_empty()
        {
            return Ok(None);
        }
        let mut tls = crate::tls::ClientTls::new(self.tls_trust.unwrap_or_default());
        if let Some(ca) = &self.tls_ca_path {
            tls = tls.with_ca(ca);
        }
        if let Some((cert, key)) = identity {
            tls = tls.with_identity(cert, key);
        }
        for pin in &self.tls_pin_sha256 {
            tls = tls.with_pin(*pin);
        }
        Ok(Some(tls))
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
    /// Removes this component from the health registry when it drops.
    #[cfg(feature = "health")]
    _health_registration: crate::health::HealthRegistration,
    /// The send side, `None` when no `endpoint` is set.
    sender: Option<send::Sender>,

    /// Receiver channel populated by the embedded HTTP server.
    /// Only available when `http-server` feature is enabled AND `listen` is configured.
    #[cfg(feature = "http-server")]
    receiver: Option<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Message<HttpToken>>>>,

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

    /// Whether the transport is closed.
    closed: Arc<AtomicBool>,

    /// Whether the proxy is refusing CONNECT, which fails readiness until a send goes through.
    refusals: super::proxy::Refusals,

    /// Receive timeout in milliseconds (used by receive side).
    #[cfg(feature = "http-server")]
    recv_timeout_ms: u64,

    /// Transport-level message filter engine.
    filter_engine: super::filter::TransportFilterEngine,
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
        // Default path: no pressure governor -> byte-identical to before.
        Self::new_inner(
            config,
            #[cfg(feature = "governor")]
            None,
        )
        .await
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
        Self::new_inner(config, pressure).await
    }

    async fn new_inner(
        config: &HttpTransportConfig,
        #[cfg(feature = "governor")] pressure: Option<Arc<crate::governor::UnifiedPressure>>,
    ) -> TransportResult<Self> {
        let mut client_builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(config.connect_timeout_ms))
            .timeout(std::time::Duration::from_millis(config.send_timeout_ms));

        if let Some(tls) = config.client_tls()? {
            let tls_cfg =
                crate::tls::build_client_tls(crate::crypto::CryptoProfile::default(), &tls)
                    .map_err(|e| TransportError::Config(format!("HTTP client TLS: {e}")))?;
            client_builder = client_builder.use_preconfigured_tls((*tls_cfg).clone());
        } else {
            tracing::debug!("HTTP transport client TLS: the client library's own verifier");
        }

        let sender = config
            .endpoint
            .as_deref()
            .map(|endpoint| send::Sender::new(config, endpoint, client_builder))
            .transpose()?;
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
                Some(tokio::sync::Mutex::new(rx)),
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

        let closed = Arc::new(AtomicBool::new(false));
        let refusals = sender
            .as_ref()
            .map(send::Sender::refusals)
            .unwrap_or_default();

        #[cfg(feature = "health")]
        let health_registration = {
            let h = Arc::clone(&closed);
            let refusals = refusals.clone();
            crate::health::HealthRegistry::register_scoped("transport:http", move || {
                if h.load(Ordering::Relaxed) || refusals.is_refused() {
                    crate::health::HealthStatus::Unhealthy
                } else {
                    crate::health::HealthStatus::Healthy
                }
            })
        };

        Ok(Self {
            #[cfg(feature = "health")]
            _health_registration: health_registration,
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
            closed,
            refusals,
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

/// Shared state for the receive handler.
#[cfg(feature = "http-server")]
#[derive(Clone)]
struct ReceiverState {
    sender: tokio::sync::mpsc::Sender<Message<HttpToken>>,
    sequence: Arc<AtomicU64>,
    /// Body bytes read or queued, see [`HttpTransportConfig::max_in_flight_bytes`].
    in_flight: Arc<tokio::sync::Semaphore>,
    max_body_bytes: usize,
    /// Optional pressure governor (`governor` feature). `None` by default
    /// -> the handler never consults it. When `Some`, the handler rejects with
    /// 503 while `UnifiedPressure::should_hold` holds -- pressure-driven
    /// shedding ON TOP of the channel-full and byte-budget 503s.
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
}

/// POST handler that accepts raw bytes and queues them into the mpsc channel.
///
/// The pressure check and the byte reservation come before the body is read,
/// so a held governor or a spent budget costs no body buffer. Answers 200 once
/// the record is queued. A full queue, a held inbound gate, a spent byte budget
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
    let Some(mut permit) = u32::try_from(reserved)
        .ok()
        .and_then(|n| Arc::clone(&state.in_flight).try_acquire_many_owned(n).ok())
    else {
        #[cfg(feature = "metrics")]
        metrics::counter!("transport_backpressured_total", "transport" => "http", "reason" => "in_flight_bytes")
            .increment(1);
        return shed_503();
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

    let seq = state.sequence.fetch_add(1, Ordering::Relaxed);
    let format = PayloadFormat::detect(&body);
    let timestamp_ms = crate::clock::epoch_millis();

    // Capture wire size before `body` moves into the message.
    #[cfg(feature = "metrics")]
    let body_len = body.len();

    // `body` is `axum::body::Bytes` (= `bytes::Bytes`) -- move it directly,
    // no copy needed.
    let msg = Message {
        key: None,
        payload: body,
        token: HttpToken::with_source(seq, addr.to_string()),
        timestamp_ms: Some(timestamp_ms),
        format,
        headers: Vec::new(),
    };

    match state.sender.try_send(msg) {
        Ok(()) => {
            // The queued record holds its bytes until `recv` hands it over.
            permit.forget();
            #[cfg(feature = "metrics")]
            {
                metrics::counter!("transport_received_bytes_total", "transport" => "http")
                    .increment(body_len as u64);
                metrics::counter!("transport_received_events_total", "transport" => "http")
                    .increment(1);
            }
            axum::http::StatusCode::OK.into_response()
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            #[cfg(feature = "metrics")]
            metrics::counter!("transport_backpressured_total", "transport" => "http").increment(1);
            shed_503()
        }
        // A closed receiver is shutting down: 503 so the sender retries, not drops.
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            #[cfg(feature = "metrics")]
            metrics::counter!("transport_refused_total", "transport" => "http").increment(1);
            shed_503()
        }
    }
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
        self.closed.store(true, Ordering::Relaxed);

        #[cfg(feature = "http-server")]
        {
            if let Some(receiver) = &self.receiver {
                receiver.lock().await.close();
            }
            self.stop_server().await;
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.closed.load(Ordering::Relaxed) && !self.refusals.is_refused()
    }

    fn name(&self) -> &'static str {
        "http"
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
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }
        let filtered = self.filter_engine.has_outbound_filters();
        let mut outcome = super::types::BlockOutcome::default();
        let mut kept: Vec<(&str, &[u8])> = Vec::new();
        for (destination, payload) in records {
            if filtered {
                match self.filter_engine.apply_outbound(payload) {
                    super::filter::FilterDisposition::Pass => {}
                    super::filter::FilterDisposition::Drop => continue,
                    super::filter::FilterDisposition::Dlq => {
                        let _ = outcome.add(SendResult::FilteredDlq);
                        continue;
                    }
                }
            }
            kept.push((destination, payload.as_ref()));
        }
        if kept.is_empty() {
            return outcome.finish();
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
        match outcome.add(sent) {
            Some(ended) => ended,
            None => outcome.finish(),
        }
    }
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
                if self.closed.load(Ordering::Relaxed) {
                    return Err(TransportError::Closed);
                }
                return Err(TransportError::Config(
                    "no listen address configured for receiving".into(),
                ));
            };

            let mut rx = receiver.lock().await;
            let mut messages = Vec::with_capacity(max.min(100));

            for _ in 0..max {
                // The first record waits up to recv_timeout_ms. The rest only take
                // what is already queued.
                let msg = if self.recv_timeout_ms == 0 || !messages.is_empty() {
                    match rx.try_recv() {
                        Ok(msg) => msg,
                        Err(TryRecvError::Disconnected) if messages.is_empty() => {
                            return Err(TransportError::Closed);
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                } else {
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(self.recv_timeout_ms),
                        rx.recv(),
                    )
                    .await
                    {
                        Ok(Some(msg)) => msg,
                        Ok(None) => return Err(TransportError::Closed),
                        Err(_elapsed) => break,
                    }
                };
                if let Some(in_flight) = &self.in_flight {
                    in_flight.add_permits(msg.payload.len());
                }
                messages.push(msg);
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
            if self.closed.load(Ordering::Relaxed) {
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

    /// Every `tls_*` key reaches the config through the cascade, from YAML and
    /// from the environment, which wins.
    #[cfg(feature = "config")]
    #[test]
    fn tls_keys_reach_the_config_through_the_cascade() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "transport:\n  http:\n    tls_trust: bundle\n    tls_ca_path: /etc/tls/ca.pem\n    \
             tls_client_cert_path: /etc/tls/client.pem\n    tls_client_key_path: /etc/tls/yaml.key\n    \
             tls_pin_sha256: [\"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\"]\n",
        )
        .unwrap();
        temp_env::with_vars(
            [
                ("HTTPTLS__TRANSPORT__HTTP__TLS_TRUST", Some("platform")),
                (
                    "HTTPTLS__TRANSPORT__HTTP__TLS_CLIENT_KEY_PATH",
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
        assert_eq!(config.tls_trust, Some(crate::tls::TrustChoice::Platform));
        assert_eq!(config.tls_ca_path.as_deref(), Some("/etc/tls/ca.pem"));
        assert_eq!(
            config.tls_client_cert_path.as_deref(),
            Some("/etc/tls/client.pem")
        );
        assert_eq!(
            config.tls_client_key_path.as_deref(),
            Some("/etc/tls/env.key")
        );
        let pins: Vec<String> = config
            .tls_pin_sha256
            .iter()
            .map(|p| p.to_string())
            .collect();
        assert_eq!(pins, ["47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="]);
    }

    /// The `tls_*` keys map onto the client TLS they name, and leave the HTTP
    /// client's own verifier in place when none is set.
    #[test]
    fn tls_keys_map_onto_client_tls() {
        use crate::tls::{ClientTls, TrustChoice};
        type Mapped = Result<Option<ClientTls>, String>;
        let with = |trust, ca: Option<&str>, cert: Option<&str>, key: Option<&str>| -> Mapped {
            HttpTransportConfig {
                tls_trust: trust,
                tls_ca_path: ca.map(str::to_string),
                tls_client_cert_path: cert.map(str::to_string),
                tls_client_key_path: key.map(str::to_string),
                ..HttpTransportConfig::default()
            }
            .client_tls()
            .map_err(|e| e.to_string())
        };
        let native = || ClientTls::new(TrustChoice::Native);
        let cases: [(&str, Mapped, Mapped); 6] = [
            ("nothing set", with(None, None, None, None), Ok(None)),
            (
                "a CA alone is native plus the CA",
                with(None, Some("ca.pem"), None, None),
                Ok(Some(native().with_ca("ca.pem"))),
            ),
            (
                "an explicit native",
                with(Some(TrustChoice::Native), None, None, None),
                Ok(Some(native())),
            ),
            (
                "the pin with an identity",
                with(
                    Some(TrustChoice::Bundle),
                    Some("ca.pem"),
                    Some("c.pem"),
                    Some("k.pem"),
                ),
                Ok(Some(
                    ClientTls::pinned("ca.pem").with_identity("c.pem", "k.pem"),
                )),
            ),
            (
                "an identity alone is native",
                with(None, None, Some("c.pem"), Some("k.pem")),
                Ok(Some(native().with_identity("c.pem", "k.pem"))),
            ),
            (
                "a certificate without its key",
                with(Some(TrustChoice::Platform), None, Some("c.pem"), None),
                Err(
                    "transport config error: HTTP client TLS: mutual TLS needs both \
                     tls_client_cert_path and tls_client_key_path"
                        .to_string(),
                ),
            ),
        ];
        for (name, got, expected) in cases {
            assert_eq!(got, expected, "{name}");
        }

        let pin: crate::tls::SpkiPin = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
            .parse()
            .unwrap();
        let pinned = HttpTransportConfig {
            tls_pin_sha256: vec![pin],
            ..HttpTransportConfig::default()
        };
        assert_eq!(
            pinned.client_tls().map_err(|e| e.to_string()),
            Ok(Some(native().with_pin(pin))),
            "a pin alone is native plus the pin"
        );
        let bad = serde_json::from_value::<HttpTransportConfig>(
            serde_json::json!({"tls_pin_sha256": ["not a pin"]}),
        )
        .map(|_| ())
        .map_err(|e| e.to_string());
        assert!(
            bad.as_ref()
                .is_err_and(|e| e.contains("\"not a pin\" is not a SHA-256")),
            "a malformed pin fails the config load: {bad:?}"
        );
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
        let ca_path = file("ca.pem", &ca.pem());
        let cert = file("client.pem", &client_leaf.cert_pem);
        let key = file("client.key", &client_leaf.key_pem);
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
                tls_trust: Some(trust),
                tls_ca_path: Some(ca_path.clone()),
                tls_client_cert_path: identity.then(|| cert.clone()),
                tls_client_key_path: identity.then(|| key.clone()),
                ..HttpTransportConfig::sender(&endpoint)
            };
            let transport = HttpTransport::new(&config).await.unwrap();
            let result = transport
                .send("ingest", bytes::Bytes::from_static(b"{}"))
                .await;
            assert_eq!(result.is_ok(), delivered, "{name}: {result:?}");
        }

        let missing = HttpTransportConfig {
            tls_trust: Some(TrustChoice::Bundle),
            tls_ca_path: Some(dir.path().join("gone.pem").to_string_lossy().into_owned()),
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

        let status = Arc::new(std::sync::atomic::AtomicU16::new(500));
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
                format: PayloadFormat::Json,
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
        status: Arc<std::sync::atomic::AtomicU16>,
    ) -> std::net::SocketAddr {
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
