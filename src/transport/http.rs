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
//! POSTs payload bytes to `{endpoint}/{key}` using reqwest.
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

/// Configuration for HTTP transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpTransportConfig {
    /// Endpoint URL for sending (e.g., "http://loader:8080/ingest"). None = send disabled.
    #[serde(default)]
    pub endpoint: Option<String>,

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

    /// Private-CA PEM path for the send-side HTTPS client (needs the `tls`
    /// feature). Trusts this CA *in addition to* the OS native roots. None =
    /// native roots only (reqwest default). Maps to `TlsTrust { native_roots:
    /// true, extra_roots: [ca] }`.
    #[serde(default)]
    pub tls_ca_path: Option<String>,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            listen: None,
            recv_path: default_recv_path(),
            recv_buffer_size: default_buffer_size(),
            recv_timeout_ms: default_recv_timeout_ms(),
            filters_in: Vec::new(),
            filters_out: Vec::new(),
            connect_timeout_ms: default_connect_timeout_ms(),
            send_timeout_ms: default_send_timeout_ms(),
            max_body_bytes: default_max_body_bytes(),
            tls_ca_path: None,
        }
    }
}

impl HttpTransportConfig {
    /// Load from the config cascade under the `transport.http` key.
    #[must_use]
    pub fn from_cascade() -> Self {
        <Self as super::traits::FromCascade>::from_cascade_key("transport.http")
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
    /// reqwest client for sending (always available when transport-http is enabled).
    client: reqwest::Client,

    /// Base URL for sending (None = send disabled).
    endpoint: Option<String>,

    /// Receiver channel populated by the embedded HTTP server.
    /// Only available when `http-server` feature is enabled AND `listen` is configured.
    #[cfg(feature = "http-server")]
    receiver: Option<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Message<HttpToken>>>>,

    /// Address the embedded server bound, with any port-0 request resolved.
    #[cfg(feature = "http-server")]
    local_addr: Option<std::net::SocketAddr>,

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
        // `mut` is only used on the TLS path; harmless without the feature.
        #[cfg_attr(not(feature = "tls"), allow(unused_mut))]
        let mut client_builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(config.connect_timeout_ms))
            .timeout(std::time::Duration::from_millis(config.send_timeout_ms));

        // Private-CA trust via the unified TLS module (augments native roots).
        #[cfg(feature = "tls")]
        if let Some(ref ca) = config.tls_ca_path {
            let trust = crate::tls::TlsTrust {
                native_roots: true,
                webpki_roots: false,
                extra_roots: vec![ca.into()],
                extra_intermediates: Vec::new(),
                exclusive: false,
            };
            let tls_cfg =
                crate::tls::build_client_config(crate::tls::TlsConfigSource::Trust(trust))
                    .map_err(|e| TransportError::Config(format!("HTTP client TLS: {e}")))?;
            client_builder = client_builder.use_preconfigured_tls((*tls_cfg).clone());
        }
        #[cfg(not(feature = "tls"))]
        if config.tls_ca_path.is_some() {
            tracing::warn!(
                "http transport tls_ca_path is set but the `tls` feature is disabled -- \
                 ignoring (using reqwest default roots)"
            );
        }

        let client = client_builder
            .build()
            .map_err(|e| TransportError::Config(format!("failed to create HTTP client: {e}")))?;

        #[cfg(feature = "http-server")]
        let (receiver, local_addr, shutdown_tx, server_handle) = if let Some(listen) =
            &config.listen
        {
            let addr: std::net::SocketAddr = listen
                .parse()
                .map_err(|e| TransportError::Config(format!("invalid listen address: {e}")))?;

            let (tx, rx) = tokio::sync::mpsc::channel(config.recv_buffer_size);
            let (sd_tx, sd_rx) = tokio::sync::oneshot::channel::<()>();

            let sequence = Arc::new(AtomicU64::new(0));
            let recv_path = config.recv_path.clone();

            let app = build_receiver_router(
                tx,
                sequence,
                &recv_path,
                config.max_body_bytes,
                #[cfg(feature = "governor")]
                pressure.clone(),
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
                Some(sd_tx),
                Some(handle),
            )
        } else {
            (None, None, None, None)
        };

        // When the governor feature is on but http-server is off there is no
        // receive server to attach the pressure governor to; consume it so the
        // signature stays uniform without an unused-variable warning.
        #[cfg(all(feature = "governor", not(feature = "http-server")))]
        let _ = pressure;

        #[cfg(feature = "logger")]
        tracing::info!(
            endpoint = ?config.endpoint,
            listen = ?config.listen,
            "HTTP transport opened"
        );

        // Fail loud on bad filter config -- silently disabling filters
        // turns a misconfigured `drop` / `dlq` rule into a permanent pass.
        let filter_engine = super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade(),
        )?;

        let closed = Arc::new(AtomicBool::new(false));

        #[cfg(feature = "health")]
        {
            let h = Arc::clone(&closed);
            crate::health::HealthRegistry::register("transport:http", move || {
                if h.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Unhealthy
                } else {
                    crate::health::HealthStatus::Healthy
                }
            });
        }

        Ok(Self {
            client,
            endpoint: config.endpoint.clone(),
            #[cfg(feature = "http-server")]
            receiver,
            #[cfg(feature = "http-server")]
            local_addr,
            #[cfg(feature = "http-server")]
            shutdown_tx: parking_lot::Mutex::new(shutdown_tx),
            #[cfg(feature = "http-server")]
            server_task: parking_lot::Mutex::new(server_handle),
            closed,
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

/// Build the axum router for the receive side.
#[cfg(feature = "http-server")]
fn build_receiver_router(
    sender: tokio::sync::mpsc::Sender<Message<HttpToken>>,
    sequence: Arc<AtomicU64>,
    recv_path: &str,
    max_body_bytes: usize,
    #[cfg(feature = "governor")] pressure: Option<Arc<crate::governor::UnifiedPressure>>,
) -> axum::Router {
    use axum::routing::post;

    let state = ReceiverState {
        sender,
        sequence,
        #[cfg(feature = "governor")]
        pressure,
    };

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
    /// Optional pressure governor (`governor` feature). `None` by default
    /// -> the handler never consults it and behaviour is byte-identical. When
    /// `Some`, the handler rejects with 503 while `UnifiedPressure::should_hold`
    /// holds -- pressure-driven shedding ON TOP of the existing channel-full
    /// 503, never replacing it.
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
}

/// POST handler that accepts raw bytes and queues them into the mpsc channel.
///
/// Answers 200 once the record is queued. A full queue, a held inbound gate
/// and a closed receiver answer 503, which a sender retries.
#[cfg(feature = "http-server")]
async fn ingest_handler(
    axum::extract::State(state): axum::extract::State<ReceiverState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    if body.is_empty() {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }

    // Pressure-driven shedding (governor feature, opt-in). BEFORE enqueuing,
    // if a governor is wired and it says hold, shed the request with 503 --
    // consistent with the existing channel-full 503 below (NOT 429). Default
    // `None` -> this is skipped and behaviour is byte-identical.
    #[cfg(feature = "governor")]
    if let Some(pressure) = &state.pressure
        && pressure.should_hold()
    {
        #[cfg(feature = "metrics")]
        metrics::counter!("transport_backpressured_total", "transport" => "http", "reason" => "pressure")
            .increment(1);
        return shed_503();
    }

    // Extract W3C traceparent from incoming HTTP headers for distributed tracing
    #[cfg(feature = "transport-trace")]
    if let Some(tp) = headers
        .get(super::propagation::TRACEPARENT_HEADER)
        .and_then(|v| v.to_str().ok())
        && super::propagation::is_valid_traceparent(tp)
    {
        tracing::Span::current().record("traceparent", tp);
    }

    // Suppress unused variable warning when otel feature is disabled
    #[cfg(not(feature = "otel"))]
    let _ = &headers;

    let seq = state.sequence.fetch_add(1, Ordering::Relaxed);
    let format = PayloadFormat::detect(&body);
    let timestamp_ms = chrono::Utc::now().timestamp_millis();

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
    };

    match state.sender.try_send(msg) {
        Ok(()) => {
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
        !self.closed.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

/// Response statuses that mean the endpoint, or the gateway in front of it, is
/// busy or down rather than refusing this request, so the send is retried.
fn downstream_unavailable(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::REQUEST_TIMEOUT
            | reqwest::StatusCode::TOO_MANY_REQUESTS
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

impl TransportSender for HttpTransport {
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        // Outbound filter check
        if self.filter_engine.has_outbound_filters() {
            match self.filter_engine.apply_outbound(&payload) {
                super::filter::FilterDisposition::Pass => {}
                super::filter::FilterDisposition::Drop => return SendResult::Ok,
                super::filter::FilterDisposition::Dlq => return SendResult::FilteredDlq,
            }
        }

        let Some(base_url) = &self.endpoint else {
            return SendResult::Fatal(TransportError::Config(
                "no endpoint configured for sending".into(),
            ));
        };

        // Build URL: {base_url}/{destination} when non-empty, otherwise just {base_url}
        let url = if destination.is_empty() {
            base_url.clone()
        } else {
            let base = base_url.trim_end_matches('/');
            let suffix = destination.trim_start_matches('/');
            format!("{base}/{suffix}")
        };

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        // Build request with optional W3C traceparent header for distributed tracing
        let request_builder = self
            .client
            .post(&url)
            .header("content-type", "application/octet-stream");

        #[cfg(feature = "transport-trace")]
        let request_builder = if let Some(tp) = super::propagation::current_traceparent() {
            request_builder.header(super::propagation::TRACEPARENT_HEADER, tp)
        } else {
            request_builder
        };

        #[cfg(feature = "logger")]
        let payload_len = payload.len();
        // Capture wire size before `payload` moves into the request body.
        #[cfg(feature = "metrics")]
        let payload_bytes = payload.len();
        let result = match request_builder.body(payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                #[cfg(feature = "logger")]
                tracing::debug!(url = %url, bytes = payload_len, "HTTP transport: POST sent");

                #[cfg(feature = "metrics")]
                {
                    metrics::counter!("transport_sent_total", "transport" => "http").increment(1);
                    metrics::counter!("transport_sent_bytes_total", "transport" => "http")
                        .increment(payload_bytes as u64);
                }
                SendResult::Ok
            }
            Ok(resp) if downstream_unavailable(resp.status()) => {
                #[cfg(feature = "logger")]
                tracing::warn!(status = %resp.status(), url = %url, "HTTP transport: backpressure");

                #[cfg(feature = "metrics")]
                metrics::counter!("transport_backpressured_total", "transport" => "http")
                    .increment(1);
                SendResult::Backpressured
            }
            Ok(resp) => {
                #[cfg(feature = "logger")]
                tracing::warn!(status = %resp.status(), url = %url, "HTTP transport: send error");

                #[cfg(feature = "metrics")]
                metrics::counter!("transport_send_errors_total", "transport" => "http")
                    .increment(1);
                SendResult::Fatal(TransportError::Send(format!(
                    "HTTP {} from {}",
                    resp.status(),
                    url
                )))
            }
            Err(e) => {
                #[cfg(feature = "logger")]
                tracing::warn!(error = %e, url = %url, "HTTP transport: request failed");

                #[cfg(feature = "metrics")]
                metrics::counter!("transport_send_errors_total", "transport" => "http")
                    .increment(1);
                // Only a request that could never be built or followed is permanent;
                // refused, reset and timed-out connections clear when the endpoint returns.
                if e.is_builder() || e.is_redirect() {
                    SendResult::Fatal(TransportError::Send(format!("HTTP request failed: {e}")))
                } else {
                    SendResult::Backpressured
                }
            }
        };

        #[cfg(feature = "metrics")]
        metrics::histogram!("transport_send_duration_seconds", "transport" => "http")
            .record(start.elapsed().as_secs_f64());

        result
    }
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
                // The first record waits up to recv_timeout_ms; the rest only take
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
                messages.push(msg);
            }

            // Apply inbound filters via the shared partition helper; DLQ
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

    /// Whether `buf` holds a full request head and the body it declares.
    fn request_complete(buf: &[u8]) -> bool {
        let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            return false;
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
        let body_len = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        buf.len() >= head_end + 4 + body_len
    }

    /// A loopback endpoint that answers every request with `status`.
    async fn status_endpoint(status: u16) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                // Read the whole request so the reply never races the upload.
                let mut buf = Vec::new();
                let mut chunk = [0_u8; 4096];
                loop {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if request_complete(&buf) {
                        break;
                    }
                }
                let reply = format!(
                    "HTTP/1.1 {status} Canned\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = stream.write_all(reply.as_bytes()).await;
            }
        });
        addr
    }

    /// Busy and gateway statuses mean the endpoint is not there right now; any
    /// other failure status is the endpoint refusing this request.
    #[tokio::test]
    async fn only_busy_and_gateway_statuses_are_backpressure() {
        for (status, retried) in [
            (408, true),
            (429, true),
            (502, true),
            (503, true),
            (504, true),
            (400, false),
            (404, false),
            (500, false),
        ] {
            let addr = status_endpoint(status).await;
            let transport =
                HttpTransport::new(&HttpTransportConfig::sender(&format!("http://{addr}")))
                    .await
                    .unwrap();
            let result = transport
                .send("ingest", bytes::Bytes::from_static(b"{}"))
                .await;
            if retried {
                assert!(result.is_backpressured(), "HTTP {status}: got {result:?}");
            } else {
                assert!(result.is_fatal(), "HTTP {status}: got {result:?}");
            }
        }
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
        // Port 0 binds a free port atomically; a probe-then-rebind races
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
            #[cfg(feature = "governor")]
            pressure: None,
        };

        let response = ingest_handler(
            axum::extract::State(state),
            axum::extract::ConnectInfo("127.0.0.1:9".parse().expect("addr")),
            axum::http::HeaderMap::new(),
            axum::body::Bytes::from_static(b"{}"),
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
        use std::sync::atomic::AtomicBool;

        let receiver = HttpTransport::new(&receiver_config(100))
            .await
            .expect("receiver");
        let sender = Arc::new(sender_to(&receiver).await);
        let stop = Arc::new(AtomicBool::new(false));

        let mut senders = tokio::task::JoinSet::new();
        for task in 0..4_u32 {
            let sender = Arc::clone(&sender);
            let stop = Arc::clone(&stop);
            senders.spawn(async move {
                let mut acked = 0_usize;
                let mut seq = task * 1_000_000;
                while !stop.load(Ordering::Relaxed) {
                    let payload = bytes::Bytes::from(format!("{{\"seq\":{seq}}}"));
                    if sender.send("", payload).await.is_ok() {
                        acked += 1;
                    }
                    seq += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                acked
            });
        }

        // Acks pile up unread, as behind a consumer that stopped calling recv.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        receiver.close().await.expect("close");
        let delivered = drain_until_closed(&receiver).await.expect("drain");
        stop.store(true, Ordering::Relaxed);

        let mut acked = 0;
        while let Some(count) = senders.join_next().await {
            acked += count.expect("sender task");
        }
        assert!(acked > 0, "the senders never got an ack");
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
