// Project:   scalo
// File:      src/transport/grpc/mod.rs
// Purpose:   gRPC transport backend
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # gRPC Transport
//!
//! Native gRPC transport using tonic. Supports client mode (sending),
//! server mode (receiving), or both.
//!
//! ## Native Protocol
//!
//! Lightweight bulk bytes transfer via `scalo.transport.v1.Transport/Push`.
//! Payload is opaque bytes (JSON, MsgPack, or Arrow IPC) with a format hint.
//!
//! ## Vector Wire Protocol Compatibility (optional)
//!
//! When the `transport-grpc-vector-compat` feature is enabled and
//! `GrpcConfig::vector_compat` is true, the server also accepts
//! `vector.Vector/PushEvents` RPCs from legacy Vector sinks.
//!
//! ## Shutdown
//!
//! The server acknowledges a record once it is queued for `recv`, so a
//! receiving service shuts down in this order: `close()`, then `recv` until it
//! returns `TransportError::Closed`, then its final flush. `close()` refuses
//! new pushes with `Unavailable`, which senders retry.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::{GrpcTransport, GrpcConfig, TransportBase, TransportError, TransportReceiver};
//!
//! // Server mode (receive from remote senders)
//! let config = GrpcConfig::server("0.0.0.0:6000");
//! let transport = GrpcTransport::new(&config).await?;
//!
//! let records = transport.recv(100).await?.records;
//! // commit is a no-op for gRPC (no persistence)
//! transport.commit(&[]).await?;
//!
//! // Shutdown: stop intake, then take everything already acknowledged.
//! transport.close().await?;
//! loop {
//!     match transport.recv(100).await {
//!         Ok(batch) => { /* process batch.records */ }
//!         Err(TransportError::Closed) => break,
//!         Err(e) => return Err(e.into()),
//!     }
//! }
//! ```

pub mod batch;
pub mod config;
pub mod proto;
pub mod token;

pub use config::GrpcConfig;
pub use token::GrpcToken;

use super::error::{TransportError, TransportResult};
use super::traits::{RecvBatch, TransportBase, TransportReceiver, TransportSender};
use super::types::{Message, PayloadFormat, SendResult};
use super::work_batch::{Record, WorkBatch};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

/// The spawned tonic serve loop.
type ServerTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

/// Holds one `RouteBatch` with more records than `recv_buffer_size`, whole,
/// until `recv` has taken all of it.
///
/// The record queue can never reserve room for such a batch at once, so it
/// waits here and is admitted in one step or refused, as a batch that fits the
/// queue is. One batch at a time bounds what the slot holds.
#[derive(Default)]
struct OversizeSlot {
    state: parking_lot::Mutex<OversizeState>,
    /// Wakes a `recv` waiting on the record queue when a batch lands here.
    landed: tokio::sync::Notify,
}

#[derive(Default)]
struct OversizeState {
    records: VecDeque<Message<GrpcToken>>,
    closed: bool,
}

impl OversizeSlot {
    /// Admit a whole batch, or refuse it with the answer for the sender.
    fn admit(&self, batch: VecDeque<Message<GrpcToken>>) -> Result<(), Status> {
        let mut state = self.state.lock();
        if state.closed {
            return Err(receiver_closed());
        }
        if !state.records.is_empty() {
            return Err(receiver_full());
        }
        state.records = batch;
        drop(state);
        self.landed.notify_one();
        Ok(())
    }

    /// Move records into `out` until it holds `max`.
    fn take(&self, out: &mut Vec<Message<GrpcToken>>, max: usize) {
        let mut state = self.state.lock();
        let n = max.saturating_sub(out.len()).min(state.records.len());
        out.extend(state.records.drain(..n));
    }

    /// Refuse batches from now on; one already admitted stays for `recv`.
    fn close(&self) {
        self.state.lock().closed = true;
    }
}

/// The answer to a push the receive queue has no room for: `ResourceExhausted`,
/// which a sender retries.
fn receiver_full() -> Status {
    #[cfg(feature = "metrics")]
    metrics::counter!("transport_backpressured_total", "transport" => "grpc").increment(1);
    Status::resource_exhausted("receiver buffer full")
}

/// The answer to a push after `close()`: `Unavailable`, which a sender retries.
pub(crate) fn receiver_closed() -> Status {
    #[cfg(feature = "metrics")]
    metrics::counter!("transport_refused_total", "transport" => "grpc").increment(1);
    Status::unavailable("receiver closed")
}

/// The pressure governor a receive server sheds on, carried to the
/// Vector-compat handler as a request extension.
#[cfg(feature = "governor")]
#[derive(Clone)]
pub(crate) struct InboundGate(pub(crate) Arc<crate::governor::UnifiedPressure>);

/// Refuse a push with `Unavailable`, the gRPC analogue of HTTP 503, while the
/// pressure governor holds intake.
#[cfg(feature = "governor")]
pub(crate) fn shed_if_held(
    pressure: Option<&Arc<crate::governor::UnifiedPressure>>,
) -> Result<(), Status> {
    if pressure.is_some_and(|p| p.should_hold()) {
        #[cfg(feature = "metrics")]
        metrics::counter!(
            "transport_backpressured_total",
            "transport" => "grpc",
            "reason" => "pressure"
        )
        .increment(1);
        return Err(Status::unavailable("under pressure -- inbound held"));
    }
    Ok(())
}

/// Count records the receive server queued for `recv`.
#[cfg(feature = "metrics")]
pub(crate) fn count_received(records: u64, bytes: usize) {
    metrics::counter!("transport_received_bytes_total", "transport" => "grpc")
        .increment(bytes as u64);
    metrics::counter!("transport_received_events_total", "transport" => "grpc").increment(records);
}

/// A lazily dialled channel to `ep`, as both gRPC clients build it.
///
/// A dial whose DNS lookup, TCP connect or TLS handshake is unfinished at nine
/// tenths of `send_timeout_ms` is abandoned. A connection that has read nothing
/// for `send_timeout_ms` is sent an HTTP/2 PING and closed if the PING goes
/// unanswered for as long again, so a peer that stays connected but stops
/// answering is dropped and the next call dials afresh. `0` leaves the dial
/// unbounded and PINGs at the default send limit.
pub(crate) fn lazy_channel(
    mut ep: tonic::transport::Endpoint,
    send_timeout_ms: u64,
) -> tonic::transport::Channel {
    // A dial left running when its send gives up hands its failure to the
    // next send, so the dial ends at nine tenths of the limit.
    if send_timeout_ms > 0 {
        ep = ep.connect_timeout(Duration::from_millis(send_timeout_ms) * 9 / 10);
    }

    let keep_alive = Duration::from_millis(if send_timeout_ms > 0 {
        send_timeout_ms
    } else {
        config::DEFAULT_SEND_TIMEOUT_MS
    });
    ep = ep
        .http2_keep_alive_interval(keep_alive)
        .keep_alive_timeout(keep_alive)
        .keep_alive_while_idle(true);

    // Given the connector, tonic abandons a dial whose DNS, TCP connect or TLS
    // handshake outruns the connect timeout, where connect_lazy() bounds the
    // TCP connect alone and the next send queues behind it.
    let mut tcp = hyper_util::client::legacy::connect::HttpConnector::new();
    // tonic's own settings: an https URI passes through to its TLS layer.
    tcp.enforce_http(false);
    tcp.set_nodelay(true);
    ep.connect_with_connector_lazy(tcp)
}

/// gRPC transport for inter-service communication.
///
/// Implements both `TransportSender` and `TransportReceiver`, so it also
/// satisfies the unified `Transport` trait via blanket impl.
pub struct GrpcTransport {
    /// Client for sending (None if server-only mode).
    client: Option<proto::transport_client::TransportClient<tonic::transport::Channel>>,

    /// Receiver channel (None if client-only mode).
    receiver: Option<tokio::sync::Mutex<mpsc::Receiver<Message<GrpcToken>>>>,

    /// A `RouteBatch` too large for the receiver channel, waiting for `recv`.
    oversize: Arc<OversizeSlot>,

    /// Graceful-shutdown signal for the server task. Behind a
    /// `Mutex<Option<..>>` so `close(&self)` can take and fire it.
    shutdown_tx: parking_lot::Mutex<Option<oneshot::Sender<()>>>,

    /// Server task. `close()` awaits it and `Drop` aborts it: a dropped
    /// `JoinHandle` only detaches the task, leaving the listener bound.
    server_task: parking_lot::Mutex<Option<ServerTask>>,

    /// Whether the transport is closed.
    closed: AtomicBool,

    /// Shared healthy flag -- read by health registry closure, written by close().
    healthy: Arc<AtomicBool>,

    /// Receive timeout (milliseconds).
    recv_timeout_ms: u64,

    /// Send deadline, end to end (milliseconds, 0 = none).
    send_timeout_ms: u64,

    /// Largest encoded message `send` and `send_batch` put on the wire,
    /// measured uncompressed, as the receiver's decoder measures it.
    max_message_size: usize,

    /// Address the receive server bound (None if client-only mode).
    local_addr: Option<std::net::SocketAddr>,

    /// In-flight send count (for metrics).
    #[cfg(feature = "metrics")]
    inflight: AtomicU64,

    /// Transport-level message filter engine.
    filter_engine: super::filter::TransportFilterEngine,
}

/// Build a tonic `ClientTlsConfig` from the unified TLS fields on `GrpcConfig`.
///
/// tonic owns its TLS stack (like librdkafka), so map `TlsTrust` onto
/// `ClientTlsConfig`: private-CA PEM (else OS native roots), optional SNI
/// override, optional mTLS client identity.
fn build_grpc_client_tls(
    config: &GrpcConfig,
) -> TransportResult<tonic::transport::ClientTlsConfig> {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};

    let mut tls = ClientTlsConfig::new();

    if let Some(ref ca) = config.tls_ca_path {
        let pem = std::fs::read(ca)
            .map_err(|e| TransportError::Config(format!("gRPC TLS: cannot read ca {ca}: {e}")))?;
        tls = tls.ca_certificate(Certificate::from_pem(pem));
    } else {
        // No private CA -> trust the OS native roots.
        tls = tls.with_native_roots();
    }

    if let Some(ref domain) = config.tls_domain {
        tls = tls.domain_name(domain.clone());
    }

    // mTLS identity -- both cert and key, or neither.
    match (&config.tls_client_cert_path, &config.tls_client_key_path) {
        (Some(cert), Some(key)) => {
            let cert_pem = std::fs::read(cert).map_err(|e| {
                TransportError::Config(format!("gRPC TLS: cannot read client cert {cert}: {e}"))
            })?;
            let key_pem = std::fs::read(key).map_err(|e| {
                TransportError::Config(format!("gRPC TLS: cannot read client key {key}: {e}"))
            })?;
            tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
        }
        (None, None) => {}
        _ => {
            return Err(TransportError::Config(
                "gRPC TLS: mTLS requires BOTH tls_client_cert_path and tls_client_key_path"
                    .to_string(),
            ));
        }
    }

    Ok(tls)
}

impl GrpcTransport {
    /// Create a new gRPC transport.
    ///
    /// # Configuration
    ///
    /// - Set `config.listen` to start a gRPC server (receive mode).
    /// - Set `config.endpoint` to connect to a remote server (send mode).
    /// - Set both for bidirectional communication.
    ///
    /// # Errors
    ///
    /// Returns error if the listen address is invalid or the server fails to start.
    pub async fn new(config: &GrpcConfig) -> TransportResult<Self> {
        Self::new_inner(
            config,
            #[cfg(feature = "governor")]
            None,
        )
        .await
    }

    /// Create a gRPC transport bound to a pressure governor (`governor`
    /// feature).
    ///
    /// Like [`new`](Self::new), but the receive server consults `pressure`
    /// before enqueuing each inbound Push / batch record: while
    /// [`UnifiedPressure::should_hold`](crate::governor::UnifiedPressure::should_hold)
    /// holds, the RPC is rejected with `Status::unavailable` (the gRPC analogue
    /// of HTTP 503). `None` is equivalent to [`new`](Self::new).
    ///
    /// # Errors
    ///
    /// Same as [`new`](Self::new).
    #[cfg(feature = "governor")]
    pub async fn with_pressure(
        config: &GrpcConfig,
        pressure: Option<Arc<crate::governor::UnifiedPressure>>,
    ) -> TransportResult<Self> {
        Self::new_inner(config, pressure).await
    }

    async fn new_inner(
        config: &GrpcConfig,
        #[cfg(feature = "governor")] pressure: Option<Arc<crate::governor::UnifiedPressure>>,
    ) -> TransportResult<Self> {
        let mut client = None;
        let mut receiver = None;
        let mut shutdown_tx = None;
        let mut server_handle = None;
        let mut local_addr = None;
        let sequence = Arc::new(AtomicU64::new(0));
        let oversize = Arc::new(OversizeSlot::default());

        // Set up client (lazy connection -- doesn't fail until first RPC)
        if let Some(endpoint) = &config.endpoint {
            let mut ep = tonic::transport::Channel::from_shared(endpoint.clone())
                .map_err(|e| TransportError::Config(format!("invalid endpoint: {e}")))?;

            // Client TLS. tonic owns its TLS stack, so we map the unified
            // vocabulary onto ClientTlsConfig (private CA, mTLS identity, SNI).
            if config.tls_enabled {
                ep = ep
                    .tls_config(build_grpc_client_tls(config)?)
                    .map_err(|e| TransportError::Config(format!("gRPC TLS config: {e}")))?;
            }

            let channel = lazy_channel(ep, config.send_timeout_ms);

            // No encoding limit: tonic's refusal arrives as a stream reset that
            // reads as an outage, so send and send_batch check the size instead.
            let mut c = proto::transport_client::TransportClient::new(channel)
                .max_decoding_message_size(config.max_message_size);

            if config.compression {
                c = c
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
            }

            client = Some(c);
        }

        // Set up server
        if let Some(listen) = &config.listen {
            let addr: std::net::SocketAddr = listen
                .parse()
                .map_err(|e| TransportError::Config(format!("invalid listen address: {e}")))?;

            let (tx, rx) = mpsc::channel(config.recv_buffer_size);
            let (sd_tx, sd_rx) = oneshot::channel();

            // Native service
            let dfe_svc = TransportServiceImpl {
                sender: tx.clone(),
                sequence: sequence.clone(),
                oversize: Arc::clone(&oversize),
                #[cfg(feature = "governor")]
                pressure: pressure.clone(),
            };

            let dfe_server = proto::transport_server::TransportServer::new(dfe_svc)
                .max_decoding_message_size(config.max_message_size)
                .max_encoding_message_size(config.max_message_size)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip);

            // Build server with optional Vector compat
            let mut builder = tonic::transport::Server::builder();

            #[cfg(feature = "transport-grpc-vector-compat")]
            let router = if config.vector_compat {
                let vector_svc =
                    super::vector_compat::source::VectorCompatService::new(tx, sequence.clone());
                let vector_server =
                    super::vector_compat::proto::vector::vector_server::VectorServer::new(
                        vector_svc,
                    )
                    .max_decoding_message_size(config.max_message_size)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip);

                // Hand the governor to the Vector-compat pushes, which shed on it
                // as the native handlers do.
                #[cfg(feature = "governor")]
                let vector_server = {
                    let gate = pressure.clone().map(InboundGate);
                    tonic::service::interceptor::InterceptedService::new(
                        vector_server,
                        move |mut request: Request<()>| {
                            if let Some(gate) = &gate {
                                request.extensions_mut().insert(gate.clone());
                            }
                            Ok(request)
                        },
                    )
                };

                builder.add_service(dfe_server).add_service(vector_server)
            } else {
                builder.add_service(dfe_server)
            };

            #[cfg(not(feature = "transport-grpc-vector-compat"))]
            let router = builder.add_service(dfe_server);

            // Bind the listener synchronously BEFORE spawning the serve task,
            // so `new()` returning is a true readiness signal -- callers connect
            // immediately, no polling. Binding inside the spawned task let
            // `new()` return before the socket existed.
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| TransportError::Config(format!("failed to bind {addr}: {e}")))?;
            local_addr = Some(listener.local_addr().map_err(|e| {
                TransportError::Config(format!("cannot read the address bound for {addr}: {e}"))
            })?);
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

            let handle = tokio::spawn(async move {
                router
                    .serve_with_incoming_shutdown(incoming, async {
                        sd_rx.await.ok();
                    })
                    .await
            });

            receiver = Some(tokio::sync::Mutex::new(rx));
            shutdown_tx = Some(sd_tx);
            server_handle = Some(handle);
        } else {
            // No receive server -> nothing to attach the governor to. Consume
            // it to silence the unused-variable warning.
            #[cfg(feature = "governor")]
            let _ = pressure;
        }

        let healthy = Arc::new(AtomicBool::new(true));

        let filter_engine = super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade(),
        )?;

        #[cfg(feature = "health")]
        {
            let h = Arc::clone(&healthy);
            crate::health::HealthRegistry::register("transport:grpc", move || {
                if h.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Healthy
                } else {
                    crate::health::HealthStatus::Unhealthy
                }
            });
        }

        Ok(Self {
            client,
            receiver,
            oversize,
            shutdown_tx: parking_lot::Mutex::new(shutdown_tx),
            server_task: parking_lot::Mutex::new(server_handle),
            closed: AtomicBool::new(false),
            healthy,
            recv_timeout_ms: config.recv_timeout_ms,
            send_timeout_ms: config.send_timeout_ms,
            max_message_size: config.max_message_size,
            local_addr,
            #[cfg(feature = "metrics")]
            inflight: AtomicU64::new(0),
            filter_engine,
        })
    }

    /// The address the receive server is listening on, or `None` in
    /// client-only mode.
    ///
    /// With `listen` set to port 0 this is where the OS-assigned port is read
    /// back.
    #[must_use]
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.local_addr
    }

    /// Run one RPC under `send_timeout_ms`, end to end.
    ///
    /// The `grpc-timeout` header only starts counting once the request is on a
    /// connection, so DNS, the TCP connect and the TLS handshake are bounded
    /// here. Running out reads as `DeadlineExceeded`, which is backpressure.
    async fn within_send_timeout<T>(
        &self,
        rpc: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        if self.send_timeout_ms == 0 {
            return rpc.await;
        }
        tokio::time::timeout(Duration::from_millis(self.send_timeout_ms), rpc)
            .await
            .unwrap_or_else(|_elapsed| {
                Err(Status::deadline_exceeded(format!(
                    "no answer within send_timeout_ms ({} ms)",
                    self.send_timeout_ms
                )))
            })
    }

    /// Stop the receive server without waiting on its clients.
    ///
    /// The graceful signal has every open connection finish its in-flight RPCs
    /// in tonic's own per-connection tasks, which outlive the serve task.
    /// Aborting the serve task frees the listener at once, so a client that
    /// never completes its RPC cannot hold `close()` open.
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
            Ok(Err(e)) => tracing::warn!(error = %e, "gRPC: server ended with an error"),
            Err(e) if e.is_cancelled() => {}
            Err(e) => tracing::warn!(error = %e, "gRPC: server task panicked"),
        }
    }
}

/// Build the `Push` body for one record, metadata included, so its encoded
/// length is the length tonic checks against `max_message_size`.
fn push_request(destination: &str, payload: bytes::Bytes) -> proto::PushRequest {
    let mut metadata = HashMap::new();
    if !destination.is_empty() {
        metadata.insert("topic".to_string(), destination.to_string());
    }

    // Inject W3C traceparent into gRPC metadata.
    #[cfg(feature = "transport-trace")]
    if let Some(tp) = super::propagation::current_traceparent() {
        metadata.insert(super::propagation::TRACEPARENT_HEADER.to_string(), tp);
    }

    proto::PushRequest {
        // proto field is `Bytes` (`.bytes(".")` in build.rs) -- move, no copy.
        payload,
        format: proto::Format::Auto.into(),
        metadata,
    }
}

/// Whether the receiver's decoder refused the message as over its
/// `max_message_size`.
fn refused_for_size(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::OutOfRange
}

/// The send result for one record over the message-size ceiling: dead-letter
/// it, because the same bytes are refused on every retry.
fn too_large_result(
    destination: &str,
    encoded_len: usize,
    limit: usize,
    detail: &str,
) -> SendResult {
    #[cfg(feature = "metrics")]
    metrics::counter!("transport_message_too_large_total", "transport" => "grpc").increment(1);
    tracing::warn!(
        destination,
        encoded_len,
        limit,
        detail,
        "gRPC: record exceeds max_message_size -- dead-lettering it; raise max_message_size \
         on the sender and the receiver together"
    );
    SendResult::FilteredDlq
}

/// Whether a failed RPC means the server is down or busy rather than refusing
/// the request.
///
/// A transient code qualifies, and so does any status carrying a source error:
/// tonic attaches one only when the client's own connection failed, or its h2
/// stream was reset, so the server never answered. A status the server sent
/// carries none. `DeadlineExceeded` is `send_timeout_ms` firing on a slow or
/// hung server, and `Cancelled` the server cutting the RPC at its own deadline,
/// as tonic's server does at the `grpc-timeout` a send carries.
fn downstream_unavailable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::ResourceExhausted
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
    ) || std::error::Error::source(status).is_some()
}

/// The send result for a failed RPC: backpressure while the server is down or
/// busy, so the caller retries rather than drops.
fn failed_rpc_result(status: &tonic::Status) -> SendResult {
    if downstream_unavailable(status) {
        #[cfg(feature = "metrics")]
        metrics::counter!("transport_backpressured_total", "transport" => "grpc").increment(1);
        SendResult::Backpressured
    } else {
        #[cfg(feature = "metrics")]
        metrics::counter!("transport_send_errors_total", "transport" => "grpc").increment(1);
        SendResult::Fatal(TransportError::Send(status.message().to_string()))
    }
}

impl TransportSender for GrpcTransport {
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

        let Some(client) = &self.client else {
            return SendResult::Fatal(TransportError::Config(
                "no endpoint configured for sending".into(),
            ));
        };

        // Capture wire size before `payload` moves into the request.
        #[cfg(feature = "metrics")]
        let payload_len = payload.len();

        // Checked uncompressed, as the receiver decompresses to this limit and
        // answers a compressed over-limit record with ResourceExhausted.
        let body = push_request(destination, payload);
        let encoded_len = prost::Message::encoded_len(&body);
        if encoded_len > self.max_message_size {
            return too_large_result(
                destination,
                encoded_len,
                self.max_message_size,
                "refused before sending",
            );
        }

        let mut request = tonic::Request::new(body);

        // The grpc-timeout header tells the server the deadline.
        if self.send_timeout_ms > 0 {
            request.set_timeout(Duration::from_millis(self.send_timeout_ms));
        }

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        #[cfg(feature = "metrics")]
        self.inflight.fetch_add(1, Ordering::Relaxed);

        // tonic clients are cheaply cloneable (shared channel)
        let mut client = client.clone();
        let result = match self.within_send_timeout(client.push(request)).await {
            Ok(_) => {
                #[cfg(feature = "metrics")]
                {
                    metrics::counter!("transport_sent_total", "transport" => "grpc").increment(1);
                    metrics::counter!("transport_sent_bytes_total", "transport" => "grpc")
                        .increment(payload_len as u64);
                }
                SendResult::Ok
            }
            // The receiver's limit can be below ours, and gzip can grow an
            // at-limit record past it.
            Err(status) if refused_for_size(&status) => too_large_result(
                destination,
                encoded_len,
                self.max_message_size,
                status.message(),
            ),
            Err(status) => failed_rpc_result(&status),
        };

        #[cfg(feature = "metrics")]
        {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            metrics::gauge!("transport_inflight", "transport" => "grpc")
                .set(self.inflight.load(Ordering::Relaxed) as f64);
            metrics::histogram!(
                "transport_send_duration_seconds",
                "transport" => "grpc"
            )
            .record(start.elapsed().as_secs_f64());
        }

        result
    }

    /// Send a whole batch of records in ONE `RouteBatch` RPC.
    ///
    /// Native batch override of [`TransportSender::send_batch`]: serde-less
    /// scalo<->scalo transfer. Records map to a proto
    /// [`Batch`](proto::Batch) via [`batch::records_to_proto`]; payloads travel
    /// as OPAQUE `bytes`, the JSON / MsgPack codec is NEVER invoked in transit.
    ///
    /// Commit tokens and inline-DLQ entries are NOT sent -- the SENDER's local
    /// concern. Pass the records (e.g. `&workbatch.records`); the caller fires
    /// its commit tokens locally after this returns `Ok`.
    ///
    /// ## Atomic (all-or-nothing) acceptance
    ///
    /// The server reserves channel capacity for the WHOLE batch (one
    /// `try_reserve_many`) before enqueuing any record -- no partial-send
    /// window. A batch with more records than the receiver's
    /// `recv_buffer_size` never fits the channel, so it is held whole in a slot
    /// of its own, one batch at a time. `Backpressured` means ZERO records were
    /// admitted, so the caller retries the whole block (at-least-once) with no
    /// duplicate prefix.
    ///
    /// # Errors / result
    ///
    /// Returns a [`SendResult`]. `Backpressured` maps the same transient gRPC
    /// codes as [`send`](TransportSender::send).
    async fn send_batch(&self, records: &[Record]) -> SendResult {
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }

        let Some(client) = &self.client else {
            return SendResult::Fatal(TransportError::Config(
                "no endpoint configured for sending".into(),
            ));
        };

        // Apply outbound filters BEFORE the wire: a record matched by a `drop`
        // or `dlq` filter must NOT be transmitted. This path once bypassed the
        // filter entirely -- a record told to drop sailed through.
        let to_send: Vec<Record> = if self.filter_engine.has_outbound_filters() {
            let mut keep = Vec::with_capacity(records.len());
            for r in records {
                match self.filter_engine.apply_outbound(&r.payload) {
                    super::filter::FilterDisposition::Pass => keep.push(r.clone()),
                    super::filter::FilterDisposition::Drop
                    | super::filter::FilterDisposition::Dlq => {}
                }
            }
            keep
        } else {
            records.to_vec()
        };

        // Everything was filtered out -- nothing to send, batch is "done".
        if to_send.is_empty() {
            return SendResult::Ok;
        }
        let sent_count = to_send.len();

        #[cfg(feature = "metrics")]
        let payload_bytes: usize = to_send.iter().map(|r| r.payload.len()).sum();

        // Map records -> proto Batch. Payloads are MOVED (Bytes handle), opaque.
        let proto_batch = batch::records_to_proto(to_send);

        // An over-limit block never succeeds on retry and the fix is a smaller
        // one, so name the limit and the byte-budget lever; checked like `send`.
        let encoded_len = prost::Message::encoded_len(&proto_batch);
        if encoded_len > self.max_message_size {
            #[cfg(feature = "metrics")]
            metrics::counter!("transport_oversize_total", "transport" => "grpc").increment(1);
            return SendResult::Fatal(TransportError::Config(format!(
                "gRPC batch of {encoded_len} encoded bytes exceeds max_message_size \
                 {} -- lower the self-regulation byte budget below the gRPC limit",
                self.max_message_size
            )));
        }

        let mut request = tonic::Request::new(proto_batch);

        // Inject W3C traceparent into gRPC metadata.
        #[cfg(feature = "transport-trace")]
        if let Some(tp) = super::propagation::current_traceparent()
            && let Ok(val) = tp.parse()
        {
            request
                .metadata_mut()
                .insert(super::propagation::TRACEPARENT_HEADER, val);
        }

        if self.send_timeout_ms > 0 {
            request.set_timeout(Duration::from_millis(self.send_timeout_ms));
        }

        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();
        #[cfg(feature = "metrics")]
        self.inflight.fetch_add(1, Ordering::Relaxed);

        let mut client = client.clone();
        let result = match self.within_send_timeout(client.route_batch(request)).await {
            Ok(response) => {
                // Server is all-or-nothing today, but the proto permits partial
                // acceptance. Treating ANY Ok as full success would fire every
                // commit token while the receiver kept only a prefix -- silent
                // loss. Require accepted == sent; a shortfall is retryable
                // (at-least-once).
                let accepted = response.into_inner().accepted;
                if accepted < sent_count as u64 {
                    #[cfg(feature = "metrics")]
                    metrics::counter!(
                        "transport_backpressured_total",
                        "transport" => "grpc"
                    )
                    .increment(1);
                    tracing::warn!(
                        accepted,
                        sent = sent_count,
                        "gRPC RouteBatch partially accepted -- retrying whole block"
                    );
                    SendResult::Backpressured
                } else {
                    #[cfg(feature = "metrics")]
                    {
                        metrics::counter!(
                            "transport_sent_total",
                            "transport" => "grpc",
                            "path" => "batch"
                        )
                        .increment(sent_count as u64);
                        metrics::counter!("transport_sent_bytes_total", "transport" => "grpc")
                            .increment(payload_bytes as u64);
                    }
                    SendResult::Ok
                }
            }
            Err(status) => failed_rpc_result(&status),
        };

        #[cfg(feature = "metrics")]
        {
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            metrics::histogram!(
                "transport_send_duration_seconds",
                "transport" => "grpc"
            )
            .record(start.elapsed().as_secs_f64());
        }

        result
    }
}

impl TransportBase for GrpcTransport {
    /// Stop sending and receiving, keeping every record already acknowledged.
    ///
    /// A push that arrives from here on is refused with `Unavailable`, which a
    /// sender retries. Records the server already acked stay queued: call
    /// [`recv`](TransportReceiver::recv) until it returns
    /// [`TransportError::Closed`] or they are lost. Open connections finish
    /// their in-flight RPCs on their own, and the listener is free when this
    /// returns. Waits for a `recv` in progress (at most `recv_timeout_ms`).
    /// Idempotent.
    async fn close(&self) -> TransportResult<()> {
        self.closed.store(true, Ordering::Relaxed);
        self.healthy.store(false, Ordering::Relaxed);

        // The slot first: once recv sees the channel closed, nothing more can
        // land in the slot.
        self.oversize.close();
        if let Some(receiver) = &self.receiver {
            receiver.lock().await.close();
        }
        self.stop_server().await;
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        let healthy = self.healthy.load(Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        metrics::gauge!("transport_healthy", "transport" => "grpc").set(if healthy {
            1.0
        } else {
            0.0
        });
        healthy
    }

    fn name(&self) -> &'static str {
        "grpc"
    }
}

impl TransportReceiver for GrpcTransport {
    type Token = GrpcToken;

    /// Receive up to `max` records the server has acked.
    ///
    /// A `RouteBatch` held whole because it outnumbers `recv_buffer_size` is
    /// handed over first. After [`close`](TransportBase::close) this keeps
    /// returning the records still queued, then [`TransportError::Closed`] once
    /// none are left.
    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
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
        // Only the first record is waited for, up to recv_timeout_ms; the rest
        // only take what is already queued.
        let wait_until = (self.recv_timeout_ms > 0)
            .then(|| tokio::time::Instant::now() + Duration::from_millis(self.recv_timeout_ms));

        while messages.len() < max {
            self.oversize.take(&mut messages, max);
            if messages.len() >= max {
                break;
            }
            match rx.try_recv() {
                Ok(msg) => {
                    messages.push(msg);
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // close() shut the slot before the channel, so what it
                    // holds now is all it will hold.
                    self.oversize.take(&mut messages, max);
                    if messages.is_empty() {
                        return Err(TransportError::Closed);
                    }
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            let Some(deadline) = wait_until.filter(|_| messages.is_empty()) else {
                break;
            };
            // A batch that lands in the slot after the take above wakes this.
            let landed = self.oversize.landed.notified();
            tokio::select! {
                msg = rx.recv() => {
                    // None: closed and drained; the next pass says Closed.
                    if let Some(msg) = msg {
                        messages.push(msg);
                    }
                }
                () = landed => {}
                () = tokio::time::sleep_until(deadline) => break,
            }
        }

        // Apply inbound filters via the shared partition helper; DLQ entries
        // are returned in the RecvBatch for the caller to route onward.
        let batch = self.filter_engine.partition_batch(
            messages,
            |m| m.payload.as_ref(),
            |m| m.key.clone(),
            |m| m.token.clone(),
        );
        let messages = batch.messages;
        let dlq_entries = batch.dlq_entries;
        let filtered_tokens = batch.filtered_tokens;

        Ok(RecvBatch {
            messages,
            dlq_entries,
            filtered_tokens,
        }
        .into())
    }

    async fn commit(&self, _tokens: &[Self::Token]) -> TransportResult<()> {
        // gRPC has no broker-side persistence -- commit is a no-op.
        // Acknowledgement is implicit in the Push RPC response.
        Ok(())
    }
}

impl Drop for GrpcTransport {
    fn drop(&mut self) {
        // Abort explicitly: dropping the handle would detach the serve task.
        if let Some(task) = self.server_task.get_mut().take() {
            task.abort();
        }
    }
}

// --- Transport gRPC service implementation ---

/// Internal service implementation that receives Push RPCs
/// and forwards messages into the transport's mpsc channel.
struct TransportServiceImpl {
    sender: mpsc::Sender<Message<GrpcToken>>,
    sequence: Arc<AtomicU64>,
    /// Where a `RouteBatch` too large for `sender` waits for `recv`.
    oversize: Arc<OversizeSlot>,
    /// Optional pressure governor (`governor` feature). `None` -> handlers
    /// never consult it. `Some` rejects an inbound Push / batch record with
    /// `Status::unavailable` while [`UnifiedPressure::should_hold`] holds --
    /// pressure-driven shedding on top of the channel-full rejection.
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
}

impl TransportServiceImpl {
    /// Wrap one `RouteBatch` record for `recv`, taking the next sequence.
    fn message(&self, record: Record) -> Message<GrpcToken> {
        // Auto means the sender did not pin a format. Detect from the lead
        // byte so the receiver gets a concrete hint; does NOT parse/decode.
        let format = if record.metadata.format == PayloadFormat::Auto {
            PayloadFormat::detect(&record.payload)
        } else {
            record.metadata.format
        };
        Message {
            key: record.key,
            payload: record.payload,
            token: GrpcToken::new(self.sequence.fetch_add(1, Ordering::Relaxed)),
            timestamp_ms: record.metadata.timestamp_ms,
            format,
        }
    }
}

#[tonic::async_trait]
impl proto::transport_server::Transport for TransportServiceImpl {
    async fn push(
        &self,
        request: Request<proto::PushRequest>,
    ) -> Result<Response<proto::PushResponse>, Status> {
        // Shed before doing any work while the governor holds intake.
        #[cfg(feature = "governor")]
        shed_if_held(self.pressure.as_ref())?;

        let req = request.into_inner();
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

        // Extract W3C traceparent from incoming gRPC metadata.
        #[cfg(feature = "transport-trace")]
        if let Some(tp) = req.metadata.get(super::propagation::TRACEPARENT_HEADER)
            && super::propagation::is_valid_traceparent(tp)
        {
            tracing::Span::current().record("traceparent", tp.as_str());
        }

        let format = PayloadFormat::detect(&req.payload);
        let key = req.metadata.get("topic").map(|s| Arc::from(s.as_str()));

        // Capture wire size before `req.payload` moves into the message.
        #[cfg(feature = "metrics")]
        let payload_len = req.payload.len();

        // `req.payload` is prost `Bytes` (`.bytes(".")`) -- zero-copy decode,
        // so this is a move not a copy.
        let msg = Message {
            key,
            payload: req.payload,
            token: GrpcToken::new(seq),
            timestamp_ms: None,
            format,
        };

        match self.sender.try_send(msg) {
            Ok(()) => {
                #[cfg(feature = "metrics")]
                {
                    count_received(1, payload_len);
                    metrics::gauge!("transport_queue_size", "transport" => "grpc").set(
                        self.sender
                            .max_capacity()
                            .saturating_sub(self.sender.capacity()) as f64,
                    );
                }
                Ok(Response::new(proto::PushResponse { accepted: 1 }))
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(receiver_full()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(receiver_closed()),
        }
    }

    async fn route_batch(
        &self,
        request: Request<proto::Batch>,
    ) -> Result<Response<proto::BatchAck>, Status> {
        // Shed the whole batch while the governor holds intake.
        #[cfg(feature = "governor")]
        shed_if_held(self.pressure.as_ref())?;

        // Extract W3C traceparent BEFORE consuming the request body.
        #[cfg(feature = "transport-trace")]
        if let Some(tp) = request
            .metadata()
            .get(super::propagation::TRACEPARENT_HEADER)
            .and_then(|v| v.to_str().ok())
            && super::propagation::is_valid_traceparent(tp)
        {
            tracing::Span::current().record("traceparent", tp);
        }

        let proto_batch = request.into_inner();

        // Decode proto Batch -> scalo Records (payloads zero-copy `Bytes`,
        // codec NOT invoked). recv() delivers them unchanged, from the channel
        // the single-message Push path uses or from the oversize slot.
        let records = batch::proto_batch_to_records(proto_batch);
        let accepted = records.len() as u64;
        // Sum raw wire bytes BEFORE the records move into the channel below.
        #[cfg(feature = "metrics")]
        let batch_bytes: usize = records.iter().map(|r| r.payload.len()).sum();

        // A batch the channel can never hold at once waits whole in its own
        // slot, admitted in one step or refused, like one that fits.
        if records.len() > self.sender.max_capacity() {
            let batch = records.into_iter().map(|r| self.message(r)).collect();
            self.oversize.admit(batch)?;
        } else {
            // ATOMICITY: reserve channel capacity for the WHOLE batch via
            // `try_reserve_many` BEFORE enqueuing ANY record. Cannot fit ->
            // reject all-or-nothing, so a retry re-sends the full block with no
            // partial-acceptance / duplicate window. A per-record `try_send`
            // loop could enqueue some then fail mid-batch, stranding a prefix.
            // An empty batch reserves zero permits (no-op).
            let permits = match self.sender.try_reserve_many(records.len()) {
                Ok(permits) => permits,
                Err(mpsc::error::TrySendError::Full(())) => return Err(receiver_full()),
                Err(mpsc::error::TrySendError::Closed(())) => return Err(receiver_closed()),
            };

            // Capacity now held for every record -- enqueuing is infallible.
            for (permit, record) in permits.zip(records) {
                permit.send(self.message(record));
            }
        }

        #[cfg(feature = "metrics")]
        count_received(accepted, batch_bytes);

        Ok(Response::new(proto::BatchAck { accepted }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        Ok(Response::new(proto::HealthCheckResponse {
            status: proto::ServingStatus::Serving.into(),
        }))
    }
}

/// Loopback peers the gRPC client tests dial.
#[cfg(test)]
pub(crate) mod test_peers {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A TCP relay to an upstream server whose open connections can be frozen:
    /// a frozen connection stays open and relays nothing more, as a peer that
    /// hangs without closing its socket does. Connections accepted after a
    /// freeze relay as normal.
    pub(crate) struct FreezingProxy {
        pub(crate) addr: SocketAddr,
        generation: Arc<AtomicUsize>,
        accepts: Arc<AtomicUsize>,
    }

    impl FreezingProxy {
        pub(crate) async fn start(upstream: SocketAddr) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind proxy");
            let addr = listener.local_addr().expect("proxy addr");
            let generation = Arc::new(AtomicUsize::new(0));
            let accepts = Arc::new(AtomicUsize::new(0));
            tokio::spawn({
                let generation = Arc::clone(&generation);
                let accepts = Arc::clone(&accepts);
                async move {
                    while let Ok((client, _)) = listener.accept().await {
                        accepts.fetch_add(1, Ordering::SeqCst);
                        let Ok(server) = tokio::net::TcpStream::connect(upstream).await else {
                            continue;
                        };
                        let born = generation.load(Ordering::SeqCst);
                        let (client_rx, client_tx) = client.into_split();
                        let (server_rx, server_tx) = server.into_split();
                        tokio::spawn(relay(client_rx, server_tx, born, Arc::clone(&generation)));
                        tokio::spawn(relay(server_rx, client_tx, born, Arc::clone(&generation)));
                    }
                }
            });
            Self {
                addr,
                generation,
                accepts,
            }
        }

        /// Stop relaying on every connection open now, keeping them open.
        pub(crate) fn freeze(&self) {
            self.generation.fetch_add(1, Ordering::SeqCst);
        }

        /// Connections accepted so far.
        pub(crate) fn accepted(&self) -> usize {
            self.accepts.load(Ordering::SeqCst)
        }
    }

    /// Copy bytes one way until the connection is frozen, then hold both
    /// halves open and relay nothing more.
    async fn relay(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        born: usize,
        generation: Arc<AtomicUsize>,
    ) {
        let mut buf = vec![0_u8; 16 * 1024];
        loop {
            let n = match from.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            if generation.load(Ordering::SeqCst) > born {
                std::future::pending::<()>().await;
            }
            if to.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_token_display() {
        let token = GrpcToken::new(42);
        assert_eq!(format!("{token}"), "grpc:42");

        let token = GrpcToken::with_source(7, Arc::from("peer-1"));
        assert_eq!(format!("{token}"), "grpc:peer-1:7");
    }

    #[test]
    fn grpc_config_defaults() {
        let config = GrpcConfig::default();
        assert!(config.listen.is_none());
        assert!(config.endpoint.is_none());
        assert_eq!(config.recv_buffer_size, 10_000);
        assert_eq!(config.recv_timeout_ms, 100);
        assert_eq!(config.send_timeout_ms, 30_000);
        assert_eq!(config.max_message_size, 16 * 1024 * 1024);
        assert!(!config.compression);
        assert!(!config.tls_enabled);
        assert!(config.tls_ca_path.is_none());
    }

    #[test]
    fn grpc_client_tls_builds_with_private_ca_and_rejects_half_mtls() {
        use std::io::Write;
        let cert = rcgen::generate_simple_self_signed(vec!["grpc.test".to_string()]).unwrap();
        let mut ca = tempfile::NamedTempFile::new().unwrap();
        ca.write_all(cert.cert.pem().as_bytes()).unwrap();
        ca.flush().unwrap();

        // Private CA + SNI -> builds.
        let cfg = GrpcConfig {
            endpoint: Some("https://peer:6000".to_string()),
            tls_enabled: true,
            tls_ca_path: Some(ca.path().to_string_lossy().into_owned()),
            tls_domain: Some("grpc.test".to_string()),
            ..Default::default()
        };
        assert!(build_grpc_client_tls(&cfg).is_ok());

        // Half-configured mTLS (cert without key) -> error.
        let cfg = GrpcConfig {
            tls_enabled: true,
            tls_client_cert_path: Some(ca.path().to_string_lossy().into_owned()),
            tls_client_key_path: None,
            ..Default::default()
        };
        assert!(build_grpc_client_tls(&cfg).is_err());
    }

    #[test]
    fn grpc_config_server() {
        let config = GrpcConfig::server("0.0.0.0:6000");
        assert_eq!(config.listen.as_deref(), Some("0.0.0.0:6000"));
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn grpc_config_client() {
        let config = GrpcConfig::client("http://loader:6000");
        assert!(config.listen.is_none());
        assert_eq!(config.endpoint.as_deref(), Some("http://loader:6000"));
    }

    #[tokio::test]
    async fn send_batch_rejects_oversize_block() {
        // Over max_message_size, reject with a clear Fatal naming the limit
        // BEFORE the RPC -- tonic would otherwise return an opaque OutOfRange
        // (also Fatal) that can never succeed on retry. The size check fires
        // before any connection, so no server is needed.
        let config = GrpcConfig::client("http://127.0.0.1:1").with_max_message_size(64);
        let transport = GrpcTransport::new(&config).await.unwrap();
        let rec = Record {
            payload: bytes::Bytes::from(vec![b'x'; 256]),
            key: None,
            headers: Vec::new(),
            metadata: crate::transport::work_batch::RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        };
        match transport.send_batch(&[rec]).await {
            SendResult::Fatal(e) => assert!(
                e.to_string().contains("max_message_size"),
                "error should name the limit, got: {e}"
            ),
            other => panic!("expected Fatal for oversize block, got {other:?}"),
        }
    }

    #[test]
    fn grpc_config_with_compression() {
        let config = GrpcConfig::server("0.0.0.0:6000").with_compression();
        assert!(config.compression);
    }

    #[tokio::test]
    async fn grpc_transport_client_only() {
        // Client-only transport (lazy connection, no server)
        let config = GrpcConfig::client("http://localhost:16000");
        let transport = GrpcTransport::new(&config).await.unwrap();

        assert!(transport.client.is_some());
        assert!(transport.receiver.is_none());
        assert!(transport.is_healthy());
        assert_eq!(transport.name(), "grpc");

        // recv should error (no server)
        let result = transport.recv(10).await;
        assert!(result.is_err());

        // commit is always ok
        transport.commit(&[]).await.unwrap();
    }

    /// With a pressure governor pinned HIGH, the gRPC Push handler rejects
    /// with `Status::unavailable` (the gRPC analogue of 503). The default `new`
    /// (no governor) accepts as before.
    #[cfg(feature = "governor")]
    #[tokio::test]
    async fn grpc_pressure_high_rejects_unavailable() {
        use crate::governor::{Hysteresis, MemoryPressureSource, PressureSource, UnifiedPressure};
        use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

        // Pinned to the reservation counter so 950/1000 is the ratio, not the
        // host's own memory usage.
        let guard = Arc::new(MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1000,
                pressure_threshold: 0.80,
                ..Default::default()
            },
            UsageSource::Reservations,
        ));
        guard.add_bytes(950); // 95%
        let pressure = Arc::new(UnifiedPressure::new(
            vec![Arc::new(MemoryPressureSource::new(Arc::clone(&guard))) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("valid band"),
        ));
        assert!(pressure.should_hold(), "pinned-high governor must hold");

        // Server bound to the governor.
        let server_cfg = GrpcConfig::server("127.0.0.1:0");
        let server = GrpcTransport::with_pressure(&server_cfg, Some(Arc::clone(&pressure)))
            .await
            .unwrap();
        let addr = server.local_addr().expect("server mode binds a listener");

        // Client pushes -> rejected as backpressure (maps to Backpressured).
        let client_cfg = GrpcConfig::client(&format!("http://{addr}"));
        let client = GrpcTransport::new(&client_cfg).await.unwrap();
        let result = client
            .send("events", bytes::Bytes::from_static(b"{\"x\":1}"))
            .await;
        assert!(
            matches!(result, SendResult::Backpressured),
            "push under pressure must surface as backpressure, got {result:?}"
        );

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    #[tokio::test]
    async fn grpc_transport_server_only() {
        // Server-only transport (no client for sending)
        let config = GrpcConfig::server("127.0.0.1:0");
        let transport = GrpcTransport::new(&config).await.unwrap();

        assert!(transport.client.is_none());
        assert!(transport.receiver.is_some());
        assert!(transport.is_healthy());

        // send should error (no client)
        let result = transport
            .send("test", bytes::Bytes::from_static(b"payload"))
            .await;
        assert!(result.is_fatal());

        // Close
        transport.close().await.unwrap();
        assert!(!transport.is_healthy());
    }

    /// A loopback endpoint whose peer dies on every connection, optionally
    /// after reading the client's first bytes.
    async fn dying_endpoint(read_first: bool) -> std::net::SocketAddr {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                if read_first {
                    let mut buf = [0_u8; 1024];
                    let _ = stream.read(&mut buf).await;
                }
                drop(stream);
            }
        });
        addr
    }

    /// A server that dies mid-connection is down, not refusing the request:
    /// both shapes returned Fatal before, which ended the sender over an outage.
    #[tokio::test]
    async fn a_server_dying_mid_connection_is_backpressure_not_fatal() {
        for read_first in [false, true] {
            let addr = dying_endpoint(read_first).await;
            let client = GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
                .await
                .unwrap();
            let one = client.send("topic", bytes::Bytes::from_static(b"{}")).await;
            assert!(
                one.is_backpressured(),
                "send, read_first={read_first}: got {one:?}"
            );
            let rec = Record {
                payload: bytes::Bytes::from_static(b"{}"),
                key: None,
                headers: Vec::new(),
                metadata: crate::transport::work_batch::RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            };
            let batch = client.send_batch(&[rec]).await;
            assert!(
                batch.is_backpressured(),
                "send_batch, read_first={read_first}: got {batch:?}"
            );
        }
    }

    /// A status the server sent is its answer and keeps its code's meaning; a
    /// status tonic raised for the client's own connection is an outage.
    #[test]
    fn only_transient_codes_and_connection_failures_are_backpressure() {
        for status in [
            tonic::Status::unavailable("down"),
            tonic::Status::resource_exhausted("full"),
            tonic::Status::deadline_exceeded("slow"),
            tonic::Status::cancelled("Timeout expired"),
            tonic::Status::from_error(Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset by peer",
            ))),
        ] {
            assert!(
                failed_rpc_result(&status).is_backpressured(),
                "{status:?} should be retried"
            );
        }
        for status in [
            tonic::Status::invalid_argument("bad record"),
            tonic::Status::permission_denied("no"),
            tonic::Status::unknown("server bug"),
            tonic::Status::internal("server bug"),
        ] {
            assert!(
                failed_rpc_result(&status).is_fatal(),
                "{status:?} is the server's answer and should stay fatal"
            );
        }
    }

    fn json_record(payload: &'static [u8]) -> Record {
        Record {
            payload: bytes::Bytes::from_static(payload),
            key: None,
            headers: Vec::new(),
            metadata: crate::transport::work_batch::RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    /// A receive service whose every RPC outlives the server's timeout.
    struct SlowService;

    #[tonic::async_trait]
    impl proto::transport_server::Transport for SlowService {
        async fn push(
            &self,
            _request: Request<proto::PushRequest>,
        ) -> Result<Response<proto::PushResponse>, Status> {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(Response::new(proto::PushResponse { accepted: 1 }))
        }

        async fn route_batch(
            &self,
            request: Request<proto::Batch>,
        ) -> Result<Response<proto::BatchAck>, Status> {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let accepted = request.into_inner().records.len() as u64;
            Ok(Response::new(proto::BatchAck { accepted }))
        }

        async fn health_check(
            &self,
            _request: Request<proto::HealthCheckRequest>,
        ) -> Result<Response<proto::HealthCheckResponse>, Status> {
            Ok(Response::new(proto::HealthCheckResponse {
                status: proto::ServingStatus::Serving.into(),
            }))
        }
    }

    /// A tonic server that cuts every RPC at `timeout` and answers `Cancelled`.
    async fn server_cutting_rpcs_at(timeout: Duration) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .timeout(timeout)
                .add_service(proto::transport_server::TransportServer::new(SlowService))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        addr
    }

    /// A server that cuts an RPC at its deadline answers `Cancelled` with
    /// nothing queued; the sender retries it rather than stopping.
    #[tokio::test]
    async fn an_rpc_the_server_cuts_at_its_deadline_is_backpressure() {
        let addr = server_cutting_rpcs_at(Duration::from_millis(100)).await;
        let client = GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
            .await
            .unwrap();

        let one = client
            .send("events", bytes::Bytes::from_static(b"{}"))
            .await;
        assert!(one.is_backpressured(), "send: got {one:?}");
        let batch = client.send_batch(&[json_record(b"{}")]).await;
        assert!(batch.is_backpressured(), "send_batch: got {batch:?}");
    }

    /// A peer that answers the first RPC's headers and then resets its stream
    /// with `INTERNAL_ERROR`, as an h2 layer does when it drops a stream
    /// mid-response. The connection stays open.
    async fn stream_resetting_endpoint() -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                // Take the client's preface and first request before answering.
                let mut buf = [0_u8; 4096];
                while let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_millis(100), stream.read(&mut buf)).await
                {
                    if n == 0 {
                        break;
                    }
                }
                let content_type = b"application/grpc";
                let mut block = vec![0x88, 0x0f, 0x10];
                block.push(u8::try_from(content_type.len()).unwrap());
                block.extend_from_slice(content_type);
                let len = u32::try_from(block.len()).unwrap().to_be_bytes();

                // SETTINGS, SETTINGS ACK, HEADERS on stream 1 (:status 200,
                // END_HEADERS), then RST_STREAM(INTERNAL_ERROR) on stream 1.
                let mut frames = vec![0, 0, 0, 0x04, 0, 0, 0, 0, 0];
                frames.extend_from_slice(&[0, 0, 0, 0x04, 0x01, 0, 0, 0, 0]);
                frames.extend_from_slice(&[len[1], len[2], len[3], 0x01, 0x04, 0, 0, 0, 1]);
                frames.extend_from_slice(&block);
                frames.extend_from_slice(&[0, 0, 4, 0x03, 0, 0, 0, 0, 1, 0, 0, 0, 0x02]);
                let _ = stream.write_all(&frames).await;
                held.push(stream);
            }
        });
        addr
    }

    /// A stream the peer's h2 layer resets mid-response is an outage, not the
    /// server refusing the record: tonic reports it as `Internal` with the h2
    /// error as its source, and a status the server sent has no source.
    #[tokio::test]
    async fn a_stream_reset_mid_response_is_backpressure() {
        let addr = stream_resetting_endpoint().await;
        let mut raw = proto::transport_client::TransportClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let status = raw
            .push(push_request("events", bytes::Bytes::from_static(b"{}")))
            .await
            .expect_err("the peer resets the stream");
        assert_eq!(status.code(), tonic::Code::Internal, "{status:?}");
        assert!(
            std::error::Error::source(&status).is_some(),
            "a reset carries the h2 error as its source: {status:?}"
        );

        let client = GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
            .await
            .unwrap();
        let one = client
            .send("events", bytes::Bytes::from_static(b"{}"))
            .await;
        assert!(one.is_backpressured(), "send: got {one:?}");
    }

    /// A peer that stays connected but stops answering at the HTTP/2 level is
    /// found by an unanswered PING, and the connection is dropped, so the next
    /// send dials afresh rather than riding the dead one.
    #[tokio::test]
    async fn a_connection_whose_peer_stops_answering_is_dropped_and_the_next_send_redials() {
        use super::test_peers::FreezingProxy;

        let (server, _) = receiver(GrpcConfig::server("127.0.0.1:0")).await;
        let proxy = FreezingProxy::start(server.local_addr().unwrap()).await;
        let mut config = GrpcConfig::client(&format!("http://{}", proxy.addr));
        config.send_timeout_ms = 400;
        let client = GrpcTransport::new(&config).await.unwrap();

        let first = client.send("events", filled(8)).await;
        assert!(first.is_ok(), "before the freeze: got {first:?}");
        proxy.freeze();
        let stalled = client.send("events", filled(8)).await;
        assert!(
            stalled.is_backpressured(),
            "on the frozen connection: got {stalled:?}"
        );

        // PING after 400 ms of silence, dropped after 400 ms unanswered.
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let fresh = client.send("events", filled(8)).await;
        assert!(
            fresh.is_ok(),
            "the send after the dead connection was found: got {fresh:?}"
        );
        assert_eq!(
            proxy.accepted(),
            2,
            "the send after the freeze should dial a second connection"
        );
        assert_eq!(server.recv(10).await.unwrap().records.len(), 2);
    }

    const LIMIT: usize = 1024;

    fn filled(len: usize) -> bytes::Bytes {
        bytes::Bytes::from(vec![b'x'; len])
    }

    fn encoded_len(destination: &str, payload_len: usize) -> usize {
        prost::Message::encoded_len(&push_request(destination, filled(payload_len)))
    }

    /// The payload length whose `Push` body encodes to exactly `limit` bytes.
    fn payload_len_encoding_to(destination: &str, limit: usize) -> usize {
        let len = (0..=limit)
            .rev()
            .find(|&n| encoded_len(destination, n) == limit)
            .expect("some payload length encodes to the limit");
        assert_eq!(encoded_len(destination, len + 1), limit + 1);
        len
    }

    /// A scalo receive server on an OS-assigned loopback port, and its URI.
    async fn receiver(config: GrpcConfig) -> (GrpcTransport, String) {
        let server = GrpcTransport::new(&config).await.unwrap();
        let addr = server.local_addr().expect("server mode binds a listener");
        (server, format!("http://{addr}"))
    }

    #[tokio::test]
    async fn a_record_over_the_limit_is_dead_lettered_and_one_at_it_is_sent() {
        let (server, endpoint) =
            receiver(GrpcConfig::server("127.0.0.1:0").with_max_message_size(LIMIT)).await;
        let client =
            GrpcTransport::new(&GrpcConfig::client(&endpoint).with_max_message_size(LIMIT))
                .await
                .unwrap();
        let at = payload_len_encoding_to("events", LIMIT);

        let over = client.send("events", filled(at + 1)).await;
        assert!(
            over.is_filtered_dlq(),
            "one byte over the limit: got {over:?}"
        );
        let at_limit = client.send("events", filled(at)).await;
        assert!(at_limit.is_ok(), "exactly at the limit: got {at_limit:?}");

        let got = server.recv(10).await.unwrap().records;
        assert_eq!(
            got.len(),
            1,
            "only the at-limit record reaches the receiver"
        );
        assert_eq!(got[0].payload.len(), at);

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    /// Compressed, an over-limit record is small on the wire and the receiver
    /// refuses it while decompressing, with ResourceExhausted, which reads as
    /// busy.
    #[tokio::test]
    async fn a_compressed_record_over_the_limit_is_dead_lettered() {
        let (server, endpoint) = receiver(
            GrpcConfig::server("127.0.0.1:0")
                .with_max_message_size(LIMIT)
                .with_compression(),
        )
        .await;
        let client = GrpcTransport::new(
            &GrpcConfig::client(&endpoint)
                .with_max_message_size(LIMIT)
                .with_compression(),
        )
        .await
        .unwrap();
        let at = payload_len_encoding_to("events", LIMIT);

        let over = client.send("events", filled(4 * LIMIT)).await;
        assert!(
            over.is_filtered_dlq(),
            "compressible, over the limit: got {over:?}"
        );
        let at_limit = client.send("events", filled(at)).await;
        assert!(
            at_limit.is_ok(),
            "compressed, at the limit: got {at_limit:?}"
        );

        let got = server.recv(10).await.unwrap().records;
        assert_eq!(
            got.len(),
            1,
            "only the at-limit record reaches the receiver"
        );
        assert_eq!(got[0].payload.len(), at);

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    /// Bytes gzip cannot shrink (xorshift output), so compressing them grows
    /// the frame by gzip's own framing.
    fn incompressible(len: usize) -> bytes::Bytes {
        let mut state: u32 = 0x9e37_79b9;
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state.to_le_bytes()[0]
            })
            .collect();
        bytes.into()
    }

    /// An at-limit record gzip cannot shrink compresses past the limit, and
    /// is refused for its size rather than retried as an outage.
    #[tokio::test]
    async fn a_record_that_compresses_past_the_limit_is_dead_lettered() {
        let (server, endpoint) = receiver(
            GrpcConfig::server("127.0.0.1:0")
                .with_max_message_size(LIMIT)
                .with_compression(),
        )
        .await;
        let client = GrpcTransport::new(
            &GrpcConfig::client(&endpoint)
                .with_max_message_size(LIMIT)
                .with_compression(),
        )
        .await
        .unwrap();
        let at = payload_len_encoding_to("events", LIMIT);

        let grown = client.send("events", incompressible(at)).await;
        assert!(
            grown.is_filtered_dlq(),
            "compressed past the limit: got {grown:?}"
        );
        let fits = client.send("events", incompressible(LIMIT / 2)).await;
        assert!(fits.is_ok(), "compressed within the limit: got {fits:?}");

        let got = server.recv(10).await.unwrap().records;
        assert_eq!(got.len(), 1, "only the record within the limit arrives");
        assert_eq!(got[0].payload.len(), LIMIT / 2);

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    /// A block whose payloads fit but whose framing does not is refused before
    /// the RPC; compressed, the receiver would answer ResourceExhausted.
    #[tokio::test]
    async fn a_batch_over_the_limit_by_its_framing_alone_is_refused() {
        let (server, endpoint) = receiver(
            GrpcConfig::server("127.0.0.1:0")
                .with_max_message_size(LIMIT)
                .with_compression(),
        )
        .await;
        let client = GrpcTransport::new(
            &GrpcConfig::client(&endpoint)
                .with_max_message_size(LIMIT)
                .with_compression(),
        )
        .await
        .unwrap();
        let record = Record {
            payload: filled(LIMIT - 2),
            key: None,
            headers: Vec::new(),
            metadata: crate::transport::work_batch::RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        };

        match client.send_batch(&[record]).await {
            SendResult::Fatal(e) => assert!(
                e.to_string().contains("max_message_size"),
                "error should name the limit, got: {e}"
            ),
            other => panic!("expected Fatal for a block over the limit, got {other:?}"),
        }
        assert!(server.recv(10).await.unwrap().records.is_empty());

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    /// A receiver configured below the sender refuses what the sender allows.
    #[tokio::test]
    async fn a_receiver_with_a_lower_limit_dead_letters_the_record() {
        let (server, endpoint) =
            receiver(GrpcConfig::server("127.0.0.1:0").with_max_message_size(LIMIT / 2)).await;
        let client =
            GrpcTransport::new(&GrpcConfig::client(&endpoint).with_max_message_size(LIMIT))
                .await
                .unwrap();

        let refused = client.send("events", filled(LIMIT - 100)).await;
        assert!(
            refused.is_filtered_dlq(),
            "over the receiver's limit only: got {refused:?}"
        );
        let small = client.send("events", filled(16)).await;
        assert!(small.is_ok(), "the sender carries on: got {small:?}");

        client.close().await.unwrap();
        server.close().await.unwrap();
    }

    /// An outage stays backpressure at the limit; over it, the record is
    /// refused without reaching for the network.
    #[tokio::test]
    async fn a_down_receiver_is_backpressure_and_an_over_limit_record_is_still_dead_lettered() {
        let addr = dying_endpoint(false).await;
        let client = GrpcTransport::new(
            &GrpcConfig::client(&format!("http://{addr}")).with_max_message_size(LIMIT),
        )
        .await
        .unwrap();
        let at = payload_len_encoding_to("events", LIMIT);

        let at_limit = client.send("events", filled(at)).await;
        assert!(
            at_limit.is_backpressured(),
            "at the limit, receiver down: got {at_limit:?}"
        );
        let over = client.send("events", filled(at + 1)).await;
        assert!(
            over.is_filtered_dlq(),
            "over the limit, receiver down: got {over:?}"
        );
    }
}
