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
//! ## Held responses
//!
//! Once a caller that releases every token it takes arms the server, a push is
//! answered only when its records are released: `OK` when they were
//! delivered, dropped by policy or dead-lettered, `Unavailable` when they were
//! not, which senders retry. A response still held when its hold budget runs
//! out, or at the drain deadline after `close()`, is answered `Unavailable`
//! too. Unarmed, or with acknowledgements disabled, the server answers once
//! the records are queued for `recv`.
//!
//! ## Shutdown
//!
//! A receiving service shuts down in this order: `close()`, then `recv` until
//! it returns `TransportError::Closed`, then its final flush. `close()` refuses
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
pub(crate) mod pending;
pub mod proto;
pub mod token;

pub use config::GrpcConfig;
pub use pending::hold_budget;
pub use token::GrpcToken;

use super::ack::{
    AckControl, AcknowledgementsConfig, AcknowledgingReceiver, DeadLetterReason, SinkConfirmation,
};
use super::error::{TransportError, TransportResult};
use super::finalizer::DeliveryStatus;
use super::traits::{RecvBatch, TransportBase, TransportReceiver, TransportSender};
use super::types::{Message, PayloadFormat, SendResult};
use super::work_batch::{Record, WorkBatch};
use pending::{Held, Outcome, PendingRegistry};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

/// Trailer on a push an armed server held past its hold budget: its records
/// may still be delivered, so the sender's retry can duplicate them.
pub(crate) const HOLD_EXPIRED: &str = "scalo-hold-expired";

/// How long a sender refused at the held-byte ceiling is asked to wait, sent
/// as `grpc-retry-pushback-ms`.
const RETRY_PUSHBACK_MS: u64 = 1_000;

/// How long `close()` leaves held responses to be released before answering
/// the rest `Unavailable`, inside Kubernetes' default 30 s grace period.
const DEFAULT_DRAIN_DEADLINE: Duration = Duration::from_secs(20);

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
#[cfg(all(feature = "governor", feature = "transport-grpc-vector-compat"))]
#[derive(Clone)]
pub(crate) struct InboundGate(pub(crate) Arc<crate::governor::UnifiedPressure>);

/// Refuse a push with `Unavailable`, the gRPC analogue of HTTP 503, while the
/// pressure governor holds intake.
#[cfg(feature = "governor")]
pub(crate) fn shed_if_held(
    pressure: Option<&Arc<crate::governor::UnifiedPressure>>,
    pending: Option<&PendingRegistry>,
) -> Result<(), Status> {
    if pressure.is_some_and(|p| p.should_hold()) {
        #[cfg(feature = "metrics")]
        metrics::counter!(
            "transport_backpressured_total",
            "transport" => "grpc",
            "reason" => "pressure"
        )
        .increment(1);
        note_refusal(pending, "pressure");
        return Err(Status::unavailable("under pressure -- inbound held"));
    }
    Ok(())
}

/// Count a push refused before it was held, while responses are held.
pub(crate) fn note_refusal(pending: Option<&PendingRegistry>, reason: &'static str) {
    if let Some(pending) = pending.filter(|p| p.holding()) {
        pending.count_refused(reason);
    }
}

/// The deadline a sender set in its `grpc-timeout` header, read as tonic's
/// server reads it: at most 8 digits and a unit of H, M, S, m, u or n.
///
/// `None` when the header is absent or malformed. An app's own gRPC listener
/// that holds its answer passes this to [`hold_budget`] to answer before the
/// sender gives up.
///
/// ```
/// use std::time::Duration;
/// use scalo::transport::grpc::sender_deadline;
///
/// let mut metadata = tonic::metadata::MetadataMap::new();
/// assert_eq!(sender_deadline(&metadata), None);
/// metadata.insert("grpc-timeout", "1500m".parse()?);
/// assert_eq!(sender_deadline(&metadata), Some(Duration::from_millis(1500)));
/// # Ok::<(), tonic::metadata::errors::InvalidMetadataValue>(())
/// ```
#[must_use]
pub fn sender_deadline(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let value = metadata.get("grpc-timeout")?.to_str().ok()?;
    let split = value.len().checked_sub(1)?;
    let (digits, unit) = value.split_at(split);
    if digits.is_empty() || digits.len() > 8 {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3_600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

/// The answer to a push the held-byte ceiling has no room for:
/// `ResourceExhausted`, with the wait a sender's retry policy honours.
fn held_ceiling_full() -> Status {
    #[cfg(feature = "metrics")]
    metrics::counter!(
        "transport_backpressured_total",
        "transport" => "grpc",
        "reason" => "ceiling"
    )
    .increment(1);
    let mut status = Status::resource_exhausted("held responses at the held-byte ceiling");
    status.metadata_mut().insert(
        "grpc-retry-pushback-ms",
        tonic::metadata::MetadataValue::from(RETRY_PUSHBACK_MS),
    );
    status
}

/// Take the sequence range for a request of `len` records carrying `bytes`
/// and, while responses are held, reserve its bytes and hold it.
///
/// The request starts at `floor`: `Dropped` when some of its input was skipped
/// rather than queued.
pub(crate) fn admit(
    pending: Option<&Arc<PendingRegistry>>,
    sequence: &AtomicU64,
    sender_deadline: Option<Duration>,
    len: u64,
    bytes: u64,
    floor: DeliveryStatus,
) -> Result<(u64, Option<Held>), Status> {
    let Some(pending) = pending.filter(|p| p.holding()) else {
        return Ok((sequence.fetch_add(len, Ordering::Relaxed), None));
    };
    let Some(reservation) = pending.reserve(bytes) else {
        pending.count_refused("ceiling");
        return Err(held_ceiling_full());
    };
    let base = sequence.fetch_add(len, Ordering::Relaxed);
    let budget = pending.budget(sender_deadline);
    Ok((base, Some(reservation.hold(base, len, budget, floor))))
}

/// Answer a held request from its outcome: `OK` once every record was
/// delivered, dropped by policy or dead-lettered, `Unavailable` otherwise.
pub(crate) fn answer<T>(outcome: Outcome, response: T) -> Result<Response<T>, Status> {
    match outcome {
        Outcome::Released(
            DeliveryStatus::Delivered | DeliveryStatus::Dropped | DeliveryStatus::Rejected,
        ) => Ok(Response::new(response)),
        Outcome::Released(DeliveryStatus::Errored) => Err(Status::unavailable(
            "records not delivered downstream -- retry",
        )),
        Outcome::Expired => {
            let mut status =
                Status::unavailable("records not released within the hold budget -- retry");
            status
                .metadata_mut()
                .insert(HOLD_EXPIRED, tonic::metadata::MetadataValue::from(1_u32));
            Err(status)
        }
        Outcome::Shutdown => Err(Status::unavailable(
            "receiver shut down before its records were released -- retry",
        )),
    }
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

    /// Responses held until their records are released (None if client-only
    /// mode).
    pending: Option<Arc<PendingRegistry>>,

    /// The `acknowledgements` section this transport was built with.
    acknowledgements: AcknowledgementsConfig,

    /// How long `close()` leaves held responses to be released.
    drain_deadline: Duration,

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

/// Builds a [`GrpcTransport`] with the settings [`GrpcConfig`] does not
/// carry: acknowledgements, the pressure governor, and the limits on
/// responses held until their records are released.
///
/// An app that releases every token it takes, through the `BatchEngine`
/// pipeline builder or `SourceAck`, builds its receive server armed, so no
/// push is answered before it is delivered:
///
/// ```rust,ignore
/// let transport = GrpcTransport::builder(&config)
///     .acknowledgements(app_config.acknowledgements)
///     .armed(true)
///     .pressure(governor.pressure())
///     .memory_guard(guard)
///     .start()
///     .await?;
/// ```
#[must_use = "the transport starts only when `start` is awaited"]
pub struct GrpcTransportBuilder<'a> {
    config: &'a GrpcConfig,
    acknowledgements: AcknowledgementsConfig,
    armed: bool,
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
    #[cfg(feature = "memory")]
    memory_guard: Option<Arc<crate::memory::MemoryGuard>>,
    max_held_bytes: Option<u64>,
    max_hold: Duration,
    drain_deadline: Duration,
}

impl std::fmt::Debug for GrpcTransportBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTransportBuilder")
            .field("config", self.config)
            .field("acknowledgements", &self.acknowledgements)
            .field("armed", &self.armed)
            .field("max_held_bytes", &self.max_held_bytes)
            .field("max_hold", &self.max_hold)
            .field("drain_deadline", &self.drain_deadline)
            .finish_non_exhaustive()
    }
}

impl GrpcTransportBuilder<'_> {
    /// The `acknowledgements` section. Enabled (the default), the receive
    /// server holds each response until its records are released, once a
    /// caller arms it. Disabled, it answers once they are queued.
    pub fn acknowledgements(mut self, acknowledgements: AcknowledgementsConfig) -> Self {
        self.acknowledgements = acknowledgements;
        self
    }

    /// Arm the receive server before it listens, so the first push that can
    /// arrive, native or Vector-compat, is held until released.
    ///
    /// Set it only when the caller releases every token it takes, through the
    /// `BatchEngine` pipeline builder or `SourceAck`: a held push nobody
    /// releases is answered `Unavailable` at its hold budget. Unarmed (the
    /// default), pushes are answered once queued until a caller calls
    /// [`AckControl::arm`], which leaves every push before that call
    /// unprotected. A later `arm` on an armed server changes nothing.
    pub fn armed(mut self, armed: bool) -> Self {
        self.armed = armed;
        self
    }

    /// Shed pushes with `Unavailable` while `pressure` holds, and hold its
    /// latch while held responses near the held-byte ceiling (`governor`
    /// feature).
    #[cfg(feature = "governor")]
    pub fn pressure(mut self, pressure: Arc<crate::governor::UnifiedPressure>) -> Self {
        self.pressure = Some(pressure);
        self
    }

    /// Lease held bytes on `guard` from admission to answer, and size the
    /// default held-byte ceiling at a quarter of its limit (`memory` feature).
    #[cfg(feature = "memory")]
    pub fn memory_guard(mut self, guard: Arc<crate::memory::MemoryGuard>) -> Self {
        self.memory_guard = Some(guard);
        self
    }

    /// Refuse a push with `ResourceExhausted` once the responses held carry
    /// this many payload bytes. One push is always admitted while none is
    /// held. Default: a quarter of the memory guard's limit, else 256 MiB.
    pub fn max_held_bytes(mut self, bytes: u64) -> Self {
        self.max_held_bytes = Some(bytes);
        self
    }

    /// The longest a response is held, and the whole budget for a sender that
    /// sets no deadline. A sender's `grpc-timeout` shortens it to leave a
    /// margin of a tenth of the deadline, at least 1 s. Default 25 s.
    pub fn max_hold(mut self, max_hold: Duration) -> Self {
        self.max_hold = max_hold;
        self
    }

    /// How long [`close`](TransportBase::close) leaves held responses to be
    /// released before answering the rest `Unavailable`. Default 20 s, which
    /// the pod's termination grace period must cover.
    pub fn drain_deadline(mut self, drain_deadline: Duration) -> Self {
        self.drain_deadline = drain_deadline;
        self
    }

    /// Bind the receive server and build the client, as configured.
    ///
    /// # Errors
    ///
    /// Returns error if the listen address is invalid or the server fails to
    /// start.
    pub async fn start(self) -> TransportResult<GrpcTransport> {
        GrpcTransport::new_inner(self).await
    }
}

impl GrpcTransportBuilder<'_> {
    /// The registry a receive server holds responses in; `None` in client-only
    /// mode.
    fn hold_registry(&self) -> Option<Arc<PendingRegistry>> {
        self.config.listen.as_ref()?;
        #[cfg(feature = "memory")]
        let guard_limit = self.memory_guard.as_ref().map(|g| g.limit_bytes());
        #[cfg(not(feature = "memory"))]
        let guard_limit: Option<u64> = None;
        Some(Arc::new(PendingRegistry::new(pending::HoldSettings {
            enabled: self.acknowledgements.enabled,
            armed: self.armed,
            // A quarter of the memory guard's limit, else a fixed default.
            max_held_bytes: self.max_held_bytes.unwrap_or_else(|| {
                guard_limit.map_or(pending::DEFAULT_MAX_HELD_BYTES, |limit| (limit / 4).max(1))
            }),
            max_hold: self.max_hold,
            label: "grpc",
            #[cfg(feature = "memory")]
            guard: self.memory_guard.clone(),
        })))
    }
}

/// The lazily connected client for `endpoint`, with the configured TLS,
/// deadlines and compression.
fn build_client(
    config: &GrpcConfig,
    endpoint: &str,
) -> TransportResult<proto::transport_client::TransportClient<tonic::transport::Channel>> {
    let mut ep = tonic::transport::Channel::from_shared(endpoint.to_string())
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
    let mut client = proto::transport_client::TransportClient::new(channel)
        .max_decoding_message_size(config.max_message_size);

    if config.compression {
        client = client
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    }
    Ok(client)
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
        Self::builder(config).start().await
    }

    /// A builder for the settings `config` does not carry.
    pub fn builder(config: &GrpcConfig) -> GrpcTransportBuilder<'_> {
        GrpcTransportBuilder {
            config,
            acknowledgements: AcknowledgementsConfig::default(),
            armed: false,
            #[cfg(feature = "governor")]
            pressure: None,
            #[cfg(feature = "memory")]
            memory_guard: None,
            max_held_bytes: None,
            max_hold: pending::DEFAULT_MAX_HOLD,
            drain_deadline: DEFAULT_DRAIN_DEADLINE,
        }
    }

    /// Create a gRPC transport bound to a pressure governor (`governor`
    /// feature).
    ///
    /// Like [`new`](Self::new), but the receive server consults `pressure`
    /// before enqueuing each inbound Push / batch record: while
    /// [`UnifiedPressure::should_hold`](crate::governor::UnifiedPressure::should_hold)
    /// holds, the RPC is rejected with `Status::unavailable` (the gRPC analogue
    /// of HTTP 503). `None` is equivalent to [`new`](Self::new). The same as
    /// [`builder`](Self::builder) with [`pressure`](GrpcTransportBuilder::pressure).
    ///
    /// # Errors
    ///
    /// Same as [`new`](Self::new).
    #[cfg(feature = "governor")]
    pub async fn with_pressure(
        config: &GrpcConfig,
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

    async fn new_inner(options: GrpcTransportBuilder<'_>) -> TransportResult<Self> {
        let config = options.config;
        let pending = options.hold_registry();
        #[cfg(feature = "governor")]
        let pressure = options.pressure;
        let mut receiver = None;
        let mut shutdown_tx = None;
        let mut server_handle = None;
        let mut local_addr = None;
        let sequence = Arc::new(AtomicU64::new(0));
        let oversize = Arc::new(OversizeSlot::default());

        // Set up client (lazy connection -- doesn't fail until first RPC)
        let client = config
            .endpoint
            .as_ref()
            .map(|e| build_client(config, e))
            .transpose()?;

        // Set up server
        if let Some(listen) = &config.listen {
            let addr: std::net::SocketAddr = listen
                .parse()
                .map_err(|e| TransportError::Config(format!("invalid listen address: {e}")))?;

            let (tx, rx) = mpsc::channel(config.recv_buffer_size);
            let (sd_tx, sd_rx) = oneshot::channel();

            // Held bytes near the ceiling hold the same latch the governor's
            // other sources do. Nothing is held while unarmed or disabled.
            #[cfg(feature = "governor")]
            if let (Some(pressure), Some(pending)) = (&pressure, &pending) {
                pressure.attach_source(Arc::new(crate::governor::AckHeldSource::new(
                    pending.held_bytes(),
                    pending.max_held_bytes(),
                )));
            }

            // Native service
            let transport_svc = TransportServiceImpl {
                sender: tx.clone(),
                sequence: sequence.clone(),
                oversize: Arc::clone(&oversize),
                pending: pending.clone(),
                #[cfg(feature = "governor")]
                pressure: pressure.clone(),
            };

            let transport_server = proto::transport_server::TransportServer::new(transport_svc)
                .max_decoding_message_size(config.max_message_size)
                .max_encoding_message_size(config.max_message_size)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip);

            // Build server with optional Vector compat
            let mut builder = tonic::transport::Server::builder();

            #[cfg(feature = "transport-grpc-vector-compat")]
            let router = if config.vector_compat {
                let vector_svc = super::vector_compat::source::VectorCompatService::with_hold(
                    tx,
                    sequence.clone(),
                    pending.clone(),
                );
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

                builder
                    .add_service(transport_server)
                    .add_service(vector_server)
            } else {
                builder.add_service(transport_server)
            };

            #[cfg(not(feature = "transport-grpc-vector-compat"))]
            let router = builder.add_service(transport_server);

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
            pending,
            acknowledgements: options.acknowledgements,
            drain_deadline: options.drain_deadline,
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

    /// Apply an `acknowledgements` section after construction, as the factory
    /// does from `<key>.grpc.acknowledgements`.
    ///
    /// Takes effect for requests arriving from then on.
    #[must_use]
    pub fn with_acknowledgements(mut self, acknowledgements: AcknowledgementsConfig) -> Self {
        self.acknowledgements = acknowledgements;
        if let Some(pending) = &self.pending {
            pending.set_enabled(acknowledgements.enabled);
        }
        self
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

/// Why a failed send may still be delivered by the receiver, so the caller's
/// retry can duplicate it: a response held past its budget, or a deadline
/// that ran out after the request may have been accepted.
#[cfg(feature = "metrics")]
fn redelivery_reason(status: &tonic::Status) -> Option<&'static str> {
    if status.metadata().get(HOLD_EXPIRED).is_some() {
        return Some("hold_expired");
    }
    match status.code() {
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => Some("deadline"),
        _ => None,
    }
}

/// The send result for a failed RPC: backpressure while the server is down or
/// busy, so the caller retries rather than drops.
fn failed_rpc_result(status: &tonic::Status) -> SendResult {
    if downstream_unavailable(status) {
        #[cfg(feature = "metrics")]
        {
            metrics::counter!("transport_backpressured_total", "transport" => "grpc").increment(1);
            if let Some(reason) = redelivery_reason(status) {
                metrics::counter!(
                    "transport_redelivered_total",
                    "transport" => "grpc",
                    "reason" => reason
                )
                .increment(1);
            }
        }
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
    /// of its own, one batch at a time.
    ///
    /// ## Blocks over `max_message_size`
    ///
    /// A block that encodes past `max_message_size` goes as several
    /// `RouteBatch` RPCs, in order, each within the limit. Each is accepted
    /// whole or not at all, but a failure after the first leaves the earlier
    /// ones accepted, and the caller's retry of the whole block sends them
    /// again (at-least-once). A record over the limit on its own is left out
    /// and counted in `transport_message_too_large_total`, as
    /// [`send`](TransportSender::send) refuses it, and when every record is,
    /// the result is `FilteredDlq`. Either way it is dropped, and counted in
    /// `pipeline_dead_letters_dropped_total`. [`dead_letter_reason`] names such a record
    /// before the send, so a caller holding a source acknowledgement
    /// dead-letters it instead.
    ///
    /// [`dead_letter_reason`]: TransportSender::dead_letter_reason
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

        // A block over the ceiling goes as several requests in order, so none
        // is one the receiver's decoder refuses.
        let (batches, left_out) = batches_within(to_send, self.max_message_size);
        if batches.is_empty() {
            // Callers take `FilteredDlq` as handled, so these are dropped too.
            count_left_out(left_out);
            return SendResult::FilteredDlq;
        }
        for batch in batches {
            let result = self.send_route_batch(client, batch).await;
            if !result.is_ok() {
                return result;
            }
        }
        // Counted once the block is sent, so a retried block counts its drops once.
        count_left_out(left_out);
        SendResult::Ok
    }

    /// The receive server answers only once the records are queued, or, armed,
    /// once they are released.
    fn confirms_delivery(&self) -> SinkConfirmation {
        SinkConfirmation::Remote
    }

    /// A record over `max_message_size` on its own, as a field of a
    /// `RouteBatch`, or one an outbound `dlq` filter matches.
    fn dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        // Measured as `send_batch` measures it: encoded, as a field of a batch.
        let framed =
            prost::encoding::message::encoded_len(1, &batch::record_to_proto(record.clone()));
        if framed > self.max_message_size {
            return Some(DeadLetterReason::TooLarge {
                bytes: framed,
                limit: self.max_message_size,
            });
        }
        if self.filter_engine.has_outbound_filters()
            && matches!(
                self.filter_engine.apply_outbound(&record.payload),
                super::filter::FilterDisposition::Dlq
            )
        {
            return Some(DeadLetterReason::OutboundFilter);
        }
        None
    }
}

/// Split `records` into `Batch` bodies that each encode within `limit`, in
/// order, and count the records left out.
///
/// A record over the limit on its own is left out, as `send` leaves it out:
/// the same bytes are refused on every retry.
fn batches_within(records: Vec<Record>, limit: usize) -> (Vec<proto::Batch>, u64) {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_len = 0;
    let mut left_out = 0_u64;
    for record in records {
        let destination = record.key.clone();
        let record = batch::record_to_proto(record);
        // The record as a field of `Batch`: its tag, its length prefix, then
        // the record itself, so the parts sum to the body's encoded length.
        let framed = prost::encoding::message::encoded_len(1, &record);
        if framed > limit {
            #[cfg(feature = "metrics")]
            metrics::counter!("transport_message_too_large_total", "transport" => "grpc")
                .increment(1);
            tracing::warn!(
                destination = destination.as_deref().unwrap_or(""),
                encoded_len = framed,
                limit,
                "gRPC send_batch: a record over max_message_size on its own is left out of the \
                 block; the block's other records are sent and this one is dropped. Screen the \
                 block with dead_letter_reason to dead-letter it instead"
            );
            left_out += 1;
            continue;
        }
        if current_len + framed > limit {
            batches.push(proto::Batch {
                records: std::mem::take(&mut current),
            });
            current_len = 0;
        }
        current_len += framed;
        current.push(record);
    }
    if !current.is_empty() {
        batches.push(proto::Batch { records: current });
    }
    (batches, left_out)
}

/// Count records `send_batch` left out of a block: they were dropped, not
/// dead-lettered, so they count with the dead letters dropped.
fn count_left_out(left_out: u64) {
    #[cfg(feature = "metrics")]
    if left_out > 0 {
        metrics::counter!(
            "pipeline_dead_letters_dropped_total",
            "reason" => super::ack::TOO_LARGE
        )
        .increment(left_out);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = left_out;
}

impl GrpcTransport {
    /// Send one `Batch` body as a `RouteBatch` RPC under `send_timeout_ms`.
    async fn send_route_batch(
        &self,
        client: &proto::transport_client::TransportClient<tonic::transport::Channel>,
        proto_batch: proto::Batch,
    ) -> SendResult {
        let sent_count = proto_batch.records.len();
        #[cfg(feature = "metrics")]
        let payload_bytes: usize = proto_batch.records.iter().map(|r| r.payload.len()).sum();

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
    /// sender retries. Records already queued stay queued: call
    /// [`recv`](TransportReceiver::recv) until it returns
    /// [`TransportError::Closed`] or they are lost. Held responses are
    /// answered as their records are released, and any still held at the drain
    /// deadline are answered `Unavailable`. Open connections finish their
    /// in-flight RPCs on their own, and the listener is free when this
    /// returns. Waits for a `recv` in progress (at most `recv_timeout_ms`).
    /// Idempotent.
    async fn close(&self) -> TransportResult<()> {
        let first = !self.closed.swap(true, Ordering::AcqRel);
        self.healthy.store(false, Ordering::Relaxed);

        // The slot first: once recv sees the channel closed, nothing more can
        // land in the slot.
        self.oversize.close();
        if let Some(receiver) = &self.receiver {
            receiver.lock().await.close();
        }
        self.stop_server().await;

        if first && let Some(pending) = &self.pending {
            // Weak, so a transport dropped before the deadline is not kept.
            let pending = Arc::downgrade(pending);
            let drain_deadline = self.drain_deadline;
            tokio::spawn(async move {
                tokio::time::sleep(drain_deadline).await;
                if let Some(pending) = pending.upgrade() {
                    pending.shutdown();
                }
            });
        }
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

    /// The held-response controls of the receive server; `None` in
    /// client-only mode.
    fn ack_control(&self) -> Option<&dyn AckControl> {
        self.pending.as_deref().map(|p| p as &dyn AckControl)
    }

    /// Answer the held requests whose last records these are: `OK` for
    /// `Delivered`, `Dropped` and `Rejected`, `Unavailable` for `Errored`,
    /// which the sender retries. Tokens of requests answered at enqueue, or
    /// released already, are ignored.
    async fn release(
        &self,
        tokens: &[Self::Token],
        outcome: DeliveryStatus,
    ) -> TransportResult<()> {
        if let Some(pending) = &self.pending {
            pending.release(tokens.iter().map(|t| t.seq), outcome);
        }
        Ok(())
    }

    /// The earliest instant by which a held request among `tokens` must be
    /// answered: its hold budget from admission.
    fn hold_deadline(&self, tokens: &[Self::Token]) -> Option<std::time::Instant> {
        self.pending
            .as_ref()?
            .deadline(tokens.iter().map(|t| t.seq))
    }
}

impl AcknowledgingReceiver for GrpcTransport {
    fn acknowledgements(&self) -> AcknowledgementsConfig {
        self.acknowledgements
    }
}

impl Drop for GrpcTransport {
    fn drop(&mut self) {
        // Abort explicitly: dropping the handle would detach the serve task.
        if let Some(task) = self.server_task.get_mut().take() {
            task.abort();
        }
        // Nothing can release a held record once the transport is gone.
        if let Some(pending) = &self.pending {
            pending.shutdown();
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
    /// Responses held until their records are released.
    pending: Option<Arc<PendingRegistry>>,
    /// Optional pressure governor (`governor` feature). `None` -> handlers
    /// never consult it. `Some` rejects an inbound Push / batch record with
    /// `Status::unavailable` while `UnifiedPressure::should_hold` holds --
    /// pressure-driven shedding on top of the channel-full rejection.
    #[cfg(feature = "governor")]
    pressure: Option<Arc<crate::governor::UnifiedPressure>>,
}

impl TransportServiceImpl {
    /// Wrap one `RouteBatch` record for `recv` under sequence number `seq`.
    fn message(record: Record, seq: u64) -> Message<GrpcToken> {
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
            token: GrpcToken::new(seq),
            timestamp_ms: record.metadata.timestamp_ms,
            format,
        }
    }

    /// Count a refusal of a push while responses are held: `closed` for a
    /// closed receiver, `full` for a receive queue with no room.
    fn refused(&self, status: Status) -> Status {
        match status.code() {
            tonic::Code::Unavailable => note_refusal(self.pending.as_deref(), "closed"),
            tonic::Code::ResourceExhausted => note_refusal(self.pending.as_deref(), "full"),
            _ => {}
        }
        status
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
        shed_if_held(self.pressure.as_ref(), self.pending.as_deref())?;

        let deadline = sender_deadline(request.metadata());
        let req = request.into_inner();
        let (seq, held) = admit(
            self.pending.as_ref(),
            &self.sequence,
            deadline,
            1,
            req.payload.len() as u64,
            DeliveryStatus::Delivered,
        )?;

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
            }
            Err(mpsc::error::TrySendError::Full(_)) => return Err(self.refused(receiver_full())),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(self.refused(receiver_closed()));
            }
        }

        let response = proto::PushResponse { accepted: 1 };
        match held {
            Some(mut held) => {
                held.queued();
                answer(held.outcome().await, response)
            }
            None => Ok(Response::new(response)),
        }
    }

    async fn route_batch(
        &self,
        request: Request<proto::Batch>,
    ) -> Result<Response<proto::BatchAck>, Status> {
        // Shed the whole batch while the governor holds intake.
        #[cfg(feature = "governor")]
        shed_if_held(self.pressure.as_ref(), self.pending.as_deref())?;

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

        let deadline = sender_deadline(request.metadata());
        let proto_batch = request.into_inner();

        // Decode proto Batch -> scalo Records (payloads zero-copy `Bytes`,
        // codec NOT invoked). recv() delivers them unchanged, from the channel
        // the single-message Push path uses or from the oversize slot.
        let records = batch::proto_batch_to_records(proto_batch);
        let count = records.len();
        let accepted = count as u64;
        // Nothing to hold or queue, but a closed receiver still says so.
        if records.is_empty() {
            if self.sender.is_closed() {
                return Err(self.refused(receiver_closed()));
            }
            return Ok(Response::new(proto::BatchAck { accepted }));
        }
        // Sum raw wire bytes BEFORE the records move into the channel below.
        let batch_bytes: usize = records.iter().map(|r| r.payload.len()).sum();
        // One contiguous sequence range per request, so a held request is
        // found from any of its tokens.
        let (base, held) = admit(
            self.pending.as_ref(),
            &self.sequence,
            deadline,
            accepted,
            batch_bytes as u64,
            DeliveryStatus::Delivered,
        )?;
        let messages = records
            .into_iter()
            .zip(base..)
            .map(|(record, seq)| Self::message(record, seq));

        // A batch the channel can never hold at once waits whole in its own
        // slot, admitted in one step or refused, like one that fits.
        if count > self.sender.max_capacity() {
            self.oversize
                .admit(messages.collect())
                .map_err(|status| self.refused(status))?;
        } else {
            // ATOMICITY: reserve channel capacity for the WHOLE batch via
            // `try_reserve_many` BEFORE enqueuing ANY record. Cannot fit ->
            // reject all-or-nothing, so a retry re-sends the full block with no
            // partial-acceptance / duplicate window. A per-record `try_send`
            // loop could enqueue some then fail mid-batch, stranding a prefix.
            let permits = match self.sender.try_reserve_many(count) {
                Ok(permits) => permits,
                Err(mpsc::error::TrySendError::Full(())) => {
                    return Err(self.refused(receiver_full()));
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    return Err(self.refused(receiver_closed()));
                }
            };

            // Capacity now held for every record -- enqueuing is infallible.
            for (permit, message) in permits.zip(messages) {
                permit.send(message);
            }
        }

        #[cfg(feature = "metrics")]
        count_received(accepted, batch_bytes);

        let response = proto::BatchAck { accepted };
        match held {
            Some(mut held) => {
                held.queued();
                answer(held.outcome().await, response)
            }
            None => Ok(Response::new(response)),
        }
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
    async fn send_batch_leaves_out_a_record_over_the_limit_on_its_own() {
        // The same bytes are refused on every retry, so the record is left
        // out before any connection, as `send` refuses it, so no server runs.
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
        let result = transport.send_batch(&[rec]).await;
        assert!(
            result.is_filtered_dlq(),
            "the only record, over the limit: got {result:?}"
        );
    }

    /// The record left out counts as too large and as a dropped dead letter.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn send_batch_counts_the_record_it_leaves_out() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = ::metrics::set_default_local_recorder(&recorder);

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
        assert!(transport.send_batch(&[rec]).await.is_filtered_dlq());

        let rendered = handle.render();
        for series in [
            r#"transport_message_too_large_total{transport="grpc"} 1"#,
            r#"pipeline_dead_letters_dropped_total{reason="too_large"} 1"#,
        ] {
            assert!(
                rendered.lines().any(|line| line == series),
                "{series} missing:\n{rendered}"
            );
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

    /// A record whose payload fits but whose framing does not is left out
    /// before the RPC. Compressed, the receiver would answer ResourceExhausted.
    #[tokio::test]
    async fn a_record_over_the_limit_by_its_framing_alone_is_left_out() {
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

        let result = client.send_batch(&[record]).await;
        assert!(result.is_filtered_dlq(), "got {result:?}");
        assert_eq!(
            server.recv(10).await.unwrap().records,
            [] as [crate::transport::work_batch::Record; 0]
        );

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

    /// An armed receive server holding at most `max_held_bytes`, and its URI.
    async fn armed(max_held_bytes: u64, drain_deadline: Duration) -> (GrpcTransport, String) {
        let config = GrpcConfig::server("127.0.0.1:0");
        let server = GrpcTransport::builder(&config)
            .armed(true)
            .max_held_bytes(max_held_bytes)
            .drain_deadline(drain_deadline)
            .start()
            .await
            .unwrap();
        let uri = format!("http://{}", server.local_addr().unwrap());
        (server, uri)
    }

    /// Release every token of `batch` with `status`, as the engine does.
    fn release(server: &GrpcTransport, batch: &WorkBatch<GrpcToken>, status: DeliveryStatus) {
        server
            .pending
            .as_ref()
            .unwrap()
            .release(batch.commit_tokens.iter().map(|t| t.seq), status);
    }

    /// Receive until `n` records have arrived.
    async fn recv_n(server: &GrpcTransport, n: usize) -> WorkBatch<GrpcToken> {
        let mut all = server.recv(n).await.unwrap();
        while all.records.len() < n {
            let more = server.recv(n - all.records.len()).await.unwrap();
            all.records.extend(more.records);
            all.commit_tokens.extend(more.commit_tokens);
        }
        all
    }

    #[tokio::test]
    async fn a_held_block_takes_one_contiguous_range_and_waits_for_release() {
        let (server, uri) = armed(1 << 20, DEFAULT_DRAIN_DEADLINE).await;
        let client = Arc::new(GrpcTransport::new(&GrpcConfig::client(&uri)).await.unwrap());
        let sending = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                client
                    .send_batch(&[json_record(b"{}"), json_record(b"{}"), json_record(b"{}")])
                    .await
            }
        });

        let batch = recv_n(&server, 3).await;
        let seqs: Vec<u64> = batch.commit_tokens.iter().map(|t| t.seq).collect();
        assert_eq!(seqs, vec![0, 1, 2], "one contiguous range per request");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!sending.is_finished(), "answered before release");

        release(&server, &batch, DeliveryStatus::Delivered);
        let result = tokio::time::timeout(Duration::from_secs(2), sending)
            .await
            .expect("answered once released")
            .unwrap();
        assert!(result.is_ok(), "released Delivered: got {result:?}");
        assert_eq!(server.pending.as_ref().unwrap().snapshot().count, 0);
    }

    #[tokio::test]
    async fn the_held_byte_ceiling_answers_with_a_retry_pushback() {
        let (server, uri) = armed(1 << 10, DEFAULT_DRAIN_DEADLINE).await;
        let client = Arc::new(GrpcTransport::new(&GrpcConfig::client(&uri)).await.unwrap());
        let first = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.send("events", filled(1 << 10)).await }
        });
        let held = recv_n(&server, 1).await;

        let mut raw = proto::transport_client::TransportClient::connect(uri)
            .await
            .unwrap();
        let status = raw
            .push(push_request("events", filled(8)))
            .await
            .expect_err("past the ceiling");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status:?}");
        assert_eq!(
            status
                .metadata()
                .get("grpc-retry-pushback-ms")
                .and_then(|v| v.to_str().ok()),
            Some("1000")
        );

        release(&server, &held, DeliveryStatus::Delivered);
        assert!(first.await.unwrap().is_ok());
    }

    /// The parts `batches_within` adds up are the body's own encoded length,
    /// so every split request fits the limit it was cut to.
    #[test]
    fn split_batches_each_encode_within_the_limit() {
        let records: Vec<Record> = (0..40).map(|_| json_record(b"0123456789abcdef")).collect();
        let whole = prost::Message::encoded_len(&batch::records_to_proto(records.clone()));
        let limit = whole / 3;
        let (batches, left_out) = batches_within(records, limit);
        assert_eq!(left_out, 0);
        assert!(batches.len() >= 3, "{} batches", batches.len());
        let total: usize = batches.iter().map(|b| b.records.len()).sum();
        assert_eq!(total, 40, "no record lost in the split");
        for b in &batches {
            assert!(prost::Message::encoded_len(b) <= limit);
        }
        let framed: usize = batches[0]
            .records
            .iter()
            .map(|r| prost::encoding::message::encoded_len(1, r))
            .sum();
        assert_eq!(framed, prost::Message::encoded_len(&batches[0]));
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn a_hold_expiry_or_a_deadline_is_a_possible_redelivery() {
        let mut expired = tonic::Status::unavailable("held");
        expired
            .metadata_mut()
            .insert(HOLD_EXPIRED, tonic::metadata::MetadataValue::from(1_u32));
        assert_eq!(redelivery_reason(&expired), Some("hold_expired"));
        assert_eq!(
            redelivery_reason(&tonic::Status::deadline_exceeded("slow")),
            Some("deadline")
        );
        assert_eq!(
            redelivery_reason(&tonic::Status::cancelled("Timeout expired")),
            Some("deadline")
        );
        assert_eq!(redelivery_reason(&tonic::Status::unavailable("down")), None);
        assert_eq!(
            redelivery_reason(&tonic::Status::resource_exhausted("full")),
            None
        );
    }

    #[test]
    fn the_sender_deadline_is_read_as_tonic_reads_it() {
        let deadline = |value: &str| {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert("grpc-timeout", value.parse().unwrap());
            sender_deadline(&metadata)
        };
        assert_eq!(deadline("2S"), Some(Duration::from_secs(2)));
        assert_eq!(deadline("1500m"), Some(Duration::from_millis(1_500)));
        assert_eq!(deadline("1H"), Some(Duration::from_secs(3_600)));
        assert_eq!(deadline("S"), None);
        assert_eq!(deadline("123456789S"), None, "at most 8 digits");
        assert_eq!(deadline("5x"), None);
        assert_eq!(sender_deadline(&tonic::metadata::MetadataMap::new()), None);
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
