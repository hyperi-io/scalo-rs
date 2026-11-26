// Project:   scalo
// File:      src/transport/syslog/mod.rs
// Purpose:   Syslog sender: RFC 5424 and 3164 over TCP, TLS, UDP and RELP
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sends records as syslog messages. [`SyslogReceiver`] receives them over
//! TCP, TLS and UDP, built from the `listen` section.
//!
//! Each record's payload is the MSG, after an RFC 5424 or RFC 3164 header
//! built from config, or as it is when `passthrough` is set and it already
//! starts with a `<PRI>`. A record's key is its RFC 5424 MSGID. With
//! `hostname_header` or `msg_id_header` set, that record header's value is
//! the HOSTNAME or MSGID instead.
//!
//! | Protocol | Wire | Guarantee |
//! |---|---|---|
//! | `tcp` | octet counting or LF lines (RFC 6587) | `written` |
//! | `tls` | the same inside TLS (RFC 5425) | `written` |
//! | `udp` | one message per datagram (RFC 5426) | `fire_and_forget` |
//! | `relp` | RELP frames, each answered, TLS when `tls` is set | `acknowledged` |
//!
//! A connection the receiver closed while idle is replaced before the next
//! batch, with nothing lost. A batch whose write or answers break off returns
//! `Backpressured`, so the caller sends it again: never `Ok`. RELP first
//! reconnects once within the call and sends again only the messages not yet
//! answered. TCP and TLS have no answer, so a receiver that fails after the
//! write loses what it held. `proxy` sends `tcp`, `tls` and `relp` through an
//! HTTP CONNECT tunnel. A [`ConnectResolver`] given to the
//! [builder](SyslogTransport::builder) picks the address each connection
//! dials. The guide is `docs/transport/syslog.md`.

mod config;
mod format;
mod receive;
pub(crate) mod relp;
mod stream;

#[cfg(test)]
mod tests;

pub use config::{Facility, Severity, SyslogConfig, SyslogFormat, SyslogFraming, SyslogProtocol};
pub use receive::{IpFilterConfig, IpFilterMode, SyslogListenConfig, SyslogReceiver, SyslogToken};

use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls_pki_types::ServerName;
use tokio::io::AsyncWriteExt;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use self::format::Formatter;
use self::relp::{Answer, RelpError, RelpSession};
use self::stream::{ConnectError, Connector, Idle, Stream, check_idle};
use super::ack::DeadLetterReason;
use super::core::{Lifecycle, Meter};
use super::delivery::DeliveryGuarantee;
use super::error::{TransportError, TransportResult};
use super::filter::{TransportFilterEngine, TransportFilterTierConfig};
use super::resolver::ConnectResolver;
use super::traits::{TransportBase, TransportSender};
use super::types::{BlockOutcome, PayloadFormat, SendResult};
use super::work_batch::{Record, RecordMeta};
use crate::framing::{Framing, Lf, OctetCounting};

/// The open connection, of the protocol's kind.
enum Session {
    Stream(Stream),
    Relp(RelpSession),
    Udp(UdpSocket),
}

impl Session {
    /// End the session: a RELP session with its `close` command, a stream
    /// shut down.
    async fn end(self) {
        match self {
            Self::Relp(session) => session.close().await,
            Self::Stream(mut stream) => {
                if let Err(e) = stream.shutdown().await {
                    tracing::debug!(error = %e, "syslog: stream shutdown");
                }
            }
            Self::Udp(_) => {}
        }
    }
}

/// Buffers reused from batch to batch, and the connection.
#[derive(Default)]
struct State {
    session: Option<Session>,
    /// The resolver's answer `session` was opened to.
    dialled: Option<SocketAddr>,
    /// The batch's messages, back to back.
    messages: Vec<u8>,
    /// Where each message sits in `messages`.
    ranges: Vec<Range<usize>>,
    /// The framed batch, for TCP and TLS.
    wire: Vec<u8>,
}

/// Builds a [`SyslogTransport`] with what [`SyslogConfig`] does not carry: a
/// connect resolver.
///
/// ```rust,ignore
/// let transport = SyslogTransport::builder(&config)
///     .resolver(Arc::new(proven_routes))
///     .build()?;
/// ```
#[must_use = "the transport is made only when `build` is called"]
pub struct SyslogTransportBuilder<'a> {
    config: &'a SyslogConfig,
    resolver: Option<Arc<dyn ConnectResolver>>,
}

impl std::fmt::Debug for SyslogTransportBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyslogTransportBuilder")
            .field("config", self.config)
            .field("resolver", &self.resolver.is_some())
            .finish()
    }
}

impl SyslogTransportBuilder<'_> {
    /// Dial where `resolver` says, asked before every batch. The connection
    /// is kept while the answer holds, and replaced at the next batch when it
    /// changes. TLS still checks the address's host name. A `proxy` other than
    /// `none` fails [`build`](Self::build). See [`ConnectResolver`].
    pub fn resolver(mut self, resolver: Arc<dyn ConnectResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Build the sender.
    ///
    /// # Errors
    ///
    /// As [`SyslogTransport::new`], and a resolver beside a proxy.
    pub fn build(self) -> TransportResult<SyslogTransport> {
        SyslogTransport::new_inner(self.config, self.resolver)
    }
}

/// Sends records to one syslog receiver. Build it from a [`SyslogConfig`].
pub struct SyslogTransport {
    address: String,
    protocol: SyslogProtocol,
    framing: SyslogFraming,
    formatter: Formatter,
    limit: usize,
    send_timeout: Duration,
    relp_window: usize,
    connector: Connector,
    filter_engine: TransportFilterEngine,
    state: Mutex<State>,
    /// Closed, or `refusals` refused: either fails readiness and the
    /// `transport:syslog` health entry.
    lifecycle: Lifecycle,
    /// Whether the proxy is refusing CONNECT, which lasts until a send goes through.
    refusals: crate::transport::proxy::Refusals,
}

impl SyslogTransport {
    /// A sender for `config`. Reads every TLS file now and connects on the
    /// first send or [`healthcheck`](TransportBase::healthcheck).
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for an address that is not `host:port`, a
    /// setting out of range, a TLS file that cannot be read, or an outbound
    /// filter that does not compile.
    pub fn new(config: &SyslogConfig) -> TransportResult<Self> {
        Self::builder(config).build()
    }

    /// A builder for the settings `config` does not carry.
    pub fn builder(config: &SyslogConfig) -> SyslogTransportBuilder<'_> {
        SyslogTransportBuilder {
            config,
            resolver: None,
        }
    }

    fn new_inner(
        config: &SyslogConfig,
        resolver: Option<Arc<dyn ConnectResolver>>,
    ) -> TransportResult<Self> {
        config.validate()?;
        if resolver.is_some() {
            super::resolver::refuse_proxy(&config.proxy, "syslog")?;
        }
        let tls = if config.uses_tls() {
            let section = config.tls.clone().unwrap_or_default();
            let host = config::host_of(&config.address).unwrap_or_default();
            let name = section.server_name.as_deref().unwrap_or(host).to_string();
            let server_name = ServerName::try_from(name.clone()).map_err(|e| {
                TransportError::Config(format!("syslog: TLS server name `{name}`: {e}"))
            })?;
            let fail = |e: crate::tls::TlsError| TransportError::Config(format!("syslog: {e}"));
            let client = section.client_tls().map_err(fail)?;
            let settings = client.profile_settings().map_err(fail)?;
            Some(stream::ConnectorTls {
                config: crate::tls::build_client_tls(&client).map_err(fail)?,
                server_name,
                settings,
            })
        } else {
            None
        };
        let filter_engine = TransportFilterEngine::new(
            &[],
            &config.filters_out,
            &TransportFilterTierConfig::from_cascade()?,
        )?;
        let refusals = crate::transport::proxy::Refusals::default();
        let escape_lf = matches!(config.protocol, SyslogProtocol::Tcp | SyslogProtocol::Tls)
            && config.framing == SyslogFraming::Lf;
        let has_resolver = resolver.is_some();
        let transport = Self {
            address: config.address.clone(),
            protocol: config.protocol,
            framing: config.framing,
            formatter: Formatter::new(config, escape_lf, jiff::tz::TimeZone::system()),
            limit: config.message_limit(),
            send_timeout: Duration::from_millis(config.send_timeout_ms),
            relp_window: config.relp_window,
            connector: Connector::new(
                (
                    config::host_of(&config.address)
                        .unwrap_or_default()
                        .to_string(),
                    config::port_of(&config.address).unwrap_or_default(),
                ),
                Duration::from_millis(config.connect_timeout_ms),
                tls,
                config.proxy.dialer()?,
                resolver,
            ),
            filter_engine,
            state: Mutex::new(State::default()),
            lifecycle: {
                let refusals = refusals.clone();
                Lifecycle::new::<Self>(move || !refusals.is_refused())
            },
            refusals,
        };
        tracing::info!(
            address = %config.address,
            protocol = config.protocol.as_str(),
            format = ?config.format,
            framing = ?config.framing,
            tls = config.uses_tls(),
            proxy = config.proxy.kind(),
            resolver = has_resolver,
            hostname_header = config.hostname_header.as_deref(),
            msg_id_header = config.msg_id_header.as_deref(),
            trust = config.tls.as_ref().map(|t| t.trust.as_str()),
            guarantee = %transport.delivery_guarantee(),
            max_message_bytes = transport.limit,
            "syslog sender built"
        );
        Ok(transport)
    }

    /// Load the config from the cascade under `transport.syslog` and build.
    ///
    /// # Errors
    ///
    /// As [`SyslogConfig::from_cascade`] and [`new`](Self::new).
    pub fn from_cascade() -> TransportResult<Self> {
        Self::new(&SyslogConfig::from_cascade()?)
    }

    /// Format the records that pass the filters into `state.messages`, one
    /// range each, and fold the ones handled without sending into `outcome`.
    fn format_batch(&self, records: &[Record], state: &mut State, outcome: &mut BlockOutcome) {
        state.messages.clear();
        state.ranges.clear();
        for record in records {
            if let Some(settled) = self.filter_engine.outbound_one(&record.payload) {
                let _ = outcome.add(settled);
                continue;
            }
            let begin = state.messages.len();
            self.formatter.write(record, &mut state.messages);
            let len = state.messages.len() - begin;
            if len > self.limit {
                if self.protocol == SyslogProtocol::Udp {
                    let cut = utf8_floor(&state.messages[begin..], self.limit);
                    state.messages.truncate(begin + cut);
                    metrics::counter!("transport_truncated_total", "transport" => "syslog")
                        .increment(1);
                    tracing::debug!(bytes = len, limit = self.limit, "syslog: datagram cut");
                } else {
                    state.messages.truncate(begin);
                    Self::count_too_large();
                    tracing::warn!(
                        bytes = len,
                        limit = self.limit,
                        "syslog: message over max_message_bytes dead-lettered"
                    );
                    let _ = outcome.add(SendResult::FilteredDlq);
                    continue;
                }
            }
            tracing::trace!(
                bytes = state.messages.len() - begin,
                "syslog: message formatted"
            );
            state.ranges.push(begin..state.messages.len());
        }
    }

    /// Send the formatted messages over the open session, opening one first
    /// when there is none, the receiver closed it, or the resolver's answer
    /// moved off it.
    async fn deliver(&self, state: &mut State) -> SendResult {
        let State {
            session,
            dialled,
            messages,
            ranges,
            wire,
        } = state;
        let messages: Vec<&[u8]> = ranges.iter().map(|r| &messages[r.clone()]).collect();
        // Taken out for the send and put back only after it, so a send dropped
        // at its deadline drops the connection with it, never one cut mid-frame.
        let current = session.take();
        let target = self.connector.target();
        let current = match (current, &target) {
            (Some(kept), Ok(now)) if *now == *dialled => Some(kept),
            (Some(moved), _) => {
                tracing::info!(
                    address = %self.address,
                    from = ?dialled,
                    to = ?target.as_ref().ok().copied().flatten(),
                    "syslog: the resolver's answer changed, reconnecting"
                );
                moved.end().await;
                None
            }
            (None, _) => None,
        };
        let target = match target {
            Ok(target) => target,
            Err(e) => return self.connect_failed(&e),
        };
        let (result, kept) = match self.protocol {
            SyslogProtocol::Tcp | SyslogProtocol::Tls => {
                self.deliver_stream(current, &messages, wire, target).await
            }
            SyslogProtocol::Relp => self.deliver_relp(current, &messages, target).await,
            SyslogProtocol::Udp => self.deliver_udp(current, &messages, target).await,
        };
        *session = kept;
        *dialled = target;
        if result.is_ok() {
            self.refusals.clear("syslog");
        }
        result
    }

    async fn deliver_stream(
        &self,
        current: Option<Session>,
        messages: &[&[u8]],
        wire: &mut Vec<u8>,
        target: Option<SocketAddr>,
    ) -> (SendResult, Option<Session>) {
        wire.clear();
        let _ = match self.framing {
            SyslogFraming::OctetCounting => OctetCounting::default().encode(messages, wire),
            SyslogFraming::Lf => Lf.encode(messages, wire),
        };
        let open = match current {
            Some(Session::Stream(mut stream)) => match check_idle(&mut stream).await {
                Idle::Closed => {
                    tracing::info!(
                        address = %self.address,
                        "syslog: receiver closed the idle connection, reconnecting"
                    );
                    None
                }
                Idle::Open | Idle::Data => Some(stream),
            },
            _ => None,
        };
        let mut stream = match open {
            Some(stream) => stream,
            None => match self.connect(target).await {
                Ok(stream) => stream,
                Err(e) => return (self.connect_failed(&e), None),
            },
        };
        let written = async {
            stream.write_all(wire).await?;
            stream.flush().await
        };
        match written.await {
            Ok(()) => (SendResult::Ok, Some(Session::Stream(stream))),
            Err(e) => {
                tracing::warn!(
                    address = %self.address,
                    error = %e,
                    records = messages.len(),
                    "syslog: write broke off, the batch is sent again"
                );
                Self::count_send_error();
                (SendResult::Backpressured, None)
            }
        }
    }

    async fn deliver_relp(
        &self,
        current: Option<Session>,
        messages: &[&[u8]],
        target: Option<SocketAddr>,
    ) -> (SendResult, Option<Session>) {
        let mut answers: Vec<Option<Answer>> = vec![None; messages.len()];
        let mut session = match current {
            Some(Session::Relp(mut session)) => {
                if check_idle(session.stream_mut()).await == Idle::Open {
                    Some(session)
                } else {
                    tracing::info!(
                        address = %self.address,
                        "syslog: RELP receiver closed the idle session, reconnecting"
                    );
                    None
                }
            }
            _ => None,
        };
        let mut reconnected = false;
        loop {
            let mut open = match session.take() {
                Some(open) => open,
                None => match self.open_relp(target).await {
                    Ok(open) => open,
                    Err(result) => return (result, None),
                },
            };
            match open.send(messages, &mut answers, self.relp_window).await {
                Ok(()) => {
                    session = Some(open);
                    break;
                }
                Err(RelpError::Io(e)) if !reconnected => {
                    let unanswered = answers.iter().filter(|a| a.is_none()).count();
                    tracing::warn!(
                        address = %self.address,
                        error = %e,
                        unanswered,
                        "syslog: RELP session lost mid-batch, resending the unanswered messages"
                    );
                    metrics::counter!(
                        "transport_redelivered_total",
                        "transport" => "syslog",
                        "reason" => "reconnect"
                    )
                    .increment(1);
                    reconnected = true;
                }
                Err(e) => {
                    tracing::warn!(
                        address = %self.address,
                        error = %e,
                        "syslog: RELP send broke off, the batch is sent again"
                    );
                    Self::count_send_error();
                    return (SendResult::Backpressured, None);
                }
            }
        }
        let refused = answers.iter().enumerate().find_map(|(i, a)| match a {
            Some(Answer::Refused(text)) => Some((i, text)),
            _ => None,
        });
        // RELP has no permanent refusal: a receiver answers 500 for a full
        // queue as well as a bad message, so the whole batch is sent again.
        if let Some((index, text)) = refused {
            tracing::warn!(
                address = %self.address,
                message = index + 1,
                records = messages.len(),
                answer = %text,
                "syslog: RELP receiver refused a message, the batch is sent again"
            );
            Self::count_send_error();
            return (SendResult::Backpressured, session.map(Session::Relp));
        }
        (SendResult::Ok, session.map(Session::Relp))
    }

    async fn deliver_udp(
        &self,
        current: Option<Session>,
        messages: &[&[u8]],
        target: Option<SocketAddr>,
    ) -> (SendResult, Option<Session>) {
        let socket = match current {
            Some(Session::Udp(socket)) => socket,
            _ => match self.bind_udp(target).await {
                Ok(socket) => socket,
                Err(e) => {
                    tracing::warn!(address = %self.address, error = %e, "syslog: UDP socket failed");
                    Self::count_send_error();
                    return (SendResult::Backpressured, None);
                }
            },
        };
        for message in messages {
            if let Err(e) = socket.send(message).await {
                tracing::warn!(
                    address = %self.address,
                    error = %e,
                    "syslog: datagram not sent, the batch is sent again"
                );
                Self::count_send_error();
                return (SendResult::Backpressured, None);
            }
        }
        (SendResult::Ok, Some(Session::Udp(socket)))
    }

    /// A connection to `target`, else to the address by name.
    async fn connect(&self, target: Option<SocketAddr>) -> Result<Stream, ConnectError> {
        let stream = self.connector.connect(target).await?;
        tracing::info!(
            address = %self.address,
            protocol = self.protocol.as_str(),
            resolved = ?target,
            "syslog: connected"
        );
        Ok(stream)
    }

    /// A new RELP session, or the result to answer the batch with.
    async fn open_relp(&self, target: Option<SocketAddr>) -> Result<RelpSession, SendResult> {
        let stream = self
            .connect(target)
            .await
            .map_err(|e| self.connect_failed(&e))?;
        let opening = tokio::time::timeout(self.connector.timeout(), RelpSession::open(stream));
        match opening.await {
            Ok(Ok(session)) => Ok(session),
            Ok(Err(RelpError::Refused(why))) => {
                tracing::error!(address = %self.address, reason = %why, "syslog: RELP session refused");
                Err(SendResult::Fatal(TransportError::Config(format!(
                    "RELP receiver {} refused the session: {why}",
                    self.address
                ))))
            }
            Ok(Err(e)) => {
                tracing::warn!(address = %self.address, error = %e, "syslog: RELP open failed");
                Self::count_send_error();
                Err(SendResult::Backpressured)
            }
            Err(_) => {
                tracing::warn!(address = %self.address, "syslog: RELP open not answered in time");
                Self::count_send_error();
                Err(SendResult::Backpressured)
            }
        }
    }

    /// A socket sending to `target`, else to the address looked up by name.
    async fn bind_udp(&self, target: Option<SocketAddr>) -> std::io::Result<UdpSocket> {
        let target = match target {
            Some(target) => target,
            None => tokio::net::lookup_host(&self.address)
                .await?
                .next()
                .ok_or_else(|| std::io::Error::other("the address resolved to nothing"))?,
        };
        let local = if target.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(local).await?;
        socket.connect(target).await?;
        tracing::info!(address = %self.address, peer = %target, "syslog: UDP socket ready");
        Ok(socket)
    }

    /// The answer to a batch whose connection could not be made.
    fn connect_failed(&self, e: &ConnectError) -> SendResult {
        Self::count_send_error();
        match e {
            ConnectError::Unreachable(_) => {
                tracing::warn!(address = %self.address, error = %e, "syslog: cannot connect");
                SendResult::Backpressured
            }
            ConnectError::Refused(refused) => self.refusals.hold(refused, "syslog"),
            ConnectError::Trust(_) => {
                tracing::error!(address = %self.address, error = %e, "syslog: TLS refused");
                SendResult::Fatal(TransportError::Config(format!(
                    "syslog {}: {e}",
                    self.address
                )))
            }
        }
    }
}

/// The longest prefix of `bytes` within `limit` that does not end inside a
/// UTF-8 sequence.
fn utf8_floor(bytes: &[u8], limit: usize) -> usize {
    let mut cut = limit.min(bytes.len());
    while cut > 0 && cut < bytes.len() && (bytes[cut] & 0b1100_0000) == 0b1000_0000 {
        cut -= 1;
    }
    cut
}

impl TransportBase for SyslogTransport {
    /// Stop sending. A RELP session is closed with its `close` command, a
    /// stream shut down. Idempotent.
    async fn close(&self) -> TransportResult<()> {
        self.lifecycle.close();
        let session = self.state.lock().await.session.take();
        if let Some(session) = session {
            session.end().await;
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.lifecycle.is_healthy()
    }

    fn name(&self) -> &'static str {
        Self::TRANSPORT
    }

    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        match self.protocol {
            SyslogProtocol::Tcp | SyslogProtocol::Tls => DeliveryGuarantee::Written,
            SyslogProtocol::Udp => DeliveryGuarantee::FireAndForget,
            SyslogProtocol::Relp => DeliveryGuarantee::Acknowledged,
        }
    }

    /// Connect, and open the RELP session, keeping it for the first send.
    /// UDP resolves the address.
    async fn healthcheck(&self) -> TransportResult<()> {
        let fail =
            |what: String| TransportError::Connection(format!("syslog {}: {what}", self.address));
        let mut state = self.state.lock().await;
        if state.session.is_some() {
            return Ok(());
        }
        let target = self.connector.target().map_err(|e| fail(e.to_string()))?;
        state.session = Some(match self.protocol {
            SyslogProtocol::Tcp | SyslogProtocol::Tls => Session::Stream(
                self.connect(target)
                    .await
                    .map_err(|e| fail(e.to_string()))?,
            ),
            SyslogProtocol::Relp => Session::Relp(
                self.open_relp(target)
                    .await
                    .map_err(|result| fail(format!("{result:?}")))?,
            ),
            SyslogProtocol::Udp => Session::Udp(
                self.bind_udp(target)
                    .await
                    .map_err(|e| fail(e.to_string()))?,
            ),
        });
        state.dialled = target;
        Ok(())
    }
}

impl Meter for SyslogTransport {
    const TRANSPORT: &'static str = "syslog";
    const CORE_COUNTS: bool = true;
}

impl TransportSender for SyslogTransport {
    /// Send `payload` as one message, with `destination` as its MSGID.
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        let format = PayloadFormat::detect(&payload);
        let record = Record {
            payload,
            key: (!destination.is_empty()).then(|| Arc::from(destination)),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format,
            },
        };
        self.send_batch(std::slice::from_ref(&record)).await
    }

    async fn send_record(&self, record: &Record) -> SendResult {
        self.send_batch(std::slice::from_ref(record)).await
    }

    /// Send the block within `send_timeout_ms`.
    async fn send_batch(&self, records: &[Record]) -> SendResult {
        self.send_batch_by(records, Instant::now() + self.send_timeout)
            .await
    }

    /// Format the block, write it as one unit, and answer by `deadline`.
    async fn send_batch_by(&self, records: &[Record], deadline: Instant) -> SendResult {
        if self.lifecycle.is_closed() {
            return SendResult::Fatal(TransportError::Closed);
        }
        let timer = Self::send_timer();
        let mut outcome = BlockOutcome::default();
        // The wait for the lock counts against the deadline too.
        let work = async {
            let mut state = self.state.lock().await;
            self.format_batch(records, &mut state, &mut outcome);
            let count = state.ranges.len();
            let bytes = state.messages.len();
            if count == 0 {
                return (None, 0, 0);
            }
            (Some(self.deliver(&mut state).await), count, bytes)
        };
        let until = tokio::time::Instant::from_std(deadline);
        let (result, count, bytes) =
            tokio::time::timeout_at(until, work)
                .await
                .unwrap_or_else(|_elapsed| {
                    tracing::warn!(
                        address = %self.address,
                        records = records.len(),
                        "syslog: send deadline elapsed, the batch is sent again"
                    );
                    metrics::counter!(
                        "transport_redelivered_total",
                        "transport" => "syslog",
                        "reason" => "deadline"
                    )
                    .increment(1);
                    (Some(SendResult::Backpressured), 0, 0)
                });
        let Some(result) = result else {
            return outcome.finish();
        };
        timer.record();
        match result {
            SendResult::Ok => {
                Self::count_sent(count as u64, bytes as u64);
                tracing::debug!(
                    address = %self.address,
                    records = count,
                    bytes,
                    "syslog: batch sent"
                );
                outcome.finish()
            }
            SendResult::Backpressured => {
                Self::count_backpressured();
                SendResult::Backpressured
            }
            other => other,
        }
    }

    /// A record an outbound `dlq` filter matches, or whose message is over
    /// `max_message_bytes` on a protocol that does not cut it.
    fn dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        if self.filter_engine.outbound_dead_letter(&record.payload) {
            return Some(DeadLetterReason::OutboundFilter);
        }
        if self.protocol == SyslogProtocol::Udp {
            return None;
        }
        let mut message = Vec::with_capacity(record.payload.len() + 128);
        self.formatter.write(record, &mut message);
        (message.len() > self.limit).then_some(DeadLetterReason::TooLarge {
            bytes: message.len(),
            limit: self.limit,
        })
    }
}
