// Project:   scalo
// File:      src/transport/syslog/receive/tcp.rs
// Purpose:   Syslog TCP and TLS listeners: admission, framing, keepalive and timeouts
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::config::millis;
use super::ip_filter::IpFilter;
use super::{Inbox, LogEvery, SyslogListenConfig, SyslogReceiver};
use crate::framing::{DecodeLimits, Frame, Framing, OctetCounting, StreamDecoder};
use crate::tls::ServerTls;
use crate::transport::core::Meter;

/// The wait after an accept that failed on this side, such as out of file
/// descriptors.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);

/// The least time between two logged refusals on one listener.
const REFUSAL_LOG_EVERY: Duration = Duration::from_secs(5);

/// How every connection of a listener reads its frames.
pub(super) struct Connection {
    pub(super) framing: Arc<dyn Framing>,
    pub(super) limits: DecodeLimits,
    pub(super) keepalive: Option<Duration>,
    pub(super) idle: Option<Duration>,
    pub(super) frame_timeout: Option<Duration>,
    pub(super) first_byte: Option<Duration>,
}

impl Connection {
    /// The framing, limits and timeouts `config` sets.
    pub(super) fn new(config: &SyslogListenConfig) -> Self {
        Self {
            framing: Arc::new(OctetCounting::new(config.carriage_returns)),
            limits: config.limits(),
            keepalive: (config.tcp_keepalive_secs > 0)
                .then(|| Duration::from_secs(config.tcp_keepalive_secs)),
            idle: millis(config.idle_timeout_ms),
            frame_timeout: millis(config.frame_timeout_ms),
            first_byte: millis(config.first_byte_timeout_ms),
        }
    }
}

/// Which connections one listener takes: the IP filter's senders, up to its
/// connection cap.
pub(super) struct Gate {
    filter: Arc<IpFilter>,
    open: Arc<AtomicUsize>,
    /// 0 holds no limit.
    max: usize,
}

impl Gate {
    pub(super) fn new(filter: Arc<IpFilter>, max: usize) -> Self {
        Self {
            filter,
            open: Arc::default(),
            max,
        }
    }

    /// A place for a connection from `peer`, held until it drops, or why
    /// there is none. Only the listener's accept loop calls it, so a check
    /// then an add cannot overshoot the cap.
    pub(super) fn admit(&self, peer: SocketAddr) -> Result<Slot, Closed> {
        if !self.filter.admits(peer.ip()) {
            return Err(Closed::IpFilter);
        }
        if self.max > 0 && self.open.load(Ordering::Acquire) >= self.max {
            return Err(Closed::OverLimit);
        }
        self.open.fetch_add(1, Ordering::AcqRel);
        Ok(Slot(Arc::clone(&self.open)))
    }
}

/// One connection's place under its listener's cap, given back on drop.
pub(super) struct Slot(Arc<AtomicUsize>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Why a connection is closed before its peer closes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Closed {
    /// The sender is not one `ip_filter` takes.
    IpFilter,
    /// The listener already holds `max_connections`.
    OverLimit,
    /// Nothing sent within `first_byte_timeout_ms` of the accept.
    NoFirstByte,
    /// No frame within `idle_timeout_ms`.
    Idle,
    /// A frame begun and not ended within `frame_timeout_ms`.
    FrameTimeout,
}

impl Closed {
    fn as_str(self) -> &'static str {
        match self {
            Self::IpFilter => "ip_filter",
            Self::OverLimit => "over_limit",
            Self::NoFirstByte => "no_first_byte",
            Self::Idle => "idle",
            Self::FrameTimeout => "frame_timeout",
        }
    }

    /// Count the close in `transport_connections_closed_total`.
    fn count(self) {
        metrics::counter!(
            "transport_connections_closed_total",
            "transport" => "syslog",
            "reason" => self.as_str()
        )
        .increment(1);
    }
}

/// Accept connections on `listener` until `shutdown`, each read on a task of
/// its own. A connection `gate` refuses is closed as it is accepted, before
/// any byte of it is read.
pub(super) async fn accept(
    listener: TcpListener,
    tls: Option<ServerTls>,
    connection: Arc<Connection>,
    gate: Gate,
    inbox: Arc<Inbox>,
    shutdown: CancellationToken,
) {
    let mut refusals = LogEvery::new(REFUSAL_LOG_EVERY);
    loop {
        let (stream, peer) = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    pause_after_error(&e).await;
                    continue;
                }
            },
        };
        let slot = match gate.admit(peer) {
            Ok(slot) => slot,
            Err(why) => {
                why.count();
                if refusals.due() {
                    tracing::info!(%peer, reason = why.as_str(), "syslog: connection refused");
                }
                continue;
            }
        };
        if let Some(idle) = connection.keepalive
            && let Err(e) = keep_alive(&stream, idle)
        {
            tracing::debug!(%peer, error = %e, "syslog: keepalive not set");
        }
        let (tls, connection, inbox, shutdown) = (
            tls.clone(),
            Arc::clone(&connection),
            Arc::clone(&inbox),
            shutdown.clone(),
        );
        tokio::spawn(async move {
            let _slot = slot;
            if let Some(limit) = connection.first_byte
                && !first_byte(&stream, peer, limit, &shutdown).await
            {
                return;
            }
            let Some(tls) = tls else {
                read(stream, peer, &connection, &inbox, &shutdown).await;
                return;
            };
            let handshake = tokio::select! {
                () = shutdown.cancelled() => return,
                handshake = tls.accept(stream, Some(peer)) => handshake,
            };
            match handshake {
                Ok(stream) => {
                    read(stream, peer, &connection, &inbox, &shutdown).await;
                }
                Err(e) => tracing::debug!(%peer, error = %e, "syslog: TLS handshake failed"),
            }
        });
    }
}

/// Wait up to `limit` for `stream`'s first byte, leaving it to be read. False
/// when none arrives in time, counted, or the peer closes first, or shutdown.
async fn first_byte(
    stream: &TcpStream,
    peer: SocketAddr,
    limit: Duration,
    shutdown: &CancellationToken,
) -> bool {
    let mut byte = [0_u8; 1];
    let peeked = tokio::select! {
        biased;
        () = shutdown.cancelled() => return false,
        peeked = tokio::time::timeout(limit, stream.peek(&mut byte)) => peeked,
    };
    match peeked {
        Ok(Ok(read)) => read > 0,
        Ok(Err(_)) => false,
        Err(_elapsed) => {
            Closed::NoFirstByte.count();
            tracing::debug!(%peer, "syslog: connection closed, nothing sent");
            false
        }
    }
}

/// A connection-level error is the client's and costs nothing. Anything
/// else, such as out of file descriptors, backs off so the loop does not spin.
async fn pause_after_error(e: &std::io::Error) {
    if matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    ) {
        return;
    }
    tracing::error!(error = %e, "syslog: accept failed");
    tokio::time::sleep(ACCEPT_RETRY).await;
}

/// Turn on keepalive for `stream`: probes start after `idle` with no traffic
/// and repeat every `idle`.
pub(super) fn keep_alive(stream: &TcpStream, idle: Duration) -> std::io::Result<()> {
    let params = TcpKeepalive::new().with_time(idle);
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    let params = params.with_interval(idle);
    SockRef::from(stream).set_tcp_keepalive(&params)
}

/// Read and queue frames until the peer closes, a timeout closes the
/// connection, the queue closes, or shutdown.
///
/// A frame begun has `frame_timeout` to end, counted while the socket is
/// awaited, so a wait on a full queue is not. A connection between frames
/// closes after `idle` with none.
pub(super) async fn read<S: AsyncRead + Unpin>(
    mut stream: S,
    peer: SocketAddr,
    connection: &Connection,
    inbox: &Inbox,
    shutdown: &CancellationToken,
) -> Option<Closed> {
    let mut decoder = StreamDecoder::new(Arc::clone(&connection.framing), connection.limits);
    let mut idle_at = connection.idle.map(|idle| Instant::now() + idle);
    let mut frame_at: Option<Instant> = None;
    loop {
        let frames = drain(&mut decoder, peer, inbox).await?;
        let now = Instant::now();
        if frames > 0 {
            idle_at = connection.idle.map(|idle| now + idle);
            frame_at = None;
        }
        let (wake, why) = if decoder.mid_frame() {
            if frame_at.is_none() {
                frame_at = connection.frame_timeout.map(|limit| now + limit);
            }
            (frame_at, Closed::FrameTimeout)
        } else {
            frame_at = None;
            (idle_at, Closed::Idle)
        };
        let read = tokio::select! {
            biased;
            () = shutdown.cancelled() => return None,
            () = until(wake) => {
                why.count();
                tracing::debug!(%peer, reason = why.as_str(), "syslog: connection closed");
                return Some(why);
            }
            read = decoder.read_from(&mut stream) => read,
        };
        match read {
            Ok(0) => {
                decoder.finish();
                drain(&mut decoder, peer, inbox).await;
                return None;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(%peer, error = %e, "syslog: read failed, connection closed");
                return None;
            }
        }
    }
}

/// Queue every whole frame the decoder holds, and return how many frames it
/// took: `None` once the queue is closed.
async fn drain(decoder: &mut StreamDecoder, peer: SocketAddr, inbox: &Inbox) -> Option<usize> {
    let timestamp_ms = crate::clock::epoch_millis();
    let (mut frames, mut records, mut bytes) = (0, 0_u64, 0_u64);
    loop {
        let frame = match decoder.next_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(%peer, error = %e, "syslog: a frame cut off at the end of the input");
                super::udp::count_dropped("cut_off");
                break;
            }
        };
        frames += 1;
        match frame {
            Frame::Record(payload) => {
                let len = payload.len() as u64;
                if inbox
                    .tx
                    .send(inbox.message(payload, peer, timestamp_ms))
                    .await
                    .is_err()
                {
                    return None;
                }
                records += 1;
                bytes += len;
            }
            Frame::Oversize { len, .. } => {
                SyslogReceiver::count_too_large();
                tracing::debug!(%peer, frame_bytes = len, "syslog: message over max_message_bytes skipped");
            }
        }
    }
    if records > 0 {
        SyslogReceiver::count_received(records, bytes);
    }
    Some(frames)
}

/// Wait until `at`, or for ever when there is none.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
