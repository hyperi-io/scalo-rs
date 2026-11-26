// Project:   scalo
// File:      src/transport/syslog/receive/tcp.rs
// Purpose:   Syslog TCP and TLS listeners: framing, keepalive, idle and frame timeouts
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::config::millis;
use super::{Inbox, SyslogListenConfig, SyslogReceiver};
use crate::framing::{DecodeLimits, Frame, Framing, OctetCounting, StreamDecoder};
use crate::tls::ServerTls;
use crate::transport::core::Meter;

/// The wait after an accept that failed on this side, such as out of file
/// descriptors.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);

/// How every connection of a listener reads its frames.
pub(super) struct Connection {
    pub(super) framing: Arc<dyn Framing>,
    pub(super) limits: DecodeLimits,
    pub(super) keepalive: Option<Duration>,
    pub(super) idle: Option<Duration>,
    pub(super) frame_timeout: Option<Duration>,
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
        }
    }
}

/// Why a connection is closed before its peer closes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Closed {
    /// No frame within `idle_timeout_ms`.
    Idle,
    /// A frame begun and not ended within `frame_timeout_ms`.
    FrameTimeout,
}

impl Closed {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::FrameTimeout => "frame_timeout",
        }
    }
}

/// Accept connections on `listener` until `shutdown`, each read on a task of its own.
pub(super) async fn accept(
    listener: TcpListener,
    tls: Option<ServerTls>,
    connection: Arc<Connection>,
    inbox: Arc<Inbox>,
    shutdown: CancellationToken,
) {
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
                metrics::counter!(
                    "transport_connections_closed_total",
                    "transport" => "syslog",
                    "reason" => why.as_str()
                )
                .increment(1);
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
