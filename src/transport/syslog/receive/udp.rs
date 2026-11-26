// Project:   scalo
// File:      src/transport/syslog/receive/udp.rs
// Purpose:   Syslog UDP listener: sized receive buffer, one record per datagram
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;

use super::{Inbox, SyslogReceiver};
use crate::transport::core::Meter;
use crate::transport::error::{TransportError, TransportResult};

/// Room for the largest datagram.
const MAX_DATAGRAM: usize = 64 * 1024;

/// One allocation that datagrams are read into back to back, each record a
/// slice of it.
const BLOCK: usize = 16 * MAX_DATAGRAM;

/// The wait after a receive error, so one that persists does not spin the loop.
const ERROR_PAUSE: Duration = Duration::from_secs(1);

/// The least time between two logged receive errors.
const ERROR_LOG_EVERY: Duration = Duration::from_secs(5);

/// Bind `addr` with a receive buffer of `recv_buffer_bytes`, saying when the
/// kernel granted less.
pub(super) fn bind(addr: SocketAddr, recv_buffer_bytes: usize) -> TransportResult<UdpSocket> {
    let fail = |what: &str, e: io::Error| {
        TransportError::Connection(format!("syslog UDP {addr}: {what}: {e}"))
    };
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .map_err(|e| fail("socket", e))?;
    socket
        .set_recv_buffer_size(recv_buffer_bytes)
        .map_err(|e| fail("SO_RCVBUF", e))?;
    // Linux reports double the room it grants, so less than asked means
    // net.core.rmem_max capped it.
    match socket.recv_buffer_size() {
        Ok(granted) if granted < recv_buffer_bytes => tracing::warn!(
            asked = recv_buffer_bytes,
            granted,
            "syslog UDP receive buffer is smaller than udp_recv_buffer_bytes: raise net.core.rmem_max"
        ),
        Ok(_) => {}
        Err(e) => tracing::debug!(error = %e, "syslog UDP receive buffer size unreadable"),
    }
    socket
        .set_nonblocking(true)
        .map_err(|e| fail("set_nonblocking", e))?;
    socket.bind(&addr.into()).map_err(|e| fail("bind", e))?;
    UdpSocket::from_std(socket.into()).map_err(|e| fail("register", e))
}

/// Queue each datagram as one record until `shutdown`, reading up to `batch`
/// per wake. A datagram the full queue cannot take is dropped and counted:
/// UDP has no way to ask the sender to wait. An empty datagram is no record,
/// and is counted.
pub(super) async fn read(
    socket: UdpSocket,
    batch: usize,
    inbox: Arc<Inbox>,
    shutdown: CancellationToken,
) {
    let mut buf = BytesMut::new();
    let mut logged: Option<Instant> = None;
    loop {
        let ready = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            ready = socket.readable() => ready,
        };
        if let Err(e) = ready {
            pause_after_error(&e, &mut logged).await;
            continue;
        }
        let timestamp_ms = crate::clock::epoch_millis();
        let (mut records, mut bytes) = (0_u64, 0_u64);
        let mut failed = None;
        for _ in 0..batch {
            if buf.capacity() < MAX_DATAGRAM {
                buf.reserve(BLOCK);
            }
            let (len, peer) = match socket.try_recv_buf_from(&mut buf) {
                Ok(read) => read,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    failed = Some(e);
                    break;
                }
            };
            if len == 0 {
                metrics::counter!(
                    "transport_dropped_total",
                    "transport" => "syslog",
                    "reason" => "empty"
                )
                .increment(1);
                continue;
            }
            let payload = buf.split().freeze();
            match inbox
                .tx
                .try_send(inbox.message(payload, peer, timestamp_ms))
            {
                Ok(()) => {
                    records += 1;
                    bytes += len as u64;
                }
                Err(TrySendError::Full(_)) => {
                    metrics::counter!(
                        "transport_dropped_total",
                        "transport" => "syslog",
                        "reason" => "queue_full"
                    )
                    .increment(1);
                }
                Err(TrySendError::Closed(_)) => return,
            }
        }
        if records > 0 {
            SyslogReceiver::count_received(records, bytes);
        }
        if let Some(e) = failed {
            pause_after_error(&e, &mut logged).await;
        }
    }
}

/// Log a receive error, at most once per [`ERROR_LOG_EVERY`], then wait
/// [`ERROR_PAUSE`].
async fn pause_after_error(e: &io::Error, logged: &mut Option<Instant>) {
    if logged.is_none_or(|at| at.elapsed() >= ERROR_LOG_EVERY) {
        tracing::warn!(error = %e, "syslog UDP: receive failed, pausing");
        *logged = Some(Instant::now());
    }
    tokio::time::sleep(ERROR_PAUSE).await;
}
