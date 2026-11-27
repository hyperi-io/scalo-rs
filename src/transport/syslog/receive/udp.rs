// Project:   scalo
// File:      src/transport/syslog/receive/udp.rs
// Purpose:   Syslog UDP listener: sized receive buffer, batched reads, one record per datagram
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use quinn_udp::{RecvMeta, UdpSocketState};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;

use super::ip_filter::IpFilter;
use super::{Inbox, LogEvery, SyslogReceiver};
use crate::transport::core::Meter;
use crate::transport::error::{TransportError, TransportResult};

/// Room for the largest datagram, or for a run of equal datagrams from one
/// sender that the kernel joined by receive offload, which it caps at 64 KiB.
const MAX_DATAGRAM: usize = 64 * 1024;

/// One allocation that datagrams are copied into back to back, each record a
/// slice of it.
const BLOCK: usize = 16 * MAX_DATAGRAM;

/// The wait after a receive error, so one that persists does not spin the loop.
const ERROR_PAUSE: Duration = Duration::from_secs(1);

/// The least time between two logged receive errors, or two logged refusals.
const LOG_EVERY: Duration = Duration::from_secs(5);

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

/// A bound UDP listener and the buffers it reads a batch into.
pub(super) struct Listener {
    socket: UdpSocket,
    batch: Batch,
}

impl Listener {
    /// Bind `addr` as [`bind`] does, reading up to `batch` datagrams per wake.
    pub(super) fn bind(
        addr: SocketAddr,
        recv_buffer_bytes: usize,
        batch: usize,
    ) -> TransportResult<Self> {
        let socket = bind(addr, recv_buffer_bytes)?;
        let batch = Batch::new(&socket, batch).map_err(|e| {
            TransportError::Connection(format!("syslog UDP {addr}: socket options: {e}"))
        })?;
        Ok(Self { socket, batch })
    }

    pub(super) fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    /// Queue each datagram as one record until `shutdown`. A datagram from a
    /// sender `filter` refuses, or one the full queue cannot take, is dropped
    /// and counted: UDP has no way to refuse or to ask the sender to wait. An
    /// empty datagram is no record, and is counted.
    pub(super) async fn read(
        mut self,
        filter: Arc<IpFilter>,
        inbox: Arc<Inbox>,
        shutdown: CancellationToken,
    ) {
        let mut pack = BytesMut::new();
        let mut errors = LogEvery::new(LOG_EVERY);
        let mut refusals = LogEvery::new(LOG_EVERY);
        loop {
            let ready = tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                ready = self.socket.readable() => ready,
            };
            if let Err(e) = ready {
                pause_after_error(&e, &mut errors).await;
                continue;
            }
            let timestamp_ms = crate::clock::epoch_millis();
            let (mut records, mut bytes, mut closed) = (0_u64, 0_u64, false);
            let read = self.batch.receive(&self.socket, |data, peer| {
                if closed {
                    return;
                }
                if data.is_empty() {
                    count_dropped("empty");
                    return;
                }
                if !filter.admits(peer.ip()) {
                    count_dropped("ip_filter");
                    if refusals.due() {
                        tracing::info!(%peer, "syslog UDP: datagram refused by ip_filter");
                    }
                    return;
                }
                let permit = match inbox.tx.try_reserve() {
                    Ok(permit) => permit,
                    Err(TrySendError::Full(())) => {
                        count_dropped("queue_full");
                        return;
                    }
                    Err(TrySendError::Closed(())) => {
                        closed = true;
                        return;
                    }
                };
                if pack.capacity() < data.len() {
                    pack.reserve(BLOCK);
                }
                pack.extend_from_slice(data);
                permit.send(inbox.message(pack.split().freeze(), peer, timestamp_ms));
                records += 1;
                bytes += data.len() as u64;
            });
            if records > 0 {
                SyslogReceiver::count_received(records, bytes);
            }
            if closed {
                return;
            }
            if let Err(e) = read {
                pause_after_error(&e, &mut errors).await;
            }
        }
    }
}

/// Count a datagram or frame dropped in `transport_dropped_total`.
pub(super) fn count_dropped(reason: &'static str) {
    metrics::counter!(
        "transport_dropped_total",
        "transport" => "syslog",
        "reason" => reason
    )
    .increment(1);
}

/// Buffers for one batch of datagrams, reused across reads. On Linux one
/// `recvmmsg` fills many of them, and elsewhere one read fills each.
struct Batch {
    state: UdpSocketState,
    buffers: Vec<Vec<u8>>,
    meta: Vec<RecvMeta>,
}

impl Batch {
    fn new(socket: &UdpSocket, size: usize) -> io::Result<Self> {
        let size = size.max(1);
        Ok(Self {
            state: UdpSocketState::new(socket.into())?,
            buffers: vec![vec![0; MAX_DATAGRAM]; size],
            meta: vec![RecvMeta::default(); size],
        })
    }

    /// Read the datagrams ready, until the buffers are full or none is left,
    /// handing each to `each` with its sender. A run joined by receive
    /// offload is handed over as the datagrams it joined.
    ///
    /// An error after some datagrams were read keeps them.
    fn receive(
        &mut self,
        socket: &UdpSocket,
        mut each: impl FnMut(&[u8], SocketAddr),
    ) -> io::Result<()> {
        let Self {
            state,
            buffers,
            meta,
        } = self;
        let mut used = 0;
        while used < buffers.len() {
            let mut slices: Vec<IoSliceMut<'_>> = buffers[used..]
                .iter_mut()
                .take(quinn_udp::BATCH_SIZE)
                .map(|buf| IoSliceMut::new(buf))
                .collect();
            let metas = &mut meta[used..used + slices.len()];
            let read = socket.try_io(Interest::READABLE, || {
                state.recv(socket.into(), &mut slices, metas)
            });
            drop(slices);
            let filled = match read {
                Ok(filled) => filled,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            };
            for (buf, m) in buffers[used..].iter().zip(&meta[used..]).take(filled) {
                if m.len == 0 {
                    each(&[], m.addr);
                    continue;
                }
                // Receive offload joins equal datagrams from one sender, each
                // `stride` bytes but the last.
                let stride = m.stride.max(1);
                let mut start = 0;
                while start < m.len {
                    let end = (start + stride).min(m.len);
                    each(&buf[start..end], m.addr);
                    start = end;
                }
            }
            if filled == 0 {
                return Ok(());
            }
            used += filled;
        }
        Ok(())
    }
}

/// Log a receive error, at most once per [`LOG_EVERY`], then wait
/// [`ERROR_PAUSE`].
async fn pause_after_error(e: &io::Error, errors: &mut LogEvery) {
    if errors.due() {
        tracing::warn!(error = %e, "syslog UDP: receive failed, pausing");
    }
    tokio::time::sleep(ERROR_PAUSE).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A burst larger than one batch is read whole, in batches no larger than
    /// asked, each datagram as sent with its sender.
    #[tokio::test]
    async fn a_burst_is_read_in_batches_with_nothing_lost() {
        let mut listener = Listener::bind("127.0.0.1:0".parse().unwrap(), 256 * 1024, 64).unwrap();
        let to = listener.socket().local_addr().unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let sent: Vec<String> = (0..150).map(|i| format!("<14>datagram {i}")).collect();
        for line in &sent {
            sender.send_to(line.as_bytes(), to).unwrap();
        }
        let from = sender.local_addr().unwrap();
        let mut got = Vec::new();
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        while got.len() < sent.len() && tokio::time::Instant::now() < until {
            listener.socket.readable().await.unwrap();
            let mut read = Vec::new();
            let Listener { socket, batch } = &mut listener;
            batch
                .receive(socket, |data, peer| {
                    assert_eq!(peer, from);
                    read.push(String::from_utf8(data.to_vec()).unwrap());
                })
                .unwrap();
            assert!(
                read.len() <= 64,
                "a batch of {} over the 64 asked",
                read.len()
            );
            got.extend(read);
        }
        assert_eq!(got, sent);
    }
}
