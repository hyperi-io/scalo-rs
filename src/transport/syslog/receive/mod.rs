// Project:   scalo
// File:      src/transport/syslog/receive/mod.rs
// Purpose:   Syslog receiver: TCP, TLS and UDP listeners feeding recv
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Receives syslog messages over TCP, TLS and UDP, one record per message.
//!
//! A record is the message's bytes as they came: parsing its PRI and header
//! is the app's. TCP and TLS take octet-counted frames and LF lines on one
//! connection (RFC 6587), through [`OctetCounting`](crate::framing::OctetCounting).
//! UDP takes one message per
//! datagram (RFC 5426). Each record is a slice of the buffer it was read into.
//!
//! Records wait in a queue of `recv_buffer_size` for [`recv`](TransportReceiver::recv).
//! A full queue stops TCP and TLS reads, so the sender's writes back up, and
//! drops a datagram, counted in `transport_dropped_total{reason="queue_full"}`.
//! No syslog protocol here answers the sender, so `commit` does nothing.

mod config;
mod tcp;
mod udp;

#[cfg(test)]
mod tests;

pub use config::SyslogListenConfig;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use self::tcp::Connection;
use crate::tls::ServerTls;
use crate::transport::core::{Lifecycle, Meter};
use crate::transport::delivery::DeliveryGuarantee;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::filter::{TransportFilterEngine, TransportFilterTierConfig};
use crate::transport::traits::{CommitToken, RecvBatch, TransportBase, TransportReceiver};
use crate::transport::types::{Message, PayloadFormat};
use crate::transport::work_batch::WorkBatch;

/// Where a received record came from. Nothing is committed back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyslogToken {
    /// Local sequence number, rising per receiver.
    pub seq: u64,
    /// The sender's address.
    pub peer: SocketAddr,
}

impl CommitToken for SyslogToken {}

impl std::fmt::Display for SyslogToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "syslog:{}:{}", self.peer, self.seq)
    }
}

/// The queue every listener puts its records on.
struct Inbox {
    tx: mpsc::Sender<Message<SyslogToken>>,
    sequence: AtomicU64,
}

impl Inbox {
    fn message(&self, payload: Bytes, peer: SocketAddr, timestamp_ms: i64) -> Message<SyslogToken> {
        Message {
            key: None,
            payload,
            token: SyslogToken {
                seq: self.sequence.fetch_add(1, Ordering::Relaxed),
                peer,
            },
            timestamp_ms: Some(timestamp_ms),
            format: PayloadFormat::Auto,
            headers: Vec::new(),
        }
    }
}

/// The addresses the listeners bound, with any port 0 resolved.
#[derive(Debug, Clone, Copy, Default)]
struct Bound {
    tcp: Option<SocketAddr>,
    tls: Option<SocketAddr>,
    udp: Option<SocketAddr>,
}

/// Receives syslog messages over TCP, TLS and UDP. Build it from a
/// [`SyslogListenConfig`].
pub struct SyslogReceiver {
    receiver: tokio::sync::Mutex<mpsc::Receiver<Message<SyslogToken>>>,
    bound: Bound,
    recv_timeout: Duration,
    guarantee: DeliveryGuarantee,
    filter_engine: TransportFilterEngine,
    /// Stops the listeners and every connection.
    shutdown: CancellationToken,
    lifecycle: Lifecycle,
}

impl SyslogReceiver {
    /// Bind every listener `config` names and start taking messages. Each
    /// listener binds before any is served, so a failed bind leaves none.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for a setting out of range, a TLS source that
    /// cannot be read or an inbound filter that does not compile, and
    /// `TransportError::Connection` for an address that cannot be bound.
    pub async fn new(config: &SyslogListenConfig) -> TransportResult<Self> {
        let addresses = config.addresses()?;
        let tls = match &config.tls {
            Some(tls) => Some(
                ServerTls::load(tls)
                    .await
                    .map_err(|e| TransportError::Config(format!("syslog listen: TLS: {e}")))?,
            ),
            None => None,
        };
        let filter_engine = TransportFilterEngine::new(
            &config.filters_in,
            &[],
            &TransportFilterTierConfig::from_cascade()?,
        )?;
        let tcp = bind_tcp(addresses.tcp).await?;
        let secure = bind_tcp(addresses.tls).await?;
        let udp = addresses
            .udp
            .map(|addr| udp::bind(addr, config.udp_recv_buffer_bytes))
            .transpose()?;
        let local = |addr: Option<std::io::Result<SocketAddr>>| addr.transpose().ok().flatten();
        let bound = Bound {
            tcp: local(tcp.as_ref().map(TcpListener::local_addr)),
            tls: local(secure.as_ref().map(TcpListener::local_addr)),
            udp: local(udp.as_ref().map(tokio::net::UdpSocket::local_addr)),
        };

        let (tx, rx) = mpsc::channel(config.recv_buffer_size);
        let inbox = Arc::new(Inbox {
            tx,
            sequence: AtomicU64::new(0),
        });
        let connection = Arc::new(Connection::new(config));
        let shutdown = CancellationToken::new();
        for (listener, tls) in [(tcp, None), (secure, tls)] {
            if let Some(listener) = listener {
                tokio::spawn(tcp::accept(
                    listener,
                    tls,
                    Arc::clone(&connection),
                    Arc::clone(&inbox),
                    shutdown.clone(),
                ));
            }
        }
        if let Some(socket) = udp {
            tokio::spawn(udp::read(
                socket,
                config.udp_batch_size,
                Arc::clone(&inbox),
                shutdown.clone(),
            ));
        }
        let guarantee = if bound.udp.is_some() {
            DeliveryGuarantee::FireAndForget
        } else {
            DeliveryGuarantee::Written
        };
        tracing::info!(
            tcp = ?bound.tcp,
            tls = ?bound.tls,
            udp = ?bound.udp,
            carriage_returns = ?config.carriage_returns,
            max_message_bytes = config.max_message_bytes,
            "syslog receiver listening"
        );
        Ok(Self {
            receiver: tokio::sync::Mutex::new(rx),
            bound,
            recv_timeout: Duration::from_millis(config.recv_timeout_ms),
            guarantee,
            filter_engine,
            shutdown,
            lifecycle: Lifecycle::new::<Self>(|| true),
        })
    }

    /// Load the config from the cascade under `transport.syslog.listen` and build.
    ///
    /// # Errors
    ///
    /// As [`SyslogListenConfig::from_cascade`] and [`new`](Self::new).
    pub async fn from_cascade() -> TransportResult<Self> {
        Self::new(&SyslogListenConfig::from_cascade()?).await
    }

    /// The address the TCP listener bound, or `None` without one.
    #[must_use]
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.bound.tcp
    }

    /// The address the TLS listener bound, or `None` without one.
    #[must_use]
    pub fn tls_addr(&self) -> Option<SocketAddr> {
        self.bound.tls
    }

    /// The address the UDP listener bound, or `None` without one.
    #[must_use]
    pub fn udp_addr(&self) -> Option<SocketAddr> {
        self.bound.udp
    }
}

/// Bind a TCP listener at `addr`, when there is one.
async fn bind_tcp(addr: Option<SocketAddr>) -> TransportResult<Option<TcpListener>> {
    let Some(addr) = addr else {
        return Ok(None);
    };
    TcpListener::bind(addr)
        .await
        .map(Some)
        .map_err(|e| TransportError::Connection(format!("syslog: failed to bind {addr}: {e}")))
}

impl Meter for SyslogReceiver {
    const TRANSPORT: &'static str = "syslog";
    const CORE_COUNTS: bool = true;
}

impl TransportBase for SyslogReceiver {
    /// Stop the listeners and close every connection, keeping the records
    /// already queued for [`recv`](TransportReceiver::recv). Idempotent.
    async fn close(&self) -> TransportResult<()> {
        self.lifecycle.close();
        self.shutdown.cancel();
        self.receiver.lock().await.close();
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.lifecycle.is_healthy()
    }

    fn name(&self) -> &'static str {
        Self::TRANSPORT
    }

    /// `fire_and_forget` with a UDP listener, `written` otherwise.
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        self.guarantee
    }
}

impl TransportReceiver for SyslogReceiver {
    type Token = SyslogToken;

    /// Up to `max` queued records, waiting `recv_timeout_ms` for the first.
    ///
    /// After [`close`](TransportBase::close) this returns the records still
    /// queued, then [`TransportError::Closed`].
    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
        use tokio::sync::mpsc::error::TryRecvError;

        let mut rx = self.receiver.lock().await;
        let mut messages = Vec::with_capacity(max.min(1024));
        while messages.len() < max {
            let message = if messages.is_empty() && !self.recv_timeout.is_zero() {
                match tokio::time::timeout(self.recv_timeout, rx.recv()).await {
                    Ok(Some(message)) => message,
                    Ok(None) => return Err(TransportError::Closed),
                    Err(_elapsed) => break,
                }
            } else {
                match rx.try_recv() {
                    Ok(message) => message,
                    Err(TryRecvError::Disconnected) if messages.is_empty() => {
                        return Err(TransportError::Closed);
                    }
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                }
            };
            messages.push(message);
        }
        let batch = self.filter_engine.partition_batch(
            messages,
            |m| m.payload.as_ref(),
            |m| m.key.clone(),
            |m| m.token,
        );
        Ok(RecvBatch {
            messages: batch.messages,
            dlq_entries: batch.dlq_entries,
            filtered_tokens: batch.filtered_tokens,
        }
        .into())
    }

    async fn commit(&self, _tokens: &[Self::Token]) -> TransportResult<()> {
        Ok(())
    }
}

impl Drop for SyslogReceiver {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
