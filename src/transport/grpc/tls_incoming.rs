// Project:   scalo
// File:      src/transport/grpc/tls_incoming.rs
// Purpose:   Push listener connections past their TLS handshake, for tonic
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The incoming stream a TLS receive server serves from.
//!
//! The stream owns the listener, so dropping it closes the socket at once and
//! the server frees its port when it stops. Each accepted connection runs its
//! handshake in its own task and reaches tonic once the handshake completes.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_stream::Stream;
use tonic::transport::server::Connected;

use crate::accept::pause_for;
use crate::tls::ServerTls;

/// Finished handshakes waiting for tonic to take them.
const HANDSHAKED_BACKLOG: usize = 1024;

/// A connection past its TLS handshake, as tonic's server takes it.
pub(super) struct TlsConnection(TlsStream<TcpStream>);

impl Connected for TlsConnection {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for TlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// The TLS listener's connections, accepted and handshaken.
pub(super) struct TlsIncoming {
    listener: TcpListener,
    tls: ServerTls,
    /// Kept so the handshake channel stays open while the stream lives.
    sender: mpsc::Sender<TlsConnection>,
    handshaked: mpsc::Receiver<TlsConnection>,
    /// Set after an accept error that is not the client's, until it passes.
    pause: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl TlsIncoming {
    pub(super) fn new(listener: TcpListener, tls: ServerTls) -> Self {
        let (sender, handshaked) = mpsc::channel(HANDSHAKED_BACKLOG);
        Self {
            listener,
            tls,
            sender,
            handshaked,
            pause: None,
        }
    }

    fn handshake(&self, tcp: TcpStream, peer: SocketAddr) {
        let tls = self.tls.clone();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            if let Err(e) = tcp.set_nodelay(true) {
                tracing::debug!(%peer, error = %e, "gRPC: TCP_NODELAY not set on a connection");
            }
            // ServerTls::accept counts and logs a failed handshake.
            if let Ok(stream) = tls.accept(tcp, Some(peer)).await {
                // The receiver is gone once the server has stopped.
                let _ = sender.send(TlsConnection(stream)).await;
            }
        });
    }
}

impl Stream for TlsIncoming {
    type Item = io::Result<TlsConnection>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(pause) = this.pause.as_mut() {
                ready!(pause.as_mut().poll(cx));
                this.pause = None;
            }
            match this.listener.poll_accept(cx) {
                Poll::Ready(Ok((tcp, peer))) => this.handshake(tcp, peer),
                Poll::Ready(Err(e)) => {
                    let Some(pause) = pause_for(e.kind()) else {
                        tracing::debug!(error = %e, "gRPC: connection failed before accept");
                        continue;
                    };
                    tracing::error!(error = %e, listener = "grpc", "listener accept failed");
                    this.pause = Some(Box::pin(tokio::time::sleep(pause)));
                }
                Poll::Pending => break,
            }
        }
        this.handshaked
            .poll_recv(cx)
            .map(|connection| connection.map(Ok))
    }
}
