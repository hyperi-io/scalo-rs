// Project:   scalo
// File:      src/http_server/tls.rs
// Purpose:   axum listener that hands over connections after the TLS handshake
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! An [`axum::serve::Listener`] that terminates TLS with a [`ServerTls`].

use std::io;
use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::server::TlsStream;

use crate::tls::ServerTls;

/// Finished handshakes waiting for axum to take them.
const HANDSHAKED_BACKLOG: usize = 1024;

/// A TCP listener whose connections reach axum after their TLS handshake.
///
/// Handshakes run in their own tasks, each bounded by the handshake timeout,
/// so a slow client holds up nobody else. Dropping the listener stops
/// accepting.
pub struct TlsListener {
    connections: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local_addr: SocketAddr,
    accept_loop: JoinHandle<()>,
}

impl TlsListener {
    /// Accept on `listener` and hand each connection through `tls`.
    ///
    /// # Errors
    ///
    /// The listener's local address cannot be read.
    pub fn new(listener: TcpListener, tls: ServerTls) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let (sender, connections) = mpsc::channel(HANDSHAKED_BACKLOG);
        let accept_loop = tokio::spawn(accept_loop(listener, tls, sender));
        Ok(Self {
            connections,
            local_addr,
            accept_loop,
        })
    }
}

impl Drop for TlsListener {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.connections.recv().await {
            Some(connection) => connection,
            // The accept loop holds a sender until it is aborted on drop.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

async fn accept_loop(
    listener: TcpListener,
    tls: ServerTls,
    sender: mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(connection) => connection,
            Err(e) => {
                crate::accept::pause_after_error("tls", &e).await;
                continue;
            }
        };
        let tls = tls.clone();
        let sender = sender.clone();
        tokio::spawn(async move {
            // accept counts and logs a failed handshake.
            if let Ok(stream) = tls.accept(stream, Some(peer)).await {
                let _ = sender.send((stream, peer)).await;
            }
        });
    }
}
