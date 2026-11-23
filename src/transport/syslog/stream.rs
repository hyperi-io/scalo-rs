// Project:   scalo
// File:      src/transport/syslog/stream.rs
// Purpose:   TCP and TLS connections to a syslog receiver
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::io;
use std::sync::Arc;
use std::time::Duration;

use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// A connection that can be written to and checked for a close by the peer.
pub(super) trait Connection: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> Connection for T {}

pub(super) type Stream = Box<dyn Connection>;

/// Why a connection could not be made.
#[derive(Debug)]
pub(super) enum ConnectError {
    /// The receiver could not be reached in time, or the handshake broke off:
    /// try again later.
    Unreachable(io::Error),
    /// The TLS handshake refused the receiver's certificate or ours: no retry
    /// fixes it.
    Trust(io::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(e) => write!(f, "unreachable: {e}"),
            Self::Trust(e) => write!(f, "TLS refused: {e}"),
        }
    }
}

/// Opens connections to one receiver.
pub(super) struct Connector {
    address: String,
    timeout: Duration,
    tls: Option<(TlsConnector, ServerName<'static>)>,
}

impl Connector {
    pub(super) fn new(
        address: String,
        timeout: Duration,
        tls: Option<(Arc<rustls::ClientConfig>, ServerName<'static>)>,
    ) -> Self {
        Self {
            address,
            timeout,
            tls: tls.map(|(config, name)| (TlsConnector::from(config), name)),
        }
    }

    /// The connect timeout, which also bounds a RELP open.
    pub(super) fn timeout(&self) -> Duration {
        self.timeout
    }

    /// A new connection, its TLS handshake done, within the connect timeout.
    pub(super) async fn connect(&self) -> Result<Stream, ConnectError> {
        let connecting = async {
            let tcp = TcpStream::connect(&self.address)
                .await
                .map_err(ConnectError::Unreachable)?;
            tcp.set_nodelay(true).map_err(ConnectError::Unreachable)?;
            let stream: Stream = match &self.tls {
                None => Box::new(tcp),
                Some((connector, name)) => Box::new(
                    connector
                        .connect(name.clone(), tcp)
                        .await
                        .map_err(classify_handshake)?,
                ),
            };
            Ok(stream)
        };
        match tokio::time::timeout(self.timeout, connecting).await {
            Ok(result) => result,
            Err(_) => Err(ConnectError::Unreachable(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no connection within {} ms", self.timeout.as_millis()),
            ))),
        }
    }
}

/// A rustls refusal (bad certificate, unknown CA, wrong name) is a trust
/// error. tokio-rustls reports one as `InvalidData` wrapping the rustls error.
fn classify_handshake(e: io::Error) -> ConnectError {
    let refused = e.kind() == io::ErrorKind::InvalidData
        && e.get_ref().is_some_and(|inner| inner.is::<rustls::Error>());
    if refused {
        ConnectError::Trust(e)
    } else {
        ConnectError::Unreachable(e)
    }
}

/// What a check of an idle connection found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Idle {
    /// Nothing to read: the connection is open.
    Open,
    /// The peer closed or reset it.
    Closed,
    /// The peer sent bytes.
    Data,
}

/// Check an idle connection without waiting. A read that would wait means
/// the connection is open.
pub(super) async fn check_idle(stream: &mut Stream) -> Idle {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(Duration::ZERO, stream.read(&mut byte)).await {
        Err(_) => Idle::Open,
        Ok(Ok(0) | Err(_)) => Idle::Closed,
        Ok(Ok(_)) => Idle::Data,
    }
}
