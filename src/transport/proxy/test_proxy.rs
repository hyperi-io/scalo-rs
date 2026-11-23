// Project:   scalo
// File:      src/transport/proxy/test_proxy.rs
// Purpose:   An in-test HTTP CONNECT proxy and a TLS front for transport proxy tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What a [`ConnectProxy`] answers to CONNECT.
#[derive(Debug, Clone)]
pub(crate) enum Policy {
    /// Open every tunnel.
    Allow,
    /// Answer every CONNECT with this status.
    Refuse(u16),
    /// Open tunnels to this port only, and answer 403 to the rest.
    OnlyPort(u16),
    /// Open tunnels when `Proxy-Authorization` is this value, else 407.
    Auth(String),
}

/// One CONNECT a [`ConnectProxy`] read.
#[derive(Debug, Clone)]
pub(crate) struct Connect {
    /// The request target, `host:port`.
    pub target: String,
    /// The `Proxy-Authorization` header.
    pub auth: Option<String>,
}

/// A loopback HTTP CONNECT proxy that records each CONNECT.
pub(crate) struct ConnectProxy {
    pub addr: SocketAddr,
    seen: Arc<Mutex<Vec<Connect>>>,
}

impl ConnectProxy {
    pub(crate) async fn start(policy: Policy) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, policy.clone(), Arc::clone(&log)));
            }
        });
        Self { addr, seen }
    }

    /// `http://127.0.0.1:<port>`.
    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The URL with `user:password@`.
    pub(crate) fn url_with(&self, user: &str, password: &str) -> String {
        format!("http://{user}:{password}@{}", self.addr)
    }

    pub(crate) fn seen(&self) -> Vec<Connect> {
        self.seen.lock().unwrap().clone()
    }
}

async fn serve(mut client: TcpStream, policy: Policy, log: Arc<Mutex<Vec<Connect>>>) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let Ok(byte) = client.read_u8().await else {
            return;
        };
        head.push(byte);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let mut lines = head.split("\r\n");
    let mut request = lines.next().unwrap_or_default().split(' ');
    let (method, target) = (request.next().unwrap_or_default(), request.next());
    let auth = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("proxy-authorization"))
        .map(|(_, value)| value.trim().to_string());
    let Some(target) = target.filter(|_| method == "CONNECT") else {
        let _ = client
            .write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            .await;
        return;
    };
    log.lock().unwrap().push(Connect {
        target: target.to_string(),
        auth: auth.clone(),
    });
    let port: u16 = target
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or_default();
    let refusal = match &policy {
        Policy::Allow => None,
        Policy::Refuse(status) => Some(*status),
        Policy::OnlyPort(only) => (port != *only).then_some(403),
        Policy::Auth(wanted) => (auth.as_deref() != Some(wanted.as_str())).then_some(407),
    };
    if let Some(status) = refusal {
        let answer = format!(
            "HTTP/1.1 {status} Refused\r\nProxy-Authenticate: Basic realm=\"test\"\r\n\
             content-length: 0\r\n\r\n"
        );
        let _ = client.write_all(answer.as_bytes()).await;
        return;
    }
    let Ok(mut upstream) = TcpStream::connect(target).await else {
        let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
        return;
    };
    if client
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await
        .is_err()
    {
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
}

#[cfg(feature = "tls")]
/// A TLS listener for `localhost` that relays each connection's plain bytes
/// to `upstream`, offering `alpn`. Returns its address and the CA PEM to trust.
pub(crate) async fn tls_front(upstream: SocketAddr, alpn: &[&[u8]]) -> (SocketAddr, String) {
    use rustls_pki_types::PrivateKeyDer;
    use rustls_pki_types::pem::PemObject;

    let ca = crate::tls::test_pki::TestCa::new("proxy test CA");
    let leaf = ca.server("tls front");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der.clone()],
            PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap(),
        )
        .unwrap();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let Ok(mut plain) = TcpStream::connect(upstream).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
            });
        }
    });
    (addr, ca.pem())
}

/// A loopback server that echoes every byte back.
pub(crate) async fn echo() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });
    addr
}
