// Project:   scalo
// File:      src/transport/proxy/test_front.rs
// Purpose:   Listeners in front of a test server, for reaching it through the Squid container
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::SocketAddr;
#[cfg(all(
    feature = "tls",
    any(
        feature = "transport-http",
        all(feature = "transport-proxy-test-support", unix)
    )
))]
use std::sync::Arc;

use tokio::net::TcpListener;

#[cfg(all(
    feature = "tls",
    any(
        feature = "transport-http",
        all(feature = "transport-proxy-test-support", unix)
    )
))]
/// A TLS listener on every interface, for `localhost` and the Squid target
/// name, that relays each connection's plain bytes to `upstream`, offering
/// `alpn`. Returns its address and the CA PEM to trust.
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

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let Ok(mut plain) = tokio::net::TcpStream::connect(upstream).await else {
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

#[cfg(all(feature = "transport-proxy-test-support", unix))]
/// A listener on every interface that relays each connection's bytes to
/// `upstream`, so the Squid container can dial a server bound to loopback.
pub(crate) async fn plain_front(upstream: SocketAddr) -> SocketAddr {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut onward) = tokio::net::TcpStream::connect(upstream).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut onward).await;
            });
        }
    });
    addr
}
