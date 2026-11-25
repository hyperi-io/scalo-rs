// Project:   scalo
// File:      src/tls/test_pki.rs
// Purpose:   Test CA, leaf certificates and a TLS client for server TLS tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A throwaway CA and the client side of a real handshake.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use super::profile::Posture;
use crate::crypto::{CryptoProfile, KeyShare, TlsProfiles};

/// A fresh ECDSA P-384 key, the key every profile verifies, cnsa2 included.
fn p384_key() -> KeyPair {
    KeyPair::generate_for(&rcgen::PKCS_ECDSA_P384_SHA384).unwrap()
}

/// A self-signed CA that signs leaves.
pub(crate) struct TestCa {
    issuer: CertifiedIssuer<'static, KeyPair>,
}

/// A certificate and key signed by a [`TestCa`].
pub(crate) struct Leaf {
    pub cert_pem: String,
    pub key_pem: String,
    pub der: CertificateDer<'static>,
}

impl Leaf {
    /// The pin an operator writes for this leaf, worked out from its key and
    /// not from the certificate.
    pub(crate) fn pin(&self) -> super::SpkiPin {
        use base64::Engine as _;
        use rcgen::PublicKeyData as _;
        use sha2::Digest as _;

        let spki = KeyPair::from_pem(&self.key_pem)
            .unwrap()
            .subject_public_key_info();
        base64::engine::general_purpose::STANDARD
            .encode(sha2::Sha256::digest(&spki))
            .parse()
            .unwrap()
    }
}

impl TestCa {
    pub(crate) fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.distinguished_name.push(DnType::CommonName, name);
        let issuer = CertifiedIssuer::self_signed(params, p384_key()).unwrap();
        Self { issuer }
    }

    pub(crate) fn pem(&self) -> String {
        self.issuer.pem()
    }

    /// A `localhost` server certificate named `name`.
    pub(crate) fn server(&self, name: &str) -> Leaf {
        self.leaf(name, ExtendedKeyUsagePurpose::ServerAuth)
    }

    /// A client certificate named `name`.
    pub(crate) fn client(&self, name: &str) -> Leaf {
        self.leaf(name, ExtendedKeyUsagePurpose::ClientAuth)
    }

    /// A `localhost` server certificate on a P-256 key, which cnsa2 refuses.
    pub(crate) fn server_p256(&self, name: &str) -> Leaf {
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        self.leaf_on(name, ExtendedKeyUsagePurpose::ServerAuth, key)
    }

    fn leaf(&self, name: &str, usage: ExtendedKeyUsagePurpose) -> Leaf {
        self.leaf_on(name, usage, p384_key())
    }

    fn leaf_on(&self, name: &str, usage: ExtendedKeyUsagePurpose, key: KeyPair) -> Leaf {
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "target.test".to_string()])
                .unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![usage];
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            der: cert.der().clone(),
        }
    }
}

/// Replace `path` in one rename, so a reader never sees half a file.
pub(crate) fn write(path: &Path, contents: &str) {
    let staged: PathBuf = path.with_extension("staged");
    std::fs::write(&staged, contents).unwrap();
    std::fs::rename(&staged, path).unwrap();
}

/// A client trusting `ca_pem`, presenting `identity` when given, offering
/// `alpn`.
pub(crate) fn connector(ca_pem: &str, identity: Option<&Leaf>, alpn: &[&[u8]]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let posture = Posture::new(
        CryptoProfile::Strong,
        &TlsProfiles::default(),
        KeyShare::Profile,
    );
    let builder = ClientConfig::builder_with_provider(posture.provider())
        .with_protocol_versions(posture.versions())
        .unwrap()
        .with_root_certificates(roots);
    let mut config = match identity {
        Some(leaf) => builder
            .with_client_auth_cert(
                vec![leaf.der.clone()],
                PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    TlsConnector::from(Arc::new(config))
}

/// Drive an in-memory handshake to completion, returning the first rustls
/// error either side raises.
///
/// Running out of rounds is an error, so a stalled handshake never reads as a
/// completed one.
pub(crate) fn pump(
    client: &mut rustls::ClientConnection,
    server: &mut rustls::ServerConnection,
) -> Result<(), rustls::Error> {
    for _ in 0..40 {
        let mut c2s: Vec<u8> = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut c2s).unwrap();
        }
        let mut rd: &[u8] = &c2s;
        while !rd.is_empty() {
            server.read_tls(&mut rd).unwrap();
            server.process_new_packets()?;
        }
        let mut s2c: Vec<u8> = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut s2c).unwrap();
        }
        let mut rd2: &[u8] = &s2c;
        while !rd2.is_empty() {
            client.read_tls(&mut rd2).unwrap();
            client.process_new_packets()?;
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return Ok(());
        }
    }
    Err(rustls::Error::General(
        "handshake did not complete within 40 pump rounds".to_string(),
    ))
}

/// An HTTPS server on localhost that requires a client certificate from `ca`,
/// presents `leaf`, reads each request whole and answers it 200 with no body.
#[cfg(any(feature = "transport-http", feature = "transport-otlp"))]
pub(crate) async fn mtls_ok_server(ca: &TestCa, leaf: &Leaf) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ca.pem().as_bytes()).unwrap())
        .unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::clone(&provider),
    )
    .build()
    .unwrap();
    let key = PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap();
    let server = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![leaf.der.clone()], key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let mut stream = match acceptor.accept(stream).into_fallible().await {
                    Ok(stream) => stream,
                    // Read until the client closes, so the alert the handshake sent
                    // reaches it ahead of a reset.
                    Err((_, mut tcp)) => {
                        let mut buf = [0u8; 4096];
                        let drain =
                            async { while matches!(tcp.read(&mut buf).await, Ok(n) if n > 0) {} };
                        let _ =
                            tokio::time::timeout(std::time::Duration::from_secs(5), drain).await;
                        return;
                    }
                };
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                // The whole body is read, so closing leaves nothing unread to reset.
                while !request_complete(&request) {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let response = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
                let _ = stream.write_all(response).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    addr
}

/// Whether `request` holds its headers and the `content-length` bytes after them.
#[cfg(any(feature = "transport-http", feature = "transport-otlp"))]
fn request_complete(request: &[u8]) -> bool {
    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let head = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
    let length = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    request.len() >= end + 4 + length
}

/// What one handshake agreed, read on the server side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Agreed {
    pub version: Option<rustls::ProtocolVersion>,
    pub suite: Option<rustls::CipherSuite>,
    pub group: Option<rustls::NamedGroup>,
}

impl Agreed {
    /// TLS 1.3 with `suite` and `group`.
    pub(crate) fn tls13(suite: rustls::CipherSuite, group: rustls::NamedGroup) -> Self {
        Self {
            version: Some(rustls::ProtocolVersion::TLSv1_3),
            suite: Some(suite),
            group: Some(group),
        }
    }

    pub(crate) fn of(connection: &rustls::CommonState) -> Self {
        Self {
            version: connection.protocol_version(),
            suite: connection.negotiated_cipher_suite().map(|s| s.suite()),
            group: connection.negotiated_key_exchange_group().map(|g| g.name()),
        }
    }
}

/// A `ServerTls` on `profile` serving a `localhost` certificate from `ca`,
/// offering `alpn`, on the default profile settings.
pub(crate) async fn server_tls(
    ca: &TestCa,
    profile: CryptoProfile,
    alpn: &[&[u8]],
) -> (super::ServerTls, tempfile::TempDir) {
    server_tls_with(ca, profile, alpn, super::ClientAuth::None).await
}

/// [`server_tls`] asking clients for a certificate from `ca` under
/// `client_auth`.
pub(crate) async fn server_tls_with(
    ca: &TestCa,
    profile: CryptoProfile,
    alpn: &[&[u8]],
    client_auth: super::ClientAuth,
) -> (super::ServerTls, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let leaf = ca.server("front");
    let (cert, key, ca_file) = (
        dir.path().join("cert.pem"),
        dir.path().join("key.pem"),
        dir.path().join("ca.pem"),
    );
    std::fs::write(&cert, &leaf.cert_pem).unwrap();
    std::fs::write(&key, &leaf.key_pem).unwrap();
    std::fs::write(&ca_file, ca.pem()).unwrap();
    let config = super::ServerTlsConfig {
        profile,
        client_auth,
        ca_files: if client_auth == super::ClientAuth::None {
            Vec::new()
        } else {
            vec![ca_file]
        },
        reload_interval_secs: 0,
        ..super::ServerTlsConfig::from_files(cert, key)
    };
    let options = super::ServerTlsOptions {
        alpn_protocols: alpn.iter().map(|p| p.to_vec()).collect(),
        profiles: Some(TlsProfiles::default()),
    };
    let tls = super::ServerTls::load_with(&config, options).await.unwrap();
    (tls, dir)
}

/// A TLS listener on localhost in front of `upstream`: each connection's
/// handshake runs on `tls` and is noted, then its plain bytes are relayed.
pub(crate) struct Front {
    pub addr: SocketAddr,
    agreed: Arc<std::sync::Mutex<Vec<Agreed>>>,
}

impl Front {
    pub(crate) async fn start(tls: super::ServerTls, upstream: SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let agreed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let noted = Arc::clone(&agreed);
        tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                let tls = tls.clone();
                let noted = Arc::clone(&noted);
                tokio::spawn(async move {
                    let Ok(mut stream) = tls.accept(stream, Some(peer)).await else {
                        return;
                    };
                    noted.lock().unwrap().push(Agreed::of(stream.get_ref().1));
                    let Ok(mut plain) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut stream, &mut plain).await;
                });
            }
        });
        Self { addr, agreed }
    }

    /// What each handshake so far agreed.
    pub(crate) fn agreed(&self) -> Vec<Agreed> {
        self.agreed.lock().unwrap().clone()
    }
}

/// Connect to `addr` and run the client handshake for `localhost`.
pub(crate) async fn connect(
    connector: &TlsConnector,
    addr: SocketAddr,
) -> std::io::Result<TlsStream<TcpStream>> {
    let stream = TcpStream::connect(addr).await?;
    connector
        .connect(ServerName::try_from("localhost").unwrap(), stream)
        .await
}
