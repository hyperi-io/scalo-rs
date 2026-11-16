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

use super::posture_provider;
use crate::crypto::PqcMode;

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

impl TestCa {
    pub(crate) fn new(name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.distinguished_name.push(DnType::CommonName, name);
        let issuer = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
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

    fn leaf(&self, name: &str, usage: ExtendedKeyUsagePurpose) -> Leaf {
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
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
    let builder = ClientConfig::builder_with_provider(posture_provider(PqcMode::Prefer))
        .with_safe_default_protocol_versions()
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
