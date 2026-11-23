// Project:   scalo
// File:      src/tls/client.rs
// Purpose:   Client trust choice, CA files and mutual-TLS identity
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Client TLS from a trust choice, CA files and an optional client identity.
//!
//! [`ClientTls`] is what a transport's config maps onto. [`build_client_tls`]
//! turns it into a rustls [`ClientConfig`], and `tonic_client_tls` (the
//! `transport-grpc` feature) into tonic's client config, so every client reads
//! its CA and identity files through the same code.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::WantsClientCert;
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, ConfigBuilder, RootCertStore};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};

use super::{TlsError, posture_provider, protocol_versions};
use crate::crypto::CryptoProfile;

/// Which certificates a client accepts a server's certificate from.
///
/// Every choice trusts the CA files given beside it, and `bundle` trusts
/// nothing else.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum TrustChoice {
    /// The OS root store, read through `rustls-native-certs` and checked by
    /// rustls.
    #[default]
    Native,
    /// The OS verifier, through `rustls-platform-verifier`, so Windows and
    /// macOS apply their own policy and revocation checks.
    Platform,
    /// Only the CA files given: an exclusive pin, with no OS root trusted.
    Bundle,
}

impl TrustChoice {
    /// The name config uses for this choice.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Platform => "platform",
            Self::Bundle => "bundle",
        }
    }
}

impl std::fmt::Display for TrustChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A client certificate chain and its private key, both PEM, for mutual TLS.
///
/// The two paths may name one file holding both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    /// The certificate, followed by any intermediates the server needs.
    pub cert_path: PathBuf,
    /// The private key: PKCS#8, PKCS#1 or SEC1.
    pub key_path: PathBuf,
}

/// The trust and identity a client TLS config is built from.
///
/// The default is [`TrustChoice::Native`] with no CA file and no client
/// certificate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientTls {
    /// Which certificates the client accepts the server's from.
    pub trust: TrustChoice,
    /// CA PEM files, each holding one certificate or a bundle.
    pub ca_paths: Vec<PathBuf>,
    /// The certificate the client presents, when the server asks for one.
    pub identity: Option<ClientIdentity>,
}

impl ClientTls {
    /// `trust` with no CA file and no client certificate.
    #[must_use]
    pub fn new(trust: TrustChoice) -> Self {
        Self {
            trust,
            ..Self::default()
        }
    }

    /// Trust only the CA in `pem_path`.
    #[must_use]
    pub fn pinned(pem_path: impl Into<PathBuf>) -> Self {
        Self::new(TrustChoice::Bundle).with_ca(pem_path)
    }

    /// Trust the CA file at `pem_path` as well.
    #[must_use]
    pub fn with_ca(mut self, pem_path: impl Into<PathBuf>) -> Self {
        self.ca_paths.push(pem_path.into());
        self
    }

    /// Present the certificate at `cert_path` with the key at `key_path`.
    #[must_use]
    pub fn with_identity(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.identity = Some(ClientIdentity {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        });
        self
    }
}

/// The config section a transport reads its client TLS from: a trust choice,
/// one CA file and an optional client certificate.
///
/// ```yaml
/// tls:
///   trust: bundle
///   ca_path: /etc/tls/ca.pem
///   client_cert_path: /etc/tls/client.pem
///   client_key_path: /etc/tls/client.key
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ClientTlsConfig {
    /// `native` (default), `platform` or `bundle`, as in [`TrustChoice`].
    pub trust: TrustChoice,
    /// CA PEM file, trusted beside the OS store under `native` and
    /// `platform`, and alone under `bundle`.
    pub ca_path: Option<PathBuf>,
    /// Client certificate PEM for mutual TLS, with `client_key_path`.
    pub client_cert_path: Option<PathBuf>,
    /// Client private key PEM for mutual TLS, with `client_cert_path`. May
    /// name the same file when it holds both.
    pub client_key_path: Option<PathBuf>,
}

impl ClientTlsConfig {
    /// The [`ClientTls`] this section names.
    ///
    /// # Errors
    ///
    /// [`TlsError::Build`] when only one of the client certificate and key is
    /// set.
    pub fn client_tls(&self) -> Result<ClientTls, TlsError> {
        let mut tls = ClientTls::new(self.trust);
        if let Some(ca) = &self.ca_path {
            tls = tls.with_ca(ca);
        }
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert), Some(key)) => Ok(tls.with_identity(cert, key)),
            (None, None) => Ok(tls),
            _ => Err(TlsError::Build(
                "mutual TLS needs both client_cert_path and client_key_path".to_string(),
            )),
        }
    }
}

/// Build a rustls [`ClientConfig`] for `tls` on `profile`.
///
/// Every file is read here, so a missing or wrong file fails the build and
/// not the first handshake.
///
/// # Errors
///
/// [`TlsError::PemRead`] or [`TlsError::NoCertsFound`] for a CA or certificate
/// file, [`TlsError::InvalidPem`] for a key file with no key,
/// [`TlsError::EmptyTrustStore`] when nothing is trusted (`bundle` with no CA
/// file), and [`TlsError::Build`] when the OS verifier cannot start or the key
/// does not match the certificate.
pub fn build_client_tls(
    profile: CryptoProfile,
    tls: &ClientTls,
) -> Result<Arc<ClientConfig>, TlsError> {
    let provider = posture_provider(profile.pqc());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(protocol_versions(profile.tls_floor()))
        .map_err(|e| TlsError::Build(e.to_string()))?;
    let builder: ConfigBuilder<ClientConfig, WantsClientCert> = match tls.trust {
        TrustChoice::Native => {
            builder.with_root_certificates(root_store(&tls.ca_paths, Roots::WithOs)?)
        }
        TrustChoice::Bundle => {
            builder.with_root_certificates(root_store(&tls.ca_paths, Roots::Only)?)
        }
        TrustChoice::Platform => builder
            .dangerous()
            .with_custom_certificate_verifier(platform_verifier(&tls.ca_paths, provider)?),
    };
    let config = match &tls.identity {
        Some(identity) => {
            let (chain, key) = read_identity(identity)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| TlsError::Build(format!("client certificate: {e}")))?
        }
        None => builder.with_no_client_auth(),
    };
    tracing::info!(
        trust = %tls.trust,
        ca_files = tls.ca_paths.len(),
        mutual_tls = tls.identity.is_some(),
        profile = ?profile,
        "TLS client config built"
    );
    Ok(Arc::new(config))
}

/// Whether a root store holds the OS roots beside the CA files.
#[derive(Debug, Clone, Copy)]
enum Roots {
    WithOs,
    Only,
}

/// A root store of the CA files at `paths`, with the OS roots for
/// [`Roots::WithOs`].
fn root_store(paths: &[PathBuf], roots: Roots) -> Result<RootCertStore, TlsError> {
    let mut store = RootCertStore::empty();
    if matches!(roots, Roots::WithOs) {
        add_native_roots(&mut store);
    }
    for path in paths {
        super::add_pem_file_certs(&mut store, path)?;
    }
    if store.is_empty() {
        return Err(TlsError::EmptyTrustStore);
    }
    Ok(store)
}

/// Add the OS root store to `store`, warning for each certificate it could not
/// read.
pub(super) fn add_native_roots(store: &mut RootCertStore) {
    let result = rustls_native_certs::load_native_certs();
    let (added, ignored) = store.add_parsable_certificates(result.certs);
    for err in result.errors {
        tracing::warn!(error = %err, "TLS: error loading a native root (continuing)");
    }
    tracing::debug!(added, ignored, "TLS: OS root store read");
}

/// The OS verifier, trusting the CA files at `paths` beside the OS store.
fn platform_verifier(
    paths: &[PathBuf],
    provider: Arc<CryptoProvider>,
) -> Result<Arc<rustls_platform_verifier::Verifier>, TlsError> {
    let mut cas = Vec::new();
    for path in paths {
        cas.extend(read_certs(path)?.1);
    }
    let build = |e: rustls::Error| TlsError::Build(format!("OS certificate verifier: {e}"));
    let verifier = if cas.is_empty() {
        rustls_platform_verifier::Verifier::new(provider).map_err(build)?
    } else {
        platform_verifier_with_roots(cas, provider).map_err(build)?
    };
    Ok(Arc::new(verifier))
}

#[cfg(any(all(unix, not(target_os = "android")), windows))]
fn platform_verifier_with_roots(
    cas: Vec<CertificateDer<'static>>,
    provider: Arc<CryptoProvider>,
) -> Result<rustls_platform_verifier::Verifier, rustls::Error> {
    rustls_platform_verifier::Verifier::new_with_extra_roots(cas, provider)
}

/// `rustls-platform-verifier` takes no extra roots on this OS.
#[cfg(not(any(all(unix, not(target_os = "android")), windows)))]
fn platform_verifier_with_roots(
    _cas: Vec<CertificateDer<'static>>,
    _provider: Arc<CryptoProvider>,
) -> Result<rustls_platform_verifier::Verifier, rustls::Error> {
    Err(rustls::Error::General(
        "this OS's verifier takes no CA file: use `native` or `bundle`".to_string(),
    ))
}

/// The bytes of the PEM file at `path` and the certificates parsed from it.
///
/// Blocks that are not certificates, and text between blocks, are skipped, so
/// a bundle carrying comments or a key still yields its certificates.
///
/// # Errors
///
/// [`TlsError::PemRead`] when the file cannot be read, and
/// [`TlsError::NoCertsFound`] when it holds no certificate.
pub(super) fn read_certs(path: &Path) -> Result<(Vec<u8>, Vec<CertificateDer<'static>>), TlsError> {
    let bytes = std::fs::read(path).map_err(|e| TlsError::PemRead {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;
    let mut certs = Vec::new();
    for block in CertificateDer::pem_slice_iter(&bytes) {
        match block {
            Ok(cert) => certs.push(cert.into_owned()),
            Err(e) => {
                tracing::trace!(path = %path.display(), error = %e, "TLS: PEM block skipped");
            }
        }
    }
    if certs.is_empty() {
        return Err(TlsError::NoCertsFound {
            path: path.to_path_buf(),
        });
    }
    tracing::debug!(path = %path.display(), certs = certs.len(), "TLS: certificate file read");
    Ok((bytes, certs))
}

/// The bytes of the PEM key file at `path` and the key parsed from it.
///
/// # Errors
///
/// [`TlsError::PemRead`] when the file cannot be read, and
/// [`TlsError::InvalidPem`] when it holds no private key.
pub(super) fn read_key(path: &Path) -> Result<(Vec<u8>, PrivateKeyDer<'static>), TlsError> {
    let bytes = std::fs::read(path).map_err(|e| TlsError::PemRead {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;
    let key = PrivateKeyDer::from_pem_slice(&bytes).map_err(|e| TlsError::InvalidPem {
        what: "client key",
        reason: format!("{}: {e}", path.display()),
    })?;
    tracing::debug!(path = %path.display(), "TLS: client key read");
    Ok((bytes, key))
}

/// The certificate chain and key of `identity`.
fn read_identity(
    identity: &ClientIdentity,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError> {
    let (_, chain) = read_certs(&identity.cert_path)?;
    let (_, key) = read_key(&identity.key_path)?;
    Ok((chain, key))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
