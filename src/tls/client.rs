// Project:   scalo
// File:      src/tls/client.rs
// Purpose:   The one client TLS builder: profile, trust choice, CA files, identity and pins
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Client TLS from a profile, a trust choice, CA files, an optional client
//! identity and key pins.
//!
//! [`ClientTls`] is what a transport's [`ClientTlsConfig`] maps onto, and
//! [`build_client_tls`] turns it into a rustls [`ClientConfig`]. reqwest takes
//! it as a preconfigured TLS config, the gRPC transports run it over each
//! stream they dial, and syslog over its own socket.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::ServerCertVerifier;
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use super::TlsError;
use super::pin;
use super::profile::Posture;
use crate::crypto::{CryptoProfile, KeyShare, TlsProfiles};
use crate::tls_config::{ClientTlsConfig, SpkiPin, TrustChoice};

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

/// What a client TLS config is built from.
///
/// The default is the `strong` profile, [`TrustChoice::Native`], no CA file,
/// no client certificate and no pin, with the profile settings read from the
/// cascade when the config is built.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClientTls {
    /// The profile the posture comes from.
    pub profile: CryptoProfile,
    /// Per-profile settings. `None` reads the `tls.profiles` cascade section
    /// when the config is built.
    pub profiles: Option<TlsProfiles>,
    /// The key share sent first.
    pub key_share: KeyShare,
    /// Which certificates the client accepts the server's from.
    pub trust: TrustChoice,
    /// CA PEM files, each holding one certificate or a bundle.
    pub ca_paths: Vec<PathBuf>,
    /// The certificate the client presents, when the server asks for one.
    pub identity: Option<ClientIdentity>,
    /// Pins on the server's public key, checked after its chain verifies.
    /// Empty checks no key.
    pub pin_sha256: Vec<SpkiPin>,
    /// The name the server's certificate is checked against, in place of the
    /// endpoint's host.
    pub server_name: Option<String>,
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

    /// Accept the server's key when it matches `pin`, or any other pin given.
    #[must_use]
    pub fn with_pin(mut self, pin: SpkiPin) -> Self {
        self.pin_sha256.push(pin);
        self
    }

    /// Build on `profile`.
    #[must_use]
    pub fn with_profile(mut self, profile: CryptoProfile) -> Self {
        self.profile = profile;
        self
    }

    /// Take the per-profile settings from `profiles`, not the cascade.
    #[must_use]
    pub fn with_profiles(mut self, profiles: TlsProfiles) -> Self {
        self.profiles = Some(profiles);
        self
    }

    /// Send `key_share` first.
    #[must_use]
    pub fn with_key_share(mut self, key_share: KeyShare) -> Self {
        self.key_share = key_share;
        self
    }

    /// Check the server's certificate against `name`.
    #[must_use]
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// The profile with its settings applied.
    ///
    /// # Errors
    ///
    /// [`TlsError::Profiles`] when the settings come from a cascade section
    /// that does not parse or is refused.
    pub fn posture(&self) -> Result<Posture, TlsError> {
        let profiles = match self.profiles {
            Some(profiles) => profiles,
            None => TlsProfiles::from_cascade().map_err(TlsError::Profiles)?,
        };
        Ok(Posture::new(self.profile, &profiles, self.key_share))
    }
}

impl ClientTlsConfig {
    /// The [`ClientTls`] this section names, with the profile settings read
    /// from the cascade now.
    ///
    /// # Errors
    ///
    /// [`TlsError::Build`] when only one of the client certificate and key is
    /// set, and [`TlsError::Profiles`] for a `tls.profiles` section that does
    /// not parse or is refused.
    pub fn client_tls(&self) -> Result<ClientTls, TlsError> {
        let profiles = TlsProfiles::from_cascade().map_err(TlsError::Profiles)?;
        let mut tls = ClientTls {
            profile: self.profile,
            profiles: Some(profiles),
            key_share: self.key_share,
            trust: self.trust,
            ca_paths: self.ca_paths.clone(),
            identity: None,
            pin_sha256: self.pin_sha256.clone(),
            server_name: self.server_name.clone(),
        };
        if let Some((cert, key)) = self.identity_paths().map_err(TlsError::Build)? {
            tls = tls.with_identity(cert, key);
        }
        Ok(tls)
    }
}

/// Build a rustls [`ClientConfig`] for `tls`.
///
/// Every file is read here, so a missing or wrong file fails the build and
/// not the first handshake.
///
/// # Errors
///
/// [`TlsError::PemRead`] or [`TlsError::NoCertsFound`] for a CA or certificate
/// file, [`TlsError::InvalidPem`] for a key file with no key,
/// [`TlsError::EmptyTrustStore`] when nothing is trusted (`bundle` with no CA
/// file), [`TlsError::Profiles`] for refused profile settings, and
/// [`TlsError::Build`] when the OS verifier cannot start or the key does not
/// match the certificate.
pub fn build_client_tls(tls: &ClientTls) -> Result<Arc<ClientConfig>, TlsError> {
    let posture = tls.posture()?;
    let provider = posture.provider();
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(posture.versions())
        .map_err(|e| TlsError::Build(e.to_string()))?;
    let verifier: Arc<dyn ServerCertVerifier> = match tls.trust {
        TrustChoice::Native => {
            webpki_verifier(root_store(&tls.ca_paths, Roots::WithOs)?, provider)?
        }
        TrustChoice::Bundle => webpki_verifier(root_store(&tls.ca_paths, Roots::Only)?, provider)?,
        TrustChoice::Platform => platform_verifier(&tls.ca_paths, provider)?,
    };
    let builder = builder
        .dangerous()
        .with_custom_certificate_verifier(pin::checked(verifier, &tls.pin_sha256, tls.profile));
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
        profile = posture.profile.as_str(),
        allow_tls12 = posture.allow_tls12,
        trust = %tls.trust,
        ca_files = tls.ca_paths.len(),
        mutual_tls = tls.identity.is_some(),
        key_pins = tls.pin_sha256.len(),
        "TLS client config built"
    );
    Ok(Arc::new(config))
}

/// rustls's WebPKI verifier over `roots`.
fn webpki_verifier(
    roots: RootCertStore,
    provider: Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, TlsError> {
    let verifier = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
        .build()
        .map_err(|e| TlsError::Build(format!("certificate verifier: {e}")))?;
    Ok(verifier)
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
        let (added, _ignored) = store.add_parsable_certificates(read_certs(path)?);
        if added == 0 {
            return Err(TlsError::NoCertsFound { path: path.clone() });
        }
    }
    if store.is_empty() {
        return Err(TlsError::EmptyTrustStore);
    }
    Ok(store)
}

/// Add the OS root store to `store`, warning for each certificate it could not
/// read.
fn add_native_roots(store: &mut RootCertStore) {
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
        cas.extend(read_certs(path)?);
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

/// The certificates parsed from the PEM file at `path`.
///
/// Blocks that are not certificates, and text between blocks, are skipped, so
/// a bundle carrying comments or a key still yields its certificates.
///
/// # Errors
///
/// [`TlsError::PemRead`] when the file cannot be read, and
/// [`TlsError::NoCertsFound`] when it holds no certificate.
pub(super) fn read_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    // Read as bytes: `pem_file_iter` loops forever on a directory (rustls/pki-types#98).
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
    Ok(certs)
}

/// The key parsed from the PEM key file at `path`.
///
/// # Errors
///
/// [`TlsError::PemRead`] when the file cannot be read, and
/// [`TlsError::InvalidPem`] when it holds no private key.
fn read_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let bytes = std::fs::read(path).map_err(|e| TlsError::PemRead {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;
    let key = PrivateKeyDer::from_pem_slice(&bytes).map_err(|e| TlsError::InvalidPem {
        what: "client key",
        reason: format!("{}: {e}", path.display()),
    })?;
    tracing::debug!(path = %path.display(), "TLS: client key read");
    Ok(key)
}

/// The certificate chain and key of `identity`.
fn read_identity(
    identity: &ClientIdentity,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError> {
    let chain = read_certs(&identity.cert_path)?;
    let key = read_key(&identity.key_path)?;
    Ok((chain, key))
}

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
