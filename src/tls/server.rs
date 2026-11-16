// Project:   scalo
// File:      src/tls/server.rs
// Purpose:   Server TLS: certificate reload from file or secret, shared accept helper
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Server TLS with certificate reload.
//!
//! [`ServerTls`] reads a certificate, its key and an optional client CA from
//! files or secret specs and builds a rustls [`ServerConfig`] on the
//! [`CryptoProfile`] posture. The certificate sits in a [`CertResolver`] that
//! every handshake asks, so swapping it reaches every listener built from the
//! same `ServerTls` on its next handshake. A changed client CA rebuilds the
//! config, which [`ServerTls::acceptor`] and [`ServerTls::accept`] pick up per
//! connection.
//!
//! Sources are polled every `reload_interval_secs`. Polling sees a Kubernetes
//! Secret volume's symlink swap and a rotated secret alike. A source that fails
//! to read or parse, or a key that does not match its certificate, keeps the
//! certificate in service. The failure is logged once, and again only when it
//! changes.

use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::crypto::CryptoProvider;
use rustls::server::danger::ClientCertVerifier;
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{NamedGroup, ProtocolVersion, RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use super::{TlsError, posture_provider, protocol_versions};
use crate::crypto::CryptoProfile;

/// Whether a server asks clients for a certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuth {
    /// No client certificate is asked for.
    #[default]
    None,
    /// A client certificate is checked against the CA when one is presented.
    #[serde(alias = "request")]
    Optional,
    /// Every client presents a certificate that chains to the CA.
    #[serde(alias = "require")]
    Required,
}

impl fmt::Display for ClientAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Optional => "optional",
            Self::Required => "required",
        })
    }
}

/// Where a server's certificate, key and client CA come from.
///
/// Each of the three takes a `*_file` path or a `*_secret` spec, never both. A
/// spec is anything [`crate::secrets::resolve`] reads (`vault:`, `file:`,
/// `aws:`, `env:`) and needs the `secrets` feature.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerTlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert_file: Option<PathBuf>,
    /// Secret spec for the PEM certificate chain.
    pub cert_secret: Option<String>,
    /// PEM private key.
    pub key_file: Option<PathBuf>,
    /// Secret spec for the PEM private key.
    pub key_secret: Option<String>,
    /// PEM CA bundle that client certificates must chain to.
    pub ca_file: Option<PathBuf>,
    /// Secret spec for the PEM CA bundle.
    pub ca_secret: Option<String>,
    /// Whether clients are asked for a certificate. Default `none`.
    pub client_auth: ClientAuth,
    /// How often the sources are re-read, in seconds. `0` turns reload off.
    /// Default 30.
    pub reload_interval_secs: u64,
    /// How long a client has to finish its handshake, in milliseconds.
    /// Default 10,000.
    pub handshake_timeout_ms: u64,
}

impl Default for ServerTlsConfig {
    fn default() -> Self {
        Self {
            cert_file: None,
            cert_secret: None,
            key_file: None,
            key_secret: None,
            ca_file: None,
            ca_secret: None,
            client_auth: ClientAuth::None,
            reload_interval_secs: 30,
            handshake_timeout_ms: 10_000,
        }
    }
}

impl fmt::Debug for ServerTlsConfig {
    /// Secret specs print as set or unset: a literal spec is the secret.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let set = |spec: &Option<String>| spec.as_ref().map(|_| "<set>");
        f.debug_struct("ServerTlsConfig")
            .field("cert_file", &self.cert_file)
            .field("cert_secret", &set(&self.cert_secret))
            .field("key_file", &self.key_file)
            .field("key_secret", &set(&self.key_secret))
            .field("ca_file", &self.ca_file)
            .field("ca_secret", &set(&self.ca_secret))
            .field("client_auth", &self.client_auth)
            .field("reload_interval_secs", &self.reload_interval_secs)
            .field("handshake_timeout_ms", &self.handshake_timeout_ms)
            .finish()
    }
}

impl ServerTlsConfig {
    /// A config serving the certificate and key at these paths.
    #[must_use]
    pub fn from_files(cert_file: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            cert_file: Some(cert_file.into()),
            key_file: Some(key_file.into()),
            ..Self::default()
        }
    }
}

/// Code-side choices for a [`ServerTls`], as opposed to deployment config.
#[derive(Debug, Clone, Default)]
pub struct ServerTlsOptions {
    /// Protocol versions and key-exchange groups. Default [`CryptoProfile::Prod`].
    pub profile: CryptoProfile,
    /// ALPN protocols offered, most preferred first. Empty offers none.
    pub alpn_protocols: Vec<Vec<u8>>,
}

impl ServerTlsOptions {
    /// Options for an HTTP listener: ALPN `h2` then `http/1.1`.
    #[must_use]
    pub fn http() -> Self {
        Self {
            profile: CryptoProfile::default(),
            alpn_protocols: vec![b"h2".to_vec(), b"http/1.1".to_vec()],
        }
    }

    /// Replace the crypto profile.
    #[must_use]
    pub fn with_profile(mut self, profile: CryptoProfile) -> Self {
        self.profile = profile;
        self
    }
}

/// What a [`ServerTls::reload`] changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Reloaded {
    /// The certificate and key in service were replaced.
    pub certificate: bool,
    /// The client CA was replaced.
    pub client_ca: bool,
}

/// Why a TLS handshake did not complete.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    /// The client did not finish within the handshake timeout.
    #[error("TLS handshake did not finish within {0:?}")]
    Timeout(Duration),
    /// The handshake failed: no common protocol, a refused client
    /// certificate, a reset connection.
    #[error("TLS handshake failed: {0}")]
    Failed(#[source] std::io::Error),
}

/// The certificate in service, swapped in place by a reload.
///
/// rustls asks it for the certificate on every handshake, so a `ServerConfig`
/// built on it serves a reloaded certificate without being rebuilt.
pub struct CertResolver {
    current: ArcSwap<CertifiedKey>,
}

impl CertResolver {
    fn new(key: CertifiedKey) -> Self {
        Self {
            current: ArcSwap::from_pointee(key),
        }
    }

    /// The certificate and key in service.
    #[must_use]
    pub fn current(&self) -> Arc<CertifiedKey> {
        self.current.load_full()
    }
}

impl fmt::Debug for CertResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertResolver")
            .field("chain_len", &self.current.load().cert.len())
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for CertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current.load_full())
    }
}

/// Server TLS whose certificate reloads from its sources.
///
/// Clones share one certificate, one config and one reload task. The task
/// stops once the last clone is dropped.
#[derive(Clone)]
pub struct ServerTls {
    inner: Arc<Inner>,
}

struct Inner {
    config: ServerTlsConfig,
    options: ServerTlsOptions,
    provider: Arc<CryptoProvider>,
    resolver: Arc<CertResolver>,
    server_config: ArcSwap<ServerConfig>,
    state: Mutex<ReloadState>,
}

/// What the last successful load read, and the reload failure being reported.
struct ReloadState {
    identity: u64,
    client_ca: u64,
    failure: Option<String>,
}

impl fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerTls")
            .field("config", &self.inner.config)
            .field("profile", &self.inner.options.profile)
            .finish_non_exhaustive()
    }
}

impl ServerTls {
    /// Load with the default [`ServerTlsOptions`].
    ///
    /// # Errors
    ///
    /// As [`load_with`](Self::load_with).
    pub async fn load(config: &ServerTlsConfig) -> Result<Self, TlsError> {
        Self::load_with(config, ServerTlsOptions::default()).await
    }

    /// Read the sources, build the config, and start the reload task when
    /// `reload_interval_secs` is above zero. Call inside a tokio runtime.
    ///
    /// # Errors
    ///
    /// [`TlsError::MissingSource`] or [`TlsError::ConflictingSource`] for a
    /// certificate, key or CA set up wrongly, [`TlsError::ClientCaRequired`]
    /// when client certificates are asked for with no CA, a read or parse
    /// error for any source, and [`TlsError::Build`] for a key that does not
    /// match its certificate.
    pub async fn load_with(
        config: &ServerTlsConfig,
        options: ServerTlsOptions,
    ) -> Result<Self, TlsError> {
        let sources = Sources::of(config)?;
        if config.client_auth == ClientAuth::None
            && (config.ca_file.is_some() || config.ca_secret.is_some())
        {
            tracing::warn!("TLS client CA is set but client_auth is none, so it is unused");
        }
        let material = sources.read().await?;
        let provider = posture_provider(options.profile.pqc());

        let key = certified_key(&material.cert, &material.key, &provider)?;
        let resolver = Arc::new(CertResolver::new(key));
        let verifier = client_verifier(config.client_auth, material.ca.as_deref(), &provider)?;
        let server_config = build_server_config(&options, &provider, verifier, &resolver)?;

        let tls = Self {
            inner: Arc::new(Inner {
                config: config.clone(),
                options,
                provider,
                resolver,
                server_config: ArcSwap::from_pointee(server_config),
                state: Mutex::new(ReloadState {
                    identity: material.identity(),
                    client_ca: material.client_ca(),
                    failure: None,
                }),
            }),
        };
        tls.spawn_reload();
        Ok(tls)
    }

    /// The config in service. Its certificate reloads in place. A client CA
    /// change replaces the config, so hold this only as long as one handshake.
    #[must_use]
    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.inner.server_config.load_full()
    }

    /// The certificate resolver every config from this `ServerTls` uses.
    #[must_use]
    pub fn resolver(&self) -> Arc<CertResolver> {
        Arc::clone(&self.inner.resolver)
    }

    /// An acceptor on the config in service, for a caller running its own
    /// handshake. Take one per connection.
    #[must_use]
    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config())
    }

    /// How long [`accept`](Self::accept) gives a client to finish.
    #[must_use]
    pub fn handshake_timeout(&self) -> Duration {
        Duration::from_millis(self.inner.config.handshake_timeout_ms)
    }

    /// Run the server handshake on `io`, bounded by the handshake timeout.
    ///
    /// Counts `tls_handshakes_total{protocol, key_exchange}` on success and
    /// `tls_handshake_failures_total{reason}` on failure. `peer` goes into the
    /// failure's log and security event.
    ///
    /// # Errors
    ///
    /// [`HandshakeError::Timeout`] when the client is too slow, and
    /// [`HandshakeError::Failed`] for any other failure.
    pub async fn accept<IO>(
        &self,
        io: IO,
        peer: Option<SocketAddr>,
    ) -> Result<TlsStream<IO>, HandshakeError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let limit = self.handshake_timeout();
        match tokio::time::timeout(limit, self.acceptor().accept(io)).await {
            Ok(Ok(stream)) => {
                let (_, connection) = stream.get_ref();
                record_handshake(
                    connection.protocol_version(),
                    connection.negotiated_key_exchange_group().map(|g| g.name()),
                );
                Ok(stream)
            }
            Ok(Err(e)) => {
                record_handshake_failure("error", peer, &e.to_string());
                Err(HandshakeError::Failed(e))
            }
            Err(_) => {
                record_handshake_failure("timeout", peer, "handshake_timeout");
                Err(HandshakeError::Timeout(limit))
            }
        }
    }

    /// Re-read the sources and swap in whatever changed.
    ///
    /// A new certificate and a new CA are both built before either is
    /// swapped, so a failure changes nothing. The reload task calls this, and
    /// a caller with its own trigger (a secrets rotation event, a signal) can
    /// too. Concurrent calls run one at a time.
    ///
    /// # Errors
    ///
    /// Any read, parse or build error. The certificate and CA in service stay.
    pub async fn reload(&self) -> Result<Reloaded, TlsError> {
        let mut state = self.inner.state.lock().await;
        let result = self.inner.try_reload(&mut state).await;
        match &result {
            Ok(reloaded) => {
                if state.failure.take().is_some() {
                    tracing::info!("TLS certificate sources readable again");
                }
                if reloaded.certificate || reloaded.client_ca {
                    tracing::info!(
                        certificate = reloaded.certificate,
                        client_ca = reloaded.client_ca,
                        "TLS certificate reloaded"
                    );
                    record_reload("success");
                    #[cfg(feature = "logger")]
                    crate::logger::security::tls_event(
                        "cert_reload",
                        crate::logger::security::SecurityOutcome::Success,
                        None,
                        None,
                    );
                }
            }
            Err(e) => {
                let reason = e.to_string();
                if state.failure.as_deref() != Some(reason.as_str()) {
                    tracing::warn!(
                        error = %reason,
                        "TLS certificate reload failed, keeping the certificate in service"
                    );
                    #[cfg(feature = "logger")]
                    crate::logger::security::tls_event(
                        "cert_reload",
                        crate::logger::security::SecurityOutcome::Failure,
                        Some(&reason),
                        None,
                    );
                    state.failure = Some(reason);
                }
                record_reload("failure");
            }
        }
        result
    }

    /// Poll the sources every `reload_interval_secs` while a clone is alive.
    fn spawn_reload(&self) {
        let period = Duration::from_secs(self.inner.config.reload_interval_secs);
        if period.is_zero() {
            return;
        }
        let weak: Weak<Inner> = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                // reload logs and counts its own outcome.
                let _ = ServerTls { inner }.reload().await;
            }
        });
    }
}

impl Inner {
    async fn try_reload(&self, state: &mut ReloadState) -> Result<Reloaded, TlsError> {
        let material = Sources::of(&self.config)?.read().await?;
        let identity = material.identity();
        let client_ca = material.client_ca();

        let key = if identity == state.identity {
            None
        } else {
            Some(certified_key(
                &material.cert,
                &material.key,
                &self.provider,
            )?)
        };
        let server_config = if client_ca == state.client_ca {
            None
        } else {
            let verifier = client_verifier(
                self.config.client_auth,
                material.ca.as_deref(),
                &self.provider,
            )?;
            Some(build_server_config(
                &self.options,
                &self.provider,
                verifier,
                &self.resolver,
            )?)
        };

        let mut reloaded = Reloaded::default();
        if let Some(key) = key {
            self.resolver.current.store(Arc::new(key));
            state.identity = identity;
            reloaded.certificate = true;
        }
        if let Some(server_config) = server_config {
            self.server_config.store(Arc::new(server_config));
            state.client_ca = client_ca;
            reloaded.client_ca = true;
        }
        Ok(reloaded)
    }
}

/// One of the three things a server reads.
#[derive(Clone, Copy)]
enum Part {
    Cert,
    Key,
    Ca,
}

impl Part {
    fn file_field(self) -> &'static str {
        match self {
            Self::Cert => "cert_file",
            Self::Key => "key_file",
            Self::Ca => "ca_file",
        }
    }

    fn secret_field(self) -> &'static str {
        match self {
            Self::Cert => "cert_secret",
            Self::Key => "key_secret",
            Self::Ca => "ca_secret",
        }
    }
}

#[derive(Clone, Copy)]
enum Source<'a> {
    File(&'a Path),
    Secret(&'a str, Part),
}

impl<'a> Source<'a> {
    fn of(
        file: Option<&'a PathBuf>,
        secret: Option<&'a String>,
        part: Part,
    ) -> Result<Option<Self>, TlsError> {
        match (file, secret) {
            (Some(_), Some(_)) => Err(TlsError::ConflictingSource {
                file: part.file_field(),
                secret: part.secret_field(),
            }),
            (Some(path), None) => Ok(Some(Self::File(path))),
            (None, Some(spec)) => Ok(Some(Self::Secret(spec, part))),
            (None, None) => Ok(None),
        }
    }

    fn required(
        file: Option<&'a PathBuf>,
        secret: Option<&'a String>,
        part: Part,
    ) -> Result<Self, TlsError> {
        Self::of(file, secret, part)?.ok_or(TlsError::MissingSource {
            file: part.file_field(),
            secret: part.secret_field(),
        })
    }

    async fn read(self) -> Result<Vec<u8>, TlsError> {
        match self {
            Self::File(path) => tokio::fs::read(path).await.map_err(|e| TlsError::PemRead {
                path: path.to_path_buf(),
                reason: e.to_string(),
            }),
            Self::Secret(spec, part) => read_secret(spec, part).await,
        }
    }
}

#[cfg(feature = "secrets")]
async fn read_secret(spec: &str, part: Part) -> Result<Vec<u8>, TlsError> {
    crate::secrets::resolve(spec)
        .await
        .map(String::into_bytes)
        .map_err(|e| TlsError::SecretRead {
            field: part.secret_field(),
            reason: e.to_string(),
        })
}

#[cfg(not(feature = "secrets"))]
#[allow(
    clippy::unused_async,
    reason = "same signature as the secrets-enabled build"
)]
async fn read_secret(_spec: &str, part: Part) -> Result<Vec<u8>, TlsError> {
    Err(TlsError::SecretsUnsupported {
        field: part.secret_field(),
    })
}

/// A config's sources, checked for shape before anything is read.
struct Sources<'a> {
    cert: Source<'a>,
    key: Source<'a>,
    ca: Option<Source<'a>>,
}

impl<'a> Sources<'a> {
    fn of(config: &'a ServerTlsConfig) -> Result<Self, TlsError> {
        let cert = Source::required(
            config.cert_file.as_ref(),
            config.cert_secret.as_ref(),
            Part::Cert,
        )?;
        let key = Source::required(
            config.key_file.as_ref(),
            config.key_secret.as_ref(),
            Part::Key,
        )?;
        let ca = Source::of(config.ca_file.as_ref(), config.ca_secret.as_ref(), Part::Ca)?;
        let ca = match (config.client_auth, ca) {
            (ClientAuth::Optional | ClientAuth::Required, None) => {
                return Err(TlsError::ClientCaRequired {
                    mode: config.client_auth,
                });
            }
            // With client_auth none a CA checks nothing, so it is not read.
            (ClientAuth::None, Some(_)) => None,
            (_, ca) => ca,
        };
        Ok(Self { cert, key, ca })
    }

    async fn read(&self) -> Result<Material, TlsError> {
        let ca = match self.ca {
            Some(source) => Some(source.read().await?),
            None => None,
        };
        Ok(Material {
            cert: self.cert.read().await?,
            key: self.key.read().await?,
            ca,
        })
    }
}

/// The bytes one read of the sources returned.
struct Material {
    cert: Vec<u8>,
    key: Vec<u8>,
    ca: Option<Vec<u8>>,
}

impl Material {
    /// Change detector for the certificate and key, not a security digest.
    fn identity(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.cert.hash(&mut hasher);
        self.key.hash(&mut hasher);
        hasher.finish()
    }

    /// Change detector for the client CA.
    fn client_ca(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.ca.hash(&mut hasher);
        hasher.finish()
    }
}

/// Parse a PEM chain and key into a certificate rustls can serve, refusing a
/// key that does not match the leaf.
fn certified_key(
    cert_pem: &[u8],
    key_pem: &[u8],
    provider: &CryptoProvider,
) -> Result<CertifiedKey, TlsError> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::InvalidPem {
            what: "certificate",
            reason: e.to_string(),
        })?;
    if chain.is_empty() {
        return Err(TlsError::InvalidPem {
            what: "certificate",
            reason: "no certificate in the PEM data".to_string(),
        });
    }
    // The parser's error can quote the input, so it is not passed on.
    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|_| TlsError::InvalidPem {
        what: "private key",
        reason: "no PKCS#1, PKCS#8 or SEC1 private key in the PEM data".to_string(),
    })?;
    CertifiedKey::from_der(chain, key, provider)
        .map_err(|e| TlsError::Build(format!("certificate and private key: {e}")))
}

/// The client-certificate check for `mode`, against the CA in `ca_pem`.
fn client_verifier(
    mode: ClientAuth,
    ca_pem: Option<&[u8]>,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
    if mode == ClientAuth::None {
        return Ok(WebPkiClientVerifier::no_client_auth());
    }
    let ca_pem = ca_pem.ok_or(TlsError::ClientCaRequired { mode })?;
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(ca_pem)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::InvalidPem {
            what: "client CA",
            reason: e.to_string(),
        })?;
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err(TlsError::InvalidPem {
            what: "client CA",
            reason: "no CA certificate in the PEM data".to_string(),
        });
    }
    let builder =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(provider));
    let builder = match mode {
        ClientAuth::Optional => builder.allow_unauthenticated(),
        ClientAuth::None | ClientAuth::Required => builder,
    };
    builder
        .build()
        .map_err(|e| TlsError::Build(format!("client certificate verifier: {e}")))
}

fn build_server_config(
    options: &ServerTlsOptions,
    provider: &Arc<CryptoProvider>,
    verifier: Arc<dyn ClientCertVerifier>,
    resolver: &Arc<CertResolver>,
) -> Result<ServerConfig, TlsError> {
    let resolver: Arc<dyn ResolvesServerCert> = Arc::clone(resolver) as _;
    let mut config = ServerConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(protocol_versions(options.profile.tls_floor()))
        .map_err(|e| TlsError::Build(e.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(resolver);
    config.alpn_protocols.clone_from(&options.alpn_protocols);
    Ok(config)
}

#[cfg_attr(not(feature = "metrics-core"), allow(unused_variables))]
fn record_handshake(protocol: Option<ProtocolVersion>, group: Option<NamedGroup>) {
    #[cfg(feature = "metrics-core")]
    {
        let protocol = match protocol {
            Some(ProtocolVersion::TLSv1_3) => "1.3",
            Some(ProtocolVersion::TLSv1_2) => "1.2",
            _ => "other",
        };
        let key_exchange = if group.is_some_and(super::is_hybrid_pqc) {
            "hybrid"
        } else {
            "classical"
        };
        metrics::counter!(
            "tls_handshakes_total",
            "protocol" => protocol,
            "key_exchange" => key_exchange
        )
        .increment(1);
    }
}

fn record_handshake_failure(reason: &'static str, peer: Option<SocketAddr>, detail: &str) {
    tracing::debug!(peer = ?peer, reason, error = detail, "TLS handshake failed");
    #[cfg(feature = "metrics-core")]
    metrics::counter!("tls_handshake_failures_total", "reason" => reason).increment(1);
    #[cfg(feature = "logger")]
    crate::logger::security::tls_event(
        "handshake",
        crate::logger::security::SecurityOutcome::Failure,
        Some(detail),
        peer.map(|p| p.ip()),
    );
}

#[cfg_attr(not(feature = "metrics-core"), allow(unused_variables))]
fn record_reload(result: &'static str) {
    #[cfg(feature = "metrics-core")]
    metrics::counter!("tls_certificate_reloads_total", "result" => result).increment(1);
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
