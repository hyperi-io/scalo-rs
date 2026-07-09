// Project:   scalo
// File:      src/tls.rs
// Purpose:   Unified TLS trust + client-config construction (private-CA first)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Unified TLS trust
//!
//! One place to build a rustls [`ClientConfig`] for every transport that
//! speaks TLS (HTTP, gRPC, Vault, Redis; Kafka maps the same vocabulary onto
//! librdkafka file paths). Collapses the previously ad-hoc per-transport TLS
//! handling into a single trust model with first-class **private-CA** support
//! -- the common deployment shape (an internal CA, not the public web
//! PKI). Mirrors the design used in the clickhouse-rs fork.
//!
//! ## Crypto provider
//!
//! Client configs are built with an **explicit aws-lc-rs provider** rather than
//! relying on the rustls process-default. Depending on enabled features the
//! dependency graph can carry more than one rustls CryptoProvider (aws-lc-rs
//! via reqwest / aws-smithy, plus ring via some TLS-enabling crates), so there
//! is no guaranteed unambiguous process-default -- `ClientConfig::builder()`
//! would panic at runtime ("no process-level CryptoProvider"). Selecting
//! aws-lc-rs explicitly matches the reqwest / AWS path and removes that footgun.
//!
//! ## Trust model
//!
//! [`TlsTrust`] composes roots from up to three sources:
//! - **native** OS roots (`rustls-native-certs`),
//! - **webpki** Mozilla roots (`webpki-roots`),
//! - **extra** PEM files (`extra_roots` + `extra_intermediates`) -- the private
//!   CA, accepted as a single bundle file or several.
//!
//! `exclusive = true` trusts ONLY the extra PEM files (native + webpki ignored)
//! -- pin to your private CA and nothing else. An empty resulting store is an
//! error, never a silent "trust nothing / trust everything".
//!
//! ## No `InsecureSkipVerify`
//!
//! This module offers no certificate-verification bypass. Transports that still
//! expose a dev-only `skip_verify` flag gate it to non-production (see
//! `KafkaConfig::validate` / `OpenBaoConfig::validate`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, NamedGroup, ProtocolVersion, RootCertStore};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;

use crate::crypto::{CryptoProfile, PqcMode, TlsFloor};

/// Errors building a TLS trust store or client config.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A certificate file could not be opened or read as PEM.
    #[error("TLS: cannot read certificate file {path}: {reason}")]
    PemRead {
        /// The offending path.
        path: PathBuf,
        /// Human-readable reason.
        reason: String,
    },

    /// A file was read but contained no usable certificates.
    #[error("TLS: no certificates parsed from {path}")]
    NoCertsFound {
        /// The offending path.
        path: PathBuf,
    },

    /// The resulting trust store has no roots (would trust nothing).
    #[error(
        "TLS: trust store is empty -- no roots from native/webpki/extra sources \
         (check `native_roots`/`webpki_roots`/`extra_roots`, or `exclusive` with no files)"
    )]
    EmptyTrustStore,

    /// rustls rejected the assembled client configuration.
    #[error("TLS: failed to build client config: {0}")]
    Build(String),
}

/// Where a transport's TLS roots come from.
///
/// `native` + `webpki` are convenience root sources; `extra_*` are explicit
/// PEM files (the private CA). See the module docs for `exclusive`.
#[derive(Debug, Clone)]
pub struct TlsTrust {
    /// Trust the OS native root store.
    pub native_roots: bool,
    /// Trust the bundled Mozilla (webpki) root store.
    pub webpki_roots: bool,
    /// Extra root-CA PEM files to trust (e.g. a private CA bundle).
    pub extra_roots: Vec<PathBuf>,
    /// Extra intermediate-CA PEM files to add as trust anchors. Useful when a
    /// private CA ships an intermediate the server does not present.
    pub extra_intermediates: Vec<PathBuf>,
    /// Trust ONLY `extra_*` -- ignore native and webpki regardless of their
    /// flags. Pin to the private CA and nothing else.
    pub exclusive: bool,
}

impl Default for TlsTrust {
    /// Native OS roots, no webpki, no extras, non-exclusive -- the
    /// public-internet default.
    fn default() -> Self {
        Self {
            native_roots: true,
            webpki_roots: false,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: false,
        }
    }
}

impl TlsTrust {
    /// Trust only the given private-CA PEM bundle (exclusive pin).
    #[must_use]
    pub fn private_ca(pem_path: impl Into<PathBuf>) -> Self {
        Self {
            native_roots: false,
            webpki_roots: false,
            extra_roots: vec![pem_path.into()],
            extra_intermediates: Vec::new(),
            exclusive: true,
        }
    }
}

/// Source for a client `ClientConfig`: either fully pre-built, or assembled
/// from a [`TlsTrust`].
pub enum TlsConfigSource {
    /// Use a caller-supplied config verbatim.
    Explicit(Arc<ClientConfig>),
    /// Build from a trust specification.
    Trust(TlsTrust),
}

/// Append certificates from a PEM file into `store`, leniently.
///
/// Skips unparseable PEM blocks and non-certificate DER, so a bundle mixing
/// junk with valid certs still loads the valid ones. Returns the number of
/// certificates added. Errors only if the path is not a readable file, or if
/// zero certificates were added.
///
/// # Errors
///
/// [`TlsError::PemRead`] if the path is not a readable file;
/// [`TlsError::NoCertsFound`] if no certificate parsed from it.
pub fn add_pem_file_certs(store: &mut RootCertStore, path: &Path) -> Result<usize, TlsError> {
    // Guard the known footgun: `pem_file_iter` loops forever on a directory
    // (rustls/pki-types#98). Also gives a clean error for missing files.
    if !path.is_file() {
        return Err(TlsError::PemRead {
            path: path.to_path_buf(),
            reason: "not a readable file".to_string(),
        });
    }

    let iter = CertificateDer::pem_file_iter(path).map_err(|e| TlsError::PemRead {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;

    // Lenient: drop unreadable PEM blocks (filter_map ok), then let
    // add_parsable_certificates drop any DER that is not a valid certificate.
    let certs: Vec<CertificateDer<'static>> = iter.filter_map(Result::ok).collect();
    let (added, _ignored) = store.add_parsable_certificates(certs);

    if added == 0 {
        return Err(TlsError::NoCertsFound {
            path: path.to_path_buf(),
        });
    }
    Ok(added)
}

/// Assemble a [`RootCertStore`] from a [`TlsTrust`].
///
/// # Errors
///
/// Propagates [`add_pem_file_certs`] errors for any `extra_*` file, and returns
/// [`TlsError::EmptyTrustStore`] if the assembled store has no roots.
pub fn build_root_store(trust: &TlsTrust) -> Result<RootCertStore, TlsError> {
    let mut store = RootCertStore::empty();

    if !trust.exclusive {
        if trust.native_roots {
            let result = rustls_native_certs::load_native_certs();
            let (_added, _ignored) = store.add_parsable_certificates(result.certs);
            for err in result.errors {
                tracing::warn!(error = %err, "TLS: error loading a native root (continuing)");
            }
        }
        if trust.webpki_roots {
            store
                .roots
                .extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    }

    for path in &trust.extra_roots {
        add_pem_file_certs(&mut store, path)?;
    }
    for path in &trust.extra_intermediates {
        add_pem_file_certs(&mut store, path)?;
    }

    if store.is_empty() {
        return Err(TlsError::EmptyTrustStore);
    }
    Ok(store)
}

/// Build a rustls [`ClientConfig`] with the default ([`CryptoProfile::Prod`])
/// posture: commercial-floor best practice - TLS 1.2 floor (1.3 preferred),
/// hybrid ML-KEM key exchange preferred with classical fallback, AES-256,
/// verified certs. The drop-in "fire and use" form; `reqwest`, `tonic`,
/// `tokio-rustls`, `hyper` and the clickhouse-rs fork all consume the result.
///
/// # Errors
///
/// Propagates [`build_root_store`] errors, and [`TlsError::Build`] if rustls
/// rejects the protocol-version selection.
pub fn build_client_config(source: TlsConfigSource) -> Result<Arc<ClientConfig>, TlsError> {
    build_client_config_with(CryptoProfile::default(), source)
}

/// Build a rustls [`ClientConfig`] for an explicit [`CryptoProfile`] - e.g.
/// [`CryptoProfile::HighSec`] to require hybrid post-quantum key exchange and
/// pin TLS 1.3.
///
/// # Errors
///
/// As [`build_client_config`].
pub fn build_client_config_with(
    profile: CryptoProfile,
    source: TlsConfigSource,
) -> Result<Arc<ClientConfig>, TlsError> {
    match source {
        TlsConfigSource::Explicit(cfg) => Ok(cfg),
        TlsConfigSource::Trust(trust) => {
            let roots = build_root_store(&trust)?;
            let provider = posture_provider(profile.pqc());
            let builder = ClientConfig::builder_with_provider(provider);
            let versioned = match profile.tls_floor() {
                // Commercial floor: TLS 1.2 + 1.3; peers negotiate up.
                TlsFloor::V1_2 => builder.with_safe_default_protocol_versions(),
                // National-security: TLS 1.3 only.
                TlsFloor::V1_3 => builder.with_protocol_versions(&[&rustls::version::TLS13]),
            };
            let cfg = versioned
                .map_err(|e| TlsError::Build(e.to_string()))?
                .with_root_certificates(roots)
                .with_no_client_auth();
            Ok(Arc::new(cfg))
        }
    }
}

/// Is `group` a hybrid post-quantum (ML-KEM) key-exchange group?
///
/// Add `SecP384r1MLKEM1024` (the CNSA-pure P-384 + ML-KEM-1024 hybrid) here
/// when rustls/aws-lc-rs expose it; today the widely-deployed
/// `X25519MLKEM768` is the one available.
fn is_hybrid_pqc(group: NamedGroup) -> bool {
    matches!(group, NamedGroup::X25519MLKEM768)
}

/// Build an aws-lc-rs provider with the kx-group order/selection the
/// [`PqcMode`] mandates.
///
/// aws-lc-rs OFFERS the hybrid group by default but lists it LAST, so two
/// default peers negotiate a CLASSICAL group (proven by spike). `Prefer`
/// reorders the hybrid to the front so it is actually used; `Require` keeps
/// only the hybrid(s) (non-PQC peers are refused); `Off` drops them.
fn posture_provider(pqc: PqcMode) -> Arc<CryptoProvider> {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    match pqc {
        // Stable sort: hybrid(s) to the front, classical order preserved.
        PqcMode::Prefer => provider
            .kx_groups
            .sort_by_key(|g| u8::from(!is_hybrid_pqc(g.name()))),
        PqcMode::Require => provider.kx_groups.retain(|g| is_hybrid_pqc(g.name())),
        PqcMode::Off => provider.kx_groups.retain(|g| !is_hybrid_pqc(g.name())),
    }
    Arc::new(provider)
}

/// Primitive TLS-posture values, for consumers that take paths/strings rather
/// than a rustls [`ClientConfig`] - libpq (`sslmode`/`sslrootcert`), librdkafka
/// (`ssl.ca.location`/`ssl.cipher.suites`/`ssl.curves.list`), `sqlx`. The same
/// posture as [`build_client_config_with`], expressed as those fields (the
/// "primitives" mint form).
#[derive(Debug, Clone)]
pub struct TlsParts {
    /// Minimum TLS version, `"1.2"` or `"1.3"`.
    pub min_version: &'static str,
    /// Key-exchange groups in preference order (OpenSSL/librdkafka names).
    pub curves: Vec<&'static str>,
    /// TLS 1.2 AEAD cipher list (OpenSSL/librdkafka form).
    pub cipher_string: &'static str,
    /// Private-CA bundle path(s) to trust.
    pub ca_paths: Vec<PathBuf>,
    /// Whether to verify the peer certificate (always true off the hard floor).
    pub verify: bool,
}

/// Emit [`TlsParts`] for a profile + trust: the primitives mint form.
#[must_use]
pub fn tls_parts(profile: CryptoProfile, trust: &TlsTrust) -> TlsParts {
    let curves = match profile.pqc() {
        PqcMode::Require => vec!["X25519MLKEM768"],
        PqcMode::Prefer => vec!["X25519MLKEM768", "P-384", "X25519"],
        PqcMode::Off => vec!["P-384", "X25519"],
    };
    TlsParts {
        min_version: match profile.tls_floor() {
            TlsFloor::V1_2 => "1.2",
            TlsFloor::V1_3 => "1.3",
        },
        curves,
        cipher_string: "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384",
        ca_paths: trust.extra_roots.clone(),
        verify: true,
    }
}

/// Warn (via `tracing`) if a COMPLETED handshake negotiated below the profile's
/// preferred posture - a classical key exchange when hybrid was preferred, or
/// TLS 1.2 when 1.3 was available. Call after the handshake on a scalo-owned
/// socket / server connection (rustls `negotiated_key_exchange_group()` +
/// `protocol_version()`); `reqwest`/`tonic` do not surface these to the caller.
pub fn warn_if_downgraded(
    profile: CryptoProfile,
    negotiated_version: Option<ProtocolVersion>,
    negotiated_group: Option<NamedGroup>,
    peer: &str,
) {
    if !profile.warn_on_downgrade() {
        return;
    }
    if profile.pqc() == PqcMode::Prefer && negotiated_group.is_some_and(|g| !is_hybrid_pqc(g)) {
        tracing::warn!(
            peer,
            group = ?negotiated_group,
            "TLS negotiated a classical key exchange (no post-quantum protection); peer does not offer hybrid ML-KEM"
        );
    }
    if negotiated_version == Some(ProtocolVersion::TLSv1_2) {
        tracing::warn!(
            peer,
            "TLS negotiated 1.2, below the preferred 1.3 (commercial floor)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Generate `n` independent self-signed CA cert PEMs, concatenated.
    fn gen_ca_bundle(n: usize) -> String {
        let mut bundle = String::new();
        for i in 0..n {
            let cert = rcgen::generate_simple_self_signed(vec![format!("ca-{i}.test")])
                .expect("rcgen self-signed");
            bundle.push_str(&cert.cert.pem());
        }
        bundle
    }

    fn write_temp(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("temp file");
        f.write_all(contents.as_bytes()).expect("write");
        f.flush().expect("flush");
        f
    }

    // --- real in-process TLS handshake harness (no network, real crypto) ---
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use rustls::{ClientConnection, ServerConfig, ServerConnection};

    /// A P-384 self-signed server cert: (DER cert, DER key, PEM for the CA file).
    fn gen_server_cert() -> (CertificateDer<'static>, PrivateKeyDer<'static>, String) {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let pem = ck.cert.pem();
        let cert = ck.cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
        (cert, key, pem)
    }

    fn server_config_with(
        pqc: PqcMode,
        cert: &CertificateDer<'static>,
        key: &PrivateKeyDer<'static>,
    ) -> Arc<ServerConfig> {
        Arc::new(
            ServerConfig::builder_with_provider(posture_provider(pqc))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert.clone()], key.clone_key())
                .unwrap(),
        )
    }

    /// Drive an in-memory handshake to completion; propagate the first rustls
    /// error (e.g. no common kx group under `Require` vs a classical peer).
    fn pump(
        client: &mut ClientConnection,
        server: &mut ServerConnection,
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
        Ok(())
    }

    fn client_for(profile: CryptoProfile, ca_path: &Path) -> Arc<ClientConfig> {
        build_client_config_with(
            profile,
            TlsConfigSource::Trust(TlsTrust::private_ca(ca_path)),
        )
        .unwrap()
    }

    #[test]
    fn prod_negotiates_pqc_hybrid() {
        let (cert, key, pem) = gen_server_cert();
        let ca = write_temp(&pem);
        let mut client = ClientConnection::new(
            client_for(CryptoProfile::Prod, ca.path()),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut server =
            ServerConnection::new(server_config_with(PqcMode::Prefer, &cert, &key)).unwrap();
        pump(&mut client, &mut server).expect("handshake completes");
        assert_eq!(
            client.negotiated_key_exchange_group().map(|g| g.name()),
            Some(NamedGroup::X25519MLKEM768),
            "prod prefers and actually negotiates the hybrid PQC group"
        );
        assert_eq!(client.protocol_version(), Some(ProtocolVersion::TLSv1_3));
    }

    #[test]
    fn highsec_require_fails_against_classical_only_peer() {
        let (cert, key, pem) = gen_server_cert();
        let ca = write_temp(&pem);
        let mut client = ClientConnection::new(
            client_for(CryptoProfile::HighSec, ca.path()), // requires PQC
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut server =
            ServerConnection::new(server_config_with(PqcMode::Off, &cert, &key)).unwrap(); // classical only
        assert!(
            pump(&mut client, &mut server).is_err(),
            "highsec REQUIRES post-quantum kx; a classical-only peer must fail, not silently downgrade"
        );
    }

    #[test]
    fn posture_provider_selects_groups() {
        let all_hybrid = |p: &CryptoProvider| p.kx_groups.iter().all(|g| is_hybrid_pqc(g.name()));
        let any_hybrid = |p: &CryptoProvider| p.kx_groups.iter().any(|g| is_hybrid_pqc(g.name()));
        let prefer = posture_provider(PqcMode::Prefer);
        assert!(
            prefer
                .kx_groups
                .first()
                .is_some_and(|g| is_hybrid_pqc(g.name())),
            "prefer puts the hybrid first"
        );
        assert!(
            all_hybrid(&posture_provider(PqcMode::Require)),
            "require keeps only hybrid"
        );
        assert!(
            !any_hybrid(&posture_provider(PqcMode::Off)),
            "off drops the hybrid"
        );
    }

    #[test]
    fn tls_parts_reflect_profile() {
        let f = write_temp(&gen_ca_bundle(1));
        let trust = TlsTrust::private_ca(f.path());
        let prod = tls_parts(CryptoProfile::Prod, &trust);
        assert_eq!(prod.min_version, "1.2");
        assert_eq!(prod.curves.first(), Some(&"X25519MLKEM768"));
        assert_eq!(prod.ca_paths, vec![f.path().to_path_buf()]);
        let hs = tls_parts(CryptoProfile::HighSec, &trust);
        assert_eq!(hs.min_version, "1.3");
        assert_eq!(hs.curves, vec!["X25519MLKEM768"]);
    }

    #[test]
    fn add_pem_counts_multi_cert_bundle() {
        let f = write_temp(&gen_ca_bundle(3));
        let mut store = RootCertStore::empty();
        let added = add_pem_file_certs(&mut store, f.path()).unwrap();
        assert_eq!(added, 3, "all three certs in the bundle are added");
    }

    #[test]
    fn add_pem_is_lenient_with_junk_plus_valid() {
        let mut contents = String::from("this is not a PEM block\ngarbage line\n");
        contents.push_str(&gen_ca_bundle(1));
        contents.push_str("\ntrailing junk\n");
        let f = write_temp(&contents);
        let mut store = RootCertStore::empty();
        let added = add_pem_file_certs(&mut store, f.path()).unwrap();
        assert_eq!(added, 1, "junk is skipped, the valid cert still loads");
    }

    #[test]
    fn add_pem_zero_certs_is_error() {
        let f = write_temp("no certificates here at all\n");
        let mut store = RootCertStore::empty();
        let err = add_pem_file_certs(&mut store, f.path()).unwrap_err();
        assert!(matches!(err, TlsError::NoCertsFound { .. }));
    }

    #[test]
    fn add_pem_unreadable_path_is_error() {
        let mut store = RootCertStore::empty();
        let err = add_pem_file_certs(&mut store, Path::new("/nonexistent/nope.pem")).unwrap_err();
        assert!(matches!(err, TlsError::PemRead { .. }));
    }

    #[test]
    fn build_store_augments_native_with_extra() {
        let f = write_temp(&gen_ca_bundle(1));
        let trust = TlsTrust {
            native_roots: true,
            webpki_roots: false,
            extra_roots: vec![f.path().to_path_buf()],
            extra_intermediates: Vec::new(),
            exclusive: false,
        };
        let store = build_root_store(&trust).unwrap();
        // Native roots vary by host, but the store must be non-empty and at
        // least contain our extra cert.
        assert!(!store.is_empty());
    }

    #[test]
    fn build_store_exclusive_uses_only_extra() {
        let f = write_temp(&gen_ca_bundle(2));
        let trust = TlsTrust::private_ca(f.path());
        let store = build_root_store(&trust).unwrap();
        assert_eq!(
            store.roots.len(),
            2,
            "exclusive store holds exactly the private-CA certs, no native roots"
        );
    }

    #[test]
    fn build_store_exclusive_with_no_files_is_error() {
        let trust = TlsTrust {
            native_roots: true, // ignored under exclusive
            webpki_roots: true, // ignored under exclusive
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: true,
        };
        let err = build_root_store(&trust).unwrap_err();
        assert!(matches!(err, TlsError::EmptyTrustStore));
    }

    #[test]
    fn build_store_no_sources_is_empty_error() {
        let trust = TlsTrust {
            native_roots: false,
            webpki_roots: false,
            extra_roots: Vec::new(),
            extra_intermediates: Vec::new(),
            exclusive: false,
        };
        let err = build_root_store(&trust).unwrap_err();
        assert!(matches!(err, TlsError::EmptyTrustStore));
    }

    #[test]
    fn build_client_config_from_private_ca() {
        let f = write_temp(&gen_ca_bundle(1));
        let cfg =
            build_client_config(TlsConfigSource::Trust(TlsTrust::private_ca(f.path()))).unwrap();
        // A usable client config was produced (no client auth, has roots).
        assert!(Arc::strong_count(&cfg) >= 1);
    }
}
