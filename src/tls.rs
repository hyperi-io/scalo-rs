// Project:   scalo
// File:      src/tls.rs
// Purpose:   One TLS builder for every client and server, on the named profiles
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # TLS
//!
//! Every TLS client scalo builds reads one config section, [`ClientTlsConfig`],
//! and builds through [`build_client_tls`]: HTTP, OTLP, the HTTP DLQ,
//! `HttpClient` and the version check over reqwest, gRPC, OTLP-gRPC and Vector
//! compat over tonic, and syslog over its own stream. Every server reads
//! [`ServerTlsConfig`] and builds through [`ServerTls`]. Kafka maps the same
//! section onto librdkafka's properties. Clients inside other libraries keep
//! those libraries' TLS: Vault through `vaultrs`, AWS Secrets Manager through
//! the AWS SDK, and the OpenTelemetry exporters.
//!
//! Versions, suites, groups and signatures come from the section's
//! [`CryptoProfile`], with its settings from the `tls.profiles` cascade
//! section: see [`crate::crypto`].
//!
//! ## Client trust and identity
//!
//! - `native` (the default): the OS root store, read through
//!   `rustls-native-certs` and checked by rustls, plus the CA files.
//! - `platform`: the OS verifier through `rustls-platform-verifier`, which
//!   applies the Windows and macOS policy and revocation checks, plus the CA
//!   files.
//! - `bundle`: the CA files and nothing else, the exclusive pin.
//!
//! Key pins ([`SpkiPin`]) add a check on top of any choice: once the chain
//! verifies, the server's public key must match one of them.
//!
//! ```rust,no_run
//! use scalo::tls::{ClientTls, build_client_tls};
//!
//! let tls = ClientTls::pinned("/etc/tls/ca.pem")
//!     .with_identity("/etc/tls/client.pem", "/etc/tls/client.key");
//! let config = build_client_tls(&tls)?;
//! # Ok::<(), scalo::tls::TlsError>(())
//! ```
//!
//! ## Failures
//!
//! [`TlsFailure::find`] reads why a handshake failed from any error chain,
//! reqwest's and tonic's included, as one of a fixed set of reasons that each
//! name the fix.
//!
//! ## Crypto provider
//!
//! Configs are built with an explicit aws-lc-rs provider carrying the
//! profile's lists, never the rustls process default, which may be ambiguous
//! when both aws-lc-rs and ring are linked. scalo installs no process
//! default. [`install_default_provider`] is for an application's `main`.
//!
//! ## No `InsecureSkipVerify`
//!
//! This module offers no certificate-verification bypass. Transports that still
//! expose a dev-only `skip_verify` flag gate it to non-production (see
//! `KafkaConfig::validate` / `OpenBaoConfig::validate`).

use rustls::crypto::CryptoProvider;
use rustls::{NamedGroup, ProtocolVersion};

mod client;
mod failure;
#[cfg(test)]
mod matrix_tests;
mod pin;
mod profile;
mod server;
#[cfg(test)]
pub(crate) mod test_pki;

pub use client::{ClientIdentity, ClientTls, build_client_tls};
pub use profile::{ProfileSettings, is_post_quantum};
pub use server::{CertResolver, HandshakeError, Reloaded, ServerTls, ServerTlsOptions};

pub use crate::crypto::{CryptoProfile, KeyShare, TlsProfiles};
pub use crate::tls_config::{
    ClientAuth, ClientTlsConfig, InvalidPin, ServerTlsConfig, SpkiPin, TlsFailure, TrustChoice,
    Whose,
};

/// Errors building a TLS client or server config.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A certificate file could not be opened or read as PEM.
    #[error("TLS: cannot read certificate file {path}: {reason}")]
    PemRead {
        /// The offending path.
        path: std::path::PathBuf,
        /// Human-readable reason.
        reason: String,
    },

    /// A file was read but contained no usable certificates.
    #[error("TLS: no certificates parsed from {path}")]
    NoCertsFound {
        /// The offending path.
        path: std::path::PathBuf,
    },

    /// The resulting trust store has no roots (would trust nothing).
    #[error("TLS: no trusted root: `trust: bundle` needs a CA file, or the OS root store is empty")]
    EmptyTrustStore,

    /// rustls rejected the assembled configuration, or a certificate does not
    /// match its private key.
    #[error("TLS: failed to build config: {0}")]
    Build(String),

    /// The `tls.profiles` cascade section does not parse or is refused.
    #[error("{0}")]
    Profiles(String),

    /// A server certificate or key has no source configured.
    #[error("TLS: set `{file}` or `{secret}`")]
    MissingSource {
        /// The file field.
        file: &'static str,
        /// The secret field.
        secret: &'static str,
    },

    /// A server certificate, key or CA has both a file and a secret configured.
    #[error("TLS: set `{file}` or `{secret}`, not both")]
    ConflictingSource {
        /// The file field.
        file: &'static str,
        /// The secret field.
        secret: &'static str,
    },

    /// Client certificates are asked for with no CA to check them against.
    #[error("TLS: client_auth `{mode}` needs `ca_files` or `ca_secret`")]
    ClientCaRequired {
        /// The configured mode.
        mode: ClientAuth,
    },

    /// A secret spec did not resolve.
    #[error("TLS: cannot read `{field}`: {reason}")]
    SecretRead {
        /// The config field holding the spec. The spec itself is never shown,
        /// because a literal spec is the secret.
        field: &'static str,
        /// Why the lookup failed.
        reason: String,
    },

    /// A secret spec is configured but the `secrets` feature is off.
    #[error("TLS: `{field}` needs the `secrets` feature")]
    SecretsUnsupported {
        /// The config field holding the spec.
        field: &'static str,
    },

    /// PEM data held no usable certificate or key.
    #[error("TLS: {what}: {reason}")]
    InvalidPem {
        /// What was being parsed.
        what: &'static str,
        /// Why it was refused.
        reason: String,
    },

    /// A key pin is not a SHA-256 in base64 or hex.
    #[error(transparent)]
    InvalidPin(#[from] InvalidPin),
}

/// Install aws-lc-rs as the process-default rustls [`CryptoProvider`], and
/// return whether this call installed it.
///
/// For an application's `main`, before anything builds a TLS config with
/// another library. With both aws-lc-rs and ring linked and no default
/// installed, rustls panics in `ServerConfig::builder()` and
/// `ClientConfig::builder()`. A provider already installed stays the default
/// and this returns `false`.
///
/// ```rust,no_run
/// scalo::tls::install_default_provider();
/// ```
pub fn install_default_provider() -> bool {
    let installed = rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_ok();
    if let Some(provider) = CryptoProvider::get_default() {
        tracing::debug!(
            key_provider = ?provider.key_provider,
            installed,
            "rustls process-default crypto provider"
        );
    }
    installed
}

/// Whether `url` is an `https://` URL, in any case.
#[cfg(any(
    feature = "transport-http",
    feature = "transport-grpc-vector-compat",
    feature = "dlq-http",
    feature = "version-check"
))]
pub(crate) fn is_https(url: &str) -> bool {
    url.get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

/// The rustls error in `error` or its causes: a certificate, name or key pin
/// refused, or an alert from the peer, such as a client certificate it would
/// not take.
///
/// An `io::Error`'s `source` skips the error it wraps, and reqwest wraps
/// tokio-rustls's `io::Error` in another, so the walk steps into each
/// `io::Error` rather than to its source.
pub(crate) fn rustls_error<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a rustls::Error> {
    let mut link = Some(error);
    while let Some(current) = link {
        if let Some(tls) = current.downcast_ref::<rustls::Error>() {
            return Some(tls);
        }
        link = match current.downcast_ref::<std::io::Error>() {
            Some(io) => io
                .get_ref()
                .map(|inner| inner as &(dyn std::error::Error + 'static)),
            None => current.source(),
        };
    }
    None
}

/// The reason in `error` for a connection on `settings`, else the rustls error
/// itself, as text for a transport's error. `None` when `error` holds no
/// rustls error.
#[cfg(any(
    feature = "transport-http",
    feature = "transport-syslog",
    feature = "transport-otlp",
    feature = "transport-grpc"
))]
pub(crate) fn describe_failure(
    error: &(dyn std::error::Error + 'static),
    settings: ProfileSettings,
) -> Option<String> {
    let tls = rustls_error(error)?;
    Some(
        TlsFailure::of(tls, settings)
            .map_or_else(|| tls.to_string(), |failure| failure.to_string()),
    )
}

/// Warn (via `tracing`) if a completed handshake negotiated below `settings`:
/// a classical key exchange when the profile leads with a post-quantum group,
/// or TLS 1.2. For a caller holding a rustls connection
/// (`negotiated_key_exchange_group()` and `protocol_version()`).
pub fn warn_if_downgraded(
    settings: ProfileSettings,
    negotiated_version: Option<ProtocolVersion>,
    negotiated_group: Option<NamedGroup>,
    peer: &str,
) {
    let leads_post_quantum = settings
        .kx_groups()
        .first()
        .is_some_and(|group| is_post_quantum(group.name()));
    if leads_post_quantum && negotiated_group.is_some_and(|g| !is_post_quantum(g)) {
        tracing::warn!(
            peer,
            profile = settings.profile.as_str(),
            group = ?negotiated_group,
            "TLS negotiated a classical key exchange: the peer offers no post-quantum group"
        );
    }
    if negotiated_version == Some(ProtocolVersion::TLSv1_2) {
        tracing::warn!(
            peer,
            profile = settings.profile.as_str(),
            "TLS negotiated 1.2, below the preferred 1.3"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A rustls error is found bare, inside an `io::Error`, and further down a
    /// chain, and an error with none in it gives `None`.
    #[test]
    fn rustls_error_is_found_anywhere_in_the_chain() {
        #[derive(Debug, thiserror::Error)]
        #[error("request failed")]
        struct Outer(#[source] std::io::Error);

        let refused = || rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let wrapped = || std::io::Error::new(std::io::ErrorKind::InvalidData, refused());
        let cases: [(&str, Box<dyn std::error::Error>, bool); 5] = [
            ("bare", Box::new(refused()), true),
            ("in an io::Error", Box::new(wrapped()), true),
            ("a link down", Box::new(Outer(wrapped())), true),
            (
                "an io::Error in an io::Error",
                Box::new(Outer(std::io::Error::other(wrapped()))),
                true,
            ),
            (
                "a refused connection",
                Box::new(Outer(std::io::ErrorKind::ConnectionRefused.into())),
                false,
            ),
        ];
        for (name, error, found) in cases {
            assert_eq!(rustls_error(error.as_ref()).is_some(), found, "{name}");
        }
    }
}
