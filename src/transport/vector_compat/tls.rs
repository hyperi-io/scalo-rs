// Project:   scalo
// File:      src/transport/vector_compat/tls.rs
// Purpose:   Caller-owned client TLS for the Vector gRPC sink
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Client TLS for [`VectorCompatClient`](super::VectorCompatClient).
//!
//! The settings are the caller's: a service that ships to a Vector source over
//! TLS builds a [`VectorCompatTls`] from its own config and passes it to
//! [`connect_lazy_tls`](super::VectorCompatClient::connect_lazy_tls).
//!
//! `VectorCompatClient` is a tonic client, so this maps onto tonic's
//! `ClientTlsConfig` through `crate::tls`, as `GrpcTransport` does. The roots
//! use the vocabulary of `TlsTrust` (`native_roots`, `extra_roots`,
//! `exclusive`) and the SNI name and client identity use the one of
//! `GrpcConfig`.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tonic::transport::ClientTlsConfig;

use crate::tls::{ClientTls, TrustChoice};
use crate::transport::error::{TransportError, TransportResult};

/// Client TLS for a [`VectorCompatClient`](super::VectorCompatClient).
///
/// The default trusts the OS root store, verifies the server name from the
/// endpoint, and presents no client certificate. The endpoint must be
/// `https://`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct VectorCompatTls {
    /// Trust the OS root store. Ignored under `exclusive`.
    pub native_roots: bool,

    /// Root-CA PEM files to trust, a bundle or several. Trusted beside the OS
    /// store, or alone under `exclusive`.
    pub extra_roots: Vec<PathBuf>,

    /// Trust only `extra_roots`, and the OS store not at all.
    pub exclusive: bool,

    /// The name to verify the server certificate against, and to send as SNI,
    /// where the endpoint's host is not that name.
    pub domain: Option<String>,

    /// Client certificate PEM for mutual TLS, with `client_key`.
    pub client_cert: Option<PathBuf>,

    /// Client private key PEM for mutual TLS, with `client_cert`.
    pub client_key: Option<PathBuf>,
}

impl Default for VectorCompatTls {
    /// The OS root store, no extra roots, no client certificate.
    fn default() -> Self {
        Self {
            native_roots: true,
            extra_roots: Vec::new(),
            exclusive: false,
            domain: None,
            client_cert: None,
            client_key: None,
        }
    }
}

impl VectorCompatTls {
    /// Trust only the given private-CA PEM bundle.
    #[must_use]
    pub fn private_ca(pem_path: impl Into<PathBuf>) -> Self {
        Self {
            native_roots: false,
            extra_roots: vec![pem_path.into()],
            exclusive: true,
            ..Self::default()
        }
    }

    /// Present a client certificate and its key, for a source that asks for
    /// mutual TLS.
    #[must_use]
    pub fn with_client_identity(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert = Some(cert_path.into());
        self.client_key = Some(key_path.into());
        self
    }

    /// Verify the server certificate against `domain` instead of the
    /// endpoint's host.
    #[must_use]
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Whether this trusts any root at all.
    fn trusts_a_root(&self) -> bool {
        if self.exclusive {
            !self.extra_roots.is_empty()
        } else {
            self.native_roots || !self.extra_roots.is_empty()
        }
    }

    /// tonic's client TLS config for this, with every file read.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when no root is trusted, a certificate has no
    /// key or a key no certificate, or a file cannot be read or holds no PEM
    /// block of the kind it should.
    pub(super) fn client_config(&self) -> TransportResult<ClientTlsConfig> {
        if !self.trusts_a_root() {
            return Err(config_error(
                "no root is trusted: set `native_roots`, or `extra_roots`, \
                 and `exclusive` needs `extra_roots`"
                    .to_string(),
            ));
        }
        let trust = if self.native_roots && !self.exclusive {
            TrustChoice::Native
        } else {
            TrustChoice::Bundle
        };
        let mut tls = self
            .extra_roots
            .iter()
            .fold(ClientTls::new(trust), |tls, path| tls.with_ca(path));
        match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => tls = tls.with_identity(cert, key),
            (None, None) => {}
            _ => {
                return Err(config_error(
                    "mutual TLS needs both `client_cert` and `client_key`".to_string(),
                ));
            }
        }
        let mut tls =
            crate::tls::tonic_client_tls(&tls).map_err(|e| config_error(e.to_string()))?;
        if let Some(domain) = &self.domain {
            tls = tls.domain_name(domain.clone());
        }
        Ok(tls)
    }
}

fn config_error(message: String) -> TransportError {
    TransportError::Config(format!("Vector TLS: {message}"))
}

/// A refusal by tonic of the assembled settings, with the cause tonic keeps
/// beside its own "transport error".
pub(super) fn tonic_error(error: &tonic::transport::Error) -> TransportError {
    let cause = std::error::Error::source(error).map_or_else(String::new, |c| format!(": {c}"));
    config_error(format!("{error}{cause}"))
}

/// An `https://` endpoint, which is the only scheme tonic puts TLS on.
///
/// # Errors
///
/// `TransportError::Config` for any other scheme, which tonic would dial in
/// plaintext with the TLS settings ignored.
pub(super) fn require_https(endpoint: &str) -> TransportResult<()> {
    if endpoint
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
    {
        Ok(())
    } else {
        Err(config_error(
            "the endpoint must start with https:// when TLS is set".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::super::sink::VectorCompatClient;
    use super::*;

    const ENDPOINT: &str = "https://127.0.0.1:9";

    fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        path
    }

    /// Each mistake in the shape of the TLS settings fails when the client is
    /// built, with a message that says which, where tonic would fail the first
    /// RPC with a handshake error or dial in plaintext. File mistakes are
    /// `crate::tls`'s, tested there.
    #[tokio::test]
    async fn a_mistake_in_the_tls_settings_fails_the_build() {
        let dir = tempfile::tempdir().unwrap();
        let pair = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let ca = write(dir.path(), "ca.pem", &pair.cert.pem());
        let key = write(dir.path(), "key.pem", &pair.signing_key.serialize_pem());
        let missing = dir.path().join("missing.pem");
        let build = |tls: &VectorCompatTls, endpoint: &str| {
            VectorCompatClient::connect_lazy_tls(endpoint, 1000, tls).err()
        };
        let no_root = VectorCompatTls {
            native_roots: false,
            ..VectorCompatTls::default()
        };
        let exclusive_alone = VectorCompatTls {
            exclusive: true,
            ..VectorCompatTls::default()
        };
        let cert_only = VectorCompatTls {
            client_cert: Some(ca.clone()),
            ..VectorCompatTls::private_ca(&ca)
        };
        let key_only = VectorCompatTls {
            client_key: Some(key.clone()),
            ..VectorCompatTls::private_ca(&ca)
        };
        let cases = [
            (
                "a plaintext endpoint",
                build(&VectorCompatTls::private_ca(&ca), "http://127.0.0.1:9"),
                "https://",
            ),
            ("no root trusted", build(&no_root, ENDPOINT), "no root"),
            (
                "exclusive with no roots",
                build(&exclusive_alone, ENDPOINT),
                "no root",
            ),
            (
                "a certificate without a key",
                build(&cert_only, ENDPOINT),
                "both `client_cert` and `client_key`",
            ),
            (
                "a key without a certificate",
                build(&key_only, ENDPOINT),
                "both `client_cert` and `client_key`",
            ),
            (
                "a file error from crate::tls",
                build(&VectorCompatTls::private_ca(&missing), ENDPOINT),
                "missing.pem",
            ),
        ];
        for (name, error, expected) in cases {
            let Some(TransportError::Config(message)) = error else {
                panic!("{name}: built a client, or failed another way: {error:?}");
            };
            assert!(
                message.contains(expected),
                "{name}: the error lacks `{expected}`"
            );
            assert!(
                message.starts_with("Vector TLS: "),
                "{name}: the error is not a Vector TLS error"
            );
        }

        let good = VectorCompatTls::private_ca(&ca)
            .with_client_identity(&ca, &key)
            .with_domain("localhost");
        let client = VectorCompatClient::connect_lazy_tls(ENDPOINT, 1000, &good);
        assert!(client.is_ok(), "{:?}", client.err());
    }

    /// The settings read from config as they are written, and take the
    /// defaults for what is left out.
    #[test]
    fn the_settings_read_from_config_with_defaults_for_the_rest() {
        let tls: VectorCompatTls =
            serde_json::from_value(serde_json::json!({ "extra_roots": ["/etc/ca.pem"] })).unwrap();
        assert!(tls.native_roots, "the OS store stays on");
        assert!(!tls.exclusive);
        assert_eq!(tls.extra_roots, [PathBuf::from("/etc/ca.pem")]);
        assert_eq!(tls.client_cert, None);
    }
}
