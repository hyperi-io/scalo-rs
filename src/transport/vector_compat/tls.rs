// Project:   scalo
// File:      src/transport/vector_compat/tls.rs
// Purpose:   Client TLS for the Vector gRPC sink, from the one client TLS section
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Client TLS for [`VectorCompatClient`](super::VectorCompatClient).
//!
//! A service that ships to a Vector source over TLS passes the
//! [`ClientTlsConfig`] it read to
//! [`connect_lazy_tls`](super::VectorCompatClient::connect_lazy_tls), the same
//! section every other transport reads, and the channel runs scalo's rustls
//! client over each stream it dials.

use crate::tls::ClientTlsConfig;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::tonic_channel::{ChannelTls, origin_host};

/// The channel TLS `section` names for `endpoint`, with every file read.
///
/// # Errors
///
/// `TransportError::Config` when a file cannot be read or holds no PEM block
/// of the kind it should, nothing is trusted, a certificate has no key or a
/// key no certificate, or the profile settings are refused.
pub(super) fn channel_tls(
    endpoint: &str,
    section: &ClientTlsConfig,
) -> TransportResult<ChannelTls> {
    let tls = section
        .client_tls()
        .map_err(|e| config_error(e.to_string()))?;
    let rustls = crate::tls::build_client_tls(&tls).map_err(|e| config_error(e.to_string()))?;
    let name = match &tls.server_name {
        Some(name) => name.clone(),
        None => origin_host(endpoint).map_err(config_error)?,
    };
    ChannelTls::new(&rustls, &name).map_err(config_error)
}

pub(super) fn config_error(message: String) -> TransportError {
    TransportError::Config(format!("Vector TLS: {message}"))
}

/// An `https://` endpoint, the only scheme a TLS client dials.
///
/// # Errors
///
/// `TransportError::Config` for any other scheme.
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
    use std::path::{Path, PathBuf};

    use super::super::sink::VectorCompatClient;
    use super::*;
    use crate::tls::TrustChoice;

    const ENDPOINT: &str = "https://127.0.0.1:9";

    fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        path
    }

    /// Each mistake in the TLS settings fails when the client is built, with
    /// a message that says which, where tonic would fail the first RPC with a
    /// handshake error or dial in plain text.
    #[tokio::test]
    async fn a_mistake_in_the_tls_settings_fails_the_build() {
        let dir = tempfile::tempdir().unwrap();
        let ca_pki = crate::tls::test_pki::TestCa::new("vector settings ca");
        let leaf = ca_pki.client("vector client");
        let ca = write(dir.path(), "ca.pem", &ca_pki.pem());
        let cert = write(dir.path(), "cert.pem", &leaf.cert_pem);
        let key = write(dir.path(), "key.pem", &leaf.key_pem);
        let missing = dir.path().join("missing.pem");
        let build = |tls: &ClientTlsConfig, endpoint: &str| {
            VectorCompatClient::connect_lazy_tls(endpoint, 1000, tls).err()
        };
        let pinned = |path: &Path| ClientTlsConfig {
            trust: TrustChoice::Bundle,
            ca_paths: vec![path.to_path_buf()],
            server_name: Some("localhost".to_string()),
            ..ClientTlsConfig::default()
        };
        let cases = [
            (
                "a plaintext endpoint",
                build(&pinned(&ca), "http://127.0.0.1:9"),
                "https://",
            ),
            (
                "a bundle with no CA",
                build(
                    &ClientTlsConfig {
                        trust: TrustChoice::Bundle,
                        ..ClientTlsConfig::default()
                    },
                    ENDPOINT,
                ),
                "no trusted root",
            ),
            (
                "a certificate without a key",
                build(
                    &ClientTlsConfig {
                        client_cert_path: Some(cert.clone()),
                        ..pinned(&ca)
                    },
                    ENDPOINT,
                ),
                "both client_cert_path and client_key_path",
            ),
            (
                "a missing CA file",
                build(&pinned(&missing), ENDPOINT),
                "missing.pem",
            ),
        ];
        for (name, error, expected) in cases {
            let Some(TransportError::Config(message)) = error else {
                panic!("{name}: built a client, or failed another way: {error:?}");
            };
            assert!(
                message.contains(expected),
                "{name}: the error lacks `{expected}`: {message}"
            );
            assert!(
                message.starts_with("Vector TLS: "),
                "{name}: the error is not a Vector TLS error"
            );
        }

        let good = ClientTlsConfig {
            client_cert_path: Some(cert),
            client_key_path: Some(key),
            ..pinned(&ca)
        };
        let client = VectorCompatClient::connect_lazy_tls(ENDPOINT, 1000, &good);
        assert!(client.is_ok(), "{:?}", client.err());
    }
}
