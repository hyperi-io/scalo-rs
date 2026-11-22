// Project:   scalo
// File:      src/tls/tonic.rs
// Purpose:   Map ClientTls onto tonic's client TLS config
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! tonic builds its own rustls config from PEM bytes, so a gRPC client takes
//! its trust and identity as tonic's [`ClientTlsConfig`], with every file read
//! and parsed here first.

use tonic::transport::{Certificate, ClientTlsConfig, Identity};

use super::TlsError;
use super::client::{ClientTls, TrustChoice, read_certs, read_key};

/// tonic's client TLS config for `tls`, with every file read and parsed.
///
/// tonic has no OS verifier, so [`TrustChoice::Platform`] is refused. tonic
/// fails a client whose OS store is empty even with CA files beside it, so
/// under [`TrustChoice::Native`] an empty OS store is left out when a CA file
/// is given.
///
/// # Errors
///
/// [`TlsError::Build`] for `platform`, [`TlsError::EmptyTrustStore`] when
/// nothing is trusted, and the file errors of
/// [`build_client_tls`](super::build_client_tls).
pub(crate) fn tonic_client_tls(tls: &ClientTls) -> Result<ClientTlsConfig, TlsError> {
    let mut config = ClientTlsConfig::new();
    match tls.trust {
        TrustChoice::Platform => {
            return Err(TlsError::Build(
                "a gRPC client takes `native` or `bundle` trust, not `platform`".to_string(),
            ));
        }
        TrustChoice::Bundle if tls.ca_paths.is_empty() => {
            return Err(TlsError::EmptyTrustStore);
        }
        TrustChoice::Bundle => {}
        TrustChoice::Native if os_store_has_roots() => config = config.with_native_roots(),
        TrustChoice::Native if tls.ca_paths.is_empty() => {
            return Err(TlsError::EmptyTrustStore);
        }
        TrustChoice::Native => {
            tracing::warn!(
                "TLS: the OS root store holds no certificate, so only the CA files are trusted"
            );
        }
    }
    for path in &tls.ca_paths {
        let (pem, _) = read_certs(path)?;
        config = config.ca_certificate(Certificate::from_pem(pem));
    }
    if let Some(identity) = &tls.identity {
        let (cert, _) = read_certs(&identity.cert_path)?;
        let (key, _) = read_key(&identity.key_path)?;
        config = config.identity(Identity::from_pem(cert, key));
    }
    tracing::info!(
        trust = %tls.trust,
        ca_files = tls.ca_paths.len(),
        mutual_tls = tls.identity.is_some(),
        "TLS client config built for gRPC"
    );
    Ok(config)
}

/// Whether the OS root store holds a certificate, read as tonic reads it.
fn os_store_has_roots() -> bool {
    !rustls_native_certs::load_native_certs().certs.is_empty()
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::tls::test_pki::TestCa;

    fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, text).unwrap();
        path
    }

    /// Each file or trust mistake fails the build, naming what was wrong,
    /// where tonic would fail the first RPC instead.
    #[test]
    fn a_mistake_fails_the_build_and_good_settings_build() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("tonic test CA");
        let leaf = ca.client("tonic client");
        let ca_file = write(dir.path(), "ca.pem", &ca.pem());
        let cert = write(dir.path(), "cert.pem", &leaf.cert_pem);
        let key = write(dir.path(), "key.pem", &leaf.key_pem);
        let both = write(
            dir.path(),
            "both.pem",
            &format!("{}{}", leaf.cert_pem, leaf.key_pem),
        );
        let junk = write(dir.path(), "junk.pem", "not a certificate");
        let missing = dir.path().join("missing.pem");

        let refused: [(&str, ClientTls, &str); 6] = [
            (
                "platform trust",
                ClientTls::new(TrustChoice::Platform),
                "not `platform`",
            ),
            (
                "a bundle with no CA",
                ClientTls::new(TrustChoice::Bundle),
                "trust store is empty",
            ),
            (
                "a missing CA file",
                ClientTls::pinned(&missing),
                "missing.pem",
            ),
            (
                "a CA file that is not PEM",
                ClientTls::pinned(&junk),
                "junk.pem",
            ),
            (
                "a client key that is not a key",
                ClientTls::pinned(&ca_file).with_identity(&cert, &cert),
                "client key",
            ),
            (
                "a client certificate that is not a certificate",
                ClientTls::pinned(&ca_file).with_identity(&key, &key),
                "key.pem",
            ),
        ];
        for (name, tls, expected) in refused {
            let error = tonic_client_tls(&tls).err().map(|e| e.to_string());
            assert!(
                error.as_deref().is_some_and(|e| e.contains(expected)),
                "{name}: expected an error naming `{expected}`, got {error:?}"
            );
        }

        let good = [
            ClientTls::pinned(&ca_file),
            ClientTls::pinned(&ca_file).with_identity(&cert, &key),
            ClientTls::pinned(&ca_file).with_identity(&both, &both),
        ];
        for tls in good {
            assert!(tonic_client_tls(&tls).is_ok(), "{tls:?}");
        }
    }
}
