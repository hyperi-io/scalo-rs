// Project:   scalo
// File:      src/tls/client_tests.rs
// Purpose:   Client TLS tests: each trust choice, the pin and mutual TLS
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::path::PathBuf;
use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConnection, RootCertStore, ServerConfig, ServerConnection};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tempfile::TempDir;

use super::{ClientTls, TrustChoice, build_client_tls, read_certs};
use crate::crypto::{CryptoProfile, PqcMode};
use crate::tls::test_pki::{Leaf, TestCa, pump};
use crate::tls::{TlsError, posture_provider};

/// What building a client and running its handshake came to.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Connects,
    /// The handshake failed, or the build failed because nothing is trusted.
    Refused,
    /// The build failed with this error text.
    BuildFails(String),
}

/// A build failure whose error holds `text`.
fn build_fails(text: &str) -> Outcome {
    Outcome::BuildFails(text.to_string())
}

/// Whether `got` is `expected`, where a `BuildFails` expectation matches any
/// build error holding its text.
fn is(expected: &Outcome, got: &Outcome) -> bool {
    match (expected, got) {
        (Outcome::BuildFails(want), Outcome::BuildFails(have)) => have.contains(want.as_str()),
        _ => expected == got,
    }
}

/// Two CAs, a server certificate from each, and their files on disk.
struct Pki {
    dir: TempDir,
    ca_a: TestCa,
    server_a: Leaf,
    server_b: Leaf,
}

impl Pki {
    fn new() -> Self {
        let ca_a = TestCa::new("client test CA a");
        let ca_b = TestCa::new("client test CA b");
        let pki = Self {
            dir: TempDir::new().unwrap(),
            server_a: ca_a.server("server a"),
            server_b: ca_b.server("server b"),
            ca_a,
        };
        pki.write("ca_a.pem", &pki.ca_a.pem());
        pki.write("ca_b.pem", &ca_b.pem());
        pki.write("junk.pem", "not a certificate\n");
        pki
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn write(&self, name: &str, text: &str) -> PathBuf {
        let path = self.path(name);
        std::fs::write(&path, text).unwrap();
        path
    }
}

/// A server presenting `leaf`, asking for a client certificate from
/// `client_ca` when one is given.
fn server_config(leaf: &Leaf, client_ca: Option<&TestCa>) -> Arc<ServerConfig> {
    let builder = ServerConfig::builder_with_provider(posture_provider(PqcMode::Prefer))
        .with_safe_default_protocol_versions()
        .unwrap();
    let builder = match client_ca {
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(ca.pem().as_bytes()).unwrap())
                .unwrap();
            let verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                posture_provider(PqcMode::Prefer),
            )
            .build()
            .unwrap();
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    Arc::new(
        builder
            .with_single_cert(
                vec![leaf.der.clone()],
                PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap(),
            )
            .unwrap(),
    )
}

/// Build a client from `tls` on `profile` and run its handshake against
/// `server`.
fn connect(profile: CryptoProfile, tls: &ClientTls, server: Arc<ServerConfig>) -> Outcome {
    let config = match build_client_tls(profile, tls) {
        Ok(config) => config,
        Err(TlsError::EmptyTrustStore) => return Outcome::Refused,
        Err(e) => return Outcome::BuildFails(e.to_string()),
    };
    let mut client =
        ClientConnection::new(config, ServerName::try_from("localhost").unwrap()).unwrap();
    let mut server = ServerConnection::new(server).unwrap();
    match pump(&mut client, &mut server) {
        Ok(()) => Outcome::Connects,
        Err(_) => Outcome::Refused,
    }
}

/// Each trust choice and the pin, against a server from the CA given and one
/// from a CA nothing names. The test CAs are in no OS store, so `native` and
/// `platform` with no CA file refuse both.
#[test]
fn each_trust_choice_trusts_what_it_names_and_nothing_else() {
    let pki = Pki::new();
    let ca_a = pki.path("ca_a.pem");
    let native = || ClientTls::new(TrustChoice::Native);
    let platform = || ClientTls::new(TrustChoice::Platform);
    let cases: Vec<(&str, ClientTls, &Leaf, Outcome)> = vec![
        (
            "native with the CA",
            native().with_ca(&ca_a),
            &pki.server_a,
            Outcome::Connects,
        ),
        (
            "native with the CA, server from another CA",
            native().with_ca(&ca_a),
            &pki.server_b,
            Outcome::Refused,
        ),
        ("native alone", native(), &pki.server_a, Outcome::Refused),
        (
            "platform with the CA",
            platform().with_ca(&ca_a),
            &pki.server_a,
            Outcome::Connects,
        ),
        (
            "platform with the CA, server from another CA",
            platform().with_ca(&ca_a),
            &pki.server_b,
            Outcome::Refused,
        ),
        (
            "platform alone",
            platform(),
            &pki.server_a,
            Outcome::Refused,
        ),
        (
            "the pin",
            ClientTls::pinned(&ca_a),
            &pki.server_a,
            Outcome::Connects,
        ),
        (
            "the pin, server from another CA",
            ClientTls::pinned(&ca_a),
            &pki.server_b,
            Outcome::Refused,
        ),
        (
            "the pin over two CA files",
            ClientTls::pinned(pki.path("ca_b.pem")).with_ca(&ca_a),
            &pki.server_a,
            Outcome::Connects,
        ),
        (
            "a bundle with no CA file",
            ClientTls::new(TrustChoice::Bundle),
            &pki.server_a,
            Outcome::Refused,
        ),
        (
            "a CA file with no certificate",
            ClientTls::pinned(pki.path("junk.pem")),
            &pki.server_a,
            build_fails("no certificates parsed from"),
        ),
        (
            "a CA file that is not there",
            platform().with_ca(pki.path("missing.pem")),
            &pki.server_a,
            build_fails("missing.pem"),
        ),
        (
            "a CA path that is a directory",
            ClientTls::pinned(pki.dir.path()),
            &pki.server_a,
            build_fails("cannot read certificate file"),
        ),
    ];
    for (name, tls, leaf, expected) in cases {
        let got = connect(CryptoProfile::Prod, &tls, server_config(leaf, None));
        assert!(
            is(&expected, &got),
            "{name}: expected {expected:?}, got {got:?}"
        );
    }
}

/// A server that asks for a client certificate from CA a, against each trust
/// choice presenting each kind of identity.
#[test]
fn mutual_tls_presents_the_identity_under_every_trust_choice() {
    let pki = Pki::new();
    let other_ca = TestCa::new("client test CA c");
    let good = pki.ca_a.client("client a");
    let foreign = other_ca.client("client c");
    let cert = pki.write("cert.pem", &good.cert_pem);
    let key = pki.write("key.pem", &good.key_pem);
    let both = pki.write("both.pem", &format!("{}{}", good.cert_pem, good.key_pem));
    let foreign_cert = pki.write("foreign_cert.pem", &foreign.cert_pem);
    let foreign_key = pki.write("foreign_key.pem", &foreign.key_pem);
    let ca_a = pki.path("ca_a.pem");

    for trust in [
        TrustChoice::Native,
        TrustChoice::Platform,
        TrustChoice::Bundle,
    ] {
        let base = || ClientTls::new(trust).with_ca(&ca_a);
        let cases: Vec<(&str, ClientTls, Outcome)> = vec![
            (
                "the identity",
                base().with_identity(&cert, &key),
                Outcome::Connects,
            ),
            (
                "certificate and key in one file",
                base().with_identity(&both, &both),
                Outcome::Connects,
            ),
            ("no identity", base(), Outcome::Refused),
            (
                "an identity from another CA",
                base().with_identity(&foreign_cert, &foreign_key),
                Outcome::Refused,
            ),
            (
                "a key that does not match the certificate",
                base().with_identity(&cert, &foreign_key),
                build_fails("client certificate"),
            ),
            (
                "a key file holding no key",
                base().with_identity(&cert, &cert),
                build_fails("client key"),
            ),
            (
                "a certificate file that is not there",
                base().with_identity(pki.path("gone.pem"), &key),
                build_fails("gone.pem"),
            ),
        ];
        for (name, tls, expected) in cases {
            let server = server_config(&pki.server_a, Some(&pki.ca_a));
            let got = connect(CryptoProfile::Prod, &tls, server);
            assert!(
                is(&expected, &got),
                "{trust} {name}: expected {expected:?}, got {got:?}"
            );
        }
    }
}

/// The profile reaches the built config: HighSec negotiates TLS 1.3 and the
/// hybrid group under every trust choice.
#[test]
fn the_profile_applies_under_every_trust_choice() {
    let pki = Pki::new();
    for trust in [
        TrustChoice::Native,
        TrustChoice::Platform,
        TrustChoice::Bundle,
    ] {
        let tls = ClientTls::new(trust).with_ca(pki.path("ca_a.pem"));
        let config = build_client_tls(CryptoProfile::HighSec, &tls).unwrap();
        let mut client =
            ClientConnection::new(config, ServerName::try_from("localhost").unwrap()).unwrap();
        let mut server = ServerConnection::new(server_config(&pki.server_a, None)).unwrap();
        pump(&mut client, &mut server).unwrap();
        assert_eq!(
            client.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3),
            "{trust}"
        );
        assert_eq!(
            client.negotiated_key_exchange_group().map(|g| g.name()),
            Some(rustls::NamedGroup::X25519MLKEM768),
            "{trust}"
        );
    }
}

/// Config spells the choices in lower case and refuses any other word.
#[test]
fn trust_choice_reads_its_three_names_and_refuses_others() {
    use serde::Deserialize;
    use serde::de::value::{Error, StrDeserializer};

    let read = |text: &str| {
        TrustChoice::deserialize(StrDeserializer::<Error>::new(text)).map_err(|e| e.to_string())
    };
    for choice in [
        TrustChoice::Native,
        TrustChoice::Platform,
        TrustChoice::Bundle,
    ] {
        assert_eq!(read(&choice.to_string()), Ok(choice));
    }
    for word in ["system", "Native", ""] {
        assert!(read(word).is_err(), "{word:?} must not parse");
    }
    assert_eq!(TrustChoice::default(), TrustChoice::Native);
}

/// A bundle keeps its certificates and skips a key and the text around them.
#[test]
fn a_bundle_file_yields_its_certificates_and_skips_the_rest() {
    let pki = Pki::new();
    let leaf = pki.ca_a.client("bundled");
    let path = pki.write(
        "mixed.pem",
        &format!(
            "comment line\n{}{}{}trailer\n",
            pki.ca_a.pem(),
            leaf.key_pem,
            leaf.cert_pem
        ),
    );
    let (bytes, certs) = read_certs(&path).unwrap();
    assert_eq!(certs.len(), 2, "the CA and the leaf, not the key");
    assert_eq!(bytes, std::fs::read(&path).unwrap());
}
