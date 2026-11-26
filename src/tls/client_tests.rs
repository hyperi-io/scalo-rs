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
use crate::crypto::{CryptoProfile, KeyShare, TlsProfiles};
use crate::tls::TlsError;
use crate::tls::profile::ProfileSettings;
use crate::tls::test_pki::{Leaf, TestCa, pump};

/// The strong profile on the default settings.
fn strong() -> ProfileSettings {
    ProfileSettings::new(
        CryptoProfile::Strong,
        &TlsProfiles::default(),
        KeyShare::Profile,
    )
}

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
    let builder = ServerConfig::builder_with_provider(strong().provider())
        .with_protocol_versions(strong().versions())
        .unwrap();
    let builder = match client_ca {
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(ca.pem().as_bytes()).unwrap())
                .unwrap();
            let verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), strong().provider())
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

/// Build a client from `tls`, on the default profile settings, and run its
/// handshake against `server`.
fn connect(tls: &ClientTls, server: Arc<ServerConfig>) -> Outcome {
    let tls = tls.clone().with_profiles(TlsProfiles::default());
    let config = match build_client_tls(&tls) {
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
        let got = connect(&tls, server_config(leaf, None));
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
            let got = connect(&tls, server);
            assert!(
                is(&expected, &got),
                "{trust} {name}: expected {expected:?}, got {got:?}"
            );
        }
    }
}

/// The profile reaches the built config under every trust choice: against a
/// strong server, cnsa2 agrees AES-256 and P-384, its one group in common.
#[test]
fn the_profile_applies_under_every_trust_choice() {
    let pki = Pki::new();
    for trust in [
        TrustChoice::Native,
        TrustChoice::Platform,
        TrustChoice::Bundle,
    ] {
        let tls = ClientTls::new(trust)
            .with_ca(pki.path("ca_a.pem"))
            .with_profile(CryptoProfile::Cnsa2)
            .with_profiles(TlsProfiles::default());
        let config = build_client_tls(&tls).unwrap();
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
            client.negotiated_cipher_suite().map(|s| s.suite()),
            Some(rustls::CipherSuite::TLS13_AES_256_GCM_SHA384),
            "{trust}"
        );
        assert_eq!(
            client.negotiated_key_exchange_group().map(|g| g.name()),
            Some(rustls::NamedGroup::secp384r1),
            "{trust}"
        );
    }
}

/// Key pins under each trust choice: the pinned key connects, a leaf re-keyed
/// under the same CA is refused, and a pinned key still needs a trusted chain.
#[test]
fn a_key_pin_accepts_its_key_and_refuses_a_rekeyed_leaf() {
    let pki = Pki::new();
    let ca_a = pki.path("ca_a.pem");
    let rekeyed = pki.ca_a.server("server a");
    let pin_a = pki.server_a.pin();
    let pin_rekeyed = rekeyed.pin();
    assert_ne!(pin_a, pin_rekeyed, "a fresh key has a fresh pin");

    for trust in [
        TrustChoice::Native,
        TrustChoice::Platform,
        TrustChoice::Bundle,
    ] {
        let base = || ClientTls::new(trust).with_ca(&ca_a);
        let cases: Vec<(&str, ClientTls, &Leaf, Outcome)> = vec![
            ("no pin", base(), &rekeyed, Outcome::Connects),
            (
                "the key pinned",
                base().with_pin(pin_a),
                &pki.server_a,
                Outcome::Connects,
            ),
            (
                "the leaf re-keyed under the same CA",
                base().with_pin(pin_a),
                &rekeyed,
                Outcome::Refused,
            ),
            (
                "the re-keyed leaf beside the old pin",
                base().with_pin(pin_a).with_pin(pin_rekeyed),
                &rekeyed,
                Outcome::Connects,
            ),
            (
                "the key pinned, its chain not trusted",
                base().with_pin(pki.server_b.pin()),
                &pki.server_b,
                Outcome::Refused,
            ),
        ];
        for (name, tls, leaf, expected) in cases {
            let got = connect(&tls, server_config(leaf, None));
            assert!(
                is(&expected, &got),
                "{trust} {name}: expected {expected:?}, got {got:?}"
            );
        }
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
    let certs = read_certs(&path).unwrap();
    assert_eq!(certs.len(), 2, "the CA and the leaf, not the key");
}

/// With the OS root store empty, `native` with no CA file fails the build,
/// and the HTTP client's mode builds a config whose handshakes all fail as
/// untrusted. `SSL_CERT_FILE` replaces the OS store on every OS.
#[cfg(feature = "http")]
#[test]
fn an_empty_os_store_fails_the_build_or_refuses_every_server() {
    let pki = Pki::new();
    let empty = pki.write("empty.pem", "");
    temp_env::with_vars(
        [
            ("SSL_CERT_FILE", Some(empty.as_os_str())),
            ("SSL_CERT_DIR", None),
        ],
        || {
            let tls = ClientTls::new(TrustChoice::Native).with_profiles(TlsProfiles::default());
            assert!(matches!(
                build_client_tls(&tls),
                Err(TlsError::EmptyTrustStore)
            ));
            let config = super::build_client_tls_with(&tls, super::NoRoots::RefuseEveryServer)
                .expect("built with no roots");
            let mut client =
                ClientConnection::new(config, ServerName::try_from("localhost").unwrap()).unwrap();
            let mut server = ServerConnection::new(server_config(&pki.server_a, None)).unwrap();
            assert_eq!(
                pump(&mut client, &mut server).err(),
                Some(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::UnknownIssuer
                ))
            );
        },
    );
}
