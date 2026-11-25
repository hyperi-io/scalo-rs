// Project:   scalo
// File:      src/tls/matrix_tests.rs
// Purpose:   Profile handshake matrix: scalo client profile against scalo server profile
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Every client profile against every server profile, each built through the
//! one builder on each side, and TLS 1.2 against a profile with and without
//! `allow_tls12`.

use std::sync::Arc;

use rustls::{CipherSuite as C, ClientConnection, NamedGroup as G, ServerConnection};
use rustls_pki_types::ServerName;

use super::test_pki::{Agreed, TestCa, pump, server_tls};
use super::{ClientTls, TlsFailure, build_client_tls};
use crate::crypto::{CryptoProfile, TlsProfiles};

/// What a client's handshake came to: what it agreed, or why it failed.
type Outcome = Result<Agreed, Option<TlsFailure>>;

/// Run `client`'s handshake against `server` in memory.
fn handshake(client: &ClientTls, server: Arc<rustls::ServerConfig>) -> Outcome {
    let settings = client.profile_settings().unwrap();
    let config = build_client_tls(client).unwrap();
    let mut client =
        ClientConnection::new(config, ServerName::try_from("localhost").unwrap()).unwrap();
    let mut server = ServerConnection::new(server).unwrap();
    match pump(&mut client, &mut server) {
        Ok(()) => Ok(Agreed::of(&client)),
        Err(e) => {
            // The client's own error names the failure: the server's arrives as an alert.
            let mut sink = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut sink).unwrap();
            }
            let mut rd: &[u8] = &sink;
            while !rd.is_empty() && client.read_tls(&mut rd).is_ok() {}
            let seen = client.process_new_packets().err().unwrap_or(e);
            Err(TlsFailure::of(&seen, settings))
        }
    }
}

fn client(ca: &std::path::Path, profile: CryptoProfile, profiles: TlsProfiles) -> ClientTls {
    ClientTls::pinned(ca)
        .with_profile(profile)
        .with_profiles(profiles)
}

/// Each client profile against each server profile agrees TLS 1.3 with the
/// suite the client prefers among those the server takes, and the client's
/// first group the server takes.
#[tokio::test]
async fn every_client_profile_against_every_server_profile() {
    use CryptoProfile::{Cnsa2, Commercial, Strong};

    let ca = TestCa::new("matrix ca");
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("ca.pem");
    tokio::fs::write(&ca_file, ca.pem()).await.unwrap();

    let hybrid = |suite| Ok(Agreed::tls13(suite, G::X25519MLKEM768));
    let p384 = || Ok(Agreed::tls13(C::TLS13_AES_256_GCM_SHA384, G::secp384r1));
    let cases: [(CryptoProfile, CryptoProfile, Outcome); 9] = [
        (Strong, Strong, hybrid(C::TLS13_AES_256_GCM_SHA384)),
        (Strong, Commercial, hybrid(C::TLS13_AES_256_GCM_SHA384)),
        (Strong, Cnsa2, p384()),
        (Commercial, Strong, hybrid(C::TLS13_AES_128_GCM_SHA256)),
        (Commercial, Commercial, hybrid(C::TLS13_AES_128_GCM_SHA256)),
        (Commercial, Cnsa2, p384()),
        (Cnsa2, Strong, p384()),
        (Cnsa2, Commercial, p384()),
        (
            Cnsa2,
            Cnsa2,
            Ok(Agreed::tls13(C::TLS13_AES_256_GCM_SHA384, G::MLKEM1024)),
        ),
    ];
    for (client_profile, server_profile, want) in cases {
        let (server, _files) = server_tls(&ca, server_profile, &[]).await;
        let got = handshake(
            &client(&ca_file, client_profile, TlsProfiles::default()),
            server.server_config(),
        );
        assert_eq!(
            got, want,
            "{client_profile} client, {server_profile} server"
        );
    }
}

/// cnsa2 refuses a server certificate on a P-256 key with the typed error
/// that names the next profile, and strong takes it.
#[tokio::test]
async fn cnsa2_refuses_a_p256_server_naming_the_next_profile() {
    use rustls_pki_types::pem::PemObject;

    let ca = TestCa::new("p256 ca");
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("ca.pem");
    tokio::fs::write(&ca_file, ca.pem()).await.unwrap();
    let leaf = ca.server_p256("p256");
    let server = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![leaf.der.clone()],
        rustls_pki_types::PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap(),
    )
    .unwrap();
    let server = Arc::new(server);

    let cnsa2 = handshake(
        &client(&ca_file, CryptoProfile::Cnsa2, TlsProfiles::default()),
        Arc::clone(&server),
    );
    assert_eq!(
        cnsa2,
        Err(Some(TlsFailure::NoSharedParameters {
            profile: CryptoProfile::Cnsa2,
            wider: Some(CryptoProfile::Strong),
        }))
    );
    let strong = handshake(
        &client(&ca_file, CryptoProfile::Strong, TlsProfiles::default()),
        server,
    );
    assert!(strong.is_ok(), "{strong:?}");
}

/// A TLS 1.2-only server is refused by the default strong client with the
/// typed error that names allow_tls12, and agreed with once the profile sets
/// it, on strong's own TLS 1.2 suite and a classical group.
#[tokio::test]
async fn a_tls12_peer_is_refused_by_default_and_agreed_with_allow_tls12() {
    use rustls_pki_types::pem::PemObject;

    let ca = TestCa::new("tls12 ca");
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("ca.pem");
    tokio::fs::write(&ca_file, ca.pem()).await.unwrap();
    let leaf = ca.server("tls12");
    let server = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS12])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![leaf.der.clone()],
        rustls_pki_types::PrivateKeyDer::from_pem_slice(leaf.key_pem.as_bytes()).unwrap(),
    )
    .unwrap();
    let server = Arc::new(server);

    let refused = handshake(
        &client(&ca_file, CryptoProfile::Strong, TlsProfiles::default()),
        Arc::clone(&server),
    );
    let failure = refused.expect_err("strong is TLS 1.3 only by default");
    assert_eq!(
        failure.as_ref().map(ToString::to_string).as_deref(),
        Some("peer supports only TLS 1.2: enable allow_tls12 on profile strong")
    );

    let allowed = TlsProfiles::default().with_tls12(CryptoProfile::Strong, true);
    let agreed = handshake(&client(&ca_file, CryptoProfile::Strong, allowed), server);
    assert_eq!(
        agreed,
        Ok(Agreed {
            version: Some(rustls::ProtocolVersion::TLSv1_2),
            suite: Some(C::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384),
            group: Some(G::secp384r1),
        })
    );
}

/// A scalo server on strong refuses a TLS 1.2-only client by default and
/// takes it once the server's profile sets allow_tls12.
#[tokio::test]
async fn a_strong_server_takes_tls12_only_with_allow_tls12() {
    let ca = TestCa::new("server tls12 ca");
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("ca.pem");
    tokio::fs::write(&ca_file, ca.pem()).await.unwrap();
    let commercial_tls12 = || {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(super::client::read_certs(&ca_file).unwrap());
        Arc::new(
            rustls::ClientConfig::builder_with_provider(
                rustls::crypto::aws_lc_rs::default_provider().into(),
            )
            .with_protocol_versions(&[&rustls::version::TLS12])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
        )
    };

    let leaf = ca.server("strong server");
    let (cert, key) = (dir.path().join("cert.pem"), dir.path().join("key.pem"));
    tokio::fs::write(&cert, &leaf.cert_pem).await.unwrap();
    tokio::fs::write(&key, &leaf.key_pem).await.unwrap();
    let config = super::ServerTlsConfig {
        reload_interval_secs: 0,
        ..super::ServerTlsConfig::from_files(cert, key)
    };
    for (allow, want_ok) in [(false, false), (true, true)] {
        let profiles = TlsProfiles::default().with_tls12(CryptoProfile::Strong, allow);
        let options = super::ServerTlsOptions::default().with_profiles(profiles);
        let server = super::ServerTls::load_with(&config, options).await.unwrap();
        let mut client = ClientConnection::new(
            commercial_tls12(),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut conn = ServerConnection::new(server.server_config()).unwrap();
        let result = pump(&mut client, &mut conn);
        assert_eq!(result.is_ok(), want_ok, "allow_tls12 {allow}: {result:?}");
        if want_ok {
            assert_eq!(
                conn.protocol_version(),
                Some(rustls::ProtocolVersion::TLSv1_2)
            );
        }
    }
}
