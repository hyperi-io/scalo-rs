// Project:   scalo
// File:      src/tls/server_tests.rs
// Purpose:   Server TLS tests: real handshakes, reload, client auth, timeouts
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls_pki_types::CertificateDer;
use tempfile::{NamedTempFile, TempDir};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;

use super::{ClientAuth, HandshakeError, Reloaded, ServerTls, ServerTlsConfig};
use crate::tls::TlsError;
use crate::tls::test_pki::{Leaf, TestCa, connect, connector, write};

/// A directory holding `cert.pem`, `key.pem` and `ca.pem`.
struct Files {
    dir: TempDir,
}

impl Files {
    fn new(leaf: &Leaf, ca_pem: &str) -> Self {
        let files = Self {
            dir: TempDir::new().unwrap(),
        };
        files.serve(leaf);
        write(&files.ca(), ca_pem);
        files
    }

    fn cert(&self) -> PathBuf {
        self.dir.path().join("cert.pem")
    }

    fn key(&self) -> PathBuf {
        self.dir.path().join("key.pem")
    }

    fn ca(&self) -> PathBuf {
        self.dir.path().join("ca.pem")
    }

    fn serve(&self, leaf: &Leaf) {
        write(&self.cert(), &leaf.cert_pem);
        write(&self.key(), &leaf.key_pem);
    }

    fn config(&self) -> ServerTlsConfig {
        ServerTlsConfig {
            reload_interval_secs: 0,
            ..ServerTlsConfig::from_files(self.cert(), self.key())
        }
    }
}

/// What one handshake against a `ServerTls` showed each side.
struct Handshake {
    /// The leaf the client was served, when its side completed.
    served: Option<CertificateDer<'static>>,
    server: Result<(), HandshakeError>,
}

async fn handshake(tls: &ServerTls, client: &TlsConnector) -> Handshake {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_tls = tls.clone();
    let server_side = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        server_tls.accept(stream, Some(peer)).await.map(drop)
    });
    // The client stream stays open until the server side has finished.
    let client_side = connect(client, addr).await;
    let served = client_side.as_ref().ok().and_then(|stream| {
        let (_, connection) = stream.get_ref();
        connection
            .peer_certificates()
            .and_then(|chain| chain.first().cloned())
    });
    let server_result = server_side.await.unwrap();
    drop(client_side);
    Handshake {
        served,
        server: server_result,
    }
}

fn temp_file(contents: &str) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    write(file.path(), contents);
    file
}

/// A short name for the kind of `e`, for a table to expect.
fn kind(e: &TlsError) -> String {
    match e {
        TlsError::MissingSource { file, .. } => format!("missing {file}"),
        TlsError::ConflictingSource { file, .. } => format!("conflicting {file}"),
        TlsError::ClientCaRequired { mode } => format!("no CA for {mode}"),
        TlsError::InvalidPem { what, .. } => format!("invalid {what}"),
        TlsError::SecretsUnsupported { field } => format!("unsupported {field}"),
        TlsError::PemRead { .. } => "unreadable".to_string(),
        TlsError::Build(_) => "build".to_string(),
        other => format!("unexpected: {other}"),
    }
}

#[tokio::test]
async fn load_refuses_a_config_it_cannot_serve() {
    let ca = TestCa::new("load ca");
    let files = Files::new(&ca.server("load"), &ca.pem());
    let empty_ca = temp_file("not a certificate\n");
    let wrong_key = temp_file(&ca.server("other").key_pem);
    let garbage_cert =
        temp_file("-----BEGIN CERTIFICATE-----\nnot base64 !!\n-----END CERTIFICATE-----\n");
    let cert_spec = Some(format!("file:{}", files.cert().display()));
    let base = files.config();
    let secret_spec = if cfg!(feature = "secrets") {
        Ok(())
    } else {
        Err("unsupported cert_secret")
    };

    let cases: Vec<(&str, ServerTlsConfig, Result<(), &str>)> = vec![
        ("files, no client auth", base.clone(), Ok(())),
        (
            "client auth required with a CA",
            ServerTlsConfig {
                client_auth: ClientAuth::Required,
                ca_files: vec![files.ca()],
                ..base.clone()
            },
            Ok(()),
        ),
        (
            "client auth required with two CA files",
            ServerTlsConfig {
                client_auth: ClientAuth::Required,
                ca_files: vec![files.ca(), files.ca()],
                ..base.clone()
            },
            Ok(()),
        ),
        (
            "client CA from files and a secret",
            ServerTlsConfig {
                client_auth: ClientAuth::Required,
                ca_files: vec![files.ca()],
                ca_secret: cert_spec.clone(),
                ..base.clone()
            },
            Err("conflicting ca_files"),
        ),
        (
            "no certificate",
            ServerTlsConfig {
                cert_file: None,
                ..base.clone()
            },
            Err("missing cert_file"),
        ),
        (
            "no key",
            ServerTlsConfig {
                key_file: None,
                ..base.clone()
            },
            Err("missing key_file"),
        ),
        (
            "certificate from a file and a secret",
            ServerTlsConfig {
                cert_secret: cert_spec.clone(),
                ..base.clone()
            },
            Err("conflicting cert_file"),
        ),
        (
            "client auth required with no CA",
            ServerTlsConfig {
                client_auth: ClientAuth::Required,
                ..base.clone()
            },
            Err("no CA for required"),
        ),
        (
            "client auth optional with no CA",
            ServerTlsConfig {
                client_auth: ClientAuth::Optional,
                ..base.clone()
            },
            Err("no CA for optional"),
        ),
        (
            "CA file with no certificate",
            ServerTlsConfig {
                client_auth: ClientAuth::Required,
                ca_files: vec![empty_ca.path().to_path_buf()],
                ..base.clone()
            },
            Err("invalid client CA"),
        ),
        (
            "key from another certificate",
            ServerTlsConfig {
                key_file: Some(wrong_key.path().to_path_buf()),
                ..base.clone()
            },
            Err("build"),
        ),
        (
            "certificate that is not base64",
            ServerTlsConfig {
                cert_file: Some(garbage_cert.path().to_path_buf()),
                ..base.clone()
            },
            Err("invalid certificate"),
        ),
        (
            "certificate path that does not exist",
            ServerTlsConfig {
                cert_file: Some(files.dir.path().join("missing.pem")),
                ..base.clone()
            },
            Err("unreadable"),
        ),
        (
            "certificate from a secret spec",
            ServerTlsConfig {
                cert_file: None,
                cert_secret: cert_spec.clone(),
                ..base.clone()
            },
            secret_spec,
        ),
    ];

    for (name, config, expected) in cases {
        let result = ServerTls::load(&config)
            .await
            .map(drop)
            .map_err(|e| kind(&e));
        assert_eq!(result, expected.map_err(str::to_string), "{name}");
    }
}

/// What a step does to the files before the reload.
enum Change<'a> {
    Nothing,
    Serve(&'a Leaf),
    Cert(&'a str),
    RemoveCert,
}

#[tokio::test]
async fn reload_swaps_the_certificate_and_keeps_it_through_bad_sources() {
    let ca = TestCa::new("reload ca");
    let (a, b, c) = (ca.server("a"), ca.server("b"), ca.server("c"));
    let files = Files::new(&a, &ca.pem());
    let tls = ServerTls::load(&files.config()).await.unwrap();
    let client = connector(&ca.pem(), None, &[]);
    assert_eq!(handshake(&tls, &client).await.served, Some(a.der.clone()));

    let swapped = Ok(Reloaded {
        certificate: true,
        client_ca: false,
    });
    let unchanged = Ok(Reloaded::default());
    let refused = Err(());
    let steps: Vec<(&str, Change<'_>, Result<Reloaded, ()>, &Leaf)> = vec![
        ("rotate to b", Change::Serve(&b), swapped, &b),
        ("nothing changed", Change::Nothing, unchanged, &b),
        (
            "garbage certificate",
            Change::Cert("garbage\n"),
            refused,
            &b,
        ),
        // Mid-rotation: the certificate is written before its key.
        (
            "c's certificate, b's key",
            Change::Cert(&c.cert_pem),
            refused,
            &b,
        ),
        ("certificate removed", Change::RemoveCert, refused, &b),
        ("rotate to c", Change::Serve(&c), swapped, &c),
    ];

    for (name, change, expected, served) in steps {
        match change {
            Change::Nothing => {}
            Change::Serve(leaf) => files.serve(leaf),
            Change::Cert(pem) => write(&files.cert(), pem),
            Change::RemoveCert => tokio::fs::remove_file(files.cert()).await.unwrap(),
        }
        let result = tls.reload().await.map_err(drop);
        assert_eq!(result, expected, "{name}");
        let outcome = handshake(&tls, &client).await;
        assert!(outcome.server.is_ok(), "{name}: {:?}", outcome.server);
        assert_eq!(outcome.served.as_ref(), Some(&served.der), "{name}");
        assert_eq!(&tls.resolver().current().cert[0], &served.der, "{name}");
    }
}

#[tokio::test]
async fn the_reload_task_serves_a_rotated_file_with_no_call() {
    let ca = TestCa::new("poll ca");
    let (a, b) = (ca.server("a"), ca.server("b"));
    let files = Files::new(&a, &ca.pem());
    let config = ServerTlsConfig {
        reload_interval_secs: 1,
        ..files.config()
    };
    let tls = ServerTls::load(&config).await.unwrap();
    let client = connector(&ca.pem(), None, &[]);

    files.serve(&b);
    // The task re-reads once a second, so five seconds is several polls.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let outcome = handshake(&tls, &client).await;
        if outcome.served.as_ref() == Some(&b.der) {
            break;
        }
        assert_eq!(outcome.served.as_ref(), Some(&a.der));
        assert!(Instant::now() < deadline, "b was never served");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn the_reload_task_holds_no_strong_reference() {
    let ca = TestCa::new("task ca");
    let files = Files::new(&ca.server("a"), &ca.pem());
    let config = ServerTlsConfig {
        reload_interval_secs: 1,
        ..files.config()
    };
    let tls = ServerTls::load(&config).await.unwrap();
    let inner = Arc::downgrade(&tls.inner);
    drop(tls);
    assert!(
        inner.upgrade().is_none(),
        "the reload task kept the ServerTls alive"
    );
}

#[tokio::test]
async fn client_auth_modes_admit_the_clients_they_should() {
    let ca = TestCa::new("mtls ca");
    let stranger = TestCa::new("stranger ca");
    let files = Files::new(&ca.server("server"), &ca.pem());
    let trusted = ca.client("trusted");
    let untrusted = stranger.client("untrusted");

    let cases = [
        ("none, no certificate", ClientAuth::None, None, true),
        ("optional, no certificate", ClientAuth::Optional, None, true),
        (
            "optional, trusted",
            ClientAuth::Optional,
            Some(&trusted),
            true,
        ),
        (
            "optional, untrusted",
            ClientAuth::Optional,
            Some(&untrusted),
            false,
        ),
        (
            "required, no certificate",
            ClientAuth::Required,
            None,
            false,
        ),
        (
            "required, trusted",
            ClientAuth::Required,
            Some(&trusted),
            true,
        ),
        (
            "required, untrusted",
            ClientAuth::Required,
            Some(&untrusted),
            false,
        ),
    ];
    for (name, mode, identity, admitted) in cases {
        let config = ServerTlsConfig {
            client_auth: mode,
            ca_files: if mode == ClientAuth::None {
                Vec::new()
            } else {
                vec![files.ca()]
            },
            ..files.config()
        };
        let tls = ServerTls::load(&config).await.unwrap();
        let client = connector(&ca.pem(), identity, &[]);
        let outcome = handshake(&tls, &client).await;
        assert_eq!(outcome.server.is_ok(), admitted, "{name}");
        if !admitted {
            assert!(
                matches!(outcome.server, Err(HandshakeError::Failed(_))),
                "{name}"
            );
        }
    }
}

#[tokio::test]
async fn a_rotated_client_ca_admits_the_new_clients() {
    let server_ca = TestCa::new("server ca");
    let (old_ca, new_ca) = (TestCa::new("old client ca"), TestCa::new("new client ca"));
    let files = Files::new(&server_ca.server("server"), &old_ca.pem());
    let config = ServerTlsConfig {
        client_auth: ClientAuth::Required,
        ca_files: vec![files.ca()],
        ..files.config()
    };
    let tls = ServerTls::load(&config).await.unwrap();
    let new_client = connector(&server_ca.pem(), Some(&new_ca.client("new")), &[]);
    assert!(handshake(&tls, &new_client).await.server.is_err());

    write(&files.ca(), &new_ca.pem());
    let reloaded = tls.reload().await.unwrap();
    assert_eq!(
        reloaded,
        Reloaded {
            certificate: false,
            client_ca: true
        }
    );
    let outcome = handshake(&tls, &new_client).await;
    assert!(outcome.server.is_ok(), "{:?}", outcome.server);
}

#[tokio::test]
async fn a_silent_client_times_out() {
    let ca = TestCa::new("timeout ca");
    let files = Files::new(&ca.server("server"), &ca.pem());
    let config = ServerTlsConfig {
        handshake_timeout_ms: 200,
        ..files.config()
    };
    let tls = ServerTls::load(&config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (stream, peer) = listener.accept().await.unwrap();
    let result = tls.accept(stream, Some(peer)).await;
    assert!(
        matches!(result, Err(HandshakeError::Timeout(t)) if t == Duration::from_millis(200)),
        "{result:?}"
    );
    drop(silent);
}

#[test]
fn config_debug_hides_secret_specs() {
    let config = ServerTlsConfig {
        key_secret: Some("-----BEGIN PRIVATE KEY-----".to_string()),
        ..ServerTlsConfig::default()
    };
    let shown = format!("{config:?}");
    assert!(!shown.contains("BEGIN"), "{shown}");
    assert!(shown.contains("<set>"), "{shown}");
}

#[cfg(feature = "serde_json")]
#[test]
fn client_auth_reads_its_spellings() {
    let cases = [
        ("none", Some(ClientAuth::None)),
        ("optional", Some(ClientAuth::Optional)),
        ("request", Some(ClientAuth::Optional)),
        ("required", Some(ClientAuth::Required)),
        ("require", Some(ClientAuth::Required)),
        ("requried", None),
    ];
    for (text, mode) in cases {
        let parsed: Option<ClientAuth> = serde_json::from_value(serde_json::json!(text)).ok();
        assert_eq!(parsed, mode, "{text}");
    }
}
