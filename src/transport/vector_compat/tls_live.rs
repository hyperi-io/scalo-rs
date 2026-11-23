// Project:   scalo
// File:      src/transport/vector_compat/tls_live.rs
// Purpose:   VectorCompatClient TLS against a TLS gRPC source, no mocks
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Vector client over TLS to a source that is the real
//! [`VectorCompatService`] behind tonic's TLS server, with a certificate from a
//! throwaway CA. Needs the `tls` feature for the test CA.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

use super::VectorCompatClient;
use super::proto::vector::vector_server::VectorServer;
use super::source::VectorCompatService;
use super::tls::VectorCompatTls;
use crate::tls::test_pki::TestCa;
use crate::transport::grpc::GrpcToken;
use crate::transport::types::Message;

/// A running source, stopped when it drops.
struct Source {
    addr: SocketAddr,
    events: mpsc::Receiver<Message<GrpcToken>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Source {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Source {
    fn endpoint(&self) -> String {
        format!("https://127.0.0.1:{}", self.addr.port())
    }
}

/// A source with a certificate from `ca`, asking for a client certificate from
/// the same CA when `ask_for_client`.
async fn source(ca: &TestCa, ask_for_client: bool) -> Source {
    crate::tls::install_default_provider();
    let leaf = ca.server("vector-source");
    let mut tls =
        ServerTlsConfig::new().identity(Identity::from_pem(&leaf.cert_pem, &leaf.key_pem));
    if ask_for_client {
        tls = tls.client_ca_root(Certificate::from_pem(ca.pem()));
    }
    let (sender, events) = mpsc::channel(16);
    let service = VectorCompatService::new(sender, Arc::new(AtomicU64::new(0)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(
                VectorServer::new(service)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    Source { addr, events, task }
}

/// Push one event and say whether the source took it. An accepted push reaches
/// the source's queue and a failed one does not, and the check holds both.
async fn push(client: &VectorCompatClient, source: &mut Source, seq: u32) -> bool {
    let outcome = client
        .send_events_status(&[serde_json::json!({ "seq": seq })])
        .await;
    let sent = outcome.is_ok();
    let queued = tokio::time::timeout(Duration::from_millis(200), source.events.recv())
        .await
        .is_ok_and(|message| message.is_some());
    assert_eq!(sent, queued, "push {seq}: {outcome:?}, queued {queued}");
    sent
}

fn client(source: &Source, tls: &VectorCompatTls) -> VectorCompatClient {
    VectorCompatClient::connect_lazy_tls(&source.endpoint(), 5000, tls).expect("the client builds")
}

fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

/// The CA's certificate as a file, in a directory that lives as long as the
/// returned guard.
fn ca_file(ca: &TestCa) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "ca.pem", &ca.pem());
    (dir, path)
}

/// A private CA trusted by file verifies the source, whatever the OS store
/// holds, and a push and a health check go through TLS.
#[tokio::test]
async fn a_private_ca_verifies_the_source() {
    let ca = TestCa::new("vector-ca");
    let mut source = source(&ca, false).await;
    let (_dir, ca_path) = ca_file(&ca);
    let tls = VectorCompatTls::private_ca(&ca_path).with_domain("localhost");
    let client = client(&source, &tls);

    assert!(push(&client, &mut source, 1).await, "a push through TLS");
    assert!(client.health_check().await.expect("a health check"));
}

/// Without a root for the source's CA the handshake fails and nothing is
/// queued, so verification is on. A pin to another CA fails the same way, and
/// the right CA beside the OS store is trusted when `exclusive` is off.
#[tokio::test]
async fn a_source_from_an_untrusted_ca_is_not_trusted() {
    let ca = TestCa::new("vector-ca");
    let mut source = source(&ca, false).await;
    let (_dir, ca_path) = ca_file(&ca);
    let (_other_dir, other_path) = ca_file(&TestCa::new("another-ca"));

    let os_store_only = VectorCompatTls::default().with_domain("localhost");
    assert!(!push(&client(&source, &os_store_only), &mut source, 1).await);

    let pinned_elsewhere = VectorCompatTls::private_ca(&other_path).with_domain("localhost");
    assert!(!push(&client(&source, &pinned_elsewhere), &mut source, 2).await);

    let beside = VectorCompatTls {
        extra_roots: vec![other_path, ca_path],
        ..VectorCompatTls::default().with_domain("localhost")
    };
    assert!(push(&client(&source, &beside), &mut source, 3).await);
}

/// The name check holds: a certificate for `localhost` fails for another domain.
#[tokio::test]
async fn the_name_in_the_certificate_must_match() {
    let ca = TestCa::new("vector-ca");
    let mut source = source(&ca, false).await;
    let (_dir, ca_path) = ca_file(&ca);
    let wrong = VectorCompatTls::private_ca(&ca_path).with_domain("not-the-source.test");
    assert!(!push(&client(&source, &wrong), &mut source, 1).await);
}

/// A source that asks for a client certificate fails a client that presents
/// none, and takes one with a certificate from its CA.
#[tokio::test]
async fn a_client_certificate_reaches_a_source_that_asks_for_one() {
    let ca = TestCa::new("vector-ca");
    let mut source = source(&ca, true).await;
    let (dir, ca_path) = ca_file(&ca);
    let agent = ca.client("agent");
    let cert = write(dir.path(), "agent.pem", &agent.cert_pem);
    let key = write(dir.path(), "agent.key", &agent.key_pem);

    let without = VectorCompatTls::private_ca(&ca_path).with_domain("localhost");
    assert!(!push(&client(&source, &without), &mut source, 1).await);

    let with = without.with_client_identity(&cert, &key);
    assert!(push(&client(&source, &with), &mut source, 2).await);
}

/// The OS store is the default trust: pointed at the CA the way
/// `SSL_CERT_FILE` points a system at one, the source verifies with no root of
/// the caller's.
#[cfg(target_os = "linux")]
#[test]
fn the_os_store_is_trusted_by_default() {
    let ca = TestCa::new("vector-ca");
    let (_dir, ca_path) = ca_file(&ca);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    temp_env::with_var("SSL_CERT_FILE", Some(&ca_path), || {
        runtime.block_on(async {
            let mut source = source(&ca, false).await;
            let tls = VectorCompatTls::default().with_domain("localhost");
            assert!(push(&client(&source, &tls), &mut source, 1).await);
        });
    });
}

/// An empty OS store, as in a minimal container, does not stop a client that
/// carries its own CA, and fails one that has nothing else to trust.
#[cfg(target_os = "linux")]
#[test]
fn an_empty_os_store_leaves_extra_roots_trusted() {
    let ca = TestCa::new("vector-ca");
    let (dir, ca_path) = ca_file(&ca);
    let empty = write(dir.path(), "empty.pem", "");
    let empty_dir = dir.path().join("no-certs");
    std::fs::create_dir(&empty_dir).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let vars = [
        ("SSL_CERT_FILE", Some(empty)),
        ("SSL_CERT_DIR", Some(empty_dir)),
    ];
    temp_env::with_vars(vars, || {
        runtime.block_on(async {
            let mut source = source(&ca, false).await;

            let os_store_alone = VectorCompatTls::default().with_domain("localhost");
            let refused = VectorCompatClient::connect_lazy_tls(&source.endpoint(), 5000, &os_store_alone);
            assert!(
                matches!(&refused, Err(crate::transport::TransportError::Config(m)) if m.contains("trust store is empty")),
                "{:?}",
                refused.err()
            );

            let beside = VectorCompatTls {
                extra_roots: vec![ca_path],
                ..os_store_alone
            };
            assert!(push(&client(&source, &beside), &mut source, 1).await);
        });
    });
}
