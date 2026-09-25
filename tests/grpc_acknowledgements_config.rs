// Project:   scalo
// File:      tests/grpc_acknowledgements_config.rs
// Purpose:   A factory-built gRPC receiver honours <key>.grpc.acknowledgements
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `AnyReceiver::from_config` reads `<key>.grpc.acknowledgements` beside the
//! gRPC section and applies it to the receive server it builds.
//!
//! The global config installs once per process, so this file owns it.

#![cfg(all(feature = "transport-grpc", feature = "config"))]

use std::time::Duration;

use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::{
    AnyReceiver, DeliveryStatus, SendResult, TransportBase, TransportReceiver, TransportSender,
};

/// A client pushing one record to `receiver`, in a task of its own.
// A `let ... else` is irrefutable, and warned about, when gRPC is the only
// transport compiled in.
#[allow(clippy::manual_let_else)]
async fn push(receiver: &AnyReceiver) -> tokio::task::JoinHandle<SendResult> {
    let server = match receiver {
        AnyReceiver::Grpc(server) => server,
        // Reachable only when another transport feature is compiled in.
        #[allow(unreachable_patterns)]
        _ => panic!("the factory built a gRPC receiver"),
    };
    let uri = format!("http://{}", server.local_addr().expect("bound"));
    let client = GrpcTransport::new(&GrpcConfig::client(&uri))
        .await
        .expect("client");
    tokio::spawn(async move { client.send("main", bytes::Bytes::from_static(b"{}")).await })
}

#[tokio::test]
async fn a_factory_built_grpc_receiver_honours_its_acknowledgements_section() {
    let dir = tempfile::tempdir().expect("config tempdir");
    std::fs::write(
        dir.path().join("settings.yaml"),
        "transport:\n  \
           off:\n    type: grpc\n    grpc:\n      listen: \"127.0.0.1:0\"\n      \
             acknowledgements:\n        enabled: false\n  \
           held:\n    type: grpc\n    grpc:\n      listen: \"127.0.0.1:0\"\n",
    )
    .expect("write settings.yaml");
    scalo::config::setup(scalo::config::ConfigOptions {
        config_paths: vec![dir.path().to_path_buf()],
        load_dotenv: false,
        ..scalo::config::ConfigOptions::default()
    })
    .expect("config setup");

    // The section says off: built armed, the push is still answered at enqueue.
    let off = AnyReceiver::from_config_armed("transport.off")
        .await
        .expect("receiver");
    let control = off.ack_control().expect("gRPC can hold");
    assert!(
        !control.enabled(),
        "the section turned acknowledgements off"
    );
    let answered = tokio::time::timeout(Duration::from_secs(2), push(&off).await)
        .await
        .expect("answered at enqueue")
        .expect("push task");
    assert!(matches!(answered, SendResult::Ok), "{answered:?}");

    // No section: acknowledgements default on, and built armed the server
    // holds the very first push, with no `arm` call after construction.
    let held = AnyReceiver::from_config_armed("transport.held")
        .await
        .expect("receiver");
    let control = held.ack_control().expect("gRPC can hold");
    assert!(control.enabled(), "acknowledgements default on");
    assert!(control.is_armed(), "armed before it listened");
    let pushing = push(&held).await;
    let mut batch = held.recv(1).await.expect("recv");
    while batch.records.is_empty() {
        batch = held.recv(1).await.expect("recv");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!pushing.is_finished(), "held until release");
    held.release(&batch.commit_tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    let answered = pushing.await.expect("push task");
    assert!(matches!(answered, SendResult::Ok), "{answered:?}");

    // Built the plain way, the same key is unarmed until a caller arms it.
    let plain = AnyReceiver::from_config("transport.held")
        .await
        .expect("receiver");
    assert!(!plain.ack_control().expect("gRPC can hold").is_armed());

    let _ = off.close().await;
    let _ = held.close().await;
    let _ = plain.close().await;
}
