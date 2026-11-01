// Project:   scalo
// File:      tests/common/kafka.rs
// Purpose:   The run's shared Kafka broker, and brokers a test holds alone
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka for the real-broker tests: the run's one shared broker, or a broker
//! one test holds alone because it stops and starts it.

use scalo::deployment::test_support::shared_service::{self, OwnedContainer};
use scalo::transport::kafka::test_support::{KAFKA_PORT, kafka_spec, shared_kafka};

use super::SUITE;

/// The run's shared broker's bootstrap address.
pub async fn shared_bootstrap() -> String {
    shared_kafka(SUITE).await.addr(KAFKA_PORT)
}

/// A broker only `test` uses, on a host port that survives a restart, and its
/// bootstrap address. Removed when the holder drops or this process exits.
pub async fn broker_alone(test: &str) -> (OwnedContainer, String) {
    let port = free_port();
    let name = shared_service::container_name(&SUITE, Some(test), "kafka");
    let node = tokio::task::spawn_blocking(move || {
        shared_service::start_alone(&SUITE, &kafka_spec(Some(port)), &name)
    })
    .await
    .expect("start a broker of the test's own");
    let bootstrap = node.container.addr(KAFKA_PORT);
    (node, bootstrap)
}

/// Stop `node` at once, off the async runtime.
pub async fn stop(node: &OwnedContainer) {
    let name = node.container.name.clone();
    tokio::task::spawn_blocking(move || {
        let out = shared_service::docker(&["stop", "--time", "0", &name]);
        assert!(
            out.status.success(),
            "docker stop {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    })
    .await
    .unwrap_or_else(|e| panic!("docker stop: {e}"));
}

/// Freeze `node`'s processes, so the broker holds connections and answers
/// nothing, off the async runtime.
pub async fn pause(node: &OwnedContainer) {
    docker_on(node, "pause").await;
}

/// Thaw `node` after [`pause`].
pub async fn unpause(node: &OwnedContainer) {
    docker_on(node, "unpause").await;
}

async fn docker_on(node: &OwnedContainer, verb: &'static str) {
    let name = node.container.name.clone();
    tokio::task::spawn_blocking(move || {
        let out = shared_service::docker(&[verb, &name]);
        assert!(
            out.status.success(),
            "docker {verb} {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    })
    .await
    .unwrap_or_else(|e| panic!("docker {verb}: {e}"));
}

/// Start `node` again, off the async runtime. The broker is serving once a
/// produce to it is delivered.
pub async fn restart(node: &OwnedContainer) {
    docker_on(node, "start").await;
}

/// A loopback port nothing holds, for a broker to keep across its restart.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("bind an ephemeral port")
        .port()
}
