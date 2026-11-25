// Project:   scalo
// File:      tests/kafka_tls_floor.rs
// Purpose:   The profile's TLS version floor reaches librdkafka's OpenSSL, against a real broker
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! librdkafka has no version property, so scalo writes the floor into the
//! file `OPENSSL_CONF` names and OpenSSL reads it at its first TLS context.
//! That is once per process, so each row re-runs this binary as a probe in a
//! process of its own, with the environment a deployment would give it, and
//! sends one record to Apache Kafka in Docker:
//!
//! - `strong` against a TLS 1.2-only listener is refused, naming
//!   `allow_tls12`.
//! - `strong` with `allow_tls12` set through the cascade reaches it.
//! - `strong` reaches a TLS 1.3-only listener.
//! - With `OPENSSL_CONF` unset nothing sets the floor, so `strong` reaches
//!   the TLS 1.2-only listener: the floor, and nothing else, refused row one.
//!
//! Skips when Docker is absent, and fails instead of skipping under CI.

#![cfg(all(feature = "transport-kafka-test-support", feature = "config", unix))]

mod common;

use std::process::Command;
use std::time::Duration;

use scalo::transport::kafka::test_support::{TlsBroker, require_docker, skip_or_fail};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{TransportBase, TransportSender};

/// Set only in the re-run: the broker address the probe sends to.
const PROBE_BROKER: &str = "KAFKA_TLS_FLOOR_PROBE_BROKER";
/// The CA file the probe trusts.
const PROBE_CA: &str = "KAFKA_TLS_FLOOR_PROBE_CA";
/// The cascade prefix the probe reads its profile settings under.
const PREFIX: &str = "KTLSFLOOR";
const TOPIC: &str = "tls-floor";
const TEST: &str = "the_profile_floor_reaches_librdkafka";

/// The line the probe prints its outcome on.
const MARKER: &str = "probe outcome: ";

/// In the re-run, send one record and print the outcome. Anywhere else, do
/// nothing.
fn ran_as_probe() -> bool {
    let (Ok(broker), Ok(ca)) = (std::env::var(PROBE_BROKER), std::env::var(PROBE_CA)) else {
        return false;
    };
    scalo::config::setup(scalo::config::ConfigOptions {
        env_prefix: PREFIX.to_string(),
        search_standard_paths: false,
        load_dotenv: false,
        ..scalo::config::ConfigOptions::default()
    })
    .expect("the probe's cascade");
    let outcome = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime for the probe")
        .block_on(async {
            let config = KafkaConfig {
                brokers: vec![broker],
                group: String::new(),
                ..KafkaConfig::default()
            }
            .with_tls(Some(ca.as_str()))
            .with_override("message.timeout.ms", "8000");
            let transport = KafkaTransport::new(&config)
                .await
                .expect("the transport builds");
            let sent = tokio::time::timeout(
                Duration::from_secs(40),
                transport.send(TOPIC, bytes::Bytes::from_static(b"{\"floor\":true}")),
            )
            .await
            .expect("the send settles");
            if sent.is_ok() {
                "delivered".to_string()
            } else {
                let health = transport.healthcheck().await;
                format!("refused: {sent:?}, health {health:?}")
            }
        });
    println!("{MARKER}{outcome}");
    true
}

/// Re-run this binary as the probe against `broker`, with `OPENSSL_CONF` at
/// `openssl_conf` or unset, and `allow_tls12` on strong when asked. Returns
/// the probe's outcome line.
fn probe(broker: &str, ca: &str, openssl_conf: Option<&std::path::Path>, tls12: bool) -> String {
    let mut command = Command::new(std::env::current_exe().expect("the test binary"));
    command
        .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(PROBE_BROKER, broker)
        .env(PROBE_CA, ca)
        .env_remove("OPENSSL_CONF");
    if let Some(path) = openssl_conf {
        command.env("OPENSSL_CONF", path);
    }
    if tls12 {
        command.env(
            format!("{PREFIX}__TLS__PROFILES__STRONG__ALLOW_TLS12"),
            "true",
        );
    }
    let output = command.output().expect("the test binary re-runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| line.split_once(MARKER).map(|(_, rest)| rest.to_string()))
        .unwrap_or_else(|| {
            panic!(
                "no outcome line:\n{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

#[test]
fn the_profile_floor_reaches_librdkafka() {
    if ran_as_probe() {
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(the_floor_holds_on_each_row());
}

async fn the_floor_holds_on_each_row() {
    if !require_docker() {
        return;
    }
    let broker = match TlsBroker::start(&common::SUITE, scalo::test_name!()).await {
        Ok(broker) => broker,
        Err(e) => {
            skip_or_fail(&format!("the TLS broker would not start ({e})"));
            return;
        }
    };
    let ca = broker.pki.file("ca.crt");
    let admin = KafkaConfig {
        brokers: vec![broker.tls.clone()],
        group: String::new(),
        ..KafkaConfig::default()
    }
    .with_tls(Some(ca.as_str()));
    KafkaAdmin::new(&admin)
        .expect("admin")
        .create_topics(&[(TOPIC, 1, 1)])
        .await
        .expect("create the topic");

    let dir = tempfile::tempdir().expect("temp dir");
    let floor = dir.path().join("openssl.cnf");
    // (row, listener, OPENSSL_CONF set, allow_tls12, the outcome starts with, and names)
    let rows: [(&str, &str, bool, bool, &str, &str); 4] = [
        (
            "strong against TLS 1.2 only",
            &broker.tls12_only,
            true,
            false,
            "refused",
            "peer supports only TLS 1.2: enable allow_tls12 on profile strong",
        ),
        (
            "strong with allow_tls12 against TLS 1.2 only",
            &broker.tls12_only,
            true,
            true,
            "delivered",
            "",
        ),
        (
            "strong against TLS 1.3 only",
            &broker.tls13_only,
            true,
            false,
            "delivered",
            "",
        ),
        (
            "strong with OPENSSL_CONF unset against TLS 1.2 only",
            &broker.tls12_only,
            false,
            false,
            "delivered",
            "",
        ),
    ];
    let mut failures = Vec::new();
    for (row, listener, conf, tls12, starts, names) in rows {
        let _ = std::fs::remove_file(&floor);
        let outcome = probe(listener, &ca, conf.then_some(floor.as_path()), tls12);
        eprintln!("{row}: {outcome}");
        if !outcome.starts_with(starts) || !outcome.contains(names) {
            failures.push(format!(
                "{row}: want {starts} naming {names:?}, got {outcome}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
