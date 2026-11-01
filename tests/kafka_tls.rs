// Project:   scalo
// File:      tests/kafka_tls.rs
// Purpose:   The Kafka transport against a real broker with TLS, mTLS and SASL_SSL SCRAM on
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `KafkaTransport` and `KafkaAdmin` against Apache Kafka in Docker, with each
//! security option turned on.
//!
//! librdkafka does the TLS here, through OpenSSL, so none of this touches the
//! rustls stack the rest of scalo uses. The only thing in the process that
//! touches the certificates is the library under test, and the broker is a
//! different implementation (a JVM), so a handshake that completes did so
//! between two independent stacks.
//!
//! One broker serves one test, which runs a table of cases in two phases:
//!
//! 1. Positive rows build a client the way an app's config does, create a topic
//!    through `KafkaAdmin`, send a record through `KafkaTransport` and receive it
//!    back through the same transport's consumer.
//! 2. Refusal rows change exactly one thing from a phase 1 row (the CA, the
//!    password, the client certificate, the protocol) and expect both the producer
//!    and the metadata client to fail. Phase 1 has just proved the broker, the
//!    listener and the topic are fine, so a refusal means the changed setting.
//!    The broker's log then names each refusal.
//!
//!    librdkafka reports a rejected certificate or SASL login to the client as
//!    `SSL` or `Authentication`, and the transport turns those into a `Fatal`
//!    send and a failed readiness check that names the cause. A broker that
//!    drops the connection before the client learns why (a missing client
//!    certificate under TLS 1.3, a plaintext client on a TLS port) reaches the
//!    client as a plain transport failure, so those rows stay `Backpressured`
//!    and ready. Each row says which it expects.
//!
//! The test sets no environment and installs no process-global state, so the
//! transport starts the way an app's `main` leaves it.
//!
//! Skips when Docker is absent, and fails instead of skipping under CI.

#![cfg(all(feature = "transport-kafka-test-support", unix))]
#![allow(clippy::large_futures)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use scalo::transport::kafka::test_support::{
    SCRAM_PASSWORD, SCRAM_USER, TlsBroker, require_docker, skip_or_fail,
};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{
    SendResult, TransportBase, TransportError, TransportReceiver, TransportSender,
};

/// Group join plus the first fetch, on a single-node broker busy with a JVM start.
const ROUND_TRIP_WITHIN: Duration = Duration::from_secs(90);

/// How long a refused producer gets to give up on a record.
const MESSAGE_TIMEOUT_MS: &str = "8000";

/// The most a refused producer may take to report failure: the record's timeout plus queue and scheduling slack.
const REFUSED_WITHIN: Duration = Duration::from_secs(40);

/// A phase 1 row: a security setup the broker must accept.
struct Accepted {
    name: &'static str,
    listener: Listener,
    security: Security,
}

/// A phase 2 row: one change to an accepted row's setup that the broker must refuse.
struct Refused {
    name: &'static str,
    /// The accepted row whose topic and listener this one reuses.
    from: &'static str,
    security: Security,
    /// The line the broker logs when it turns the client away.
    broker_says: &'static str,
    /// What the client is told.
    client_sees: ClientSees,
}

/// How a refusal reaches the client.
#[derive(Clone, Copy)]
enum ClientSees {
    /// librdkafka names the cause, with this error code, so the send is `Fatal`
    /// and readiness fails.
    Cause(&'static str),
    /// The broker drops the connection and librdkafka reports a transport
    /// failure, so the send is `Backpressured` and readiness is unchanged.
    DroppedConnection,
}

#[derive(Clone, Copy)]
enum Listener {
    Tls,
    SaslSsl,
    Mtls,
}

/// How a client secures its connection. Paths are names inside the PKI directory.
#[derive(Clone)]
enum Security {
    Plaintext,
    /// TLS, trusting the system store only.
    TlsDefaultTrust,
    /// TLS, trusting this CA file.
    Tls {
        ca: &'static str,
    },
    /// TLS trusting `ca`, and a client certificate and key.
    Mtls {
        ca: &'static str,
        cert: &'static str,
        key: &'static str,
    },
    /// TLS trusting `ca`, and SCRAM-SHA-512 with `password`.
    ScramSsl {
        ca: &'static str,
        password: &'static str,
    },
}

const ACCEPTED: &[Accepted] = &[
    Accepted {
        name: "tls",
        listener: Listener::Tls,
        security: Security::Tls { ca: "ca.crt" },
    },
    Accepted {
        name: "sasl-ssl-scram-sha-512",
        listener: Listener::SaslSsl,
        security: Security::ScramSsl {
            ca: "ca.crt",
            password: SCRAM_PASSWORD,
        },
    },
    Accepted {
        name: "mtls",
        listener: Listener::Mtls,
        security: Security::Mtls {
            ca: "ca.crt",
            cert: "client.crt",
            key: "client.key",
        },
    },
];

const REFUSED: &[Refused] = &[
    Refused {
        name: "tls-client-trusts-another-ca",
        from: "tls",
        security: Security::Tls { ca: "rogue-ca.crt" },
        broker_says: "SSL handshake failed",
        client_sees: ClientSees::Cause("SSL"),
    },
    Refused {
        name: "tls-client-trusts-the-system-store-only",
        from: "tls",
        security: Security::TlsDefaultTrust,
        broker_says: "SSL handshake failed",
        client_sees: ClientSees::Cause("SSL"),
    },
    Refused {
        name: "tls-listener-meets-a-plaintext-client",
        from: "tls",
        security: Security::Plaintext,
        broker_says: "SSL handshake failed",
        client_sees: ClientSees::DroppedConnection,
    },
    Refused {
        name: "sasl-ssl-scram-wrong-password",
        from: "sasl-ssl-scram-sha-512",
        security: Security::ScramSsl {
            ca: "ca.crt",
            password: "not-the-password",
        },
        broker_says: "invalid credentials with SASL mechanism SCRAM-SHA-512",
        client_sees: ClientSees::Cause("Authentication"),
    },
    Refused {
        name: "mtls-client-presents-no-certificate",
        from: "mtls",
        security: Security::Tls { ca: "ca.crt" },
        broker_says: "SSL handshake failed",
        client_sees: ClientSees::DroppedConnection,
    },
    Refused {
        name: "mtls-client-certificate-from-another-ca",
        from: "mtls",
        security: Security::Mtls {
            ca: "ca.crt",
            cert: "rogue-client.crt",
            key: "rogue-client.key",
        },
        broker_says: "SSL handshake failed",
        client_sees: ClientSees::DroppedConnection,
    },
];

fn listener_address(broker: &TlsBroker, listener: Listener) -> &str {
    match listener {
        Listener::Tls => &broker.tls,
        Listener::SaslSsl => &broker.sasl_ssl,
        Listener::Mtls => &broker.mtls,
    }
}

fn client_config(
    broker: &TlsBroker,
    listener: Listener,
    security: &Security,
    group: &str,
    topic: &str,
) -> KafkaConfig {
    let base = KafkaConfig {
        brokers: vec![listener_address(broker, listener).to_string()],
        group: group.to_string(),
        topics: if group.is_empty() {
            Vec::new()
        } else {
            vec![topic.to_string()]
        },
        ..KafkaConfig::default()
    };
    let file = |name: &str| broker.pki.file(name);
    match security {
        Security::Plaintext => base,
        Security::TlsDefaultTrust => base.with_tls(None),
        Security::Tls { ca } => base.with_tls(Some(file(ca).as_str())),
        Security::Mtls { ca, cert, key } => base
            .with_tls(Some(file(ca).as_str()))
            .with_client_cert(&file(cert), &file(key)),
        Security::ScramSsl { ca, password } => base
            .with_scram_ssl("SCRAM-SHA-512", SCRAM_USER, password)
            .with_tls(Some(file(ca).as_str())),
    }
}

/// Create the row's topic, send one record and receive it back.
async fn round_trip(broker: &TlsBroker, row: &Accepted) -> Result<String, String> {
    let topic = format!("tls-{}", row.name);
    let group = format!("group-{}", row.name);

    let admin_config = client_config(broker, row.listener, &row.security, "", &topic);
    let admin = KafkaAdmin::new(&admin_config).map_err(|e| format!("admin client: {e}"))?;
    admin
        .create_topics(&[(topic.as_str(), 1, 1)])
        .await
        .map_err(|e| format!("create topic over {}: {e}", row.name))?;
    // A consumer that subscribes before the metadata lists the topic is told it does not exist.
    {
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match admin.describe_topic(&topic) {
                    Ok(info) if info.partition_count > 0 => return Ok(()),
                    other if Instant::now() > deadline => {
                        return Err(format!(
                            "topic {topic} never appeared in metadata: {other:?}"
                        ));
                    }
                    _ => std::thread::sleep(Duration::from_millis(200)),
                }
            }
        })
        .await
        .map_err(|e| format!("metadata task: {e}"))??;
    }

    let config = client_config(broker, row.listener, &row.security, &group, &topic);
    let transport = KafkaTransport::new(&config)
        .await
        .map_err(|e| format!("transport: {e}"))?;
    let payload = format!("{{\"row\":\"{}\"}}", row.name);
    let sent = transport
        .send(&topic, bytes::Bytes::from(payload.clone()))
        .await;
    if !sent.is_ok() {
        return Err(format!("send was not confirmed: {sent:?}"));
    }

    let deadline = Instant::now() + ROUND_TRIP_WITHIN;
    while Instant::now() < deadline {
        let batch = transport.recv(10).await.map_err(|e| format!("recv: {e}"))?;
        if batch
            .records
            .iter()
            .any(|record| record.payload.as_ref() == payload.as_bytes())
        {
            let _ = transport.close().await;
            return Ok(format!("record sent and received back on {topic}"));
        }
    }
    Err(format!(
        "record never came back on {topic} within {ROUND_TRIP_WITHIN:?}"
    ))
}

/// A producer and a metadata client on the changed setup both fail.
async fn refusal(broker: &TlsBroker, row: &Refused) -> Result<String, String> {
    let from = ACCEPTED
        .iter()
        .find(|a| a.name == row.from)
        .ok_or_else(|| format!("no accepted row {}", row.from))?;
    let topic = format!("tls-{}", from.name);

    let config = client_config(broker, from.listener, &row.security, "", &topic)
        .with_override("message.timeout.ms", MESSAGE_TIMEOUT_MS);

    let producer = async {
        let transport = KafkaTransport::new(&config)
            .await
            .map_err(|e| format!("transport refused at build, before dialling the broker: {e}"))?;
        let started = Instant::now();
        let sent = tokio::time::timeout(
            REFUSED_WITHIN,
            transport.send(&topic, bytes::Bytes::from_static(b"{\"refused\":true}")),
        )
        .await
        .map_err(|_| format!("send still pending after {REFUSED_WITHIN:?}"))?;
        let took = started.elapsed();
        let healthcheck = transport.healthcheck().await;
        let told = match (row.client_sees, &sent) {
            (_, SendResult::Ok) => {
                return Err::<String, String>(
                    "the producer delivered a record the broker should have refused".to_string(),
                );
            }
            (ClientSees::Cause(code), SendResult::Fatal(TransportError::Config(why))) => {
                if !why.contains(code) {
                    return Err(format!("the Fatal send does not name {code}: {why}"));
                }
                if transport.is_healthy() {
                    return Err("readiness stayed ready after the broker refused the client".into());
                }
                match &healthcheck {
                    Err(e) if e.is_fatal() && e.to_string().contains(code) => {}
                    other => {
                        return Err(format!("the health check does not name {code}: {other:?}"));
                    }
                }
                format!("Fatal naming {code}, not ready")
            }
            (ClientSees::DroppedConnection, SendResult::Backpressured) => {
                if !transport.is_healthy() || healthcheck.is_err() {
                    return Err(format!(
                        "readiness failed for a connection the broker dropped: {healthcheck:?}"
                    ));
                }
                "Backpressured, still ready (the client was never told why)".to_string()
            }
            (expected, got) => {
                return Err(format!(
                    "expected {}, the send returned {got:?}",
                    match expected {
                        ClientSees::Cause(code) => format!("Fatal naming {code}"),
                        ClientSees::DroppedConnection => "Backpressured".to_string(),
                    }
                ));
            }
        };
        Ok::<String, String>(format!("send after {took:?}: {told}"))
    };
    let metadata = async {
        let admin = KafkaAdmin::new(&config)
            .map_err(|e| format!("admin refused at build, before dialling the broker: {e}"))?;
        let listed = tokio::task::spawn_blocking(move || admin.list_topics())
            .await
            .map_err(|e| format!("metadata task: {e}"))?;
        match listed {
            Ok(topics) => {
                Err::<String, String>(format!("metadata fetch succeeded, saw {topics:?}"))
            }
            Err(e) => Ok(format!("metadata fetch failed: {e}")),
        }
    };
    let (producer, metadata) = tokio::join!(producer, metadata);
    Ok(format!("{} | {}", producer?, metadata?))
}

/// The broker's log has a line refusing a connection to `address`, saying `needle`.
///
/// The broker names the listener in the channel id (`channelId=127.0.0.1:<port>-...`).
fn broker_logged(logs: &str, address: &str, needle: &str) -> bool {
    let channel = format!("channelId={address}-");
    logs.lines().any(|line| {
        line.contains("Failed authentication") && line.contains(&channel) && line.contains(needle)
    })
}

type Outcome = (
    &'static str,
    tokio::task::JoinHandle<Result<String, String>>,
);

/// Log each row's outcome, and note the failures.
async fn report(phase: &str, rows: Vec<Outcome>, failures: &mut Vec<String>) {
    for (name, handle) in rows {
        let outcome = handle
            .await
            .unwrap_or_else(|e| Err(format!("row task panicked: {e}")));
        match outcome {
            Ok(detail) => eprintln!("PASS {phase} {name}: {detail}"),
            Err(why) => {
                eprintln!("FAIL {phase} {name}: {why}");
                failures.push(format!("{phase} {name}: {why}"));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_transport_against_a_tls_broker() {
    if !require_docker() {
        return;
    }
    let broker = match TlsBroker::start(&common::SUITE, scalo::test_name!()).await {
        Ok(broker) => Arc::new(broker),
        Err(e) => {
            skip_or_fail(&format!("the TLS broker would not start ({e})"));
            return;
        }
    };

    let mut failures = Vec::new();

    let accepted = ACCEPTED
        .iter()
        .map(|row| {
            let broker = Arc::clone(&broker);
            (
                row.name,
                tokio::spawn(async move { round_trip(&broker, row).await }),
            )
        })
        .collect();
    report("accepted", accepted, &mut failures).await;

    // A refusal means something only if the accepted row it derives from passed.
    if failures.is_empty() {
        let refused = REFUSED
            .iter()
            .map(|row| {
                let broker = Arc::clone(&broker);
                (
                    row.name,
                    tokio::spawn(async move { refusal(&broker, row).await }),
                )
            })
            .collect();
        report("refused", refused, &mut failures).await;

        let logs = broker.logs().await;
        for row in REFUSED {
            let listener = ACCEPTED
                .iter()
                .find(|a| a.name == row.from)
                .map_or(Listener::Tls, |a| a.listener);
            let address = listener_address(&broker, listener);
            if !broker_logged(&logs, address, row.broker_says) {
                failures.push(format!(
                    "refused {}: the broker never logged a failed authentication on {address} \
                     saying {:?}",
                    row.name, row.broker_says
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
