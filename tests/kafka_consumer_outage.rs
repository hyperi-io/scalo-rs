// Project:   scalo
// File:      tests/kafka_consumer_outage.rs
// Purpose:   Real-broker proof that a Kafka consumer survives a broker outage
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-broker tests for `KafkaTransport::recv` across a broker outage.
//!
//! NO mocks: the broker container is stopped under a live scalo consumer and
//! started again. A raw rdkafka producer writes the records, so scalo's write
//! path never feeds scalo's read path.
//!
//! `#[ignore]` because they need a running Docker daemon -- run on a Docker host:
//!
//! ```text
//! cargo nextest run --features transport-kafka --test kafka_consumer_outage \
//!     --run-ignored only --no-capture
//! ```

#![cfg(feature = "transport-kafka")]

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::transport::kafka::{
    ConsumerProtocol, KafkaAdmin, KafkaConfig, KafkaToken, KafkaTransport,
};
use scalo::transport::{TransportError, TransportReceiver};
use testcontainers_modules::kafka::apache::{self, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Kafka to test against, pinned by digest.
// renovate: datasource=docker depName=apache/kafka-native
const KAFKA_IMAGE_REF: &str =
    "4.3.1@sha256:2885898ba17065023f1bd605f3a81efcfa986014f062b73b91ef5462485f9060";

const TOPIC: &str = "outage";

/// Records written before the outage and after it.
const RECORDS: usize = 20;

/// How long the broker stays down while the consumer keeps polling.
const OUTAGE: Duration = Duration::from_secs(20);

/// A free loopback port for the broker to keep across its restart.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("bind an ephemeral port")
        .port()
}

/// Start a single-node KRaft broker on a fixed host port and return it plus
/// its bootstrap address.
///
/// The port is fixed because Docker hands a restarted container a new
/// ephemeral port, and the broker advertises the one it started with.
async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
    let port = free_port();
    let node = Kafka::default()
        .with_tag(KAFKA_IMAGE_REF)
        .with_mapped_port(port, apache::KAFKA_PORT)
        .start()
        .await
        .expect("start kafka container");
    (node, format!("127.0.0.1:{port}"))
}

async fn create_topic(bootstrap: &str) {
    let admin = KafkaAdmin::new(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    })
    .expect("kafka admin");
    admin
        .create_topics(&[(TOPIC, 1, 1)])
        .await
        .expect("create topic");
}

fn consumer_config(bootstrap: &str, group: &str, protocol: ConsumerProtocol) -> KafkaConfig {
    KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: group.to_string(),
        topics: vec![TOPIC.to_string()],
        consumer_protocol: protocol,
        ..Default::default()
    }
}

/// A raw producer that keeps a record queued through a broker restart.
fn raw_producer(bootstrap: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "120000")
        .create()
        .expect("raw producer")
}

/// Produce `{prefix}-0` .. `{prefix}-{RECORDS - 1}` and wait for each delivery.
async fn produce(producer: &FutureProducer, prefix: &str) -> BTreeSet<String> {
    let mut sent = BTreeSet::new();
    for seq in 0..RECORDS {
        let payload = format!("{prefix}-{seq}");
        producer
            .send(
                FutureRecord::<(), str>::to(TOPIC).payload(payload.as_str()),
                Duration::from_secs(10),
            )
            .await
            .unwrap_or_else(|(e, _)| panic!("deliver {payload}: {e}"));
        sent.insert(payload);
    }
    sent
}

/// Poll until every payload in `want` has arrived, failing on any recv error.
async fn consume_all(
    consumer: &KafkaTransport,
    want: &BTreeSet<String>,
    within: Duration,
) -> Vec<KafkaToken> {
    let deadline = Instant::now() + within;
    let mut seen = BTreeSet::new();
    let mut tokens = Vec::new();
    while !want.is_subset(&seen) {
        assert!(
            Instant::now() < deadline,
            "consumed {} of {} records within {within:?}; missing {:?}",
            want.intersection(&seen).count(),
            want.len(),
            want.difference(&seen).collect::<Vec<_>>()
        );
        let batch = consumer
            .recv(100)
            .await
            .unwrap_or_else(|e| panic!("recv failed while consuming: {e}"));
        for record in &batch.records {
            seen.insert(String::from_utf8_lossy(&record.payload).into_owned());
        }
        tokens.extend(batch.commit_tokens);
    }
    tokens
}

/// Consume, lose the broker for [`OUTAGE`], get it back, consume again.
async fn ride_out_an_outage(protocol: ConsumerProtocol, group: &str) {
    let (node, bootstrap) = start_kafka().await;
    create_topic(&bootstrap).await;
    let producer = raw_producer(&bootstrap);

    let consumer = KafkaTransport::new(&consumer_config(&bootstrap, group, protocol))
        .await
        .expect("kafka consumer");

    let before = produce(&producer, "before").await;
    let tokens = consume_all(&consumer, &before, Duration::from_secs(60)).await;
    consumer
        .commit(&tokens)
        .await
        .expect("commit before the outage");

    node.stop_with_timeout(Some(0))
        .await
        .expect("stop the broker");

    let outage = Instant::now();
    let mut calls = 0_u32;
    let mut slowest = Duration::ZERO;
    while outage.elapsed() < OUTAGE {
        let call = Instant::now();
        let batch = consumer.recv(100).await.unwrap_or_else(|e| {
            panic!(
                "recv failed {:?} into the broker outage: {e}",
                outage.elapsed()
            )
        });
        slowest = slowest.max(call.elapsed());
        assert!(batch.records.is_empty(), "no broker, so no records");
        calls += 1;
    }
    eprintln!(
        "{protocol:?}: broker down {OUTAGE:?}, {calls} recv calls, every one Ok, slowest {slowest:?}"
    );
    // Every call blocks in the 50 ms poll or the backoff, so 20 s allows about 400.
    assert!(
        calls <= 500,
        "{calls} recv calls in {OUTAGE:?} -- the failing poll is spinning"
    );
    assert!(
        slowest <= Duration::from_secs(3),
        "one recv held the caller for {slowest:?}, past the 2 s backoff ceiling"
    );

    // A plain Docker start: the image's start hook would rewrite the broker's
    // launch script while the restarted container is already running it.
    (*node).start().await.expect("restart the broker");
    let restarted = Instant::now();

    let after = produce(&producer, "after").await;
    let tokens = consume_all(&consumer, &after, Duration::from_secs(120)).await;
    eprintln!(
        "{protocol:?}: all {RECORDS} records produced after the restart consumed {:?} after it",
        restarted.elapsed()
    );
    consumer
        .commit(&tokens)
        .await
        .expect("commit after the outage");
}

#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn a_consumer_rides_out_a_broker_outage() {
    ride_out_an_outage(ConsumerProtocol::Consumer, "outage-kip848").await;
}

#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn a_classic_consumer_rides_out_a_broker_outage() {
    ride_out_an_outage(ConsumerProtocol::Classic, "outage-classic").await;
}

/// A topic that does not exist, with the consumer barred from creating it, is
/// a failure no retry clears, so recv must still return it.
///
/// Classic protocol: under KIP-848 librdkafka waits for a missing topic to
/// appear instead of raising an error.
#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn a_missing_topic_still_ends_the_consumer() {
    let (_node, bootstrap) = start_kafka().await;
    let config = KafkaConfig {
        topics: vec!["no-such-topic".to_string()],
        ..consumer_config(&bootstrap, "outage-missing", ConsumerProtocol::Classic)
    };
    let consumer = KafkaTransport::new(&config).await.expect("kafka consumer");

    let deadline = Instant::now() + Duration::from_secs(60);
    let err = loop {
        assert!(
            Instant::now() < deadline,
            "recv never reported the missing topic"
        );
        match consumer.recv(100).await {
            Ok(batch) => assert!(batch.records.is_empty()),
            Err(e) => break e,
        }
    };
    eprintln!("missing topic reported as: {err}");
    assert!(
        matches!(&err, TransportError::Recv(detail) if detail.contains("UnknownTopicOrPartition")),
        "expected a receive error naming the missing topic, got {err:?}"
    );
}
