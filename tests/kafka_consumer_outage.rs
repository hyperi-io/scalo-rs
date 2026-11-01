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
//! path never feeds scalo's read path. A test that stops the broker holds one
//! of its own, on a host port kept across the restart because the broker
//! advertises it. The missing-topic test uses the run's shared broker.
//!
//! They need a Docker daemon: without one they skip on a developer's machine and
//! fail when `CI` is set.
//!
//! ```text
//! cargo nextest run --features transport-kafka,transport-kafka-test-support --test kafka_consumer_outage --no-capture
//! ```

#![cfg(all(feature = "transport-kafka-test-support", unix))]

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::test_name;
use scalo::transport::kafka::test_support::require_docker;
use scalo::transport::kafka::{
    ConsumerProtocol, KafkaAdmin, KafkaConfig, KafkaToken, KafkaTransport,
};
use scalo::transport::{TransportBase, TransportError, TransportReceiver};

use common::kafka::{broker_alone, restart, shared_bootstrap, stop};

const TOPIC: &str = "outage";

/// Records written before the outage and after it.
const RECORDS: usize = 20;

/// How long the broker stays down while the consumer keeps polling.
const OUTAGE: Duration = Duration::from_secs(20);

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
    // A consumer that subscribes before the broker's metadata lists the new
    // topic waits for the next metadata refresh, minutes by default.
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        while admin
            .describe_topic(TOPIC)
            .map_or(true, |t| t.partition_count == 0)
        {
            assert!(
                Instant::now() < deadline,
                "topic {TOPIC} never appeared in the broker's metadata"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    })
    .await
    .expect("metadata wait");
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
async fn ride_out_an_outage(test: &str, protocol: ConsumerProtocol, group: &str) {
    let (node, bootstrap) = broker_alone(test).await;
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

    stop(&node).await;

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

    restart(&node).await;
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
async fn a_consumer_rides_out_a_broker_outage() {
    if !require_docker() {
        return;
    }
    ride_out_an_outage(test_name!(), ConsumerProtocol::Consumer, "outage-kip848").await;
}

#[tokio::test]
async fn a_classic_consumer_rides_out_a_broker_outage() {
    if !require_docker() {
        return;
    }
    ride_out_an_outage(test_name!(), ConsumerProtocol::Classic, "outage-classic").await;
}

/// A commit caught by an outage retries for its window and then reports the
/// failure, rather than holding the caller until the broker returns; the
/// consumer carries on once it does.
#[tokio::test]
async fn a_commit_during_an_outage_reports_instead_of_hanging() {
    if !require_docker() {
        return;
    }
    let (node, bootstrap) = broker_alone(test_name!()).await;
    create_topic(&bootstrap).await;
    let producer = raw_producer(&bootstrap);
    let consumer = KafkaTransport::new(&consumer_config(
        &bootstrap,
        "outage-commit",
        ConsumerProtocol::Consumer,
    ))
    .await
    .expect("kafka consumer");

    let before = produce(&producer, "before").await;
    let tokens = consume_all(&consumer, &before, Duration::from_secs(60)).await;

    stop(&node).await;
    let started = Instant::now();
    let result = consumer.commit(&tokens).await;
    let took = started.elapsed();
    eprintln!("commit during the outage returned {result:?} after {took:?}");
    assert!(
        matches!(result, Err(TransportError::Commit(_))),
        "a commit the broker never answered must be reported, got {result:?}"
    );
    // The 60 s retry window plus one synchronous attempt still in flight.
    assert!(
        took < Duration::from_secs(150),
        "commit held the caller for {took:?}"
    );

    restart(&node).await;
    let after = produce(&producer, "after").await;
    let tokens = consume_all(&consumer, &after, Duration::from_secs(120)).await;
    consumer
        .commit(&tokens)
        .await
        .expect("commit after the outage");
}

/// A consumer nothing polls through a broker outage picks up again soon after
/// the broker returns, not after one backoff per error librdkafka queued
/// meanwhile.
///
/// The outage must stay inside the group session, or the broker drops the
/// member and the consumer waits out a rejoin that is not what this test
/// measures. A JVM broker restart takes 15 s idle and 36 s on a busy
/// host, so the session is set well past any restart rather than racing it.
/// A 100 ms reconnect backoff keeps queueing errors through the outage.
#[tokio::test]
async fn a_consumer_left_unpolled_through_an_outage_resumes_promptly() {
    if !require_docker() {
        return;
    }
    const UNPOLLED: Duration = Duration::from_secs(15);
    const RESUME_WITHIN: Duration = Duration::from_secs(30);
    const SESSION_TIMEOUT_MS: u32 = 240_000;

    let (node, bootstrap) = broker_alone(test_name!()).await;
    create_topic(&bootstrap).await;
    let producer = raw_producer(&bootstrap);
    let consumer = KafkaTransport::new(
        &KafkaConfig {
            session_timeout_ms: SESSION_TIMEOUT_MS,
            ..consumer_config(&bootstrap, "outage-unpolled", ConsumerProtocol::Classic)
        }
        .with_override("reconnect.backoff.max.ms", "100"),
    )
    .await
    .expect("kafka consumer");

    let before = produce(&producer, "before").await;
    let tokens = consume_all(&consumer, &before, Duration::from_secs(60)).await;
    consumer
        .commit(&tokens)
        .await
        .expect("commit before the outage");

    stop(&node).await;
    tokio::time::sleep(UNPOLLED).await;
    restart(&node).await;

    // Delivery is the broker's own readiness signal: the clock starts once it
    // is serving again, however long the restart took.
    let after = produce(&producer, "after").await;
    let delivered = Instant::now();
    let tokens = consume_all(&consumer, &after, RESUME_WITHIN).await;
    eprintln!(
        "all {RECORDS} records produced after the restart consumed {:?} after their delivery",
        delivered.elapsed()
    );
    consumer
        .commit(&tokens)
        .await
        .expect("commit after the outage");
}

/// A topic that does not exist, with the consumer barred from creating it, is
/// waited for: every receive returns an empty batch and readiness is false.
///
/// Classic protocol: under KIP-848 librdkafka waits for a missing topic to
/// appear instead of raising an error.
#[tokio::test]
async fn a_missing_topic_fails_readiness_not_the_consumer() {
    if !require_docker() {
        return;
    }
    let bootstrap = shared_bootstrap().await;
    let config = KafkaConfig {
        topics: vec![common::unique("no-such-topic")],
        ..consumer_config(
            &bootstrap,
            &common::unique("outage-missing"),
            ConsumerProtocol::Classic,
        )
    };
    let consumer = KafkaTransport::new(&config).await.expect("kafka consumer");

    let deadline = Instant::now() + Duration::from_secs(60);
    while consumer.is_healthy() {
        assert!(
            Instant::now() < deadline,
            "readiness never reported the missing topic"
        );
        let batch = consumer
            .recv(100)
            .await
            .expect("a missing topic never fails a receive");
        assert_eq!(batch.records, [] as [scalo::Record; 0]);
    }
    let healthcheck = consumer.healthcheck().await;
    assert!(
        matches!(healthcheck, Err(TransportError::Connection(_))),
        "{healthcheck:?}"
    );
}
