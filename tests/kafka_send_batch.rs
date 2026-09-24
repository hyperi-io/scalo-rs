// Project:   scalo
// File:      tests/kafka_send_batch.rs
// Purpose:   Real-broker proof of the Kafka transport's pipelined send_batch
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-broker tests for `KafkaTransport::send_batch` against a Kafka container.
//!
//! NO mocks: scalo's own transport produces, and a raw rdkafka consumer grades
//! what landed, so scalo's read path never marks scalo's writes. The per-record
//! baseline is the trait's default `send_batch`, reached through a sender that
//! does not override it, so both legs run in the same harness against the same
//! broker.
//!
//! `#[ignore]` because they need a running Docker daemon -- run on a Docker host:
//!
//! ```text
//! cargo nextest run --features transport-kafka --test kafka_send_batch \
//!     --run-ignored only --no-capture
//! ```

#![cfg(feature = "transport-kafka")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{
    PayloadFormat, Record, RecordMeta, SendResult, TransportBase, TransportResult, TransportSender,
};
use testcontainers_modules::kafka::apache::{self, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Kafka to test against, pinned by digest.
///
/// testcontainers-modules defaults to 3.8.0; hoisting the reference into our
/// own source puts it under dependency review.
// renovate: datasource=docker depName=apache/kafka-native
const KAFKA_IMAGE_REF: &str =
    "4.3.1@sha256:2885898ba17065023f1bd605f3a81efcfa986014f062b73b91ef5462485f9060";

/// Block size: the order of a real transform batch.
const RECORDS: usize = 2_000;

/// Partitions per test topic, so the block spreads over more than one.
const PARTITIONS: i32 = 3;

/// Start a single-node KRaft broker and return it plus its bootstrap address.
async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
    let node = Kafka::default()
        .with_tag(KAFKA_IMAGE_REF)
        .start()
        .await
        .expect("start kafka container");
    let port = node
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka host port");
    (node, format!("127.0.0.1:{port}"))
}

/// A producer-only transport config: an empty group subscribes to nothing.
fn producer_config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    }
}

async fn create_topics(bootstrap: &str, topics: &[&str]) {
    let admin = KafkaAdmin::new(&producer_config(bootstrap)).expect("kafka admin");
    let specs: Vec<(&str, i32, i32)> = topics.iter().map(|t| (*t, PARTITIONS, 1)).collect();
    admin.create_topics(&specs).await.expect("create topics");
}

fn record(topic: &Arc<str>, payload: String) -> Record {
    Record {
        payload: bytes::Bytes::from(payload),
        key: Some(Arc::clone(topic)),
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

/// `RECORDS` records bound for `topic`, each payload unique within `run`.
fn block(topic: &str, run: &str) -> Vec<Record> {
    let topic: Arc<str> = Arc::from(topic);
    (0..RECORDS)
        .map(|seq| record(&topic, format!("{{\"run\":\"{run}\",\"seq\":{seq}}}")))
        .collect()
}

/// The trait's per-record `send_batch` default, reached through a sender that
/// forwards `send` and does not override `send_batch`.
struct PerRecord<'a>(&'a KafkaTransport);

impl TransportBase for PerRecord<'_> {
    fn close(&self) -> impl Future<Output = TransportResult<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn is_healthy(&self) -> bool {
        self.0.is_healthy()
    }

    fn name(&self) -> &'static str {
        "kafka-per-record"
    }
}

impl TransportSender for PerRecord<'_> {
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        self.0.send(destination, payload).await
    }
}

/// Every payload on `topic`, read from the beginning of each partition by a
/// raw consumer until the high watermarks are reached.
fn read_topic(bootstrap: &str, topic: &str) -> Vec<Vec<u8>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("verify-{topic}"))
        .set("enable.auto.commit", "false")
        .create()
        .expect("verify consumer");

    let mut assignment = TopicPartitionList::new();
    let mut expected = 0i64;
    for partition in 0..PARTITIONS {
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(10))
            .expect("fetch watermarks");
        expected += high - low;
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .expect("assign partition");
    }
    consumer.assign(&assignment).expect("assign");

    let expected = usize::try_from(expected).expect("non-negative record count");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut payloads = Vec::with_capacity(expected);
    while payloads.len() < expected && Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => payloads.push(msg.payload().unwrap_or_default().to_vec()),
            Some(Err(e)) => panic!("verify consumer error on {topic}: {e}"),
            None => {}
        }
    }
    assert_eq!(
        payloads.len(),
        expected,
        "read every record up to the high watermarks of {topic}"
    );
    payloads
}

async fn read_topic_async(bootstrap: &str, topic: &str) -> Vec<Vec<u8>> {
    let bootstrap = bootstrap.to_string();
    let topic = topic.to_string();
    tokio::task::spawn_blocking(move || read_topic(&bootstrap, &topic))
        .await
        .expect("verify task")
}

/// How many times each payload landed.
fn landed(payloads: Vec<Vec<u8>>) -> HashMap<Vec<u8>, usize> {
    let mut counts = HashMap::with_capacity(payloads.len());
    for payload in payloads {
        *counts.entry(payload).or_insert(0) += 1;
    }
    counts
}

#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn send_batch_pipelines_the_block_and_lands_each_record_once() {
    let (_node, bootstrap) = start_kafka().await;
    let (warm, per_record_topic, batch_topic) = ("warm", "per-record", "batch");
    create_topics(&bootstrap, &[warm, per_record_topic, batch_topic]).await;

    let transport = KafkaTransport::new(&producer_config(&bootstrap))
        .await
        .expect("kafka transport");
    // Connection, producer id and metadata are paid here, not by either leg.
    assert!(
        transport
            .send(warm, bytes::Bytes::from_static(b"{}"))
            .await
            .is_ok()
    );

    // The pipelined leg runs FIRST, so any warm-state advantage goes to the
    // per-record baseline it is measured against.
    let batch_records = block(batch_topic, "batch");
    let started = Instant::now();
    let batch_result = transport.send_batch(&batch_records).await;
    let batch_elapsed = started.elapsed();
    assert!(
        batch_result.is_ok(),
        "pipelined block failed: {batch_result:?}"
    );

    let per_record_records = block(per_record_topic, "per-record");
    let started = Instant::now();
    let per_record_result = PerRecord(&transport).send_batch(&per_record_records).await;
    let per_record_elapsed = started.elapsed();
    assert!(
        per_record_result.is_ok(),
        "per-record block failed: {per_record_result:?}"
    );

    eprintln!(
        "send_batch of {RECORDS} records: pipelined {} ms, per-record default {} ms",
        batch_elapsed.as_millis(),
        per_record_elapsed.as_millis()
    );
    assert!(
        batch_elapsed * 10 <= per_record_elapsed,
        "pipelined send_batch took {batch_elapsed:?} against {per_record_elapsed:?} \
         per-record -- expected at most a tenth"
    );

    let counts = landed(read_topic_async(&bootstrap, batch_topic).await);
    assert_eq!(counts.len(), RECORDS, "every record landed");
    for (payload, times) in &counts {
        assert_eq!(
            *times,
            1,
            "{} landed {times} times",
            String::from_utf8_lossy(payload)
        );
    }
    for record in &batch_records {
        assert!(
            counts.contains_key(record.payload.as_ref()),
            "missing {}",
            String::from_utf8_lossy(&record.payload)
        );
    }

    // The baseline delivered too, so its time is a real delivery time.
    assert_eq!(
        read_topic_async(&bootstrap, per_record_topic).await.len(),
        RECORDS
    );
}

/// Half the block goes to a topic the broker rejects. The other half is
/// confirmed and lands, and the block still reports failure, so the caller
/// retries it instead of committing records that were never delivered.
#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn send_batch_fails_a_partly_delivered_block() {
    let (_node, bootstrap) = start_kafka().await;
    let good_topic = "partial-good";
    create_topics(&bootstrap, &[good_topic]).await;
    // Space and '!' are outside Kafka's topic-name alphabet.
    let bad_topic: Arc<str> = Arc::from("partial bad!");
    let good: Arc<str> = Arc::from(good_topic);

    let transport = KafkaTransport::new(&producer_config(&bootstrap))
        .await
        .expect("kafka transport");

    let mixed: Vec<Record> = (0..200)
        .map(|seq| {
            let topic = if seq % 2 == 0 { &good } else { &bad_topic };
            record(topic, format!("{{\"seq\":{seq}}}"))
        })
        .collect();

    let result = tokio::time::timeout(Duration::from_secs(60), transport.send_batch(&mixed))
        .await
        .expect("every delivery report resolves");
    eprintln!("partly delivered block reported: {result:?}");
    assert!(
        !result.is_ok() && !result.is_filtered_dlq(),
        "a block with undelivered records must not report success, got {result:?}"
    );

    let counts = landed(read_topic_async(&bootstrap, good_topic).await);
    eprintln!(
        "records confirmed on {good_topic} despite the failed block: {}",
        counts.len()
    );
    assert!(
        !counts.is_empty(),
        "the good half was in flight with the bad half and should have landed"
    );
    assert!(
        counts.values().all(|times| *times == 1),
        "no record landed twice"
    );
}
