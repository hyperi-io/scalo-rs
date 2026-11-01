// Project:   scalo
// File:      tests/dlq_kafka_durable.rs
// Purpose:   Real-broker proof of what a Kafka DLQ flush() waits for and reports
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-broker tests for `Dlq::flush` over the Kafka backend.
//!
//! NO mocks: scalo's DLQ produces, and a raw rdkafka consumer reads what
//! landed. The broker is paused, stopped and restarted under a live DLQ, and
//! it refuses records past its own size ceiling.
//!
//! They need a Docker daemon: without one they skip on a developer's machine and
//! fail when `CI` is set.
//!
//! ```text
//! cargo nextest run --features dlq-kafka,transport-kafka-test-support --test dlq_kafka_durable --no-capture
//! ```

#![cfg(all(feature = "dlq-kafka", feature = "transport-kafka-test-support", unix))]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use scalo::dlq::{
    Dlq, DlqConfig, DlqEntry, DlqError, DlqMode, DlqRouting, FileDlqConfig, KafkaDlqConfig,
    RotationPeriod,
};
use scalo::test_name;
use scalo::transport::kafka::test_support::require_docker;
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig};
use tokio_util::sync::CancellationToken;

/// An ack wait long enough to outlast a paused broker, or one restarting.
const LONG_ACK_WAIT: Duration = Duration::from_secs(30);

/// The default `kafka.send_timeout_ms`, the barrier's ack wait.
fn ack_wait() -> Duration {
    Duration::from_millis(KafkaDlqConfig::default().send_timeout_ms)
}

/// `config` with its Kafka ack wait set to [`LONG_ACK_WAIT`].
fn long_ack_wait(mut config: DlqConfig) -> DlqConfig {
    config.kafka.send_timeout_ms = u64::try_from(LONG_ACK_WAIT.as_millis()).expect("fits");
    config
}

/// Record ceiling of the topics the broker refuses oversize records on.
const SMALL_TOPIC_MAX_BYTES: &str = "262144";

/// Raw payload bytes past [`SMALL_TOPIC_MAX_BYTES`] once base64'd, and
/// under the producer's own 16 MiB ceiling.
const OVER_BROKER_LIMIT: usize = 600_000;

/// Entries sent per phase.
const ENTRIES: usize = 5;

/// The run's shared broker, warmed up, and its bootstrap address.
async fn shared_broker() -> String {
    let bootstrap = common::kafka::shared_bootstrap().await;
    warm_up(&bootstrap).await;
    bootstrap
}

/// A broker only `test` uses, because the test pauses or stops it, warmed
/// up, and its bootstrap address.
async fn own_broker(
    test: &str,
) -> (
    scalo::deployment::test_support::shared_service::OwnedContainer,
    String,
) {
    let (node, bootstrap) = common::kafka::broker_alone(test).await;
    warm_up(&bootstrap).await;
    (node, bootstrap)
}

/// Wait until the broker has taken a record, off the topics under test.
///
/// A JVM broker that has just logged "started" can take longer than the 5 s
/// ack wait to answer its first produce, most of all with several brokers
/// starting on one host. A test whose own warm-up flush paid that cost failed
/// on the ack wait rather than on what it checks.
async fn warm_up(bootstrap: &str) {
    create_topic(bootstrap, WARM_UP_TOPIC).await;
    broker_takes_a_record(bootstrap).await;
}

/// The topic [`warm_up`] writes to, apart from the topics under test.
const WARM_UP_TOPIC: &str = "dlq.broker-warm-up";

/// Wait until the broker acks a record on [`WARM_UP_TOPIC`]: after a start,
/// and after a restart, which can take longer than the 30 s ack wait.
async fn broker_takes_a_record(bootstrap: &str) {
    const WARM_UP_WITHIN: Duration = Duration::from_secs(120);
    let topic = WARM_UP_TOPIC;
    let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("warm-up producer");
    producer
        .send(
            rdkafka::producer::FutureRecord::to(topic)
                .payload("warm")
                .key("warm"),
            WARM_UP_WITHIN,
        )
        .await
        .unwrap_or_else(|(e, _)| {
            panic!("the broker took no record within {WARM_UP_WITHIN:?}: {e}")
        });
}

/// The producer side of the DLQ: an empty group subscribes to nothing, and a
/// short reconnect backoff picks a restarted broker up promptly.
fn kafka_config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    }
    .with_override("reconnect.backoff.max.ms", "500")
}

async fn create_topic(bootstrap: &str, topic: &str) {
    let admin = KafkaAdmin::new(&kafka_config(bootstrap)).expect("kafka admin");
    admin
        .create_topics(&[(topic, 1, 1)])
        .await
        .expect("create topic");
}

/// Create `topic` and land one entry on it through a DLQ of its own with
/// [`LONG_ACK_WAIT`], so the topic's first produce is paid before the test's
/// own DLQ, whose ack wait is the default 5 s, writes to it. Under load that
/// first produce outlasted 5 s and failed a warm-up flush, not the check.
async fn create_warm_topic(bootstrap: &str, topic: &str) {
    create_topic(bootstrap, topic).await;
    let warmer = spawn_dlq(
        &long_ack_wait(dlq_config(DlqMode::KafkaOnly, topic, None)),
        &kafka_config(bootstrap),
    );
    warmer.send(entry("topic-warm-up")).await.expect("queued");
    warmer
        .flush()
        .await
        .expect("the topic took its first entry");
    warmer.shutdown().await.expect("warm-up DLQ shutdown");
}

/// A topic whose own record ceiling is [`SMALL_TOPIC_MAX_BYTES`], so the
/// broker refuses a record the producer queued. `KafkaAdmin` sets 16 MiB on
/// every topic it creates, hence the raw client.
async fn create_small_topic(bootstrap: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("raw admin");
    let spec = NewTopic::new(topic, 1, TopicReplication::Fixed(1))
        .set("max.message.bytes", SMALL_TOPIC_MAX_BYTES);
    for result in admin
        .create_topics(&[spec], &AdminOptions::new())
        .await
        .expect("create topics")
    {
        result.expect("topic created");
    }
}

/// Only the barrier writes: the batch and tick triggers are out of reach.
fn dlq_config(mode: DlqMode, topic: &str, file_dir: Option<&Path>) -> DlqConfig {
    DlqConfig {
        mode,
        queue_capacity: 1024,
        batch_size: 1024,
        flush_interval_ms: 600_000,
        file: FileDlqConfig {
            enabled: file_dir.is_some(),
            path: file_dir.map_or_else(|| "/nonexistent".into(), Path::to_path_buf),
            rotation: RotationPeriod::Daily,
            max_age_days: 1,
            compress_rotated: false,
        },
        kafka: KafkaDlqConfig {
            enabled: true,
            routing: DlqRouting::Common,
            common_topic: Some(topic.to_string()),
            ..KafkaDlqConfig::default()
        },
        ..DlqConfig::default()
    }
}

fn spawn_dlq(config: &DlqConfig, kafka: &KafkaConfig) -> Dlq {
    Dlq::spawn(config, "svc", Some(kafka), CancellationToken::new()).expect("spawn DLQ")
}

fn entry(reason: &str) -> DlqEntry {
    DlqEntry::new("svc", reason, br#"{"k":"v"}"#.to_vec())
}

/// An entry past [`SMALL_TOPIC_MAX_BYTES`] once base64'd. The bytes are
/// pseudo-random so compression cannot bring the record back under it.
fn oversize_entry(reason: &str) -> DlqEntry {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let payload: Vec<u8> = (0..OVER_BROKER_LIMIT)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect();
    DlqEntry::new("svc", reason, payload)
}

async fn send_all(dlq: &Dlq, prefix: &str, count: usize) {
    for i in 0..count {
        dlq.send(entry(&format!("{prefix}-{i}")))
            .await
            .expect("queued");
    }
}

/// The `reason` of every entry on `topic`, read from the start of partition 0
/// up to its high watermark.
fn read_reasons(bootstrap: &str, topic: &str) -> Vec<String> {
    read_entries(bootstrap, topic)
        .iter()
        .map(|entry| entry["reason"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Every entry on `topic` as JSON, read from the start of partition 0 up to
/// its high watermark.
fn read_entries(bootstrap: &str, topic: &str) -> Vec<serde_json::Value> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("verify-{topic}"))
        .set("enable.auto.commit", "false")
        .create()
        .expect("verify consumer");
    let (low, high) = consumer
        .fetch_watermarks(topic, 0, Duration::from_secs(10))
        .expect("fetch watermarks");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .expect("assign partition");
    consumer.assign(&assignment).expect("assign");

    let expected = usize::try_from(high - low).expect("non-negative record count");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut entries = Vec::with_capacity(expected);
    while entries.len() < expected && Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(msg)) => entries
                .push(serde_json::from_slice(msg.payload().unwrap_or_default()).expect("DLQ JSON")),
            Some(Err(e)) => panic!("verify consumer error on {topic}: {e}"),
            None => {}
        }
    }
    assert_eq!(
        entries.len(),
        expected,
        "read {topic} up to its high watermark"
    );
    entries
}

async fn read_reasons_async(bootstrap: &str, topic: &str) -> Vec<String> {
    let bootstrap = bootstrap.to_string();
    let topic = topic.to_string();
    tokio::task::spawn_blocking(move || read_reasons(&bootstrap, &topic))
        .await
        .expect("verify task")
}

/// The `reason` of every entry the file backend wrote, oldest first.
fn file_reasons(dir: &Path) -> Vec<String> {
    common::dlq_file_lines(dir, "svc")
        .iter()
        .map(|line| {
            let entry: serde_json::Value = serde_json::from_str(line).expect("DLQ JSON");
            entry["reason"].as_str().unwrap_or_default().to_string()
        })
        .collect()
}

/// Replace the file backend's directory with a regular file so every write fails.
fn break_file_backend(dir: &Path) {
    std::fs::remove_dir_all(dir.join("svc")).expect("remove DLQ dir");
    std::fs::write(dir.join("svc"), b"not a directory").expect("plant file");
}

/// A flush returns only once the broker has acked what it covers. With the
/// broker paused nothing can be acked, so the flush must still be waiting.
#[tokio::test]
async fn a_flush_returns_only_after_the_broker_acks() {
    if !require_docker() {
        return;
    }
    let topic = "dlq.acked";
    let (node, bootstrap) = own_broker(test_name!()).await;
    create_warm_topic(&bootstrap, topic).await;
    let dlq = spawn_dlq(
        &long_ack_wait(dlq_config(DlqMode::KafkaOnly, topic, None)),
        &kafka_config(&bootstrap),
    );
    // Connection and topic metadata are paid before the broker is paused.
    send_all(&dlq, "warm", 1).await;
    dlq.flush().await.expect("warm-up flush");

    common::kafka::pause(&node).await;
    send_all(&dlq, "acked", ENTRIES).await;
    let flusher = {
        let dlq = dlq.clone();
        tokio::spawn(async move { dlq.flush().await })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    let waited = !flusher.is_finished();
    common::kafka::unpause(&node).await;
    assert!(
        waited,
        "flush returned while the paused broker could not have acked anything"
    );

    let result = tokio::time::timeout(LONG_ACK_WAIT + Duration::from_secs(15), flusher)
        .await
        .expect("flush resolves within its bound")
        .expect("flush task");
    result.expect("flush is Ok once the broker acks");

    // Read straight after the flush: everything it covered is already there.
    let reasons = read_reasons_async(&bootstrap, topic).await;
    for i in 0..ENTRIES {
        let want = format!("acked-{i}");
        assert!(reasons.contains(&want), "{want} missing from {reasons:?}");
    }
    assert_eq!(dlq.dropped(), 0);
}

/// With the broker stopped, the flush gives up at its bound, reports the
/// loss, and counts every entry once -- and not again once the broker returns.
/// The long ack wait gives the producer time to find the restarted broker.
#[tokio::test]
async fn a_flush_with_the_broker_down_fails_within_its_bound_and_counts_the_loss() {
    if !require_docker() {
        return;
    }
    let topic = "dlq.down";
    let (node, bootstrap) = own_broker(test_name!()).await;
    create_warm_topic(&bootstrap, topic).await;
    let dlq = spawn_dlq(
        &long_ack_wait(dlq_config(DlqMode::KafkaOnly, topic, None)),
        &kafka_config(&bootstrap),
    );
    send_all(&dlq, "warm", 1).await;
    dlq.flush().await.expect("warm-up flush");

    common::kafka::stop(&node).await;
    send_all(&dlq, "lost", ENTRIES).await;
    let started = Instant::now();
    let result = dlq.flush().await;
    let took = started.elapsed();
    eprintln!("flush with the broker down returned {result:?} after {took:?}");
    assert!(
        matches!(result, Err(DlqError::Kafka(_))),
        "a flush the broker never acked must fail, got {result:?}"
    );
    assert!(
        took < LONG_ACK_WAIT + Duration::from_secs(10),
        "flush held the caller for {took:?}"
    );
    assert_eq!(dlq.dropped(), ENTRIES as u64, "every unacked entry counted");

    common::kafka::restart(&node).await;
    broker_takes_a_record(&bootstrap).await;
    send_all(&dlq, "after", ENTRIES).await;
    dlq.flush()
        .await
        .expect("flush is Ok once the broker is back");
    assert_eq!(
        dlq.dropped(),
        ENTRIES as u64,
        "the entries lost at the earlier flush are not counted again"
    );
    let reasons = read_reasons_async(&bootstrap, topic).await;
    assert!(
        !reasons.iter().any(|r| r.starts_with("lost-")),
        "an entry reported lost must not land later: {reasons:?}"
    );
    for i in 0..ENTRIES {
        let want = format!("after-{i}");
        assert!(reasons.contains(&want), "{want} missing from {reasons:?}");
    }
}

/// The flush waits on Kafka off the runtime: a single-threaded runtime keeps
/// running a ticking task for the whole wait.
#[tokio::test(flavor = "current_thread")]
async fn the_runtime_keeps_running_while_a_flush_waits_on_kafka() {
    if !require_docker() {
        return;
    }
    let topic = "dlq.runtime";
    let (node, bootstrap) = own_broker(test_name!()).await;
    create_warm_topic(&bootstrap, topic).await;
    let dlq = spawn_dlq(
        &dlq_config(DlqMode::KafkaOnly, topic, None),
        &kafka_config(&bootstrap),
    );
    send_all(&dlq, "warm", 1).await;
    dlq.flush().await.expect("warm-up flush");

    common::kafka::stop(&node).await;
    send_all(&dlq, "stalled", ENTRIES).await;

    let ticks = Arc::new(AtomicU64::new(0));
    let ticker = {
        let ticks = Arc::clone(&ticks);
        tokio::spawn(async move {
            let mut every = tokio::time::interval(Duration::from_millis(10));
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                every.tick().await;
                ticks.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    let started = Instant::now();
    let result = dlq.flush().await;
    let took = started.elapsed();
    ticker.abort();
    let advanced = ticks.load(Ordering::Relaxed);
    eprintln!("flush took {took:?}; the ticker advanced {advanced} times meanwhile");

    assert!(result.is_err(), "no broker, so the flush fails: {result:?}");
    assert!(
        took >= ack_wait(),
        "the flush returned after {took:?} -- it did not wait on Kafka"
    );
    let possible = u64::try_from(took.as_millis() / 10).unwrap_or(u64::MAX);
    assert!(
        advanced * 2 >= possible,
        "the ticker advanced {advanced} of a possible {possible} times -- the flush \
         held the runtime thread"
    );
}

/// A payload whose whole entry is over the topic's ceiling lands cut to
/// `max_payload_bytes`, saying how long it was, and nothing is lost.
#[tokio::test]
async fn an_oversize_payload_lands_truncated() {
    if !require_docker() {
        return;
    }
    let topic = common::unique("dlq.truncated");
    let topic = topic.as_str();
    let bootstrap = shared_broker().await;
    create_small_topic(&bootstrap, topic).await;
    let cap = 100_000;
    let dlq = spawn_dlq(
        &DlqConfig {
            max_payload_bytes: Some(cap),
            ..dlq_config(DlqMode::KafkaOnly, topic, None)
        },
        &kafka_config(&bootstrap),
    );

    dlq.send(oversize_entry("oversize")).await.expect("queued");
    dlq.flush().await.expect("the truncated entry lands");
    assert_eq!(dlq.dropped(), 0);

    let bootstrap_for_read = bootstrap.clone();
    let topic_for_read = topic.to_string();
    let entries =
        tokio::task::spawn_blocking(move || read_entries(&bootstrap_for_read, &topic_for_read))
            .await
            .expect("verify task");
    let [entry] = entries.as_slice() else {
        panic!("one entry on {topic}, got {}", entries.len());
    };
    let landed: DlqEntry = serde_json::from_value(entry.clone()).expect("a DLQ entry");
    assert_eq!(
        (
            landed.payload.len(),
            landed.truncated,
            landed.original_bytes
        ),
        (cap, true, Some(OVER_BROKER_LIMIT))
    );
}

/// A record the broker refuses fails the flush that covers it and is counted
/// once; entries acked around it are not.
#[tokio::test]
async fn a_delivery_the_broker_refuses_fails_the_flush_and_is_counted_once() {
    if !require_docker() {
        return;
    }
    let topic = common::unique("dlq.refused");
    let topic = topic.as_str();
    let bootstrap = shared_broker().await;
    create_small_topic(&bootstrap, topic).await;
    let dlq = spawn_dlq(
        &dlq_config(DlqMode::KafkaOnly, topic, None),
        &kafka_config(&bootstrap),
    );

    send_all(&dlq, "before", ENTRIES).await;
    dlq.flush().await.expect("acked entries flush clean");

    for i in 0..2 {
        dlq.send(oversize_entry(&format!("oversize-{i}")))
            .await
            .expect("queued");
    }
    let started = Instant::now();
    let result = dlq.flush().await;
    eprintln!(
        "flush over refused records returned {result:?} after {:?}",
        started.elapsed()
    );
    assert!(
        matches!(result, Err(DlqError::Kafka(_))),
        "a refused delivery must fail the flush, got {result:?}"
    );
    assert_eq!(dlq.dropped(), 2, "each refused record counted");

    send_all(&dlq, "after", ENTRIES).await;
    dlq.flush().await.expect("the next flush starts clean");
    assert_eq!(dlq.dropped(), 2, "a refusal is counted once");

    let reasons = read_reasons_async(&bootstrap, topic).await;
    assert_eq!(
        reasons.len(),
        2 * ENTRIES,
        "only the acked entries landed: {reasons:?}"
    );
}

/// Fan-out: a record Kafka loses is not a loss while the file backend holds it,
/// and is one once the file backend refused it too.
#[tokio::test]
async fn fan_out_counts_a_kafka_loss_only_where_no_other_backend_holds_the_entry() {
    if !require_docker() {
        return;
    }
    let topic = common::unique("dlq.fanout");
    let topic = topic.as_str();
    let bootstrap = shared_broker().await;
    create_small_topic(&bootstrap, topic).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let dlq = spawn_dlq(
        &dlq_config(DlqMode::FanOut, topic, Some(dir.path())),
        &kafka_config(&bootstrap),
    );

    for i in 0..2 {
        dlq.send(oversize_entry(&format!("mirrored-{i}")))
            .await
            .expect("queued");
    }
    dlq.flush()
        .await
        .expect("the file backend holds what Kafka refused");
    assert_eq!(dlq.dropped(), 0);
    assert_eq!(file_reasons(dir.path()), ["mirrored-0", "mirrored-1"]);

    break_file_backend(dir.path());
    for i in 0..2 {
        dlq.send(oversize_entry(&format!("kafka-only-{i}")))
            .await
            .expect("queued");
    }
    let result = dlq.flush().await;
    assert!(
        matches!(result, Err(DlqError::Kafka(_))),
        "Kafka was the only backend to take these, and lost them: {result:?}"
    );
    assert_eq!(dlq.dropped(), 2);
}

/// Cascade: when Kafka queues part of a batch and refuses the rest, the next
/// backend takes only the rest, so no entry is held twice or counted twice.
#[tokio::test]
async fn cascade_hands_the_next_backend_only_what_kafka_refused() {
    if !require_docker() {
        return;
    }
    let topic = common::unique("dlq.cascade");
    let topic = topic.as_str();
    let bootstrap = shared_broker().await;
    create_topic(&bootstrap, topic).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut kafka = kafka_config(&bootstrap);
    // Refuse the oversize entry locally, before anything reaches the broker.
    kafka.sizing.producer.message_max_bytes = Some(262_144);
    let dlq = spawn_dlq(
        &dlq_config(DlqMode::Cascade, topic, Some(dir.path())),
        &kafka,
    );

    dlq.send(entry("first")).await.expect("queued");
    dlq.send(entry("second")).await.expect("queued");
    dlq.send(oversize_entry("oversize")).await.expect("queued");
    dlq.send(entry("fourth")).await.expect("queued");
    dlq.flush().await.expect("every entry has a home");

    assert_eq!(dlq.dropped(), 0);
    assert_eq!(file_reasons(dir.path()), ["oversize", "fourth"]);
    assert_eq!(
        read_reasons_async(&bootstrap, topic).await,
        ["first", "second"]
    );
}

/// Shutdown with no flush delivers what the producer still held. `linger.ms`
/// keeps the entries queued in the producer when the drain exits, which is
/// when dropping the producer discards them.
#[tokio::test]
async fn shutdown_without_a_flush_delivers_what_the_producer_held() {
    if !require_docker() {
        return;
    }
    let topic = common::unique("dlq.shutdown.healthy");
    let topic = topic.as_str();
    let bootstrap = shared_broker().await;
    create_topic(&bootstrap, topic).await;
    let mut kafka = kafka_config(&bootstrap);
    kafka
        .sizing
        .producer_librdkafka
        .insert("linger.ms".to_string(), "3000".to_string());
    let dlq = spawn_dlq(&dlq_config(DlqMode::KafkaOnly, topic, None), &kafka);

    send_all(&dlq, "held", ENTRIES).await;
    dlq.shutdown().await.expect("shutdown");

    assert_eq!(dlq.dropped(), 0);
    let reasons = read_reasons_async(&bootstrap, topic).await;
    for i in 0..ENTRIES {
        let want = format!("held-{i}");
        assert!(reasons.contains(&want), "{want} missing from {reasons:?}");
    }
}

/// Shutdown with no flush and the broker stopped counts every entry no broker
/// acked, within the ack wait plus the purge.
#[tokio::test]
async fn shutdown_without_a_flush_counts_what_a_stopped_broker_never_acked() {
    if !require_docker() {
        return;
    }
    let topic = "dlq.shutdown.down";
    let (node, bootstrap) = own_broker(test_name!()).await;
    create_warm_topic(&bootstrap, topic).await;
    let dlq = spawn_dlq(
        &dlq_config(DlqMode::KafkaOnly, topic, None),
        &kafka_config(&bootstrap),
    );
    send_all(&dlq, "warm", 1).await;
    dlq.flush().await.expect("warm-up flush");

    common::kafka::stop(&node).await;
    send_all(&dlq, "lost", ENTRIES).await;
    let started = Instant::now();
    dlq.shutdown().await.expect("shutdown");
    let took = started.elapsed();
    eprintln!("shutdown with the broker down took {took:?}");

    assert_eq!(dlq.dropped(), ENTRIES as u64, "every unacked entry counted");
    assert!(
        took < ack_wait() + Duration::from_secs(10),
        "shutdown held the caller for {took:?}"
    );
}

/// `send_timeout_ms` is the barrier's ack wait: a value other than the
/// default bounds the flush.
#[tokio::test]
async fn the_configured_send_timeout_bounds_the_flush() {
    if !require_docker() {
        return;
    }
    const SEND_TIMEOUT: Duration = Duration::from_secs(15);
    let topic = "dlq.timeout";
    let (node, bootstrap) = own_broker(test_name!()).await;
    create_warm_topic(&bootstrap, topic).await;
    let mut config = dlq_config(DlqMode::KafkaOnly, topic, None);
    config.kafka.send_timeout_ms = u64::try_from(SEND_TIMEOUT.as_millis()).expect("fits");
    let dlq = spawn_dlq(&config, &kafka_config(&bootstrap));
    send_all(&dlq, "warm", 1).await;
    dlq.flush().await.expect("warm-up flush");

    common::kafka::stop(&node).await;
    send_all(&dlq, "lost", ENTRIES).await;
    let started = Instant::now();
    let result = dlq.flush().await;
    let took = started.elapsed();
    eprintln!("flush with a {SEND_TIMEOUT:?} send timeout returned {result:?} after {took:?}");

    assert!(result.is_err(), "no broker acked these: {result:?}");
    assert!(
        took >= SEND_TIMEOUT,
        "gave up after {took:?}, before the configured wait"
    );
    assert!(
        took < SEND_TIMEOUT + Duration::from_secs(10),
        "the configured wait did not bound the flush: {took:?}"
    );
}
