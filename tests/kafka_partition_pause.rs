// Project:   scalo
// File:      tests/kafka_partition_pause.rs
// Purpose:   Real-broker proof of per-partition pause, resume and seek
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-broker tests for `KafkaTransport::pause_partitions`,
//! `resume_partitions` and `seek_partition`: a held partition delivers
//! nothing while another flows, a seek reads it again from the seek point,
//! and the broker's committed offset never passes the seek point before the
//! copies read again are released.
//!
//! NO mocks: a real broker, filled by a raw rdkafka producer. They need a
//! Docker daemon: without one they skip on a developer's machine and fail
//! when `CI` is set.
//!
//! ```text
//! cargo nextest run --features transport-kafka,transport-kafka-test-support --test kafka_partition_pause --no-capture
//! ```

#![cfg(all(feature = "transport-kafka-test-support", unix))]

mod common;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use scalo::transport::kafka::test_support::require_docker;
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaToken, KafkaTransport};
use scalo::transport::{DeliveryStatus, TransportBase, TransportReceiver};

use common::kafka::shared_bootstrap;

/// Group join plus the first fetch, on a cold single-node broker.
const FIRST_RECORD_WITHIN: Duration = Duration::from_secs(60);

/// How long a held partition is watched for a record it must not deliver.
const QUIET_FOR: Duration = Duration::from_secs(3);

/// Create a topic of `partitions` and wait until the broker's metadata lists them.
async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin = KafkaAdmin::new(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    })
    .expect("kafka admin");
    admin
        .create_topics(&[(topic, partitions, 1)])
        .await
        .expect("create topic");
    let topic = topic.to_string();
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        while admin
            .describe_topic(&topic)
            .map_or(true, |t| t.partition_count < partitions)
        {
            assert!(
                Instant::now() < deadline,
                "topic {topic} never listed {partitions} partitions"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    })
    .await
    .expect("metadata wait");
}

/// Write `records` records to each of partitions 0 and 1 of `topic`.
async fn fill(bootstrap: &str, topic: &str, records: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "5")
        .create()
        .expect("raw producer");
    for partition in 0..2 {
        for seq in 0..records {
            let payload = format!("p{partition}-{seq}");
            producer
                .send(
                    FutureRecord::<(), str>::to(topic)
                        .partition(partition)
                        .payload(&payload),
                    Duration::from_secs(30),
                )
                .await
                .unwrap_or_else(|(e, _)| panic!("deliver {payload}: {e}"));
        }
    }
}

/// An armed consumer transport on `topic` in group `group`, reading from the start.
async fn armed_consumer(bootstrap: &str, topic: &str, group: &str) -> KafkaTransport {
    let transport = KafkaTransport::receiver(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: group.to_string(),
        topics: vec![topic.to_string()],
        ..Default::default()
    })
    .await
    .expect("kafka consumer");
    transport
        .ack_control()
        .expect("a kafka transport has acknowledgements")
        .arm();
    transport
}

/// Receive until `want` says the tokens so far are enough, or `within` ends.
async fn receive_until(
    transport: &KafkaTransport,
    within: Duration,
    want: impl Fn(&[KafkaToken]) -> bool,
) -> Vec<KafkaToken> {
    let deadline = Instant::now() + within;
    let mut tokens = Vec::new();
    while !want(&tokens) && Instant::now() < deadline {
        let batch = transport.recv(500).await.expect("recv on a healthy broker");
        tokens.extend(batch.commit_tokens);
    }
    tokens
}

/// The offsets `tokens` hold of `partition`, in the order received.
fn offsets_of(tokens: &[KafkaToken], partition: i32) -> Vec<i64> {
    tokens
        .iter()
        .filter(|t| t.partition == partition)
        .map(|t| t.offset)
        .collect()
}

/// The group's committed next-to-read offset per partition, from the broker.
async fn committed(bootstrap: &str, group: &str, topic: &str) -> BTreeMap<i32, i64> {
    let (bootstrap, group, topic) = (bootstrap.to_string(), group.to_string(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        let reader: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", &group)
            .create()
            .expect("raw consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&topic, 0);
        tpl.add_partition(&topic, 1);
        reader
            .committed_offsets(tpl, Duration::from_secs(10))
            .expect("committed offsets")
            .elements()
            .iter()
            .filter_map(|e| match e.offset() {
                Offset::Offset(next) => Some((e.partition(), next)),
                _ => None,
            })
            .collect()
    })
    .await
    .expect("committed task")
}

/// Partition 0 is read to offset 40, its records from 10 on are not kept, so
/// it is held and sought back to 10. Partition 1 flows on meanwhile, the
/// broker's commit for partition 0 stays at 10 until the copies read again
/// are released, and the resumed partition delivers 10 onwards once each.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_partition_waits_and_reads_again_from_its_seek_point() {
    if !require_docker() {
        return;
    }
    const FIRST: usize = 40;
    let bootstrap = shared_bootstrap().await;
    let topic = common::unique("pause-seek");
    let group = topic.clone();
    create_topic(&bootstrap, &topic, 2).await;
    fill(&bootstrap, &topic, FIRST).await;
    let transport = armed_consumer(&bootstrap, &topic, &group).await;

    let handed_out = receive_until(&transport, FIRST_RECORD_WITHIN, |t| {
        offsets_of(t, 0).len() >= FIRST && offsets_of(t, 1).len() >= FIRST
    })
    .await;
    assert_eq!(offsets_of(&handed_out, 0), (0..40).collect::<Vec<i64>>());
    let (kept, not_kept): (Vec<KafkaToken>, Vec<KafkaToken>) = handed_out
        .iter()
        .filter(|t| t.partition == 0)
        .cloned()
        .partition(|t| t.offset < 10);
    let other: Vec<KafkaToken> = handed_out
        .iter()
        .filter(|t| t.partition == 1)
        .cloned()
        .collect();

    transport
        .release(&kept, DeliveryStatus::Delivered)
        .await
        .expect("release the kept records");
    transport
        .pause_partitions(&[(topic.as_str(), 0)])
        .expect("pause partition 0");
    transport
        .seek_partition(&topic, 0, 10)
        .await
        .expect("seek partition 0 back to 10");
    assert_eq!(
        transport.paused_partitions(),
        vec![(std::sync::Arc::<str>::from(topic.as_str()), 0)]
    );
    transport
        .release(&not_kept, DeliveryStatus::Dropped)
        .await
        .expect("release the old copies after the seek");
    transport
        .release(&other, DeliveryStatus::Delivered)
        .await
        .expect("release partition 1");
    let at_seek = committed(&bootstrap, &group, &topic).await;
    assert_eq!(
        at_seek.get(&0),
        Some(&10),
        "the commit stops at the seek point: {at_seek:?}"
    );
    assert_eq!(at_seek.get(&1), Some(&40), "{at_seek:?}");

    fill(&bootstrap, &topic, 10).await;
    let while_held = receive_until(&transport, QUIET_FOR, |_| false).await;
    assert!(
        offsets_of(&while_held, 0).is_empty(),
        "a held partition delivered {:?}",
        offsets_of(&while_held, 0)
    );
    assert_eq!(
        offsets_of(&while_held, 1),
        (40..50).collect::<Vec<i64>>(),
        "the other partition flows on"
    );

    transport
        .resume_partitions(&[(topic.as_str(), 0)])
        .expect("resume partition 0");
    assert_eq!(
        transport.paused_partitions(),
        Vec::<(std::sync::Arc<str>, i32)>::new()
    );
    let read_again = receive_until(&transport, FIRST_RECORD_WITHIN, |t| {
        offsets_of(t, 0).len() >= 40
    })
    .await;
    assert_eq!(
        offsets_of(&read_again, 0),
        (10..50).collect::<Vec<i64>>(),
        "the seek point first, each record once"
    );
    let resumed: Vec<KafkaToken> = read_again
        .iter()
        .filter(|t| t.partition == 0)
        .cloned()
        .collect();
    transport
        .release(&resumed, DeliveryStatus::Delivered)
        .await
        .expect("release the copies read again");
    let after = committed(&bootstrap, &group, &topic).await;
    assert_eq!(after.get(&0), Some(&50), "{after:?}");
    transport.close().await.expect("close");
}

/// A seek while records of the partition sit in librdkafka's local queue
/// hands out none of them: the first record after the seek is the one at
/// the seek point.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_hands_out_nothing_fetched_from_before_it() {
    if !require_docker() {
        return;
    }
    let bootstrap = shared_bootstrap().await;
    let topic = common::unique("seek-prefetch");
    create_topic(&bootstrap, &topic, 2).await;
    fill(&bootstrap, &topic, 2_000).await;
    let transport = armed_consumer(&bootstrap, &topic, &topic).await;

    // A small receive, so most of the partition is still in the local queue.
    let deadline = Instant::now() + FIRST_RECORD_WITHIN;
    let mut first = Vec::new();
    while offsets_of(&first, 0).len() < 5 {
        assert!(Instant::now() < deadline, "partition 0 delivered nothing");
        first.extend(transport.recv(5).await.expect("recv").commit_tokens);
    }
    transport
        .seek_partition(&topic, 0, 2)
        .await
        .expect("seek partition 0 back to 2");
    let after = receive_until(&transport, FIRST_RECORD_WITHIN, |t| {
        !offsets_of(t, 0).is_empty()
    })
    .await;
    assert_eq!(
        offsets_of(&after, 0).first(),
        Some(&2),
        "the first record of partition 0 after the seek is the seek point"
    );
    transport.close().await.expect("close");
}
