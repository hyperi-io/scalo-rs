// Project:   scalo
// File:      src/transport/kafka/rebuild_tests.rs
// Purpose:   Consumer rebuilds against a real Kafka broker
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Consumer rebuilds against the run's shared broker. Each new client joins a
//! real group and is assigned a real partition, and only the fatal error that
//! starts a rebuild is given, through `replace_fatal_consumer`. Every test
//! needs Docker, so each is ignored and runs in the full tier.

use std::time::{Duration, Instant};

use rdkafka::error::RDKafkaErrorCode;

use super::{ConsumerProtocol, KafkaConfig, KafkaTransport, REBUILD_QUIET};
use crate::deployment::test_support::shared_service::Suite;
use crate::transport::kafka::KafkaAdmin;
use crate::transport::kafka::test_support::{KAFKA_PORT, require_docker, shared_kafka};
use crate::transport::{TransportBase, TransportReceiver, TransportSender};

const SUITE: Suite = Suite {
    repo: "scalo-rs",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// The longest a client takes to join and be assigned, or a record to arrive.
const WITHIN: Duration = Duration::from_secs(60);

/// The shared broker's address, `None` where no Docker daemon answers outside CI.
async fn broker() -> Option<String> {
    if !require_docker() {
        return None;
    }
    Some(shared_kafka(SUITE).await.addr(KAFKA_PORT))
}

/// A one-partition topic unique to this process, and a classic consumer of it
/// in a group of its own.
async fn consumer_of_new_topic(bootstrap: &str, name: &str) -> (String, KafkaConfig) {
    let topic = format!("rebuild-{name}-p{}", std::process::id());
    let admin = KafkaAdmin::new(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    })
    .expect("kafka admin");
    admin
        .create_topics(&[(topic.as_str(), 1, 1)])
        .await
        .expect("create topic");
    let named = topic.clone();
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + WITHIN;
        while admin
            .describe_topic(&named)
            .ok()
            .is_none_or(|t| t.partition_count == 0)
        {
            assert!(Instant::now() < deadline, "topic {named} never appeared");
            std::thread::sleep(Duration::from_millis(100)); // allow-sync-in-async: inside spawn_blocking
        }
    })
    .await
    .expect("metadata wait");
    let config = KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: format!("{topic}-group"),
        topics: vec![topic.clone()],
        consumer_protocol: ConsumerProtocol::Classic,
        ..Default::default()
    };
    (topic, config)
}

/// Write `count` records to `topic` and wait for every delivery.
async fn produce(bootstrap: &str, topic: &str, count: usize) {
    let sender = KafkaTransport::sender(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    })
    .await
    .expect("kafka sender");
    for seq in 0..count {
        let sent = sender
            .send(topic, bytes::Bytes::from(format!("{{\"seq\":{seq}}}")))
            .await;
        assert!(sent.is_ok(), "deliver record {seq}: {sent:?}");
    }
}

/// Receive until the current client is assigned partition 0 of `topic`.
/// Returns the records those receives read.
async fn receive_until_assigned(transport: &KafkaTransport, topic: &str) -> usize {
    let deadline = Instant::now() + WITHIN;
    let mut read = 0;
    while transport.lease(topic, 0).is_none() {
        assert!(
            Instant::now() < deadline,
            "not assigned {topic} within {WITHIN:?}"
        );
        read += transport.recv(100).await.expect("recv").records.len();
    }
    read
}

/// Receive until `count` records have arrived, failing after [`WITHIN`].
async fn receive_records(transport: &KafkaTransport, count: usize) {
    let deadline = Instant::now() + WITHIN;
    let mut read = 0;
    while read < count {
        assert!(
            Instant::now() < deadline,
            "read {read} of {count} within {WITHIN:?}"
        );
        read += transport.recv(100).await.expect("recv").records.len();
    }
}

/// Two fatal rebuilds on a topic no record reaches. Back to back, the second
/// fails readiness and spaces the next poll. With [`REBUILD_QUIET`] between
/// them, the second starts a new run and keeps readiness. Either way the
/// client the second built joins, is ready once it has run [`REBUILD_QUIET`]
/// with no record, and reads.
#[tokio::test]
#[ignore = "needs Docker for a Kafka broker -- run: cargo nextest run --run-ignored all --ignore-default-filter"]
async fn a_fatal_after_a_quiet_period_starts_a_new_run() {
    let Some(bootstrap) = broker().await else {
        return;
    };
    // (case, how long before the second fatal the first rebuild ran, ready after it)
    let cases = [
        ("back-to-back", Duration::ZERO, false),
        ("quiet", REBUILD_QUIET, true),
    ];
    for (case, gap, ready) in cases {
        let (topic, config) = consumer_of_new_topic(&bootstrap, case).await;
        let transport = KafkaTransport::new(&config).await.expect("consumer");
        receive_until_assigned(&transport, &topic).await;

        transport
            .replace_fatal_consumer(RDKafkaErrorCode::Fatal, "first")
            .await
            .expect("first rebuild");
        assert_eq!(
            receive_until_assigned(&transport, &topic).await,
            0,
            "{case}"
        );
        assert!(
            transport.is_healthy(),
            "{case}: one rebuild keeps readiness"
        );

        let rebuilt_at = Instant::now().checked_sub(gap);
        *transport.last_rebuild.lock() = Some(rebuilt_at.expect("the clock reaches back"));
        transport
            .replace_fatal_consumer(RDKafkaErrorCode::Fatal, "second")
            .await
            .expect("second rebuild");
        assert_eq!(transport.is_healthy(), ready, "{case}: readiness");
        assert_eq!(
            transport.rebuild_not_before.lock().is_some(),
            !ready,
            "{case}: only a repeat spaces the next poll"
        );

        assert_eq!(
            receive_until_assigned(&transport, &topic).await,
            0,
            "{case}"
        );
        assert_eq!(
            transport.is_healthy(),
            ready,
            "{case}: assigned, still quiet"
        );
        *transport.last_rebuild.lock() = Instant::now().checked_sub(REBUILD_QUIET);
        assert_eq!(transport.recv(100).await.expect("recv").records.len(), 0);
        assert!(
            transport.is_healthy(),
            "{case}: ready once quiet, with no record"
        );

        produce(&bootstrap, &topic, 1).await;
        receive_records(&transport, 1).await;
        assert!(transport.is_healthy(), "{case}: a record keeps readiness");
    }
}

/// A client rebuilt while the inbound gate holds is assigned the partition
/// and reads nothing until the gate releases, then reads every record.
#[cfg(feature = "governor")]
#[tokio::test]
#[ignore = "needs Docker for a Kafka broker -- run: cargo nextest run --run-ignored all --ignore-default-filter"]
async fn a_client_rebuilt_while_the_gate_holds_reads_nothing_until_it_releases() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::governor::{Hysteresis, InboundGate, Pressure, PressureSource, UnifiedPressure};

    /// A pressure level the test sets.
    struct Level(AtomicU64);
    impl PressureSource for Level {
        fn name(&self) -> &'static str {
            "test"
        }
        fn sample(&self) -> Pressure {
            Pressure::new(f64::from_bits(self.0.load(Ordering::Relaxed)))
        }
        fn is_hard(&self) -> bool {
            true
        }
    }
    const RECORDS: usize = 20;
    /// How long the rebuilt client is watched for a record under the hold.
    const WATCH: Duration = Duration::from_secs(5);

    let Some(bootstrap) = broker().await else {
        return;
    };
    let (topic, config) = consumer_of_new_topic(&bootstrap, "gate-held").await;
    produce(&bootstrap, &topic, RECORDS).await;
    let level = Arc::new(Level(AtomicU64::new(0.95_f64.to_bits())));
    let pressure = UnifiedPressure::new(
        vec![Arc::clone(&level) as Arc<dyn PressureSource>],
        Hysteresis::new(0.80, 0.65).expect("valid band"),
    )
    .with_max_hold(Duration::from_secs(600));
    let transport = KafkaTransport::new(&config).await.expect("consumer");
    let actuator = transport.gate_actuator();
    let transport = transport.with_inbound_gate(InboundGate::new(Arc::new(pressure), actuator));

    assert_eq!(
        receive_until_assigned(&transport, &topic).await,
        0,
        "held from the start"
    );
    transport
        .replace_fatal_consumer(RDKafkaErrorCode::Fatal, "rebuilt under the hold")
        .await
        .expect("rebuild");
    let mut read = receive_until_assigned(&transport, &topic).await;
    let until = Instant::now() + WATCH;
    while Instant::now() < until {
        read += transport.recv(100).await.expect("recv").records.len();
    }
    assert_eq!(read, 0, "the rebuilt client read under the hold");

    level.0.store(0.10_f64.to_bits(), Ordering::Relaxed);
    receive_records(&transport, RECORDS).await;
}
