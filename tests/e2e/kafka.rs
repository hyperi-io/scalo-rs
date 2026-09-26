// Project:   scalo
// File:      tests/e2e/kafka.rs
// Purpose:   Kafka transport integration tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for Kafka transport.
//!
//! These tests require a running Kafka broker. They are ignored by default.
//! Run with: `TEST_KAFKA_BROKERS=localhost:9092 cargo test --features transport-kafka -- --ignored`
//!
//! The held-acknowledgement tests start their own broker in a container instead:
//! they build with the `testcontainers` feature and need a Docker daemon.
//!
//! Or set up via environment variables:
//! - `TEST_KAFKA_BROKERS`: Kafka broker addresses (default: localhost:9092)
//! - `TEST_KAFKA_TOPIC`: Test topic name (default: scalo-test)
//! - `TEST_KAFKA_GROUP`: Consumer group ID (default: scalo-test-group)

use scalo::transport::kafka::{
    BrokerMetrics, DEVTEST_PROFILE, KafkaAdmin, KafkaConfig, KafkaMetrics, KafkaProfile,
    KafkaToken, PRODUCTION_PROFILE, StatsContext, TopicInfo, healthy_broker_count,
    total_consumer_lag,
};
use std::sync::Arc;

use crate::common::EnvGuard;

// --- Unit Tests (no Kafka required) ---

// --- Profile Tests ---

#[test]
fn test_kafka_profile_default_is_production() {
    let config = KafkaConfig::default();
    assert_eq!(config.profile, KafkaProfile::Production);
}

#[test]
fn test_kafka_profile_production() {
    let config = KafkaConfig::production();
    assert_eq!(config.profile, KafkaProfile::Production);
    assert!(!config.ssl_skip_verify);
}

#[test]
fn test_kafka_profile_devtest() {
    let config = KafkaConfig::devtest();
    assert_eq!(config.profile, KafkaProfile::DevTest);
    assert!(config.ssl_skip_verify); // Auto-enabled for devtest
}

#[test]
fn test_kafka_profile_with_profile() {
    let config = KafkaConfig::default().with_profile(KafkaProfile::DevTest);
    assert_eq!(config.profile, KafkaProfile::DevTest);
    assert!(config.ssl_skip_verify);
}

#[test]
fn test_kafka_profile_defaults_production() {
    let config = KafkaConfig::production();
    let defaults = config.profile_defaults();
    assert_eq!(defaults, PRODUCTION_PROFILE);
}

#[test]
fn test_kafka_profile_defaults_devtest() {
    let config = KafkaConfig::devtest();
    let defaults = config.profile_defaults();
    assert_eq!(defaults, DEVTEST_PROFILE);
}

#[test]
fn test_kafka_profile_from_str() {
    assert_eq!(
        "production".parse::<KafkaProfile>().unwrap(),
        KafkaProfile::Production
    );
    assert_eq!(
        "prod".parse::<KafkaProfile>().unwrap(),
        KafkaProfile::Production
    );
    assert_eq!(
        "devtest".parse::<KafkaProfile>().unwrap(),
        KafkaProfile::DevTest
    );
    assert_eq!(
        "dev".parse::<KafkaProfile>().unwrap(),
        KafkaProfile::DevTest
    );
    assert_eq!(
        "test".parse::<KafkaProfile>().unwrap(),
        KafkaProfile::DevTest
    );
    assert!("invalid".parse::<KafkaProfile>().is_err());
}

#[test]
fn test_kafka_profile_display() {
    assert_eq!(KafkaProfile::Production.to_string(), "production");
    assert_eq!(KafkaProfile::DevTest.to_string(), "devtest");
}

// --- Override Tests ---

#[test]
fn test_kafka_config_with_override() {
    let config = KafkaConfig::production().with_override("fetch.min.bytes", "2097152");

    assert_eq!(
        config.librdkafka_overrides.get("fetch.min.bytes"),
        Some(&"2097152".to_string())
    );
}

#[test]
fn test_kafka_config_with_overrides() {
    let config = KafkaConfig::production().with_overrides(&[
        ("fetch.min.bytes", "2097152"),
        ("statistics.interval.ms", "5000"),
    ]);

    assert_eq!(
        config.librdkafka_overrides.get("fetch.min.bytes"),
        Some(&"2097152".to_string())
    );
    assert_eq!(
        config.librdkafka_overrides.get("statistics.interval.ms"),
        Some(&"5000".to_string())
    );
}

#[test]
fn test_kafka_build_librdkafka_config_priority() {
    let mut config = KafkaConfig::production();
    // Profile sets fetch.min.bytes = 1048576 (1MB)
    // Override should win
    config
        .librdkafka_overrides
        .insert("fetch.min.bytes".to_string(), "2097152".to_string());

    let built = config.build_librdkafka_config();
    assert_eq!(built.get("fetch.min.bytes"), Some(&"2097152".to_string()));
}

// --- Basic Config Tests ---

// Placeholder SASL credentials: these tests check the built config and reach no broker.

#[test]
fn test_kafka_config_defaults() {
    let config = KafkaConfig::default();

    assert_eq!(config.brokers, vec!["localhost:9092"]);
    assert_eq!(config.group, "scalo-consumer");
    assert_eq!(config.client_id, "scalo");
    assert!(!config.enable_auto_commit);
    assert_eq!(config.auto_offset_reset, "earliest");
    assert_eq!(config.fetch_max_bytes, 52_428_800); // 50MB
    assert_eq!(config.session_timeout_ms, 45000);
    assert_eq!(config.heartbeat_interval_ms, 3000);
    assert_eq!(config.max_poll_interval_ms, 300_000);
}

#[test]
fn test_kafka_config_for_testing() {
    let config = KafkaConfig::for_testing("kafka:9092", "test-group", vec!["events".to_string()]);

    assert_eq!(config.brokers, vec!["kafka:9092"]);
    assert_eq!(config.group, "test-group");
    assert_eq!(config.topics, vec!["events"]);
}

#[test]
fn test_kafka_config_with_scram() {
    let config = KafkaConfig::default().with_scram("SCRAM-SHA-256", "user", "pass");

    assert_eq!(config.security_protocol, "sasl_plaintext");
    assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-256".to_string()));
    assert_eq!(config.sasl_username, Some("user".to_string()));
    assert_eq!(
        config.sasl_password.as_ref().map(|p| p.expose()),
        Some("pass")
    );
}

#[test]
fn test_kafka_config_with_scram_ssl() {
    let config = KafkaConfig::default().with_scram_ssl("SCRAM-SHA-512", "user", "pass");

    assert_eq!(config.security_protocol, "sasl_ssl");
    assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-512".to_string()));
}

#[test]
fn test_kafka_config_with_tls() {
    let config = KafkaConfig::default().with_tls(Some("/path/to/ca.crt"));

    assert_eq!(config.security_protocol, "ssl");
    assert_eq!(config.ssl_ca_location, Some("/path/to/ca.crt".to_string()));
}

#[test]
fn test_kafka_config_with_tls_upgrades_sasl() {
    let config = KafkaConfig::default()
        .with_scram("PLAIN", "user", "pass")
        .with_tls(None);

    assert_eq!(config.security_protocol, "sasl_ssl");
}

#[test]
fn test_kafka_config_with_client_cert() {
    let config = KafkaConfig::default().with_client_cert("/path/cert.pem", "/path/key.pem");

    assert_eq!(
        config.ssl_certificate_location,
        Some("/path/cert.pem".to_string())
    );
    assert_eq!(config.ssl_key_location, Some("/path/key.pem".to_string()));
}

#[test]
fn test_kafka_config_with_ssl_skip_verify() {
    let config = KafkaConfig::default().with_ssl_skip_verify();

    assert!(config.ssl_skip_verify);
}

#[test]
fn test_kafka_config_with_ssl_insecure() {
    let config = KafkaConfig::default().with_ssl_insecure();

    assert_eq!(config.security_protocol, "ssl");
    assert!(config.ssl_skip_verify);
}

#[test]
fn test_kafka_config_with_ssl_insecure_upgrades_sasl() {
    let config = KafkaConfig::default()
        .with_scram("PLAIN", "user", "pass")
        .with_ssl_insecure();

    assert_eq!(config.security_protocol, "sasl_ssl");
    assert!(config.ssl_skip_verify);
}

#[test]
#[allow(deprecated)]
fn test_kafka_config_with_producer_defaults() {
    let config = KafkaConfig::default().with_producer_defaults();
    let built = config.build_librdkafka_config();

    // The sizing surface owns batching and codec on every producer path, so
    // the profile constant names neither -- see PRODUCER_HIGH_THROUGHPUT.
    assert_eq!(built.get("compression.type"), None);
    assert_eq!(built.get("linger.ms"), None);
    assert_eq!(built.get("socket.nagle.disable"), Some(&"true".to_string()));
    assert_eq!(
        built.get("statistics.interval.ms"),
        Some(&"1000".to_string())
    );
}

#[test]
fn test_kafka_production_profile_settings() {
    let config = KafkaConfig::production();
    let built = config.build_librdkafka_config();

    assert_eq!(
        built.get("partition.assignment.strategy"),
        Some(&"cooperative-sticky".to_string())
    );
    assert_eq!(built.get("fetch.min.bytes"), Some(&"1048576".to_string()));
    assert_eq!(built.get("fetch.wait.max.ms"), Some(&"100".to_string()));
    assert_eq!(built.get("queued.min.messages"), Some(&"20000".to_string()));
    assert_eq!(built.get("enable.auto.commit"), Some(&"false".to_string()));
    assert_eq!(
        built.get("statistics.interval.ms"),
        Some(&"1000".to_string())
    );
    // Verify removed settings are gone
    assert_eq!(built.get("check.crcs"), None);
    assert_eq!(built.get("socket.nagle.disable"), None);
    assert_eq!(built.get("queued.max.messages.kbytes"), None);
}

#[test]
fn test_kafka_devtest_profile_settings() {
    let config = KafkaConfig::devtest();
    let built = config.build_librdkafka_config();

    assert_eq!(built.get("queued.min.messages"), Some(&"1000".to_string()));
    assert_eq!(
        built.get("partition.assignment.strategy"),
        Some(&"cooperative-sticky".to_string())
    );
    assert_eq!(built.get("enable.auto.commit"), Some(&"false".to_string()));
    assert_eq!(built.get("reconnect.backoff.ms"), Some(&"10".to_string()));
    assert_eq!(
        built.get("reconnect.backoff.max.ms"),
        Some(&"100".to_string())
    );
    assert_eq!(built.get("log.connection.close"), Some(&"true".to_string()));
    // Verify removed settings are gone
    assert_eq!(built.get("check.crcs"), None);
    assert_eq!(built.get("queued.max.messages.kbytes"), None);
}

#[test]
#[allow(deprecated)]
fn test_kafka_config_with_low_latency() {
    let config = KafkaConfig::default().with_low_latency();
    let built = config.build_librdkafka_config();

    assert_eq!(built.get("fetch.wait.max.ms"), Some(&"10".to_string()));
    assert_eq!(built.get("reconnect.backoff.ms"), Some(&"10".to_string()));
    assert_eq!(
        built.get("reconnect.backoff.max.ms"),
        Some(&"100".to_string())
    );
    assert_eq!(built.get("queued.min.messages"), Some(&"1000".to_string()));
}

#[test]
fn test_kafka_config_with_statistics() {
    let config = KafkaConfig::default().with_statistics(5000);

    // with_statistics uses librdkafka_overrides
    assert_eq!(
        config.librdkafka_overrides.get("statistics.interval.ms"),
        Some(&"5000".to_string())
    );

    // Also verify it appears in built config
    let built = config.build_librdkafka_config();
    assert_eq!(
        built.get("statistics.interval.ms"),
        Some(&"5000".to_string())
    );
}

#[test]
fn test_kafka_config_with_cloud_connection_tuning() {
    let config = KafkaConfig::default().with_cloud_connection_tuning();
    let built = config.build_librdkafka_config();

    // Cloud tuning is in librdkafka_overrides
    assert_eq!(
        built.get("socket.keepalive.enable"),
        Some(&"true".to_string())
    );
    assert_eq!(
        built.get("metadata.max.age.ms"),
        Some(&"180000".to_string())
    );
    assert_eq!(
        built.get("socket.connection.setup.timeout.ms"),
        Some(&"30000".to_string())
    );
}

#[test]
fn test_kafka_config_chained_builders() {
    // Test that all builder methods can be chained together
    let config = KafkaConfig::production()
        .with_scram_ssl("SCRAM-SHA-512", "user", "pass")
        .with_statistics(1000)
        .with_cloud_connection_tuning()
        .with_override("fetch.min.bytes", "2097152");

    let built = config.build_librdkafka_config();

    // Verify SASL
    assert_eq!(config.security_protocol, "sasl_ssl");
    assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-512".to_string()));

    // Verify production profile defaults are present
    assert_eq!(built.get("queued.min.messages"), Some(&"20000".to_string()));

    // Verify statistics override
    assert_eq!(
        built.get("statistics.interval.ms"),
        Some(&"1000".to_string())
    );

    // Verify cloud tuning
    assert_eq!(
        built.get("socket.keepalive.enable"),
        Some(&"true".to_string())
    );

    // Verify explicit override wins
    assert_eq!(built.get("fetch.min.bytes"), Some(&"2097152".to_string()));
}

#[test]
fn test_kafka_config_from_env() {
    let _guard = EnvGuard::new(&[
        ("TESTAPP_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092"),
        ("TESTAPP_GROUP_ID", "test-consumer"),
        ("TESTAPP_CLIENT_ID", "test-client"),
        ("TESTAPP_SECURITY_PROTOCOL", "sasl_ssl"),
        ("TESTAPP_SASL_MECHANISM", "SCRAM-SHA-256"),
        ("TESTAPP_SASL_USERNAME", "testuser"),
        ("TESTAPP_SASL_PASSWORD", "testpass"),
        ("TESTAPP_SSL_SKIP_VERIFY", "true"),
        ("TESTAPP_TOPICS", "topic1,topic2,topic3"),
    ]);

    let config = KafkaConfig::from_env("TESTAPP");

    assert_eq!(config.brokers, vec!["kafka1:9092", "kafka2:9092"]);
    assert_eq!(config.group, "test-consumer");
    assert_eq!(config.client_id, "test-client");
    assert_eq!(config.security_protocol, "sasl_ssl");
    assert_eq!(config.sasl_mechanism, Some("SCRAM-SHA-256".to_string()));
    assert_eq!(config.sasl_username, Some("testuser".to_string()));
    assert_eq!(
        config.sasl_password.as_ref().map(|p| p.expose()),
        Some("testpass")
    );
    assert!(config.ssl_skip_verify);
    assert_eq!(config.topics, vec!["topic1", "topic2", "topic3"]);
}

#[test]
fn test_kafka_librdkafka_overrides_win() {
    // User overrides should win over profile defaults
    let config = KafkaConfig::production().with_override("queued.min.messages", "50000"); // Override the profile's 100000

    let built = config.build_librdkafka_config();

    // User override should win
    assert_eq!(built.get("queued.min.messages"), Some(&"50000".to_string()));
}

#[test]
fn test_kafka_config_from_env_with_profile() {
    let _guard = EnvGuard::new(&[
        ("TESTAPP2_PROFILE", "devtest"),
        ("TESTAPP2_BOOTSTRAP_SERVERS", "kafka:9092"),
    ]);

    let config = KafkaConfig::from_env("TESTAPP2");

    assert_eq!(config.profile, KafkaProfile::DevTest);
    assert!(config.ssl_skip_verify); // Auto-enabled for devtest
    assert_eq!(config.brokers, vec!["kafka:9092"]);
}

// --- Token Tests ---

#[test]
fn test_kafka_token_display() {
    let token = KafkaToken::new(Arc::from("events"), 0, 12345);
    assert_eq!(token.to_string(), "kafka:events:0:12345");
}

#[test]
fn test_kafka_token_equality() {
    let token1 = KafkaToken::new(Arc::from("events"), 0, 100);
    let token2 = KafkaToken::new(Arc::from("events"), 0, 100);
    let token3 = KafkaToken::new(Arc::from("events"), 1, 100);

    assert_eq!(token1, token2);
    assert_ne!(token1, token3);
}

#[test]
fn test_kafka_token_hash() {
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(KafkaToken::new(Arc::from("events"), 0, 100));
    set.insert(KafkaToken::new(Arc::from("events"), 0, 100)); // Duplicate
    set.insert(KafkaToken::new(Arc::from("events"), 1, 100));

    assert_eq!(set.len(), 2);
}

// --- Metrics Tests ---

#[test]
fn test_kafka_metrics_default() {
    let metrics = KafkaMetrics::default();

    assert_eq!(metrics.messages_sent, 0);
    assert_eq!(metrics.messages_received, 0);
    assert_eq!(metrics.bytes_sent, 0);
    assert_eq!(metrics.bytes_received, 0);
    assert!(metrics.brokers.is_empty());
    assert!(metrics.partition_lag.is_empty());
}

#[test]
fn test_total_consumer_lag() {
    let mut metrics = KafkaMetrics::default();
    metrics.partition_lag.insert(("events".to_string(), 0), 100);
    metrics.partition_lag.insert(("events".to_string(), 1), 200);
    metrics.partition_lag.insert(("events".to_string(), 2), 50);

    assert_eq!(total_consumer_lag(&metrics), 350);
}

#[test]
fn test_healthy_broker_count() {
    let mut metrics = KafkaMetrics::default();
    metrics.brokers.insert(
        "broker1".to_string(),
        BrokerMetrics {
            state: "UP".to_string(),
            ..Default::default()
        },
    );
    metrics.brokers.insert(
        "broker2".to_string(),
        BrokerMetrics {
            state: "DOWN".to_string(),
            ..Default::default()
        },
    );
    metrics.brokers.insert(
        "broker3".to_string(),
        BrokerMetrics {
            state: "UP".to_string(),
            ..Default::default()
        },
    );

    assert_eq!(healthy_broker_count(&metrics), 2);
}

#[test]
fn test_stats_context_creation() {
    let ctx = StatsContext::new();
    let metrics = ctx.get_metrics();

    assert_eq!(metrics.messages_sent, 0);
    assert!(ctx.get_raw_stats().is_none());
}

#[test]
fn test_stats_context_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StatsContext>();
}

// --- TopicInfo Tests ---

#[test]
fn test_topic_info_debug() {
    let info = TopicInfo {
        name: "events".to_string(),
        partition_count: 12,
        replication_factor: 3,
    };

    let debug = format!("{info:?}");
    assert!(debug.contains("events"));
    assert!(debug.contains("12"));
    assert!(debug.contains('3'));
}

// --- Integration Tests (require running Kafka) ---

fn get_test_config() -> Option<KafkaConfig> {
    let brokers = std::env::var("TEST_KAFKA_BROKERS").ok()?;

    Some(KafkaConfig {
        brokers: brokers.split(',').map(|s| s.to_string()).collect(),
        group: std::env::var("TEST_KAFKA_GROUP").unwrap_or_else(|_| "scalo-test-group".to_string()),
        topics: vec![
            std::env::var("TEST_KAFKA_TOPIC").unwrap_or_else(|_| "scalo-test".to_string()),
        ],
        ..Default::default()
    })
}

#[tokio::test]
#[ignore = "requires Kafka broker - set TEST_KAFKA_BROKERS to run"]
async fn test_kafka_transport_connection() {
    use scalo::transport::kafka::KafkaTransport;

    let Some(config) = get_test_config() else {
        eprintln!("Skipping: TEST_KAFKA_BROKERS not set");
        return;
    };

    let transport = KafkaTransport::new(&config).await;
    assert!(
        transport.is_ok(),
        "Failed to connect: {:?}",
        transport.err()
    );
}

#[tokio::test]
#[ignore = "requires Kafka broker - set TEST_KAFKA_BROKERS to run"]
async fn test_kafka_admin_list_topics() {
    let Some(config) = get_test_config() else {
        eprintln!("Skipping: TEST_KAFKA_BROKERS not set");
        return;
    };

    let admin = KafkaAdmin::new(&config);
    assert!(admin.is_ok(), "Failed to create admin: {:?}", admin.err());

    let admin = admin.unwrap();
    let topics = admin.list_topics();
    assert!(topics.is_ok(), "Failed to list topics: {:?}", topics.err());

    println!("Available topics: {:?}", topics.unwrap());
}

#[tokio::test]
#[ignore = "requires Kafka broker - set TEST_KAFKA_BROKERS to run"]
async fn test_kafka_admin_describe_topic() {
    let Some(config) = get_test_config() else {
        eprintln!("Skipping: TEST_KAFKA_BROKERS not set");
        return;
    };

    let admin = KafkaAdmin::new(&config).unwrap();
    let topic = config.topics.first().unwrap();

    let info = admin.describe_topic(topic);
    if let Ok(info) = info {
        println!("Topic info: {info:?}");
        assert_eq!(info.name, *topic);
        assert!(info.partition_count > 0);
    } else {
        eprintln!("Topic {topic} not found (expected in integration tests)");
    }
}

#[tokio::test]
#[ignore = "requires Kafka broker - set TEST_KAFKA_BROKERS to run"]
async fn test_kafka_send_receive_batch() {
    use scalo::transport::{TransportReceiver, TransportSender, kafka::KafkaTransport};

    let Some(mut config) = get_test_config() else {
        eprintln!("Skipping: TEST_KAFKA_BROKERS not set");
        return;
    };

    // Use unique group to avoid interference
    config.group = format!("scalo-test-{}", std::process::id());

    let transport = KafkaTransport::new(&config).await.unwrap();
    let topic = config.topics.first().unwrap();

    // Send a batch of messages
    for i in 0..10 {
        let payload = format!(r#"{{"id": {i}, "data": "test"}}"#);
        let result = transport.send(topic, bytes::Bytes::from(payload)).await;
        assert!(result.is_ok(), "Send failed: {result:?}");
    }

    // Receive messages (may not get all if topic is shared)
    let batch = transport.recv(100).await;
    assert!(batch.is_ok(), "Recv failed: {:?}", batch.err());

    let batch = batch.unwrap();
    println!("Received {} records", batch.records.len());

    // Commit if we got records.
    if !batch.records.is_empty() {
        let result = transport.commit(&batch.commit_tokens).await;
        assert!(result.is_ok(), "Commit failed: {:?}", result.err());
    }
}

#[tokio::test]
#[ignore = "requires Kafka broker - set TEST_KAFKA_BROKERS to run"]
async fn test_kafka_consumer_lag() {
    let Some(config) = get_test_config() else {
        eprintln!("Skipping: TEST_KAFKA_BROKERS not set");
        return;
    };

    let admin = KafkaAdmin::new(&config).unwrap();
    let topic = config.topics.first().unwrap();

    let lag = admin.get_consumer_lag(&config.group, topic).await;
    if let Ok(lag) = lag {
        println!("Consumer lag per partition: {lag:?}");
        for (partition, lag) in lag {
            println!("  Partition {partition}: lag {lag}");
        }
    } else {
        eprintln!("Could not get lag (may need messages in topic)");
    }
}

/// A real broker in a container, for the tests below.
///
/// Built with the `testcontainers` feature, which CI's Test job enables, and
/// needs a Docker daemon.
#[cfg(feature = "testcontainers")]
mod broker {
    use std::ops::Range;
    use std::time::{Duration, Instant};

    use rdkafka::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
    use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
    use testcontainers_modules::kafka::apache::{self, Kafka};
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

    /// Kafka to test against, pinned by digest.
    // renovate: datasource=docker depName=apache/kafka-native
    const KAFKA_IMAGE_REF: &str =
        "4.3.1@sha256:2885898ba17065023f1bd605f3a81efcfa986014f062b73b91ef5462485f9060";

    /// Container starts to try: on a busy CI runner a broker started alongside
    /// others can exit before it logs that it is ready.
    const START_ATTEMPTS: u32 = 3;

    pub(super) async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
        let mut attempt = 1;
        let node = loop {
            match Kafka::default().with_tag(KAFKA_IMAGE_REF).start().await {
                Ok(node) => break node,
                Err(e) if attempt < START_ATTEMPTS => {
                    eprintln!("kafka container attempt {attempt} did not start: {e}");
                    attempt += 1;
                }
                Err(e) => panic!("start kafka container: {e}"),
            }
        };
        let port = node
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("kafka host port");
        (node, format!("127.0.0.1:{port}"))
    }

    /// Create a one-partition `topic` and wait for its metadata.
    pub(super) async fn create_topic(bootstrap: &str, topic: &'static str) {
        let admin = KafkaAdmin::new(&KafkaConfig {
            brokers: vec![bootstrap.to_string()],
            group: String::new(),
            ..Default::default()
        })
        .expect("kafka admin");
        admin
            .create_topics(&[(topic, 1, 1)])
            .await
            .expect("create topic");
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            while admin
                .describe_topic(topic)
                .ok()
                .is_none_or(|t| t.partition_count == 0)
            {
                assert!(Instant::now() < deadline, "topic {topic} never appeared");
                std::thread::sleep(Duration::from_millis(100));
            }
        })
        .await
        .expect("metadata wait");
    }

    /// Write `{"seq":N}` to `topic` for each N in `seqs`, and wait for every
    /// delivery.
    pub(super) async fn produce(bootstrap: &str, topic: &str, seqs: Range<usize>) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .create()
            .expect("raw producer");
        for seq in seqs {
            let payload = format!("{{\"seq\":{seq}}}");
            producer
                .send(
                    FutureRecord::<(), str>::to(topic).payload(&payload),
                    Duration::from_secs(10),
                )
                .await
                .unwrap_or_else(|(e, _)| panic!("deliver record {seq}: {e}"));
        }
    }

    pub(super) fn consumer_config(bootstrap: &str, topic: &str, group: &str) -> KafkaConfig {
        KafkaConfig {
            brokers: vec![bootstrap.to_string()],
            group: group.to_string(),
            topics: vec![topic.to_string()],
            ..Default::default()
        }
    }

    pub(super) async fn consumer(bootstrap: &str, topic: &str, group: &str) -> KafkaTransport {
        KafkaTransport::new(&consumer_config(bootstrap, topic, group))
            .await
            .expect("kafka consumer")
    }

    /// The offset `group` last committed on partition 0 of `topic`, asked of
    /// the broker by a client that joins no group.
    pub(super) async fn committed(bootstrap: &str, group: &str, topic: &str) -> Option<i64> {
        let (bootstrap, group, topic) =
            (bootstrap.to_string(), group.to_string(), topic.to_string());
        tokio::task::spawn_blocking(move || {
            let client: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("group.id", &group)
                .set("enable.auto.commit", "false")
                .create()
                .expect("offset client");
            let mut partitions = TopicPartitionList::new();
            partitions.add_partition(&topic, 0);
            let found = client
                .committed_offsets(partitions, Duration::from_secs(10))
                .expect("committed offsets");
            match found.find_partition(&topic, 0)?.offset() {
                Offset::Offset(offset) => Some(offset),
                _ => None,
            }
        })
        .await
        .expect("offset task")
    }
}

/// Held source acknowledgements against a real broker in a container.
#[cfg(all(feature = "testcontainers", feature = "worker-batch"))]
mod held_acknowledgements {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use scalo::transport::kafka::{KafkaToken, KafkaTransport};
    use scalo::transport::{AcknowledgementsConfig, WorkBatch};
    use scalo::worker::{BatchEngine, BatchProcessingConfig, EngineError};
    use tokio_util::sync::CancellationToken;

    use super::broker::{committed, consumer, create_topic, produce, start_kafka};

    /// Records written to each test topic.
    const RECORDS: usize = 20;

    /// Create a one-partition `topic` and write `{"seq":N}` for N in
    /// `0..RECORDS`.
    async fn topic_with_records(bootstrap: &str, topic: &'static str) {
        create_topic(bootstrap, topic).await;
        produce(bootstrap, topic, 0..RECORDS).await;
    }

    fn seq_of(payload: &[u8]) -> usize {
        let digits = payload
            .strip_prefix(b"{\"seq\":")
            .and_then(|rest| rest.strip_suffix(b"}"))
            .expect("a record written by topic_with_records");
        std::str::from_utf8(digits)
            .expect("ASCII digits")
            .parse()
            .expect("seq fits usize")
    }

    /// Run a pipeline over `transport` whose sink takes its first block and
    /// never returns, then kill it once the sink holds the block. Returns the
    /// records in that block.
    async fn kill_mid_sink(transport: KafkaTransport) -> usize {
        let (took, mut taken) = tokio::sync::mpsc::unbounded_channel::<usize>();
        let run = tokio::spawn(async move {
            let engine = BatchEngine::new(BatchProcessingConfig::default());
            let result: Result<(), EngineError> = engine
                .pipeline(&transport)
                .run(
                    |batch| Ok(batch),
                    move |out: &WorkBatch<KafkaToken>| {
                        let _ = took.send(out.records.len());
                        std::future::pending::<Result<(), EngineError>>()
                    },
                )
                .await;
            result
        });
        let records = tokio::time::timeout(Duration::from_secs(60), taken.recv())
            .await
            .expect("the consumer read a block within 60 s")
            .expect("the sink reported its block");
        run.abort();
        let _ = run.await;
        records
    }

    #[tokio::test]
    async fn offsets_stay_uncommitted_until_the_sink_delivers() {
        let (_node, bootstrap) = start_kafka().await;
        let topic = "held-acks";
        let group = "held-acks-group";
        topic_with_records(&bootstrap, topic).await;

        let taken = kill_mid_sink(consumer(&bootstrap, topic, group).await).await;
        assert!(taken > 0, "the killed consumer had records in its sink");
        assert_eq!(
            committed(&bootstrap, group, topic).await,
            None,
            "a block the sink never delivered commits nothing"
        );

        // The restarted consumer reads every record again, then commits them.
        let restarted = consumer(&bootstrap, topic, group).await;
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let seen = std::sync::Arc::new(parking_lot::Mutex::new(BTreeSet::new()));
        let sink_seen = std::sync::Arc::clone(&seen);
        let engine = BatchEngine::new(BatchProcessingConfig::default());
        let run = engine.pipeline(&restarted).shutdown(shutdown.clone()).run(
            |batch| Ok(batch),
            move |out: &WorkBatch<KafkaToken>| {
                let mut seen = sink_seen.lock();
                seen.extend(out.records.iter().map(|r| seq_of(&r.payload)));
                if seen.len() == RECORDS {
                    stop.cancel();
                }
                std::future::ready(Ok(()))
            },
        );
        tokio::time::timeout(Duration::from_secs(120), run)
            .await
            .expect("the restarted consumer read every record within 120 s")
            .expect("clean run");

        assert_eq!(
            *seen.lock(),
            (0..RECORDS).collect::<BTreeSet<_>>(),
            "every record is read again after the kill"
        );
        assert_eq!(
            committed(&bootstrap, group, topic).await,
            Some(i64::try_from(RECORDS).expect("fits")),
            "and committed once delivered"
        );
    }

    #[tokio::test]
    async fn acknowledgements_disabled_commits_at_receipt() {
        let (_node, bootstrap) = start_kafka().await;
        let topic = "acks-off";
        let group = "acks-off-group";
        topic_with_records(&bootstrap, topic).await;

        let transport = consumer(&bootstrap, topic, group)
            .await
            .with_acknowledgements(AcknowledgementsConfig::new(false));
        let taken = kill_mid_sink(transport).await;

        assert_eq!(
            committed(&bootstrap, group, topic).await,
            Some(i64::try_from(taken).expect("fits")),
            "with acknowledgements off the block is committed before the sink runs, \
             so the kill loses it"
        );
    }
}

/// Consumer lag against a real broker. The committed offset, the offset read
/// to and the log end are three offsets, and the end must keep moving while
/// the self-regulation gate holds the partition paused.
#[cfg(all(feature = "testcontainers", feature = "governor"))]
mod lag {
    use std::time::{Duration, Instant};

    use scalo::transport::TransportReceiver;
    use scalo::transport::kafka::{KafkaToken, KafkaTransport, total_consumer_lag};

    use super::broker::{consumer_config, create_topic, produce, start_kafka};

    /// Records written before the consumer starts, all of which it reads.
    const READ: usize = 20;
    /// Of those, the records it commits.
    const COMMITTED: usize = 5;
    /// Records written while the partition is paused.
    const WRITTEN_PAUSED: usize = 30;

    /// A consumer whose statistics, and so its lag, refresh every 200 ms.
    async fn lag_consumer(bootstrap: &str, topic: &str, group: &str) -> KafkaTransport {
        let mut config = consumer_config(bootstrap, topic, group);
        config
            .librdkafka_overrides
            .insert("statistics.interval.ms".to_string(), "200".to_string());
        KafkaTransport::new(&config).await.expect("kafka consumer")
    }

    /// Lag behind the committed offset, and behind the read position.
    fn lags(transport: &KafkaTransport) -> (i64, i64) {
        (
            total_consumer_lag(&transport.stats()),
            transport.total_position_lag(),
        )
    }

    /// Read until `tokens` holds `total` records.
    async fn read_to(transport: &KafkaTransport, tokens: &mut Vec<KafkaToken>, total: usize) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while tokens.len() < total {
            assert!(
                Instant::now() < deadline,
                "read {} of {total} records within 60 s",
                tokens.len()
            );
            let batch = transport.recv(total - tokens.len()).await.expect("recv");
            tokens.extend(batch.commit_tokens);
        }
    }

    /// Poll as the run loop does, which serves the statistics, until the
    /// lags read `want` or 30 s pass. Returns the last reading. Nothing more
    /// may be read meanwhile.
    async fn settle(transport: &KafkaTransport, want: (i64, i64)) -> (i64, i64) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let batch = transport.recv(100).await.expect("recv");
            assert!(
                batch.records.is_empty(),
                "read {} records the test did not expect",
                batch.records.len()
            );
            let now = lags(transport);
            if now == want || Instant::now() >= deadline {
                return now;
            }
        }
    }

    fn offset(count: usize) -> i64 {
        i64::try_from(count).expect("fits an i64")
    }

    #[tokio::test]
    async fn lag_counts_from_the_commit_and_the_read_position_and_rises_while_paused() {
        let (_node, bootstrap) = start_kafka().await;
        let topic = "lag";
        create_topic(&bootstrap, topic).await;
        produce(&bootstrap, topic, 0..READ).await;
        let transport = lag_consumer(&bootstrap, topic, "lag-group").await;

        let mut tokens = Vec::new();
        read_to(&transport, &mut tokens, READ).await;
        transport
            .commit(&tokens[..COMMITTED])
            .await
            .expect("commit");
        let (committed, read) = (offset(COMMITTED), offset(READ));
        let want = (read - committed, 0);
        assert_eq!(
            settle(&transport, want).await,
            want,
            "end {read}, read to {read}, committed {committed}"
        );

        transport.gate_actuator().pause();
        produce(&bootstrap, topic, READ..READ + WRITTEN_PAUSED).await;
        let end = read + offset(WRITTEN_PAUSED);
        let want = (end - committed, end - read);
        assert_eq!(
            settle(&transport, want).await,
            want,
            "paused: end {end}, read to {read}, committed {committed}. The end must keep \
             moving while nothing is fetched"
        );

        transport.gate_actuator().resume();
        read_to(&transport, &mut tokens, READ + WRITTEN_PAUSED).await;
        let want = (end - committed, 0);
        assert_eq!(
            settle(&transport, want).await,
            want,
            "resumed and read to the end: end {end}, read to {end}, committed {committed}"
        );
    }
}
