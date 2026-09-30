// Project:   scalo
// File:      src/transport/kafka/admin.rs
// Purpose:   Kafka administrative operations
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka administrative operations.
//!
//! `KafkaAdmin` provides programmatic access to managing consumer group offsets,
//! topic configuration, and partition management. Matches the Python
//! `scalo.kafka.KafkaAdmin` API.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::kafka::{KafkaAdmin, KafkaConfig};
//!
//! let config = KafkaConfig::default();
//! let admin = KafkaAdmin::new(&config)?;
//!
//! // Reset consumer group to earliest
//! admin.reset_offsets_to_earliest("my-group", "events", None).await?;
//!
//! // Get consumer lag
//! let lag = admin.get_consumer_lag("my-group", "events").await?;
//! for (partition, lag) in lag {
//!     println!("Partition {}: lag {}", partition, lag);
//! }
//! ```

use super::config::{KafkaConfig, MESSAGE_MAX_BYTES, raw_layer};
use crate::transport::error::{TransportError, TransportResult};
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewPartitions, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::MetadataTopic;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;
use std::collections::HashMap;
use std::time::Duration;

/// Information about a topic.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub partition_count: i32,
    /// Replication factor (from first partition's ISR).
    pub replication_factor: i32,
}

/// Build the create-topic requests, each carrying the record-size ceiling.
///
/// A topic created with no `max.message.bytes` of its own inherits the broker's
/// `message.max.bytes`, which is 1 MiB on a stock broker -- so the 16 MiB record
/// the producer is configured to send is refused by the very topic scalo just
/// created, and the refusal arrives as a dead-lettered record rather than a
/// configuration error. `max_message_bytes` is a string because librdkafka's
/// topic config is string-valued, and it has to outlive the returned requests.
fn create_topic_requests<'a>(
    topics: &'a [(&'a str, i32, i32)],
    max_message_bytes: &'a str,
) -> Vec<NewTopic<'a>> {
    topics
        .iter()
        .map(|(name, partitions, replication)| {
            NewTopic::new(name, *partitions, TopicReplication::Fixed(*replication))
                .set("max.message.bytes", max_message_bytes)
        })
        .collect()
}

/// Build the admin client's config: connection and security, then the profile
/// defaults and `librdkafka_overrides`, whose keys replace the other librdkafka
/// name for their property.
fn admin_client_config(config: &KafkaConfig) -> ClientConfig {
    let mut client_config = ClientConfig::new();

    client_config.set("bootstrap.servers", config.brokers.join(","));
    client_config.set("client.id", &config.client_id);

    // Security.
    client_config.set("security.protocol", &config.security_protocol);
    if let Some(ref mechanism) = config.sasl_mechanism {
        client_config.set("sasl.mechanism", mechanism);
    }
    if let Some(ref username) = config.sasl_username {
        client_config.set("sasl.username", username);
    }
    if let Some(ref password) = config.sasl_password {
        client_config.set("sasl.password", password.expose());
    }

    // TLS.
    if let Some(ref ca) = config.ssl_ca_location {
        client_config.set("ssl.ca.location", ca);
    }
    if let Some(ref cert) = config.ssl_certificate_location {
        client_config.set("ssl.certificate.location", cert);
    }
    if let Some(ref key) = config.ssl_key_location {
        client_config.set("ssl.key.location", key);
    }
    if config.ssl_skip_verify {
        client_config.set("enable.ssl.certificate.verification", "false");
    }

    // Profile defaults and user overrides.
    let rdkafka_config = config.build_librdkafka_config();
    super::apply_layer(&mut client_config, &raw_layer(&rdkafka_config));
    client_config
}

/// Role naming the admin's offset-query consumer in its derived group id.
const OFFSET_QUERY_GROUP_ROLE: &str = "admin";

/// Build the config for the admin's offset-query consumer.
///
/// librdkafka will not build a consumer without a `group.id` and queries that
/// group's coordinator as soon as it connects, so the id is derived from
/// `config` (see [`KafkaConfig::internal_group_id`]) rather than a literal the
/// broker's group ACLs would refuse. The consumer never joins or commits.
fn offset_query_consumer_config(
    client_config: &ClientConfig,
    config: &KafkaConfig,
) -> ClientConfig {
    let mut consumer_config = client_config.clone();
    consumer_config.set(
        "group.id",
        config.internal_group_id(OFFSET_QUERY_GROUP_ROLE),
    );
    consumer_config
}

/// Kafka administrative client.
///
/// Provides operations for managing consumer group offsets, topic configuration,
/// and partition scaling. Designed to match the Python `KafkaAdmin` API.
pub struct KafkaAdmin {
    admin: AdminClient<DefaultClientContext>,
    consumer: BaseConsumer,
    config: ClientConfig,
}

impl KafkaAdmin {
    /// Create a new Kafka admin client.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when the provider preset or
    /// [`KafkaConfig::validate`] refuses the config, as
    /// [`KafkaTransport::new`](super::KafkaTransport::new) does;
    /// `TransportError::Connection` when librdkafka cannot build a client.
    pub fn new(config: &KafkaConfig) -> TransportResult<Self> {
        let config = &super::checked_config(config)?;
        let client_config = admin_client_config(config);

        let admin: AdminClient<DefaultClientContext> = client_config.create().map_err(|e| {
            TransportError::Connection(format!("Failed to create admin client: {e}"))
        })?;

        let consumer: BaseConsumer = offset_query_consumer_config(&client_config, config)
            .create()
            .map_err(|e| TransportError::Connection(format!("Failed to create consumer: {e}")))?;

        Ok(Self {
            admin,
            consumer,
            config: client_config,
        })
    }

    // --- Consumer Group Offset Management ---

    /// Reset consumer group offsets to earliest (reprocess all messages).
    ///
    /// The consumer group must be stopped (no active consumers) before resetting.
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group ID
    /// * `topic` - Topic name
    /// * `partitions` - Specific partitions to reset, or None for all
    ///
    /// # Errors
    ///
    /// Returns error if offset reset fails.
    pub async fn reset_offsets_to_earliest(
        &self,
        group_id: &str,
        topic: &str,
        partitions: Option<&[i32]>,
    ) -> TransportResult<()> {
        let partition_list: TopicPartitionList = self.get_partition_list(topic, partitions).await?;

        let mut tpl = TopicPartitionList::new();
        for elem in partition_list.elements() {
            tpl.add_partition_offset(elem.topic(), elem.partition(), Offset::Beginning)
                .map_err(|e| TransportError::Admin(format!("Failed to build TPL: {e}")))?;
        }

        self.commit_offsets_for_group(group_id, &tpl).await
    }

    /// Reset consumer group offsets to latest (skip to end).
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group ID
    /// * `topic` - Topic name
    /// * `partitions` - Specific partitions to reset, or None for all
    ///
    /// # Errors
    ///
    /// Returns error if offset reset fails.
    pub async fn reset_offsets_to_latest(
        &self,
        group_id: &str,
        topic: &str,
        partitions: Option<&[i32]>,
    ) -> TransportResult<()> {
        let partition_list: TopicPartitionList = self.get_partition_list(topic, partitions).await?;

        // Each partition resets to its high watermark.
        let mut tpl = TopicPartitionList::new();
        for elem in partition_list.elements() {
            let (_, high) = self
                .consumer
                .fetch_watermarks(topic, elem.partition(), Duration::from_secs(10))
                .map_err(|e| {
                    TransportError::Admin(format!(
                        "Failed to fetch watermarks for partition {}: {e}",
                        elem.partition()
                    ))
                })?;

            tpl.add_partition_offset(elem.topic(), elem.partition(), Offset::Offset(high))
                .map_err(|e| TransportError::Admin(format!("Failed to build TPL: {e}")))?;
        }

        self.commit_offsets_for_group(group_id, &tpl).await
    }

    /// Reset consumer group offsets to a specific timestamp.
    ///
    /// Offsets are set to the first message at or after the specified timestamp.
    ///
    /// # Arguments
    ///
    /// * `group_id` - Consumer group ID
    /// * `topic` - Topic name
    /// * `timestamp_ms` - Unix timestamp in milliseconds
    /// * `partitions` - Specific partitions to reset, or None for all
    ///
    /// # Errors
    ///
    /// Returns error if offset reset fails.
    pub async fn reset_offsets_to_timestamp(
        &self,
        group_id: &str,
        topic: &str,
        timestamp_ms: i64,
        partitions: Option<&[i32]>,
    ) -> TransportResult<()> {
        let partition_list: TopicPartitionList = self.get_partition_list(topic, partitions).await?;

        // Seed the TPL with the target timestamp per partition.
        let mut tpl = TopicPartitionList::new();
        for elem in partition_list.elements() {
            tpl.add_partition_offset(elem.topic(), elem.partition(), Offset::Offset(timestamp_ms))
                .map_err(|e| TransportError::Admin(format!("Failed to build TPL: {e}")))?;
        }

        // Resolve timestamps to concrete offsets.
        let offsets = self
            .consumer
            .offsets_for_times(tpl, Duration::from_secs(30))
            .map_err(|e| TransportError::Admin(format!("Failed to get offsets for times: {e}")))?;

        self.commit_offsets_for_group(group_id, &offsets).await
    }

    /// Get consumer lag per partition.
    ///
    /// Lag is the difference between the high watermark and the committed offset.
    ///
    /// # Returns
    ///
    /// Map of partition ID to lag (messages behind).
    ///
    /// # Errors
    ///
    /// Returns error if lag calculation fails.
    pub async fn get_consumer_lag(
        &self,
        group_id: &str,
        topic: &str,
    ) -> TransportResult<HashMap<i32, i64>> {
        // Bind a consumer to the queried group so committed offsets resolve.
        let mut group_config = self.config.clone();
        group_config.set("group.id", group_id);
        let group_consumer: BaseConsumer = group_config
            .create()
            .map_err(|e| TransportError::Connection(format!("Failed to create consumer: {e}")))?;

        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| TransportError::Admin(format!("Failed to fetch metadata: {e}")))?;

        let topic_meta: &MetadataTopic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| TransportError::Admin(format!("Topic {topic} not found")))?;

        let mut tpl = TopicPartitionList::new();
        for partition in topic_meta.partitions() {
            tpl.add_partition(topic, partition.id());
        }

        let committed = group_consumer
            .committed_offsets(tpl, Duration::from_secs(10))
            .map_err(|e| TransportError::Admin(format!("Failed to get committed offsets: {e}")))?;

        // lag = high watermark - committed offset, per partition.
        let mut lag_map = HashMap::new();
        for elem in committed.elements() {
            let (_, high) = self
                .consumer
                .fetch_watermarks(topic, elem.partition(), Duration::from_secs(10))
                .map_err(|e| {
                    TransportError::Admin(format!(
                        "Failed to fetch watermarks for partition {}: {e}",
                        elem.partition()
                    ))
                })?;

            let committed_offset = if let Offset::Offset(o) = elem.offset() {
                o
            } else {
                0
            };

            let lag = high - committed_offset;
            lag_map.insert(elem.partition(), lag.max(0));
        }

        Ok(lag_map)
    }

    // --- Topic Management ---

    /// Create one or more topics, each accepting a record up to
    /// [`MESSAGE_MAX_BYTES`].
    ///
    /// Ignores "topic already exists" errors -- safe to call repeatedly, though
    /// an existing topic keeps whatever ceiling it was created with.
    ///
    /// # Arguments
    ///
    /// * `topics` - Slice of `(name, num_partitions, replication_factor)` tuples
    ///
    /// # Errors
    ///
    /// Returns error if topic creation fails for reasons other than already existing.
    pub async fn create_topics(&self, topics: &[(&str, i32, i32)]) -> TransportResult<()> {
        let max_message_bytes = MESSAGE_MAX_BYTES.to_string();
        let new_topics = create_topic_requests(topics, &max_message_bytes);

        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

        let results = self
            .admin
            .create_topics(&new_topics, &opts)
            .await
            .map_err(|e| TransportError::Admin(format!("Failed to create topics: {e}")))?;

        for result in results {
            if let Err((topic_name, err_code)) = result {
                if err_code == RDKafkaErrorCode::TopicAlreadyExists {
                    continue;
                }
                return Err(TransportError::Admin(format!(
                    "Failed to create topic {topic_name}: {err_code:?}"
                )));
            }
        }

        Ok(())
    }

    /// Delete one or more topics.
    ///
    /// # Errors
    ///
    /// Returns error if topic deletion fails.
    pub async fn delete_topics(&self, topics: &[&str]) -> TransportResult<()> {
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

        let results = self
            .admin
            .delete_topics(topics, &opts)
            .await
            .map_err(|e| TransportError::Admin(format!("Failed to delete topics: {e}")))?;

        for result in results {
            if let Err((topic_name, err_code)) = result {
                return Err(TransportError::Admin(format!(
                    "Failed to delete topic {topic_name}: {err_code:?}"
                )));
            }
        }

        Ok(())
    }

    /// Increase the partition count for a topic.
    ///
    /// Partition count can only be increased, not decreased.
    ///
    /// # Errors
    ///
    /// Returns error if partition increase fails.
    #[allow(clippy::cast_sign_loss)] // new_total is validated to be positive
    pub async fn increase_partitions(&self, topic: &str, new_total: i32) -> TransportResult<()> {
        let new_partitions = NewPartitions::new(topic, new_total.max(0) as usize);
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(30)));

        let results = self
            .admin
            .create_partitions(&[new_partitions], &opts)
            .await
            .map_err(|e| TransportError::Admin(format!("Failed to create partitions: {e}")))?;

        for result in results {
            if let Err((topic_name, err_code)) = result {
                return Err(TransportError::Admin(format!(
                    "Failed to increase partitions for {topic_name}: {err_code:?}"
                )));
            }
        }

        Ok(())
    }

    /// Set the retention period for a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `retention_ms` - Retention period in milliseconds
    ///
    /// # Errors
    ///
    /// Returns error if configuration change fails.
    pub async fn set_retention(&self, topic: &str, retention_ms: i64) -> TransportResult<()> {
        let retention_str = retention_ms.to_string();
        let alter_config =
            AlterConfig::new(ResourceSpecifier::Topic(topic)).set("retention.ms", &retention_str);
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(30)));

        let results = self
            .admin
            .alter_configs(&[alter_config], &opts)
            .await
            .map_err(|e| TransportError::Admin(format!("Failed to alter config: {e}")))?;

        for result in results {
            if let Err((_, e)) = result {
                return Err(TransportError::Admin(format!(
                    "Failed to set retention: {e}"
                )));
            }
        }

        Ok(())
    }

    /// Get topic configuration.
    ///
    /// # Returns
    ///
    /// Map of configuration key to value.
    ///
    /// # Errors
    ///
    /// Returns error if configuration fetch fails.
    pub async fn get_topic_config(&self, topic: &str) -> TransportResult<HashMap<String, String>> {
        let resource = ResourceSpecifier::Topic(topic);
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(30)));

        let results = self
            .admin
            .describe_configs(&[resource], &opts)
            .await
            .map_err(|e| TransportError::Admin(format!("Failed to describe configs: {e}")))?;

        let mut config_map = HashMap::new();
        for result in results {
            match result {
                Ok(config_resource) => {
                    for entry in config_resource.entries {
                        if let Some(value) = entry.value {
                            config_map.insert(entry.name, value);
                        }
                    }
                }
                Err(e) => {
                    return Err(TransportError::Admin(format!(
                        "Failed to get topic config: {e}"
                    )));
                }
            }
        }

        Ok(config_map)
    }

    /// List all topics.
    ///
    /// # Returns
    ///
    /// List of topic names.
    ///
    /// # Errors
    ///
    /// Returns error if metadata fetch fails.
    pub fn list_topics(&self) -> TransportResult<Vec<String>> {
        let metadata = self
            .consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .map_err(|e| TransportError::Admin(format!("Failed to fetch metadata: {e}")))?;

        Ok(metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect())
    }

    /// Get topic metadata including partition count and replication factor.
    ///
    /// # Errors
    ///
    /// Returns error if metadata fetch fails.
    pub fn describe_topic(&self, topic: &str) -> TransportResult<TopicInfo> {
        let metadata = self
            .consumer
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .map_err(|e| TransportError::Admin(format!("Failed to fetch metadata: {e}")))?;

        let topic_meta: &MetadataTopic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| TransportError::Admin(format!("Topic {topic} not found")))?;

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let replication_factor = topic_meta
            .partitions()
            .first()
            .map_or(0, |p| p.replicas().len() as i32);

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let partition_count = topic_meta.partitions().len() as i32;

        Ok(TopicInfo {
            name: topic.to_string(),
            partition_count,
            replication_factor,
        })
    }

    // --- Internal helpers ---

    /// Get partition list for a topic.
    async fn get_partition_list(
        &self,
        topic: &str,
        partitions: Option<&[i32]>,
    ) -> TransportResult<TopicPartitionList> {
        let mut tpl = TopicPartitionList::new();

        if let Some(parts) = partitions {
            for &partition in parts {
                tpl.add_partition(topic, partition);
            }
        } else {
            // All partitions when none specified.
            let metadata = self
                .consumer
                .fetch_metadata(Some(topic), Duration::from_secs(10))
                .map_err(|e| TransportError::Admin(format!("Failed to fetch metadata: {e}")))?;

            let topic_meta: &MetadataTopic =
                metadata
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .ok_or_else(|| TransportError::Admin(format!("Topic {topic} not found")))?;

            for partition in topic_meta.partitions() {
                tpl.add_partition(topic, partition.id());
            }
        }

        Ok(tpl)
    }

    /// Commit offsets for a consumer group.
    ///
    /// Note: This requires stopping all consumers in the group first.
    /// The Kafka protocol requires using a consumer from the group to commit offsets,
    /// so we create a temporary consumer with the target group ID.
    async fn commit_offsets_for_group(
        &self,
        group_id: &str,
        offsets: &TopicPartitionList,
    ) -> TransportResult<()> {
        // Commit must come from a consumer bound to the target group.
        let mut group_config = self.config.clone();
        group_config.set("group.id", group_id);
        group_config.set("enable.auto.commit", "false");

        let group_consumer: BaseConsumer = group_config.create().map_err(|e| {
            TransportError::Connection(format!("Failed to create group consumer: {e}"))
        })?;

        group_consumer
            .commit(offsets, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| TransportError::Commit(format!("Failed to commit offsets: {e}")))?;

        Ok(())
    }
}

impl std::fmt::Debug for KafkaAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaAdmin").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::client_gate;
    use super::*;

    #[test]
    fn the_admin_refuses_the_configs_the_transport_refuses() {
        client_gate::assert_refuses_as_the_transport_does(|config| {
            client_gate::config_refusal(KafkaAdmin::new(config))
        });
    }

    #[test]
    fn the_admin_builds_on_a_verified_config_under_production() {
        client_gate::assert_builds_under_production(|config| {
            client_gate::config_refusal(KafkaAdmin::new(config))
        });
    }

    /// The offset-query consumer asks for its group's coordinator on connect,
    /// and a broker refuses any group outside the `dfe-` prefix it grants.
    #[test]
    fn offset_query_consumer_takes_a_group_under_the_app_prefix() {
        let config = KafkaConfig {
            group: "dfe-loader".to_string(),
            ..Default::default()
        };
        let base = ClientConfig::new();
        let built = offset_query_consumer_config(&base, &config);
        assert_eq!(built.get("group.id"), Some("dfe-loader-admin"));

        // The admin client's own config carries no group.
        assert_eq!(base.get("group.id"), None);
    }

    /// A raw `group.id` override is aimed at the app's own consumer; the
    /// admin's offset-query consumer keeps its derived id.
    #[test]
    fn offset_query_group_id_wins_over_a_raw_override() {
        let config = KafkaConfig {
            group: "dfe-archiver".to_string(),
            ..Default::default()
        };
        let mut base = ClientConfig::new();
        base.set("group.id", "operator-override");
        let built = offset_query_consumer_config(&base, &config);
        assert_eq!(built.get("group.id"), Some("dfe-archiver-admin"));
    }

    /// The admin lays `librdkafka_overrides` over its connection settings, so
    /// an override by the other librdkafka name replaces the configured one,
    /// and the offset-query consumer inherits the result.
    #[test]
    fn admin_overrides_replace_the_other_librdkafka_name() {
        let mut config = KafkaConfig {
            security_protocol: "sasl_ssl".to_string(),
            sasl_mechanism: Some("PLAIN".to_string()),
            ..Default::default()
        };
        config
            .librdkafka_overrides
            .insert("sasl.mechanisms".to_string(), "SCRAM-SHA-256".to_string());
        config
            .librdkafka_overrides
            .insert("fetch.message.max.bytes".to_string(), "2097152".to_string());

        let admin = admin_client_config(&config);
        assert_eq!(admin.get("sasl.mechanism"), None);
        assert_eq!(
            super::super::librdkafka_resolves(&admin, "sasl.mechanisms"),
            "SCRAM-SHA-256"
        );

        let consumer = offset_query_consumer_config(&admin, &config);
        assert_eq!(
            super::super::librdkafka_resolves(&consumer, "fetch.message.max.bytes"),
            "2097152"
        );
    }

    #[test]
    fn test_topic_info_debug() {
        let info = TopicInfo {
            name: "test".to_string(),
            partition_count: 3,
            replication_factor: 2,
        };
        assert!(format!("{info:?}").contains("test"));
    }

    /// Without an explicit `max.message.bytes` the new topic inherits the
    /// broker's 1 MiB default, and the 16 MiB record the producer is
    /// configured to send is refused by a topic scalo created itself.
    #[test]
    fn created_topics_carry_the_chain_wide_record_ceiling() {
        let ceiling = MESSAGE_MAX_BYTES.to_string();
        let requests = create_topic_requests(&[("events", 3, 2), ("events-dlq", 1, 1)], &ceiling);

        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(
                request.config,
                vec![("max.message.bytes", ceiling.as_str())],
                "topic {} must accept the same record the producer sends",
                request.name
            );
        }

        // The rest of the request is unchanged.
        assert_eq!(requests[0].name, "events");
        assert_eq!(requests[0].num_partitions, 3);
        assert!(matches!(
            requests[0].replication,
            TopicReplication::Fixed(2)
        ));
        assert!(matches!(
            requests[1].replication,
            TopicReplication::Fixed(1)
        ));
    }
}
