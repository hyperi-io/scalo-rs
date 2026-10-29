// Project:   scalo
// File:      src/transport/kafka/types.rs
// Purpose:   Kafka config, provider and token types, which link no librdkafka
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The Kafka types that need no client: [`KafkaConfig`] and its security
//! floor, the provider presets, the contract types and the commit token.
//!
//! `transport-kafka-config` compiles only this, so an app that reads or checks
//! Kafka config without talking to a broker never builds or links librdkafka.
//! `transport-kafka` adds the clients on top.

// The client-building helpers in these modules are called only by the
// clients `transport-kafka` adds.
#[cfg_attr(
    not(feature = "transport-kafka"),
    allow(dead_code, reason = "the client helpers are used by transport-kafka")
)]
#[path = "config.rs"]
pub(super) mod config;
#[path = "contract.rs"]
pub mod contract;
#[cfg_attr(
    not(feature = "transport-kafka"),
    allow(dead_code, reason = "a lease is minted by the consumer")
)]
#[path = "lease.rs"]
pub(super) mod lease;
#[path = "providers.rs"]
pub mod providers;
#[path = "token.rs"]
pub(super) mod token;

#[allow(deprecated)]
pub use config::{
    CLASSIC_ONLY_CONSUMER_KEYS, ConsumerKnobs, ConsumerProtocol, DEVTEST_PROFILE,
    HIGH_THROUGHPUT_CONSUMER_DEFAULTS, KafkaConfig, KafkaProfile, KafkaSizingConfig,
    LOW_LATENCY_CONSUMER_DEFAULTS, MESSAGE_MAX_BYTES, PARTITION_FETCH_BYTES, PRODUCER_DEFAULTS,
    PRODUCER_DEVTEST, PRODUCER_EXACTLY_ONCE, PRODUCER_HIGH_THROUGHPUT, PRODUCER_LOW_LATENCY,
    PRODUCTION_PROFILE, ProducerKnobs, SelfRegulationProfile, SuppressionRule,
    merge_with_overrides,
};
pub use lease::PartitionLease;
pub use providers::{
    AuthKind, KafkaProvider, KnownProvider, MetadataMode, ProviderCapabilities, SchemaRegistry,
};
pub use token::KafkaToken;
