// Project:   scalo
// File:      src/transport/kafka/providers.rs
// Purpose:   Generic, open Kafka provider abstraction - normalise per-provider
//            auth + capabilities (transport + cluster lifecycle are trait seams).
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka provider abstraction: one open trait ([`KafkaProvider`]) behind which every
//! provider-specific quirk is normalised. Each managed/self-hosted Kafka has its own
//! weirdness across MANY axes -- this trait is the single seam that combines them.
//!
//! # Generic and OPEN
//!
//! [`KafkaProvider`] is a trait, not a closed set. A provider with its own odd
//! requirements is added by IMPLEMENTING the trait -- no change to this module. The
//! built-in [`KnownProvider`] enum implements it for the common providers; a third
//! party writes `impl KafkaProvider for MyProvider { .. }` for anything else.
//!
//! # Concerns
//!
//! - **auth** -- [`KafkaProvider::auth`] picks `(security_protocol, sasl_mechanism)`.
//! - **capabilities** -- [`KafkaProvider::capabilities`] flags the behaviour that
//!   drives cost + lifecycle (managed? always-on billing? billable side resources?
//!   serverless tier? IAM vs user/pass?).
//! - **transport tuning** -- [`KafkaProvider::transport_overrides`] (default: none).
//! - **cluster control (lifecycle) + topic-admin quirks** hang off this same identity
//!   at their own seams (the managed-cluster lifecycle + admin layers) -- added there.
//!
//! # Vanilla core, opinions on top
//!
//! This module is pure, generic FACT: what each provider's platform requires. It holds
//! NO deployment POLICY. An app that wants an opinionated contract -- "SCRAM mandatory
//! on brokers we own, never weakened; only this blessed set is allowed" (the DFE
//! credential contract, dfe-engine#98) -- layers that ON TOP, opt-in, in the consumer.
//! The vanilla core works irrespective of any such opinion.
//!
//! One credential shape holds across the built-in managed providers: SASL over TLS
//! with a username+password pair. Only the `sasl.mechanism` differs, and it is a
//! property of the PROVIDER: SCRAM-SHA-512 where the platform offers SCRAM
//! (self-hosted Kafka/Strimzi, Redpanda self-hosted + Cloud, AWS MSK provisioned);
//! PLAIN (API key) where it does not (Confluent Cloud); IAM/OAUTHBEARER as a separate,
//! quarantined credential shape (AWS MSK Serverless is IAM-only).
//!
//! The one hard floor: PLAIN must ride SASL_SSL. Enforced by [`validate`] and by
//! [`KafkaConfig::validate`](super::KafkaConfig::validate).
//!
//! The built-in table is the canonical record for dfe-engine#98 and is MIRRORED in
//! scalo-py + dfe-engine (Python) -- keep the three tables identical.

use super::KafkaConfig;

/// The credential family a provider authenticates with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKind {
    /// SASL username + password (SCRAM or PLAIN) -- the common contract.
    UserPassword,
    /// AWS IAM (OAUTHBEARER token via the MSK IAM callback) -- quarantined.
    Iam,
    /// No auth (local dev plaintext).
    None,
}

/// Provider-specific behaviour flags -- the "weirdness" of each Kafka provider,
/// combined behind one type. Static data (no I/O); the cluster-control lifecycle that
/// ACTS on these (create/delete/status, teardown-to-empty) reads them from the opt-in
/// lifecycle layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// Managed SaaS/cloud (vs a self-hosted broker you run in-cluster).
    pub managed: bool,
    /// Bills continuously while it exists -- only DELETE stops spend (no pause).
    pub always_on: bool,
    /// Creating a cluster auto-provisions a side resource that ALSO bills and must be
    /// swept on teardown (Confluent Cloud's Flink compute pool).
    pub has_billable_side_resources: bool,
    /// A serverless / elastic (pay-per-use) tier is available for this identity.
    pub serverless: bool,
    /// TLS is mandatory (managed clouds) vs optional (in-cluster dev / mesh TLS).
    pub requires_tls: bool,
    /// The credential family.
    pub auth_kind: AuthKind,
}

/// The cluster metadata plane -- a version-implication FACT that shapes deployment
/// and admin tooling. (Modern kafbat auto-senses topology via the AdminClient and the
/// precise running version is runtime-detected; this is the deployment SHAPE, which
/// still governs which admin features light up and how the cluster is stood up.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataMode {
    /// KRaft (KIP-500) -- no ZooKeeper. The modern default; Redpanda is KRaft-native.
    KRaft,
    /// ZooKeeper-backed (legacy self-hosted).
    Zookeeper,
    /// Managed -- the platform hides the metadata plane (Confluent Cloud, MSK Serverless).
    Managed,
}

/// A schema registry bundled with the provider, if any. The endpoint is a runtime
/// connection detail; this is just which KIND, so a config emitter knows to wire it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaRegistry {
    /// No bundled registry -- bring your own.
    None,
    /// Confluent Schema Registry (Confluent Cloud, or self-hosted).
    Confluent,
    /// Redpanda's built-in schema registry.
    Redpanda,
}

/// The generic, open Kafka provider abstraction.
///
/// Implement this for any provider -- built-in or your own -- to teach scalo its auth
/// shape, capabilities, and (at their seams) transport tuning. Everything
/// provider-specific hangs off this one trait rather than being bolted on beside it.
pub trait KafkaProvider {
    /// A stable identifier (e.g. `"confluent-cloud"`). Used in config + logs.
    fn name(&self) -> &str;

    /// The auth shape: `(security_protocol, sasl_mechanism)`. An empty mechanism means
    /// no SASL (the plaintext dev case). Values are librdkafka strings.
    fn auth(&self) -> (&'static str, &'static str);

    /// Behaviour flags that drive cost + lifecycle handling.
    fn capabilities(&self) -> ProviderCapabilities;

    /// Provider-specific librdkafka overrides (transport tuning). Default: none.
    fn transport_overrides(&self) -> &'static [(&'static str, &'static str)] {
        &[]
    }

    /// The metadata plane (KRaft / ZooKeeper / managed-hidden). Default: KRaft, the
    /// modern default. Self-hosted providers may run either; this is the recommended.
    fn metadata_mode(&self) -> MetadataMode {
        MetadataMode::KRaft
    }

    /// A bundled schema registry, if the provider ships one. Default: none.
    fn schema_registry(&self) -> SchemaRegistry {
        SchemaRegistry::None
    }

    /// Apply this provider's auth onto a vanilla [`KafkaConfig`].
    ///
    /// Sets `security_protocol` (lowercased to scalo's convention) + `sasl_mechanism`;
    /// the caller supplies brokers + credentials. Providers rarely override this.
    fn apply_auth(&self, config: &mut KafkaConfig) {
        let (proto, mech) = self.auth();
        config.security_protocol = proto.to_ascii_lowercase();
        config.sasl_mechanism = (!mech.is_empty()).then(|| mech.to_string());
    }
}

/// The built-in, generic providers. `impl KafkaProvider` -- the canonical facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownProvider {
    /// Self-hosted Apache Kafka / Strimzi -- SCRAM-SHA-512.
    Strimzi,
    /// Self-hosted Redpanda -- SCRAM-SHA-512.
    Redpanda,
    /// AWS MSK provisioned -- SCRAM-SHA-512 (serverless is IAM-only: use `MskIam`).
    Msk,
    /// Redpanda Cloud -- SASL_SSL + SCRAM-SHA-256/512.
    RedpandaCloud,
    /// Confluent Cloud -- no SCRAM, API-key PLAIN over TLS.
    ConfluentCloud,
    /// Local dev broker with no auth.
    Plaintext,
    /// Quarantined: IAM auth (OAUTHBEARER). MANDATORY for MSK Serverless.
    MskIam,
}

impl KnownProvider {
    /// Parse a provider name. The string keys match scalo-py + dfe-engine.
    ///
    /// # Errors
    /// Returns `Err` with the list of known providers for an unknown name.
    pub fn parse(s: &str) -> Result<Self, String> {
        Ok(match s {
            "strimzi" => Self::Strimzi,
            "redpanda" => Self::Redpanda,
            "msk" => Self::Msk,
            "redpanda-cloud" => Self::RedpandaCloud,
            "confluent-cloud" => Self::ConfluentCloud,
            "plaintext" => Self::Plaintext,
            "msk_iam" => Self::MskIam,
            other => {
                return Err(format!(
                    "unknown kafka provider {other:?}; expected one of: strimzi, redpanda, \
                     msk, redpanda-cloud, confluent-cloud, plaintext, msk_iam"
                ));
            }
        })
    }
}

impl KafkaProvider for KnownProvider {
    fn name(&self) -> &str {
        match self {
            Self::Strimzi => "strimzi",
            Self::Redpanda => "redpanda",
            Self::Msk => "msk",
            Self::RedpandaCloud => "redpanda-cloud",
            Self::ConfluentCloud => "confluent-cloud",
            Self::Plaintext => "plaintext",
            Self::MskIam => "msk_iam",
        }
    }

    fn auth(&self) -> (&'static str, &'static str) {
        match self {
            Self::Strimzi | Self::Redpanda | Self::Msk | Self::RedpandaCloud => {
                ("SASL_SSL", "SCRAM-SHA-512")
            }
            Self::ConfluentCloud => ("SASL_SSL", "PLAIN"),
            Self::Plaintext => ("PLAINTEXT", ""),
            Self::MskIam => ("SASL_SSL", "OAUTHBEARER"),
        }
    }

    fn capabilities(&self) -> ProviderCapabilities {
        // Self-hosted: you run the broker -- no managed billing, TLS is the operator's
        // choice (in-cluster mesh often terminates it).
        let self_hosted = ProviderCapabilities {
            managed: false,
            always_on: false,
            has_billable_side_resources: false,
            serverless: false,
            requires_tls: false,
            auth_kind: AuthKind::UserPassword,
        };
        // Managed base: SaaS/cloud, always-on billing, TLS mandatory, elastic tier.
        let managed = ProviderCapabilities {
            managed: true,
            always_on: true,
            has_billable_side_resources: false,
            serverless: true,
            requires_tls: true,
            auth_kind: AuthKind::UserPassword,
        };
        match self {
            Self::Strimzi | Self::Redpanda => self_hosted,
            // Provisioned MSK: fixed broker-hour capacity (MSK Serverless is the
            // separate `MskIam` identity).
            Self::Msk => ProviderCapabilities {
                serverless: false,
                ..managed
            },
            Self::RedpandaCloud => managed,
            // Cluster-create auto-provisions a billable Flink compute pool to sweep.
            Self::ConfluentCloud => ProviderCapabilities {
                has_billable_side_resources: true,
                ..managed
            },
            Self::Plaintext => ProviderCapabilities {
                requires_tls: false,
                auth_kind: AuthKind::None,
                ..self_hosted
            },
            // MSK Serverless / IAM-auth: managed, IAM credential family.
            Self::MskIam => ProviderCapabilities {
                auth_kind: AuthKind::Iam,
                ..managed
            },
        }
    }

    fn metadata_mode(&self) -> MetadataMode {
        match self {
            // Redpanda is KRaft-native (no ZooKeeper); Strimzi + local dev default to
            // KRaft on modern Kafka; provisioned MSK is KRaft-forward.
            Self::Redpanda | Self::Strimzi | Self::Plaintext | Self::Msk => MetadataMode::KRaft,
            // The clouds hide their metadata plane entirely.
            Self::RedpandaCloud | Self::ConfluentCloud | Self::MskIam => MetadataMode::Managed,
        }
    }

    fn schema_registry(&self) -> SchemaRegistry {
        match self {
            Self::ConfluentCloud => SchemaRegistry::Confluent,
            Self::Redpanda | Self::RedpandaCloud => SchemaRegistry::Redpanda,
            Self::Strimzi | Self::Msk | Self::Plaintext | Self::MskIam => SchemaRegistry::None,
        }
    }
}

/// Refuse a configuration that breaks the security floor.
///
/// - PLAIN credentials MUST ride SASL_SSL (never PLAIN over a plaintext transport).
/// - SASL_SSL requires a mechanism.
///
/// # Errors
/// Returns `Err` with a description when either invariant is violated.
pub fn validate(security_protocol: &str, sasl_mechanism: &str) -> Result<(), String> {
    if sasl_mechanism == "PLAIN" && !security_protocol.eq_ignore_ascii_case("SASL_SSL") {
        return Err("PLAIN credentials require security_protocol=SASL_SSL \
             (never send PLAIN over a plaintext transport)"
            .to_string());
    }
    if security_protocol.eq_ignore_ascii_case("SASL_SSL") && sasl_mechanism.is_empty() {
        return Err("security_protocol=SASL_SSL requires a sasl.mechanism".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The cross-language contract (dfe-engine#98). scalo-py + dfe-engine MUST produce
    // this exact table: (provider_key, security_protocol, sasl_mechanism).
    const CANONICAL_TABLE: &[(&str, &str, &str)] = &[
        ("strimzi", "SASL_SSL", "SCRAM-SHA-512"),
        ("redpanda", "SASL_SSL", "SCRAM-SHA-512"),
        ("msk", "SASL_SSL", "SCRAM-SHA-512"),
        ("redpanda-cloud", "SASL_SSL", "SCRAM-SHA-512"),
        ("confluent-cloud", "SASL_SSL", "PLAIN"),
        ("plaintext", "PLAINTEXT", ""),
        ("msk_iam", "SASL_SSL", "OAUTHBEARER"),
    ];

    #[test]
    fn auth_matches_canonical_table() {
        for (key, proto, mech) in CANONICAL_TABLE {
            let provider = KnownProvider::parse(key).expect("known provider");
            assert_eq!(provider.auth(), (*proto, *mech), "provider {key}");
            assert_eq!(provider.name(), *key);
        }
    }

    #[test]
    fn confluent_is_the_only_plain() {
        let plain: Vec<_> = CANONICAL_TABLE
            .iter()
            .filter(|(_, _, mech)| *mech == "PLAIN")
            .map(|(k, _, _)| *k)
            .collect();
        assert_eq!(plain, vec!["confluent-cloud"]);
    }

    #[test]
    fn unknown_provider_is_rejected() {
        assert!(KnownProvider::parse("kinesis").is_err());
    }

    #[test]
    fn validate_refuses_plain_over_plaintext() {
        assert!(validate("PLAINTEXT", "PLAIN").is_err());
        assert!(validate("sasl_plaintext", "PLAIN").is_err());
        assert!(validate("SASL_SSL", "PLAIN").is_ok());
    }

    #[test]
    fn validate_requires_mechanism_for_sasl_ssl() {
        assert!(validate("SASL_SSL", "").is_err());
        assert!(validate("SASL_SSL", "SCRAM-SHA-512").is_ok());
    }

    #[test]
    fn every_derived_pair_passes_validation() {
        for (key, _, _) in CANONICAL_TABLE {
            let (proto, mech) = KnownProvider::parse(key).unwrap().auth();
            validate(proto, mech).expect("derived pair must validate");
        }
    }

    #[test]
    fn apply_auth_sets_lowercase_protocol_and_mechanism() {
        let mut cfg = KafkaConfig::default();
        KnownProvider::ConfluentCloud.apply_auth(&mut cfg);
        assert_eq!(cfg.security_protocol, "sasl_ssl");
        assert_eq!(cfg.sasl_mechanism.as_deref(), Some("PLAIN"));
        assert!(
            cfg.validate(true).is_ok(),
            "applied config passes the floor"
        );

        let mut dev = KafkaConfig::default();
        KnownProvider::Plaintext.apply_auth(&mut dev);
        assert_eq!(dev.security_protocol, "plaintext");
        assert_eq!(dev.sasl_mechanism, None);
    }

    #[test]
    fn managed_providers_are_always_on() {
        for key in ["msk", "confluent-cloud", "redpanda-cloud", "msk_iam"] {
            let caps = KnownProvider::parse(key).unwrap().capabilities();
            assert!(caps.managed, "{key} is managed");
            assert!(caps.always_on, "{key} bills continuously");
            assert!(caps.requires_tls, "{key} mandates TLS");
        }
    }

    #[test]
    fn self_hosted_is_not_managed() {
        for key in ["strimzi", "redpanda", "plaintext"] {
            let caps = KnownProvider::parse(key).unwrap().capabilities();
            assert!(!caps.managed, "{key} is self-hosted");
            assert!(!caps.always_on);
        }
    }

    #[test]
    fn confluent_has_billable_side_resources() {
        // The Flink compute pool auto-provisioned on cluster-create.
        assert!(
            KnownProvider::ConfluentCloud
                .capabilities()
                .has_billable_side_resources
        );
        assert!(
            !KnownProvider::RedpandaCloud
                .capabilities()
                .has_billable_side_resources
        );
    }

    #[test]
    fn auth_kind_reflects_credential_family() {
        assert_eq!(
            KnownProvider::MskIam.capabilities().auth_kind,
            AuthKind::Iam
        );
        assert_eq!(
            KnownProvider::Strimzi.capabilities().auth_kind,
            AuthKind::UserPassword
        );
        assert_eq!(
            KnownProvider::Plaintext.capabilities().auth_kind,
            AuthKind::None
        );
    }

    #[test]
    fn metadata_mode_reflects_deployment_shape() {
        assert_eq!(KnownProvider::Redpanda.metadata_mode(), MetadataMode::KRaft);
        assert_eq!(KnownProvider::Strimzi.metadata_mode(), MetadataMode::KRaft);
        // The clouds hide their metadata plane.
        for key in ["confluent-cloud", "redpanda-cloud", "msk_iam"] {
            assert_eq!(
                KnownProvider::parse(key).unwrap().metadata_mode(),
                MetadataMode::Managed,
                "{key} is managed-hidden"
            );
        }
    }

    #[test]
    fn schema_registry_kind_per_provider() {
        assert_eq!(
            KnownProvider::ConfluentCloud.schema_registry(),
            SchemaRegistry::Confluent
        );
        assert_eq!(
            KnownProvider::RedpandaCloud.schema_registry(),
            SchemaRegistry::Redpanda
        );
        assert_eq!(
            KnownProvider::Redpanda.schema_registry(),
            SchemaRegistry::Redpanda
        );
        assert_eq!(
            KnownProvider::Strimzi.schema_registry(),
            SchemaRegistry::None
        );
        assert_eq!(KnownProvider::Msk.schema_registry(), SchemaRegistry::None);
    }

    // A third-party provider with its own weirdness plugs in by implementing the
    // trait -- no change to this module. (Illustrative: NOT a blessed provider.)
    struct DemoProvider;
    impl KafkaProvider for DemoProvider {
        fn name(&self) -> &str {
            "demo"
        }
        fn auth(&self) -> (&'static str, &'static str) {
            ("SASL_SSL", "SCRAM-SHA-256")
        }
        fn capabilities(&self) -> ProviderCapabilities {
            KnownProvider::Redpanda.capabilities()
        }
    }

    #[test]
    fn third_party_provider_implements_the_trait() {
        let mut cfg = KafkaConfig::default();
        DemoProvider.apply_auth(&mut cfg);
        assert_eq!(cfg.security_protocol, "sasl_ssl");
        assert_eq!(cfg.sasl_mechanism.as_deref(), Some("SCRAM-SHA-256"));
        let (proto, mech) = DemoProvider.auth();
        assert!(validate(proto, mech).is_ok());
    }
}
