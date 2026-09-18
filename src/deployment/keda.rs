// Project:   scalo
// File:      src/deployment/keda.rs
// Purpose:   KEDA autoscaling configuration and contract types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! KEDA autoscaling configuration.
//!
//! [`KedaConfig`] lives in the app's config cascade so thresholds are
//! overridable via env vars (e.g., `DFE_LOADER__KEDA__KAFKA_LAG_THRESHOLD=5000`).
//!
//! [`KedaContract`] is the subset validated against Helm `values.yaml`.

use serde::{Deserialize, Serialize};

/// KEDA autoscaling configuration for the app config cascade.
///
/// Include this in your app's `Config` struct so KEDA thresholds
/// participate in the figment cascade and are env-var overridable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KedaConfig {
    /// Whether KEDA scaling is enabled.
    pub enabled: bool,
    /// Minimum replica count (0 = scale-to-zero).
    pub min_replicas: u32,
    /// Maximum replica count.
    pub max_replicas: u32,
    /// Seconds between KEDA polling the scaler.
    pub polling_interval: u32,
    /// Seconds before scale-down after load drops.
    pub cooldown_period: u32,
    /// Scale when consumer group lag exceeds this per partition.
    pub kafka_lag_threshold: u64,
    /// Wake from zero replicas when lag exceeds this.
    pub activation_lag_threshold: u64,
    /// Enable CPU-based scaling trigger.
    pub cpu_enabled: bool,
    /// CPU utilisation percentage threshold.
    pub cpu_threshold: u32,
}

impl Default for KedaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_replicas: 1,
            max_replicas: 10,
            polling_interval: 15,
            cooldown_period: 300,
            kafka_lag_threshold: 1000,
            activation_lag_threshold: 0,
            cpu_enabled: true,
            cpu_threshold: 80,
        }
    }
}

/// KEDA contract points validated against Helm `values.yaml`.
///
/// Built from [`KedaConfig`] defaults. Use [`KedaContract::from_config`]
/// to convert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KedaContract {
    /// Whether the chart turns KEDA on. `false` generates exactly what
    /// `keda: None` does, so there is one meaning for off.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub min_replicas: u32,
    pub max_replicas: u32,
    pub polling_interval: u32,
    pub cooldown_period: u32,
    pub kafka_lag_threshold: u64,
    pub activation_lag_threshold: u64,
    pub cpu_enabled: bool,
    pub cpu_threshold: u32,
    /// Where the Kafka lag trigger finds its connection details in the chart's
    /// values, or that there is no Kafka lag trigger at all.
    #[serde(default)]
    pub kafka_trigger: KafkaLagTrigger,
}

fn default_enabled() -> bool {
    true
}

impl KedaContract {
    /// Build a contract from a [`KedaConfig`].
    ///
    /// The Kafka lag trigger reads the default `config.kafka` layout; use
    /// [`with_kafka_trigger`](Self::with_kafka_trigger) for any other.
    #[must_use]
    pub fn from_config(config: &KedaConfig) -> Self {
        Self {
            enabled: config.enabled,
            min_replicas: config.min_replicas,
            max_replicas: config.max_replicas,
            polling_interval: config.polling_interval,
            cooldown_period: config.cooldown_period,
            kafka_lag_threshold: config.kafka_lag_threshold,
            activation_lag_threshold: config.activation_lag_threshold,
            cpu_enabled: config.cpu_enabled,
            cpu_threshold: config.cpu_threshold,
            kafka_trigger: KafkaLagTrigger::default(),
        }
    }

    /// Point the Kafka lag trigger at another values layout, or turn it off.
    #[must_use]
    pub fn with_kafka_trigger(mut self, trigger: KafkaLagTrigger) -> Self {
        self.kafka_trigger = trigger;
        self
    }
}

/// Where the generated Kafka lag trigger reads the broker list, consumer group
/// and topics in the chart's values.
///
/// This describes the chart's layout, not an operator setting, which is why it
/// lives on the contract and not on [`KedaConfig`]. Each path is a dotted,
/// `.Values`-relative chain of Go identifiers. The default suits an app whose
/// config has a top-level `kafka` section; an app that keeps them elsewhere
/// names that section with [`under`](Self::under).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaLagTrigger {
    /// Emit the Kafka lag trigger. Off leaves CPU as the only scaler.
    pub enabled: bool,
    /// Values path of the broker list, as a list or a comma-separated string.
    pub brokers_path: String,
    /// Values path of the consumer group id.
    pub group_path: String,
    /// Values path of the topics, as a list or a comma-separated string. KEDA
    /// watches the first.
    pub topics_path: String,
}

impl Default for KafkaLagTrigger {
    fn default() -> Self {
        Self::under("config.kafka")
    }
}

impl KafkaLagTrigger {
    /// A trigger reading `brokers`, `group_id` and `topics` under `base`,
    /// e.g. `config.source`.
    #[must_use]
    pub fn under(base: &str) -> Self {
        Self {
            enabled: true,
            brokers_path: format!("{base}.brokers"),
            group_path: format!("{base}.group_id"),
            topics_path: format!("{base}.topics"),
        }
    }

    /// No Kafka lag trigger, for an app that does not consume from Kafka.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// The three values paths, in brokers, group, topics order.
    pub(crate) fn paths(&self) -> [&str; 3] {
        [&self.brokers_path, &self.group_path, &self.topics_path].map(String::as_str)
    }
}

impl Default for KedaContract {
    fn default() -> Self {
        Self::from_config(&KedaConfig::default())
    }
}

impl From<&KedaConfig> for KedaContract {
    fn from(config: &KedaConfig) -> Self {
        Self::from_config(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keda_config_defaults() {
        let cfg = KedaConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.min_replicas, 1);
        assert_eq!(cfg.max_replicas, 10);
        assert_eq!(cfg.polling_interval, 15);
        assert_eq!(cfg.cooldown_period, 300);
        assert_eq!(cfg.kafka_lag_threshold, 1000);
        assert_eq!(cfg.activation_lag_threshold, 0);
        assert!(cfg.cpu_enabled);
        assert_eq!(cfg.cpu_threshold, 80);
    }

    #[test]
    fn test_keda_contract_from_config() {
        let cfg = KedaConfig {
            kafka_lag_threshold: 5000,
            cpu_threshold: 90,
            ..Default::default()
        };
        let contract = KedaContract::from_config(&cfg);
        assert_eq!(contract.kafka_lag_threshold, 5000);
        assert_eq!(contract.cpu_threshold, 90);
        assert!(contract.enabled);

        let off = KedaContract::from_config(&KedaConfig {
            enabled: false,
            ..Default::default()
        });
        assert!(!off.enabled, "from_config dropped KedaConfig::enabled");
    }

    /// A contract serialised before `enabled` and `kafka_trigger` existed must
    /// still load, and load as the behaviour it had then.
    #[test]
    fn test_keda_contract_without_newer_fields_deserialises_to_defaults() {
        let json = r#"{
            "min_replicas": 1, "max_replicas": 10, "polling_interval": 15,
            "cooldown_period": 300, "kafka_lag_threshold": 1000,
            "activation_lag_threshold": 0, "cpu_enabled": true, "cpu_threshold": 80
        }"#;
        let contract: KedaContract = serde_json::from_str(json).unwrap();
        assert!(contract.enabled);
        assert_eq!(contract.kafka_trigger, KafkaLagTrigger::default());
        assert!(contract.kafka_trigger.enabled);
        assert_eq!(contract.kafka_trigger.brokers_path, "config.kafka.brokers");
        assert_eq!(contract.kafka_trigger.group_path, "config.kafka.group_id");
        assert_eq!(contract.kafka_trigger.topics_path, "config.kafka.topics");
    }

    #[test]
    fn test_kafka_lag_trigger_under_and_disabled() {
        let source = KafkaLagTrigger::under("config.source");
        assert!(source.enabled);
        assert_eq!(
            source.paths(),
            [
                "config.source.brokers",
                "config.source.group_id",
                "config.source.topics"
            ]
        );
        assert!(!KafkaLagTrigger::disabled().enabled);

        let contract = KedaContract::default().with_kafka_trigger(source.clone());
        assert_eq!(contract.kafka_trigger, source);
    }

    #[test]
    fn test_keda_config_serde_roundtrip() {
        let cfg = KedaConfig::default();
        let yaml = serde_yaml_ng::to_string(&cfg).unwrap();
        let parsed: KedaConfig = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(parsed.kafka_lag_threshold, cfg.kafka_lag_threshold);
    }
}
