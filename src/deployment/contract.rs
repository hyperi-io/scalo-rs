// Project:   scalo
// File:      src/deployment/contract.rs
// Purpose:   Deployment contract types
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Deployment contract types.

use serde::{Deserialize, Serialize};

use super::keda::KedaContract;
use super::native_deps::NativeDepsContract;

/// Container image profile -- controls what goes into the generated Dockerfile.
///
/// Both profiles use the same linking strategy (dynamic). The difference is
/// optimisation level, debug tooling, and image metadata.
///
/// # Image tagging convention
///
/// | Profile | Tag | Example |
/// |---------|-----|---------|
/// | `Production` | `:<version>`, `:latest` | `dfe-loader:1.15.0` |
/// | `Development` | `:<version>-dev`, `:latest-dev` | `dfe-loader:1.15.0-dev` |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageProfile {
    /// Minimal production image -- stripped binary, no debug tools.
    #[default]
    Production,
    /// Development image -- includes diagnostic tools (bash, strace, tcpdump,
    /// procps, dnsutils, net-tools). Same binary, same linking.
    Development,
}

/// Deployment-facing contract points derived from the app config cascade.
///
/// Apps build this from their `Config::default()`. Validation functions
/// compare Helm charts and Dockerfiles against these values. Generation
/// functions create deployment artifacts (Dockerfile, Helm chart, Compose
/// fragment) from scratch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentContract {
    /// Contract schema version. CI checks this and fails if unsupported.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,

    /// Application name (e.g., "dfe-loader") -- matched against Chart.yaml `name`.
    pub app_name: String,

    /// Binary name (e.g., "dfe-loader"). Defaults to app_name if empty.
    #[serde(default)]
    pub binary_name: String,

    /// One-line description for Chart.yaml.
    #[serde(default)]
    pub description: String,

    /// Metrics/health listen port (e.g., 9090).
    pub metrics_port: u16,

    /// Health probe endpoint paths.
    pub health: HealthContract,

    /// Environment variable prefix (e.g., "DFE_LOADER").
    /// Used with `__` nesting for figment config cascade.
    pub env_prefix: String,

    /// Prometheus metric namespace/prefix (e.g., "loader").
    pub metric_prefix: String,

    /// Config file mount path (e.g., "/etc/dfe/loader.yaml").
    pub config_mount_path: String,

    /// Container registry base (e.g., "ghcr.io/hyperi-io").
    #[serde(default = "default_image_registry")]
    pub image_registry: String,

    /// Additional ports beyond metrics (e.g., HTTP data port for receiver).
    #[serde(default)]
    pub extra_ports: Vec<PortContract>,

    /// Default ENTRYPOINT args (e.g., `["--config", "/etc/dfe/loader.yaml"]`).
    #[serde(default)]
    pub entrypoint_args: Vec<String>,

    /// Secret groups injected from K8s Secrets.
    #[serde(default)]
    pub secrets: Vec<SecretGroupContract>,

    /// App-specific config YAML for values.yaml (serialised as serde_json::Value).
    #[serde(default)]
    pub default_config: Option<serde_json::Value>,

    /// Docker Compose service dependencies (e.g., `["kafka", "clickhouse"]`).
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// KEDA autoscaling contract (None if KEDA not used).
    pub keda: Option<KedaContract>,

    /// Base container image for the runtime stage.
    #[serde(default = "default_base_image")]
    pub base_image: String,

    /// Runtime native dependencies for the container image.
    ///
    /// Use [`NativeDepsContract::for_scalo_features`] to auto-populate from
    /// scalo feature flags. The Dockerfile generator emits the correct
    /// APT repo setup and package installation commands.
    #[serde(default)]
    pub native_deps: NativeDepsContract,

    /// Image profile -- production (minimal) or development (debug tools).
    ///
    /// Defaults to [`ImageProfile::Production`]. Use [`with_dev_profile`](Self::with_dev_profile)
    /// to derive a development variant from an existing contract.
    #[serde(default)]
    pub image_profile: ImageProfile,

    /// OCI image labels (static -- dynamic labels injected by CI at build time).
    #[serde(default)]
    pub oci_labels: OciLabels,

    /// Reflectable JSON Schema (draft 2020-12) of the app's full `Config`,
    /// derived via schemars (scalo-rs#6). `None` when the app does not provide
    /// one. Secret fields carry the `x-dfe-secret` marker. Carried inline so a
    /// single fetch of the contract gives the schema; also written to
    /// `config-schema.{json,yaml}` by [`emit_config_artifacts`](super::emit_config_artifacts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<serde_json::Value>,

    /// Capability catalog -- the runtime-data surface schemars cannot derive
    /// (service names + their knobs). Hand-authored per app. Empty when the app
    /// does not provide one. Also written to `capability-catalog.{json,yaml}`.
    /// See [`Capability`](super::Capability).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<super::Capability>,
}

/// OCI image labels for the container.
///
/// Static labels are set from the contract. Dynamic labels (source, revision,
/// version, created) are injected by CI at build time via `--build-arg`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciLabels {
    /// Image title (defaults to app_name).
    #[serde(default)]
    pub title: String,
    /// Image description.
    #[serde(default)]
    pub description: String,
    /// Image vendor.
    #[serde(default = "default_vendor")]
    pub vendor: String,
    /// License identifier (SPDX). Drives BOTH the OCI
    /// `org.opencontainers.image.licenses` label AND the generated Dockerfile's
    /// `# License` header comment, so a non-Apache consumer (e.g. a BUSL-1.1
    /// app) gets a correct header. Defaults to scalo's own (Apache-2.0).
    #[serde(default = "default_license")]
    pub licenses: String,
    /// Copyright holder line for the generated Dockerfile's `# Copyright`
    /// header comment. Consumer-supplied so a non-scalo consumer does not get
    /// scalo's copyright stamped into their repo. Defaults to scalo's own.
    #[serde(default = "default_copyright")]
    pub copyright: String,
}

impl Default for OciLabels {
    fn default() -> Self {
        Self {
            title: String::new(),
            description: String::new(),
            vendor: default_vendor(),
            licenses: default_license(),
            copyright: default_copyright(),
        }
    }
}

fn default_vendor() -> String {
    "HYPERI PTY LIMITED".to_string()
}

fn default_license() -> String {
    "Apache-2.0".to_string()
}

fn default_copyright() -> String {
    "(c) 2026 HYPERI PTY LIMITED".to_string()
}

fn default_schema_version() -> u32 {
    // v3: added `config_schema` + `capabilities` (scalo-rs#6). Back-compat --
    // old consumers ignore the new optional fields.
    3
}

/// Health probe endpoint paths.
///
/// There is no startup path. A `startupProbe` targets `liveness_path`:
/// Kubernetes suspends liveness until the startup probe passes, so one path
/// gives both a generous boot budget and a tight liveness period without the
/// two drifting apart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthContract {
    /// Liveness probe path (e.g., "/livez").
    pub liveness_path: String,

    /// Readiness probe path (e.g., "/readyz").
    pub readiness_path: String,

    /// Prometheus metrics path (e.g., "/metrics").
    pub metrics_path: String,
}

/// Additional container port beyond the metrics port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortContract {
    /// Port name (e.g., "http").
    pub name: String,
    /// Port number (e.g., 8080).
    pub port: u16,
    /// Protocol (default: "TCP").
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

/// A group of secrets from the same K8s Secret (e.g., "kafka", "clickhouse").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretGroupContract {
    /// Group name (e.g., "kafka", "clickhouse").
    /// Used in values.yaml section name and helper template names.
    pub group_name: String,

    /// Environment variables injected from this secret group.
    pub env_vars: Vec<SecretEnvContract>,
}

/// A single environment variable sourced from a K8s Secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretEnvContract {
    /// Full env var name (e.g., "DFE_LOADER__KAFKA__PASSWORD").
    pub env_var: String,

    /// Key name in values.yaml secretKeys and default values
    /// (e.g., "password", "username").
    pub key_name: String,

    /// Default K8s secret key name (e.g., "kafka-password").
    pub secret_key: String,
}

fn default_base_image() -> String {
    "debian:trixie-slim".to_string()
}

fn default_image_registry() -> String {
    "ghcr.io/hyperi-io".to_string()
}

fn default_protocol() -> String {
    "TCP".to_string()
}

impl DeploymentContract {
    /// Get the effective binary name (falls back to app_name).
    #[must_use]
    pub fn binary(&self) -> &str {
        if self.binary_name.is_empty() {
            &self.app_name
        } else {
            &self.binary_name
        }
    }

    /// Get the config file name from the mount path (e.g., "loader.yaml").
    #[must_use]
    pub fn config_filename(&self) -> &str {
        self.config_mount_path
            .rsplit('/')
            .next()
            .unwrap_or("config.yaml")
    }

    /// Get the config mount directory (e.g., "/etc/dfe").
    #[must_use]
    pub fn config_dir(&self) -> &str {
        self.config_mount_path
            .rsplit_once('/')
            .map_or("/etc", |(dir, _)| dir)
    }

    /// Serialise the contract to JSON for `--emit-contract` CLI support.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// Serialise the contract to YAML.
    #[must_use]
    pub fn to_yaml(&self) -> String {
        serde_yaml_ng::to_string(self).unwrap_or_default()
    }

    /// Return a clone with [`ImageProfile::Development`] set.
    ///
    /// Useful for generating both production and dev Dockerfiles from a single
    /// contract definition.
    #[must_use]
    pub fn with_dev_profile(&self) -> Self {
        let mut dev = self.clone();
        dev.image_profile = ImageProfile::Development;
        dev
    }
}

impl Default for HealthContract {
    fn default() -> Self {
        Self {
            liveness_path: "/livez".to_string(),
            readiness_path: "/readyz".to_string(),
            metrics_path: "/metrics".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_contract_defaults() {
        let h = HealthContract::default();
        assert_eq!(h.liveness_path, "/livez");
        assert_eq!(h.readiness_path, "/readyz");
        assert_eq!(h.metrics_path, "/metrics");
    }

    #[test]
    fn test_contract_to_json() {
        let contract = DeploymentContract {
            app_name: "test-app".into(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "TEST_APP".into(),
            metric_prefix: "test".into(),
            config_mount_path: "/etc/test/config.yaml".into(),
            keda: None,
            binary_name: String::new(),
            description: String::new(),
            image_registry: default_image_registry(),
            extra_ports: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            base_image: "ubuntu:24.04".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            schema_version: 3,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        };
        let json = contract.to_json();
        assert!(json.contains("test-app"));
        assert!(json.contains("9090"));
    }

    #[test]
    fn test_contract_roundtrip_json() {
        let contract = DeploymentContract {
            app_name: "roundtrip".into(),
            metrics_port: 8080,
            health: HealthContract::default(),
            env_prefix: "RT".into(),
            metric_prefix: "rt".into(),
            config_mount_path: "/config.yaml".into(),
            keda: None,
            binary_name: String::new(),
            description: String::new(),
            image_registry: default_image_registry(),
            extra_ports: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            base_image: "ubuntu:24.04".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            schema_version: 3,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        };
        let json = contract.to_json();
        let parsed: DeploymentContract = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.app_name, "roundtrip");
        assert_eq!(parsed.metrics_port, 8080);
    }

    #[test]
    fn test_binary_name_fallback() {
        let contract = DeploymentContract {
            app_name: "my-app".into(),
            binary_name: String::new(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "MY_APP".into(),
            metric_prefix: "app".into(),
            config_mount_path: "/etc/app/config.yaml".into(),
            keda: None,
            description: String::new(),
            image_registry: default_image_registry(),
            extra_ports: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            base_image: "ubuntu:24.04".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            schema_version: 3,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        };
        assert_eq!(contract.binary(), "my-app");
    }

    #[test]
    fn test_config_filename() {
        let contract = DeploymentContract {
            app_name: "test".into(),
            config_mount_path: "/etc/dfe/loader.yaml".into(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "T".into(),
            metric_prefix: "t".into(),
            keda: None,
            binary_name: String::new(),
            description: String::new(),
            image_registry: default_image_registry(),
            extra_ports: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            base_image: "ubuntu:24.04".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            schema_version: 3,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        };
        assert_eq!(contract.config_filename(), "loader.yaml");
        assert_eq!(contract.config_dir(), "/etc/dfe");
    }
}
