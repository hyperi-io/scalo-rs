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
/// # Image tags
///
/// The profile sets what goes into the image, not its tag. The release
/// pipeline tags images:
///
/// | Build | Tags | Example |
/// |-------|------|---------|
/// | Release channel | `:v<version>`, `:latest`, `:sha-<short>` | `myapp:v1.15.0` |
/// | Pre-GA channel | `:v<version>-<channel>`, `:sha-<short>` | `myapp:v1.15.0-beta` |
/// | Branch dev image | `:branch-<slug>`, `:branch-<slug>-sha-<short>` | `myapp:branch-fix-x` |
///
/// Version tags carry the `v`. There is no `-dev` tag: a `Development` image
/// is built from its own Dockerfile where it is needed.
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

    /// Application name (e.g., "my-app") -- matched against Chart.yaml `name`.
    pub app_name: String,

    /// Binary name (e.g., "my-app"). Defaults to app_name if empty.
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

    /// Config file mount path (e.g., "/etc/my-app/config.yaml").
    pub config_mount_path: String,

    /// Container registry base the image is pushed to and pulled from (e.g.,
    /// "registry.example.com/team"). Required: there is no default, and
    /// [`validate`](Self::validate) refuses an empty value. Read it from the
    /// cascade with [`image_registry_from_cascade`](super::image_registry_from_cascade).
    #[serde(default)]
    pub image_registry: String,

    /// Additional ports beyond metrics (e.g., HTTP data port for receiver).
    #[serde(default)]
    pub extra_ports: Vec<PortContract>,

    /// `default_config` listen paths that need no port, e.g. the bind address
    /// of a client that only sends. Waives them from
    /// [`undeclared_listeners`](Self::undeclared_listeners); no generated
    /// artefact changes with it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unbound_listen_paths: Vec<String>,

    /// Default ENTRYPOINT args (e.g., `["--config", "/etc/my-app/config.yaml"]`).
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
    /// one. Secret fields carry the `x-scalo-secret` marker. Carried inline so a
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

/// Reverse-DNS namespace of the labels and annotations scalo writes itself.
pub const DEFAULT_LABEL_NAMESPACE: &str = "io.scalo";

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
    /// Image vendor, the `org.opencontainers.image.vendor` label. Empty by
    /// default, and an empty vendor writes no label.
    #[serde(default)]
    pub vendor: String,
    /// Reverse-DNS namespace of the keys scalo stamps itself:
    /// `<namespace>.profile`, `<namespace>.app` and `<namespace>.metrics_port`
    /// on the image, and the three `<namespace>.contract.*` identity keys on
    /// every artefact. Defaults to [`DEFAULT_LABEL_NAMESPACE`].
    #[serde(default = "default_label_namespace")]
    pub label_namespace: String,
    /// The app's licence (SPDX). Drives BOTH the OCI
    /// `org.opencontainers.image.licenses` label AND the generated Dockerfile's
    /// `# License` header comment. Empty by default, and an empty licence
    /// writes neither.
    #[serde(default)]
    pub licenses: String,
    /// The app's copyright line for the generated Dockerfile's `# Copyright`
    /// header comment. Empty by default, and an empty copyright writes no line.
    #[serde(default)]
    pub copyright: String,
}

impl Default for OciLabels {
    fn default() -> Self {
        Self {
            title: String::new(),
            description: String::new(),
            vendor: String::new(),
            label_namespace: default_label_namespace(),
            licenses: String::new(),
            copyright: String::new(),
        }
    }
}

fn default_label_namespace() -> String {
    DEFAULT_LABEL_NAMESPACE.to_string()
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
///
/// Build one with [`tcp`](Self::tcp) or [`udp`](Self::udp), then say when its
/// listener exists with [`when`](Self::when()) and which listen address it
/// serves with [`bound_from`](Self::bound_from()).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortContract {
    /// Port name (e.g., "http").
    pub name: String,
    /// Port number (e.g., 8080).
    pub port: u16,
    /// Protocol (default: "TCP").
    #[serde(default = "default_protocol")]
    pub protocol: String,
    /// The values condition under which the listener behind this port exists.
    /// `None` means it always listens. A gated port renders in the chart only
    /// while the condition holds, and stays out of the Dockerfile `EXPOSE`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<PortCondition>,
    /// Dotted `default_config` path of the listen address this port serves,
    /// e.g. `grpc.listen`. Read only by
    /// [`undeclared_listeners`](DeploymentContract::undeclared_listeners); no
    /// generated artefact changes with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_from: Option<String>,
}

impl PortContract {
    /// A TCP port that always listens.
    #[must_use]
    pub fn tcp(name: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            port,
            protocol: "TCP".to_string(),
            when: None,
            bound_from: None,
        }
    }

    /// A UDP port that always listens.
    #[must_use]
    pub fn udp(name: impl Into<String>, port: u16) -> Self {
        Self {
            protocol: "UDP".to_string(),
            ..Self::tcp(name, port)
        }
    }

    /// Listen only while `condition` holds.
    #[must_use]
    pub fn when(mut self, condition: PortCondition) -> Self {
        self.when = Some(condition);
        self
    }

    /// Listen only while the value at `path` counts as true, e.g. `config.grpc.enabled`.
    #[must_use]
    pub fn when_enabled(self, path: impl Into<String>) -> Self {
        self.when(PortCondition::Enabled { path: path.into() })
    }

    /// Listen only while the value at `path` equals `value`.
    #[must_use]
    pub fn when_equals(self, path: impl Into<String>, value: impl Into<String>) -> Self {
        self.when(PortCondition::Equals {
            path: path.into(),
            value: value.into(),
        })
    }

    /// Listen only while the value at `path` is one of `values`, for a setting
    /// that accepts an alias.
    #[must_use]
    pub fn when_one_of<I, S>(self, path: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.when(PortCondition::OneOf {
            path: path.into(),
            values: values.into_iter().map(Into::into).collect(),
        })
    }

    /// Name the `default_config` listen address this port serves, e.g. `grpc.listen`.
    #[must_use]
    pub fn bound_from(mut self, path: impl Into<String>) -> Self {
        self.bound_from = Some(path.into());
        self
    }
}

/// When a port's listener exists, as a test on a chart values path.
///
/// `path` is dotted and `.Values`-relative, and each segment must be a Go
/// identifier, e.g. `config.source.transport`. The chart reads app config under
/// `config`, so only a path under `config.` can be checked against
/// `default_config`. A missing or null value never satisfies a condition.
///
/// `Equals` and `OneOf` compare the chart's `toString` of the value, so gate on
/// a string or boolean setting. A numeric one compares unreliably: Helm reads a
/// large number in `values.yaml` as a float and prints `1e+06` where the config
/// says `1000000`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PortCondition {
    /// The value counts as true: anything but false, null, zero or empty.
    Enabled {
        /// Values path of the switch.
        path: String,
    },
    /// The value, as a string, equals `value`.
    Equals {
        /// Values path of the setting.
        path: String,
        /// The value that turns the listener on.
        value: String,
    },
    /// The value, as a string, is one of `values`.
    OneOf {
        /// Values path of the setting.
        path: String,
        /// The values that turn the listener on.
        values: Vec<String>,
    },
}

impl PortCondition {
    /// The values path the condition reads.
    #[must_use]
    pub fn path(&self) -> &str {
        match self {
            Self::Enabled { path } | Self::Equals { path, .. } | Self::OneOf { path, .. } => path,
        }
    }

    /// Whether the condition holds for `default_config` rendered under
    /// `config`, as the generated chart would evaluate it.
    ///
    /// `None` when the path is not under `config.` or names nothing in
    /// `default_config`, so the answer depends on values set at install time.
    #[must_use]
    pub fn holds_in(&self, default_config: &serde_json::Value) -> Option<bool> {
        let value = value_at(default_config, self.path().strip_prefix("config.")?)?;
        Some(match self {
            Self::Enabled { .. } => helm_truthy(value),
            Self::Equals { value: wanted, .. } => helm_string(value).as_ref() == Some(wanted),
            Self::OneOf { values, .. } => {
                helm_string(value).is_some_and(|value| values.contains(&value))
            }
        })
    }
}

impl std::fmt::Display for PortCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enabled { path } => write!(f, "{path} is true"),
            Self::Equals { path, value } => write!(f, "{path} is \"{value}\""),
            Self::OneOf { path, values } => {
                write!(f, "{path} is one of ")?;
                for (index, value) in values.iter().enumerate() {
                    let sep = if index == 0 { "" } else { ", " };
                    write!(f, "{sep}\"{value}\"")?;
                }
                Ok(())
            }
        }
    }
}

/// Helm's truthiness: false, null, zero and empty strings, lists and maps are false.
fn helm_truthy(value: &serde_json::Value) -> bool {
    use serde_json::Value;
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

/// What the chart's `toString` makes of a scalar; `None` for null, a list or a map.
fn helm_string(value: &serde_json::Value) -> Option<String> {
    use serde_json::Value;
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

/// The value at a dotted path under `root`, where a numeric segment indexes a list.
pub(crate) fn value_at<'a>(
    root: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    path.split('.').try_fold(root, |node, key| match node {
        serde_json::Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => node.get(key),
    })
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
    super::DEFAULT_BASE_IMAGE.to_string()
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

    /// Get the config mount directory (e.g., "/etc/my-app").
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

    /// The KEDA contract when it turns KEDA on; `None` when absent or disabled.
    pub(crate) fn enabled_keda(&self) -> Option<&KedaContract> {
        self.keda.as_ref().filter(|keda| keda.enabled)
    }

    /// `.Values` paths the generated chart reads that `default_config` does not
    /// supply.
    ///
    /// The chart writes `default_config` under `config`, so only a path under
    /// `config.` can resolve. A path that resolves to null is reported as well,
    /// because the chart renders it empty, except a port gate's path, where
    /// null reads as off. An empty result means every path the chart reads has
    /// a value to render.
    #[must_use]
    pub fn unresolved_values_paths(&self) -> Vec<String> {
        self.chart_values_paths()
            .into_iter()
            .filter(|&(path, read)| !self.default_config_supplies(path, read))
            .map(|(path, _)| path.to_owned())
            .collect()
    }

    /// Every `.Values` path the generated chart reads from app config, gathered
    /// from each part of the contract that names one.
    fn chart_values_paths(&self) -> Vec<(&str, ValuesRead)> {
        let mut paths = Vec::new();
        if let Some(keda) = self.enabled_keda()
            && keda.kafka_trigger.enabled
        {
            paths.extend(
                keda.kafka_trigger
                    .paths()
                    .map(|path| (path, ValuesRead::Rendered)),
            );
        }
        for gate in self.extra_ports.iter().filter_map(|p| p.when.as_ref()) {
            if !paths.iter().any(|&(path, _)| path == gate.path()) {
                paths.push((gate.path(), ValuesRead::Gate));
            }
        }
        paths
    }

    /// True when `path` names a value in `default_config` that the chart can
    /// use the way `read` says it is used.
    fn default_config_supplies(&self, path: &str, read: ValuesRead) -> bool {
        let found = self
            .default_config
            .as_ref()
            .zip(path.strip_prefix("config."))
            .and_then(|(config, rest)| value_at(config, rest));
        match read {
            ValuesRead::Rendered => found.is_some_and(|value| !value.is_null()),
            ValuesRead::Gate => found.is_some(),
        }
    }
}

/// How the chart uses a values path, which decides whether null counts as supplied.
#[derive(Debug, Clone, Copy)]
enum ValuesRead {
    /// Written into a manifest, where null renders empty.
    Rendered,
    /// Tested by a port gate, where null is plain off.
    Gate,
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
            image_registry: "registry.example.com".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
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
            image_registry: "registry.example.com".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
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
            image_registry: "registry.example.com".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
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
            image_registry: "registry.example.com".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
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

    /// A contract with KEDA on and the given `default_config`.
    fn keda_contract(default_config: Option<serde_json::Value>) -> DeploymentContract {
        DeploymentContract {
            app_name: "test".into(),
            config_mount_path: "/etc/test/config.yaml".into(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "T".into(),
            metric_prefix: "t".into(),
            keda: Some(KedaContract::default()),
            binary_name: String::new(),
            description: String::new(),
            image_registry: "registry.example.com".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config,
            depends_on: vec![],
            base_image: "ubuntu:24.04".into(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::default(),
            schema_version: 3,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        }
    }

    #[test]
    fn test_unresolved_values_paths_finds_a_missing_path_and_passes_a_present_one() {
        let contract = keda_contract(Some(serde_json::json!({
            "kafka": {
                "brokers": ["kafka:9092"],
                "group_id": null,
            }
        })));
        // brokers is set; group_id is null and topics is absent, and the chart
        // would render both empty.
        assert_eq!(
            contract.unresolved_values_paths(),
            vec![
                "config.kafka.group_id".to_string(),
                "config.kafka.topics".to_string()
            ]
        );

        let complete = keda_contract(Some(serde_json::json!({
            "kafka": { "brokers": ["kafka:9092"], "group_id": "g", "topics": ["t"] }
        })));
        assert_eq!(
            complete.unresolved_values_paths(),
            [] as [std::string::String; 0]
        );
    }

    /// A contract written before `when`, `bound_from` and
    /// `unbound_listen_paths` existed must load, and one that uses none of
    /// them must serialise without their keys.
    #[test]
    fn test_contract_without_listener_fields_loads_and_serialises_without_them() {
        let json = r#"{
            "app_name": "old", "metrics_port": 9090,
            "health": { "liveness_path": "/livez", "readiness_path": "/readyz",
                        "metrics_path": "/metrics" },
            "env_prefix": "OLD", "metric_prefix": "old",
            "config_mount_path": "/etc/old/config.yaml", "keda": null,
            "extra_ports": [ { "name": "http", "port": 8080 } ]
        }"#;
        let contract: DeploymentContract = serde_json::from_str(json).unwrap();
        assert_eq!(contract.extra_ports, vec![PortContract::tcp("http", 8080)]);
        assert_eq!(
            contract.unbound_listen_paths,
            [] as [std::string::String; 0]
        );

        let out = contract.to_json();
        for key in ["\"when\"", "\"bound_from\"", "\"unbound_listen_paths\""] {
            assert!(!out.contains(key), "{key} serialised when unset:\n{out}");
        }
    }

    /// The labels name no vendor, licence or copyright of their own, so an app
    /// gets none it did not set.
    #[test]
    fn test_oci_labels_default_to_no_vendor_licence_or_copyright() {
        let labels = OciLabels::default();
        assert_eq!(labels.vendor, "");
        assert_eq!(labels.licenses, "");
        assert_eq!(labels.copyright, "");
        assert_eq!(labels.label_namespace, "io.scalo");
        assert_eq!(labels.label_namespace, DEFAULT_LABEL_NAMESPACE);
    }

    /// Labels written before `label_namespace` existed load under the default
    /// namespace and keep the vendor, licence and copyright they name.
    #[test]
    fn test_oci_labels_without_a_namespace_load_under_the_default() {
        let labels: OciLabels = serde_json::from_str(
            r#"{ "vendor": "Example Ltd", "licenses": "MIT", "copyright": "(c) 2026 Example Ltd" }"#,
        )
        .unwrap();
        assert_eq!(labels.vendor, "Example Ltd");
        assert_eq!(labels.licenses, "MIT");
        assert_eq!(labels.copyright, "(c) 2026 Example Ltd");
        assert_eq!(labels.label_namespace, DEFAULT_LABEL_NAMESPACE);
    }

    /// A contract that names no registry still loads, and `validate` refuses it.
    #[test]
    fn test_contract_without_a_registry_loads_with_an_empty_one() {
        let json = r#"{
            "app_name": "old", "metrics_port": 9090,
            "health": { "liveness_path": "/livez", "readiness_path": "/readyz",
                        "metrics_path": "/metrics" },
            "env_prefix": "OLD", "metric_prefix": "old",
            "config_mount_path": "/etc/old/config.yaml", "keda": null
        }"#;
        let contract: DeploymentContract = serde_json::from_str(json).unwrap();
        assert_eq!(contract.image_registry, "");
        match contract.validate() {
            Err(crate::deployment::DeploymentError::InvalidContract { field, reason }) => {
                assert_eq!(field, "image_registry");
                assert!(reason.contains("deployment.image_registry"), "{reason}");
            }
            other => panic!("an empty registry passed validate: {other:?}"),
        }
    }

    #[test]
    fn test_port_condition_serde_shape() {
        let port = PortContract::udp("relay", 6000)
            .when_one_of("config.source.transport", ["direct", "grpc"])
            .bound_from("source.grpc.listen");
        let json = serde_json::to_value(&port).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "name": "relay", "port": 6000, "protocol": "UDP",
                "when": { "kind": "one_of", "path": "config.source.transport",
                          "values": ["direct", "grpc"] },
                "bound_from": "source.grpc.listen",
            })
        );
        let back: PortContract = serde_json::from_value(json).unwrap();
        assert_eq!(back, port);

        for (condition, shape) in [
            (
                PortCondition::Enabled {
                    path: "config.grpc.enabled".into(),
                },
                serde_json::json!({ "kind": "enabled", "path": "config.grpc.enabled" }),
            ),
            (
                PortCondition::Equals {
                    path: "config.transport".into(),
                    value: "grpc".into(),
                },
                serde_json::json!({ "kind": "equals", "path": "config.transport", "value": "grpc" }),
            ),
        ] {
            assert_eq!(serde_json::to_value(&condition).unwrap(), shape);
        }
    }

    #[test]
    fn test_port_condition_holds_in_uses_chart_truthiness() {
        let config = serde_json::json!({
            "on": true, "off": false, "none": null, "zero": 0, "one": 1,
            "empty": "", "text": "x", "list": [], "items": [1], "map": {},
            "source": { "transport": "direct" }, "port": 6000,
        });
        let enabled = |path: &str| {
            PortCondition::Enabled {
                path: format!("config.{path}"),
            }
            .holds_in(&config)
        };
        for truthy in ["on", "one", "text", "items", "source"] {
            assert_eq!(enabled(truthy), Some(true), "{truthy}");
        }
        for falsy in ["off", "none", "zero", "empty", "list", "map"] {
            assert_eq!(enabled(falsy), Some(false), "{falsy}");
        }
        // Absent from the config, so only install-time values can decide.
        assert_eq!(enabled("missing"), None);
        assert_eq!(enabled("source.missing"), None);
        // Not under `config.`, so `default_config` cannot answer for it.
        assert_eq!(
            PortCondition::Enabled { path: "on".into() }.holds_in(&config),
            None
        );

        let equals = |path: &str, value: &str| {
            PortCondition::Equals {
                path: format!("config.{path}"),
                value: value.into(),
            }
            .holds_in(&config)
        };
        assert_eq!(equals("source.transport", "direct"), Some(true));
        assert_eq!(equals("source.transport", "bus"), Some(false));
        assert_eq!(equals("port", "6000"), Some(true));
        assert_eq!(equals("on", "true"), Some(true));
        assert_eq!(equals("none", "direct"), Some(false));
        assert_eq!(equals("missing", "direct"), None);

        let one_of = PortCondition::OneOf {
            path: "config.source.transport".into(),
            values: vec!["grpc".into(), "direct".into()],
        };
        assert_eq!(one_of.holds_in(&config), Some(true));
        let other = PortCondition::OneOf {
            path: "config.source.transport".into(),
            values: vec!["grpc".into()],
        };
        assert_eq!(other.holds_in(&config), Some(false));
    }

    #[test]
    fn test_port_condition_reads_as_a_sentence() {
        let port = |p: PortContract| p.when.map(|w| w.to_string()).unwrap_or_default();
        assert_eq!(
            port(PortContract::tcp("g", 1).when_enabled("config.grpc.enabled")),
            "config.grpc.enabled is true"
        );
        assert_eq!(
            port(PortContract::tcp("p", 1).when_equals("config.source.transport", "direct")),
            "config.source.transport is \"direct\""
        );
        assert_eq!(
            port(PortContract::tcp("v", 1).when_one_of("config.t", ["direct", "grpc"])),
            "config.t is one of \"direct\", \"grpc\""
        );
    }

    /// A gate the config cannot answer is reported like any other unresolved
    /// values path; a gate whose key is null is off, not unresolved.
    #[test]
    fn test_unresolved_values_paths_reports_a_gate_the_config_lacks() {
        let mut contract = keda_contract(Some(serde_json::json!({
            "kafka": { "brokers": ["k:9092"], "group_id": "g", "topics": ["t"] },
            "grpc": { "enabled": null },
        })));
        contract.extra_ports = vec![
            PortContract::tcp("grpc", 6000).when_enabled("config.grpc.enabled"),
            PortContract::tcp("push", 6001).when_equals("config.source.transport", "direct"),
            PortContract::tcp("again", 6002).when_equals("config.source.transport", "grpc"),
            PortContract::tcp("top", 6003).when_enabled("push.enabled"),
            PortContract::tcp("always", 6004),
        ];
        assert_eq!(
            contract.unresolved_values_paths(),
            vec![
                "config.source.transport".to_string(),
                "push.enabled".to_string()
            ]
        );
    }

    #[test]
    fn test_unresolved_values_paths_follows_the_trigger_and_keda_switches() {
        let source = serde_json::json!({
            "source": { "brokers": "kafka:9092", "group_id": "g", "topics": "t" }
        });

        // The default trigger reads config.kafka, which a source-shaped config lacks.
        assert_eq!(
            keda_contract(Some(source.clone()))
                .unresolved_values_paths()
                .len(),
            3
        );

        let mut pointed = keda_contract(Some(source));
        pointed.keda = pointed.keda.map(|k| {
            k.with_kafka_trigger(crate::deployment::KafkaLagTrigger::under("config.source"))
        });
        assert_eq!(
            pointed.unresolved_values_paths(),
            [] as [std::string::String; 0]
        );

        // No config at all resolves nothing.
        assert_eq!(keda_contract(None).unresolved_values_paths().len(), 3);

        // Nothing is read when the trigger or KEDA itself is off.
        let mut no_trigger = keda_contract(None);
        no_trigger.keda = no_trigger
            .keda
            .map(|k| k.with_kafka_trigger(crate::deployment::KafkaLagTrigger::disabled()));
        assert_eq!(
            no_trigger.unresolved_values_paths(),
            [] as [std::string::String; 0]
        );

        let mut keda_off = keda_contract(None);
        if let Some(keda) = keda_off.keda.as_mut() {
            keda.enabled = false;
        }
        assert_eq!(
            keda_off.unresolved_values_paths(),
            [] as [std::string::String; 0]
        );
    }
}
