// Project:   scalo
// File:      src/version_check/mod.rs
// Purpose:   Startup version check against a configured version API
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Startup version check.
//!
//! Calls a configured version API on startup to check if a newer version is
//! available. The check is non-blocking, fire-and-forget, and gracefully
//! handles all failure modes (network errors, timeouts, bad responses).
//!
//! # Usage
//!
//! ```rust,no_run
//! use scalo::version_check::{VersionCheck, VersionCheckConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let checker = VersionCheck::new(VersionCheckConfig {
//!         product: "my-service".into(),
//!         current_version: env!("CARGO_PKG_VERSION").into(),
//!         ..Default::default()
//!     });
//!
//!     // Fire-and-forget -- spawns a background task, never blocks startup
//!     checker.check_on_startup();
//! }
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default HTTP timeout for the version check.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for the startup version check.
///
/// When the `config` feature is enabled, this can be loaded from the config
/// cascade under the `version_check` key:
///
/// ```yaml
/// version_check:
///   enabled: true
///   api_url: "https://releases.example.com/api/v1/check"
///   timeout: 5
///   send_instance_id: true   # false = no identifier in the payload
///   instance_id: ""          # explicit override of the derived id
/// ```
///
/// `product` and `current_version` are always set programmatically -- they
/// come from the binary, not from config files.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VersionCheckConfig {
    /// Product identifier (e.g., "dfe-loader", "dfe-receiver").
    #[serde(default)]
    pub product: String,
    /// Current version of this product (e.g., "1.8.0").
    #[serde(default)]
    pub current_version: String,
    /// Deployment type (e.g., "k8s", "docker", "bare").
    #[serde(default)]
    pub deployment: Option<String>,
    /// Version API endpoint URL. No default -- set it via the `version_check`
    /// config cascade (or programmatically). Empty disables the check.
    #[serde(default)]
    pub api_url: String,
    /// HTTP request timeout in seconds.
    #[serde(default = "default_timeout", with = "duration_secs")]
    pub timeout: Duration,
    /// Enable the startup version check. Opt-in: off unless explicitly true.
    #[serde(default)]
    pub enabled: bool,
    /// Include the platform-derived instance id in the payload, so the same
    /// install reports as the same install across restarts. On by default
    /// within the opt-in check; set `version_check.send_instance_id: false`
    /// for a payload with no identifier at all.
    #[serde(default = "default_true")]
    pub send_instance_id: bool,
    /// Explicit instance id, sent verbatim when set. Overrides the
    /// platform-derived id -- for deployments that carry their own stable
    /// identifier in config.
    #[serde(default)]
    pub instance_id: String,
}

fn default_true() -> bool {
    true
}

fn default_timeout() -> Duration {
    DEFAULT_TIMEOUT
}

/// Serde helper to serialise `Duration` as seconds (u64).
mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for VersionCheckConfig {
    fn default() -> Self {
        Self {
            product: String::new(),
            current_version: String::new(),
            deployment: None,
            api_url: String::new(),
            timeout: DEFAULT_TIMEOUT,
            enabled: false,
            send_instance_id: true,
            instance_id: String::new(),
        }
    }
}

impl VersionCheckConfig {
    /// Load from the config cascade, then overlay product/version.
    ///
    /// Reads the `version_check` key from the cascade for `enabled`,
    /// `api_url`, and `timeout`. The `product` and `current_version`
    /// fields are always set from the provided arguments (they come
    /// from the binary, not from config files).
    #[must_use]
    pub fn from_cascade(product: &str, current_version: &str) -> Self {
        let mut config = Self::cascade_base();
        config.product = product.into();
        config.current_version = current_version.into();
        config
    }

    /// Load just the cascade portion (enabled, api_url, timeout).
    fn cascade_base() -> Self {
        #[cfg(feature = "config")]
        {
            if let Some(cfg) = crate::config::try_get()
                && let Ok(vc) = cfg.unmarshal_key_registered::<Self>("version_check")
            {
                return vc;
            }
        }
        Self::default()
    }
}

/// Startup version checker.
///
/// Call [`VersionCheck::check_on_startup`] during application init to spawn
/// a background task that checks for newer versions. The check never blocks
/// the main thread and gracefully handles all errors.
#[derive(Debug, Clone)]
pub struct VersionCheck {
    config: VersionCheckConfig,
}

impl VersionCheck {
    /// Create a new version checker with the given configuration.
    #[must_use]
    pub fn new(config: VersionCheckConfig) -> Self {
        Self { config }
    }

    /// Spawn a background task to check for a newer version.
    ///
    /// This method returns immediately. The check runs asynchronously and
    /// logs the result. Any errors are logged at warn level and swallowed.
    pub fn check_on_startup(&self) {
        if !self.config.enabled {
            tracing::debug!("version check not enabled (opt-in)");
            return;
        }

        if self.config.api_url.is_empty() {
            tracing::debug!("version check skipped: no api_url configured");
            return;
        }

        if self.config.product.is_empty() || self.config.current_version.is_empty() {
            tracing::debug!("version check skipped: product or version not set");
            return;
        }

        let config = self.config.clone();
        tokio::spawn(async move {
            match do_version_check(&config).await {
                Ok(resp) => log_version_response(&config, &resp),
                Err(e) => {
                    tracing::warn!(error = %e, "version check failed (non-fatal)");
                }
            }
        });
    }
}

// ============================================================================
// Request / response types
// ============================================================================

/// Payload sent to the version check API.
///
/// `product`, `current_version`, `os` (family -- Linux/Darwin/Windows) and
/// `arch` (x86_64/aarch64), plus `instance_id` unless
/// `version_check.send_instance_id: false`. The id is derived from the
/// platform (see [`resolve_instance_id`]) so one install reports as one
/// install across restarts; it is a one-way UUIDv5, so nothing about the
/// host can be recovered from it. `deployment` is never sent: operators
/// embed sensitive names in free-form deployment strings.
#[derive(Debug, Serialize)]
struct CheckPayload {
    product: String,
    current_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    os: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    arch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance_id: Option<String>,
}

/// UUIDv5 namespace for platform-derived instance ids:
/// `uuid5(NAMESPACE_DNS, "scalo.hyperi.io")`. Shared with scalo-py so both
/// chassis derive the SAME id from the same platform material.
const INSTANCE_ID_NS: uuid::Uuid = uuid::uuid!("10ada713-52f0-5b77-aab7-7792712f92a0");

/// Stable per-install instance id, derived from what the app is running on.
///
/// Resolution order, first hit wins:
/// 1. `version_check.instance_id` from the config, verbatim.
/// 2. Kubernetes: UUIDv5 over the serviceaccount cluster CA cert plus the
///    pod namespace. Both are readable in-pod with no API permissions, the
///    CA is unique per cluster and stable for its lifetime, so the id
///    survives every pod restart and reschedule.
/// 3. `/etc/machine-id` (UUIDv5, app-scoped per machine-id(5) -- the raw id
///    never leaves the host). Skipped inside a container, where a
///    machine-id baked into the image would make every install report as
///    the same one.
/// 4. A UUID persisted at `~/.config/scalo/instance_id` (dev machines).
/// 5. An ephemeral UUID for this run alone.
fn resolve_instance_id(config: &VersionCheckConfig) -> String {
    if !config.instance_id.is_empty() {
        return config.instance_id.clone();
    }
    if let Some(id) = k8s_instance_id() {
        return id;
    }
    if let Some(id) = machine_instance_id() {
        return id;
    }
    if let Some(id) = persisted_instance_id() {
        return id;
    }
    uuid::Uuid::new_v4().to_string()
}

fn k8s_instance_id() -> Option<String> {
    let sa = std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount");
    let ca = std::fs::read(sa.join("ca.crt")).ok()?;
    let ns = std::fs::read_to_string(sa.join("namespace")).ok()?;
    let mut material = b"k8s:".to_vec();
    material.extend_from_slice(&ca);
    material.extend_from_slice(b":");
    material.extend_from_slice(ns.trim().as_bytes());
    Some(uuid::Uuid::new_v5(&INSTANCE_ID_NS, &material).to_string())
}

fn machine_instance_id() -> Option<String> {
    if in_container() {
        return None;
    }
    let raw = std::fs::read_to_string("/etc/machine-id")
        .or_else(|_| std::fs::read_to_string("/var/lib/dbus/machine-id"))
        .ok()?;
    let id = raw.trim();
    if id.len() < 32 || id.chars().all(|c| c == '0') {
        return None;
    }
    let material = format!("machine:{id}");
    Some(uuid::Uuid::new_v5(&INSTANCE_ID_NS, material.as_bytes()).to_string())
}

fn in_container() -> bool {
    std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists()
        || std::fs::read_to_string("/proc/1/cgroup").is_ok_and(|c| {
            c.contains("docker") || c.contains("containerd") || c.contains("kubepods")
        })
}

fn persisted_instance_id() -> Option<String> {
    let dir = dirs::config_dir()?.join("scalo");
    let path = dir.join("instance_id");
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let existing = existing.trim();
        if !existing.is_empty() {
            return Some(existing.to_string());
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    std::fs::create_dir_all(&dir).ok()?;
    std::fs::write(&path, &id).ok()?;
    Some(id)
}

/// Response from the version check API.
#[derive(Debug, Deserialize)]
pub struct VersionCheckResponse {
    /// Latest available version (e.g., "1.9.0").
    pub latest_version: Option<String>,
    /// Whether an update is available.
    pub update_available: bool,
    /// URL to the release page.
    pub release_url: Option<String>,
    /// When the latest version was published (ISO 8601).
    pub published_at: Option<String>,
    /// Optional message from the server.
    pub message: Option<String>,
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Once-per-process announcement of the version check. The first time a
/// check runs, log what gets sent. Subsequent calls stay quiet (`info!`,
/// so log-level filtering still applies).
fn announce_once(config: &VersionCheckConfig) {
    static ANNOUNCED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ANNOUNCED.get_or_init(|| {
        tracing::info!(
            endpoint = %config.api_url,
            send_instance_id = config.send_instance_id,
            "version check: sending {{product, current_version, os, arch, instance_id}} \
             to endpoint (opt-in via version_check.enabled; id off via \
             version_check.send_instance_id)"
        );
    });
}

/// Perform the HTTP version check.
async fn do_version_check(
    config: &VersionCheckConfig,
) -> Result<VersionCheckResponse, VersionCheckError> {
    announce_once(config);

    let payload = CheckPayload {
        product: config.product.clone(),
        current_version: config.current_version.clone(),
        os: Some(std::env::consts::OS.into()),
        arch: Some(std::env::consts::ARCH.into()),
        instance_id: config.send_instance_id.then(|| resolve_instance_id(config)),
    };

    let client = reqwest::Client::builder()
        .timeout(config.timeout)
        .build()
        .map_err(|e| VersionCheckError::Http(e.to_string()))?;

    let resp = client
        .post(&config.api_url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| VersionCheckError::Http(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(VersionCheckError::Http(format!("HTTP {}", resp.status())));
    }

    resp.json::<VersionCheckResponse>()
        .await
        .map_err(|e| VersionCheckError::Parse(e.to_string()))
}

/// Log the version check response at the appropriate level.
fn log_version_response(config: &VersionCheckConfig, resp: &VersionCheckResponse) {
    if resp.update_available {
        if let Some(ref latest) = resp.latest_version {
            let age = resp
                .published_at
                .as_deref()
                .and_then(format_age)
                .unwrap_or_default();

            tracing::info!(
                product = %config.product,
                current = %config.current_version,
                latest = %latest,
                age = %age,
                url = resp.release_url.as_deref().unwrap_or(""),
                "new version available"
            );
        }
    } else {
        tracing::debug!(
            product = %config.product,
            version = %config.current_version,
            "running latest version"
        );
    }

    if let Some(ref msg) = resp.message
        && !msg.is_empty()
    {
        tracing::info!(product = %config.product, "{msg}");
    }
}

/// Format an ISO 8601 timestamp into a human-readable age string.
///
/// Returns `None` if the timestamp cannot be parsed.
fn format_age(published_at: &str) -> Option<String> {
    // Parse ISO 8601 with timezone (e.g., "2026-01-15T10:00:00Z")
    // Try with timezone first, then without
    let published = published_at
        .parse::<chrono::DateTime<chrono::Utc>>()
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(published_at, "%Y-%m-%dT%H:%M:%S")
                .map(|dt| dt.and_utc())
        })
        .ok()?;

    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(published);

    let days = duration.num_days();
    if days < 0 {
        return Some("just released".into());
    }
    if days == 0 {
        return Some("released today".into());
    }
    if days == 1 {
        return Some("released 1 day ago".into());
    }
    if days < 30 {
        return Some(format!("released {days} days ago"));
    }
    let months = days / 30;
    if months == 1 {
        return Some("released 1 month ago".into());
    }
    if months < 12 {
        return Some(format!("released {months} months ago"));
    }
    let years = months / 12;
    let remaining_months = months % 12;
    if remaining_months == 0 {
        Some(format!("released {years}y ago"))
    } else {
        Some(format!("released {years}y {remaining_months}m ago"))
    }
}

// Persistent-instance-id helpers removed in 2.7.5 -- the disk-stored
// UUID was a tracking cookie in all but name (see CheckPayload above).
// Friction for SOC2 / regulated consumers outweighed the fleet-
// uniqueness signal.

/// Errors during version check (internal, never exposed to caller).
#[derive(Debug)]
enum VersionCheckError {
    Http(String),
    Parse(String),
}

impl std::fmt::Display for VersionCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(e) => write!(f, "http: {e}"),
            Self::Parse(e) => write!(f, "parse: {e}"),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = VersionCheckConfig::default();
        assert!(config.api_url.is_empty());
        assert_eq!(config.timeout, Duration::from_secs(5));
        assert!(!config.enabled);
        assert!(config.product.is_empty());
    }

    #[test]
    fn check_payload_never_carries_deployment() {
        // Free-form deployment strings carry operator-sensitive names, so
        // the payload struct must not have the field at all.
        let payload = CheckPayload {
            product: "dfe-loader".into(),
            current_version: "1.0.0".into(),
            os: Some("linux".into()),
            arch: Some("x86_64".into()),
            instance_id: None,
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(!json.contains("deployment"));
        assert!(!json.contains("instance_id"));
        assert!(json.contains("product"));
        assert!(json.contains("current_version"));
        assert!(json.contains("\"os\":\"linux\""));
        assert!(json.contains("\"arch\":\"x86_64\""));
    }

    #[test]
    fn test_check_payload_serialization() {
        let payload = CheckPayload {
            product: "dfe-loader".into(),
            current_version: "1.8.0".into(),
            os: Some("linux".into()),
            arch: Some("x86_64".into()),
            instance_id: Some("10ada713-52f0-5b77-aab7-7792712f92a0".into()),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["product"], "dfe-loader");
        assert_eq!(json["current_version"], "1.8.0");
        assert_eq!(json["os"], "linux");
        assert_eq!(json["arch"], "x86_64");
        assert_eq!(json["instance_id"], "10ada713-52f0-5b77-aab7-7792712f92a0");
        assert!(json.get("deployment").is_none());
    }

    #[test]
    fn test_explicit_instance_id_wins() {
        let config = VersionCheckConfig {
            instance_id: "operator-chosen".into(),
            ..Default::default()
        };
        assert_eq!(resolve_instance_id(&config), "operator-chosen");
    }

    #[test]
    fn test_resolved_instance_id_is_stable() {
        // Whatever rung of the derivation ladder this host lands on, two
        // resolutions must agree -- the id exists to be stable.
        let config = VersionCheckConfig::default();
        assert_eq!(resolve_instance_id(&config), resolve_instance_id(&config));
    }

    #[test]
    fn test_send_instance_id_defaults_on() {
        assert!(VersionCheckConfig::default().send_instance_id);
    }

    #[test]
    fn test_k8s_id_matches_py_derivation() {
        // uuid5 over the same material must equal python's
        // uuid.uuid5(SCALO_NS, material) -- the two chassis share the
        // namespace so one platform yields one id.
        let id = uuid::Uuid::new_v5(&INSTANCE_ID_NS, b"machine:test-fixture");
        assert_eq!(id.to_string(), "4f9f9577-e391-5835-8236-3e88e902b11b");
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "latest_version": "1.9.0",
            "update_available": true,
            "release_url": "https://github.com/hyperi-io/dfe-loader/releases/tag/v1.9.0",
            "published_at": "2026-02-15T10:00:00Z",
            "message": null
        }"#;

        let resp: VersionCheckResponse = serde_json::from_str(json).unwrap();
        assert!(resp.update_available);
        assert_eq!(resp.latest_version.as_deref(), Some("1.9.0"));
        assert_eq!(resp.published_at.as_deref(), Some("2026-02-15T10:00:00Z"));
        assert!(resp.message.is_none());
    }

    #[test]
    fn test_response_no_update() {
        let json = r#"{
            "latest_version": "1.8.0",
            "update_available": false,
            "release_url": null,
            "published_at": null,
            "message": null
        }"#;

        let resp: VersionCheckResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.update_available);
    }

    #[test]
    fn test_format_age_today() {
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let age = format_age(&now).unwrap();
        assert_eq!(age, "released today");
    }

    #[test]
    fn test_format_age_days() {
        let ten_days_ago = (chrono::Utc::now() - chrono::Duration::days(10))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let age = format_age(&ten_days_ago).unwrap();
        assert_eq!(age, "released 10 days ago");
    }

    #[test]
    fn test_format_age_months() {
        let three_months_ago = (chrono::Utc::now() - chrono::Duration::days(90))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let age = format_age(&three_months_ago).unwrap();
        assert_eq!(age, "released 3 months ago");
    }

    #[test]
    fn test_format_age_invalid() {
        assert!(format_age("not-a-date").is_none());
    }

    #[test]
    fn test_not_enabled_does_not_spawn() {
        let checker = VersionCheck::new(VersionCheckConfig {
            enabled: false,
            ..Default::default()
        });
        // Should return immediately without panic (no tokio runtime needed)
        checker.check_on_startup();
    }

    #[test]
    fn test_empty_product_does_not_spawn() {
        let checker = VersionCheck::new(VersionCheckConfig::default());
        // Should return immediately without panic
        checker.check_on_startup();
    }
}
