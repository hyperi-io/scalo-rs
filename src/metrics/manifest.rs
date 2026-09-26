// Project:   scalo
// File:      src/metrics/manifest.rs
// Purpose:   Metric manifest types and registry for /metrics/manifest endpoint
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Machine-readable metric metadata for the `/metrics/manifest` endpoint:
//! [`MetricDescriptor`], [`MetricRegistry`], [`ManifestResponse`].
//!
//! Field semantics align with
//! [OpenMetrics](https://prometheus.io/docs/specs/om/open_metrics_spec/) (type,
//! description, unit) and [OTel Advisory Parameters](https://opentelemetry.io/docs/specs/otel/metrics/api/)
//! (labels, buckets). `group`, `use_cases`, `dashboard_hint` are local extensions.
//!
//! ## Standards Alignment
//!
//! | Field | Standard |
//! |-------|----------|
//! | `type` | OpenMetrics `TYPE` |
//! | `description` | OpenMetrics `HELP` |
//! | `unit` | OpenMetrics `UNIT` |
//! | `labels` | OTel Advisory `Attributes` |
//! | `buckets` | OTel Advisory `ExplicitBucketBoundaries` |
//! | `group`, `use_cases`, `dashboard_hint` | local extensions |

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

/// Describes a single registered metric for the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDescriptor {
    /// Full metric name including namespace prefix.
    pub name: String,
    /// Metric type (aligns with OpenMetrics TYPE).
    #[serde(rename = "type")]
    pub metric_type: MetricType,
    /// Human-readable description (aligns with OpenMetrics HELP).
    pub description: String,
    /// Unit suffix (aligns with OpenMetrics UNIT). Empty for counters.
    pub unit: String,
    /// Known label keys (aligns with OTel Advisory Attributes).
    pub labels: Vec<String>,
    /// Metric group membership. Always present. Defaults to `"custom"`.
    pub group: String,
    /// Histogram bucket boundaries (aligns with OTel Advisory ExplicitBucketBoundaries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<Vec<f64>>,
    /// Operational guidance: when to alert, what dashboard to use.
    /// Novel local extension. Omitted from JSON when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub use_cases: Vec<String>,
    /// Suggested Grafana panel type. Novel local extension.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dashboard_hint: Option<String>,
}

/// Metric type discriminator (aligns with OpenMetrics TYPE).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

/// JSON response for `GET /metrics/manifest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestResponse {
    pub schema_version: u32,
    /// The service the manifest describes. Holds the namespace until the
    /// service runtime or the CLI names the service.
    pub app: String,
    /// The `{namespace}_` prefix every metric name carries; empty when names
    /// are bare.
    #[serde(default)]
    pub namespace: String,
    pub version: String,
    pub commit: String,
    /// When the running service's registry was created, RFC 3339. Empty in a
    /// manifest the CLI generates offline, so a regenerated file is the same
    /// bytes on every run.
    pub registered_at: String,
    pub metrics: Vec<MetricDescriptor>,
}

/// Inner state of the metric registry.
struct MetricRegistryInner {
    descriptors: Vec<MetricDescriptor>,
    /// The prefix `push` puts on every name; empty for bare names.
    namespace: String,
    app: String,
    version: String,
    commit: String,
    /// Whether an app metric set has recorded the build info.
    #[cfg(feature = "service-metrics")]
    build_info_claimed: bool,
    registered_at: String,
}

/// Cloneable handle to the metric registry.
///
/// Obtained via [`super::MetricsManager::registry`]. Safe to clone into
/// axum route handlers or share across tasks.
#[derive(Clone)]
pub struct MetricRegistry {
    inner: Arc<RwLock<MetricRegistryInner>>,
}

impl MetricRegistry {
    /// Create a new registry for the given namespace. The manifest's `app`
    /// starts as the namespace until `set_app_name` names the service.
    pub(crate) fn new(namespace: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetricRegistryInner {
                descriptors: Vec::new(),
                namespace: namespace.to_string(),
                app: namespace.to_string(),
                version: String::new(),
                commit: String::new(),
                #[cfg(feature = "service-metrics")]
                build_info_claimed: false,
                registered_at: now_rfc3339(),
            })),
        }
    }

    /// Name the service the manifest describes, apart from its namespace.
    #[cfg(any(feature = "cli-service", test))]
    pub(crate) fn set_app_name(&self, app: &str) {
        if let Ok(mut inner) = self.inner.write() {
            inner.app = app.to_string();
        }
    }

    /// Drop the registration time, for a manifest generated offline: nothing
    /// was registered by a running service, and a time stamped per run would
    /// make every regenerated artefact differ.
    #[cfg(any(feature = "cli-service", test))]
    pub(crate) fn clear_registered_at(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.registered_at.clear();
        }
    }

    /// Push a metric descriptor into the registry.
    ///
    /// The descriptor's `name` is expected to be BARE (no namespace prefix).
    /// When the registry has a non-empty namespace, `{namespace}_` is prepended
    /// here -- the single place the manifest applies the namespace, mirroring
    /// the prefix layer on the global recorder so emitted and manifest names
    /// match.
    ///
    /// A name already held keeps its first descriptor: one name is one series,
    /// and the runtime describes its own metrics before the app does, so an app
    /// describing a platform metric again cannot strip its labels, group, use
    /// cases or dashboard hint.
    pub(crate) fn push(&self, mut descriptor: MetricDescriptor) {
        if let Ok(mut inner) = self.inner.write() {
            if !inner.namespace.is_empty() {
                descriptor.name = format!("{}_{}", inner.namespace, descriptor.name);
            }
            match inner
                .descriptors
                .iter()
                .find(|held| held.name == descriptor.name)
            {
                #[cfg_attr(not(feature = "logger"), allow(unused_variables))]
                Some(held) => {
                    #[cfg(feature = "logger")]
                    if held.metric_type != descriptor.metric_type
                        || held.labels != descriptor.labels
                        || held.group != descriptor.group
                    {
                        tracing::debug!(
                            metric = %descriptor.name,
                            "metric described again with a different type, labels or group, keeping the first"
                        );
                    }
                }
                None => inner.descriptors.push(descriptor),
            }
        }
    }

    /// Set the application version and commit.
    pub(crate) fn set_build_info(&self, version: &str, commit: &str) {
        if let Ok(mut inner) = self.inner.write() {
            inner.version = version.to_string();
            inner.commit = commit.to_string();
        }
    }

    /// Record the build info for the first app metric set built on this
    /// registry. Returns whether this call did, so that set alone emits it.
    #[cfg(feature = "service-metrics")]
    pub(crate) fn claim_build_info(&self, version: &str, commit: &str) -> bool {
        let Ok(mut inner) = self.inner.write() else {
            return false;
        };
        if inner.build_info_claimed {
            return false;
        }
        inner.build_info_claimed = true;
        inner.version = version.to_string();
        inner.commit = commit.to_string();
        true
    }

    /// Set use cases for a metric by BARE name. No-op if not found.
    ///
    /// Pass the bare metric name (no namespace prefix); `{namespace}_` is
    /// prepended here to match the stored descriptor name.
    pub(crate) fn set_use_cases(&self, metric_name: &str, use_cases: &[&str]) {
        if let Ok(mut inner) = self.inner.write() {
            let lookup = prefixed_lookup(&inner.namespace, metric_name);
            if let Some(desc) = inner.descriptors.iter_mut().find(|d| d.name == lookup) {
                desc.use_cases = use_cases.iter().map(|s| (*s).to_string()).collect();
            } else {
                #[cfg(feature = "logger")]
                tracing::warn!(
                    metric = lookup,
                    "set_use_cases: metric not found in registry"
                );
            }
        }
    }

    /// Set dashboard hint for a metric by BARE name. No-op if not found.
    ///
    /// Pass the bare metric name (no namespace prefix); `{namespace}_` is
    /// prepended here to match the stored descriptor name.
    pub(crate) fn set_dashboard_hint(&self, metric_name: &str, hint: &str) {
        if let Ok(mut inner) = self.inner.write() {
            let lookup = prefixed_lookup(&inner.namespace, metric_name);
            if let Some(desc) = inner.descriptors.iter_mut().find(|d| d.name == lookup) {
                desc.dashboard_hint = Some(hint.to_string());
            } else {
                #[cfg(feature = "logger")]
                tracing::warn!(
                    metric = lookup,
                    "set_dashboard_hint: metric not found in registry"
                );
            }
        }
    }

    /// Build the manifest response snapshot.
    #[must_use]
    pub fn manifest(&self) -> ManifestResponse {
        let inner = self.inner.read().expect("registry lock poisoned");
        ManifestResponse {
            schema_version: 1,
            app: inner.app.clone(),
            namespace: inner.namespace.clone(),
            version: inner.version.clone(),
            commit: inner.commit.clone(),
            registered_at: inner.registered_at.clone(),
            metrics: inner.descriptors.clone(),
        }
    }
}

/// Prepend `{namespace}_` to a bare metric name, or return it unchanged when
/// `namespace` is empty. Used to translate bare lookup keys into stored
/// descriptor names.
fn prefixed_lookup(namespace: &str, bare: &str) -> String {
    if namespace.is_empty() {
        bare.to_string()
    } else {
        format!("{namespace}_{bare}")
    }
}

/// Current UTC time as RFC 3339, second precision. Output: `2026-03-31T02:00:00Z`.
pub(crate) fn now_rfc3339() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_secs = d.as_secs();

    #[allow(clippy::cast_possible_wrap)]
    let days = (total_secs / 86400) as i64;
    let time_of_day = total_secs % 86400;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    #[allow(clippy::cast_possible_wrap)]
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_type_serializes_to_snake_case() {
        assert_eq!(
            serde_json::to_string(&MetricType::Counter).unwrap(),
            "\"counter\""
        );
        assert_eq!(
            serde_json::to_string(&MetricType::Gauge).unwrap(),
            "\"gauge\""
        );
        assert_eq!(
            serde_json::to_string(&MetricType::Histogram).unwrap(),
            "\"histogram\""
        );
    }

    #[test]
    fn test_metric_descriptor_serializes_type_as_type() {
        let desc = MetricDescriptor {
            name: "test_total".into(),
            metric_type: MetricType::Counter,
            description: "A test counter".into(),
            unit: String::new(),
            labels: vec![],
            group: "custom".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        };
        let json = serde_json::to_value(&desc).unwrap();
        assert_eq!(json["type"], "counter");
        assert!(json.get("metric_type").is_none());
    }

    #[test]
    fn test_empty_use_cases_omitted_from_json() {
        let desc = MetricDescriptor {
            name: "test_gauge".into(),
            metric_type: MetricType::Gauge,
            description: "A gauge".into(),
            unit: String::new(),
            labels: vec![],
            group: "custom".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        };
        let json = serde_json::to_value(&desc).unwrap();
        assert!(json.get("use_cases").is_none());
        assert!(json.get("buckets").is_none());
        assert!(json.get("dashboard_hint").is_none());
    }

    #[test]
    fn test_populated_use_cases_included() {
        let desc = MetricDescriptor {
            name: "test_hist".into(),
            metric_type: MetricType::Histogram,
            description: "A histogram".into(),
            unit: "seconds".into(),
            labels: vec!["backend".into()],
            group: "sink".into(),
            buckets: Some(vec![0.01, 0.1, 1.0]),
            use_cases: vec!["Alert when p99 > 5s".into()],
            dashboard_hint: Some("heatmap".into()),
        };
        let json = serde_json::to_value(&desc).unwrap();
        assert_eq!(
            json["use_cases"],
            serde_json::json!(["Alert when p99 > 5s"])
        );
        assert_eq!(json["buckets"], serde_json::json!([0.01, 0.1, 1.0]));
        assert_eq!(json["dashboard_hint"], "heatmap");
    }

    #[test]
    fn test_manifest_response_round_trips() {
        let manifest = ManifestResponse {
            schema_version: 1,
            app: "test_app".into(),
            namespace: String::new(),
            version: "1.0.0".into(),
            commit: "abc123".into(),
            registered_at: "2026-03-31T00:00:00Z".into(),
            metrics: vec![MetricDescriptor {
                name: "test_total".into(),
                metric_type: MetricType::Counter,
                description: "test".into(),
                unit: String::new(),
                labels: vec![],
                group: "custom".into(),
                buckets: None,
                use_cases: vec![],
                dashboard_hint: None,
            }],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: ManifestResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.schema_version, 1);
        assert_eq!(parsed.app, "test_app");
        assert_eq!(parsed.metrics.len(), 1);
        assert_eq!(parsed.metrics[0].metric_type, MetricType::Counter);
    }

    #[test]
    fn test_counter_unit_is_empty_not_total() {
        let desc = MetricDescriptor {
            name: "requests_total".into(),
            metric_type: MetricType::Counter,
            description: "Requests".into(),
            unit: String::new(),
            labels: vec![],
            group: "custom".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        };
        let json = serde_json::to_value(&desc).unwrap();
        assert_eq!(json["unit"], "");
    }

    #[test]
    fn test_now_rfc3339_format() {
        let ts = now_rfc3339();
        assert_eq!(ts.len(), 20);
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], "T");
        assert_eq!(&ts[13..14], ":");
        assert_eq!(&ts[16..17], ":");
    }

    #[test]
    fn test_registry_push_and_manifest() {
        let reg = MetricRegistry::new("test_app");
        // Push a BARE name; the registry applies the `{app}_` prefix.
        reg.push(MetricDescriptor {
            name: "requests_total".into(),
            metric_type: MetricType::Counter,
            description: "Total requests".into(),
            unit: String::new(),
            labels: vec!["method".into()],
            group: "app".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        let manifest = reg.manifest();
        assert_eq!(manifest.app, "test_app");
        assert_eq!(manifest.schema_version, 1);
        assert_eq!(manifest.metrics.len(), 1);
        assert_eq!(manifest.metrics[0].name, "test_app_requests_total");
        assert_eq!(manifest.metrics[0].labels, vec!["method"]);
    }

    #[test]
    fn test_registry_push_bare_namespace_is_bare() {
        // Empty namespace -> names stay bare.
        let reg = MetricRegistry::new("");
        reg.push(MetricDescriptor {
            name: "transport_sent_total".into(),
            metric_type: MetricType::Counter,
            description: "sent".into(),
            unit: String::new(),
            labels: vec![],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        let manifest = reg.manifest();
        assert_eq!(manifest.metrics[0].name, "transport_sent_total");
    }

    #[test]
    fn test_registry_push_applies_namespace_prefix() {
        // Non-empty namespace -> single `{app}_` prefix on the manifest name.
        let reg = MetricRegistry::new("acme");
        reg.push(MetricDescriptor {
            name: "transport_sent_total".into(),
            metric_type: MetricType::Counter,
            description: "sent".into(),
            unit: String::new(),
            labels: vec![],
            group: "platform".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        let manifest = reg.manifest();
        assert_eq!(manifest.metrics[0].name, "acme_transport_sent_total");
    }

    fn counter(name: &str, labels: &[&str], group: &str) -> MetricDescriptor {
        MetricDescriptor {
            name: name.into(),
            metric_type: MetricType::Counter,
            description: format!("{group} {name}"),
            unit: String::new(),
            labels: labels.iter().map(|l| (*l).to_string()).collect(),
            group: group.into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        }
    }

    #[test]
    fn test_registry_push_of_a_held_name_keeps_the_first() {
        for namespace in ["", "acme"] {
            let reg = MetricRegistry::new(namespace);
            let mut canonical = counter("records_received_total", &["source"], "platform");
            canonical.use_cases = vec!["Alert when it stops rising".into()];
            canonical.dashboard_hint = Some("stat".into());
            reg.push(canonical);
            reg.push(counter("records_dlq_total", &[], "platform"));
            reg.push(counter("records_received_total", &[], "custom"));

            let manifest = reg.manifest();
            let name = prefixed_lookup(namespace, "records_received_total");
            let held: Vec<&MetricDescriptor> =
                manifest.metrics.iter().filter(|m| m.name == name).collect();
            assert_eq!(held.len(), 1, "{name} listed once under `{namespace}`");
            assert_eq!(held[0].group, "platform", "the first descriptor stands");
            assert_eq!(held[0].labels, vec!["source"]);
            assert_eq!(held[0].use_cases, vec!["Alert when it stops rising"]);
            assert_eq!(held[0].dashboard_hint.as_deref(), Some("stat"));
            assert_eq!(
                manifest.metrics.len(),
                2,
                "a second description is not an addition"
            );
            assert_eq!(manifest.metrics[0].name, name, "and keeps its place");
        }
    }

    #[test]
    fn test_an_offline_registry_carries_no_registration_time() {
        let live = MetricRegistry::new("");
        assert_eq!(
            live.manifest().registered_at.len(),
            20,
            "a live registry is stamped"
        );

        let offline = MetricRegistry::new("");
        offline.clear_registered_at();
        assert_eq!(offline.manifest().registered_at, "");
    }

    #[test]
    fn test_registry_names_the_app_apart_from_the_namespace() {
        let reg = MetricRegistry::new("acme");
        reg.set_app_name("my-service");
        reg.push(counter("records_dlq_total", &[], "platform"));

        let manifest = reg.manifest();

        assert_eq!(manifest.app, "my-service");
        assert_eq!(manifest.namespace, "acme");
        assert_eq!(manifest.metrics[0].name, "acme_records_dlq_total");
    }

    #[test]
    fn test_manifest_without_a_namespace_field_still_parses() {
        let parsed: ManifestResponse = serde_json::from_str(
            r#"{"schema_version":1,"app":"a","version":"","commit":"","registered_at":"","metrics":[]}"#,
        )
        .unwrap();
        assert!(parsed.namespace.is_empty());
    }

    #[test]
    fn test_registry_set_build_info() {
        let reg = MetricRegistry::new("test_app");
        reg.set_build_info("2.0.0", "def456");
        let manifest = reg.manifest();
        assert_eq!(manifest.version, "2.0.0");
        assert_eq!(manifest.commit, "def456");
    }

    #[test]
    fn test_registry_set_use_cases() {
        let reg = MetricRegistry::new("test_app");
        reg.push(MetricDescriptor {
            name: "my_metric".into(),
            metric_type: MetricType::Gauge,
            description: "test".into(),
            unit: String::new(),
            labels: vec![],
            group: "custom".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        reg.set_use_cases("my_metric", &["Alert when > 90%"]);
        let manifest = reg.manifest();
        assert_eq!(manifest.metrics[0].use_cases, vec!["Alert when > 90%"]);
    }

    #[test]
    fn test_registry_set_use_cases_nonexistent_is_noop() {
        let reg = MetricRegistry::new("test_app");
        // Should not panic
        reg.set_use_cases("nonexistent", &["some use case"]);
    }

    #[test]
    fn test_registry_set_dashboard_hint() {
        let reg = MetricRegistry::new("test_app");
        reg.push(MetricDescriptor {
            name: "my_metric".into(),
            metric_type: MetricType::Gauge,
            description: "test".into(),
            unit: String::new(),
            labels: vec![],
            group: "custom".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });
        reg.set_dashboard_hint("my_metric", "stat");
        let manifest = reg.manifest();
        assert_eq!(manifest.metrics[0].dashboard_hint, Some("stat".to_string()));
    }

    #[test]
    fn test_group_always_present_in_json() {
        let desc = MetricDescriptor {
            name: "test".into(),
            metric_type: MetricType::Counter,
            description: "test".into(),
            unit: String::new(),
            labels: vec![],
            group: "custom".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        };
        let json = serde_json::to_value(&desc).unwrap();
        assert_eq!(json["group"], "custom");
    }
}
