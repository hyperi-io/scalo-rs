// Project:   scalo
// File:      src/metrics/groups/usage.rs
// Purpose:   Per-org ingest usage metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Ingest usage per org, for an app that meters what each tenant sends.

/// Payload bytes accepted for delivery, by org.
///
/// The catalogue describes `usage_ingested_bytes_total{org_id}` when the manager
/// is built, so the group has nothing to register.
#[derive(Clone, Copy, Debug, Default)]
pub struct UsageMetrics;

impl UsageMetrics {
    /// Count `bytes` for `org` in a batch that has settled as taken.
    ///
    /// `bytes` is the payload after any sender `_org_id` is stripped and before the
    /// app stamps its header, and `org` is the `_org_id` the app stamped. A batch
    /// that is dead-lettered, dropped or aborted is not passed here.
    #[inline]
    pub fn record(&self, org: &str, bytes: u64) {
        metrics::counter!("usage_ingested_bytes_total", "org_id" => org.to_string())
            .increment(bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::UsageMetrics;
    use crate::metrics::MetricsManager;
    use crate::metrics::catalogue::describe_library;

    /// `record` sums the bytes per org into `usage_ingested_bytes_total{org_id}`.
    #[test]
    fn record_sums_bytes_per_org() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let usage = UsageMetrics;
            usage.record("acme", 10);
            usage.record("beta", 7);
            usage.record("acme", 5);
        });

        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let key = key.key();
            if key.name() != "usage_ingested_bytes_total" {
                continue;
            }
            let org = key
                .labels()
                .find(|label| label.key() == "org_id")
                .map(|label| label.value().to_string())
                .expect("org_id label");
            let DebugValue::Counter(bytes) = value else {
                panic!("usage_ingested_bytes_total is not a counter");
            };
            totals.insert(org, bytes);
        }
        assert_eq!(
            totals,
            BTreeMap::from([("acme".to_string(), 15), ("beta".to_string(), 7)])
        );
    }

    /// The catalogue describes the series with its label, HELP text and group.
    #[test]
    fn the_catalogue_describes_the_series_with_its_org_label() {
        let manager = MetricsManager::new_for_test("");
        describe_library(&manager.registry());
        let manifest = manager.registry().manifest();
        let found = manifest
            .metrics
            .iter()
            .find(|m| m.name == "usage_ingested_bytes_total")
            .expect("usage_ingested_bytes_total is in the manifest");
        assert_eq!(
            found.description,
            "Payload bytes accepted for delivery, by the org each record was stamped with"
        );
        assert_eq!(found.labels, ["org_id"]);
        assert_eq!(found.group, "usage");
    }
}
