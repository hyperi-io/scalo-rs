// Project:   scalo
// File:      src/metrics/groups/schema_cache.rs
// Purpose:   Pipeline schema cache metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Schema cache metrics for apps with dynamic schema reflection.

use metrics::{Counter, Gauge};

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

/// Schema cache metrics.
///
/// Tracks cache hit/miss rates, cached table count, and schema recovery events.
#[derive(Clone)]
pub struct SchemaCacheMetrics {
    pub hits: Counter,
    pub misses: Counter,
    pub tables: Gauge,
}

impl SchemaCacheMetrics {
    #[must_use]
    pub fn new(manager: &MetricsManager) -> Self {
        // BARE names -- the recorder prefix layer and registry apply the namespace.

        // schema_recovery_total -- label-based, register descriptor manually
        metrics::describe_counter!("schema_recovery_total", "Schema mismatch recovery events");
        manager.registry().push(MetricDescriptor {
            name: "schema_recovery_total".into(),
            metric_type: MetricType::Counter,
            description: "Schema mismatch recovery events".into(),
            unit: String::new(),
            labels: vec!["table".into()],
            group: "schema_cache".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        Self {
            hits: manager.counter_with_labels(
                "schema_cache_hits_total",
                "Schema cache hits",
                &[],
                "schema_cache",
            ),
            misses: manager.counter_with_labels(
                "schema_cache_misses_total",
                "Schema cache misses (triggers fetch)",
                &[],
                "schema_cache",
            ),
            tables: manager.gauge_with_labels(
                "schema_cache_tables",
                "Number of cached table schemas",
                &[],
                "schema_cache",
            ),
        }
    }

    #[inline]
    pub fn record_hit(&self) {
        self.hits.increment(1);
    }

    #[inline]
    pub fn record_miss(&self) {
        self.misses.increment(1);
    }

    #[inline]
    pub fn set_tables(&self, count: usize) {
        self.tables.set(count as f64);
    }

    /// Record a schema recovery event for a specific table.
    #[inline]
    pub fn record_recovery(&self, table: &str) {
        metrics::counter!("schema_recovery_total", "table" => table.to_string()).increment(1);
    }
}
