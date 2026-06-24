// Project:   scalo
// File:      src/metrics/groups/enrichment.rs
// Purpose:   DFE enrichment metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Enrichment cache metrics (GeoIP, reputation, lookup tables).

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

/// Enrichment cache metrics.
///
/// Tracks cache hit/miss rates, cache size, and lookup latency.
/// The `type` label distinguishes enrichment sources (e.g., `geoip`, `reputation`).
#[derive(Clone)]
pub struct EnrichmentMetrics {
    _private: (),
}

impl EnrichmentMetrics {
    #[must_use]
    pub fn new(manager: &MetricsManager) -> Self {
        // BARE names -- the recorder prefix layer and registry apply the namespace.

        // enrichment_cache_hits_total -- label-based
        metrics::describe_counter!("enrichment_cache_hits_total", "Enrichment cache hits");
        manager.registry().push(MetricDescriptor {
            name: "enrichment_cache_hits_total".into(),
            metric_type: MetricType::Counter,
            description: "Enrichment cache hits".into(),
            unit: String::new(),
            labels: vec!["type".into()],
            group: "enrichment".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // enrichment_cache_misses_total -- label-based
        metrics::describe_counter!("enrichment_cache_misses_total", "Enrichment cache misses");
        manager.registry().push(MetricDescriptor {
            name: "enrichment_cache_misses_total".into(),
            metric_type: MetricType::Counter,
            description: "Enrichment cache misses".into(),
            unit: String::new(),
            labels: vec!["type".into()],
            group: "enrichment".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // enrichment_cache_size -- label-based
        metrics::describe_gauge!("enrichment_cache_size", "Current enrichment cache entries");
        manager.registry().push(MetricDescriptor {
            name: "enrichment_cache_size".into(),
            metric_type: MetricType::Gauge,
            description: "Current enrichment cache entries".into(),
            unit: String::new(),
            labels: vec!["type".into()],
            group: "enrichment".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // enrichment_duration_seconds -- label-based
        metrics::describe_histogram!(
            "enrichment_duration_seconds",
            metrics::Unit::Seconds,
            "Enrichment lookup latency"
        );
        manager.registry().push(MetricDescriptor {
            name: "enrichment_duration_seconds".into(),
            metric_type: MetricType::Histogram,
            description: "Enrichment lookup latency".into(),
            unit: "seconds".into(),
            labels: vec!["type".into()],
            group: "enrichment".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        Self { _private: () }
    }

    #[inline]
    pub fn record_hit(&self, enrichment_type: &str) {
        metrics::counter!("enrichment_cache_hits_total", "type" => enrichment_type.to_string())
            .increment(1);
    }

    #[inline]
    pub fn record_miss(&self, enrichment_type: &str) {
        metrics::counter!("enrichment_cache_misses_total", "type" => enrichment_type.to_string())
            .increment(1);
    }

    #[inline]
    pub fn set_cache_size(&self, enrichment_type: &str, size: usize) {
        metrics::gauge!("enrichment_cache_size", "type" => enrichment_type.to_string())
            .set(size as f64);
    }

    #[inline]
    pub fn record_duration(&self, enrichment_type: &str, seconds: f64) {
        metrics::histogram!("enrichment_duration_seconds", "type" => enrichment_type.to_string())
            .record(seconds);
    }
}
