// Project:   scalo
// File:      src/metrics/groups/enrichment.rs
// Purpose:   Enrichment cache metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Enrichment cache metrics (GeoIP, reputation, lookup tables).

use super::super::MetricsManager;
use super::super::Series;

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

        let by_type = ["type"];
        manager.registry().describe(
            "enrichment",
            &[
                Series::counter("enrichment_cache_hits_total", "Enrichment cache hits")
                    .labels(&by_type),
                Series::counter("enrichment_cache_misses_total", "Enrichment cache misses")
                    .labels(&by_type),
                Series::gauge("enrichment_cache_size", "Current enrichment cache entries")
                    .labels(&by_type),
                Series::histogram("enrichment_duration_seconds", "Enrichment lookup latency")
                    .labels(&by_type),
            ],
        );

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
