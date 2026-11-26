// Project:   scalo
// File:      src/worker/engine/metrics.rs
// Purpose:   Metric registration and threshold gauge emission for BatchEngine
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use crate::metrics::MetricsManager;

use super::config::BatchProcessingConfig;

/// Register all `BatchEngine` metrics with the `MetricsManager`.
///
/// Called by [`BatchEngine::auto_wire`](super::BatchEngine::auto_wire). Registers
/// descriptors for all operational metrics and immediately emits the current
/// config thresholds as gauges (for Grafana overlay of scaling decision lines).
pub fn register(manager: &MetricsManager, config: &BatchProcessingConfig) {
    describe(manager);
    emit_thresholds(config);
}

/// Describe the batch engine's operational metrics, and the received block size
/// when `governor` is on, into the manager's manifest registry, with no engine
/// and no config needed.
pub fn describe(manager: &MetricsManager) {
    // Counters
    let _ = manager.counter(
        "batch_engine_messages_received_total",
        "Messages received from transport",
    );
    let _ = manager.counter(
        "batch_engine_messages_parsed_total",
        "Messages successfully SIMD-parsed",
    );
    let _ = manager.counter(
        "batch_engine_messages_filtered_total",
        "Messages filtered at pre-route",
    );
    let _ = manager.counter(
        "engine_filter_dlq_discarded_total",
        "DLQ entries discarded under FilterDlqPolicy::DiscardWithMetric",
    );
    let _ = manager.gauge(
        "batch_engine_max_chunk_size",
        "Configured batch_processing.max_chunk_size",
    );

    // The one governor series the engine's driver writes. The governor's own
    // are described with the other library series.
    #[cfg(feature = "governor")]
    let _ = manager.gauge(
        "self_regulation_recv_block_bytes",
        "Bytes in the most recent received block (vs the byte budget)",
    );
}

/// Count one chunk the engine processed, once per chunk. `parsed` is `None`
/// on the raw path, which parses nothing.
#[cfg(feature = "transport")]
pub(crate) fn count_chunk(received: usize, parsed: Option<u64>, filtered: u64) {
    metrics::counter!("batch_engine_messages_received_total").increment(received as u64);
    if let Some(parsed) = parsed {
        metrics::counter!("batch_engine_messages_parsed_total").increment(parsed);
    }
    metrics::counter!("batch_engine_messages_filtered_total").increment(filtered);
}

/// Emit config threshold values as gauge metrics.
///
/// Called at startup (via `register`) and optionally on config reload.
/// Metric names are mechanically derived from config field names so that
/// Grafana dashboards can overlay config changes on operational graphs.
pub fn emit_thresholds(config: &BatchProcessingConfig) {
    metrics::gauge!("batch_engine_max_chunk_size").set(config.max_chunk_size as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_does_not_panic() {
        let manager = MetricsManager::new_for_test("test_engine_metrics");
        let config = BatchProcessingConfig::default();
        // Should complete without panic even with no recorder installed.
        register(&manager, &config);
    }

    #[test]
    fn emit_thresholds_does_not_panic() {
        let config = BatchProcessingConfig::default();
        // metrics macros are no-ops when no recorder is installed.
        emit_thresholds(&config);
    }

    #[test]
    fn describe_lists_every_name_register_does() {
        let names = |manager: &MetricsManager| {
            let mut names: Vec<String> = manager
                .registry()
                .manifest()
                .metrics
                .into_iter()
                .map(|m| m.name)
                .collect();
            names.sort();
            names
        };
        let described = MetricsManager::new_for_test("");
        describe(&described);
        let registered = MetricsManager::new_for_test("");
        register(&registered, &BatchProcessingConfig::default());

        let mut expected = vec![
            "batch_engine_messages_received_total",
            "batch_engine_messages_parsed_total",
            "batch_engine_messages_filtered_total",
            "engine_filter_dlq_discarded_total",
            "batch_engine_max_chunk_size",
        ];
        #[cfg(feature = "governor")]
        expected.push("self_regulation_recv_block_bytes");
        expected.sort_unstable();
        assert_eq!(names(&described), expected);
        assert_eq!(names(&registered), expected);
    }

    #[test]
    fn register_returns_handles() {
        let manager = MetricsManager::new_for_test("test_engine_metrics_handles");
        let config = BatchProcessingConfig::default();
        // Calling twice should be idempotent.
        register(&manager, &config);
        register(&manager, &config);
    }
}
