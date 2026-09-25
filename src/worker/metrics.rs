// Project:   scalo
// File:      src/worker/metrics.rs
// Purpose:   Metric registration and threshold gauge emission
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use crate::metrics::{MetricDescriptor, MetricType, MetricsManager};

use super::config::WorkerPoolConfig;

/// Register all worker pool metrics with the `MetricsManager`.
///
/// Threshold gauges are emitted immediately with current config values.
pub fn register(manager: &MetricsManager, config: &WorkerPoolConfig) {
    describe(manager);
    emit_thresholds(config);
}

/// Describe the worker pool's operational metrics into the manager's manifest
/// registry, with no pool and no config needed.
pub fn describe(manager: &MetricsManager) {
    // Return values intentionally unused: only the description is wanted.
    let _ = manager.gauge(
        "worker_pool_active_threads",
        "Permits currently leased (in-flight worker count)",
    );
    let _ = manager.gauge(
        "worker_pool_target_threads",
        "Target thread count from scaler (admission ceiling)",
    );
    let _ = manager.gauge(
        "worker_pool_available_threads",
        "Headroom: leasable permits right now (target - leased)",
    );
    let _ = manager.gauge("worker_pool_max_threads", "Maximum pool threads");
    let _ = manager.gauge(
        "worker_pool_cpu_utilisation",
        "Current CPU utilisation sample",
    );
    let _ = manager.gauge(
        "worker_pool_memory_utilisation",
        "Effective memory pressure",
    );
    let _ = manager.gauge(
        "worker_pool_saturation",
        "Pool saturation ratio (active/max)",
    );
    let _ = manager.counter(
        "worker_pool_tasks_total",
        "Total tasks submitted to rayon pool",
    );
    let _ = manager.histogram(
        "worker_pool_task_duration_seconds",
        "Per-task execution time",
    );
    let _ = manager.histogram(
        "worker_pool_batch_duration_seconds",
        "End-to-end batch processing time",
    );
    let _ = manager.histogram(
        "worker_pool_semaphore_wait_seconds",
        "Time waiting for semaphore permit",
    );
    describe_scale_events(manager);
    let _ = manager.gauge(
        "worker_pool_async_inflight",
        "Current async fan-out tasks in flight",
    );
}

/// Describe `worker_pool_scale_events_total` with its `direction` label.
///
/// The scaler emits it only with a direction, so this registers no series:
/// a handle from `manager.counter` would add an unlabelled one stuck at 0.
fn describe_scale_events(manager: &MetricsManager) {
    const NAME: &str = "worker_pool_scale_events_total";
    const DESCRIPTION: &str = "Scaling events";
    metrics::describe_counter!(NAME, DESCRIPTION);
    manager.registry().push(MetricDescriptor {
        name: NAME.into(),
        metric_type: MetricType::Counter,
        description: DESCRIPTION.into(),
        unit: String::new(),
        labels: vec!["direction".into()],
        group: "custom".into(),
        buckets: None,
        use_cases: vec![],
        dashboard_hint: None,
    });
}

/// Emit threshold gauge values (called at startup and on config reload).
///
/// Metric names match config keys exactly for mechanical derivation:
/// config key `grow_below` -> metric `worker_pool_grow_below`.
pub fn emit_thresholds(config: &WorkerPoolConfig) {
    metrics::gauge!("worker_pool_min_threads").set(config.min_threads as f64);
    metrics::gauge!("worker_pool_max_threads").set(config.max_threads as f64);
    metrics::gauge!("worker_pool_grow_below").set(config.grow_below);
    metrics::gauge!("worker_pool_shrink_above").set(config.shrink_above);
    metrics::gauge!("worker_pool_emergency_above").set(config.emergency_above);
    metrics::gauge!("worker_pool_memory_pressure_cap").set(config.memory_pressure_cap);
    metrics::gauge!("worker_pool_scale_interval_secs").set(config.scale_interval_secs as f64);
    metrics::gauge!("worker_pool_async_concurrency").set(config.async_concurrency as f64);
    metrics::gauge!("worker_pool_health_saturation_timeout_secs")
        .set(config.health_saturation_timeout_secs as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(manager: &MetricsManager) -> Vec<String> {
        let mut names: Vec<String> = manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|m| m.name)
            .collect();
        names.sort();
        names
    }

    #[test]
    fn describe_lists_every_name_register_does() {
        let described = MetricsManager::new_for_test("");
        describe(&described);
        let registered = MetricsManager::new_for_test("");
        register(&registered, &WorkerPoolConfig::default());

        let mut expected = vec![
            "worker_pool_active_threads",
            "worker_pool_target_threads",
            "worker_pool_available_threads",
            "worker_pool_max_threads",
            "worker_pool_cpu_utilisation",
            "worker_pool_memory_utilisation",
            "worker_pool_saturation",
            "worker_pool_tasks_total",
            "worker_pool_task_duration_seconds",
            "worker_pool_batch_duration_seconds",
            "worker_pool_semaphore_wait_seconds",
            "worker_pool_scale_events_total",
            "worker_pool_async_inflight",
        ];
        expected.sort_unstable();
        assert_eq!(names(&described), expected);
        assert_eq!(names(&registered), expected);
    }

    /// Scale events are emitted only with their `direction`, so describing
    /// them adds no unlabelled series beside the labelled ones.
    #[test]
    fn scale_events_are_described_with_their_label_and_no_bare_series() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let manager = MetricsManager::new_for_test("");

        describe(&manager);
        metrics::counter!("worker_pool_scale_events_total", "direction" => "up").increment(1);

        let rendered = handle.render();
        let series: Vec<&str> = rendered
            .lines()
            .filter(|line| line.starts_with("worker_pool_scale_events_total"))
            .collect();
        assert_eq!(
            series,
            vec!["worker_pool_scale_events_total{direction=\"up\"} 1"],
            "{rendered}"
        );
        let manifest = manager.registry().manifest();
        let described = manifest
            .metrics
            .iter()
            .find(|m| m.name == "worker_pool_scale_events_total")
            .expect("described");
        assert_eq!(described.labels, vec!["direction".to_string()]);
    }
}
