// Project:   scalo
// File:      src/metrics/groups/app.rs
// Purpose:   Mandatory app-level pipeline metrics
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Mandatory app-level metrics for every pipeline application.

use metrics::{Counter, Gauge};

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

/// Mandatory metrics for every pipeline application.
///
/// Registers `info`, `start_time_seconds`, record counters, byte counters,
/// memory gauges, and config reload counter, under bare names or the
/// `MetricsManager` namespace prefix.
///
/// `records_received_total` is one series shared with
/// [`ServiceMetrics::records_received`](crate::metrics::ServiceMetrics::records_received),
/// which owns it: one name with no labels is one series, so counting a record
/// through both adds it twice.
#[derive(Clone)]
pub struct AppMetrics {
    /// Handle on `records_received_total`, the series
    /// [`ServiceMetrics::records_received`](crate::metrics::ServiceMetrics::records_received)
    /// counts. For an app that sets the total rather than incrementing it; an
    /// app that increments counts through `ServiceMetrics` alone.
    pub records_received: Counter,
    pub records_processed: Counter,
    pub records_error: Counter,
    pub bytes_received: Counter,
    pub bytes_written: Counter,
    pub memory_used_bytes: Gauge,
    pub memory_limit_bytes: Gauge,
    pub config_reloads_success: Counter,
    pub config_reloads_error: Counter,
}

impl AppMetrics {
    /// Create and register app metrics.
    ///
    /// `version` and `commit` are emitted as labels on the `info` gauge by the
    /// first set built on `manager`, which in a service is the runtime's; a
    /// later set leaves `info` and the manifest's build info as they are.
    #[must_use]
    pub fn new(manager: &MetricsManager, version: &str, commit: &str) -> Self {
        // Info metric for service discovery. Names are BARE -- the prefix layer
        // on the global recorder and the registry apply the namespace.
        metrics::describe_gauge!("info", "Application info for service discovery");
        if manager.registry().claim_build_info(version, commit) {
            metrics::gauge!(
                "info",
                "version" => version.to_string(),
                "commit" => commit.to_string()
            )
            .set(1.0);
        }
        manager.registry().push(MetricDescriptor {
            name: "info".into(),
            metric_type: MetricType::Gauge,
            description: "Application info for service discovery".into(),
            unit: String::new(),
            labels: vec!["version".into(), "commit".into()],
            group: "app".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // Start time
        let start_time = manager.gauge_with_labels(
            "start_time_seconds",
            "Unix timestamp of process start",
            &[],
            "app",
        );
        start_time.set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64()),
        );

        // config_reloads_total -- label-based, register descriptor manually.
        // BARE name; the recorder's prefix layer adds the namespace at emit time.
        let config_key = "config_reloads_total";
        metrics::describe_counter!(config_key, "Config reload attempts");
        manager.registry().push(MetricDescriptor {
            name: config_key.into(),
            metric_type: MetricType::Counter,
            description: "Config reload attempts".into(),
            unit: String::new(),
            labels: vec!["result".into()],
            group: "app".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        Self {
            records_received: manager.counter_with_labels(
                "records_received_total",
                "Records received from source",
                &[],
                "app",
            ),
            records_processed: manager.counter_with_labels(
                "records_processed_total",
                "Records successfully processed",
                &[],
                "app",
            ),
            records_error: manager.counter_with_labels(
                "records_error_total",
                "Records that failed processing",
                &[],
                "app",
            ),
            bytes_received: manager.counter_with_labels(
                "bytes_received_total",
                "Bytes received from source",
                &[],
                "app",
            ),
            bytes_written: manager.counter_with_labels(
                "bytes_written_total",
                "Bytes written to sink",
                &[],
                "app",
            ),
            memory_used_bytes: manager.gauge_with_labels(
                "memory_used_bytes",
                "Current memory usage (cgroup-aware)",
                &[],
                "app",
            ),
            memory_limit_bytes: manager.gauge_with_labels(
                "memory_limit_bytes",
                "Effective memory limit",
                &[],
                "app",
            ),
            config_reloads_success: metrics::counter!(config_key, "result" => "success"),
            config_reloads_error: metrics::counter!(config_key, "result" => "error"),
        }
    }

    /// Add `count` to `records_received_total`.
    ///
    /// The series is the one
    /// [`ServiceMetrics::records_received`](crate::metrics::ServiceMetrics::records_received)
    /// counts, so an app calling both counts every record twice. Call one.
    #[inline]
    pub fn record_received(&self, count: u64) {
        self.records_received.increment(count);
    }

    #[inline]
    pub fn record_processed(&self, count: u64) {
        self.records_processed.increment(count);
    }

    #[inline]
    pub fn record_error(&self, count: u64) {
        self.records_error.increment(count);
    }

    #[inline]
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.increment(bytes);
    }

    #[inline]
    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.increment(bytes);
    }

    #[inline]
    pub fn set_memory(&self, used: u64, limit: u64) {
        self.memory_used_bytes.set(used as f64);
        self.memory_limit_bytes.set(limit as f64);
    }

    #[inline]
    pub fn record_config_reload(&self, success: bool) {
        if success {
            self.config_reloads_success.increment(1);
        } else {
            self.config_reloads_error.increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};

    use super::*;
    use crate::metrics::ServiceMetrics;

    /// Keeps every counter by its key, name and labels, as Prometheus keys a series.
    #[derive(Default)]
    struct SeriesCapture {
        counters: Mutex<HashMap<Key, Arc<AtomicU64>>>,
    }

    impl SeriesCapture {
        /// Every series named `name`, with its value.
        fn series(&self, name: &str) -> Vec<(Key, u64)> {
            self.counters
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.name() == name)
                .map(|(key, cell)| (key.clone(), cell.load(Ordering::Acquire)))
                .collect()
        }
    }

    impl Recorder for SeriesCapture {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> metrics::Counter {
            let cell = Arc::clone(
                self.counters
                    .lock()
                    .unwrap()
                    .entry(key.clone())
                    .or_default(),
            );
            metrics::Counter::from_arc(cell)
        }

        fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// The runtime set a service gets, built against `capture`.
    fn runtime_set(capture: &SeriesCapture) -> (ServiceMetrics, AppMetrics) {
        let manager = MetricsManager::new_for_test("");
        metrics::with_local_recorder(capture, || {
            (
                ServiceMetrics::register(&manager),
                AppMetrics::new(&manager, "1.2.3", "abc"),
            )
        })
    }

    #[test]
    fn records_counted_through_the_owner_read_once_each() {
        let capture = SeriesCapture::default();
        let (svc, _app) = runtime_set(&capture);

        metrics::with_local_recorder(&capture, || {
            for _ in 0..7 {
                svc.records_received(1);
            }
        });

        let series = capture.series("records_received_total");
        assert_eq!(series.len(), 1, "one name, one series: {series:?}");
        assert_eq!(series[0].1, 7, "seven records read as seven");
    }

    #[test]
    fn the_app_group_handle_writes_the_owners_series() {
        let capture = SeriesCapture::default();
        let (svc, app) = runtime_set(&capture);

        metrics::with_local_recorder(&capture, || svc.records_received(3));
        app.records_received.increment(2);

        let series = capture.series("records_received_total");
        assert_eq!(
            series.len(),
            1,
            "both handles name one unlabelled series: {series:?}"
        );
        assert_eq!(
            series[0].1, 5,
            "so a record counted through both reads twice"
        );
    }

    #[test]
    fn the_app_group_handle_sets_the_total_for_an_app_that_mirrors_one() {
        let capture = SeriesCapture::default();
        let (_svc, app) = runtime_set(&capture);

        app.records_received.absolute(85);
        app.records_received.absolute(84);

        assert_eq!(
            capture.series("records_received_total")[0].1,
            85,
            "absolute keeps the running maximum"
        );
    }

    /// The runtime builds the app set first; an app building it again adds no
    /// second `info` series and leaves the build info the runtime set.
    #[test]
    fn a_second_app_set_emits_no_second_info_series() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let manager = MetricsManager::new_for_test("");

        let _runtime = AppMetrics::new(&manager, "1.2.3", "abc1234");
        let _app = AppMetrics::new(&manager, "1.2.3", "dev");

        let rendered = handle.render();
        let info: Vec<&str> = rendered
            .lines()
            .filter(|line| line.starts_with("info{"))
            .collect();
        assert_eq!(info.len(), 1, "one info series:\n{rendered}");
        assert!(info[0].contains("commit=\"abc1234\""), "{}", info[0]);
        assert_eq!(manager.registry().manifest().commit, "abc1234");
    }
}
