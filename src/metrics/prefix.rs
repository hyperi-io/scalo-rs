// Project:   scalo
// File:      src/metrics/prefix.rs
// Purpose:   Underscore-joining namespace prefix layer for the global recorder
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Namespace prefix layer.
//!
//! Applies a single `{namespace}_` prefix to every metric key and describe
//! call as they pass through the global recorder. This is the ONE place
//! namespacing happens for emitted metrics: library-internal metrics,
//! [`ServiceMetrics`](super::ServiceMetrics), the scattered
//! `metrics::counter!(...)` component metrics, manager-created custom metrics,
//! and process/container metrics are ALL prefixed uniformly, with no
//! double-prefix.
//!
//! `metrics-util`'s own `PrefixLayer` joins with a `.` (`<prefix>.<name>`),
//! which does not match the crate's `{namespace}_<name>` convention (and the
//! dot only collapses to `_` for the Prometheus exporter, not OTel or the
//! manifest). So we use a tiny underscore-joining wrapper instead, keeping the
//! emitted names byte-identical to the manifest names produced by
//! [`MetricRegistry`](super::manifest::MetricRegistry).

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

/// Recorder wrapper that prepends `{prefix}_` to every key and describe call.
pub(crate) struct PrefixRecorder<R> {
    prefix: String,
    inner: R,
}

impl<R> PrefixRecorder<R> {
    /// Wrap `inner`, prepending `{prefix}_` to every metric name.
    ///
    /// The caller is responsible for passing a non-empty `prefix`; an empty
    /// prefix would emit a leading underscore. `install_recorders` only wraps
    /// when the namespace is non-empty.
    pub(crate) fn new(prefix: impl Into<String>, inner: R) -> Self {
        Self {
            prefix: prefix.into(),
            inner,
        }
    }

    fn prefixed_key(&self, key: &Key) -> Key {
        let mut name = String::with_capacity(self.prefix.len() + 1 + key.name().len());
        name.push_str(&self.prefix);
        name.push('_');
        name.push_str(key.name());
        Key::from_parts(name, key.labels())
    }

    fn prefixed_key_name(&self, key_name: KeyName) -> KeyName {
        let mut name = String::with_capacity(self.prefix.len() + 1 + key_name.as_str().len());
        name.push_str(&self.prefix);
        name.push('_');
        name.push_str(key_name.as_str());
        KeyName::from(name)
    }
}

impl<R: Recorder> Recorder for PrefixRecorder<R> {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe_counter(self.prefixed_key_name(key_name), unit, description);
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe_gauge(self.prefixed_key_name(key_name), unit, description);
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe_histogram(self.prefixed_key_name(key_name), unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.inner
            .register_counter(&self.prefixed_key(key), metadata)
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.inner.register_gauge(&self.prefixed_key(key), metadata)
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        self.inner
            .register_histogram(&self.prefixed_key(key), metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Recording recorder: captures the names it receives so we can assert the
    /// prefix is applied exactly once.
    #[derive(Clone, Default)]
    struct CapturingRecorder {
        registered: Arc<Mutex<Vec<String>>>,
        described: Arc<Mutex<Vec<String>>>,
    }

    impl Recorder for CapturingRecorder {
        fn describe_counter(&self, key_name: KeyName, _u: Option<Unit>, _d: SharedString) {
            self.described
                .lock()
                .unwrap()
                .push(key_name.as_str().to_string());
        }
        fn describe_gauge(&self, key_name: KeyName, _u: Option<Unit>, _d: SharedString) {
            self.described
                .lock()
                .unwrap()
                .push(key_name.as_str().to_string());
        }
        fn describe_histogram(&self, key_name: KeyName, _u: Option<Unit>, _d: SharedString) {
            self.described
                .lock()
                .unwrap()
                .push(key_name.as_str().to_string());
        }
        fn register_counter(&self, key: &Key, _m: &Metadata<'_>) -> Counter {
            self.registered.lock().unwrap().push(key.name().to_string());
            Counter::noop()
        }
        fn register_gauge(&self, key: &Key, _m: &Metadata<'_>) -> Gauge {
            self.registered.lock().unwrap().push(key.name().to_string());
            Gauge::noop()
        }
        fn register_histogram(&self, key: &Key, _m: &Metadata<'_>) -> Histogram {
            self.registered.lock().unwrap().push(key.name().to_string());
            Histogram::noop()
        }
    }

    fn meta() -> Metadata<'static> {
        Metadata::new("test", metrics::Level::INFO, None)
    }

    #[test]
    fn register_prefixes_key_with_underscore() {
        let inner = CapturingRecorder::default();
        let rec = PrefixRecorder::new("acme", inner.clone());
        let _ = rec.register_counter(&Key::from_name("transport_sent_total"), &meta());
        let names = inner.registered.lock().unwrap();
        assert_eq!(names.as_slice(), &["acme_transport_sent_total"]);
    }

    #[test]
    fn describe_prefixes_key_name_with_underscore() {
        let inner = CapturingRecorder::default();
        let rec = PrefixRecorder::new("acme", inner.clone());
        rec.describe_gauge(
            KeyName::from("pipeline_ready"),
            None,
            SharedString::const_str("ready"),
        );
        let names = inner.described.lock().unwrap();
        assert_eq!(names.as_slice(), &["acme_pipeline_ready"]);
    }

    #[test]
    fn prefix_applied_exactly_once() {
        let inner = CapturingRecorder::default();
        let rec = PrefixRecorder::new("app", inner.clone());
        let _ = rec.register_gauge(&Key::from_name("transport_healthy"), &meta());
        let names = inner.registered.lock().unwrap();
        // Single prefix, no double-prefix.
        assert_eq!(names.as_slice(), &["app_transport_healthy"]);
    }

    #[test]
    fn labels_are_preserved() {
        let inner = CapturingRecorder::default();
        let rec = PrefixRecorder::new("acme", inner.clone());
        let key = Key::from_parts(
            "transport_sent_total",
            vec![metrics::Label::new("t", "kafka")],
        );
        let prefixed = rec.prefixed_key(&key);
        assert_eq!(prefixed.name(), "acme_transport_sent_total");
        let labels: Vec<_> = prefixed.labels().collect();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].key(), "t");
        assert_eq!(labels[0].value(), "kafka");
    }
}
