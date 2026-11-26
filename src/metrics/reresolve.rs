// Project:   scalo
// File:      src/metrics/reresolve.rs
// Purpose:   Recorder layer whose counter and histogram handles survive idle expiry
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Handles that outlive an idle-timeout expiry.
//!
//! The Prometheus registry drops an idle counter or histogram, and a handle
//! taken before the drop keeps writing to storage the registry no longer
//! renders. With the idle timeout on, this layer hands out counter and
//! histogram handles that look their series up on every write, so a series
//! dropped while idle is created again by its next write. Gauges pass through
//! unchanged because they never expire.

use std::sync::Arc;

use metrics::{
    Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Level, Metadata, Recorder,
    SharedString, Unit,
};

/// The Prometheus recorder ignores metadata, so one static value serves every lookup.
const METADATA: Metadata<'static> = Metadata::new(module_path!(), Level::INFO, None);

/// Recorder layer that re-resolves counter and histogram handles on each write
/// when `reresolve` is set, and delegates unchanged otherwise.
pub(crate) struct ReResolveRecorder<R> {
    inner: Arc<R>,
    reresolve: bool,
}

impl<R> ReResolveRecorder<R> {
    pub(crate) fn new(inner: R, reresolve: bool) -> Self {
        Self {
            inner: Arc::new(inner),
            reresolve,
        }
    }
}

/// A handle that resolves `key` in `recorder` on every write.
struct ReResolved<R> {
    key: Key,
    recorder: Arc<R>,
}

impl<R: Recorder> CounterFn for ReResolved<R> {
    fn increment(&self, value: u64) {
        self.recorder
            .register_counter(&self.key, &METADATA)
            .increment(value);
    }

    fn absolute(&self, value: u64) {
        self.recorder
            .register_counter(&self.key, &METADATA)
            .absolute(value);
    }
}

impl<R: Recorder> HistogramFn for ReResolved<R> {
    fn record(&self, value: f64) {
        self.recorder
            .register_histogram(&self.key, &METADATA)
            .record(value);
    }

    fn record_many(&self, value: f64, count: usize) {
        self.recorder
            .register_histogram(&self.key, &METADATA)
            .record_many(value, count);
    }
}

impl<R: Recorder + Send + Sync + 'static> Recorder for ReResolveRecorder<R> {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_counter(key_name, unit, description);
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_gauge(key_name, unit, description);
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_histogram(key_name, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        if !self.reresolve {
            return self.inner.register_counter(key, metadata);
        }
        Counter::from_arc(Arc::new(ReResolved {
            key: key.clone(),
            recorder: Arc::clone(&self.inner),
        }))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.inner.register_gauge(key, metadata)
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        if !self.reresolve {
            return self.inner.register_histogram(key, metadata);
        }
        Histogram::from_arc(Arc::new(ReResolved {
            key: key.clone(),
            recorder: Arc::clone(&self.inner),
        }))
    }
}
