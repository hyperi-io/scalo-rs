// Project:   scalo
// File:      src/metrics/test_recorder.rs
// Purpose:   A recorder that panics, for tests of metrics recorded from a drop
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A recorder that panics, as a caller's recorder might.

/// Panics on registering each metric kind set to `true`, and hands out a
/// no-op for the rest.
#[derive(Clone, Copy)]
pub(crate) struct PanickingRecorder {
    counters: bool,
    gauges: bool,
    histograms: bool,
}

impl PanickingRecorder {
    /// Every metric panics.
    pub(crate) const ALL: Self = Self {
        counters: true,
        gauges: true,
        histograms: true,
    };

    /// Counters panic, gauges and histograms are no-ops.
    #[cfg_attr(
        not(feature = "transport-grpc"),
        allow(dead_code, reason = "only the gRPC registry's tests use it")
    )]
    pub(crate) const COUNTERS: Self = Self {
        counters: true,
        gauges: false,
        histograms: false,
    };
}

impl ::metrics::Recorder for PanickingRecorder {
    fn describe_counter(
        &self,
        _: ::metrics::KeyName,
        _: Option<::metrics::Unit>,
        _: ::metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: ::metrics::KeyName,
        _: Option<::metrics::Unit>,
        _: ::metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: ::metrics::KeyName,
        _: Option<::metrics::Unit>,
        _: ::metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        _: &::metrics::Key,
        _: &::metrics::Metadata<'_>,
    ) -> ::metrics::Counter {
        assert!(!self.counters, "recorder refuses counters");
        ::metrics::Counter::noop()
    }

    fn register_gauge(&self, _: &::metrics::Key, _: &::metrics::Metadata<'_>) -> ::metrics::Gauge {
        assert!(!self.gauges, "recorder refuses gauges");
        ::metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &::metrics::Key,
        _: &::metrics::Metadata<'_>,
    ) -> ::metrics::Histogram {
        assert!(!self.histograms, "recorder refuses histograms");
        ::metrics::Histogram::noop()
    }
}
