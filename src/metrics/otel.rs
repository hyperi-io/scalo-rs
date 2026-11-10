// Project:   scalo
// File:      src/metrics/otel.rs
// Purpose:   OTel MeterProvider setup and recorder installation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! OpenTelemetry metrics setup.
//!
//! Configures an OTel `SdkMeterProvider` with an OTLP periodic exporter
//! via the `metrics-exporter-opentelemetry` crate. When both the `metrics`
//! (Prometheus) and `otel-metrics` features are enabled, a `Fanout` recorder
//! sends each measurement to both backends.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use dashmap::DashMap;
use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use metrics_util::{KeyHasher, MetricKind};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};

use super::MetricsError;
use super::otel_types::{OtelMetricsConfig, OtelProtocol};

/// Makes each metric export one steady OTel stream.
///
/// `metrics::gauge!` registers on every call, and the exporter answers each registration with a new instrument, so one handle is kept per key. The exporter hands a name's unit and description only to the next instrument it builds, and OTel splits instruments that differ in them into separate streams, so each name's first unit and description are kept here and given to every instrument built for it. A gauge's moves run one at a time, so the value it records last is its running value.
pub(crate) struct SteadyRecorder<R> {
    counters: Handles<Counter>,
    gauges: Handles<Gauge>,
    histograms: Handles<Histogram>,
    /// Each name's first unit and description, held while a handle is built so the inner describe and register run as a pair.
    described: Mutex<HashMap<(MetricKind, KeyName), Described>>,
    inner: R,
}

/// One kind's handles. A lookup never waits on a handle being built.
type Handles<H> = DashMap<Key, H, BuildHasherDefault<KeyHasher>>;

/// A name's first unit and description, fixed once an instrument is built for it.
#[derive(Default)]
struct Described {
    unit: Option<Unit>,
    description: Option<SharedString>,
    built: bool,
}

/// A gauge whose moves run one at a time, so the exporter records them in the order they apply.
struct SerialGauge {
    gauge: Gauge,
    serial: Mutex<()>,
}

impl GaugeFn for SerialGauge {
    fn increment(&self, value: f64) {
        let _serial = self.serial.lock().unwrap_or_else(PoisonError::into_inner);
        self.gauge.increment(value);
    }

    fn decrement(&self, value: f64) {
        let _serial = self.serial.lock().unwrap_or_else(PoisonError::into_inner);
        self.gauge.decrement(value);
    }

    fn set(&self, value: f64) {
        let _serial = self.serial.lock().unwrap_or_else(PoisonError::into_inner);
        self.gauge.set(value);
    }
}

impl<R> SteadyRecorder<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            counters: Handles::default(),
            gauges: Handles::default(),
            histograms: Handles::default(),
            described: Mutex::default(),
            inner,
        }
    }

    /// Keeps the first unit and description a name is given before its first instrument is built.
    fn describe(&self, kind: MetricKind, name: KeyName, unit: Option<Unit>, text: SharedString) {
        let mut described = self
            .described
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let first = described.entry((kind, name)).or_default();
        if !first.built && first.description.is_none() {
            first.unit = unit;
            first.description = Some(text);
        }
    }

    /// The handle `key` already has, or one `register` builds after `describe` hands the inner recorder the name's first unit and description.
    fn handle<H: Clone>(
        &self,
        handles: &Handles<H>,
        kind: MetricKind,
        key: &Key,
        describe: impl FnOnce(KeyName, Option<Unit>, SharedString),
        register: impl FnOnce() -> H,
    ) -> H {
        if let Some(handle) = handles.get(key) {
            return handle.value().clone();
        }
        let mut described = self
            .described
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Another caller may have built it while this one waited.
        if let Some(handle) = handles.get(key) {
            return handle.value().clone();
        }
        let first = described.entry((kind, key.name_shared())).or_default();
        first.built = true;
        if let Some(text) = &first.description {
            describe(key.name_shared(), first.unit, text.clone());
        }
        let handle = register();
        handles.insert(key.clone(), handle.clone());
        handle
    }
}

impl<R: Recorder> Recorder for SteadyRecorder<R> {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(MetricKind::Counter, key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(MetricKind::Gauge, key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(MetricKind::Histogram, key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.handle(
            &self.counters,
            MetricKind::Counter,
            key,
            |name, unit, text| self.inner.describe_counter(name, unit, text),
            || self.inner.register_counter(key, metadata),
        )
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.handle(
            &self.gauges,
            MetricKind::Gauge,
            key,
            |name, unit, text| self.inner.describe_gauge(name, unit, text),
            || {
                Gauge::from_arc(Arc::new(SerialGauge {
                    gauge: self.inner.register_gauge(key, metadata),
                    serial: Mutex::default(),
                }))
            },
        )
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        self.handle(
            &self.histograms,
            MetricKind::Histogram,
            key,
            |name, unit, text| self.inner.describe_histogram(name, unit, text),
            || self.inner.register_histogram(key, metadata),
        )
    }
}

/// Resolved OTLP configuration after applying env var overrides.
struct ResolvedOtelConfig {
    endpoint: String,
    protocol: OtelProtocol,
    export_interval: Duration,
    export_timeout: Duration,
    headers: std::collections::HashMap<String, String>,
    service_name: String,
    /// Deployment environment (dev/staging/prod), tagged on the OTel resource
    /// as the stable semconv attribute `deployment.environment.name`. Sourced
    /// from scalo's env detection ([`crate::env::get_app_env`]).
    deployment_environment: String,
    /// Consumer-configured extra resource attributes, applied LAST so they can
    /// override the auto-set ones (e.g. a custom `deployment.environment.name`).
    resource_attributes: std::collections::HashMap<String, String>,
}

/// Resolve OTel config with env var overrides.
fn resolve_config(config: &OtelMetricsConfig) -> ResolvedOtelConfig {
    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .ok()
        .and_then(|p| match p.as_str() {
            "grpc" => Some(OtelProtocol::Grpc),
            "http/protobuf" | "http" => Some(OtelProtocol::Http),
            _ => None,
        })
        .unwrap_or(config.protocol);

    // Resolved after the protocol, as the default endpoint depends on it.
    let endpoint =
        super::otel_types::resolved_endpoint(crate::otel_backoff::endpoint_for_protocol(
            &config.endpoint,
            protocol == OtelProtocol::Http,
        ));

    let export_interval = std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or_else(
            || Duration::from_secs(config.export_interval_secs),
            Duration::from_millis,
        );

    let export_timeout = std::env::var("OTEL_METRIC_EXPORT_TIMEOUT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or_else(
            || Duration::from_secs(config.export_timeout_secs),
            Duration::from_millis,
        );

    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| config.service_name.clone());

    // Deployment environment for the OTel resource. Sourced from scalo's own
    // env detection (APP_ENV/ENVIRONMENT/ENV -> "development"), so traces and
    // metrics are tagged with the tier without per-app wiring. A consumer can
    // override via `resource_attributes["deployment.environment.name"]`.
    let deployment_environment = crate::env::get_app_env();

    ResolvedOtelConfig {
        endpoint,
        protocol,
        export_interval,
        export_timeout,
        headers: config.headers.clone(),
        service_name,
        deployment_environment,
        resource_attributes: config.resource_attributes.clone(),
    }
}

/// Build the OTLP metric exporter for the given protocol and endpoint.
///
/// The timeout is the per-attempt deadline, so an endpoint that accepts and
/// then stalls fails the attempt instead of holding the exporter open.
fn build_otlp_exporter(
    resolved: &ResolvedOtelConfig,
) -> Result<opentelemetry_otlp::MetricExporter, MetricsError> {
    match resolved.protocol {
        OtelProtocol::Grpc => {
            if !resolved.headers.is_empty() {
                tracing::warn!(
                    "metrics.otel.headers are only applied on the http protocol; \
                     set OTEL_EXPORTER_OTLP_HEADERS for grpc"
                );
            }
            opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&resolved.endpoint)
                .with_timeout(resolved.export_timeout)
                .build()
                .map_err(|e| MetricsError::BuildError(format!("OTel gRPC exporter: {e}")))
        }
        // The builder POSTs to the endpoint verbatim, so the base URL needs the signal path.
        OtelProtocol::Http => opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(crate::otel_backoff::append_signal_path(
                &resolved.endpoint,
                "v1/metrics",
            ))
            .with_timeout(resolved.export_timeout)
            .with_headers(resolved.headers.clone())
            .build()
            .map_err(|e| MetricsError::BuildError(format!("OTel HTTP exporter: {e}"))),
    }
}

/// The OTLP resource every exported metric carries: `service.name`,
/// `deployment.environment.name`, the replica's identity, and the consumer's
/// attributes, which win over all three.
///
/// `deployment.environment.name` is the stable semconv key; the bare
/// `deployment.environment` is deprecated.
fn resource(resolved: &ResolvedOtelConfig) -> opentelemetry_sdk::Resource {
    let mut builder = opentelemetry_sdk::Resource::builder();
    if !resolved.service_name.is_empty() {
        builder = builder.with_service_name(resolved.service_name.clone());
    }
    if !resolved.deployment_environment.is_empty() {
        builder = builder.with_attribute(opentelemetry::KeyValue::new(
            "deployment.environment.name",
            resolved.deployment_environment.clone(),
        ));
    }
    builder = builder.with_attributes(crate::otel_resource::instance_attributes());
    for (k, v) in &resolved.resource_attributes {
        builder = builder.with_attribute(opentelemetry::KeyValue::new(k.clone(), v.clone()));
    }
    builder.build()
}

/// Build the OTel recorder (without installing it globally).
///
/// Uses `metrics-exporter-opentelemetry` to create a `metrics::Recorder`
/// backed by an OTel `SdkMeterProvider` with an OTLP periodic exporter.
///
/// The recorder is returned but NOT installed -- the caller decides whether
/// to install it directly or compose it with Prometheus via Fanout.
pub(crate) fn build_otel_recorder(
    scope_name: &str,
    config: &OtelMetricsConfig,
) -> Result<
    (
        SteadyRecorder<metrics_exporter_opentelemetry::Recorder>,
        opentelemetry_sdk::metrics::SdkMeterProvider,
    ),
    MetricsError,
> {
    let resolved = resolve_config(config);

    let exporter = build_otlp_exporter(&resolved)?;
    // Backoff starts at one export interval so the first retry is the next
    // scheduled tick, then doubles while the collector stays unreachable.
    let exporter =
        crate::otel_backoff::GatedMetricExporter::new(exporter, resolved.export_interval);
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(resolved.export_interval)
        .build();

    let resource = resource(&resolved);

    // Use with_meter_provider to attach our OTLP reader to the internal provider
    let reader_for_closure = reader;
    let resource_for_closure = resource;

    // build() returns (SdkMeterProvider, Recorder) -- provider first
    let (provider, recorder) =
        metrics_exporter_opentelemetry::Recorder::builder(scope_name.to_string())
            .with_meter_provider(move |mpb| {
                mpb.with_reader(reader_for_closure)
                    .with_resource(resource_for_closure)
            })
            .build();

    tracing::info!(
        endpoint = %resolved.endpoint,
        protocol = ?resolved.protocol,
        export_interval_secs = resolved.export_interval.as_secs(),
        export_timeout_secs = resolved.export_timeout.as_secs(),
        "OTLP metric push enabled"
    );

    Ok((SteadyRecorder::new(recorder), provider))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;

    use metrics::{Label, Level, Metadata};
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    use super::*;

    const GAUGE: &str = "a_gauge";
    const COUNTER: &str = "a_counter";
    const HISTOGRAM: &str = "a_histogram";
    const SLOW: &str = "slow";
    static METADATA: Metadata<'static> = Metadata::new("test", Level::INFO, None);

    /// A metric as exported: name, unit, description and data points.
    type Exported = (String, String, String, usize);

    fn streams(data: &AggregatedMetrics) -> usize {
        match data {
            AggregatedMetrics::F64(MetricData::Gauge(gauge)) => gauge.data_points().count(),
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                histogram.data_points().count()
            }
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum.data_points().count(),
            other => panic!("the exporter sent an unexpected shape: {other:?}"),
        }
    }

    /// An exporter recorder whose provider flushes to the returned in-memory exporter.
    fn otel() -> (
        SdkMeterProvider,
        InMemoryMetricExporter,
        metrics_exporter_opentelemetry::Recorder,
    ) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let (provider, recorder) = metrics_exporter_opentelemetry::Recorder::builder("test")
            .with_meter_provider(move |builder| builder.with_reader(reader))
            .build();
        (provider, exporter, recorder)
    }

    /// Every metric the provider exports on a flush, sorted.
    fn exported(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Vec<Exported> {
        provider
            .force_flush()
            .expect("the in-memory exporter takes the flush");
        let finished = exporter.get_finished_metrics().expect("the flush exported");
        let mut found: Vec<Exported> = finished
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .map(|metric| {
                (
                    metric.name().to_string(),
                    metric.unit().to_string(),
                    metric.description().to_string(),
                    streams(metric.data()),
                )
            })
            .collect();
        found.sort();
        found
    }

    fn labelled(name: &'static str) -> Key {
        Key::from_parts(name, vec![Label::new("k", "v")])
    }

    /// Describe each kind once with a unit, record through three registrations as `metrics::gauge!` does and once under a label, and list what is exported.
    fn exported_streams(
        wrap: impl FnOnce(metrics_exporter_opentelemetry::Recorder) -> Box<dyn Recorder>,
    ) -> Vec<Exported> {
        let (provider, exporter, recorder) = otel();
        let recorder = wrap(recorder);

        recorder.describe_gauge(GAUGE.into(), Some(Unit::Percent), "described".into());
        recorder.describe_counter(COUNTER.into(), Some(Unit::Bytes), "described".into());
        recorder.describe_histogram(HISTOGRAM.into(), Some(Unit::Seconds), "described".into());
        for value in [1, 2, 3] {
            recorder
                .register_gauge(&Key::from_name(GAUGE), &METADATA)
                .set(f64::from(value));
            recorder
                .register_counter(&Key::from_name(COUNTER), &METADATA)
                .increment(1);
            recorder
                .register_histogram(&Key::from_name(HISTOGRAM), &METADATA)
                .record(f64::from(value));
        }
        recorder
            .register_gauge(&labelled(GAUGE), &METADATA)
            .set(1.0);
        recorder
            .register_counter(&labelled(COUNTER), &METADATA)
            .increment(1);
        recorder
            .register_histogram(&labelled(HISTOGRAM), &METADATA)
            .record(1.0);
        exported(&provider, &exporter)
    }

    #[test]
    fn each_metric_exports_one_stream_however_often_it_registers() {
        let found = exported_streams(|recorder| Box::new(SteadyRecorder::new(recorder)));

        let described = |name: &str, unit: &str| {
            (
                name.to_string(),
                unit.to_string(),
                "described".to_string(),
                2,
            )
        };
        assert_eq!(
            found,
            vec![
                described(COUNTER, "B"),
                described(GAUGE, "%"),
                described(HISTOGRAM, "s"),
            ]
        );
    }

    /// A name's first describe sets the unit and description of every instrument built for it. A describe after its first use changes nothing, so the name stays one stream.
    #[test]
    fn every_instrument_of_a_name_carries_its_first_description() {
        type Describe = (Unit, &'static str);
        // (case, describes before first use, describe after it, unit and description exported)
        let cases: [(&str, &[Describe], Option<Describe>, (&str, &str)); 3] = [
            (
                "first describe wins",
                &[(Unit::Seconds, "first"), (Unit::Milliseconds, "second")],
                None,
                ("s", "first"),
            ),
            (
                "described after first use",
                &[],
                Some((Unit::Seconds, "late")),
                ("", ""),
            ),
            ("never described", &[], None, ("", "")),
        ];
        for (name, before, after, (unit, description)) in cases {
            let (provider, exporter, bare) = otel();
            let recorder = SteadyRecorder::new(bare);
            for (first_unit, text) in before {
                recorder.describe_histogram(HISTOGRAM.into(), Some(*first_unit), (*text).into());
            }
            recorder
                .register_histogram(&Key::from_name(HISTOGRAM), &METADATA)
                .record(1.0);
            if let Some((late_unit, text)) = after {
                recorder.describe_histogram(HISTOGRAM.into(), Some(late_unit), text.into());
            }
            recorder
                .register_histogram(&labelled(HISTOGRAM), &METADATA)
                .record(1.0);

            let expected = (
                HISTOGRAM.to_string(),
                unit.to_string(),
                description.to_string(),
                2,
            );
            assert_eq!(exported(&provider, &exporter), vec![expected], "{name}");
        }
    }

    /// Builds no-op counters, holding the build of `SLOW` until released, and gauges that note overlapping moves.
    struct TestRecorder {
        entered: Mutex<mpsc::Sender<()>>,
        release: Mutex<mpsc::Receiver<()>>,
        builds: AtomicUsize,
        gauge: Arc<OverlapGauge>,
    }

    impl TestRecorder {
        /// The recorder, the receiver told when the `SLOW` build starts, and the sender that releases it.
        fn new() -> (Self, mpsc::Receiver<()>, mpsc::Sender<()>) {
            let (entered, started) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let recorder = Self {
                entered: Mutex::new(entered),
                release: Mutex::new(released),
                builds: AtomicUsize::new(0),
                gauge: Arc::default(),
            };
            (recorder, started, release)
        }
    }

    impl Recorder for TestRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            self.builds.fetch_add(1, Ordering::SeqCst);
            if key.name() == SLOW {
                let entered = self.entered.lock().expect("entered sender");
                entered.send(()).expect("the test waits for the build");
                let release = self.release.lock().expect("release receiver");
                release.recv().expect("the test releases the build");
            }
            Counter::noop()
        }

        fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::from_arc(Arc::clone(&self.gauge))
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    /// Notes the most moves of one gauge in flight at once.
    #[derive(Default)]
    struct OverlapGauge {
        in_flight: AtomicUsize,
        most: AtomicUsize,
    }

    impl OverlapGauge {
        fn step(&self) {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.most.fetch_max(now, Ordering::SeqCst);
            thread::yield_now();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl GaugeFn for OverlapGauge {
        fn increment(&self, _: f64) {
            self.step();
        }

        fn decrement(&self, _: f64) {
            self.step();
        }

        fn set(&self, _: f64) {
            self.step();
        }
    }

    #[test]
    fn a_built_handle_is_read_while_another_is_built() {
        let (inner, started, release) = TestRecorder::new();
        let recorder = Arc::new(SteadyRecorder::new(inner));
        recorder
            .register_counter(&Key::from_name("fast"), &METADATA)
            .increment(1);

        let building = {
            let recorder = Arc::clone(&recorder);
            thread::spawn(move || {
                recorder
                    .register_counter(&Key::from_name(SLOW), &METADATA)
                    .increment(1);
            })
        };
        started.recv().expect("the slow build started");
        let (read_tx, read) = mpsc::channel();
        let reader = {
            let recorder = Arc::clone(&recorder);
            thread::spawn(move || {
                recorder
                    .register_counter(&Key::from_name("fast"), &METADATA)
                    .increment(1);
                read_tx.send(()).expect("the test waits for the read");
            })
        };
        // A deadlock bound, not a timing assertion: a read that waits on the build returns only after the release.
        let read_back = read.recv_timeout(Duration::from_secs(30));
        release.send(()).expect("the build waits for the release");
        building.join().expect("the build thread");
        reader.join().expect("the reader thread");
        assert!(
            read_back.is_ok(),
            "an existing handle waited on another key's build"
        );

        let racers: Vec<thread::JoinHandle<()>> = (0..8)
            .map(|_| {
                let recorder = Arc::clone(&recorder);
                thread::spawn(move || {
                    recorder
                        .register_counter(&Key::from_name("racing"), &METADATA)
                        .increment(1);
                })
            })
            .collect();
        for racer in racers {
            racer.join().expect("a racing thread");
        }
        assert_eq!(
            recorder.inner.builds.load(Ordering::SeqCst),
            3,
            "fast, slow and racing are each built once"
        );
    }

    #[test]
    fn a_gauges_moves_run_one_at_a_time() {
        let (inner, _started, _release) = TestRecorder::new();
        let recorder = Arc::new(SteadyRecorder::new(inner));
        let movers: Vec<thread::JoinHandle<()>> = (0..8)
            .map(|_| {
                let recorder = Arc::clone(&recorder);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        let gauge = recorder.register_gauge(&Key::from_name(GAUGE), &METADATA);
                        gauge.increment(1.0);
                        gauge.decrement(1.0);
                    }
                })
            })
            .collect();
        for mover in movers {
            mover.join().expect("a moving thread");
        }
        assert_eq!(recorder.inner.gauge.most.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_gauge_moved_up_and_down_reads_its_running_value() {
        let (provider, exporter, bare) = otel();
        let recorder = SteadyRecorder::new(bare);
        let key = Key::from_name(GAUGE);

        // A connection gauge: two opens and one close, each through its own registration
        recorder.register_gauge(&key, &METADATA).increment(1.0);
        recorder.register_gauge(&key, &METADATA).increment(1.0);
        recorder.register_gauge(&key, &METADATA).decrement(1.0);
        provider
            .force_flush()
            .expect("the in-memory exporter takes the flush");

        let finished = exporter.get_finished_metrics().expect("the flush exported");
        let values: Vec<f64> = finished
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::F64(MetricData::Gauge(gauge)) => Some(
                    gauge
                        .data_points()
                        .map(|point| point.value())
                        .collect::<Vec<f64>>(),
                ),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(values, vec![1.0]);
    }

    /// When this fails the exporter gives a name's description to every instrument, and `SteadyRecorder` can stop handing it over.
    #[test]
    fn the_bare_exporter_splits_a_described_metric_into_two_instruments() {
        let found = exported_streams(|recorder| Box::new(recorder));

        for name in [COUNTER, GAUGE, HISTOGRAM] {
            let count = found
                .iter()
                .filter(|(found_name, ..)| found_name == name)
                .count();
            assert_eq!(count, 2, "{name} in {found:?}");
        }
    }

    // Deploy-env OTel: the deployment environment must be sourced from scalo's
    // env detection (APP_ENV) and configured resource_attributes must pass
    // through to the resolved config (they were previously dropped). Mirrors
    // the temp_env pattern used by env.rs tests.
    #[test]
    fn resolve_sets_deployment_environment_and_keeps_resource_attributes() {
        temp_env::with_vars(
            [
                ("APP_ENV", Some("production")),
                ("ENVIRONMENT", None::<&str>),
                ("ENV", None::<&str>),
                ("OTEL_SERVICE_NAME", None::<&str>),
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
            ],
            || {
                let mut cfg = OtelMetricsConfig::default();
                cfg.resource_attributes
                    .insert("team".to_string(), "data-plane".to_string());
                let resolved = resolve_config(&cfg);
                assert_eq!(
                    resolved.deployment_environment, "production",
                    "deployment environment must come from APP_ENV"
                );
                assert_eq!(
                    resolved.resource_attributes.get("team").map(String::as_str),
                    Some("data-plane"),
                    "configured resource attributes must survive resolution"
                );
            },
        );
    }

    fn attribute(resource: &opentelemetry_sdk::Resource, key: &'static str) -> Option<String> {
        resource
            .get(&opentelemetry::Key::from_static_str(key))
            .map(|value| value.to_string())
    }

    /// Every replica pushing one series makes the pushed copy useless for rates.
    #[test]
    fn the_metrics_resource_names_the_replica() {
        temp_env::with_vars(
            [
                ("OTEL_RESOURCE_ATTRIBUTES", None::<&str>),
                ("POD_NAME", Some("loader-7d9f-abcde")),
                ("HOSTNAME", Some("node-host")),
            ],
            || {
                let resource = resource(&resolve_config(&OtelMetricsConfig::default()));
                assert_eq!(
                    attribute(&resource, "service.instance.id").as_deref(),
                    Some("loader-7d9f-abcde")
                );
                assert_eq!(
                    attribute(&resource, "k8s.pod.name").as_deref(),
                    Some("loader-7d9f-abcde")
                );
            },
        );
    }

    #[test]
    fn configured_resource_attributes_win_over_the_replica_identity() {
        temp_env::with_vars(
            [
                ("OTEL_RESOURCE_ATTRIBUTES", None::<&str>),
                ("POD_NAME", Some("loader-7d9f-abcde")),
            ],
            || {
                let mut cfg = OtelMetricsConfig::default();
                cfg.resource_attributes
                    .insert("service.instance.id".to_string(), "from-config".to_string());
                let resource = resource(&resolve_config(&cfg));
                assert_eq!(
                    attribute(&resource, "service.instance.id").as_deref(),
                    Some("from-config")
                );
            },
        );
    }

    #[test]
    fn the_default_endpoint_follows_the_protocol() {
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", None::<&str>),
            ],
            || {
                assert_eq!(
                    resolve_config(&OtelMetricsConfig::default()).endpoint,
                    "http://localhost:4317"
                );
                let http = OtelMetricsConfig {
                    protocol: OtelProtocol::Http,
                    ..OtelMetricsConfig::default()
                };
                assert_eq!(resolve_config(&http).endpoint, "http://localhost:4318");
            },
        );
    }

    #[test]
    fn the_protocol_env_var_moves_the_default_endpoint() {
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", Some("http/protobuf")),
            ],
            || {
                let resolved = resolve_config(&OtelMetricsConfig::default());
                assert_eq!(resolved.protocol, OtelProtocol::Http);
                assert_eq!(resolved.endpoint, "http://localhost:4318");
            },
        );
    }

    #[test]
    fn a_configured_or_env_endpoint_is_used_as_given() {
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", None::<&str>),
            ],
            || {
                let cfg = OtelMetricsConfig {
                    protocol: OtelProtocol::Http,
                    endpoint: "http://collector:4317".to_string(),
                    ..OtelMetricsConfig::default()
                };
                assert_eq!(resolve_config(&cfg).endpoint, "http://collector:4317");
            },
        );
        temp_env::with_vars(
            [
                ("OTEL_EXPORTER_OTLP_ENDPOINT", Some("http://localhost:4317")),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", Some("http/protobuf")),
            ],
            || {
                let resolved = resolve_config(&OtelMetricsConfig::default());
                assert_eq!(resolved.endpoint, "http://localhost:4317");
            },
        );
    }

    #[test]
    fn resolve_deployment_environment_defaults_to_development() {
        temp_env::with_vars(
            [
                ("APP_ENV", None::<&str>),
                ("ENVIRONMENT", None::<&str>),
                ("ENV", None::<&str>),
            ],
            || {
                let resolved = resolve_config(&OtelMetricsConfig::default());
                assert_eq!(resolved.deployment_environment, "development");
            },
        );
    }
}
