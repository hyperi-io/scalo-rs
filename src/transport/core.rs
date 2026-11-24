// Project:   scalo
// File:      src/transport/core.rs
// Purpose:   Send scaffolding the transports share: their transport_* series and lifecycle
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Send scaffolding the transports share.
//!
//! [`Meter`] counts one transport's `transport_*` series under its `transport`
//! label. Every key is built at compile time, so a count costs what a
//! `metrics::counter!` with literal labels costs: no allocation and no hashing.
//! Without the `metrics` feature a transport's counts compile away, except
//! syslog's, which reach a recorder the app installs through `metrics-core`.
//!
//! [`Lifecycle`] holds a transport's closed flag and readiness, and its
//! `transport:<name>` entry in the health registry, which answers from the same
//! readiness.

use metrics::{Key, Label};

static METADATA: metrics::Metadata<'static> =
    metrics::Metadata::new(module_path!(), metrics::Level::INFO, Some(module_path!()));

/// A transport's `transport_*` series, under its `transport` label.
///
/// Each transport implements it with its name. The counts are associated
/// functions, so a free function in the transport's module counts through the
/// transport's type.
pub(crate) trait Meter {
    /// The `transport` label, and the transport's
    /// [`TransportBase::name`](super::TransportBase::name).
    const TRANSPORT: &'static str;

    /// The label every series carries.
    const LABELS: &'static [Label] = &[Label::from_static_parts("transport", Self::TRANSPORT)];

    /// Whether the counts reach a recorder without the `metrics` feature, through
    /// `metrics-core` alone. Syslog's always have, and the others' never have.
    const CORE_COUNTS: bool = false;

    /// Whether this build counts the series at all.
    const COUNTING: bool = Self::CORE_COUNTS || cfg!(feature = "metrics");

    /// `records` written to the wire, carrying `bytes` of payload.
    fn count_sent(records: u64, bytes: u64) {
        count::<Self>(
            &const { Key::from_static_parts("transport_sent_total", Self::LABELS) },
            records,
        );
        count::<Self>(
            &const { Key::from_static_parts("transport_sent_bytes_total", Self::LABELS) },
            bytes,
        );
    }

    /// `records` read off the wire, carrying `bytes` of payload.
    #[cfg(any(
        feature = "transport-file",
        feature = "transport-pipe",
        feature = "transport-grpc",
        feature = "transport-kafka",
        all(feature = "transport-http", feature = "http-server")
    ))]
    fn count_received(records: u64, bytes: u64) {
        count::<Self>(
            &const { Key::from_static_parts("transport_received_bytes_total", Self::LABELS) },
            bytes,
        );
        count::<Self>(
            &const { Key::from_static_parts("transport_received_events_total", Self::LABELS) },
            records,
        );
    }

    /// The messages a receive returns, when there are any. Their bytes are
    /// summed only with the `metrics` feature.
    #[cfg(any(
        feature = "transport-file",
        feature = "transport-pipe",
        feature = "transport-kafka"
    ))]
    fn count_received_messages<T: super::traits::CommitToken>(
        messages: &[super::types::Message<T>],
    ) {
        if Self::COUNTING && !messages.is_empty() {
            let bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
            Self::count_received(messages.len() as u64, bytes as u64);
        }
    }

    /// A send answered `Backpressured`.
    #[cfg(any(
        feature = "transport-http",
        feature = "transport-grpc",
        feature = "transport-kafka",
        feature = "transport-syslog",
        feature = "transport-otlp"
    ))]
    fn count_backpressured() {
        count::<Self>(
            &const { Key::from_static_parts("transport_backpressured_total", Self::LABELS) },
            1,
        );
    }

    /// A send that failed.
    #[cfg(any(
        feature = "transport-http",
        feature = "transport-grpc",
        feature = "transport-kafka",
        feature = "transport-syslog",
        feature = "transport-otlp"
    ))]
    fn count_send_error() {
        count::<Self>(
            &const { Key::from_static_parts("transport_send_errors_total", Self::LABELS) },
            1,
        );
    }

    /// A record over the transport's size limit.
    #[cfg(any(
        feature = "transport-file",
        feature = "transport-grpc",
        feature = "transport-kafka",
        feature = "transport-syslog"
    ))]
    fn count_too_large() {
        count::<Self>(
            &const { Key::from_static_parts("transport_message_too_large_total", Self::LABELS) },
            1,
        );
    }

    /// A push the receiver refused because it is closed.
    #[cfg(any(
        feature = "transport-grpc",
        all(feature = "transport-http", feature = "http-server")
    ))]
    fn count_refused() {
        count::<Self>(
            &const { Key::from_static_parts("transport_refused_total", Self::LABELS) },
            1,
        );
    }

    /// A timer for `transport_send_duration_seconds`, started now.
    #[cfg(any(
        feature = "transport-http",
        feature = "transport-grpc",
        feature = "transport-kafka",
        feature = "transport-syslog",
        feature = "transport-otlp"
    ))]
    fn send_timer() -> SendTimer<Self>
    where
        Self: Sized,
    {
        SendTimer {
            started: Self::COUNTING.then(std::time::Instant::now),
            _meter: std::marker::PhantomData,
        }
    }
}

/// Times a send into `transport_send_duration_seconds` under `M`'s label. It
/// reads no clock where `M` does not count.
#[cfg(any(
    feature = "transport-http",
    feature = "transport-grpc",
    feature = "transport-kafka",
    feature = "transport-syslog",
    feature = "transport-otlp"
))]
pub(crate) struct SendTimer<M> {
    started: Option<std::time::Instant>,
    _meter: std::marker::PhantomData<M>,
}

#[cfg(any(
    feature = "transport-http",
    feature = "transport-grpc",
    feature = "transport-kafka",
    feature = "transport-syslog",
    feature = "transport-otlp"
))]
impl<M: Meter> SendTimer<M> {
    /// Record the time since the timer started.
    pub(crate) fn record(&self) {
        if let Some(started) = self.started {
            let key =
                const { Key::from_static_parts("transport_send_duration_seconds", M::LABELS) };
            metrics::with_recorder(|recorder| recorder.register_histogram(&key, &METADATA))
                .record(started.elapsed().as_secs_f64());
        }
    }
}

#[cfg(any(
    feature = "transport-file",
    feature = "transport-pipe",
    feature = "transport-http",
    feature = "transport-grpc",
    feature = "transport-syslog",
    feature = "transport-otlp"
))]
pub(crate) use lifecycle::Lifecycle;

#[cfg(any(
    feature = "transport-file",
    feature = "transport-pipe",
    feature = "transport-http",
    feature = "transport-grpc",
    feature = "transport-syslog",
    feature = "transport-otlp"
))]
mod lifecycle {
    use super::Meter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A transport's closed flag and readiness, and its `transport:<name>` entry
    /// in the health registry while it lives.
    ///
    /// The entry and `is_healthy` answer from one predicate: open, and `ready`.
    pub(crate) struct Lifecycle {
        state: Arc<State>,
        /// Removes the transport from the health registry when it drops.
        #[cfg(feature = "health")]
        _registration: crate::health::HealthRegistration,
    }

    struct State {
        closed: AtomicBool,
        /// What the transport needs beside being open.
        ready: Box<dyn Fn() -> bool + Send + Sync>,
    }

    impl State {
        fn is_healthy(&self) -> bool {
            !self.closed.load(Ordering::Relaxed) && (self.ready)()
        }
    }

    impl Lifecycle {
        /// Open, and registered as `transport:<name>` of `M`: healthy while open
        /// and `ready` answers true.
        pub(crate) fn new<M: Meter>(ready: impl Fn() -> bool + Send + Sync + 'static) -> Self {
            let state = Arc::new(State {
                closed: AtomicBool::new(false),
                ready: Box::new(ready),
            });
            Self {
                #[cfg(feature = "health")]
                _registration: {
                    let state = Arc::clone(&state);
                    crate::health::HealthRegistry::register_scoped(
                        format!("transport:{}", M::TRANSPORT),
                        move || {
                            if state.is_healthy() {
                                crate::health::HealthStatus::Healthy
                            } else {
                                crate::health::HealthStatus::Unhealthy
                            }
                        },
                    )
                },
                state,
            }
        }

        /// Mark the transport closed: `true` on the first call.
        pub(crate) fn close(&self) -> bool {
            !self.state.closed.swap(true, Ordering::AcqRel)
        }

        /// Whether [`close`](Self::close) has been called.
        pub(crate) fn is_closed(&self) -> bool {
            self.state.closed.load(Ordering::Relaxed)
        }

        /// Open, and ready: what the health registry entry answers.
        pub(crate) fn is_healthy(&self) -> bool {
            self.state.is_healthy()
        }

        /// The registry entry this transport owns.
        #[cfg(all(
            test,
            feature = "health",
            any(feature = "transport-file", feature = "transport-syslog")
        ))]
        #[allow(
            clippy::used_underscore_binding,
            reason = "the field is held only for its Drop, and the tests read its id"
        )]
        pub(crate) fn registration_id(&self) -> u64 {
            self._registration.id()
        }
    }
}

/// Add `n` to the counter under `key`, as `metrics::counter!` does, where `M`
/// counts.
fn count<M: Meter + ?Sized>(key: &Key, n: u64) {
    if M::COUNTING {
        metrics::with_recorder(|recorder| recorder.register_counter(key, &METADATA)).increment(n);
    }
}

/// A recorder that keeps each series registered on it, as `name{labels}`.
#[cfg(all(test, any(feature = "transport-grpc", feature = "transport-syslog")))]
pub(crate) mod capture {
    use metrics::{
        Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    #[derive(Default)]
    pub(crate) struct Capture(Mutex<BTreeSet<String>>);

    impl Capture {
        /// The series registered so far.
        pub(crate) fn series(&self) -> BTreeSet<String> {
            self.0.lock().unwrap().clone()
        }

        fn note(&self, key: &Key) {
            let labels: Vec<String> = key
                .labels()
                .map(|l| format!("{}=\"{}\"", l.key(), l.value()))
                .collect();
            let series = format!("{}{{{}}}", key.name(), labels.join(","));
            self.0.lock().unwrap().insert(series);
        }
    }

    impl Recorder for Capture {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            self.note(key);
            Counter::noop()
        }
        fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
            self.note(key);
            Gauge::noop()
        }
        fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
            self.note(key);
            Histogram::noop()
        }
    }
}

#[cfg(all(test, feature = "transport-grpc"))]
mod tests {
    use super::Meter;
    use std::collections::{BTreeSet, HashMap};
    use std::path::{Path, PathBuf};

    /// The committed metric set of `src/transport`.
    const FIXTURE: &str = "tests/fixtures/transport-metrics.txt";

    struct Probe;

    impl Meter for Probe {
        const TRANSPORT: &'static str = "probe";
    }

    struct CoreProbe;

    impl Meter for CoreProbe {
        const TRANSPORT: &'static str = "core-probe";
        const CORE_COUNTS: bool = true;
    }

    /// A transport that counts through `metrics-core` reaches an installed
    /// recorder in every build. The others reach it only with `metrics`.
    #[test]
    fn core_counts_reach_a_recorder_without_the_metrics_feature() {
        let capture = super::capture::Capture::default();
        metrics::with_local_recorder(&capture, || {
            CoreProbe::count_sent(1, 1);
            CoreProbe::send_timer().record();
            Probe::count_sent(1, 1);
            Probe::send_timer().record();
        });
        let core = [
            "transport_send_duration_seconds{transport=\"core-probe\"}",
            "transport_sent_bytes_total{transport=\"core-probe\"}",
            "transport_sent_total{transport=\"core-probe\"}",
        ];
        let probe = [
            "transport_send_duration_seconds{transport=\"probe\"}",
            "transport_sent_bytes_total{transport=\"probe\"}",
            "transport_sent_total{transport=\"probe\"}",
        ];
        let mut want: BTreeSet<String> = core.iter().map(ToString::to_string).collect();
        if cfg!(feature = "metrics") {
            want.extend(probe.iter().map(ToString::to_string));
        }
        assert_eq!(capture.series(), want);
    }

    /// Each count, and the series it adds under its transport's label.
    const COUNTS: [(&str, fn(), &[&str]); 8] = [
        (
            "count_sent",
            || Probe::count_sent(1, 1),
            &[
                "counter transport_sent_total",
                "counter transport_sent_bytes_total",
            ],
        ),
        (
            "count_received",
            || Probe::count_received(1, 1),
            &[
                "counter transport_received_bytes_total",
                "counter transport_received_events_total",
            ],
        ),
        (
            "count_received_messages",
            || Probe::count_received(1, 1),
            &[
                "counter transport_received_bytes_total",
                "counter transport_received_events_total",
            ],
        ),
        (
            "count_backpressured",
            Probe::count_backpressured,
            &["counter transport_backpressured_total"],
        ),
        (
            "count_send_error",
            Probe::count_send_error,
            &["counter transport_send_errors_total"],
        ),
        (
            "count_too_large",
            Probe::count_too_large,
            &["counter transport_message_too_large_total"],
        ),
        (
            "count_refused",
            Probe::count_refused,
            &["counter transport_refused_total"],
        ),
        (
            "send_timer",
            || Probe::send_timer().record(),
            &["histogram transport_send_duration_seconds"],
        ),
    ];

    /// What the Prometheus exporter renders for `count`, as `kind name labels`.
    #[cfg(feature = "metrics")]
    fn rendered(count: fn()) -> BTreeSet<String> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, count);
        let text = handle.render();
        let mut kinds = HashMap::new();
        let mut series = BTreeSet::new();
        for line in text.lines() {
            if let Some(typed) = line.strip_prefix("# TYPE ") {
                let (name, kind) = typed.split_once(' ').unwrap_or((typed, ""));
                kinds.insert(
                    name.to_string(),
                    if kind == "summary" { "histogram" } else { kind },
                );
            } else if let Some((name, labels)) = line.split_once('{') {
                let labels = labels.split_once('}').map_or("", |(l, _)| l);
                if !labels.contains("quantile") {
                    let name = name.trim_end_matches("_sum").trim_end_matches("_count");
                    series.insert(format!(
                        "{} {name} {}",
                        kinds[name],
                        labels.replace(',', " ")
                    ));
                }
            }
        }
        series
    }

    /// Every count renders exactly the series the census expands it to, with
    /// the names and labels the transports' literal macros gave them.
    #[cfg(feature = "metrics")]
    #[test]
    fn each_count_renders_its_series() {
        for (name, count, series) in COUNTS {
            let want: BTreeSet<String> = series
                .iter()
                .map(|s| format!("{s} transport=\"probe\""))
                .collect();
            assert_eq!(rendered(count), want, "{name}");
        }
    }

    /// The Rust files of `dir` and below, test modules cut, test files left out.
    fn sources(dir: &Path, out: &mut Vec<(PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.is_dir() && name != "test_support" {
                sources(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs")
                && !name.contains("test")
                && name != "core.rs"
            {
                let text = std::fs::read_to_string(&path).unwrap();
                let cut = text.find("#[cfg(test)]\nmod tests {").unwrap_or(text.len());
                out.push((path, text[..cut].to_string()));
            }
        }
    }

    /// `text`, which follows a `(`, up to its matching `)`.
    fn parenthesised(text: &str) -> &str {
        let mut depth = 1;
        let end = text.char_indices().find(|&(_, c)| {
            depth += i32::from(c == '(') - i32::from(c == ')');
            depth == 0
        });
        &text[..end.map_or(text.len(), |(i, _)| i)]
    }

    /// Top-level comma-separated arguments.
    fn arguments(args: &str) -> Vec<String> {
        let (mut out, mut depth, mut current) = (Vec::new(), 0, String::new());
        for c in args.chars() {
            depth += i32::from("([{".contains(c)) - i32::from(")]}".contains(c));
            if c == ',' && depth == 0 {
                out.push(std::mem::take(&mut current));
            } else {
                current.push(c);
            }
        }
        out.push(current);
        out.into_iter()
            .map(|a| a.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|a| !a.is_empty())
            .collect()
    }

    /// `src/transport`'s metric series, one `kind name labels` line each: every
    /// literal `metrics::` call, and every [`Meter`] count under its
    /// transport's label. A label value that is not a literal shows as `<expr>`.
    fn census() -> BTreeSet<String> {
        let mut files = Vec::new();
        sources(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport"),
            &mut files,
        );
        let transport_of = |path: &Path| -> String {
            let rel = path.strip_prefix(env!("CARGO_MANIFEST_DIR")).unwrap();
            let top = rel
                .components()
                .nth(2)
                .unwrap()
                .as_os_str()
                .to_string_lossy();
            top.trim_end_matches(".rs").to_string()
        };
        let mut labels = HashMap::new();
        for (path, text) in &files {
            for (at, _) in text.match_indices("impl Meter for ") {
                let rest = &text[at + "impl Meter for ".len()..];
                let ty = rest.split(' ').next().unwrap().to_string();
                let label = rest.split('"').nth(1).unwrap().to_string();
                labels.insert(transport_of(path), label.clone());
                labels.insert(ty, label);
            }
        }
        let mut series = BTreeSet::new();
        for (path, text) in &files {
            for kind in ["counter", "gauge", "histogram"] {
                for (at, _) in text.match_indices(&format!("metrics::{kind}!(")) {
                    let args = arguments(parenthesised(&text[at + kind.len() + 11..]));
                    let shown: Vec<String> = args[1..]
                        .iter()
                        .filter_map(|a| a.split_once(" => "))
                        .map(|(k, v)| {
                            let v = if v.starts_with('"') {
                                v.to_string()
                            } else {
                                format!("<{v}>")
                            };
                            format!("{}={v}", k.trim_matches('"'))
                        })
                        .collect();
                    let name = match args[0].strip_prefix('"') {
                        Some(literal) => literal.trim_end_matches('"').to_string(),
                        None => format!("<{}>", args[0]),
                    };
                    series.insert(
                        format!("{kind} {name} {}", shown.join(" "))
                            .trim_end()
                            .to_string(),
                    );
                }
            }
            for (name, _, adds) in COUNTS {
                for (at, _) in text.match_indices(&format!("::{name}(")) {
                    let ty = text[..at]
                        .rsplit(|c: char| !c.is_alphanumeric() && c != '_')
                        .next()
                        .unwrap();
                    let key = if ty == "Self" {
                        transport_of(path)
                    } else {
                        ty.to_string()
                    };
                    for added in adds {
                        series.insert(format!("{added} transport=\"{}\"", labels[&key]));
                    }
                }
            }
        }
        series
    }

    /// The transports' metric set matches the committed one, so a change to a
    /// name, a label or the transport that counts it shows in review.
    /// `env SCALO_WRITE_TRANSPORT_METRICS=1` rewrites the file after a
    /// deliberate change.
    #[test]
    fn transport_metrics_match_the_committed_set() {
        let got: String = census().into_iter().map(|line| line + "\n").collect();
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
        if std::env::var_os("SCALO_WRITE_TRANSPORT_METRICS").is_some() {
            std::fs::write(&path, &got).unwrap();
        }
        let want = std::fs::read_to_string(&path).unwrap_or_default();
        pretty_assertions::assert_eq!(want, got, "{FIXTURE} is out of date");
    }
}
