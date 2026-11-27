// Project:   scalo
// File:      src/metrics/catalogue.rs
// Purpose:   The manifest entries for the series scalo's modules write
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The manifest entries for the series scalo's modules write.
//!
//! [`MetricsManager`](super::MetricsManager) describes them when it is built,
//! so a manifest lists what a service serves whether or not its app describes
//! anything. Each entry sits under the feature of the code that writes it, so a
//! build lists only series it can serve.
//!
//! Three other places describe the rest of scalo's series, each for the names
//! it owns: `ServiceMetrics::register` (the transport, pipeline, record, scaling,
//! spool, DLQ and security set), the worker pool and batch engine `describe`
//! functions, and the opt-in groups an app builds. A name is described in one
//! of these places. The `every_series_scalo_writes_is_in_the_manifest` test
//! reads the source and fails on a written series none of them describes, and
//! on a described one nothing writes.

use super::{MetricRegistry, Series};

/// `records_received_total`, which `ServiceMetrics` counts and `AppMetrics`
/// holds a handle on.
pub(crate) fn records_received_total() -> Series<'static> {
    Series::counter(
        "records_received_total",
        "Records received from all sources",
    )
}

/// `config_reloads_total`, which the config reloader counts and `AppMetrics`
/// holds handles on.
#[cfg(any(
    feature = "service-metrics",
    all(feature = "config-reload", feature = "metrics")
))]
pub(crate) fn config_reloads_total() -> Series<'static> {
    Series::counter("config_reloads_total", "Config reload attempts").labels(&["result"])
}

/// Describe every series scalo's modules write that no other describer
/// owns, into `registry` and the current recorder.
pub(crate) fn describe_library(registry: &MetricRegistry) {
    process_and_container(registry);
    #[cfg(feature = "metrics")]
    registry.describe(
        "platform",
        &[Series::counter(
            "metrics_server_connections_refused_total",
            "Scrape connections closed unanswered because the server was at its connection cap",
        )],
    );
    #[cfg(feature = "shutdown")]
    registry.describe(
        "platform",
        &[Series::counter(
            "pod_eviction_received_total",
            "SIGTERMs received while running in Kubernetes",
        )],
    );
    #[cfg(all(feature = "config-reload", feature = "metrics"))]
    registry.describe("app", &[config_reloads_total()]);
    #[cfg(feature = "dlq")]
    dlq(registry);
    #[cfg(feature = "worker-pool")]
    worker_pool(registry);
    #[cfg(all(feature = "io", feature = "metrics"))]
    file_writer(registry);
    #[cfg(all(feature = "http", feature = "metrics"))]
    http_client(registry);
    #[cfg(feature = "governor")]
    governor(registry);
    #[cfg(all(feature = "secrets", feature = "metrics"))]
    secrets(registry);
    #[cfg(all(feature = "sink-stack", feature = "metrics"))]
    registry.describe(
        "platform",
        &[Series::counter(
            "sink_stack_retries_total",
            "Sends the sink stack retried after a transient failure",
        )],
    );
    #[cfg(all(feature = "strmatch", feature = "metrics"))]
    registry.describe(
        "platform",
        &[Series::counter(
            "strmatch_regex_fallback_total",
            "Matches that fell back to the regex engine",
        )],
    );
    #[cfg(feature = "tls")]
    tls(registry);
    #[cfg(feature = "transport")]
    transport_filter(registry);
    #[cfg(feature = "transport-kafka")]
    kafka(registry);
    #[cfg(feature = "transport-syslog")]
    syslog(registry);
    #[cfg(any(feature = "transport-kafka", feature = "transport-syslog"))]
    registry.describe(
        "platform",
        &[Series::counter(
            "transport_dropped_total",
            "Records a transport dropped before delivery, by reason",
        )
        .labels(&["transport", "reason"])],
    );
    #[cfg(all(feature = "transport-proxy", feature = "metrics"))]
    registry.describe(
        "platform",
        &[Series::counter(
            "transport_proxy_refused_total",
            "CONNECT requests a proxy refused, by transport",
        )
        .labels(&["transport"])],
    );
    #[cfg(feature = "wasm")]
    wasm(registry);
    #[cfg(feature = "service-metrics")]
    registry.describe(
        "usage",
        &[Series::counter(
            "usage_ingested_bytes_total",
            "Payload bytes accepted for delivery, by the org each record was stamped with",
        )
        .labels(&["org_id"])],
    );
}

/// Process gauges, written while `metrics.enable_process_metrics` is on, and
/// container gauges, while `metrics.enable_container_metrics` is.
fn process_and_container(registry: &MetricRegistry) {
    use metrics::Unit;

    registry.describe(
        "process",
        &[
            Series::gauge(
                "process_cpu_seconds_total",
                "Total user and system CPU time spent in seconds",
            )
            .unit(Unit::Seconds),
            Series::gauge(
                "process_resident_memory_bytes",
                "Resident memory size in bytes",
            )
            .unit(Unit::Bytes),
            Series::gauge(
                "process_virtual_memory_bytes",
                "Virtual memory size in bytes",
            )
            .unit(Unit::Bytes),
            Series::gauge("process_open_fds", "Number of open file descriptors").unit(Unit::Count),
            Series::gauge(
                "process_start_time_seconds",
                "Start time of the process since unix epoch in seconds",
            )
            .unit(Unit::Seconds),
        ],
    );
    #[cfg(windows)]
    registry.describe(
        "process",
        &[Series::gauge("process_open_handles", "Number of open handles").unit(Unit::Count)],
    );
    registry.describe(
        "container",
        &[
            Series::gauge(
                "container_memory_limit_bytes",
                "Container memory limit in bytes",
            )
            .unit(Unit::Bytes),
            Series::gauge(
                "container_memory_usage_bytes",
                "Container memory usage in bytes",
            )
            .unit(Unit::Bytes),
            Series::gauge("container_cpu_limit_cores", "Container CPU limit in cores"),
        ],
    );
}

/// The DLQ's own series. The background sink under the orchestrator names its
/// five from a metric prefix at run time, and the orchestrator sets `dlq`.
#[cfg(feature = "dlq")]
fn dlq(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[
            Series::counter(
                "dlq_backend_start_failures_total",
                "DLQ backends that failed to start, by backend",
            )
            .labels(&["backend"]),
            Series::counter(
                "dlq_pushed_total",
                "Dead letters the DLQ's background sink accepted",
            ),
            Series::counter(
                "dlq_writes_total",
                "Batch writes the DLQ's background sink attempted",
            ),
            Series::counter(
                "dlq_write_errors_total",
                "Batch writes of the DLQ's background sink that returned an error",
            ),
            Series::gauge(
                "dlq_pending",
                "Dead letters queued in the DLQ's background sink",
            ),
        ],
    );
    #[cfg(feature = "metrics")]
    registry.describe(
        "platform",
        &[
            Series::counter("dlq_entries_total", "Dead letters the file backend wrote"),
            Series::gauge(
                "dlq_entries_written",
                "Lines the file backend's current file holds",
            ),
        ],
    );
    #[cfg(all(feature = "dlq-http", feature = "metrics"))]
    registry.describe(
        "platform",
        &[Series::counter(
            "dlq_sent_total",
            "Dead letters the HTTP backend delivered, by backend",
        )
        .labels(&["backend"])],
    );
}

/// The worker pool's configured thresholds, written at start and on a config
/// reload, and the pool's fan-out series. The pool's operational series are
/// described by `worker::metrics::describe`.
#[cfg(feature = "worker-pool")]
fn worker_pool(registry: &MetricRegistry) {
    use metrics::Unit;

    registry.describe(
        "custom",
        &[
            Series::gauge("worker_pool_min_threads", "Configured minimum pool threads"),
            Series::gauge(
                "worker_pool_grow_below",
                "Configured utilisation below which the pool grows",
            ),
            Series::gauge(
                "worker_pool_shrink_above",
                "Configured utilisation above which the pool shrinks",
            ),
            Series::gauge(
                "worker_pool_emergency_above",
                "Configured utilisation above which the pool sheds to its minimum",
            ),
            Series::gauge(
                "worker_pool_memory_pressure_cap",
                "Configured memory pressure above which the pool stops growing",
            ),
            Series::gauge(
                "worker_pool_scale_interval_secs",
                "Configured seconds between scaling decisions",
            )
            .unit(Unit::Seconds),
            Series::gauge(
                "worker_pool_async_concurrency",
                "Configured ceiling on async fan-out tasks in flight",
            ),
            Series::gauge(
                "worker_pool_health_saturation_timeout_secs",
                "Configured seconds of saturation before the pool reports unhealthy",
            )
            .unit(Unit::Seconds),
        ],
    );
    #[cfg(feature = "memory")]
    registry.describe(
        "custom",
        &[Series::gauge(
            "worker_pool_memory_psi_some",
            "Share of recent time some task stalled on memory, from the kernel's pressure stall information",
        )],
    );
    #[cfg(feature = "metrics")]
    registry.describe(
        "custom",
        &[
            Series::gauge("fanout_inflight", "Async fan-out tasks in flight"),
            Series::counter(
                "fanout_timeout_total",
                "Fan-out tasks that hit their timeout",
            ),
            Series::counter("fanout_panic_total", "Fan-out tasks that panicked"),
            Series::histogram(
                "fanout_batch_duration_seconds",
                "Time one async fan-out batch took",
            ),
        ],
    );
}

#[cfg(all(feature = "io", feature = "metrics"))]
fn file_writer(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[
            Series::counter(
                "file_writer_cut_bytes_total",
                "Bytes cut from a line to fit the writer's line ceiling",
            )
            .unit(metrics::Unit::Bytes)
            .labels(&["writer"]),
            Series::counter(
                "file_writer_rotations_total",
                "File rotations, by writer and the trigger that fired",
            )
            .labels(&["writer", "trigger"]),
        ],
    );
}

#[cfg(all(feature = "http", feature = "metrics"))]
fn http_client(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[
            Series::counter(
                "http_client_requests_total",
                "HTTP client requests, by method and response status",
            )
            .labels(&["method", "status"]),
            Series::counter(
                "http_client_retries_total",
                "HTTP client requests retried, by method",
            )
            .labels(&["method"]),
            Series::histogram(
                "http_client_duration_seconds",
                "Time an HTTP client request took, by method",
            )
            .labels(&["method"]),
        ],
    );
}

/// The governor's gate, byte budget and rate limiters.
#[cfg(feature = "governor")]
fn governor(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[Series::gauge(
            "self_regulation_pressure_ratio",
            "Combined self-regulation pressure level (0.0-1.0)",
        )],
    );
    #[cfg(feature = "metrics")]
    registry.describe(
        "platform",
        &[
            Series::gauge(
                "self_regulation_byte_budget",
                "Current AIMD byte budget (inbound block size lever)",
            ),
            Series::gauge(
                "self_regulation_inbound_paused",
                "1 while the inbound gate is holding under pressure, else 0 (per source)",
            )
            .labels(&["source"]),
            Series::counter(
                "self_regulation_inbound_pauses_total",
                "Inbound gate pause (rising-edge) events (per source)",
            )
            .labels(&["source"]),
            Series::counter(
                "self_regulation_max_hold_releases_total",
                "Inbound holds ended by max_hold with pressure above resume_below (per signal)",
            )
            .labels(&["signal"]),
            Series::counter(
                "rate_limited_total",
                "Calls a rate limiter delayed, by limiter",
            )
            .labels(&["limiter"]),
            Series::histogram(
                "rate_limit_wait_seconds",
                "Time a call waited on a rate limiter, by limiter",
            )
            .labels(&["limiter"]),
        ],
    );
}

#[cfg(all(feature = "secrets", feature = "metrics"))]
fn secrets(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[
            Series::counter(
                "secrets_cache_hits_total",
                "Secret reads served from the cache",
            ),
            Series::counter(
                "secrets_cache_misses_total",
                "Secret reads the cache could not serve",
            ),
            Series::counter("secrets_fetch_total", "Secrets fetched from a backend"),
        ],
    );
}

#[cfg(feature = "tls")]
fn tls(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[
            Series::counter(
                "tls_handshakes_total",
                "Completed server TLS handshakes, by protocol and key exchange",
            )
            .labels(&["protocol", "key_exchange"]),
            Series::counter(
                "tls_handshake_failures_total",
                "Server TLS handshakes that failed, by reason",
            )
            .labels(&["reason"]),
            Series::counter(
                "tls_certificate_reloads_total",
                "Server certificate reloads, by result",
            )
            .labels(&["result"]),
            Series::counter(
                "tls_tls12_handshakes_total",
                "TLS 1.2 handshakes under a pinned profile, by role and profile",
            )
            .labels(&["role", "profile"]),
        ],
    );
}

/// The transport filter's series, which every transport's filter writes.
#[cfg(feature = "transport")]
fn transport_filter(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[Series::counter(
            "transport_filtered_total",
            "Records the transport filter dropped or sent to the DLQ, by direction and action",
        )
        .labels(&["direction", "action"])],
    );
    #[cfg(feature = "metrics")]
    registry.describe(
        "platform",
        &[
            Series::counter(
                "transport_filter_msgpack_bypass_total",
                "MsgPack records the filters could not read, by direction",
            )
            .labels(&["direction"]),
            Series::counter(
                "transport_filter_cel_payload_skip_total",
                "Records a CEL filter skipped because the payload is over the size cap",
            ),
            Series::counter(
                "transport_filter_cel_eval_errors_total",
                "Records a CEL filter could not evaluate: a missing field, a type mismatch or a non-bool result",
            ),
        ],
    );
}

#[cfg(feature = "transport-kafka")]
fn kafka(registry: &MetricRegistry) {
    let transport = ["transport"];
    registry.describe(
        "platform",
        &[
            Series::counter(
                "transport_auth_refused_total",
                "Kafka requests a broker refused for credentials, by transport",
            )
            .labels(&transport),
            Series::counter(
                "transport_refusal_rebuilds_total",
                "Kafka clients rebuilt after a credential refusal, by transport",
            )
            .labels(&transport),
            Series::counter(
                "transport_send_missing_topic_total",
                "Kafka sends refused because the destination names no topic, by transport",
            )
            .labels(&transport),
        ],
    );
    #[cfg(feature = "metrics")]
    registry.describe(
        "platform",
        &[
            Series::histogram("kafka_poll_duration_seconds", "Time one Kafka poll took"),
            Series::gauge(
                "rdkafka_global_msg_cnt",
                "Messages in the client's producer queue",
            )
            .labels(&["client_id", "client_type"]),
            Series::gauge(
                "rdkafka_global_msg_size_bytes",
                "Bytes in the client's producer queue",
            )
            .unit(metrics::Unit::Bytes)
            .labels(&["client_id", "client_type"]),
            Series::gauge(
                "rdkafka_broker_rtt_avg_seconds",
                "Average round-trip time to a broker",
            )
            .unit(metrics::Unit::Seconds)
            .labels(&["client_id", "client_type", "broker"]),
            Series::gauge(
                "rdkafka_broker_outbuf_cnt",
                "Requests queued to send to a broker",
            )
            .labels(&["client_id", "client_type", "broker"]),
            Series::gauge(
                "rdkafka_broker_waitresp_cnt",
                "Requests sent to a broker and awaiting a response",
            )
            .labels(&["client_id", "client_type", "broker"]),
            Series::gauge(
                "rdkafka_topic_partition_consumer_lag",
                "Records past the committed offset on a partition, from librdkafka's statistics",
            )
            .labels(&["client_id", "client_type", "topic", "partition"]),
            Series::gauge(
                "rdkafka_topic_partition_committed_offset",
                "Committed offset of a partition, from librdkafka's statistics",
            )
            .labels(&["client_id", "client_type", "topic", "partition"]),
            Series::gauge(
                "rdkafka_consumer_rebalance_count",
                "Rebalances the consumer group has run, from librdkafka's statistics",
            )
            .labels(&["client_id", "client_type"]),
        ],
    );
    #[cfg(all(feature = "governor", feature = "metrics"))]
    registry.describe(
        "platform",
        &[
            Series::gauge(
                "kafka_partition_limited",
                "1 while the consumer has more members than partitions to share, else 0",
            ),
            Series::counter(
                "self_regulation_kafka_gate_errors_total",
                "Kafka pause/resume actuator failures (brake degraded), by operation",
            )
            .labels(&["op"]),
        ],
    );
    #[cfg(all(feature = "service-metrics", feature = "metrics"))]
    registry.describe("consumer", &super::groups::consumer_series());
}

#[cfg(feature = "transport-syslog")]
fn syslog(registry: &MetricRegistry) {
    registry.describe(
        "platform",
        &[Series::counter(
            "transport_connections_closed_total",
            "Syslog connections closed, by transport and reason",
        )
        .labels(&["transport", "reason"])],
    );
}

#[cfg(feature = "wasm")]
fn wasm(registry: &MetricRegistry) {
    let plugin = ["plugin"];
    registry.describe(
        "platform",
        &[
            Series::gauge(
                "wasm_plugin_info",
                "1 for each loaded plugin, with its digest and the signer that vouched for it",
            )
            .labels(&["plugin", "digest", "signer"]),
            Series::counter(
                "wasm_plugin_loads_total",
                "Plugin load attempts, by plugin and outcome",
            )
            .labels(&["plugin", "outcome"]),
            Series::histogram(
                "wasm_plugin_compile_duration_seconds",
                "Time a plugin took to compile",
            )
            .labels(&plugin),
            Series::counter(
                "wasm_plugin_calls_total",
                "Plugin calls, by plugin and outcome",
            )
            .labels(&["plugin", "outcome"]),
            Series::histogram(
                "wasm_plugin_call_duration_seconds",
                "Time a plugin call took",
            )
            .labels(&plugin),
            Series::counter(
                "wasm_plugin_instantiations_total",
                "Instances created for a plugin",
            )
            .labels(&plugin),
            Series::gauge(
                "wasm_plugin_memory_bytes",
                "Peak linear memory a plugin instance has used",
            )
            .unit(metrics::Unit::Bytes)
            .labels(&plugin),
            Series::gauge(
                "wasm_pool_component_instances",
                "Component instances the pooling allocator holds",
            ),
            Series::gauge(
                "wasm_pool_core_instances",
                "Core module instances the pooling allocator holds",
            ),
            Series::gauge(
                "wasm_pool_memories",
                "Linear memories the pooling allocator holds",
            ),
            Series::gauge("wasm_pool_tables", "Tables the pooling allocator holds"),
        ],
    );
}

/// Every feature a described series sits under, so the source census compares
/// against a build that can serve all of them.
#[cfg(all(
    test,
    feature = "metrics",
    feature = "service-metrics",
    feature = "worker",
    feature = "transport",
    feature = "transport-kafka",
    feature = "transport-syslog",
    feature = "transport-proxy",
    feature = "dlq",
    feature = "dlq-http",
    feature = "io",
    feature = "http",
    feature = "governor",
    feature = "memory",
    feature = "secrets",
    feature = "sink-stack",
    feature = "strmatch",
    feature = "tls",
    feature = "wasm",
    feature = "shutdown",
    feature = "config-reload",
    feature = "spool",
    feature = "tiered-sink",
    feature = "rules"
))]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::metrics::groups::{
        AppMetrics, BackpressureMetrics, BufferMetrics, CircuitBreakerMetrics, ConsumerMetrics,
        EnrichmentMetrics, SchemaCacheMetrics, SinkMetrics,
    };
    use crate::metrics::{MetricsManager, ServiceMetrics};
    use crate::source_census::{described_names, metric_names};

    /// The sites that name their series at run time: the file under `src`, the
    /// expression, and the series it writes.
    const DYNAMIC: &[(&str, &str, &[&str])] = &[
        // The background sink under the DLQ orchestrator, which sets the prefix `dlq`.
        (
            "concurrency/sink.rs",
            r#"format!("{p}_pushed_total")"#,
            &["dlq_pushed_total"],
        ),
        (
            "concurrency/sink.rs",
            r#"format!("{p}_dropped_total")"#,
            &["dlq_dropped_total"],
        ),
        (
            "concurrency/sink.rs",
            r#"format!("{p}_writes_total")"#,
            &["dlq_writes_total"],
        ),
        (
            "concurrency/sink.rs",
            r#"format!("{p}_write_errors_total")"#,
            &["dlq_write_errors_total"],
        ),
        (
            "concurrency/sink.rs",
            r#"format!("{p}_pending")"#,
            &["dlq_pending"],
        ),
        // A name held in a local, shared with the reloader's literal.
        (
            "metrics/groups/app.rs",
            "config_key",
            &["config_reloads_total"],
        ),
        // The consumer group's names, held in constants.
        ("metrics/groups/consumer.rs", "LAG", &["consumer_lag"]),
        (
            "metrics/groups/consumer.rs",
            "self.0",
            &["consumer_partitions_assigned", "consumer_rebalance_total"],
        ),
        // The Kafka statistics writers take their name from a constant.
        (
            "transport/kafka/metrics.rs",
            "name",
            &[
                "rdkafka_topic_partition_consumer_lag",
                "rdkafka_topic_partition_committed_offset",
                "consumer_lag",
                "consumer_partitions_assigned",
                "consumer_rebalance_total",
            ],
        ),
        // The queue's failure counter, named by its callers.
        (
            "yaque/mod.rs",
            "name",
            &[
                "spool_torn_entries_cut_total",
                "spool_position_save_failures_total",
                "spool_segment_delete_failures_total",
            ],
        ),
        // The manager's own constructors, which write an app's series under its names.
        ("metrics/mod.rs", "name.to_string()", &[]),
    ];

    /// The names a manager describes with every scalo describer run on it, bare.
    fn described() -> BTreeSet<String> {
        let manager = MetricsManager::new_for_test("");
        super::describe_library(&manager.registry());
        let _service = ServiceMetrics::register(&manager);
        let _app = AppMetrics::new(&manager, "1.0.0", "abc");
        let _ = (
            BackpressureMetrics::new(&manager),
            BufferMetrics::new(&manager),
            CircuitBreakerMetrics::new(&manager),
            ConsumerMetrics::new(&manager),
            EnrichmentMetrics::new(&manager),
            SchemaCacheMetrics::new(&manager),
            SinkMetrics::new(&manager),
        );
        crate::worker::metrics::describe(&manager);
        crate::worker::engine::metrics::describe(&manager);
        manager
            .registry()
            .manifest()
            .metrics
            .into_iter()
            .map(|m| m.name)
            .collect()
    }

    /// A series scalo's source writes is in the manifest, and a series in the
    /// manifest is one the source writes.
    #[test]
    fn every_series_scalo_writes_is_in_the_manifest() {
        let census = metric_names();
        let mut written = census.literal.clone();
        let mut unlisted = Vec::new();
        for (file, expr) in &census.dynamic {
            match DYNAMIC.iter().find(|(f, e, _)| f == file && e == expr) {
                Some((_, _, names)) => written.extend(names.iter().map(ToString::to_string)),
                None => unlisted.push(format!("{file}: {expr}")),
            }
        }
        assert!(
            unlisted.is_empty(),
            "a series named at run time needs a row in DYNAMIC with the names it writes: {unlisted:#?}"
        );
        let stale_rows: Vec<&(&str, &str, &[&str])> = DYNAMIC
            .iter()
            .filter(|(f, e, _)| {
                !census
                    .dynamic
                    .contains(&((*f).to_string(), (*e).to_string()))
            })
            .collect();
        assert!(
            stale_rows.is_empty(),
            "DYNAMIC rows no source site matches: {stale_rows:#?}"
        );
        // Written only on Windows, so only a Windows build describes it.
        if !cfg!(windows) {
            written.remove("process_open_handles");
        }

        let described = described();
        let missing: Vec<&String> = written.difference(&described).collect();
        let stale: Vec<&String> = described.difference(&written).collect();
        assert!(
            missing.is_empty(),
            "written by scalo, in no manifest: {missing:#?}"
        );
        assert!(
            stale.is_empty(),
            "in the manifest, written by no scalo source: {stale:#?}"
        );
    }

    /// One name has one description, group and label set, whichever path a
    /// service reaches it by.
    #[test]
    fn a_series_is_described_in_one_place() {
        let mut places: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (file, name) in described_names() {
            places.entry(name).or_default().push(file);
        }
        places.retain(|_, files| files.len() > 1);
        assert!(
            places.is_empty(),
            "described in more than one place: {places:#?}"
        );
    }
}
