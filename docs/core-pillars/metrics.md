# Metrics

`MetricsManager::new("dfe_loader")` installs a global `metrics` recorder, builds a
Prometheus exporter, and serves `/metrics`. Any module then calls
`metrics::counter!` / `gauge!` / `histogram!` -- the macros are no-ops with no
recorder, so library code compiles without the metrics feature.

Every counter / gauge / histogram built through `MetricsManager` also pushes a
`MetricDescriptor` into a `MetricRegistry`, rendered as JSON at `/metrics/manifest`
and to `docs/metrics-manifest.json` via the `metrics-manifest` CLI subcommand.
That manifest is the only source of metric metadata for downstream tools (Grafana
provisioning, alert validators, the data-plane docs site).

Process and container metrics (RSS, CPU, FDs, cgroup limits) auto-collect on a
fixed interval via [sysinfo](https://crates.io/crates/sysinfo) when the relevant
features are on; container metrics read cgroup v1 and v2 transparently.

---

## Feature tiers

| Feature | Adds | Use |
|---|---|---|
| `metrics-core` | Macros + `MetricRegistry` | Library crates that record but don't host the exporter |
| `metrics-process` | `metrics-core` + sysinfo process probe | Single binaries wanting RSS/CPU without an HTTP server |
| `metrics` | `metrics-process` + Prometheus exporter + `/metrics` server | Services |

Data-plane services pull `metrics`; published library crates pull `metrics-core` so they
don't drag a TCP listener into dependents.

---

## Setup

```rust
use scalo::metrics::MetricsManager;

let mut mgr = MetricsManager::new("dfe_loader");

// Construct metrics -- each call also registers a descriptor
let sent = mgr.counter("transport_sent_total", "Messages sent");
let lag = mgr.histogram("send_latency_seconds", "Send latency");

// Declare label keys + group at construction:
mgr.counter_with_labels("transport_sent_total", "Messages sent",
    &["transport", "topic"], "transport");
// Apply label values at recording time:
metrics::counter!("dfe_loader_transport_sent_total",
    "transport" => "kafka", "topic" => "events").increment(1);

mgr.start_server("0.0.0.0:9090").await?;
```

Names are namespace-prefixed automatically -- `counter("foo")` records as
`dfe_loader_foo`. Use `*_with_labels` so the label keys and group land in the
manifest (i.e. nearly always; unlabeled metrics are rare in data-plane pipelines).

Single-binary services use `ServiceRuntime` from `cli`, which constructs the
manager, wires the readiness callback, attaches optional `ScalingPressure` and
`MemoryGuard` endpoints, and merges service routes via `start_server_with_routes`.
See [../runtime/service-runtime.md](../runtime/service-runtime.md).

---

## The manifest

The registry tracks: name, type, description, unit, label keys, group, bucket
spec, use cases, dashboard hint, app version, git commit, registration timestamp.

`GET /metrics/manifest` returns the full catalogue:

```json
{
  "schema_version": 1,
  "app": "my-app",
  "namespace": "",
  "version": "x.y.z",
  "commit": "<git-sha>",
  "registered_at": "<rfc3339-timestamp>",
  "metrics": [
    {
      "name": "transport_sent_total",
      "type": "counter",
      "description": "Messages successfully sent to transport",
      "labels": ["transport"],
      "group": "platform"
    }
  ]
}
```

`app` is the service name. `namespace` is the `metrics.namespace` prefix every
name carries, empty (bare names) by default. A name is listed once, and the first
descriptor stands: the runtime describes its own metrics before the app does, so
an app describing a platform metric again cannot strip its labels, group, use
cases or dashboard hint. `registered_at` is when the running service's registry
was created.

The manifest lists what the registry holds, not everything `/metrics` serves:
the process, container, HTTP client and memory guard gauges are served but not
yet described (scalo-rs#137).

Two CLI subcommands produce the same JSON without running the service, reading
`metrics.namespace` from the same config cascade the service loads.
`metrics-manifest` prints it to stdout, `generate-artefacts` writes
`metrics-manifest.json` into its output directory, and the two are
byte-identical. Neither stamps a time: `registered_at` is empty, so two runs
over the same build and config give the same bytes. `version` and `commit`
change with the build, and anything read from the cascade changes with the
config and environment the command runs under, so a dev box and CI can differ:

```bash
my-app metrics-manifest > docs/metrics-manifest.json
my-app generate-artefacts --output-dir docs/
```

Add metadata after registration:

```rust
mgr.set_use_cases("dfe_loader_send_latency_seconds",
    &["SLO p99 < 500ms", "Page on sustained > 1s"]);
mgr.set_dashboard_hint("dfe_loader_send_latency_seconds", "heatmap");
mgr.set_build_info(env!("CARGO_PKG_VERSION"), env!("GIT_COMMIT"));
```

`ServiceMetrics::register(&mgr)` describes the canonical data-plane metric set --
transport, pipeline, records, scaling, spool, security -- in one call so every
consumer service exports the same metrics with matching labels. The service runtime
and both manifest subcommands describe it for you, with app info (feature
`service-metrics`) and the worker pool and batch engine sets when those features are
compiled in, so a service's `register_metrics` override describes only its own.

### Count each thing once

A name with no labels is one series, whichever handle writes it, so two call sites
for one event double the count:

| Series | Counted by | Not also by |
|---|---|---|
| `records_received_total` | `ServiceMetrics::records_received`, once per record | `AppMetrics::record_received`, or an `increment` on `AppMetrics::records_received`. That field is the same series, for an app that sets the total with `absolute` |
| `transport_*` for a scalo transport | the transport itself, under its own `transport` label | the matching `ServiceMetrics::transport_*` method |
| `transport_*` for a sink or source scalo does not provide | the `ServiceMetrics::transport_*` methods | -- |

A counter emitted both with and without labels is two series under one name, and a
`sum()` across labels adds them. Emit it one way.

---

## Transport throughput

The transport layer counts both events AND bytes, in both directions, modelled on
Vector's component instrumentation. All carry the `transport` label (backend kind:
`kafka` / `grpc` / `http` / `file` / `pipe`, plus `routed` for the
aggregate routed view). Bytes are RAW wire bytes (summed `payload.len()` per
`WorkBatch`), incremented once per batch send/recv -- not per event.

| Metric | Direction | Meaning |
|---|---|---|
| `transport_sent_total` | egress | events written to the wire |
| `transport_sent_bytes_total` | egress | raw bytes written to the wire |
| `transport_received_events_total` | ingress | events read off the wire |
| `transport_received_bytes_total` | ingress | raw bytes read off the wire |

Transport-level ingress counts raw wire receipt (post-filter, pre-decode) and is
distinct from the pipeline-level `records_received_total` (post-decode records).
scalo's transports record these series themselves, so an app using one does not
call the matching `ServiceMetrics::transport_*` method as well.
Graph volume with a rate query, e.g. egress bytes/sec by backend:

```promql
sum by (transport) (rate(transport_sent_bytes_total[1m]))
```

### Outages

A source or sink that goes away is waited out rather than ending the app, and these count what that cost:

| Metric | Labels | Meaning |
|---|---|---|
| `transport_recv_errors_total` | `transport`, `class` | receive failures; `class="transient"` were retried, `class="permanent"` were returned. Kafka emits it |
| `transport_commit_errors_total` | `transport` | source commits that failed after the block was delivered; the `BatchEngine` driver counts them and carries on |
| `pipeline_retries_total` | `stage` | `BatchEngine` run-loop steps retried after a transient failure, `stage` being `recv` or `sink` |

A rising `transport_recv_errors_total{class="transient"}` or `pipeline_retries_total` with flat throughput is an outage being ridden out. Behaviour per backend: [../transport/backends.md](../transport/backends.md).

---

## Endpoints

| Path | Body |
|---|---|
| `/metrics` | Prometheus text |
| `/metrics/manifest` | JSON catalogue |
| `/livez` | `{"status":"alive"}` -- process alive |
| `/readyz` | 200 if readiness callback + [`HealthRegistry`](health.md) both pass, else 503 |
| `/scaling/pressure` | Float `0.0-1.0` (feature `scaling` + `set_scaling_pressure`) |
| `/memory/pressure` | JSON ratio + bytes (feature `memory` + `set_memory_guard`) |

`/metrics/manifest` is matched before `/metrics` in the prefix-match handler --
don't reorder.

---

## OTel mode

With `otel-metrics`, `MetricsManager` installs an OTel SDK meter provider and
pushes via OTLP. With **both** `metrics` and `otel-metrics`, a `metrics-util`
`FanoutBuilder` composes the two recorders so every macro records to both --
`/metrics` for scrape, OTLP for push. Call `mgr.shutdown_otel()` before exit to
flush the batch exporter, or lose the last interval's data. See
[`OtelMetricsConfig`](../../src/metrics/otel_types.rs) for endpoint / protocol /
batching config.

---

## API surface

| Item | Purpose |
|---|---|
| `MetricsManager::new(namespace)` | Construct + install recorder |
| `MetricsManager::with_config(MetricsConfig)` | Custom namespace, intervals, OTel config |
| `MetricsManager::new_for_test(namespace)` *(test only)* | No global install -- safe for parallel tests |
| `counter` / `gauge` / `histogram` | Construct + auto-register |
| `*_with_labels(name, desc, labels, group)` | Same, with manifest label keys + group |
| `histogram_with_buckets` | Custom bucket spec (captured in manifest) |
| `set_readiness_check(fn)` | Wire the `/readyz` gate |
| `set_scaling_pressure(Arc<ScalingPressure>)` / `set_memory_guard(Arc<MemoryGuard>)` | Add `/scaling/pressure` / `/memory/pressure` |
| `set_build_info` / `set_use_cases` / `set_dashboard_hint` | Manifest metadata |
| `registry() -> MetricRegistry` | Cloneable handle for embedding `/metrics/manifest` in custom routers |
| `render_handle() -> Option<RenderHandle>` | Cloneable Prometheus text renderer for axum routes |
| `start_server(addr)` / `start_server_with_routes(addr, extra)` | Built-in router / merge service routes |
| `shutdown_otel()` | Flush OTLP batch exporter |
| `ServiceMetrics::register(&mgr)` | Canonical data-plane metric set (feature `service-metrics`) |
| `latency_buckets()` / `size_buckets()` | Standard histogram bucket presets |

---

## Testing

Parallel tests panic if multiple call `MetricsManager::new()` -- the global
Prometheus recorder installs once per process. Use `new_for_test()`: it skips the
install but keeps the registry, descriptor push, and namespacing, and the macros
become no-ops. Most tests verify descriptor registration and naming, not recorded
values; end-to-end recording is verified in the integration suite where one
fixture installs the recorder.

---

## Related

- [config.md](config.md) -- `MetricsConfig` sources from the cascade
- [logging.md](logging.md) -- sampled log + counter is the standard pair
- [health.md](health.md) -- `/readyz` consults `HealthRegistry`
- [tracing.md](tracing.md) -- OTel-metrics is configured separately from OTel-tracing
- [../runtime/service-runtime.md](../runtime/service-runtime.md) -- `ServiceRuntime` wires the manager
- [../auto-wiring.md](../auto-wiring.md), [../feature-flags.md](../feature-flags.md)
- Source: [`src/metrics/mod.rs`](../../src/metrics/mod.rs), [`src/metrics/manifest.rs`](../../src/metrics/manifest.rs), [`src/metrics/service.rs`](../../src/metrics/service.rs)
