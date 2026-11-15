# Tracing

`otel_tracing::build_tracer_layer(&cfg)?` builds an OpenTelemetry tracer with an
OTLP batch exporter and returns a `tracing-subscriber` layer with its
`SdkTracerProvider`. Add the layer to your
subscriber and `tracing::span!` / `#[instrument]` calls become OTel spans that
ship to a collector (Tempo, Jaeger, Honeycomb, Grafana Cloud) with no further
wiring.

It also installs the resulting `SdkTracerProvider` as the **global** OTel tracer
provider. This matters: the transport propagation layer
([`transport::propagation`](../../src/transport/propagation.rs)) reads
`opentelemetry::Context::current()` to format outgoing `traceparent` headers.
Without a global provider there is no current context to read, and the
distributed trace breaks at every transport hop.

Three features cover OTel. Turning on `otel` alone does not give you W3C wire
propagation, and the metrics bridge is a different module from the tracing bridge.

---

## Feature breakdown

| Feature | Provides | Pair with |
| --- | --- | --- |
| `otel` | OTel SDK + OTLP exporter crates for other modules | umbrella, rarely useful alone |
| `otel-metrics` | `metrics`-crate bridge to OTel meters | [metrics.md](metrics.md) |
| `otel-tracing` | `tracing-opentelemetry` layer bridging spans to OTel spans | this doc |
| `transport-trace` | `traceparent` formatting + injection at transport boundaries | [../transport/README.md](../transport/README.md) |

Full distributed tracing needs `otel-tracing` **and** `transport-trace`: the first
turns internal spans into exportable OTel spans, the second writes the current
context as a `traceparent` header on outgoing Kafka / gRPC / HTTP. Inbound HTTP and
gRPC record the header they receive as the span's `traceparent` field.

---

## Setup

```rust
use scalo::otel_tracing::{OtelTracingConfig, build_tracer_layer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

let cfg = OtelTracingConfig {
    service_name: "my-service".into(),
    endpoint: "http://otel-collector:4317".into(),
    ..Default::default()
};
let (otel_layer, provider) = build_tracer_layer(&cfg)?;

tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())   // stdout/stderr formatter
    .with(otel_layer)                          // OTLP export
    .init();

// On graceful shutdown -- flush the batch exporter or lose pending spans:
provider.shutdown()?;
```

Env-var overrides resolve at build time, so one config covers dev and prod:

| Var | Overrides |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `endpoint` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `protocol` (`grpc` / `http/protobuf`) |
| `OTEL_SERVICE_NAME` | `service_name` |

`run_app`'s logger setup wires all of this from the `otel_tracing` config section
when `otel-tracing` is enabled -- apps don't build the layer manually.

---

## Span flow

```rust
use tracing::{info_span, instrument, Instrument};

// Macro form -- span scope is the closure body
info_span!("process_batch", batch_size = items.len()).in_scope(|| {
    for item in items { process(item); }   // child spans nest automatically
});

// Attribute form -- span covers the whole fn
#[instrument(skip(payload), fields(payload_bytes = payload.len()))]
async fn handle(payload: Vec<u8>) -> Result<()> {
    fetch_metadata().await?;   // span context flows through
    Ok(())
}
```

`#[instrument]` and `.instrument(span)` are the only correct ways to attach a span
across `.await`. **Do not** hold `_guard = span.enter()` across `.await`:

```rust
// WRONG -- guard leaks into other tasks via the runtime's task-locals,
// span context attaches to whatever future the runtime polls next.
let _guard = span.enter();
fetch_data().await;

// RIGHT
let response = handle(req).instrument(span).await;
```

No lint catches this: `clippy::await_holding_lock` covers lock guards, not span
guards.

---

## W3C propagation

With `transport-trace`, `transport::propagation::current_traceparent()` returns the
current OTel context as a W3C `traceparent` header:

```text
00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

Transports inject it on outbound (Kafka headers, gRPC metadata, HTTP header).
Inbound HTTP and gRPC validate a received header and record it as the current
span's `traceparent` field. They do not restore the OTel context from it, so an
inbound span does not join the upstream trace. The injection lives in each
transport backend -- no per-app wiring.

`current_traceparent()` is gated behind `transport-trace`, **not** `otel`. Without
`transport-trace` it does not compile and transports silently skip propagation.

---

## Exporter

OTLP gRPC over `tonic` by default (endpoint `http://localhost:4317`), OTLP
HTTP/protobuf optional. With no endpoint configured the default follows the
protocol: `http://localhost:4318` once `protocol` or `OTEL_EXPORTER_OTLP_PROTOCOL`
picks `http`. On the `http` protocol `endpoint` is a base URL, the
way the OTel spec defines `OTEL_EXPORTER_OTLP_ENDPOINT`, and `/v1/traces` is
appended to it. An endpoint that already ends in `/v1/traces` is used as given.
The batch processor coalesces spans every 5 seconds
(`batch_scheduled_delay_ms`) with a 2 048-span queue (`batch_max_queue_size`).
Tune both for high-volume services.

The span resource carries the replica's identity, the same as the metrics
resource: see [metrics.md](metrics.md#otel-mode).

If the collector is unreachable a backoff gate around the exporter suppresses
export attempts, doubling the wait after each failure with jitter, and warns once
per outage. Batches it suppresses are dropped, and so is anything past the full
queue, while the process keeps running. There is no retry of a failed batch and
no fallback metric. Treat the OTLP path as best-effort and rely on collector-side
monitoring for export loss.

---

## API surface

| Item | Purpose |
| --- | --- |
| `OtelTracingConfig` | `enabled`, `endpoint`, `protocol`, `service_name`, `sample_ratio`, `batch_scheduled_delay_ms`, `batch_max_queue_size`, `batch_max_export_batch_size`, `export_timeout_ms` |
| `OtelTracingProtocol::{Grpc, Http}` | OTLP transport choice |
| `build_tracer_layer(&cfg)` | Returns `(layer, provider)` and installs the provider as global |
| `provider.shutdown()` | Flush + stop the batch exporter (call on graceful exit) |
| `transport::propagation::current_traceparent()` *(feature `transport-trace`)* | Format current context for header injection |
| `transport::propagation::format_traceparent_raw(trace_id, span_id, flags)` | Build a header from raw IDs (test / non-OTel use) |
| `transport::propagation::is_valid_traceparent(value)` | Structural validation of an incoming header |
| `transport::propagation::TRACEPARENT_HEADER` | The header name constant |

---

## Related

- [metrics.md](metrics.md) -- OTel-metrics is a separate bridge with its own config
- [logging.md](logging.md) -- `tracing::info!` inside a span attaches the log to the span
- [../transport/README.md](../transport/README.md) -- per-transport propagation
- [../auto-wiring.md](../auto-wiring.md), [../feature-flags.md](../feature-flags.md)
- Source: [`src/otel_tracing/mod.rs`](../../src/otel_tracing/mod.rs), [`src/transport/propagation.rs`](../../src/transport/propagation.rs)
