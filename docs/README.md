# scalo docs

Shared Rust library for data-plane services. A service built on `run_app` gets the config cascade, structured logs, Prometheus metrics, health probes, OTel traces, graceful shutdown and the K8s pre-stop delay from one call, and the `deployment` feature generates its deployment artefacts. Add a `Transport`, a `TieredSink`, a `BatchEngine` and the same deal extends: counters, span propagation, DLQ routing, backpressure, scaling signals.

This is the index. Read [architecture.md](architecture.md) for the
10,000-foot view of how the modules fit together, [integration.md](integration.md)
for a recipe walkthrough on building a data-plane service, [auto-wiring.md](auto-wiring.md)
for what is wired in for you, [feature-flags.md](feature-flags.md)
for how features cascade into one another, and [platforms.md](platforms.md)
for what differs on macOS and Windows.

---

## What you get for free

| Wire this at startup | And these come along | No need to |
| ---------------------- | ---------------------- | ------------ |
| `config::setup(opts)` | 7-layer cascade, env-var nesting, `.env`, sensitive masking, section registry. Hot reload comes with `config-reload`, the `/config` endpoint with `HttpServer` | Wire figment, write a settings loader, build a reload watcher |
| `logger::setup_default()` | Structured tracing, JSON or text picked from the OTEL endpoint, CI and the terminal, colour only on a terminal, RFC 3339 timestamps, sensitive-field masking, flooding helpers | Install a tracing subscriber, format JSON, pick a logger crate |
| `MetricsManager::new("app")` | Prometheus exporter, `/metrics` endpoint once `start_server` runs, process metrics, bounded label values, `/metrics/manifest` catalogue | Stand up an exporter, wire a process collector, hand-roll a manifest |
| `run_app(app)` | All of the above + memory guard + scaling pressure + shutdown token + K8s pre-stop delay + runtime context, and the worker pool and batch engine when `worker-pool` or `worker-batch` is on. It builds the `ServiceRuntime` and hands it to `run_service` | Glue them together manually |
| Any `Transport` impl | 3-tier filter engine, DLQ routing, per-direction/action metrics, W3C traceparent propagation | Add filters, wire DLQ, instrument send/recv |
| `TieredSink::new(...)` | Transport + disk-spillover spool + circuit breaker + background drain + backpressure signal (`SpoolFull` for the caller to route, usually to the DLQ) | Compose those primitives by hand |
| `BatchEngine::process_mid_tier()` | SIMD JSON parse, pre-route filter, field interning, parallel transform on the worker pool | Write a parse loop and a thread pool |
| `DeploymentContract { ... }` | Dockerfile, container manifest, OCI labels, and the contract JSON the scalo-service chart renders the Kubernetes objects from | Hand-write a Dockerfile and chart values |
| `HealthRegistry::register("module", ...)` | The module's status in the aggregate `/readyz` decision. The answer body is status only | Aggregate readiness by hand |

That's the value proposition. Everything else in these docs is "and here's
how the pieces work".

---

## 10,000-foot view

`run_app` builds a `ServiceRuntime` that wires the core pillars, self-regulation and the worker pool, the app builds its pipeline, and the deployment contract writes the artefacts the scalo-service chart renders.

```mermaid
flowchart LR
    accTitle: L3 Components -- scalo
    accDescr: An app hands itself to run_app, which builds the ServiceRuntime, builds its own pipeline, and fills in a deployment contract. The runtime wires the core pillars, self-regulation and the worker pool and batch engine. The pipeline sends through the transports. The contract writes the Dockerfile and contract JSON that the scalo-service chart renders.

    app["Your ServiceApp"]
    runtime[["ServiceRuntime"]]
    pipeline[["Pipeline"]]
    contract[["Deployment<br/>contract"]]
    pillars["Core pillars"]
    regulation[["Self-regulation"]]
    workers["Worker pool,<br/>batch engine"]
    transports[["Transports"]]
    artefacts[("Dockerfile and<br/>contract JSON")]
    chart["scalo-service<br/>chart"]

    app --> runtime
    app --> pipeline
    app --> contract
    runtime --> pillars
    runtime --> regulation
    runtime --> workers
    pipeline --> transports
    contract --> artefacts
    artefacts --> chart

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef store fill:#DDF3EC,stroke:#00785A,stroke-width:1px,color:#0B3327
    classDef ext fill:#EEF0F2,stroke:#6B7280,stroke-width:1px,stroke-dasharray:4 3,color:#1F2937
    class app ext
    class artefacts store
```

*Figure 1. L3 Components -- scalo. The dashed box is the app built on scalo.* Up: top of scalo-rs. Down: [L4 Detail -- Module layers](architecture.md), [L4 Detail -- Service lifecycle](runtime/service-runtime.md), [L4 Detail -- Self-regulation loop](self-regulation.md), [L4 Detail -- Pipeline stages](pipeline/README.md), [L4 Detail -- Transport traits](transport/README.md), [L4 Detail -- Contract to chart](deployment/contract.md).

The runtime builds the worker pool and batch engine only when `worker-pool` or `worker-batch` is on. Transports, the tiered sink and the DLQ are built by the app in `run_service`.

---

## Where to read what

| Area | Page | What it covers |
| --- | --- | --- |
| Start here | [architecture.md](architecture.md) | Module map, dependency graph, layering |
| Start here | [integration.md](integration.md) | "I'm building a data-plane app" walkthrough |
| Start here | [auto-wiring.md](auto-wiring.md) | What's wired into what, and why |
| Start here | [feature-flags.md](feature-flags.md) | Feature tree, native deps, recommended bundles |
| Start here | [crate.md](crate.md) | The crates.io landing page |
| Data plane | [self-regulation.md](self-regulation.md) | ON by default. The three brains (MemoryGuard / ScalingPressure / UnifiedPressure), observe and tune |
| Data plane | [backpressure.md](backpressure.md) | Gate the source, never the sink. The per-stage brake/commit-token table, streaming sub-blocks |
| Data plane | [kafka-path.md](kafka-path.md) | The three batch sizes, sizing profiles and librdkafka names, how the byte budget moves, the partition-limited diagnostic |
| Core pillars | [core-pillars/README.md](core-pillars/README.md) | Section landing |
| Core pillars | [core-pillars/config.md](core-pillars/config.md) | 7-layer cascade, hot-reload, registry, `/config` endpoint |
| Core pillars | [core-pillars/managed-config.md](core-pillars/managed-config.md) | Opt-in Group Policy and MDM layer, after the files and before env vars |
| Core pillars | [core-pillars/logging.md](core-pillars/logging.md) | tracing setup, JSON/text autodetect, masking, flood control |
| Core pillars | [core-pillars/metrics.md](core-pillars/metrics.md) | Prometheus, the manifest, transport throughput, OTel mode |
| Core pillars | [core-pillars/tracing.md](core-pillars/tracing.md) | OTel, W3C traceparent, transport propagation |
| Core pillars | [core-pillars/health.md](core-pillars/health.md) | `HealthRegistry`, `/livez` / `/readyz` |
| Core pillars | [core-pillars/shutdown.md](core-pillars/shutdown.md) | `CancellationToken`, K8s pre-stop delay |
| Core pillars | [core-pillars/lifecycle.md](core-pillars/lifecycle.md) | Idle until configured, `WorkState`, `pipeline_idle` |
| Runtime | [runtime/README.md](runtime/README.md) | Section landing |
| Runtime | [runtime/service-runtime.md](runtime/service-runtime.md) | `ServiceRuntime`, `ServiceApp` trait, `run_app` |
| Runtime | [runtime/runtime-context.md](runtime/runtime-context.md) | Kubernetes, Docker, container and bare-metal detection, pod metadata |
| Runtime | [runtime/memory.md](runtime/memory.md) | `MemoryGuard`, cgroup-aware backpressure |
| Transport | [transport/README.md](transport/README.md) | Trait architecture, factory, `AnySender`, commit tokens |
| Transport | [transport/backends.md](transport/backends.md) | Kafka, gRPC, Memory, File, Pipe, HTTP, Syslog, OTLP |
| Transport | [transport/syslog.md](transport/syslog.md) | RFC 5424 and 3164 over TCP, TLS, UDP and RELP, and how each failure answers |
| Transport | [transport/grpc.md](transport/grpc.md) | gRPC `send_batch` blocks joined into one call |
| Transport | [transport/http.md](transport/http.md) | HTTP sending: credentials, body framing, compression, failover, proxy, redirects, status answers |
| Transport | [transport/otlp.md](transport/otlp.md) | OTLP log records to a collector over gRPC or HTTP, and how each answer maps |
| Transport | [transport/filter-engine.md](transport/filter-engine.md) | 3-tier filter (SIMD / compiled [CEL](https://github.com/cel-expr/cel-spec/blob/master/doc/langdef.md) / complex CEL) |
| Transport | [transport/routing.md](transport/routing.md) | `RoutedSender`, the named sink set: routes, fan-out |
| Transport | [transport/delivery-path.md](transport/delivery-path.md) | Inline delivery, the gRPC fan-in buffer, the benched alternatives and their numbers, parked parallel fan-out |
| Transport | [transport/custom.md](transport/custom.md) | Writing a custom sender, registering it by type name, the delivery guarantee |
| Transport | [compression.md](compression.md) | The `compression` key every surface reads, the codecs each takes, the block and stream engines |
| Transport | [transport/framing.md](transport/framing.md) | NDJSON, JSON array, RFC 6587 and the other batch framings, decode limits, streams, your own framing |
| Deployment | [deployment/README.md](deployment/README.md) | Section landing |
| Deployment | [deployment/contract.md](deployment/contract.md) | `DeploymentContract` struct, schema versioning |
| Deployment | [deployment/artefacts.md](deployment/artefacts.md) | Generated Dockerfile, container manifest, contract JSON |
| Deployment | [deployment/native-deps.md](deployment/native-deps.md) | `NativeDepsContract`, feature -> APT package map |
| Deployment | [deployment/keda.md](deployment/keda.md) | `KedaContract`, scaler triggers, fallback HPA |
| Deployment | [deployment/conformance.md](deployment/conformance.md) | The at-least-once delivery conformance harness |
| Pipeline | [pipeline/README.md](pipeline/README.md) | Section landing |
| Pipeline | [pipeline/acknowledgements.md](pipeline/acknowledgements.md) | When a source's acknowledgement is released |
| Pipeline | [pipeline/batch-engine.md](pipeline/batch-engine.md) | SIMD parse, pre-route filter, field interning |
| Pipeline | [pipeline/worker-pool.md](pipeline/worker-pool.md) | `AdaptiveWorkerPool`, pressure-based scaling |
| Pipeline | [pipeline/tiered-sink.md](pipeline/tiered-sink.md) | Resilient delivery, disk spillover, circuit breaker |
| Pipeline | [pipeline/sink-stack.md](pipeline/sink-stack.md) | Outbound control stack: timeout / load-shed / concurrency / retry / rate-limit |
| Pipeline | [pipeline/dlq.md](pipeline/dlq.md) | File, Kafka, HTTP backends |
| Pipeline | [pipeline/spool.md](pipeline/spool.md) | Disk-backed async FIFO |
| Pipeline | [pipeline/strmatch.md](pipeline/strmatch.md) | 4-tier regex->fast-path matcher (Byte / Literal / LiteralSet / Regex) |
| Pipeline | [pipeline/rules.md](pipeline/rules.md) | Ordered first-match rules that set target fields: native tests or CEL Fast, clamp, hit counters, hot reload |
| Pipeline | [pipeline/scaling.md](pipeline/scaling.md) | `ScalingPressure`, KEDA external scaler signal |
| Other subsystems | [api/README.md](api/README.md) | Section landing |
| Other subsystems | [api/auth.md](api/auth.md) | Credential acquisition and placement |
| Other subsystems | [api/wasm.md](api/wasm.md) | WebAssembly component plugins under limits, loaded only when a trusted signer signed them |
| Other subsystems | [api/secrets.md](api/secrets.md) | OpenBao/Vault, AWS Secrets Manager |
| Other subsystems | [api/http-server.md](api/http-server.md) | axum server, probe wiring, route extensions |
| Other subsystems | [api/tls.md](api/tls.md) | The one client and server TLS section, certificate reload from file or secret, handshake helper |
| Other subsystems | [tls-profiles.md](tls-profiles.md) | The `strong`, `commercial` and `cnsa2` profiles, what each connects to, TLS 1.2 and the Kafka floor |
| Other subsystems | [api/http-client.md](api/http-client.md) | `reqwest` + retry + request signing |
| Other subsystems | [api/directory-config.md](api/directory-config.md) | YAML directory store with optional `git2` |
| Other subsystems | [api/concurrency.md](api/concurrency.md) | `BackgroundSink`, `PeriodicWorker`, `ActorHandle` |
| Other subsystems | [api/geoip-download.md](api/geoip-download.md) | GeoIP database provisioning |
| Other subsystems | [version-check.md](version-check.md) | Opt-out startup version check |
| Reference | [reflectable-config-shape.md](reflectable-config-shape.md) | The schema and capability catalogue scalo and scalo-py both emit |
| Reference | [migrations.md](migrations.md), [migrations/](migrations/) | API and behaviour changes by scalo version |
| Reference | [migrating.md](migrating.md) | Moving from hyperi-rustlib to scalo |
| Reference | [dependency-duplication.md](dependency-duplication.md) | Duplicate crate versions in the dependency tree |
| Reference | [dts-rust-memory-tier-list.md](dts-rust-memory-tier-list.md) | Rust memory structures, tier list |
| Reference | [vector-baselining.md](vector-baselining.md) | The `baseline` benches |

---

## Project facts

- Crate: [scalo](https://crates.io/crates/scalo) on crates.io. Edition, MSRV and the sibling lib are in [architecture.md](architecture.md#project-facts).
- Used by several downstream data-plane services: Kafka-to-ClickHouse loaders, push and pull ingress, long-term archivers, VRL, Vector and Elastic transform apps.
