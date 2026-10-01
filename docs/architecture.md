# Architecture

The crate is organised in five layers. Code in a higher layer can depend on
lower layers but never the other way round. Most modules are feature-gated
so consumers only pay for what they wire in.

```mermaid
flowchart TB
    subgraph L5["L5 - App scaffolding"]
        CLI["cli / cli-service<br/>ServiceApp, ServiceRuntime, run_app"]
        DEP["deployment<br/>DeploymentContract + generators"]
    end

    subgraph L4["L4 - Pipeline"]
        TS["tiered-sink"]
        SK["sink-stack<br/>timeout / load-shed / concurrency / retry / rate-limit"]
        BE["worker-batch<br/>BatchEngine"]
        WP["worker-pool<br/>AdaptiveWorkerPool"]
        DLQ["dlq + kafka / http backends"]
        SPL["spool"]
    end

    subgraph L3["L3 - Transport and I/O"]
        T["transport<br/>Kafka / gRPC / Memory / File / Pipe / HTTP"]
        TF["transport-filter<br/>3-tier filter engine"]
        HS["http-server (axum)"]
        HC["http (reqwest)"]
        SEC["secrets (Vault / AWS)"]
        DC["directory-config (YAML / git2)"]
        OF["output-file"]
    end

    subgraph L2["L2 - Runtime and self-regulation"]
        RC["runtime / env<br/>RuntimeContext (K8s/Docker/BareMetal)"]
        MEM["memory<br/>MemoryGuard"]
        SCA["scaling<br/>ScalingPressure"]
        GOV["governor<br/>UnifiedPressure gate (hard memory + soft signals)"]
        CON["concurrency<br/>BackgroundSink / PeriodicWorker / ActorHandle"]
        STR["strmatch"]
        EXP["expression (CEL)"]
    end

    subgraph L1["L1 - Core pillars"]
        CFG["config"]
        LOG["logger"]
        MET["metrics / metrics-core / metrics-process"]
        OTEL["otel / otel-metrics / otel-tracing"]
        HLT["health"]
        SHUT["shutdown"]
    end

    L5 --> L4
    L5 --> L3
    L5 --> L2
    L5 --> L1
    L4 --> L3
    L4 --> L2
    L4 --> L1
    L3 --> L2
    L3 --> L1
    L2 --> L1
    SK -.->|wraps| T
    TF -.->|embedded in| T
```

Dashed lines mark embedded composition, not separate callers:
`transport-filter` lives inside every transport backend, and `sink-stack`
wraps a transport sender. Solid arrows show layer dependencies.

---

## Module map

### L1 - Core pillars (always-on, auto-wired)

| Module | Feature | Purpose |
| -------- | --------- | --------- |
| `config` | `config` (default) | 7-layer cascade (CLI -> env -> .env -> YAML -> defaults), hot-reload, section registry, `/config` admin endpoint |
| `logger` | `logger` (default) | `tracing-subscriber` with JSON/text autodetect, RFC 3339 timestamps, sensitive-field masking, flood-control helpers |
| `metrics` | `metrics-core`, `metrics-process`, `metrics` | Lock-free counters/gauges/histograms, Prometheus exporter, `/metrics` + `/metrics/manifest` |
| `otel_metrics` / `otel_tracing` | `otel`, `otel-metrics`, `otel-tracing` | OTLP exporter, OTel SDK bridge for `tracing` spans |
| `health` | `health` | `HealthRegistry`, health probes (`/livez` / `/readyz`) |
| `shutdown` | `shutdown` | `CancellationToken`, SIGTERM/SIGINT, K8s pre-stop delay |

Pillars are singletons. Modules in higher layers call into them via macros
(`tracing::info!`, `metrics::counter!`) or global getters
(`config::get`). No handle passing.

### L2 - Runtime and self-regulation

| Module | Feature | Purpose |
| -------- | --------- | --------- |
| `env` | always | Detect environment (Kubernetes, Docker, container, bare metal) |
| `runtime` | `runtime` | XDG/container-aware paths, `RuntimeContext` singleton (pod, namespace, node, memory limit, CPU quota) |
| `memory` | `memory` | `MemoryGuard` - cgroup-aware OOM prevention with auto-detected limits |
| `scaling` | `scaling` | `ScalingPressure` - KEDA external-scaler signal (0.0-100.0) |
| `governor` | `governor` | `UnifiedPressure` self-regulation gate - latches a hard memory signal with weighted soft signals under hysteresis, driving inbound backpressure |
| `concurrency` | `concurrency` | `BackgroundSink`, `PeriodicWorker`, `ActorHandle` - fire-and-forget, timer, command-queue primitives |
| `strmatch` | `strmatch` | 4-tier string matcher: `Byte`, `Literal`, `LiteralSet`, `Regex` |
| `expression` | `expression` | CEL evaluator (used by transport filters) |

### L3 - Transport and I/O

| Module | Feature | Purpose |
| -------- | --------- | --------- |
| `transport` | `transport`, `transport-{kafka,grpc,memory,file,pipe,http}` | Trait architecture (`TransportBase`, `TransportSender`, `TransportReceiver`, `Transport`), `AnySender` enum dispatch, factory |
| `transport::filter` | `transport` | 3-tier engine (SIMD field ops / compiled CEL / complex CEL) embedded in every backend |
| `http_server` | `http-server` | axum-based server, probe wiring, `/config` / `/metrics` / `/metrics/manifest` mount points |
| `http_client` | `http` | `reqwest`, with retry and backoff from `backon` |
| `secrets` | `secrets`, `secrets-vault`, `secrets-aws` | `SecretsManager` trait, OpenBao/Vault and AWS Secrets Manager backends |
| `directory_config` | `directory-config`, `directory-config-git` | YAML directory store with optional `git2` |
| `output` | `output-file` | NDJSON file output sink |

### L4 - Pipeline

| Module | Feature | Purpose |
| -------- | --------- | --------- |
| `spool` | `spool` | Disk-backed async FIFO queue (`yaque` + `zstd`), per-record CRC32C integrity |
| `tiered_sink` | `tiered-sink` | Transport + spool + circuit breaker + retry + DLQ fallback |
| `sink_stack` | `sink-stack` | Outbound control stack - composes timeout, load-shed, concurrency-limit, retry/backoff and rate-limit around a transport sender (tower `ServiceBuilder`); preserves at-least-once |
| `worker::pool` | `worker-pool` | `AdaptiveWorkerPool` (rayon + tokio), pressure-based scaling |
| `worker::engine` | `worker-batch` | `BatchEngine` - SIMD parse (`sonic-rs`), pre-route filter, field interning |
| `dlq` | `dlq`, `dlq-kafka`, `dlq-http` | DLQ sink with file always available, Kafka/HTTP backends opt-in |

### L5 - App scaffolding

| Module | Feature | Purpose |
| -------- | --------- | --------- |
| `cli` | `cli` | `clap` types: `CommonArgs`, `StandardCommand`, `VersionInfo`, output helpers |
| `cli::service` | `cli-service` | `ServiceApp` trait, `run_app`, `ServiceRuntime` - full data-plane app scaffolding |
| `top` | `top` | TUI metrics dashboard (`ratatui`) |
| `deployment` | `deployment`, `deployment-smoke` | `DeploymentContract`, generators for Dockerfile / Helm chart / ArgoCD Application / container manifest |
| `version_check` | `version-check` | Startup HTTP probe to the version API |

---

## Layering rules

1. **A module never depends upward.** `transport` cannot import from `cli`.
   `config` cannot import from `worker`. Cargo's feature graph enforces
   most of this; code review catches the rest.
2. **Pillars don't depend on each other beyond what's structurally
   required.** `metrics` uses `tracing` for its own logging, but does not
   know `config` exists.
3. **L2 modules can be used standalone.** `MemoryGuard`, `ScalingPressure`,
   `cache`, `strmatch` all work without the L4 pipeline above them.
4. **L3 transports always embed the filter engine.** Even when no filters
   are configured the engine is present as a no-op (a single
   `has_inbound_filters()` branch on every send/recv). Zero-cost when empty.
5. **L4 pipeline modules compose L3 transports with L2 runtime concerns.**
   `tiered_sink` is the canonical example: a transport plus spool plus
   memory pressure plus circuit breaker plus DLQ.
6. **L5 scaffolding glues the whole stack.** `ServiceRuntime::new` wires
   the pillars, runtime, pipeline primitives, and shutdown into one
   object the app holds. `DeploymentContract` does the same job for the
   "ship it" side: one struct, generates every artefact CI/CD needs.

---

## Feature defaults

- `default = ["config", "logger"]`. Nothing else. Apps explicitly opt in.
- Trimmed defaults keep the "I-just-want-config" use case off the full
  transitive dependency set.
- See [feature-flags.md](feature-flags.md) for the full tree and which
  features pull in which.

---

## Where the dependency graph gets non-obvious

A handful of dependencies aren't visible from layer naming alone:

- `worker-batch` depends on `worker-pool` (the engine sits on top of the
  pool), which in turn depends on `metrics` and `config`.
- `tiered-sink` does not enable `spool`. It runs its own `yaque` queue through the crate-private `spool_codec` it shares with `spool`, plus an L3 transport.
- `dlq` requires `concurrency` (L2) for the `BackgroundSink` actor that
  drains queued entries.
- `transport-trace` is the *only* feature that pulls in the OpenTelemetry
  SDK on the transport side. Apps that send/receive without distributed
  tracing avoid that dep entirely.
- `cli-service` (L5) reaches across the whole stack - it pulls `cli + metrics + memory + scaling + shutdown + governor + sink-stack + lifecycle` because `ServiceRuntime::new` wires all of them. It does not pull `worker-pool`; an app that runs one declares it.

Read [feature-flags.md](feature-flags.md) for the full feature-to-feature
edges and [auto-wiring.md](auto-wiring.md) for which dependencies are
auto-wired vs explicit.

---

## Project facts

- **Edition:** 2024
- **MSRV:** see `rust-version` in `Cargo.toml`
- **Sibling lib:** `scalo-py` (Python control-plane equivalent)
- **Downstream:** the six core consumer services consume `scalo` in
  lockstep (see [README.md § Project facts](README.md#project-facts))
