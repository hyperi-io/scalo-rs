# scalo

<!-- BADGES:START -->
<!-- Build Status and docs.rs badges omitted. The repo is private, so GitHub's
     Actions SVG 404s for anonymous crates.io viewers, and the docs.rs badge
     renders its own build state, which is worse than absent when it is red.
     Re-add the Actions one at the public-visibility flip:
     [![Build Status](https://github.com/hyperi-io/scalo-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/scalo-rs/actions) -->
[![Crates.io](https://img.shields.io/crates/v/scalo?logo=rust)](https://crates.io/crates/scalo)
[![Rust Version](https://img.shields.io/badge/rust-1.95%2B-blue?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green)](https://www.apache.org/licenses/LICENSE-2.0)
<!-- BADGES:END -->

> The stuff for your app to operate at scale, in one place

What you get:

- A 7-layer config cascade, hot-reloadable
- Structured logging, JSON or text by context, secrets masked
- Prometheus metrics, plus cgroup-aware process gauges
- OpenTelemetry traces and metrics over OTLP
- Kubernetes health probes on their own port
- Backpressure, load shedding and adaptive scaling, on by default
- A memory guard that reads the cgroup, not the host
- Graceful shutdown that drains first
- Kafka, gRPC, Redis Streams, HTTP, file and in-memory transports
- A disk-backed spool, and a tiered sink with a circuit breaker
- A dead-letter queue
- Secrets from OpenBao, Vault or AWS Secrets Manager, under one spec grammar
- Credential minting and request signing
- CEL expressions
- An adaptive worker pool with SIMD JSON batching

Wired together already, which is the part you would otherwise build:

- Config, logging and metrics are global singletons. No init dance.
- The cascade feeds the CLI, so `run`, `version` and `config-check` work
  without you parsing an argument.
- Metrics and health feed the Kubernetes probes.
- The deployment contract writes your Helm chart, Dockerfile and Argo
  manifests from the config the app already declares.

Two halves, one set of conventions, each idiomatic. **scalo-rs** (this crate)
is the data plane, the Rust hot path where every microsecond and byte counts
(`cargo add scalo`). **scalo-py** is the control plane -- orchestration, APIs
and integration in Python (`pip install scalo`).

Opinionated about correctness -- backpressure, memory safety and the health
probes are on by default. Unopinionated about your domain -- no web framework,
no ORM, no enforced transport. Built as the foundation for PB/hr data services.

This module exists because of this --
<https://www.youtube.com/watch?v=xE9W9Ghe4Jk> -- but for the backend. And of
course, no microservices.

## Quick Start

```toml
[dependencies]
scalo = "2"
```

Default features: `config`, `logger`. Add the others you want explicitly.

```rust
use scalo::{config, logger, env};

fn main() -> anyhow::Result<()> {
    let environment = env::Environment::detect();
    logger::setup_default()?;
    config::setup(config::ConfigOptions {
        env_prefix: "MYAPP".into(),
        ..Default::default()
    })?;

    tracing::info!("Running in {environment:?}");
    Ok(())
}
```

## Features

Pick the slice you need; pay only for what you use.

| Feature | Description |
|---------|-------------|
| `env` | Environment detection (K8s, Docker, Container, BareMetal) |
| `runtime` | Runtime path resolution (XDG/container-aware) |
| `config` | 7-layer config cascade (figment-based) |
| `config-reload` | `SharedConfig<T>` + `ConfigReloader` hot-reload |
| `logger` | Structured logging, JSON/text auto-detect, sensitive-field masking |
| `metrics` | Prometheus metrics + process/container metrics |
| `otel-metrics` | OpenTelemetry metrics export (OTLP) |
| `otel-tracing` | OpenTelemetry distributed tracing |
| `http` | HTTP client with retry, backoff and request signing (reqwest) |
| `auth` | Credential acquisition and placement |
| `http-server` | Axum HTTP server with health probes (`/livez` and `/readyz`) |
| `transport-kafka` | Kafka transport (rdkafka, dynamic-linking) |
| `transport-grpc` | gRPC transport (tonic/prost) |
| `transport-memory` | In-memory transport (testing/dev) |
| `transport-redis` | Redis / Valkey Streams transport |
| `transport-grpc-vector-compat` | Vector wire-protocol compatibility |
| `spool` | Disk-backed async FIFO queue (yaque + zstd, CRC32C integrity) |
| `tiered-sink` | Resilient delivery: hot buffer + circuit breaker + disk spillover |
| `sink-stack` | Outbound control stack: timeout / load-shed / concurrency-limit / retry / rate-limit (tower) |
| `worker` | Adaptive worker pool + `BatchEngine` (SIMD parse, pre-route, field interning) |
| `memory` | Cgroup-aware `MemoryGuard` (OOM prevention) |
| `governor` | Unified self-regulation gate (hard memory + weighted soft signals) |
| `secrets` | Secrets management core (file backend) |
| `secrets-vault` | OpenBao / HashiCorp Vault provider |
| `secrets-aws` | AWS Secrets Manager provider |
| `directory-config` | YAML directory-backed config store |
| `directory-config-git` | Git integration for directory-config (git2) |
| `scaling` | Back-pressure / scaling-pressure primitives |
| `cli` | Standard CLI framework (clap) |
| `top` | TUI metrics dashboard (ratatui) |
| `io` | File rotation, NDJSON writer |
| `dlq` | Dead-letter queue (file backend) |
| `dlq-kafka` | DLQ Kafka backend |
| `output-file` | File output sink |
| `expression` | CEL expression evaluation |
| `deployment` | Deployment-contract validation |
| `version-check` | Optional startup version check |
| `geoip-download` | GeoIP MMDB database provisioning (download + freshness, no lookup engine) |
| `full` | Everything |

## Native System Dependencies

This crate dynamically links against system C libraries, so BOTH the build host
and the deployment target need packages -- the `-dev` ones to build, the `.so`
runtimes to run. Which ones depends on the features you enable.

The full matrix, the Confluent APT repo a current `librdkafka` needs, the
per-release package names and a worked Dockerfile are in
[docs/deployment/native-deps.md](docs/deployment/native-deps.md). The
`deployment` feature derives all of it from the contract's release, so a
generated Dockerfile already carries the right names.

## Health Check Endpoints

For services deployed to Kubernetes, the paths are the K8s-standard ones. Two
routers can serve them and they do NOT carry the same set, so the "served by"
column is the one to read before pointing a probe or a scrape at a port:

| Path | Serves | Served by | Checks | On failure |
|---|---|---|---|---|
| `/livez` | liveness | metrics server + `http-server` | Process not deadlocked | Restart pod |
| `/readyz` | readiness | metrics server + `http-server` | Deps healthy + ready flag set | Stop routing traffic |
| `/metrics` | Prometheus scrape | metrics server ONLY | - | - |

Those are the whole surface -- there are no aliases, and every retired path
returns 404. A second path meaning the same thing eventually stops meaning the
same thing, and an alias that keeps answering 200 hides a probe still aimed at
the old name.

The deployment contract's `metrics_path` defaults to `/metrics`, and the
generated Helm chart puts the Prometheus scrape annotations on the contract's
`metrics_port`. That only answers if the METRICS server is the thing listening
on that port -- an app that stands up only the `http-server` router there will
serve the two health paths and 404 the scrape.

There is no startup path. Point a K8s `startupProbe` at `/livez`: Kubernetes
suspends liveness until the startup probe passes, so one path gives both a
generous boot budget (`failureThreshold`) and a tight liveness period, without
the two drifting apart.

Liveness MUST NEVER check downstream dependencies (a DB outage shouldn't
restart your replicas). Readiness checks dependencies AND requires an
explicit `set_ready()` call - cleared during graceful shutdown.

## Self-regulation (default vertical scaling)

A scalo data-plane app regulates its own intake. Sized for steady state, a
pod slows down or speeds up WITHIN itself first -- the default, fast, local
response to a burst, a stalled upstream, or a transform that balloons memory.
Only when that vertical headroom is exhausted does it escalate to horizontal
scale (KEDA adding pods), driven by the same pressure signal. Memory is the
hard, never-OOM authority; CPU is left to the kernel scheduler (CFS), which
the byte-budget loop reads through longer process times. It is ON by default
and opt-out via `self_regulation.enabled = false`.

The loop, since "it tunes itself" is a claim and this is the part you can
check:

- **AIMD on the byte budget.** The streaming sub-block budget grows additively
  while things are healthy and is cut multiplicatively when they are not --
  the same additive-increase/multiplicative-decrease shape TCP congestion
  control has used since the 1980s.
- **HARD signals are never masked.** The memory guard contributes its raw
  reading with no weight applied. A saturated soft signal cannot pull the
  level below what memory demands, and a busy soft signal cannot hide a
  missing hard one. That is the never-OOM guarantee.
- **SOFT signals are weighted** and compete for the level, so a low-weight
  source at full saturation cannot force a hold that memory would not.
- **Hysteresis, because coupled controllers oscillate.** The latch arms at
  `pause_above` and releases at `resume_below`; between the two it holds. A
  reading hunting around a single threshold cannot flap pause/resume, which is
  the failure mode that makes people distrust self-tuning systems.
- **It gates the SOURCE, never the sink.** Backpressure stops intake. It never
  slows the write side and calls that regulation.

Full write-up, including the three pressure brains and why CPU was
deliberately dropped as a source, in
[docs/self-regulation.md](docs/self-regulation.md) and
[docs/backpressure.md](docs/backpressure.md).

## Architecture

See [docs/](docs/README.md) for the full documentation index -
[docs/architecture.md](docs/architecture.md) for the module map and layering,
and [docs/core-pillars/config.md](docs/core-pillars/config.md) for the 7-layer
config cascade reference.

## License

[Apache-2.0](LICENSE).

## Related

- **[scalo-py](https://github.com/hyperi-io/scalo-py)** -- sister library for
  Python services. Same opinions, same patterns, expressive Python ergonomics
  for control planes, APIs, and integration layers.

## Context

### What this is

The Rust half of scalo: a runtime a data-plane service links, not a framework it
plugs into. Config, logging and metrics are global singletons with no init dance,
and the transports, spool, tiered sink, DLQ, worker pool, probes and deployment
contract all sit on top. Published as `scalo` on crates.io under Apache-2.0.

What it is not:

- Not DFE. `.hyperi-ci.yaml` declares neutral branding -- no HyperI or DFE name
  in code, comments, metrics or on the wire, copyright and attribution aside.
  scalo-rs#136 tracks the brand strings still in the tree.
- Not scalo-py's twin. Same conventions, DIFFERENT API, separate repo. Never
  assume parity between the two.
- Not a library you bump on its own. Seven repos build against this crate and
  their Dockerfiles, charts and manifests are written by `scalo::deployment`, so
  a release here is a fleet move. See "Where this sits".

### Where things live

| Path | What is there |
|---|---|
| `src/lib.rs` | Crate entry point and public re-exports. Its module doc is the docs.rs front page |
| `src/<module>/` | One directory per feature area, layered L1 pillars up to L5 scaffolding. A module never depends upward -- rules in `docs/architecture.md` |
| `Cargo.toml` `[features]` | The real API surface. `default = ["config", "logger"]` and nothing more |
| `Cargo.toml` `[package.metadata.docs.rs]` | Hand-enumerated feature list docs.rs builds with. Not derived, not checked by CI |
| `VERSION` | The released version. `Cargo.toml`'s `version` field is not it |
| `tests/` | Integration tests, plus `e2e/`, `integration/`, `common/` and `fixtures/` |
| `benches/` | Criterion benches, declared as `[[bench]]` in `Cargo.toml` |
| `examples/` | `quickstart` and `full_demo`, plus the `mem_loadgen` and `cpu_loadgen` harnesses that `scripts/operational-*-test.sh` drive under a cgroup cap |
| `proto/` | `proto/scalo/transport/v1` is ours. `proto/vector/*.proto` is vendored from Vector and stays MPL-2.0 -- see `NOTICE` |
| `docs/` | `docs/README.md` is the index, `docs/architecture.md` the module map and layering rules |
| `.hyperi-ci.yaml` | What CI actually runs |
| `.githooks/commit-msg` | Conventional-commit check, inert until you set `core.hooksPath` |

### Commands that prove a change

| Command | What it is |
|---|---|
| `make quality` | `hyperi-ci run quality` |
| `make test` | `hyperi-ci run test` |
| `make build` | `hyperi-ci run build` |
| `hyperi-ci check` | The whole local gate, what `CONTRIBUTING.md` tells a contributor to run before every push |

CI is the shared `hyperi-io/hyperi-ci/.github/workflows/rust-ci.yml@main`, driven
by `.hyperi-ci.yaml`. Four ways green means less than it looks:

| Green still hides | Because |
|---|---|
| A clippy failure, dead code or a broken intra-doc link under a narrow feature set | CI builds and lints with `features: all`, so a subset-only fault never shows. A consumer enabling a narrow set is exactly who hits it (scalo-rs#144) |
| Anything that depends on a process-global | `nextest: true` forks a process per test, and `coverage: false`, so a single-process runner fails where CI passes (scalo-rs#150) |
| A docs-only branch push | `.github/workflows/ci.yml` sets `paths-ignore` for `docs/**` and `**.md` on push, so that run is skipped outright. The `pull_request` trigger carries no `paths-ignore`, so the PR does run it |
| A docs.rs build failure | Nothing in CI builds the hand-written docs.rs feature list (scalo-rs#122) |

`.cargo/config.toml` sets `rustflags = ["-D", "warnings"]`, so any warning fails a
local build too. An all-features build needs the build-host packages in
[docs/deployment/native-deps.md](docs/deployment/native-deps.md).

### What tends to bite

| Don't | Do | Why |
|---|---|---|
| Read the released version out of `Cargo.toml` | Read `VERSION` | A release commit touches `CHANGELOG.md` and `VERSION` only. `Cargo.toml` still says 2.12.0 where `VERSION` says 2.12.3 |
| Add a `pub` field to `DeploymentContract`, `PortContract` or `KedaContract` and treat it as a patch | Add it, then regenerate and fix every consumer in the same pass | None of those three is `#[non_exhaustive]`, so a new field breaks every consumer struct literal. `PortCondition`, `KafkaLagTrigger` and `ChartPatch` are. Four consumer repos carried a comment claiming `KedaContract` was, and it never has been |
| Assert on `render()` in two metrics tests in one process | Assert in one, and drive the rest through the installed global | `set_global_recorder` succeeds once per process. Every later `MetricsManager` warns, keeps the existing recorder and renders an empty string (scalo-rs#150) |
| Rename or drop a cargo feature and stop there | Edit `[package.metadata.docs.rs].features` in the same commit | That list is hand-written and nothing in CI builds it. The 2.12.3 docs.rs build failed on a name that no longer existed, so the crate had no rendered API docs (scalo-rs#122) |
| Prove a change with `--all-features` alone | Check the narrow sets a consumer actually declares as well | Clippy, dead-code and intra-doc-link faults live only in the subsets (scalo-rs#144) |
| Build or test a consumer with only `transport-kafka` | Compile every transport feature | The backend comes from the `transport.type` config key and each backend's config field is `#[cfg(feature)]`-gated, so a narrow binary compiles and tests green, then refuses at runtime with "transport type '...' is not available" |
| Expect the commit-msg hook to run in a fresh clone | `git config core.hooksPath .githooks` once | `core.hooksPath` is unset by default. The hook file documents that exact line and still nobody runs it |
| Type a non-ASCII character | `--`, `->`, `...` | It keeps coming back. Non-ASCII is still in `tests/`, `benches/` and about 17 `docs/` files (scalo-rs#143) |

### Where this sits

Generated with `dfe-infra/scripts/dfe-stack suite --consumer scalo-rs` and the
same command with `--producer scalo-rs`.

**Inbound: nothing.** `--consumer scalo-rs` returns zero edges. This repo depends
on no other repo in the suite, only on crates.io and the system C libraries in
[docs/deployment/native-deps.md](docs/deployment/native-deps.md). Bottom of the
graph, deliberately.

**Outbound: seven repos, each on two edges at once.**

- `cargo-dep` -- the consumer's `Cargo.toml` names `scalo` by range. If the range
  admits the new version, `cargo update -p scalo` and rebuild. If not, widen the
  range first.
- `generated-file` -- files committed in the consumer are written by
  `scalo::deployment`, each carrying a header that names the generator and the
  schema version it was emitted at. dfe-loader's `Dockerfile` names
  `generate_dockerfile()`, schema version 3, and its own regenerate command.
  Change a generator or its schema and every consumer regenerates.

| Consumer repo | What else moves with it |
|---|---|
| dfe-loader | - |
| dfe-receiver | A second `scalo` range for the test-support feature. Both must move together |
| dfe-fetcher | - |
| dfe-archiver | The workspace root declares it and three crates inherit via `workspace = true` |
| dfe-transform-vrl | A second range for the dev dependency |
| dfe-transform-vector | A second range for the dev dependency |
| dfe-transform-elastic | Alpha, outside the default pass, another team's. Its floor is lower than the other six, so it can resolve an older scalo. Do not edit it |

dfe-fetcher, dfe-receiver and dfe-loader each assert that the committed chart
equals the generator's output, so a scalo bump with no regenerate fails the
consumer's own suite. That is the guard working, not a flake.

Two edges the suite graph does not declare: dfe-engine reflects on the config
schema and capability catalogue both scalo halves emit, through one code path,
and scalo-py mirrors that same shape (`docs/reflectable-config-shape.md`).
Convention, not a checked dependency.
