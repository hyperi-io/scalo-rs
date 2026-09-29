# scalo

<!-- BADGES:START -->
<!-- Build Status and docs.rs badges omitted. The repo is private, so GitHub's
     Actions SVG 404s for anonymous crates.io viewers, and the docs.rs badge
     renders its own build state, which is worse than absent when it is red.
     Re-add the Actions one at the public-visibility flip, pointing at this
     repo's own actions/workflows/ci.yml badge and actions page. -->
[![Crates.io](https://img.shields.io/crates/v/scalo?logo=rust)](https://crates.io/crates/scalo)
[![Rust Version](https://img.shields.io/badge/rust-1.95%2B-blue?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green)](https://www.apache.org/licenses/LICENSE-2.0)
<!-- BADGES:END -->

An integrated runtime for at scale data plane services. Attach and 'enterprise-up' your application.

## Key features

- configuration cascade for local test through at scale k8s deployments (env, config, cli)
- automatically configured and integrated logger (line and json) for enterprise deployments
- auto enabled Otel and Prometheus metrics in expected forms and buckets for cloud and k8s deployments
- secrets management integration
- authentication and secrets integration for most common deployments and services
- deployment contracts, your code automatically generates docker, and k8s artefacts for deployment consumption
- memory capping, align your apps throughput to memory caps with back-pressure by default. Avoid OOMs
- built in transport layer for kafka and gRPC

## Quick Start

```sh
cargo add scalo
```

Default features are `config` and `logger`. Add the rest explicitly -- pick the
slice you need and pay only for what you use.

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

All 40 feature flags, what each pulls in and which combinations matter are in
[docs/feature-flags.md](docs/feature-flags.md). The ones most people reach for:
`metrics`, `http-server` (health probes), `transport-kafka`, `worker` (adaptive
pool plus SIMD batching), `memory` (the cgroup-aware guard), `secrets-vault`.

**This crate dynamically links system C libraries**, so the build host and the
deployment target both need packages -- `-dev` to build, the `.so` runtimes to
run, and which ones depends on your features. The matrix, the Confluent APT repo
a current `librdkafka` needs, and a worked Dockerfile are in
[docs/deployment/native-deps.md](docs/deployment/native-deps.md). The `deployment`
feature derives all of it from the contract, so a generated Dockerfile already
carries the right names.

## What it is for

The data plane -- the Rust hot path where every microsecond and byte counts.
Built as the foundation for PB/hr data services.

scalo-py is the control plane half:
orchestration, APIs and integration glue in Python. Same conventions, DIFFERENT
API, separate repo. Never assume parity.

Opinionated about correctness -- backpressure, memory safety and the health
probes are on by default. Unopinionated about your domain -- no web framework, no
ORM, no enforced transport.

This module exists because of this --
<https://www.youtube.com/watch?v=xE9W9Ghe4Jk> -- but for the backend. And of
course, no microservices.

## Wired together already

Which is the part you would otherwise build:

- config, logging and metrics are global singletons, so there is no init dance
- the cascade feeds the CLI, so `run`, `version` and `config-check` work without
  you parsing an argument
- metrics and health feed the Kubernetes probes
- the deployment contract writes your Helm chart, Dockerfile and Argo manifests
  from the config the app already declares

## Documentation

[docs/README.md](docs/README.md) is the index. The ones you want first:

| Topic | Doc |
|---|---|
| Config cascade and hot reload | [core-pillars/config.md](docs/core-pillars/config.md) |
| Logging and masking | [core-pillars/logging.md](docs/core-pillars/logging.md) |
| Metrics | [core-pillars/metrics.md](docs/core-pillars/metrics.md) |
| Health probes, and which router serves what | [core-pillars/health.md](docs/core-pillars/health.md) |
| Graceful shutdown | [core-pillars/shutdown.md](docs/core-pillars/shutdown.md) |
| Self-regulation and vertical scaling | [self-regulation.md](docs/self-regulation.md) |
| Backpressure | [backpressure.md](docs/backpressure.md) |
| The cgroup-aware memory guard | [runtime/memory.md](docs/runtime/memory.md) |
| Transports | [transport/backends.md](docs/transport/backends.md) |
| Spool, DLQ, tiered sink, worker pool | [pipeline/](docs/pipeline/) |
| Secrets backends | [api/secrets.md](docs/api/secrets.md) |
| Deployment contract | [deployment/contract.md](docs/deployment/contract.md) |
| Feature flags | [feature-flags.md](docs/feature-flags.md) |
| Layering and the crate graph | [architecture.md](docs/architecture.md) |
| Rust memory tier list | [dts-rust-memory-tier-list.md](docs/dts-rust-memory-tier-list.md) |

Read [health.md](docs/core-pillars/health.md) before you point a probe at a port:
two routers can serve the health paths and they do NOT carry the same set.

## License

[Apache-2.0](LICENSE). Third-party attributions are recorded in [NOTICE](NOTICE).

## Related

- **scalo-py** -- sister library for
  Python control-plane services. Same opinions, same patterns, Python idiom.

## Context

### What this is

A shared Rust library -- config cascade, logging, metrics, health,
self-regulation, transports, spool and DLQ, secrets and the deployment-contract
generators -- published as `scalo` on crates.io under Apache-2.0.

It is scalo-py's SISTER, not its twin: same conventions, DIFFERENT API, separate
repo. Never assume parity.

### Where things live

| Path | What it holds |
|------|---------------|
| `src/<module>/` | One directory per module, gated by the feature of the same name |
| `docs/core-pillars/` | config, logging, metrics, health, tracing, shutdown, lifecycle |
| `docs/pipeline/` | spool, DLQ, tiered sink, worker pool, batch engine, scaling |
| `benches/` | Criterion benches -- config, logger, engine, strmatch, auth, loadgen |
| `docs/architecture.md` | Layering, the crate dependency graph, and what scalo-py has that this does not |

### Commands that prove a change

```bash
make quality   # fmt, clippy, audit
make test      # the suite
make bench     # criterion
```

Rust builds on this host run under `nice -n 19` via the `~/.local/bin/cargo`
shim. Never set `CARGO_BUILD_JOBS` -- it beats the shim. Read the per-job CI
result, never a local summary.

### What tends to bite

| Don't | Do | Why |
|-------|----|-----|
| Declare rustflags under `[build]` | Put them under `[target.<triple>]` | The ARC pod sets `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS`, which counts as a target entry, and cargo reads `build.rustflags` ONLY when no target entry exists. A committed, correct-looking config.toml ships without its flags and nothing fails |
| Negate a file inside an excluded directory in `.gitignore` | Exclude with `.cargo/*` so the negation can apply | `.cargo/` excludes the directory, so `!.cargo/config.toml` is inert and the file never commits |
| Read an instruction count as proof a target-cpu applied | Compare VEX to legacy-SSE encodings within one binary | Crates with runtime dispatch compile AVX2 paths whatever `target-cpu` says, so a raw BMI2 or ymm count measures what the binary LINKS, not what the compiler was told |

### Where this sits

Generated from the suite's own membership manifest. Six downstream Rust
consumers declare this crate: receiver, loader, fetcher, archiver, transform-vrl
and transform-vector.
