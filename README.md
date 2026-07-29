# scalo

<!-- BADGES:START -->
[![Build Status](https://github.com/hyperi-io/scalo-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/scalo-rs/actions)
[![Crates.io](https://img.shields.io/crates/v/scalo?logo=rust)](https://crates.io/crates/scalo)
[![docs.rs](https://img.shields.io/docsrs/scalo?logo=rust)](https://docs.rs/scalo)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
<!-- BADGES:END -->

> There's plenty of sage advice about running services in production at
> scale -- config cascades, structured logging, secret masking, Prometheus,
> OpenTelemetry, health probes, backpressure, graceful shutdown -- but almost
> none of it as code you can just install and use.
>
> This is that code.

scalo is an integrated, self-regulating runtime for hyperscale-grade
data-plane services. Config, logging and metrics come as one pre-wired
trinity -- global singletons you just use, no plumbing, no init dance.
Everything else leans on that same integration: the config cascade flows
straight into the CLI so `run`/`version`/`config-check` just work; the metrics
and health wiring feed the K8s probes; and the deployment contract
generates your Helm, Dockerfile and Argo manifests from the config the app
already declares.

Attach scalo to your service and a whole class of production pain -- the kind
done wrong a hundred times elsewhere -- just goes away. Battle-tested, and
almost no code on your side **to do it properly**. It's not a bag of utility
functions you wire up yourself; it's the wiring, done right, for free.

scalo comes in two halves that share one set of conventions, idiomatic in each
language. **scalo-rs** (this crate) is the **data plane** -- the Rust hot path
where every microsecond and byte counts (`cargo add scalo`). **scalo-py** is
the **control plane** -- orchestration, APIs and integration in Python
(`pip install scalo`).

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
| `http` | HTTP client with retry middleware (reqwest) |
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
| `full` | Everything |

## Native System Dependencies

This crate dynamically links against system C libraries for several features.
**Both build hosts and deployment targets need the appropriate packages.**

### Build Host (CI / Development)

| Feature | Crate | Build Package | Notes |
|---------|-------|--------------|-------|
| `transport-kafka` | `rdkafka-sys` | `librdkafka-dev` (>= 2.12.1) | Requires [Confluent APT repo](https://packages.confluent.io/clients/deb) - Ubuntu's default is too old |
| `directory-config-git` | `libgit2-sys` | `libgit2-dev`, `libssh2-1-dev` | System lib avoids vendored C build |
| `spool`, `tiered-sink` | `zstd-sys` | `libzstd-dev` | System lib avoids vendored C build |
| (transitive) | `libz-sys` | `zlib1g-dev` | Used by multiple deps |
| (transitive) | `openssl-sys` | `libssl-dev` | Dynamic linking via pkg-config |
| `secrets-aws` | `aws-lc-sys` | - | C/C++ compiled from source (no system lib available); ~20-30s first build, cached by sccache |

For `librdkafka-dev` >= 2.12.1, add the Confluent APT repo. The suite below is
`bookworm`, which is what a Debian trixie host uses - Confluent publishes no
trixie suite and the bookworm .deb installs cleanly on trixie. On an Ubuntu
24.04 host use `noble`:

```bash
curl -fsSL https://packages.confluent.io/clients/deb/archive.key \
  | sudo gpg --dearmor -o /usr/share/keyrings/confluent-clients.gpg
echo "deb [signed-by=/usr/share/keyrings/confluent-clients.gpg] \
  https://packages.confluent.io/clients/deb bookworm main" \
  | sudo tee /etc/apt/sources.list.d/confluent-clients.list
sudo apt-get update
sudo apt-get install -y librdkafka-dev libssl-dev libsasl2-dev pkg-config
```

### Deployment Host (Runtime)

The compiled binary links against `.so` files at runtime. Install the
**runtime** packages (not `-dev`) on deployment hosts or in Docker images.

| Feature | Runtime Package | Shared Object |
|---------|----------------|---------------|
| `transport-kafka` | `librdkafka1` (from Confluent repo) | `librdkafka.so.1` |
| `directory-config-git` | `libgit2-1.9` on trixie, `libgit2-1.7` on noble | `libgit2.so` |
| `spool`, `tiered-sink` | `libzstd1` | `libzstd.so.1` |
| (transitive) | `zlib1g` | `libz.so.1` |
| (transitive) | `libssl3t64` on trixie/noble, `libssl3` on bookworm/jammy | `libssl.so.3` |

Only install what you use. Check the features your binary enables to
determine which runtime packages are needed.

### Docker Example

This is the shape the `deployment` feature's generator emits, minus the
generated LABEL and APT blocks. The `WORKDIR /app` in the build stage is what
makes `/app/target/release/...` resolvable from the runtime stage.

```dockerfile
# Build stage
FROM rust:1 AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev librdkafka-dev libgit2-dev libzstd-dev
COPY . .
RUN cargo build --release

# Runtime stage - the contract's base_image (default: the org base, currently
# Debian trixie slim)
FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 libssl3t64 libgit2-1.9 libzstd1 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/myapp /usr/local/bin/myapp
RUN chmod +x /usr/local/bin/myapp

RUN useradd --create-home --uid 1000 appuser
USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/livez > /dev/null || exit 1

ENTRYPOINT ["myapp"]
```

`libgit2-1.9` and `libssl3t64` are the trixie package names; on a noble base
they are `libgit2-1.7` and `libssl3t64`, on bookworm `libgit2-1.5` and
`libssl3`. The `deployment` feature works these out for you from the
contract's release - see the release table in
[docs/deployment/native-deps.md](docs/deployment/native-deps.md), which also
covers adding the Confluent APT repo to both stages for `librdkafka1`.
The generator also drops any pre-existing UID 1000 account (ubuntu bases ship
one) before creating `appuser`; trixie slim does not, so the example skips it.
Note that Kubernetes ignores `HEALTHCHECK` - it is there for plain Docker and
Compose. K8s uses the probe paths above.

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
and opt-out via `self_regulation.enabled = false`. See
[docs/self-regulation.md](docs/self-regulation.md).

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
