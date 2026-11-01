# scalo

The runtime layer for Rust data-plane services: config, logging, metrics, health probes, memory backpressure, transports and secrets, already wired together. Pick the features you need, and each one plugs into the same config and the same metrics.

It is built for the hot path. For APIs, orchestrators and integration glue there is a Python sister library, [scalo-py](https://github.com/hyperi-io/scalo-py), with the same conventions and a different API.

## What it gives you

- A seven-layer config cascade, from CLI args and env vars down to YAML files and defaults, read the same way in a laptop shell and a Kubernetes pod.
- Structured logs, JSON in a container and readable text at a terminal. Passwords, tokens, keys, URL passwords and named personal-data fields are written as `[REDACTED]`.
- Prometheus and OpenTelemetry metrics with the `metrics` feature, with `/livez` and `/readyz` served on the same listener as `/metrics`.
- `MemoryGuard`, which reads the cgroup memory limit and tells the hot path when to stop taking work, so a service sheds load before the kernel OOM-kills it.
- Transports for Kafka, gRPC, HTTP, files, pipes and in-memory, behind one sender and receiver interface.
- `TieredSink`, which wraps any sender with a timeout and a circuit breaker and spills to a disk spool when the downstream is slow or gone, then drains it. A dead letter queue takes the records a downstream refuses for good, to a file, Kafka or HTTP.
- Secrets from files, OpenBao or Vault, and AWS Secrets Manager, cached with a TTL. Call `refresh_all` on your own schedule and it re-reads them, announcing any that rotated.
- A deployment contract: fill in one struct and scalo writes your Dockerfile and the JSON the scalo-service Helm chart renders from.

## Example

```console
cargo add scalo
cargo add tracing
```

```rust,no_run
use scalo::{config, env, logger};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let environment = env::Environment::detect();
    logger::setup_default()?;
    config::setup(config::ConfigOptions {
        env_prefix: "MYAPP".into(),
        ..Default::default()
    })?;

    let db_host = config::get().get_string("database.host").unwrap_or_default();
    tracing::info!(?environment, %db_host, "started");
    Ok(())
}
```

`MYAPP_DATABASE__HOST=db.local cargo run` logs `started` with `db_host=db.local`. A `database.host` in `settings.yaml` is the fallback, and the env var overrides it. For a whole service, implement `ServiceApp` (`cli-service` feature) and get `run`, `version` and `config-check`, with config, logging, metrics and the memory guard started for you.

## Features

The default is `config` and `logger`. Everything else is opt-in, so you pay only for what you use.

| Feature | Adds |
| --- | --- |
| `config`, `logger` | The cascade and the logger (default) |
| `metrics` | Prometheus `/metrics` and the health probes, OTLP metric push and span export, process gauges |
| `memory` | The cgroup-aware memory guard |
| `cli-service` | `ServiceApp` and the service runtime, with metrics, memory and shutdown |
| `transport-kafka`, `transport-grpc`, `transport-http`, `transport-file`, `transport-pipe`, `transport-memory` | One transport each |
| `tiered-sink`, `spool`, `dlq` | Resilient delivery, the disk spool, the dead letter queue |
| `worker` | The adaptive worker pool and the SIMD batch engine |
| `secrets-vault`, `secrets-aws` | One secrets backend each |
| `deployment` | The deployment contract and its Dockerfile and manifest generators |
| `full` | All of the above and more |

There are 69 features in all. The full list, what each pulls in and which combinations matter are at <https://github.com/hyperi-io/scalo-rs/blob/main/docs/feature-flags.md>. `transport-kafka` links librdkafka, so the build host and the image need it: <https://github.com/hyperi-io/scalo-rs/blob/main/docs/deployment/native-deps.md>.

API docs are at <https://docs.rs/scalo>, the source and the guides at <https://github.com/hyperi-io/scalo-rs>. Requires Rust 1.95 or later. Licensed Apache-2.0: <https://github.com/hyperi-io/scalo-rs/blob/main/LICENSE>.
