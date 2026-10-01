# Core pillars

The seven things every scalo service gets whether it asks or not. Wire
`ServiceRuntime` (or the three setup calls) and all of this is running before
your first line of business logic.

These are singletons on purpose: a service has one config, one log stream, one
metrics registry. Making them global removes the plumbing that every service
would otherwise write, identically, and get subtly wrong.

| Doc | Covers |
| --- | --- |
| [config.md](config.md) | 7-layer cascade, hot-reload, section registry, `/config` endpoint |
| [logging.md](logging.md) | tracing setup, JSON/text autodetect, field masking, flood control |
| [metrics.md](metrics.md) | Prometheus exporter, manifest catalogue, cardinality cap |
| [tracing.md](tracing.md) | OTel, W3C traceparent, transport propagation |
| [health.md](health.md) | `HealthRegistry`, `/livez` / `/readyz` |
| [shutdown.md](shutdown.md) | `CancellationToken`, K8s pre-stop delay, drain ordering |
| [lifecycle.md](lifecycle.md) | Idle until configured -- `WorkState`, the gate, `pipeline_idle` |

Start with [../auto-wiring.md](../auto-wiring.md) if you want to know what is
wired automatically and what you still have to call yourself.
