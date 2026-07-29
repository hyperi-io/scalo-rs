# Health

Three K8s probes with three distinct semantics, all mounted by the metrics HTTP
server. Modules register a health-check callback into a global `HealthRegistry`;
`/readyz` aggregates every registered check plus an optional caller callback to
decide 200 vs 503.

**Liveness must NEVER check downstream dependencies.** Mixing liveness with
dependency checks is the single most common cause of cascading restart loops --
when the DB goes down, restarting every replica makes recovery slower. Liveness
exists only to detect a deadlocked process.

---

## The two probes

| Endpoint | Semantics | Fails when | K8s action |
|---|---|---|---|
| `/livez` | Liveness -- process alive | Never (always 200) | Kill + restart pod |
| `/readyz` | Readiness -- deps OK + ready flag | Registry unhealthy OR readiness callback false OR ready flag cleared | Remove from Service endpoints (no traffic), don't restart |

That is the whole surface. There are no aliases and no startup endpoint --
every retired path returns 404. A `startupProbe` targets `/livez`.

Bodies: `{"status":"alive"}` / `{"status":"ready"}` on 200;
`{"status":"not_ready"}` on 503.

Readiness aggregates the registry AND the explicit ready flag. The shutdown
handler clears the flag before draining, so K8s pulls the pod from Service
endpoints before in-flight work ends. See [shutdown.md](shutdown.md).

---

## Registry

`HealthRegistry` is a global `OnceLock` -- registered checks live for the process
lifetime. **Registration is manual** (modules call `register` at construction);
**aggregation is automatic** (every `/readyz` query walks all entries, no
endpoint-side wiring).

```rust
use scalo::health::{HealthRegistry, HealthStatus};
use std::sync::atomic::{AtomicBool, Ordering};

static KAFKA_HEALTHY: AtomicBool = AtomicBool::new(true);

HealthRegistry::register("kafka_consumer", || {
    if KAFKA_HEALTHY.load(Ordering::Relaxed) { HealthStatus::Healthy }
    else { HealthStatus::Unhealthy }
});
```

The callback fires on every health request -- keep it cheap (an `AtomicBool` load
or cached value, never a syscall or network call).

Empty registry is vacuously healthy and ready -- a service with no registered
checks always returns 200. Register at least one (transport, DB, downstream API)
so the probe means something.

---

## Three states

```rust
pub enum HealthStatus { Healthy, Degraded, Unhealthy }
```

| Status | `is_healthy()` | `is_ready()` | Use for |
|---|---|---|---|
| `Healthy` | Yes | Yes | Fully operational |
| `Degraded` | No | Yes | Impaired but serving -- circuit half-open, fallback active, elevated latency |
| `Unhealthy` | No | No | Not operational -- stop traffic |

`is_ready()` (registry: no component `Unhealthy`) is the readiness predicate --
degraded still serves. `is_healthy()` (registry: all `Healthy`) is strict, used
for detailed status output, not the K8s probe. Both are registry-level methods,
not on the enum.

---

## Per-component status

```rust
let components: Vec<(String, HealthStatus)> = HealthRegistry::components();
// [("kafka_consumer", Healthy), ("clickhouse", Degraded)]
```

With `serde_json`, `HealthRegistry::to_json()` produces the detailed-endpoint
response. Overall status is `healthy` if all components are healthy, `degraded`
if any are degraded but none unhealthy, `unhealthy` if any are unhealthy:

```json
{
  "status": "degraded",
  "components": [
    {"name": "kafka_consumer", "status": "healthy"},
    {"name": "clickhouse", "status": "degraded"}
  ]
}
```

---

## K8s manifest

> **Point the startupProbe at `/livez`.** There is no startup endpoint.
> Kubernetes suspends liveness until the startup probe passes, so one path
> gives both a generous boot budget (`failureThreshold`) and a tight liveness
> period, without the two drifting apart. This is what the generated Helm
> chart does.

```yaml
spec:
  containers:
    - name: myapp
      ports:
        - { name: metrics, containerPort: 9090 }
      startupProbe:
        httpGet: { path: /livez, port: metrics }
        failureThreshold: 30          # 30 * 2s = 1 min boot budget
        periodSeconds: 2
      livenessProbe:
        httpGet: { path: /livez, port: metrics }
        periodSeconds: 10
      readinessProbe:
        httpGet: { path: /readyz, port: metrics }
        periodSeconds: 5
```

Use the metrics port -- the metrics server hosts the probes and the scrape on
the one listener, so no separate health listener is needed. Note this is the
METRICS server, not `HttpServer`: that one is a separate optional listener and
does not serve `/metrics`.

---

## Wire-up checklist

1. Construct `MetricsManager`. Call `mgr.set_readiness_check(|| ...)` for an extra
   callback gate (ANDed with the registry).
2. Modules `HealthRegistry::register()` at construction.
3. Call `mgr.set_ready()` once init is complete (DB connected, Kafka
   subscribed, config loaded) -- `/readyz` flips to 200 and traffic arrives.
4. Start the server via `mgr.start_server` / `mgr.start_server_with_routes`.
5. On SIGTERM the shutdown handler clears the ready flag, waits the pre-stop
   delay, then cancels the global token. See [shutdown.md](shutdown.md).

---

## API surface

| Item | Purpose |
|---|---|
| `HealthRegistry::register(name, fn)` | Add a component health-check callback |
| `HealthRegistry::is_healthy() -> bool` | All components `Healthy` |
| `HealthRegistry::is_ready() -> bool` | No components `Unhealthy` |
| `HealthRegistry::components() -> Vec<(String, HealthStatus)>` | Snapshot |
| `HealthRegistry::to_json() -> serde_json::Value` | Detailed-endpoint response |
| `HealthStatus::{Healthy, Degraded, Unhealthy}` | Three-state enum |
| `HealthStatus::as_str()` | `"healthy"` / `"degraded"` / `"unhealthy"` |
| `MetricsManager::set_readiness_check(fn)` | Caller callback gate (ANDed with registry) |

---

## Testing

The registry is process-global. Tests that register components use
`HealthRegistry::reset()` (test-only) under a serialised lock, otherwise test
order changes the registered set. For readiness-aggregation tests, register a
callback reading an `AtomicU8` and flip it between states. See
[`src/health/registry.rs`](../../src/health/registry.rs).

---

## Related

- [metrics.md](metrics.md) -- `/readyz` is served by the metrics HTTP server
- [shutdown.md](shutdown.md) -- ready flag clearing before drain
- [../auto-wiring.md](../auto-wiring.md), [../feature-flags.md](../feature-flags.md) -- `health`
- Source: [`src/health/mod.rs`](../../src/health/mod.rs), [`src/health/registry.rs`](../../src/health/registry.rs)
