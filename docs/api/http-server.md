# HTTP server

`HttpServer` is an axum-backed server for apps that want their own HTTP
listener with the health paths attached. It is an OPTIONAL extra, not the
observability port.

Read this first, because the distinction has bitten people. There are TWO
servers and they are not interchangeable:

| | Metrics server | `HttpServer` |
|---|---|---|
| Started by | `ServiceRuntime` automatically (`metrics` feature) | the app, explicitly |
| Default bind | `--metrics-addr`, `0.0.0.0:9090` | `0.0.0.0:8080` |
| Implementation | hand-rolled over tokio, no axum | axum |
| Serves `/metrics` | YES | **no** |
| Serves `/metrics/manifest` | YES | **no** |

**The observability port -- the one the deployment contract advertises and
Prometheus scrapes -- is the METRICS server, not this one.** `ServiceRuntime`
never constructs `HttpServer`. An app that stands up only `HttpServer` on the
contract's `metrics_port` will answer the health probes and 404 the scrape.

The server respects the same `CancellationToken` as the rest of the runtime so
SIGTERM drains in-flight requests before exit.

---

## Mounted endpoints

What `build_router` actually mounts:

| Path | Wired by | What it returns |
|------|----------|-----------------|
| `/livez` | on by default (`enable_health_endpoints`) | 200 if process is alive (no dep checks -- never restart on dep down) |
| `/readyz` | same | 200 if `ready_flag` is true AND all registered checks pass; 503 otherwise |
| `/health/detailed` | `health` + `serde_json` | per-check JSON breakdown |
| `/config` | opt-in via `enable_config_endpoint` | JSON dump of every registered config section, with secrets redacted |

NOT mounted here, whatever the feature set: `/metrics`, `/metrics/manifest`,
`/scaling/pressure`, `/memory/pressure`. Those belong to the metrics server --
see [../core-pillars/metrics.md](../core-pillars/metrics.md).

`enable_metrics_endpoint` on `HttpServerConfig` is currently inert: the field
exists and `build_router` does not read it. Setting it changes nothing.

---

## Usage

The common case is implicit -- `ServiceRuntime` calls `HttpServer::serve`
on your behalf. Apps that need to mount extra routes do it through
the runtime hook (or call `HttpServer` directly for tooling-style
apps):

```rust
use scalo::http_server::{HttpServer, HttpServerConfig};
use axum::{Router, routing::get};

let server = HttpServer::new(HttpServerConfig::default());
let app = Router::new()
    .route("/whoami", get(|| async { "myapp" }));

server.serve_with_shutdown(app, shutdown.cancelled()).await?;
```

The probe, metrics, and (opt-in) `/config` routes are merged onto your
router internally by `serve` / `serve_with_shutdown` / `serve_with_handle`
-- you only supply your own extra routes.

---

## Ready flag

The ready flag is an `Arc<AtomicBool>` the server hands out via
`ready_flag()`. The shutdown handler clears it before draining traffic,
so K8s sees `/readyz` flip to 503 BEFORE the cancellation token starts
draining work. That is what avoids in-flight requests dying mid-flight
during a rolling deploy.

```rust
let flag = server.ready_flag();
// Later, after init completes:
flag.store(true, Ordering::Release);
// Pre-stop hook (auto-wired by ServiceRuntime):
flag.store(false, Ordering::Release);    // K8s stops routing
tokio::time::sleep(PRESTOP_DELAY).await; // give K8s time to notice
shutdown_token.cancel();                  // now drain
```

---

## TLS

**In-process TLS termination is not supported.** The K8s pattern is to
terminate TLS at the ingress / service mesh and run cleartext in-pod.

`HttpServerConfig` exposes `tls_cert_path` / `tls_key_path`, but they are
**not wired** -- setting either is rejected by `HttpServerConfig::validate()`,
which `serve` / `serve_with_shutdown` / `serve_with_handle` call before
binding, so a config expecting in-pod TLS fails loudly rather than silently
serving cleartext. Front the service with a TLS sidecar or ingress instead.

---

## Graceful shutdown

`serve_with_shutdown` takes any `Future<Output = ()>`; typical wiring
is `shutdown_token.cancelled()`. axum drains in-flight requests, then
the future returns.

For test wiring or programmatic shutdown, `serve_with_handle` returns a
`ShutdownHandle` you can call `.shutdown()` on plus a
`ServerFuture` you await for the drain.

---

## Config shape

```yaml
http_server:
  bind_address: "0.0.0.0:9090"
  enable_config_endpoint: false   # opt-in -- exposes redacted /config
  tls:
    cert_path: /etc/myapp/tls.crt
    key_path:  /etc/myapp/tls.key
  request_timeout: 30s
```

---

## API surface

| Item | Purpose |
|------|---------|
| `HttpServer::new(config)` | Build from explicit config |
| `HttpServer::bind(addr)` | Build with just a bind address |
| `.serve(app)` | Run until the future is dropped (merges probes/metrics/config routes) |
| `.serve_with_shutdown(app, shutdown)` | Run until the shutdown future resolves |
| `.serve_with_handle(app)` | Returns (`ShutdownHandle`, `ServerFuture`) |
| `.set_ready(bool)` | Toggle the ready flag |
| `.is_ready() -> bool` | Read the ready flag |
| `.ready_flag() -> Arc<AtomicBool>` | Hand out the flag for external coordination |
| `ShutdownHandle::shutdown()` | Trigger graceful shutdown from outside |

---

## Related

- [../core-pillars/health.md](../core-pillars/health.md) -- health probes semantics
- [../core-pillars/metrics.md](../core-pillars/metrics.md) -- `/metrics` + `/metrics/manifest`
- [../core-pillars/config.md](../core-pillars/config.md) -- `/config` endpoint
- [../core-pillars/shutdown.md](../core-pillars/shutdown.md) -- pre-stop, K8s drain flow
- [../runtime/service-runtime.md](../runtime/service-runtime.md) -- automatic wiring
- [../pipeline/scaling.md](../pipeline/scaling.md) -- `/scaling/pressure` endpoint
- Source: [../../src/http_server/](../../src/http_server/)
