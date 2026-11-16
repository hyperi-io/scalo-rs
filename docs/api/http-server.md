# HTTP server

`HttpServer` is an axum-backed server for apps that want their own HTTP
listener with the health paths attached. It is an OPTIONAL extra, not the
observability port.

Read this first, because the distinction has bitten people. There are TWO
servers and they are not interchangeable:

| | Metrics server | `HttpServer` |
| --- | --- | --- |
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

What `build_router` mounts:

| Path | Wired by | What it returns |
| ------ | ---------- | ----------------- |
| `/livez` | on by default (`enable_health_endpoints`) | 200 if process is alive (no dep checks -- never restart on dep down) |
| `/readyz` | same | 200 if `ready_flag` is true AND all registered checks pass, 503 otherwise |
| `/health/detailed` | `health` + `serde_json` | per-check JSON breakdown |
| `/config` | opt-in via `enable_config_endpoint` | JSON dump of every registered config section, with secrets redacted |

NOT mounted here, whatever the feature set: `/metrics`, `/metrics/manifest`,
`/scaling/pressure`, `/memory/pressure`. Those belong to the metrics server --
see [../core-pillars/metrics.md](../core-pillars/metrics.md).

Three `HttpServerConfig` fields parse and do nothing. `enable_metrics_endpoint` is never read by `build_router`. `keep_alive_timeout_ms` (75 s) and `enable_http2` (`true`) are not wired: axum's `serve()` sets no keep-alive and always negotiates HTTP/1 and HTTP/2 on cleartext, so `false` does not turn HTTP/2 off.

---

## Usage

`ServiceRuntime` never builds an `HttpServer`, so an app that wants one
constructs and serves it itself:

```rust
use scalo::http_server::{HttpServer, HttpServerConfig};
use axum::{Router, routing::get};

let server = HttpServer::new(HttpServerConfig::default());
let app = Router::new()
    .route("/whoami", get(|| async { "my-app" }));

server.serve_with_shutdown(app, shutdown.cancelled()).await?;
```

The probe and (opt-in) `/config` routes are merged onto your
router internally by `serve` / `serve_with_shutdown` / `serve_with_handle`
-- you only supply your own extra routes.

---

## Ready flag

The ready flag is an `Arc<AtomicBool>`, true from construction, that the
server hands out via `ready_flag()`. The shutdown handler clears it before draining traffic,
so K8s sees `/readyz` flip to 503 BEFORE the cancellation token starts
draining work. That is what avoids in-flight requests dying mid-flight
during a rolling deploy.

```rust
let flag = server.ready_flag();
flag.store(false, Ordering::Release); // not ready until init completes
// ... init ...
flag.store(true, Ordering::Release);
```

---

## TLS

With the `tls` feature the server terminates TLS itself. Set `tls_cert_path` and `tls_key_path` and it serves that certificate, offers ALPN `h2` and `http/1.1`, and re-reads both files every 30 s, so a rotated certificate is served from the next handshake with no restart. A bad new certificate keeps the old one in service.

For a certificate from a secret, client certificates, a crypto profile or one certificate shared with other listeners, load a `ServerTls` and pass it in:

```rust
let tls = ServerTls::load_with(&config.tls, ServerTlsOptions::http()).await?;
HttpServer::new(config.http_server).with_tls(tls).serve(app).await?;
```

`validate()` runs before anything binds, so the server never serves cleartext while its config asks for TLS. It rejects one path set without the other, either path in a build without the `tls` feature, and `with_tls` together with the paths. [tls.md](tls.md) covers sources, reload and the handshake metrics.

---

## Graceful shutdown

`serve_with_shutdown` takes any `Future<Output = ()>`, and the typical wiring
is `shutdown_token.cancelled()`. axum drains in-flight requests, then
the future returns.

For test wiring or programmatic shutdown, `serve_with_handle` returns a
`ShutdownHandle` you can call `.shutdown()` on plus a
`ServerFuture` you await for the drain.

---

## Config shape

```yaml
http_server:
  bind_address: "0.0.0.0:8080"
  request_timeout_ms: 30000
  max_connections: 10000          # in-flight cap; excess requests queue
  max_body_bytes: 16777216        # 413 past this; 0 turns the cap off
  enable_health_endpoints: true
  enable_config_endpoint: false   # opt-in -- exposes redacted /config
  shutdown_timeout_ms: 30000
```

`max_body_bytes` applies to every route, so a handler that takes the body through a custom extractor or streams it is capped too. A declared `Content-Length` over the limit is answered 413 before the handler runs. `tls_cert_path` and `tls_key_path` turn on TLS, see [TLS](#tls).

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `HttpServer::new(config)` | Build from explicit config |
| `HttpServer::bind(addr)` | Build with just a bind address |
| `.with_tls(ServerTls)` | Terminate TLS with a loaded `ServerTls` (`tls` feature) |
| `.serve(app)` | Run until SIGTERM or Ctrl-C, merging the probe and config routes |
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
- [tls.md](tls.md) -- server TLS, certificate reload, the shared handshake helper
- [../runtime/service-runtime.md](../runtime/service-runtime.md) -- automatic wiring
- [../pipeline/scaling.md](../pipeline/scaling.md) -- `/scaling/pressure` endpoint
- Source: [../../src/http_server/](../../src/http_server/)
