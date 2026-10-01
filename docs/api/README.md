# Subsystem APIs

The parts a service reaches for when it needs them, rather than getting for
free. Each is behind its own feature flag and none is wired automatically.

If you are looking for config, logging, metrics, tracing, health or shutdown,
those are auto-wired and live in [../core-pillars/README.md](../core-pillars/README.md).

| Doc | Covers |
| --- | --- |
| [secrets.md](secrets.md) | OpenBao / Vault, AWS Secrets Manager, file backend |
| [http-server.md](http-server.md) | the optional axum listener, and what it does NOT serve |
| [http-client.md](http-client.md) | `reqwest` plus retry, backoff and the signing hook |
| [auth.md](auth.md) | credential acquisition (token exchanges, metadata servers) and placement |
| [directory-config.md](directory-config.md) | YAML directory store, optional git backing |
| [concurrency.md](concurrency.md) | `BackgroundSink`, `PeriodicWorker`, `ActorHandle` |
| [geoip-download.md](geoip-download.md) | GeoIP MMDB provisioning -- files onto disk, no lookup engine |
| [cache.md](cache.md) | documented, NOT built into this release -- see the note in it |

> **Note:** [http-server.md](http-server.md) is worth reading before you point a
> probe or a scrape at a port. The `http-server` listener and the metrics
> listener are different servers and do not serve the same paths.
