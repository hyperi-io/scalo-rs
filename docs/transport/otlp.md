# OTLP -- log records and metrics to a collector

`transport-otlp` sends records to an OpenTelemetry collector as OTLP log records, over gRPC (`LogsService/Export`) or HTTP/protobuf (`POST /v1/logs`). It sends only. A block goes out as one export request, and `Ok` means the collector answered that it took every record, so its delivery guarantee is `Acknowledged`. The same transport carries metrics on the same connection ([Metrics](#metrics)). The protobuf types come from `opentelemetry-proto`, so it needs no `protoc` to build.

```yaml
transport:
  output:
    type: otlp
    otlp:
      endpoint: "https://collector:4317"
      protocol: grpc              # or http
      headers:
        authorization: "Bearer <token>"
      timeout_ms: 10000
      compression: {codec: gzip}  # default none. No level
      max_retry_delay_ms: 300000
      service_name: my-app
      resource_attributes:
        deployment.environment.name: prod
      tls:
        trust: bundle             # native (default), platform (http only) or bundle
        ca_path: /etc/tls/ca.pem
        client_cert_path: /etc/tls/client.pem
        client_key_path: /etc/tls/client.key
      proxy: system               # none (default), system or a proxy URL
      connect_to: 10.0.0.5:4317   # unset by default. Not with a proxy
```

## Settings

- Endpoint: a base URL, `http://localhost` by default. Its port follows four rules:
  - an `http://` endpoint naming no port, the default included, takes 4317 under `grpc` and 4318 under `http`
  - an `https://` endpoint naming no port keeps 443, where hosted collectors behind a TLS gateway listen
  - a named port is used as given, so `http://localhost:4317` under `http` stays on 4317
  - under `http`, `/v1/logs` and `/v1/metrics` are appended, and an endpoint ending in either is read as the base before it, so `https://collector:4318/v1/logs` sends metrics to `https://collector:4318/v1/metrics`
- TLS: an `https://` endpoint with no `tls` section trusts the OS root store, and a `tls` section beside an `http://` endpoint is refused at build. The section is the `ClientTlsConfig` that syslog reads too ([../api/tls.md](../api/tls.md#client-trust)). gRPC takes `native` or `bundle` and no `pin_sha256`, which `http` checks.
- Headers: gRPC metadata or HTTP headers on every request, commonly an API key. They are never logged or serialised back out, and a name or value the protocol cannot carry fails the build, naming the header and not its value.
- Deadline: `timeout_ms` (default 10 s, 0 refused) bounds each export, connect and TLS handshake included. `send_batch_by` answers by its deadline when that comes first. Either one running out is `Backpressured`: the records may have arrived, so the retry can duplicate them. gRPC sends the limit as `grpc-timeout` too.
- Proxy: `proxy` is `none`, `system` or an `http://` proxy URL, as for the HTTP transport ([http.md](http.md#proxy-redirects-and-logs)). gRPC and an `https` collector go through an HTTP CONNECT tunnel. A refused CONNECT is `Backpressured`, so the records are held and sent again. It is counted in `transport_proxy_refused_total`, logged once per run with the proxy's status and the collector's port, and fails `is_healthy()` and the `transport:otlp` health check until an export goes through.
- Connect-to: `connect_to` is an IP address, or `IP:port` (`[v6]:port`), dialled in place of the endpoint's host and port. TLS still checks the endpoint's host name, and requests carry the endpoint's host and port as `Host` (HTTP) or `:authority` (gRPC). The endpoint must name its host, and `connect_to` beside a `proxy` is refused at build, since the proxy looks the name up itself. Only the OTLP transport takes it for now.
- Redirects: not followed.
- Filters: `filters_out` runs on each payload before the request is built. A `dlq` match makes the block's result `FilteredDlq`, and `dead_letter_reason` names the record.

## Records

| Record | Log record |
| --- | --- |
| payload | `body`: a string when it is UTF-8, bytes otherwise |
| `time_unix_nano` header | `time_unix_nano`, decimal nanoseconds since the epoch. Anything else stays an attribute |
| `metadata.timestamp_ms` | `time_unix_nano` when there is no `time_unix_nano` header, 0 (unknown) when unset. `observed_time_unix_nano` is the send time |
| `severity` header | `severity_text` as given, and `severity_number`: trace 1, debug 5, info 9, notice 10, warn 13, error 17, crit 18, alert 19, fatal or emerg 21, or a number from 1 to 24 as given. Not UTF-8, it stays an attribute |
| every other header | an attribute, a string when it is UTF-8, bytes otherwise |
| `key`, and `send`'s destination | not sent |

The header names are `TIME_UNIX_NANO_HEADER` and `SEVERITY_HEADER` in `scalo::transport::otlp`. A source with the time in nanoseconds sets the header, and the millisecond timestamp stays for every other transport.

The resource carries `service.name` (`service_name`, else `OTEL_SERVICE_NAME`, else `unknown_service`), `deployment.environment.name` from the resolved app env as the metrics resource has it ([../runtime/runtime-context.md](../runtime/runtime-context.md#app-env-and-production-mode)), the replica identity metrics and spans carry (`service.instance.id`, `k8s.pod.name`), `service.version` from `SERVICE_VERSION`, the host's `host.name`, `host.id` (the machine id on Linux and macOS, left off when empty) and `host.arch`, `os.type`, `OTEL_RESOURCE_ATTRIBUTES`, and `resource_attributes`, which win over the rest. A key `OTEL_RESOURCE_ATTRIBUTES` sets is never replaced by a detected one. An app that needs another mapping registers its own sender ([custom.md](custom.md)).

## Collector answers

| Collector answer | `SendResult` |
| --- | --- |
| accepted | `Ok` |
| accepted with `rejected_log_records` above 0 | `Reject`, carrying the collector's message |
| HTTP 429, 502, 503, 504, gRPC `UNAVAILABLE` | `RetryAfter(wait)` from `Retry-After` in seconds or `RetryInfo`, else `Backpressured` |
| gRPC `RESOURCE_EXHAUSTED` | `RetryAfter(wait)` with `RetryInfo`, else `Reject`: without one it is most often a request over the collector's size limit |
| gRPC `CANCELLED`, `DEADLINE_EXCEEDED`, `ABORTED`, `OUT_OF_RANGE`, `DATA_LOSS`, or no answer (refused, reset, timed out) | `Backpressured` |
| HTTP 401, 403, 404, 405, 407, 410, 421, 1xx and 3xx, gRPC `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `UNIMPLEMENTED` | `Fatal(Config)`: the credential or endpoint is wrong, which says nothing about the records |
| a TLS refusal of the collector's certificate or name, and over `http` a key off the pin or the client certificate turned away | `Fatal(Config)`, naming the rustls error |
| HTTP 413 | `Reject(MessageTooLarge)` |
| any other HTTP 4xx or 5xx, any other gRPC code | `Reject` |

The split is the OTLP specification's. `Backpressured` and `RetryAfter` hold the records and send them again. `Reject` goes to the caller's dead-letter path and is never retried, because the same request gets the same answer: that includes HTTP 500, gRPC `INTERNAL`, `UNKNOWN` and `INVALID_ARGUMENT`, and `RESOURCE_EXHAUSTED` without `RetryInfo`. `Fatal(Config)` stops until the config or the collector is fixed.

- Waits: a `Retry-After` or `RetryInfo` wait is capped at `max_retry_delay_ms` (default 5 min), so a collector cannot hold the sender indefinitely. A zero wait is `Backpressured`, and so is a `Retry-After` given as an HTTP date, which is not read.
- A `Reject` names no record: a caller holding the records sends the block again one record at a time to find the one refused, which also gets a block over the collector's size limit through.
- Logs: the build at info, with the endpoint (user name and password left out), `connect_to`, protocol, trust and header count. Each export at debug, a retry or `Reject` at warn, `Fatal` at error.
- Counters: `transport_sent_total` and `transport_sent_bytes_total` (payload bytes) for each record taken, `transport_backpressured_total` and `transport_send_errors_total` for each failed export, and `transport_send_duration_seconds`, all with `transport="otlp"`.
- `is_healthy()`: `false` after `close()`, which refuses further sends with `Fatal(Closed)`. It does not probe the collector.

## Metrics

`OtlpMetricExporter` is an OpenTelemetry SDK push exporter over an `OtlpTransport`. Each export is one `MetricsService/Export` call or `POST /v1/metrics` on the transport's connection, with its TLS, client certificate, proxy, `connect_to`, headers, compression and `timeout_ms`. A refused proxy CONNECT fails readiness for a metric export as it does for logs, and an export that goes through clears it.

```rust
let transport = Arc::new(OtlpTransport::new(&OtlpConfig::from_cascade()?)?);
let exporter = OtlpMetricExporter::new(Arc::clone(&transport))?; // inside a Tokio runtime
let provider = SdkMeterProvider::builder()
    .with_reader(PeriodicReader::builder(exporter).build())
    .build();
```

- Resource: the meter provider's, not the transport's, so a `MetricsManager` keeps its own `service.name` and `metrics.otel.resource_attributes`. Both carry the same replica and host identity.
- Units: the meter provider's. `MetricsManager` exports the facade's as UCUM codes, `By` for bytes ([../core-pillars/metrics.md](../core-pillars/metrics.md#otel-mode)).
- Temporality: cumulative. A failed export is not retried: the next one that lands carries the totals.
- Results: a collector answer maps as for logs, and anything but `Ok` is an export error the SDK reports, `rejected_data_points` included. A metric export moves no `transport_*` counter, runs no filter and is logged at debug.
- Shutdown: the exporter's stops it exporting and leaves the transport open for its logs. A closed transport fails every metric export.

`MetricsManager` pushes through the transport with `metrics.otel.via_transport: true`, which builds one from `transport.otlp`, or with `MetricsManager::with_otlp_transport(config, transport)`, which shares the app's own. Push then needs no `/metrics` server, and the process and container gauges refresh with it ([../core-pillars/metrics.md](../core-pillars/metrics.md#otel-mode)).

Source: [../../src/transport/otlp/](../../src/transport/otlp/), [../../src/transport/connect_to.rs](../../src/transport/connect_to.rs).
