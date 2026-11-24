# OTLP -- log records to a collector

`transport-otlp` sends records to an OpenTelemetry collector as OTLP log records, over gRPC (`LogsService/Export`) or HTTP/protobuf (`POST /v1/logs`). It sends only. A block goes out as one export request, and `Ok` means the collector answered that it took every record, so its delivery guarantee is `Acknowledged`. The protobuf types come from `opentelemetry-proto`, so it needs no `protoc` to build.

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
```

- Endpoint: a base URL. Unset, it is `http://localhost:4317`, or `http://localhost:4318` under `http`. Under `http`, `/v1/logs` is appended unless the URL already ends in it.
- TLS: an `https://` endpoint with no `tls` section trusts the OS root store, and a `tls` section beside an `http://` endpoint is refused at build. The section is the `ClientTlsConfig` that syslog reads too ([../api/tls.md](../api/tls.md#client-trust)). gRPC takes `native` or `bundle` and no `pin_sha256`, which `http` checks.
- Headers: gRPC metadata or HTTP headers on every request, commonly an API key. They are never logged or serialised back out, and a name or value the protocol cannot carry fails the build, naming the header and not its value.
- Deadline: `timeout_ms` (default 10 s, 0 refused) bounds each export, connect and TLS handshake included. `send_batch_by` answers by its deadline when that comes first. Either one running out is `Backpressured`: the records may have arrived, so the retry can duplicate them. gRPC sends the limit as `grpc-timeout` too.
- Proxy: `proxy` is `none`, `system` or an `http://` proxy URL, as for the HTTP transport ([http.md](http.md#proxy-redirects-and-logs)). gRPC and an `https` collector go through an HTTP CONNECT tunnel, A refused CONNECT is `Backpressured`, so the records are held and sent again. It is counted in `transport_proxy_refused_total`, logged once per run with the proxy's status and the collector's port, and fails `is_healthy()` and the `transport:otlp` health check until an export goes through.
- Redirects: not followed.
- Filters: `filters_out` runs on each payload before the request is built. A `dlq` match makes the block's result `FilteredDlq`, and `dead_letter_reason` names the record.

## Records

| Record | Log record |
| --- | --- |
| payload | `body`: a string when it is UTF-8, bytes otherwise |
| `metadata.timestamp_ms` | `time_unix_nano`, 0 (unknown) when unset. `observed_time_unix_nano` is the send time |
| `severity` header | `severity_text` as given, and `severity_number`: trace 1, debug 5, info 9, notice 10, warn 13, error 17, crit 18, alert 19, fatal or emerg 21, or a number from 1 to 24 as given. Not UTF-8, it stays an attribute |
| every other header | an attribute, a string when it is UTF-8, bytes otherwise |
| `key`, and `send`'s destination | not sent |

The resource carries `service.name` (`service_name`, else `OTEL_SERVICE_NAME`, else `unknown_service`), the replica identity metrics and spans carry (`service.instance.id`, `k8s.pod.name`), `OTEL_RESOURCE_ATTRIBUTES`, and `resource_attributes`, which win over the rest. An app that needs another mapping registers its own sender ([custom.md](custom.md)).

Each answer from the collector maps onto the result the caller acts on:

| Collector answer | `SendResult` |
| --- | --- |
| accepted | `Ok` |
| accepted with `rejected_log_records` above 0 | `Reject`, carrying the collector's message |
| HTTP 429, 502, 503, 504, gRPC `UNAVAILABLE` | `RetryAfter(wait)` from `Retry-After` in seconds or `RetryInfo`, else `Backpressured` |
| gRPC `RESOURCE_EXHAUSTED` | `RetryAfter(wait)` with `RetryInfo`, else `Reject`: without one it is most often a request over the collector's size limit |
| gRPC `CANCELLED`, `DEADLINE_EXCEEDED`, `ABORTED`, `OUT_OF_RANGE`, `DATA_LOSS`, or no answer (refused, reset, timed out) | `Backpressured` |
| HTTP 401, 403, 404, 405, 407, 410, 421, 1xx and 3xx, gRPC `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `UNIMPLEMENTED` | `Fatal(Config)`: the credential or endpoint is wrong, which says nothing about the records |
| over `http`, a TLS refusal: an untrusted or misnamed certificate, a key off the pin, or the client certificate turned away | `Fatal(Config)`, naming the rustls error |
| HTTP 413 | `Reject(MessageTooLarge)` |
| any other HTTP 4xx or 5xx, any other gRPC code | `Reject` |

- Waits: a `Retry-After` or `RetryInfo` wait is capped at `max_retry_delay_ms` (default 5 min), so a collector cannot hold the sender indefinitely. A zero wait is `Backpressured`, and so is a `Retry-After` given as an HTTP date, which is not read.
- A `Reject` names no record: a caller holding the records sends the block again one record at a time to find the one refused, which also gets a block over the collector's size limit through.
- Logs: the build at info, with the endpoint (user name and password left out), protocol, trust and header count. Each export at debug, a retry or `Reject` at warn, `Fatal` at error.
- Counters: `transport_sent_total` and `transport_sent_bytes_total` (payload bytes) for each record taken, `transport_backpressured_total` and `transport_send_errors_total` for each failed export, and `transport_send_duration_seconds`, all with `transport="otlp"`.
- `is_healthy()`: `false` after `close()`, which refuses further sends with `Fatal(Closed)`. It does not probe the collector.

Source: [../../src/transport/otlp/](../../src/transport/otlp/).
