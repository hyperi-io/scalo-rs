# HTTP -- sending

The HTTP sender POSTs records to `endpoint`, and to `failover` endpoints in order when it can't. Every key is opt-in. The defaults post one record per request as `application/octet-stream`, refuse redirects and use no proxy. The receive side and the shared keys are in [backends.md](backends.md#http).

```yaml
transport:
  output:
    type: http
    http:
      endpoint: "https://intake.example/v1/ingest"
      failover:
        - url: "https://intake-dr.example/v1/ingest"
          auth: { type: bearer, token: "${DR_TOKEN}" }   # unset uses `auth` below
      auth: { type: basic, username: svc, password: "${INTAKE_PASSWORD}" }
      headers: { X-Tenant: acme }
      user_agent: "my-app/1.4"
      framing: { type: json_wrapped, key: events }        # default single
      compression: {codec: gzip}                          # gzip or zstd, default none
      success_status: [200, 201, 204]                     # default any 2xx
      proxy: system                                       # none (default), system or a URL
      follow_redirects: false                             # default false
      max_retry_after_ms: 60000
      send_timeout_ms: 30000
```

## Credentials and headers

- `auth` is `none`, `basic` (`username`, `password`), `bearer` (`token`) or `api_key` (`header`, `key`). A `failover` entry with its own `auth` sends that one instead.
- `user:password@` in an endpoint URL is basic auth, and an endpoint with both that and `auth` is refused at build.
- `headers` go on every request. Their values are secrets, like passwords and tokens: they serialise as `***REDACTED***`, are marked sensitive on the request, and never appear in `Debug`, an error or a log. A header the sender sets itself (`Content-Type`, `Content-Encoding`, the `auth` header) wins over one of the same name here. Env vars can't name a header with a `-` in it, so set `headers` in a file.
- `user_agent` unset sends no `User-Agent`.
- Logs, errors and `Debug` show each URL without its user name and password.

## Body shape

`framing` is any [framing](framing.md) by name: `ndjson`, `json_array`, `json_wrapped` (records under `key`), `json_concat`, `single`, or one an app registered with `register_framing`. The body's `Content-Type` is the framing's. A block becomes one body per run of records with the same key, posted to `{endpoint}/{key}`. `single` posts each record as its own request.

An app's own shape is a `Framing` registered by name at startup, then named in config. The [framing guide](framing.md) has a worked example, and this one posts records as a JSON-lines array under a `data` key that also carries a count:

```rust
use std::sync::Arc;
use scalo::framing::{Framing, register_framing};

#[derive(Debug)]
struct Counted;

impl Framing for Counted {
    fn content_type(&self) -> &'static str {
        "application/json"
    }
    fn appends(&self) -> bool {
        false
    }
    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        out.extend_from_slice(format!("{{\"count\":{},\"data\":[", records.len()).as_bytes());
        for (i, record) in records.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(record);
        }
        out.extend_from_slice(b"]}");
        records.len()
    }
}

register_framing("counted", Arc::new(Counted))?;
// transport.output.http.framing: { type: counted }
```

`compression` compresses each body and sends the codec as `Content-Encoding`: `gzip` (level 6 unless `level` is set) or `zstd` (level 3). The receiver must decode it, as scalo's own HTTP receive side does ([backends.md](backends.md#http)). LZ4 and snappy have no HTTP content coding, so they are refused at build. See [../compression.md](../compression.md).

## Answers, failover and the deadline

| Status | Answer | The caller |
| --- | --- | --- |
| one in `success_status`, or any 2xx when it is empty | `Ok` | releases the records |
| 400, 413, 422 | `Reject` (413 as `MessageTooLarge`) | dead-letters them, never resends |
| 408, 429, 5xx, a 2xx not in `success_status` | `RetryAfter(wait)` with a `Retry-After` in seconds, else `Backpressured` | sends them again |
| any other 3xx or 4xx | `Fatal(Config)` | keeps them until the endpoint or its credential is fixed |

- A refused, reset or timed-out connection is `Backpressured`. A request that can't be built (a key that makes a bad URL) is `Reject`.
- A failed TLS handshake (an untrusted or misnamed certificate, a key off `tls.pin_sha256`, a client certificate the endpoint turns away, no version, suite or group in common) is not retried. When no other endpoint takes the body or is worth a retry, the answer is `Fatal(Config)` naming the `TlsFailure`.
- `Retry-After` is taken in its delta-seconds form, capped at `max_retry_after_ms`. A date, or a `max_retry_after_ms` of 0, leaves the caller's own schedule in charge.
- A listed 202 counts as stored. Left out of the list, it is sent again: accepted is not stored.

Each body goes to `endpoint`, then each `failover` endpoint in order, starting from `endpoint` on every send. The first `Ok` or `Reject` is the answer. When every endpoint fails, a busy or unreachable one anywhere makes the answer a retry, with the longest `Retry-After` any gave, and otherwise it is `Fatal(Config)` naming each endpoint and its status. The endpoints are one destination, so a receiver that drops repeats by record id sees the same ids from each.

`send_batch_by(records, deadline)` answers by `deadline`. Each request's timeout is an equal share of the time left between the endpoints still to try, so a hung first endpoint leaves the next one its turn. `send` and `send_batch` give each body `send_timeout_ms` of its own instead. A deadline that passes is `Backpressured`: the records may have arrived, and are sent again.

## Proxy, redirects and logs

- `proxy: none`, the default, connects directly whatever `HTTP_PROXY` says. `system` takes `HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY` and `NO_PROXY` from the environment, then fills what they leave unset from the machine WinHTTP proxy on Windows (`netsh winhttp set proxy`) or the System Settings proxies on macOS. A URL sends every request through that proxy, and `user:password@` in it is basic proxy auth, never logged. Only an `http://` proxy is taken. An https endpoint goes through a CONNECT tunnel. A CONNECT the proxy refuses is `Backpressured`, so the batch is held and sent again: the proxy's policy is the operator's to fix and the records are fine, and many proxies allow CONNECT to port 443 only. Each refusal adds to `transport_proxy_refused_total`, the first of a run is logged with the proxy's status and the endpoint's port, and `is_healthy()` and the `transport:http` health check report not-ready until a send goes through. The gRPC transport takes the same `proxy` key.
- Redirects are refused: a 3xx is `Fatal(Config)`, and the `Location` is never requested. `follow_redirects: true` follows up to 5 redirects within the endpoint's own scheme, host and port, so a credential never goes to a host the config didn't name. A 307 or 308 resends the body. A 301, 302 or 303 turns the POST into a body-less GET, which an intake answers with an error.

- `info` once at build: the endpoints, the `auth` kind, the header names, framing, compression, proxy kind and redirect setting.
- `warn` per endpoint that fails a body, `error` when no endpoint takes it for a reason a retry won't fix, `debug` per body delivered, `trace` per record framed.
- `transport_sent_total` counts records, `transport_sent_bytes_total` bytes on the wire, `transport_backpressured_total` and `transport_send_errors_total` endpoint answers, and `transport_send_duration_seconds` each request.

Source: [../../src/transport/http/send.rs](../../src/transport/http/send.rs).
