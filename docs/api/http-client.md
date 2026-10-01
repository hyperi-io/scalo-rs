# HTTP client

`HttpClient` is the wrapping `reqwest::Client` you should use for every
outbound HTTP call from a service. It wires exponential backoff with
jitter (the `backon` crate) around every request, owns a connection
pool, emits the request metrics, and reads its config from the cascade.

Use this rather than rolling a `reqwest::Client` per call site -- the
retry policy, the `Retry-After` handling and the metrics all come with
it.

---

## Usage

```rust
use scalo::http_client::HttpClient;

let client = HttpClient::from_cascade()?;

let resp = client.get("https://api.example/v1/things").await?;
let body: ThingList = resp.json().await?;

// JSON helpers handle serialise + content-type:
let created: Thing = client.post_json("https://api.example/v1/things", &payload).await?.json().await?;
```

`from_cascade()` reads the `http_client.*` config section and is the
canonical way to build the client. Pass an explicit `HttpClientConfig`
only when you need a per-call-site variant (a longer timeout for a large
transfer, say).

---

## What the retry loop gives you

- **Retry** with exponential backoff and jitter, on the transient set
  only: 408, 429, 500, 502, 503, 504, plus connect and timeout failures.
  Any other 4xx returns immediately.
- **Idempotent methods only** by default. GET, HEAD, PUT, DELETE and
  OPTIONS are replayable; POST and PATCH need
  `retry_non_idempotent: true`, which is for endpoints that dedupe. A
  request the signer could not sign never left, so a transient signing
  failure is retried whatever the method.
- **`Retry-After` honoured** in preference to the exponential schedule,
  capped at `max_retry_interval_ms`. The cap matters: a throttled
  downstream can legally advertise hours, and taking that whole parks
  the request inside the retry loop. It paces the attempts `max_retries`
  already grants and never adds one, so a downstream answering 429 plus
  the header on every attempt still ends the loop.
- **The last response returned** once retries are exhausted, even a 5xx,
  so the caller can read the status and body rather than getting a
  transport error.
- **Request metrics** per method: `http_client_requests_total`,
  `http_client_duration_seconds`, `http_client_retries_total`.
- **No request URL on an error.** reqwest keeps a copy of the URL and
  renders it, and a credential placed in the query is inside that URL,
  so the loop drops it from every error it returns.

---

## Signing a request

`RequestSigner` is the hook that puts a credential on a request. It runs
on the built `reqwest::Request` -- after the body and query are final,
before the request is sent -- and it runs again on every attempt.

That ordering is the point. A signature that covers the body or the
query can only be computed once they are final, and a per-request nonce,
a timestamp or a token that expired between attempts has to be
regenerated rather than replayed.

```rust
use reqwest::header::HeaderValue;
use scalo::http_client::{RequestSigner, SignError};

struct StampSigner;

impl RequestSigner for StampSigner {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| SignError::with_cause("clock is before the epoch", e))?;
        let value = HeaderValue::from_str(&stamp.as_secs().to_string())
            .map_err(|e| SignError::with_cause("stamp is not a header value", e))?;
        request.headers_mut().insert("x-stamp", value);
        Ok(())
    }
}

let resp = client.get_signed("https://api.example/v1/things", &StampSigner).await?;
```

Signers are passed per call and taken by generic, not stored on the
client: `async fn` in a trait is not object safe, so there is no `dyn`
form, and the client is shared by every call site in a service. A list
of placements is a tuple -- `(A, B)`, `(A, B, C)` and `(A, B, C, D)` are
signers themselves, applied left to right.

The unsigned methods are the same loop with `Unsigned` in place of a
signer, so there is one retry path and not two.

For the credential itself -- a token exchange, a metadata server, a
static key, and where to put it -- see [auth.md](auth.md). A placement
from there reports an acquisition failure as `SignError::Auth` with the
`AuthError` whole, so the status of a refusal is there to match on. A
signing scheme with its own crypto dependencies (SigV4, a request HMAC)
belongs in the consumer as one more implementation of this trait, and
reports through `SignError::Failed` -- `retryable()` when another
attempt could get past it.

---

## Config shape

```yaml
http_client:
  timeout_secs: 30
  connect_timeout_secs: 10
  max_retries: 3
  min_retry_interval_ms: 100
  max_retry_interval_ms: 30000
  retry_non_idempotent: false
  user_agent: "my-app/1.0"
```

`max_retries: 0` disables retries. `user_agent` unset leaves reqwest's
own default.

Redirects are not in the config: they are a property of what the client
is for, so they are set where it is built.
`HttpClient::with_redirect_policy(config, policy)` takes any
`reqwest::redirect::Policy`; `new(config)` is that with reqwest's
default. reqwest strips `Authorization` on a cross-origin hop but carries
the body, the query and any custom header across it, so a client whose
calls are signed with a provider's own header -- or with a query
parameter -- should refuse the hop (`Policy::none()`) or allow only the
same origin. The token exchanges in [auth.md](auth.md) build themselves
such a client; a client that only downloads keeps the default.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `HttpClient::new(config)` | Build from explicit config |
| `HttpClient::with_redirect_policy(config, policy)` | Build from explicit config with a `reqwest::redirect::Policy` of the caller's choosing |
| `HttpClient::from_cascade()` | Build from the `http_client` config section |
| `.get(url)` | GET request |
| `.get_with(url, f)` | GET with the builder decorated by `f` (headers, query) -- keeps retry and metrics, unlike `.client()` |
| `.get_signed(url, signer)` | GET with `signer` putting the credential on each attempt |
| `.send_signed(method, url, body, f, signer)` | Any method, raw body, decorated by `f`, signed per attempt; retries follow the method |
| `.post_json(url, &body)` | POST with JSON body and content-type |
| `.put_json(url, &body)` | PUT with JSON body |
| `.delete(url)` | DELETE request |
| `.client() -> &reqwest::Client` | The underlying client, for requests the helpers don't cover -- no retry, no metrics |
| `.config() -> &HttpClientConfig` | Read back the effective config |

---

## When to use which

| Need | Use |
| ------ | ----- |
| Outbound HTTP from a service | `HttpClient` -- always |
| A credential on the request | `.get_signed` / `.send_signed` with a placement from [auth.md](auth.md) |
| A signature over the body or query | A `RequestSigner`; the hook runs after the request is built |
| Streaming download | `.get(...)` then `response.chunk()` in a loop -- the body streams, the retry still applies to the request |
| Webhook receiver | Different concern -- that's [HTTP-SERVER](http-server.md) |
| gRPC | Different concern -- see [../transport/backends.md](../transport/backends.md) |

---

## Related

- [auth.md](auth.md) -- credential acquisition and the placements that sign with it
- [http-server.md](http-server.md) -- sibling for inbound HTTP
- [../core-pillars/metrics.md](../core-pillars/metrics.md) -- the request metrics
- [../feature-flags.md](../feature-flags.md) -- `http`
- Source: [../../src/http_client/](../../src/http_client/)
