# Backends

Seven concrete backends behind the
[transport traits](README.md). Each is gated behind its own
feature flag — apps pull only what they ship.

| Backend | Feature flag | Native dep | Use case |
|---------|--------------|------------|----------|
| Kafka | `transport-kafka` | `librdkafka1` (runtime), `librdkafka-dev` (build) | Production default, persistence, replay |
| gRPC | `transport-grpc` | None (pure Rust — `tonic`) | Inter-service mesh, low latency |
| Memory | `transport-memory` | None | Unit tests, same-process pipelines |
| File | `transport-file` | None | Debugging, audit trails, replay |
| Pipe | `transport-pipe` | None | Unix pipeline composition |
| HTTP | `transport-http` | None | Webhook delivery, REST ingest |
| Redis | `transport-redis` | None (uses `redis` crate) | Edge deployments, lightweight pub/sub |

The Vector-compat shim lives behind `transport-grpc-vector-compat` —
it isn't a separate backend, it's a wire-protocol overlay on the
gRPC server.

---

## Two deployment models (Kafka vs gRPC)

The picture below applies to the Kafka and gRPC backends — the other
five don't make a transit-network choice.

| Model | Persistence | Replay | Latency | Failure mode | Use when |
|-------|-------------|--------|---------|--------------|----------|
| **Kafka-mediated** | Yes (broker disk) | Yes | ~ms | Producer keeps writing if consumer down | Default for staged pipelines, audit-trail required, consumer-failure tolerance matters |
| **Direct gRPC** | No | No | ~µs | Sender fails fast if receiver down | Tight data-plane mesh, latency-sensitive, broker overhead unacceptable |

Apps pick per-stage. A typical data-plane deployment runs
`receiver → Kafka → loader` (durability at ingress) and
`loader → gRPC → archiver` (latency on the sink) — same binary set,
config-only difference.

---

## Kafka

`rdkafka` with dynamic linking against system librdkafka — see
[../feature-flags.md](../feature-flags.md) for the package matrix.
Profile-based config (`production`, `devtest`) with
`librdkafka_overrides` for fine control. Supports auto-discovery
(`auto_discover: true` with include/exclude regex), SASL/SSL,
suppression rules (`_load` masks `_land` by convention).

```yaml
transport:
  output:
    type: kafka
    kafka:
      profile: production
      brokers: ["kafka-0:9092", "kafka-1:9092"]
      group: dfe-loader
      topics: ["events.land"]
      security_protocol: sasl_ssl
      sasl_mechanism: SCRAM-SHA-512
      sasl_username: dfe
      sasl_password: ${KAFKA_PASSWORD}
```

- **Cancellation safety**: `recv` uses `rdkafka`'s internal poll —
  safe to drop at any `.await`, including during its outage backoff.
- **`send_batch()`**: queues the whole block, then awaits every delivery
  report, so the block costs about one `linger.ms` window. Outbound filters
  apply per record first. `Ok` means every record was confirmed or filtered;
  otherwise it is the first `Backpressured`/`Fatal` in record order and part
  of the block may be on the broker -- retry it whole (at-least-once).
  Records carry their `key` as the topic and no headers.
- **`is_healthy()`**: `false` after `close()` only; a broker outage does not flip it.
- **`commit()`**: synchronous (`CommitMode::Sync`) on tokio's blocking pool, so a broker rejection reaches the caller. `commit_weak_async()` is the fire-and-forget form.

### Broker outages

librdkafka reconnects and rejoins by itself, so an outage ends neither the consumer nor the producer.

| Call | Broker unavailable | Returned as an error |
|------|--------------------|----------------------|
| `recv` | Empty batch; the next poll waits a jittered backoff, 100 ms doubling to 2 s | ACL failure, a missing topic the consumer may not create, bad config, a librdkafka fatal error, an unlisted code: `TransportError::Recv` |
| `send` / `send_batch` | `Backpressured` once the queue stays full 5 s or a record outlives `message.timeout.ms` (default 300 s) | ACL or permanent topic error: `Fatal`; oversize record: `FilteredDlq` |
| `commit` | Retried with the same backoff for up to 60 s, never after `close()` | Once that runs out, or when a newer group generation owns the partitions: `TransportError::Commit`, which the `BatchEngine` driver logs and carries on from |

An auth or TLS failure is permanent only until the credentials first work (a broker `UP`, or a record); after that it is a restarting broker. [classify.rs](../../src/transport/kafka/classify.rs) lists the transient codes; an unlisted code is permanent and logged by name. A missing topic is transient only with `allow.auto.create.topics: "true"`. A permanent error met mid-drain comes back on the next `recv`, after the drained records. Counters: [../core-pillars/metrics.md](../core-pillars/metrics.md).

Source: [../../src/transport/kafka/](../../src/transport/kafka/).

---

## gRPC

`tonic`, pure Rust. Each gRPC backend can be client-only
(`endpoint` set), server-only (`listen` set), or both. Default
recv-buffer 10k, max-message 16 MB, gzip optional. The
[transport filter engine](filter-engine.md) is wired in the same as
every other backend.

```yaml
transport:
  output:
    type: grpc
    grpc:
      endpoint: "http://dfe-loader:6000"
      max_message_size: 16777216
      compression: false
```

- **Cancellation safety**: `recv` reads from an internal mpsc, safe
  to drop. `send` is a single unary RPC — drop cancels cleanly.
- **Send failures**: `Unavailable`, `ResourceExhausted`, `DeadlineExceeded` (`send_timeout_ms`), and a connection that fails before the server answers are `Backpressured`, so an absent or restarting receiver is waited out. Any other status the server returns is `Fatal`.
- **`is_healthy()`**: `AtomicBool`, also emits `dfe_transport_healthy{transport="grpc"}`
  gauge on every read.
- **`commit()`**: no-op — gRPC has no persistence to advance.

Source: [../../src/transport/grpc/](../../src/transport/grpc/).

### `transport-grpc-vector-compat`

Wire-compat shim for `vector.Vector/PushEvents`. Only used by
`dfe-transform-vector` so legacy Vector sinks can target a native gRPC
endpoint without recompile. Enable with `vector_compat: true` in the
gRPC config — the server then accepts both native and Vector RPCs on
the same listener. Not a separate backend, not for any other app.

Source: [../../src/transport/vector_compat/](../../src/transport/vector_compat/).

---

## Memory

`tokio::sync::mpsc` bounded channel. Same-process only — sender and
receiver are tied to the same `MemoryTransport` instance. **Not a
deployable backend** — for tests and in-process pipelines (e.g.
unit tests against the `BatchEngine`).

```yaml
transport:
  output:
    type: memory
    memory:
      buffer_size: 1000
      recv_timeout_ms: 100
```

- **Cancellation safety**: `recv` is a `select!` on `recv_timeout`
  and channel `recv` — safe to drop.
- **`is_healthy()`**: `!closed` — atomic flag flipped by `close()`.
- **`commit()`**: advances an internal `AtomicU64` sequence.

Source: [../../src/transport/memory/](../../src/transport/memory/).

---

## File

NDJSON file I/O. Each `send()` appends one newline-delimited line.
Read side tracks a byte offset and persists it to a `.pos` sidecar
file so reads survive restarts. `FileToken` carries the byte offset;
`commit()` writes the highest committed offset to disk.

```yaml
transport:
  output:
    type: file
    file:
      path: "/var/log/dfe/events.ndjson"
      append: true
```

- **Cancellation safety**: read/write are guarded by a `tokio::Mutex`
  — cancellation drops the lock cleanly.
- **`is_healthy()`**: `!closed` atomic flag.

Source: [../../src/transport/file.rs](../../src/transport/file.rs).

---

## Pipe

Reads from stdin, writes to stdout. Newline-delimited, one line per
message. The `destination` arg to `send()` is ignored — there's only one
stdout. `PipeToken` is a monotonic sequence number; `commit()` is a
no-op because stdin is forward-only.

```yaml
transport:
  output:
    type: pipe
    pipe:
      recv_timeout_ms: 100
```

- **Cancellation safety**: read path uses `tokio::io::BufReader::read_line`
  — drop-safe.
- **`is_healthy()`**: `!closed`.

Source: [../../src/transport/pipe.rs](../../src/transport/pipe.rs).

---

## HTTP

Two halves, independent: `endpoint` enables send (POST to URL),
`listen` enables receive (embedded axum on `recv_path`, default
`/ingest`). The receive side requires the `http-server` feature
(transitively for axum). Bounded recv-buffer with backpressure.

```yaml
transport:
  output:
    type: http
    http:
      endpoint: "http://collector:8080/ingest"
      # OR for receive:
      listen: "0.0.0.0:8080"
      recv_path: "/ingest"
      recv_buffer_size: 10000
```

- **Cancellation safety**: send is `reqwest`'s async path — drop
  cancels the in-flight request. Receive drains from an internal
  mpsc, drop-safe.
- **Send failures**: a refused, reset or timed-out connection, and HTTP 408, 429, 502, 503 or 504, are `Backpressured`, so a down endpoint is waited out. Any other non-2xx status, and a request that cannot be built, is `Fatal`.
- **`is_healthy()`**: `!closed`. Does not probe the endpoint.

Source: [../../src/transport/http.rs](../../src/transport/http.rs).

---

## Redis

Redis/Valkey Streams via the `redis` crate. Producer writes via
`XADD`, consumer uses `XREADGROUP` with consumer-group semantics.
`commit()` issues `XACK`. Supports `redis://`, `rediss://` (TLS),
and `unix://`. `max_stream_len` enables approximate trimming via
`MAXLEN ~`.

```yaml
transport:
  output:
    type: redis
    redis:
      url: "redis://valkey:6379"
      stream: "events.land"
      group: "dfe"
      consumer: "dfe-loader-1"
      max_stream_len: 100000
      block_ms: 5000
```

- **Cancellation safety**: the `XREADGROUP` block is a single async
  call — cancelling drops the connection back to the pool.
- **`is_healthy()`**: `!closed`.
- **Outages**: a dropped, refused or timed-out connection makes `send` return `Backpressured`, while `recv` and `commit` return an error. The connection is not re-established: after Redis restarts, `send` stays `Backpressured` and `recv` keeps failing until the transport is rebuilt.
- **`commit()`**: `XACK` on the configured stream/group.

Source: [../../src/transport/redis_transport.rs](../../src/transport/redis_transport.rs).

---

## Filter wiring

Every backend reads `filters_in` and `filters_out` from its own
config section and instantiates a [`TransportFilterEngine`](filter-engine.md)
at construction. No backend-specific filter code — the engine is the
same across all seven. Tier-1 filters cost ~50-100 ns when present
and zero when absent.

---

## Related

- [README.md](README.md) — traits, factory, enum dispatch
- [filter-engine.md](filter-engine.md) — embedded filtering
- [routing.md](routing.md) — per-key dispatch over multiple backends
- [../feature-flags.md](../feature-flags.md) — feature-to-dep table
- [../integration.md](../integration.md) — ServiceApp recipe
- [../pipeline/dlq.md](../pipeline/dlq.md) — DLQ sink backends
