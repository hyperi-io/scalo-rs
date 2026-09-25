# Backends

Six concrete backends behind the
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

The Vector-compat shim lives behind `transport-grpc-vector-compat` —
it isn't a separate backend, it's a wire-protocol overlay on the
gRPC server.

---

## Two deployment models (Kafka vs gRPC)

The picture below applies to the Kafka and gRPC backends — the other
four don't make a transit-network choice.

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

- **Cancellation safety**: `recv` polls on tokio's blocking pool, so a loop on it never holds a runtime worker. It is safe to drop at any `.await`, including its outage backoff: a poll still running when `recv` is dropped is kept, and the next `recv` returns its records.
- **Idle wait**: with nothing queued, `recv` waits up to 50 ms for a record, then returns an empty batch.
- **`send_batch()`**: queues the whole block, then awaits every delivery
  report, so the block costs about one `linger.ms` window. Outbound filters
  apply per record first. `Ok` means every record was confirmed or filtered;
  otherwise it is the first `Backpressured`/`Fatal` in record order and part
  of the block may be on the broker -- retry it whole (at-least-once).
  Records carry their `key` as the topic and no headers.
- **`is_healthy()`**: `false` after `close()` only; a broker outage does not flip it.
- **`commit()`**: synchronous (`CommitMode::Sync`) on tokio's blocking pool, so a broker rejection reaches the caller. `commit_weak_async()` is the fire-and-forget form.
- **Acknowledgements**: `acknowledgements.enabled` (default `true`) from `<key>.kafka.acknowledgements`, or `with_acknowledgements`. Once armed (`AckControl::arm`, which the `BatchEngine` pipeline builder calls), every offset `recv` hands out is held until released, and `commit` or `release` moves each partition only up to its lowest offset still held. An `Errored` release keeps its offsets held, so no later release commits past them: they are read again after a restart or rebalance. Unarmed, `commit` commits the highest offset per partition, as before. See [../pipeline/acknowledgements.md](../pipeline/acknowledgements.md).
- **Sink confirmation**: `confirms_delivery()` is `Remote`. `dead_letter_reason` names a record over `message.max.bytes` less 128 bytes of framing, and an outbound `dlq` filter match, so the pipeline dead-letters them itself instead of taking the `FilteredDlq` answer as handled.
- **Position lag**: `total_position_lag()` counts records past the consumer's read position. `total_consumer_lag` counts from the committed offset, so a commit held for delivery reads as backlog there and not here.

### Broker outages

librdkafka reconnects and rejoins by itself, so an outage ends neither the consumer nor the producer.

| Call | Broker unavailable | Returned as an error |
|------|--------------------|----------------------|
| `recv` | Empty batch; the next poll waits a jittered backoff, 100 ms doubling to 2 s | ACL failure, a missing topic the consumer may not create, bad config, a librdkafka fatal error, an unlisted code: `TransportError::Recv` |
| `send` / `send_batch` | `Backpressured` once the queue stays full 5 s or a record outlives `message.timeout.ms` (default 300 s) | ACL or permanent topic error: `Fatal`; oversize record: `FilteredDlq` |
| `commit` | Retried with the same backoff for up to 60 s, never after `close()` | Once that runs out, or when a newer group generation owns the partitions: `TransportError::Commit`, which the `BatchEngine` driver logs and carries on from |

An auth or TLS failure is permanent only until the credentials first work (a broker `UP`, or a record); after that it is a restarting broker. [classify.rs](../../src/transport/kafka/classify.rs) lists the transient codes; an unlisted code is permanent and logged by name. A missing topic is transient only with `allow.auto.create.topics: "true"`. A permanent error met mid-drain comes back on the next `recv`, after the drained records. The next `recv` clears the errors queued while nothing polled, and any rebalance behind them, backing off once only if no record follows. Counters: [../core-pillars/metrics.md](../core-pillars/metrics.md).

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
- **Send deadline**: `send_timeout_ms` (default 30 s, `0` for none) bounds each `send` and `send_batch` end to end, DNS, connect and TLS handshake included, and a send past it is `Backpressured`. A dial whose DNS lookup, TCP connect or TLS handshake has not finished by nine tenths of the limit is abandoned, so the send that started it reports the failure and the next send dials afresh.
- **Send failures**: `Unavailable`, `ResourceExhausted`, `DeadlineExceeded` (`send_timeout_ms`), `Cancelled` (a server cutting the RPC at its deadline), and a connection that fails before the server answers are `Backpressured`, so an absent, restarting or overrun receiver is waited out. Any other status the server returns is `Fatal`.
- **Dead connections**: the client sends an HTTP/2 PING once a connection has read nothing for `send_timeout_ms` (30 s when it is `0`), and closes the connection when the PING goes unanswered for as long again. A receiver that stays connected but stops answering is dropped that way, and the next send dials afresh.
- **Message-size ceiling**: `max_message_size` bounds the encoded request, measured uncompressed, as the receiver's decoder measures it. `send` returns `FilteredDlq` for a record over it without making the RPC, and for a record the receiver refuses with `OutOfRange` (its limit is lower, or gzip grew the frame past it). `send_batch` returns `Fatal` naming the limit for a block over it; send a smaller block.
- **Acknowledgement**: the server answers once the records are queued for `recv`, not once the consumer reads them. A full queue (`recv_buffer_size`) answers `ResourceExhausted`.
- **`RouteBatch`**: a batch is queued whole or not at all. One with more records than `recv_buffer_size` never fits the queue, so it is held whole in a slot of its own, one batch at a time: it lands in one step, or is answered `ResourceExhausted` with nothing queued while an earlier one is still waiting. `recv` hands that batch over before the queue, so the receiver holds at most `recv_buffer_size` records plus one such batch.
- **`close()`**: refuses new pushes with `Unavailable`, which senders retry, and keeps every acknowledged record: `recv` returns them, then `TransportError::Closed`. Open connections finish their in-flight RPCs on their own, the listener is free when `close()` returns, and a client that never finishes its RPC does not hold it open. Dropping the transport stops the server too.
- **Shutdown order**: `close()`, then `recv` until `Closed`, then flush. A service that stops calling `recv` before it returns `Closed` loses the records still queued, whenever it closes.
- **Counters**: receipts count in `transport_received_*`, never in `transport_sent_total`.
- **`is_healthy()`**: `false` after `close()`. Also sets the `transport_healthy{transport="grpc"}` gauge on every read.
- **`commit()`**: no-op — gRPC has no persistence to advance.

Source: [../../src/transport/grpc/](../../src/transport/grpc/).

### `transport-grpc-vector-compat`

Wire-compat shim for `vector.Vector/PushEvents`. Only used by
`dfe-transform-vector` so legacy Vector sinks can target a native gRPC
endpoint without recompile. Enable with `vector_compat: true` in the
gRPC config — the server then accepts both native and Vector RPCs on
the same listener. Not a separate backend, not for any other app.

A `PushEvents` request is queued whole or not at all: every event is converted first, then room for all of them is reserved in the receive queue, waiting while it is full. A receiver closed under the request refuses it with `Unavailable` (`receiver closed`), which Vector retries, and none of its events were queued. A request with more events than `recv_buffer_size` cannot be reserved at once, so it is queued one event at a time; if the receiver closes part-way, the events already queued arrive again when Vector retries the request.

A `PushEvents` goes through the same intake as a native push: refused with `Unavailable` while the pressure governor holds intake (`GrpcTransport::with_pressure`), and counted in `transport_received_events_total` and `transport_received_bytes_total` (the JSON queued for `recv`) once queued. A refusal because the receiver closed counts in `transport_refused_total`.

`VectorCompatClient` (the sending side) is plaintext only; an `https` endpoint fails on the first RPC. A dial whose DNS lookup or TCP connect is unfinished at nine tenths of the gRPC transport's default `send_timeout_ms` (30 s) is abandoned, so the call that started it returns an error and the next call dials afresh. `health_check`, a short probe, also gives up at the full 30 s. `send_events` has no limit once connected: a Vector source with end-to-end acknowledgements holds `PushEvents` open until its own sink has delivered, and cutting that off to retry would push the same events twice. The client sends an HTTP/2 PING once a connection has read nothing for 30 s and closes the connection when the PING goes unanswered for 30 s more, so a send to a peer that stays connected but stops answering returns an error, and the next call dials afresh. A peer that holds the RPC and still answers the PING is waited on.

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
- **`close()`**: refuses every `send` from then on, and keeps what `send` already accepted: `recv` returns it, then `TransportError::Closed`.
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

- **Cancellation safety**: `recv` keeps a partly read line, and any
  records it has not returned yet, in the transport, so a dropped call
  loses nothing and the next `recv` returns them. Offsets count every
  byte of a line read across dropped calls.
- **Line bytes**: passed through as read; a line need not be UTF-8.
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

- **Cancellation safety**: `recv` keeps a partly read line, and any
  records it has not returned yet, in the transport, so a dropped call
  or an expired `recv_timeout_ms` loses nothing and the next `recv`
  returns them.
- **Line bytes**: passed through as read; a line need not be UTF-8.
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
- **Acknowledgement**: the server answers 200 once the record is queued for `recv`, not once the consumer reads it. A full queue (`recv_buffer_size`), a held inbound gate, and a closed receiver answer 503 with `Retry-After: 1`.
- **`close()`**: answers new POSTs with 503, which senders retry, and keeps every acknowledged record: `recv` returns them, then `TransportError::Closed`. Open connections finish their in-flight requests on their own, the listener is free when `close()` returns, and a client that never finishes its request does not hold it open. Dropping the transport stops the server too.
- **Shutdown order**: `close()`, then `recv` until `Closed`, then flush. The `BatchEngine` run loops do this at shutdown ([../pipeline/batch-engine.md](../pipeline/batch-engine.md#shutdown)). Flushing first loses the records still queued.
- **Counters**: receipts count in `transport_received_*`, never in `transport_sent_total`.
- **`is_healthy()`**: `!closed`. Does not probe the endpoint.

Source: [../../src/transport/http.rs](../../src/transport/http.rs).

---

## Filter wiring

Every backend reads `filters_in` and `filters_out` from its own
config section and instantiates a [`TransportFilterEngine`](filter-engine.md)
at construction. No backend-specific filter code — the engine is the
same across all six. Tier-1 filters cost ~50-100 ns when present
and zero when absent.

---

## Related

- [README.md](README.md) — traits, factory, enum dispatch
- [filter-engine.md](filter-engine.md) — embedded filtering
- [routing.md](routing.md) — per-key dispatch over multiple backends
- [../feature-flags.md](../feature-flags.md) — feature-to-dep table
- [../integration.md](../integration.md) — ServiceApp recipe
- [../pipeline/dlq.md](../pipeline/dlq.md) — DLQ sink backends
