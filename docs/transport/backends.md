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
      group: my-app
      topics: ["events.land"]
      security_protocol: sasl_ssl
      sasl_mechanism: SCRAM-SHA-512
      sasl_username: myapp
      sasl_password: ${KAFKA_PASSWORD}
```

- **Security floor**: every constructor that builds a client from a `KafkaConfig` applies `provider`, then runs `KafkaConfig::validate`: `KafkaTransport::new`, `KafkaProducer::new` and its profile shorthands, `KafkaAdmin::new`, `TopicResolver::new` and the Kafka DLQ backend. In any environment it refuses an unknown `provider`, and a `PLAIN` SASL mechanism unless the transport is `sasl_ssl`. Where `APP_ENV` (else `ENVIRONMENT`, else `ENV`) is `production` or `prod`, it also refuses `ssl_skip_verify: true`, certificate verification turned off or `ssl.endpoint.identification.algorithm: none`, and `plaintext` or `sasl_plaintext` without `allow_insecure_transport: true`. In production it also refuses a raw map's `sasl.oauthbearer.token.endpoint.url` with the `http` scheme, which would send the OAUTHBEARER client secret in cleartext, and a raw map's `ssl.cipher.suites` that names a suite with no encryption. That is a token, split on `:`, `,`, `;` or whitespace, with a `+`, `-` or `_` separated part that is `NULL` or `eNULL` (`eNULL`, `NULL-SHA`, `ECDHE-RSA-NULL-SHA`), unless a leading `!` or `-` excludes it. The cipher rule reads suite names, not OpenSSL's full cipher-string semantics, so an alias such as `COMPLEMENTOFALL` that reaches a NULL suite indirectly passes. Neither key has a typed field. Each rule judges the typed field and every value `librdkafka_overrides`, `sizing.producer_librdkafka`, `sizing.consumer_librdkafka` or `extra_config` gives `security.protocol`, `sasl.mechanism` (or `sasl.mechanisms`), `enable.ssl.certificate.verification` or `ssl.endpoint.identification.algorithm`, keys and values in any case, so no raw map takes a client below the floor. A refusal a raw map caused names the map and the key. The consumer runs the typed `security_protocol` and `sasl_mechanism` over those maps, and the producer and admin clients run the maps' values, so an override to `ssl` does not lift a typed `plaintext` either. The refusal is `TransportError::Config`, or `DlqError::Kafka` from `Dlq::spawn`. `producer_client_config` does neither step, so an app building its own client from it calls `apply_provider` and `validate` first.
- **Cancellation safety**: `recv` polls on tokio's blocking pool, so a loop on it never holds a runtime worker. It is safe to drop at any `.await`, including its outage backoff and the wait after a repeated consumer rebuild: a poll still running when `recv` is dropped is kept, and the next `recv` returns its records, or an empty batch when the client it polled has been rebuilt since and the poll failed.
- **Idle wait**: with nothing queued, `recv` waits up to 50 ms for a record, then returns an empty batch.
- **`send_batch()`**: queues the whole block, then awaits every delivery
  report, so the block costs about one `linger.ms` window. Outbound filters
  apply per record first. `Ok` means every record was confirmed or filtered;
  otherwise it is the first `Backpressured`/`Fatal` in record order and part
  of the block may be on the broker -- retry it whole (at-least-once).
  Records carry their `key` as the topic and no headers.
- **`is_healthy()`**: `false` after `close()`; for a fenced static member; and from a failed consumer rebuild, or a second one with no record between, until a record arrives. A broker outage or a receive error does not flip it.
- **Group protocol**: `classic` unless `consumer_protocol: consumer` opts in to KIP-848; `group_protocol()` reports the one in use. A consumer librdkafka flags fatal is rebuilt by the next `recv`, as `classic` when the broker refused KIP-848, except a fenced static member, which is not. See [../kafka-path.md](../kafka-path.md).
- **`commit()`**: synchronous (`CommitMode::Sync`) on tokio's blocking pool, so a broker rejection reaches the caller. `commit_weak_async()` is the fire-and-forget form.
- **Acknowledgements**: `acknowledgements.enabled` (default `true`) from `<key>.kafka.acknowledgements`, or `with_acknowledgements`. Once armed (`AckControl::arm`, which the `BatchEngine` pipeline builder calls), every offset `recv` hands out is held until released, and `commit` or `release` moves each partition only up to its lowest offset still held. An `Errored` release keeps its offsets held, so no later release commits past them: they are read again after a restart or rebalance. `transport_ack_withheld` counts them, so a partition pinned that way can alert. A partition a rebalance revokes is its next owner's: its held offsets go, nothing of it is held or committed until an assignment gives it back, and an offset handed out before the revoke commits nothing when released, even after the partition comes back. In a hand-rolled or re-run loop, that partition's commit can stall until its next revoke when the stale copy is released `Errored`, or when the copy read again is released first. That costs time and backlog, not data, and a restart reads the partition again. Unarmed, `commit` commits the highest offset per partition, as before. See [../pipeline/acknowledgements.md](../pipeline/acknowledgements.md).
- **Revokes and partition leases**: a partition a rebalance revokes is its next owner's, armed or not, and the next owner reads it again from the committed offset. `recv` never hands out a record read before a revoke of its partition in the same poll, and counts each one in `transport_revoke_discarded_total{transport="kafka",stage="receive"}`. A caller that holds records before writing them, a buffer that fills for seconds, takes each record's lease when `recv` returns it and before the next `recv`: `lease(topic, partition)`, one call per partition per batch. Right before the write, `holds(topic, partition, lease)` says whether the lease still stands. A revoke ends it, and so does a consumer rebuild, since the new client reads every partition again too. A partition given back, as an eager rebalance does at once, is under a new lease, so the copy read before the revoke and the copy read again never match, where asking whether the consumer holds the partition would keep both. A record whose lease has ended is discarded and counted with `discarded_after_revoke(n)` (`stage="buffer"`). Unarmed, its token stays out of `commit`, which commits the highest offset it is given and so could move the commit past a record nobody wrote. Armed, its token is released like that of any record the caller drops: an offset handed out before its partition's revoke commits nothing, and the release keeps the copy read again from stalling the partition's commit. `holds` reads the live assignment, so it covers a revoke served by a poll whose `recv` a `select!` arm dropped. The next owner reads a partition nothing has committed to yet from where `auto.offset.reset` points: the log start under `earliest`, the default. Under `latest` it starts at the log end, so a record of such a partition, left out by `recv` or discarded by a caller, is read by nobody. `lease`, `holds`, `discarded_after_revoke` and `PartitionLease` are public API.
- **Sink confirmation**: `confirms_delivery()` is `Remote`. `dead_letter_reason` names a record over `message.max.bytes` less 128 bytes of framing, and an outbound `dlq` filter match, so the pipeline dead-letters them itself instead of taking the `FilteredDlq` answer as handled.
- **Position lag**: `total_position_lag()` counts records past the consumer's read position. `total_consumer_lag` counts from the committed offset, so a commit held for delivery reads as backlog there and not here. A partition with nothing committed counts from the read position in both: the application's, else librdkafka's fetch position. Both count only the partitions the consumer holds, and measure to the log end, which librdkafka learns only from fetches. That end is the one librdkafka's own `consumer_lag` measures to, committed partition or not: the last stable offset under `read_committed`, librdkafka's default, which a transaction open upstream holds back, and the high watermark under `read_uncommitted`. The transport reads `isolation.level` from the consumer's own config, `librdkafka_overrides` included. A `StatsContext` built with `new()` counts as `read_committed`. While the inbound gate holds the assignment paused, the transport asks the broker for the end once per statistics interval (at least 1 s apart), so both keep rising ([../backpressure.md](../backpressure.md)).
- **Client statistics metrics**: each statistics callback publishes the client's `rdkafka_global_msg_cnt` and `rdkafka_global_msg_size_bytes` (its producer queue), `rdkafka_broker_rtt_avg_seconds`, `rdkafka_broker_outbuf_cnt` and `rdkafka_broker_waitresp_cnt` per `broker`, the two per-partition series below, and `rdkafka_consumer_rebalance_count` once its group has rebalanced. Every `rdkafka_` series carries `client_id`, the configured `client.id`, and `client_type`, `consumer` or `producer`, both from librdkafka's statistics. A transport's consumer and producer share its `client_id` and still keep separate series, as do transports with different `client_id`s. Two transports in one process with the same `client_id` write the same series, so give each its own. Both labels hold across a consumer rebuild and a restart, where librdkafka's handle name counts every client the process creates.
- **Lag and assignment metrics**: each statistics callback publishes `rdkafka_topic_partition_consumer_lag{client_id,client_type,topic,partition}` and `consumer_lag{group_id,topic,partition}` for every partition the consumer holds, committed or not, and sets `consumer_partitions_assigned{group_id}` to how many it holds. It publishes `rdkafka_topic_partition_committed_offset{client_id,client_type,topic,partition}` for every held partition with a commit. Each revoke and each assignment adds 1 to `consumer_rebalance_total{group_id}`, as librdkafka's own `rebalance_cnt` counts them. `group_id` is the `group.id` the consumer joins, `librdkafka_overrides` included, and holds across a consumer rebuild and a restart. Consumers in one process in different groups each keep their own series, and a revoke zeroes only its own group's `consumer_lag`. Two transports in one process in the same group write one `consumer_partitions_assigned`, so the last write wins. A `StatsContext` built with `new()` knows no group and writes the three without `group_id`, as `ConsumerMetrics`' own setters do. A partition's lag appears once librdkafka knows its end, from the first fetch reply or, while paused, the end asked of the broker. A partition a rebalance revokes has its lag, `consumer_lag` and committed offset set to 0 at the revoke, and leaves `partition_committed` and `partition_high_watermark` in `stats()`. librdkafka keeps reporting a revoked partition's last committed offset and high watermark, and the transport drops them. The transport's producer and a producer-only transport's consumer never rebalance, so they leave `consumer_partitions_assigned` and `consumer_lag` alone. A `StatsContext` on a consumer of your own that takes partitions by `assign()` serves no rebalance either, and keeps the commit-only rule: lag for every partition with a committed offset, position lag from the application's or committed position, offsets for every partition librdkafka reports, and no `consumer_lag`.

### Broker outages

librdkafka reconnects and rejoins by itself, so an outage ends neither the consumer nor the producer.

| Call | Broker unavailable | Returned as an error |
|------|--------------------|----------------------|
| `recv` | Empty batch; the next poll waits a jittered backoff, 100 ms doubling to 2 s | ACL failure, a missing topic the consumer may not create, bad config, an unlisted code: `TransportError::Recv`, readiness untouched. A librdkafka fatal error rebuilds the consumer and returns an empty batch; a fenced static member returns `TransportError::Recv` and fails readiness |
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
      endpoint: "http://my-app:6000"
      max_message_size: 16777216
      compression: false
```

- **Cancellation safety**: `recv` reads from an internal mpsc, safe
  to drop. `send` is a single unary RPC — drop cancels cleanly.
- **Send deadline**: `send_timeout_ms` (default 30 s, `0` for none) bounds each `send`, and each request of a `send_batch`, end to end, DNS, connect and TLS handshake included, and a send past it is `Backpressured`. A dial whose DNS lookup, TCP connect or TLS handshake has not finished by nine tenths of the limit is abandoned, so the send that started it reports the failure and the next send dials afresh.
- **Send failures**: `Unavailable`, `ResourceExhausted`, `DeadlineExceeded` (`send_timeout_ms`), `Cancelled` (a server cutting the RPC at its deadline), and a connection that fails before the server answers are `Backpressured`, so an absent, restarting or overrun receiver is waited out. Any other status the server returns is `Fatal`. A send answered `Unavailable` with the `scalo-hold-expired` trailer, or ended by `DeadlineExceeded` or `Cancelled`, may still be delivered by the receiver, so the retry can duplicate it: it counts in `transport_redelivered_total{reason="hold_expired"|"deadline"}`.
- **Dead connections**: the client sends an HTTP/2 PING once a connection has read nothing for `send_timeout_ms` (30 s when it is `0`), and closes the connection when the PING goes unanswered for as long again. A receiver that stays connected but stops answering is dropped that way, and the next send dials afresh.
- **Message-size ceiling**: `max_message_size` bounds the encoded request, measured uncompressed, as the receiver's decoder measures it. `send` returns `FilteredDlq` for a record over it without making the RPC, and for a record the receiver refuses with `OutOfRange` (its limit is lower, or gzip grew the frame past it). `send_batch` sends a block over it as several `RouteBatch` requests, in order, each within the limit. A failure after the first leaves the earlier ones accepted, and the retry of the whole block sends them again. A record over the limit on its own is left out and counted in `transport_message_too_large_total`. When every record is, the result is `FilteredDlq`, which callers take as handled. Otherwise the rest of the block is sent and the result is `Ok`. Either way the record left out is DROPPED: it is counted in `pipeline_dead_letters_dropped_total{reason="too_large"}`, once the rest of the block is sent when there is a rest. To dead-letter it instead, take out every record `dead_letter_reason` names before the call, as the pipeline's `.sender(&sender)` does.
- **Changed in 2.13.0, for `send_batch` callers**: 2.12 returned `Fatal` for a block over the limit and sent none of it. 2.13.0 splits the block, and a record over the limit on its own no longer fails the block: it is dropped and counted, as above.
- **Acknowledgement**: unarmed, or with acknowledgements disabled, the server answers once the records are queued for `recv`, not once the consumer reads them. Armed, it answers once they are released: see [Held responses](#held-responses). A full queue (`recv_buffer_size`) answers `ResourceExhausted`.
- **`RouteBatch`**: a batch is queued whole or not at all. One with more records than `recv_buffer_size` never fits the queue, so it is held whole in a slot of its own, one batch at a time: it lands in one step, or is answered `ResourceExhausted` with nothing queued while an earlier one is still waiting. `recv` hands that batch over before the queue, so the receiver holds at most `recv_buffer_size` records plus one such batch. Each request takes one contiguous range of token sequence numbers.
- **`close()`**: refuses new pushes with `Unavailable`, which senders retry, and keeps every acknowledged record: `recv` returns them, then `TransportError::Closed`. Held responses are answered as their records are released, and those still held at the drain deadline (`drain_deadline`, default 20 s, kept under the pod's termination grace period) are answered `Unavailable`. Open connections finish their in-flight RPCs on their own, the listener is free when `close()` returns, and a client that never finishes its RPC does not hold it open. Dropping the transport stops the server too, and answers every held response `Unavailable` at once.
- **Shutdown order**: `close()`, then `recv` until `Closed`, releasing each block as it completes, then flush. A service that stops calling `recv` before it returns `Closed` loses the records still queued, whenever it closes.
- **Counters**: receipts count in `transport_received_*`, never in `transport_sent_total`.
- **`is_healthy()`**: `false` after `close()`. Also sets the `transport_healthy{transport="grpc"}` gauge on every read.
- **`commit()`**: no-op -- gRPC has no persistence to advance. `release` answers held responses.

Source: [../../src/transport/grpc/](../../src/transport/grpc/).

### Held responses

A server armed by a caller that releases every token it takes answers a push only once its records are released: `OK` when every one was delivered, dropped by policy or dead-lettered, `Unavailable` when any was not, which senders retry. A caller that never arms it keeps the answer at enqueue, so an existing service is not stalled by a release it never makes.

- **Arming**: an app that releases every token it takes, through the `BatchEngine` pipeline builder or `SourceAck`, builds its server armed: `GrpcTransport::builder(..).armed(true)`, or `AnyReceiver::from_config_armed(key)` / `from_config_with_governor_armed(key, governor)`. The server is then armed before it listens, so the first push that can arrive, native or Vector-compat, is held. Arming later with `AckControl::arm`, as the pipeline also does, leaves every push before that call answered at enqueue and unprotected. On an armed server `arm` changes nothing.

- **Config**: `acknowledgements.enabled` (default `true`) from `<key>.grpc.acknowledgements` when the factory builds the receiver, else `acknowledgements` on `GrpcTransport::builder` or `with_acknowledgements`. Disabled, the server answers at enqueue even when armed, and a release of its tokens does nothing. The limits below are set on the builder.
- **Sink confirmation**: as a sender, `confirms_delivery()` is `Remote`, and `dead_letter_reason` names a record over `max_message_size` on its own, measured as `send_batch` measures it, and an outbound `dlq` filter match.

- **Hold budget**: a held response is answered within the lesser of `max_hold` (default 25 s) and the sender's `grpc-timeout` less a margin of a tenth of it, at least 1 s and at most half. Past it the server answers `Unavailable` with the trailer `scalo-hold-expired: 1`, and the records stay held until released. tonic cuts a handler at the sender's deadline with `Cancelled`, which a sender cannot tell from a crash, so the answer goes first.
- **Held-byte ceiling**: held responses carry at most `max_held_bytes` of payload (default a quarter of the memory guard's limit, else 256 MiB). Past it a push is answered `ResourceExhausted` with `grpc-retry-pushback-ms: 1000`. One push is always admitted while nothing is held, so a request over the ceiling on its own still gets through. Held bytes are leased on the memory guard (`memory_guard`) from admission to answer. Admission runs governor, then ceiling, then queue.
- **Pressure**: a server built with a governor (`with_pressure`, or `pressure` on the builder) adds an `AckHeldSource` to the governor's latch, so held bytes near the ceiling pause Kafka intake and refuse pushes with `Unavailable` through the same gate as memory. See [../backpressure.md](../backpressure.md#held-responses).
- **Tokens**: each request takes one contiguous range of sequence numbers, and a token is found by its number alone, so a `GrpcToken` rebuilt from a stored `seq` releases the same record. Releasing a token twice counts once.
- **Counters**: `transport_ack_held` and `transport_ack_held_bytes` (gauges); `transport_ack_latency_seconds{outcome}`, admission to answer; `transport_ack_released_total{outcome}`, outcome `delivered`, `dropped`, `rejected`, `errored`, `expired`, `shutdown`, or `orphaned` for one freed after its sender went away unanswered; `transport_ack_refused_total{reason}`, reason `ceiling`, `pressure`, `closed` or `full` (a receive queue with no room), counted while responses are held.

### `transport-grpc-vector-compat`

Wire-compat shim for `vector.Vector/PushEvents`. Only used by
a Vector-compat transform consumer so legacy Vector sinks can target a native gRPC
endpoint without recompile. Enable with `vector_compat: true` in the
gRPC config — the server then accepts both native and Vector RPCs on
the same listener. Not a separate backend, not for any other app.

A `PushEvents` request is queued whole or not at all: every event is converted first, then room for all of them is reserved in the receive queue, waiting while it is full. A receiver closed under the request refuses it with `Unavailable` (`receiver closed`), which Vector retries, and none of its events were queued. A request with more events than `recv_buffer_size` cannot be reserved at once, so it is queued one event at a time; if the receiver closes part-way, the events already queued arrive again when Vector retries the request.

A `PushEvents` goes through the same intake as a native push: refused with `Unavailable` while the pressure governor holds intake (`GrpcTransport::with_pressure`), and counted in `transport_received_events_total` and `transport_received_bytes_total` (the JSON queued for `recv`) once queued. A refusal because the receiver closed counts in `transport_refused_total`.

Armed, a `PushEvents` is held like a native push, over the same registry, ceiling and hold budget, one entry per request: `OK` once its events are released, `Unavailable`, which Vector retries, when any was not delivered or the hold ran out. Metric events are skipped rather than queued, and a request that skipped any is released no better than `Dropped`. A request larger than `recv_buffer_size` that the receiver closes part-way is answered `Unavailable`, and the events already queued still release.

`VectorCompatClient` (the sending side) is plaintext only; an `https` endpoint fails on the first RPC. A dial whose DNS lookup or TCP connect is unfinished at nine tenths of the gRPC transport's default `send_timeout_ms` (30 s) is abandoned, so the call that started it returns an error and the next call dials afresh. `health_check`, a short probe, also gives up at the full 30 s. `send_events` has no limit once connected: a Vector source with end-to-end acknowledgements holds `PushEvents` open until its own sink has delivered, and cutting that off to retry would push the same events twice. The client sends an HTTP/2 PING once a connection has read nothing for 30 s and closes the connection when the PING goes unanswered for 30 s more, so a send to a peer that stays connected but stops answering returns an error, and the next call dials afresh. A peer that holds the RPC and still answers the PING is waited on. `VectorCompatClient::connect_lazy_within(endpoint, send_timeout_ms)` sets that limit instead of 30 s, for the dial, the health check and the PING alike (0 = none, with the PING at 30 s). A transform that holds its own source's answer sets it below that hold, so a stalled dial fails while the source can still answer its sender.

`send_events` fails with `TransportError::Send`, which does not say whether a resend can succeed. `send_events_status` fails with the gRPC status instead, and `VectorCompatClient::is_permanent_rejection(&status)` names the refusals no resend clears:

| Code | Permanent | Why |
|---|---|---|
| `DataLoss` | yes | Vector's `vector` source answers it when a sink it feeds rejected the events |
| `InvalidArgument` | yes | the source cannot use the request |
| `OutOfRange` | yes | the request is over the source's message-size limit |
| `Unavailable`, `ResourceExhausted` | no | the source is down, busy or shutting down |
| `DeadlineExceeded`, `Cancelled` | no | the send ran out of time, and the source may still take it |
| any other, `Unimplemented`, `PermissionDenied`, `Unauthenticated` included | no | a configuration fault, not these events: dropping them would lose them |

A status carrying a source error is the client's own connection failing, and is never permanent. A caller holding its source releases a permanent refusal `Rejected` (dead-lettered) or `Dropped`, and holds and resends everything else. One it drops counts in `pipeline_dead_letters_dropped_total{reason="rejected"}`, the `transport::DEAD_LETTER_REJECTED` label.

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
      path: "/var/log/myapp/events.ndjson"
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
