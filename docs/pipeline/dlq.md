# DLQ

The DLQ (dead-letter queue) is the last-resort sink for messages the
primary pipeline couldn't deliver — parse errors, validation
failures, persistent transport failure, `TieredSink::SpoolFull`,
poison records. Every data-plane service shares one DLQ orchestrator built
from cascade config.

The orchestrator is `Dlq` — a clone-cheap handle wrapping a
`BackgroundSink<DlqEntry>`. Calling `send` queues the entry on an
in-memory mpsc and returns. A drain task pulled out of the runtime
loop coalesces queued entries into batches and writes to one or more
backends. Callers never block on disk, Kafka, or HTTP I/O.

---

## Three backends

| Backend | Feature | Storage |
|---------|---------|---------|
| File | `dlq` (always available) | NDJSON to disk via the shared `io::NdjsonWriter`, with rotation (`Hourly` default) and gzip on rotation |
| Kafka | `dlq-kafka` (needs `transport-kafka`) | Publish to a dedicated DLQ topic — per-table (`acme.auth` → `acme.auth.dlq`) or single common topic |
| HTTP | `dlq-http` (needs `reqwest`) | POST batched entries as NDJSON |

After a refused write the file backend reopens its file on a later
write, waiting 250 ms and doubling up to 30 s while writes keep failing.
So a deleted file or a restored directory recovers without a restart. It
never recreates a missing directory, because that could put the DLQ on
the filesystem under an unmounted volume.

Backends are concrete variants of a `DlqBackend` enum (static
dispatch, no `Box<dyn>`, no `async-trait` macro). Adding a new backend
means extending the enum in scalo — consumers never construct backend
types directly.

---

## Modes

| Mode | Behaviour |
|------|-----------|
| `Cascade` (default) | Try backends in order (Kafka → File → HTTP), stop on first success |
| `FanOut` | Write to every enabled backend, succeed if any succeed |
| `FileOnly` | File backend only — no Kafka dependency |
| `KafkaOnly` | Kafka backend only |

Cascade is the production default — Kafka primary, file fallback for
when the broker is unreachable. FanOut is for compliance setups that
need every entry mirrored to two destinations.

---

## Queue-admission semantics

`send` / `try_send` return as soon as the entry is on the in-memory
queue -- **not** when a backend has it. `flush` is the barrier that
says whether the backends took everything:

```rust
dlq.send(entry).await?;     // queued (non-blocking)
dlq.flush().await?;         // every entry queued before this call is
                            // written, and no write was refused
```

`flush()` returns once the drain has written every entry queued before
the call. It returns `Ok` only if every batch the drain wrote since the
previous `flush()` was accepted by a backend. That covers all three
write triggers: a full batch (`batch_size`), the `flush_interval_ms`
tick, and the batch the barrier itself writes.

If every backend refused a batch, the next `flush()` returns
`Err(DlqError::File(..))`. The drain does not retry refused entries.
They are counted in `dropped()` and in
`dlq_dropped_total{reason="backends_failed"}`.

A refusal is reported once. The first barrier the drain processes after
it returns the error, and the `flush()` after that starts clean. With
concurrent callers, the other barriers return `Ok`.

A `flush()` dropped before its ack, such as by a timeout around it, does
not consume the error. The next `flush()` returns it.

What "accepted" means depends on the backend:

| Backend | Accepted means |
|---------|----------------|
| File | Written to the kernel page cache. No `fsync`, so power loss before write-back can still lose it |
| Kafka | Queued to the producer. `flush()` does not wait for the broker to acknowledge it, and a delivery the broker later refuses is not reported by `flush()` or `dropped()` |
| HTTP | The endpoint returned a 2xx status |

An entry refused at admission (`try_send` returning `QueueFull`) never
reached the queue, so `flush()` does not cover it -- `dropped()` does.

For at-least-once handling, `flush().await` before treating a dead
letter as safe, and read `Err` as "entries written since the last flush
were lost". Most callers don't need to -- DLQ delivery is best-effort
by design and the drain will eventually write.

`try_send` returns `Err(DlqError::QueueFull)` immediately when the
in-memory queue is full (`Overflow::Drop`). The drop counter is
incremented for visibility — the caller decides whether to log,
escalate, or proceed.

---

## Shutdown

The drain finishes its in-flight batch, drains the remaining queue,
then exits. Triggered by either:

- `CancellationToken::cancel()` passed to `spawn`, or
- All `Dlq` handles dropped (channel closes naturally).

Then `Dlq::shutdown().await` joins the drain task. Idempotent — safe
to call from any clone.

---

## Configuration

```yaml
dlq:
  enabled: true
  mode: cascade
  queue_capacity: 10000         # in-memory mpsc bound
  batch_size: 256                # drain coalescence
  flush_interval_ms: 100         # partial-batch flush
  file:
    enabled: true
    path: /var/spool/dfe/dlq
    rotation: hourly             # hourly | daily
    max_age_days: 30
    compress_rotated: true
  kafka:                         # dlq-kafka feature
    enabled: true
    routing: per_table           # per_table | common
    topic_suffix: .dlq
    common_topic: dfe.dlq
    send_timeout_ms: 5000
  http:                          # dlq-http feature
    enabled: false
    endpoint: https://dlq.example/ingest
```

---

## Upgrading from the older DLQ API

Earlier releases exposed `Dlq::file_only` / `Dlq::with_kafka` constructors
and a `DlqBackend` trait object. Both are gone — every backend mix now goes
through `Dlq::spawn` with `DlqMode` selecting routing, and `DlqBackend` is an
enum (static dispatch). The orchestrator gained `try_send` (non-blocking,
`QueueFull` on overflow), `flush` (write barrier), and `shutdown`
(drain + join). `send` semantics changed from "wait for durable write" to
queue-admission — see [Queue-admission semantics](#queue-admission-semantics).

The version-keyed upgrade path lives in [migrations.md](../migrations.md).

---

## API surface

| Item | Purpose |
|------|---------|
| `Dlq::disabled()` | No-op handle — `send` succeeds, nothing written; each routed entry is counted in `dropped()`, emitted as `dlq_dropped_total{reason="disabled"}`, and logged at ERROR (rate-limited) |
| `Dlq::spawn(config, service_name, kafka_config, shutdown)` | Build backends, spawn drain, return cloneable handle |
| `try_send(entry) -> Result<(), DlqError>` | Sync-shape queue submission; `QueueFull` on overflow |
| `send(entry).await` | Async submission that awaits queue space |
| `send_batch(entries).await` | Queue many entries (drain coalesces) |
| `flush().await` | Barrier -- wait until every entry queued before this call is written; `Err` if any batch written since the previous flush was refused by every backend (see [Queue-admission semantics](#queue-admission-semantics)) |
| `shutdown().await` | Wait for drain task to exit cleanly |
| `is_enabled() / mode() / pending() / dropped()` | Introspection — `dropped()` totals queue overflow + disabled-DLQ sends + batches every backend refused (`dlq_dropped_total{reason="backends_failed"}` + rate-limited ERROR) |
| `DlqEntry::new(service, error_type, payload)` + `.with_destination(...)`, `.with_source(...)`, `.with_metadata(...)` | Entry builder |
| `DlqSource::kafka(topic, partition, offset) / ::http(url) / ...` | Provenance for the entry |
| `DlqBackend` (enum) | `File / Kafka / Http` — feature-gated variants |
| `DlqMode` | `Cascade / FanOut / FileOnly / KafkaOnly` |
| `DlqError` | `Io / Serialization / File / Kafka / BackendError / AllBackendsFailed / NotConfigured / QueueFull / Closed` |

`Dlq` is `Clone` — clones share the same drain. The single-owner
shutdown handle lives inside `Arc<AsyncMutex<Option<...>>>` so any
clone can call `shutdown()`.

---

## Source

- [`../../src/dlq/mod.rs`](../../src/dlq/mod.rs)
- [`../../src/dlq/orchestrator.rs`](../../src/dlq/orchestrator.rs) — `Dlq`, `DlqDrain`, cascade/fan-out dispatch
- [`../../src/dlq/backend.rs`](../../src/dlq/backend.rs) — `DlqBackend` enum
- [`../../src/dlq/config.rs`](../../src/dlq/config.rs)
- [`../../src/dlq/entry.rs`](../../src/dlq/entry.rs) — `DlqEntry`, `DlqSource`
- [`../../src/dlq/file.rs`](../../src/dlq/file.rs)
- [`../../src/dlq/kafka.rs`](../../src/dlq/kafka.rs)
- [`../../src/dlq/http.rs`](../../src/dlq/http.rs)

---

## Related

- [tiered-sink.md](tiered-sink.md) — common upstream caller (routes `SpoolFull` / `Fatal` to DLQ)
- [batch-engine.md](batch-engine.md) — parse errors and pre-route DLQ outcomes flow here
- [../transport/README.md](../transport/README.md) — Kafka backend reuses `KafkaConfig`
- [../transport/filter-engine.md](../transport/filter-engine.md) — wire-level filter drains DLQ entries here
- [../feature-flags.md](../feature-flags.md) — `dlq`, `dlq-kafka`, `dlq-http`
- [../auto-wiring.md](../auto-wiring.md)
- [../architecture.md](../architecture.md)
