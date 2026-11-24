# Tiered Sink

`TieredSink<S>` wraps any `TransportSender` (Kafka, gRPC, HTTP, file, pipe, or the factory's `AnySender`) with the resilience primitives every production data-plane pipeline needs -- timeout, circuit breaker, disk spillover, background drain. Apps call `sink.send(&record).await?` and the sink picks the right failure response.

It is the resilient-delivery primitive: put every downstream write in the data plane behind one.

---

## What it composes

A send goes to the sender when the breaker admits it, spills to the spool when the sender cannot take it, and reaches the caller as an `Err` for a fatal send or a spool write that fails.

```mermaid
flowchart LR
    accTitle: L4 Detail -- Tiered sink delivery
    accDescr: A send is one send_batch the breaker admits. Delivered returns Ok. Refused by the breaker, backpressured or timed out spills to the spool. Fatal returns Err Sink, and a record the sender dead-letters returns Err DeadLetter. A spool write returns Ok when written, Ok with the record dropped under drop_newest, Err SpoolFull when full, or Err DiskUnavailable past the disk threshold.

    hot{"Hot send<br/>result?"}
    delivered(["Ok, delivered"])
    spool{"Spool<br/>write?"}
    fatal(["Err Sink"])
    dead(["Err Dead-<br/>Letter"])
    spooled(["Ok, spooled"])
    shed(["Ok, dropped<br/>and counted"])
    full(["Err SpoolFull"])
    disk(["Err Disk-<br/>Unavailable"])

    hot -->|delivered| delivered
    hot -->|not delivered| spool
    hot -->|Fatal| fatal
    hot -->|FilteredDlq| dead
    spool -->|written| spooled
    spool -->|drop newest| shed
    spool -->|full| full
    spool -->|disk threshold| disk

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef store fill:#DDF3EC,stroke:#00785A,stroke-width:1px,color:#0B3327
    classDef decision fill:#FCF1D6,stroke:#9A6700,stroke-width:1px,color:#3D2A00
    classDef error fill:#FBE3D6,stroke:#C2410C,stroke-width:1px,color:#4A1A05
    class hot,spool decision
    class delivered,spooled store
    class fatal,dead,full,disk error
```

*Figure 1. L4 Detail -- Tiered sink delivery. Green ends return `Ok`, orange ends `Err`, and the plain end `Ok` for a record it dropped.* Up: [Pipeline](README.md).

The hot send is one `send_batch`, bounded by `send_timeout_ms`, made when the breaker admits it and, under `StrictFifo`, the spool is empty. Not delivered covers a breaker refusal, `Backpressured` and a timeout. Spool writes encode and compress (zstd, LZ4 or snappy) and append to the yaque spool, and a background drain sends the spool on once the breaker admits.

A `disk_capacity_poller` watches the spool filesystem when
`disk_aware` is configured -- if usage crosses the threshold the
`disk_available` flag flips and new spool writes return
`DiskUnavailable` instead of filling the disk.

Each `send_batch` answer and what the sink does with it:

| `send_batch` result | Disposition | Circuit effect |
| ------------- | ------------- | ---------------- |
| `Ok` | `Ok` to the caller | Success recorded: failure count reset, circuit closes |
| `FilteredDlq` | `Err(TieredSinkError::DeadLetter)` to the caller. Not spooled | Success recorded |
| `Backpressured` | Spool to disk | Failure recorded and counted in `spool_circuit_trips_total`, may open the circuit |
| `Fatal(e)` | `Err(TieredSinkError::Sink)` to the caller, with `e` as the cause | None. Not spooled |
| No answer within `send_timeout_ms` | Spool to disk | Failure recorded and counted in `spool_circuit_trips_total`, may open the circuit |

Before any of that, `send` asks the sender's `dead_letter_reason`, and a record it names comes back as `Err(TieredSinkError::DeadLetter)` without being sent or spooled. The caller holds that record, so it writes it to its DLQ, as it does for `Sink` and `SpoolFull`.

The drain has no caller to hand a record back to. A spooled record the sender names, answers `FilteredDlq` for, or refuses with `Fatal` when sent on its own goes to the DLQ set with `TieredSink::with_dlq(Arc<Dlq>)`, with the cause as its reason, and its spool entry is committed only once the DLQ confirms it. A DLQ that does not confirm within `send_timeout_ms` leaves the batch spooled for the next pass. A dead letter a Kafka-only DLQ's broker refuses for its size (`DlqError::Refused`) is dropped and counted as `dropped`, and the rest of the batch is committed, while `when_full: dlq` blocks on such a record instead. With no DLQ such a record is dropped, logged at ERROR and counted in `tiered_sink_dead_letters_total{outcome="dropped"}`. A `Fatal` of `TransportError::Config` or `Closed` describes the sink and not the record (a Kafka sink whose credentials are refused, say), so the record stays spooled and the circuit counts a failure until the sink is fixed.

---

## Circuit breaker

The breaker opens on consecutive failures and probes once from half-open, evaluated per `send` and per drain pass.

```mermaid
stateDiagram-v2
    direction LR
    accTitle: L4 Detail -- Circuit breaker states
    accDescr: The breaker starts Closed, opens after the threshold of consecutive failures, goes half-open once reset_timeout has elapsed, and from half-open closes on a success or reopens on a failure.
    [*] --> Closed
    Closed --> Open: threshold failures
    Open --> HalfOpen: reset_timeout
    HalfOpen --> Closed: success
    HalfOpen --> Open: failure
```

*Figure 2. L4 Detail -- Circuit breaker states. A label names what moves the breaker.* Up: [Pipeline](README.md).

- Closed -- hot path active, failures counted.
- Open -- hot path skipped, every message goes straight to spool.
- Half-open -- one probe allowed, taken by whichever of a `send` or the drain asks first. Success -> Closed, failure -> Open. A probe that records neither, because its future was dropped or its outcome counts as neither, is lost once `reset_timeout` has passed since it went out, and the next caller probes again.

Hooks into the global `HealthRegistry` (when `health` feature is on)
so `/readyz` reports the sink as Degraded (HalfOpen) or Unhealthy
(Open).

See [`../../src/tiered_sink/circuit.rs`](../../src/tiered_sink/circuit.rs).

---

## Ordering modes

| Mode | Hot path policy | Trade-off |
| ------ | ----------------- | ----------- |
| `Interleaved` (default) | Always try hot path when circuit allows | Max throughput, and new messages may arrive before older spooled ones |
| `StrictFifo` | Hot path only when spool is empty | Strict order, and new traffic queues behind drain |

`StrictFifo` is what you pick when downstream ordering matters
(transaction log replication, append-only stores). Most pipelines
accept the interleave for the throughput win.

---

## Drain strategies

The background drain (`drainer::drain_loop`) pulls from the spool and
re-tries the primary sink at a rate controlled by `DrainStrategy`:

| Strategy | Behaviour |
| ---------- | ----------- |
| `Adaptive { initial_rate, max_rate }` (default) | Start slow (100 msg/s), accelerate based on success rate, cap at `max_rate` (10 000 msg/s) |
| `RateLimited { msgs_per_sec }` | Fixed rate |
| `Greedy` | Drain as fast as possible -- risks overwhelming a recovering sink |

Each pass takes up to `drain_batch_max_records` (500) records and `drain_batch_max_bytes` (4 MiB, counted as stored after compression and framing), no more than a second's worth at the strategy's current rate, and sends them in one `send_batch` under one `send_timeout_ms`. Records the sender names in `dead_letter_reason` are taken out first. Every entry is committed only when the sink answers `Ok` and the DLQ holds the dead letters. `Backpressured`, a timeout, or `Fatal(Config)`/`Fatal(Closed)` rolls the whole batch back, so a retry can deliver part of it twice, never lose it. Any other `Fatal`, and a `FilteredDlq`, names no record, so the batch is kept and its entries are sent one at a time, which dead-letters only the record the sink rejects. An entry that fails its CRC is dropped on its own, and the rest of its batch is sent. One that does not decompress or decode holds the drain ([Changing the codec](#changing-the-codec)).

A pass that takes records starts the next after the strategy's pause for that many records, so the strategy sets the rate. `drain_interval_ms` (100 ms) is added after a pass that found the spool empty or the sink down. Shutdown cuts either wait short.

The drain task is spawned at construction. It and the disk poller both stop when the sink's shutdown `CancellationToken` is cancelled, by `shutdown()` or by drop. With the `shutdown` feature the drain task stops on the global token.

Each drain send is bounded by `send_timeout_ms`, the limit the hot path uses. A send that runs past it counts as a circuit failure. The record stays at the head of the spool, uncommitted, and is offered again on the next drain pass. The downstream may have taken the record before the timeout, so the retry can duplicate it (at-least-once).

---

## Spool sizing and disk awareness

- `max_spool_items` -- record limit for the spool.
- `max_spool_bytes` -- byte limit, as stored (post-compression).
- `disk_aware: { max_usage_percent, poll_interval_secs }` -- background
  `statvfs` poller. Once filesystem use crosses the threshold,
  `disk_available` flips false and spool writes return
  `DiskUnavailable`.

On a Kubernetes emptyDir `statvfs` reads the node's disk, not the volume's `sizeLimit`, so a spool past the limit gets the pod evicted and the spool lost. The scalo-service chart sets `SCALO_WRITABLE_PATH_LIMITS` to each emptyDir writable path with a `sizeLimit`. With `max_spool_bytes` unset, `TieredSink::new` caps a spool on such a path at nine tenths of its limit. On Kubernetes, `disk_aware` with no cap at all is refused with `SpoolOpen`.

A spill reserves its item slot and bytes before it writes, and gives them back if the write fails or the `send` future is dropped, so a cancelled spill leaves nothing counted. A spill past `max_spool_items` or `max_spool_bytes` applies `when_full`:

| `when_full` | `send` returns |
| --- | --- |
| `block` (default) | `Err(TieredSinkError::SpoolFull)` |
| `drop_newest` | `Ok`. The record is dropped and counted in `tiered_sink_dropped_total{policy="drop_newest"}` |
| `dlq` | `Ok` once the DLQ set with `with_dlq` confirms the record, counted in `tiered_sink_overflow_total{policy="dlq"}`. With no DLQ, or no confirmation within `send_timeout_ms`, behaves as `block` |

Crossing the `disk_aware` threshold returns `Err(TieredSinkError::DiskUnavailable)` whatever `when_full` says. The caller decides what to do with either error -- the typical pattern is to route the record to the DLQ.

`TieredSink::new` clears spool locks a killed process left behind and rejects a `spool_path` another live sink holds, so each sink needs a path of its own. A spill cache that will not open is quarantined into a `corrupt-*` subdirectory of `spool_path`, never by renaming the path. A receive position torn by a kill is not that case: the sink replays from the first segment instead, so some consumed records come again. A spill is flushed to the operating system, so it survives the process being killed. It survives the host losing power once the cache is synced: `TieredSink::sync().await` syncs every spill so far and the directories of the cache's path (the first call flushes each one up to the root, since opening creates any that are missing, and Windows has no directory sync, so there it flushes the files alone), so a caller that accepts records in batches syncs once per batch, and `sync_on_spill: true` syncs inside every spilling `send`. The default is off because a sync costs a disk sync per spilled record on the path that runs when the sink is down. A sync that fails returns `TieredSinkError::Spool` with the record already spilled, so a retry can deliver it twice. It finds the segments through the cache's path, so a spool directory renamed or replaced while the sink runs is not flushed, and a directory above the cache's parent that the process may not open is skipped. See [spool.md](spool.md#locks-after-a-hard-kill).

---

## Changing the codec

A spilled entry carries no codec, so the drain reads every entry with the `compression` the sink runs now. An entry that does not decompress or decode under it is never dropped: the drain sends what comes before it, then holds with that entry and everything after it on disk. It logs the codec and the error once, sets `tiered_sink_drain_held` to `1`, fails readiness (the `tiered_sink_drain` health component) and `is_drain_held()` returns `true`. New records still spill behind it. Reopen the sink under the codec that wrote the entries and the drain resumes, in order. An entry that fails its CRC is damaged rather than foreign, and is still dropped. Drain the spill before changing `compression` to avoid the hold.

## Configuration

```yaml
tiered_sink:
  spool_path: /var/spool/myapp/clickhouse
  send_timeout_ms: 1000
  compression: {codec: zstd, level: 3}  # the default. Or none, lz4, snappy (compression-snappy)
  drain_strategy:
    type: adaptive
    initial_rate: 100
    max_rate: 10000
  ordering: interleaved
  max_spool_items: 1000000
  max_spool_bytes: 10737418240   # 10 GiB
  when_full: block
  crc: false
  on_corruption: quarantine
  sync_on_spill: false             # true syncs to disk before a spilling send returns
  circuit_failure_threshold: 5
  circuit_reset_timeout_ms: 30000
  drain_interval_ms: 100
  drain_batch_max_records: 500
  drain_batch_max_bytes: 4194304   # 4 MiB, as stored
  disk_aware:
    max_usage_percent: 0.8
    poll_interval_secs: 5
```

A `circuit_reset_timeout_ms` below `send_timeout_ms` logs a WARN naming both when the sink starts. A half-open probe still sending is then lost before its send can time out, so a second probe can join it.

`TieredSinkConfig::new(path)` plus the builder methods cover the
common cases without writing the full struct.

---

## Metrics

Emitted through the `metrics` crate, and only when scalo is built with the `metrics` feature (`metrics-core` alone does not emit them). Names are bare. A `metrics.namespace` prefixes each with `{namespace}_`, see [../core-pillars/metrics.md](../core-pillars/metrics.md). `ServiceMetrics::register` describes every one of them in `/metrics/manifest`.

| Metric | Type | Labels | What it reports |
| --- | --- | --- | --- |
| `spool_hot_path_total` | counter | none | Records the sink took directly, answering `Ok` within `send_timeout_ms` |
| `spool_cold_path_total` | counter | none | `send` calls that ended on the spool path without error. Includes records shed by `drop_newest`, since `send` returns `Ok` for them |
| `spool_circuit_trips_total` | counter | none | Hot-path sends that failed against the breaker (`Backpressured`, or past `send_timeout_ms`). One per failed send, not per Closed-to-Open change. Drain failures are not counted |
| `spool_circuit_state` | gauge | none | Breaker state at the last `send`: `0` closed, `1` half-open, `2` open. Half-open shows once the reset timeout has passed, before a probe goes out. An idle sink keeps its last value |
| `tiered_sink_overflow_total` | counter | `policy` | Records a full spool did not take: `drop_newest` sheds them, `dlq` writes them to the DLQ. `block` returns `SpoolFull` and does not count |
| `tiered_sink_dead_letters_total` | counter | `reason`, `outcome` | Records the drain or `when_full: dlq` could not deliver. `outcome` is `dlq` when the DLQ confirmed it, `dropped` with no DLQ or one no backend can hold it. `reason` is `too_large`, `outbound_filter`, `rejected` (refused when sent) or `spool_full` |
| `tiered_sink_dropped_total` | counter | `policy` | Records dropped on overflow. Moves with `tiered_sink_overflow_total` |
| `tiered_sink_drain_held` | gauge | none | `1` while the drain holds at a spilled entry its codec cannot read, else `0`. See [Changing the codec](#changing-the-codec) |
| `spool_messages` | gauge | none | Records in the spool after the last spill. The drainer never updates it, so it goes stale once draining starts. `spool_len()` is the live count. `ServiceMetrics::spool_messages` writes the same series |
| `spool_bytes` | gauge | none | Bytes in the spool as stored (compressed, plus the CRC header when `crc` is on). Stale the same way. `spool_bytes()` is the live figure. `ServiceMetrics::spool_bytes` writes the same series |
| `spool_disk_available_bytes` | gauge | none | Bytes free to unprivileged users on the spool's filesystem, set every `poll_interval_secs` by the disk poller, so only with `disk_aware` |
| `spool_disk_total_bytes` | gauge | none | Size of that filesystem, set with `spool_disk_available_bytes` |
| `spool_dropped_without_shutdown_total` | counter | none | Sinks dropped with records still spooled and no completed `shutdown().await`. Flat after a clean `shutdown()` |
| `spool_stale_locks_cleared_total` | counter | `lock`, `reason` | Lock files left by a dead process, removed on `TieredSink::new`. `lock` is `send`, `recv` or `version`, `reason` is `dead_owner` or `unparseable`. Non-zero means a previous run ended without releasing its locks. See [spool.md](spool.md#locks-after-a-hard-kill) |
| `spool_quarantined_total` | counter | `trigger` | A spill cache that would not open, moved into `corrupt-*` under `spool_path`. The tiered sink emits only `trigger="open_failed"`. See [spool.md](spool.md#quarantine) |

Alert on:

- `spool_dropped_without_shutdown_total` above 0. A sink was dropped with records still spooled and no completed `shutdown().await`, so fix the caller. Spooled records stay on disk and replay on the next open, but `Drop` does not wait for a drain in flight.
- `tiered_sink_dropped_total` rising. `drop_newest` is discarding records.
- `spool_circuit_state` held at `2`. The sink is down and everything is going to the spool.
- `1 - spool_disk_available_bytes / spool_disk_total_bytes` nearing `max_usage_percent`. Spool writes then return `DiskUnavailable`.
- `spool_quarantined_total` above 0. A spill cache was set aside and its records are not replayed.
- `tiered_sink_drain_held` at `1`. The spill holds entries another codec wrote, and nothing drains until the sink runs under that codec again.

No metric counts a `SpoolFull`, `DiskUnavailable` or `DeadLetter` error, which goes back to the caller, or an entry the drainer discards because it fails its CRC, which is logged at error level.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `TieredSink::new(sender, config)` | Construct, open spool, spawn drain task. `S` is any `TransportSender`, and the sink calls its `send_batch` with one record and acts on the `SendResult` |
| `send(&Record) -> Result<()>` | Hot path, else spool. Errors go back to the caller, which routes the record (usually to the DLQ) |
| `with_dlq(Arc<Dlq>)` | The DLQ the drain writes records it cannot deliver to, and `when_full: dlq` the records a full spool cannot take (`dlq` feature) |
| `spool_len() / spool_is_empty() / spool_bytes()` | Spool depth introspection |
| `circuit_state()` | Current `CircuitState` |
| `reset_circuit()` | Manual reset (admin / test) |
| `hot_path_count() / cold_path_count()` | Cumulative path counters |
| `is_disk_available()` | Disk-aware poller flag |
| `is_drain_held()` | Whether the drain holds at an entry its codec cannot read |
| `inner() -> &S` | Borrow the wrapped backend |
| `shutdown()` | Stop the drain task and the disk poller, and wait for both to exit |
| `TieredSinkError` | `SpoolOpen` from `new`. From `send`: `DeadLetter` (the sender named it or answered `FilteredDlq`), `Sink` (a `Fatal` answer, with its cause), `SpoolFull`, `DiskUnavailable`, `Spool` (the spool write failed), `Codec` (compression failed, or from `new` a `compression` the spill does not take) |
| `TieredSinkConfig` / `WhenFull` / `OrderingMode` / `DrainStrategy` / `DiskAwareConfig` / `CorruptionPolicy` | Configuration, see [Configuration](#configuration) |
| `CircuitBreaker` / `CircuitState` | Exposed for tests and custom integrations |

`shutdown()` lets a drain in flight finish its send and commit, then returns. A drain send to a downstream that never answers is cut off at `send_timeout_ms`, so `shutdown()` waits at most that long. It does not drain the spill cache to the sink: records still spooled stay on disk and replay on the next open, so shutdown loses nothing `send` accepted.

Drop cancels the same token, so both background tasks exit even if the consumer forgets to call `shutdown().await`, but drop cannot wait for them.

---

## Related

- [spool.md](spool.md) -- the standalone yaque queue. This sink runs a separate one, with the same lock recovery and quarantine
- [dlq.md](dlq.md) -- where to send `SpoolFull` / `Fatal` errors
- [batch-engine.md](batch-engine.md) -- the run loops this is a common sink for
- [../transport/README.md](../transport/README.md)
- [../feature-flags.md](../feature-flags.md) -- `tiered-sink`
- [../auto-wiring.md](../auto-wiring.md)
- [../architecture.md](../architecture.md)
- [`../../src/tiered_sink/mod.rs`](../../src/tiered_sink/mod.rs)
- [`../../src/tiered_sink/tiered.rs`](../../src/tiered_sink/tiered.rs)
- [`../../src/tiered_sink/circuit.rs`](../../src/tiered_sink/circuit.rs)
- [`../../src/tiered_sink/error.rs`](../../src/tiered_sink/error.rs)
- [`../../src/tiered_sink/drainer.rs`](../../src/tiered_sink/drainer.rs)
- [../compression.md](../compression.md) -- the `compression` key and its codecs
- [`../../src/tiered_sink/config.rs`](../../src/tiered_sink/config.rs)
