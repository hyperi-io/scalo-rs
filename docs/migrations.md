# Migrations

API surface changes that require consumer adjustment. Indexed by the
scalo version where the change first ships. The local `rebuild-consumers`
skill reads this when `cargo check` flags breakage on a downstream bump.

Pre-GA discipline: no `BREAKING CHANGE:` footer, no major bump. All
six core consumer services migrate in lockstep.

---

## Unreleased -- WorkBatch data-plane spine + self-regulation

The data plane flips onto a single zero-copy currency -- `WorkBatch` (a block
of `Record`s) -- driven `get -> process -> send -> commit` by ONE unified
engine driver, with self-regulation (memory guard + inbound/byte-budget
backpressure) ON by default. See [self-regulation.md](self-regulation.md),
[backpressure.md](backpressure.md), [kafka-path.md](kafka-path.md).

The six core consumer services migrate in lockstep; items below are the
consumer-facing surface changes.

### `WorkBatch` / `Record` -- the canonical currency (BREAKING)

A new `WorkBatch<T>` (a `Vec<Record>` + the block's `commit_tokens` + any
inline `dlq_entries`) collapses the old `Message` / `RawMessage` / `RecvBatch`
trio into ONE block type. `Record` is payload (`bytes::Bytes`, zero-copy) +
routing key + headers + lean `RecordMeta` (timestamp + format), with **no**
commit token.

The headline contract: **commit tokens live on the BATCH, not the record**,
and `commit_tokens.len()` is decoupled from `records.len()`. A transform that
fans `N` records out to `2N` does NOT multiply the source acks -- the driver
commits EXACTLY the `N` input tokens after the `2N`-record block is sent
(at-least-once). Use `WorkBatch::map_records` to transform records while the
commit tokens and DLQ entries flow through untouched.

`WorkBatch`, `Record`, `RecordMeta`, `FramingError` are re-exported as
`scalo::transport::*`. Zero-copy framing helpers: `WorkBatch::single`
(whole blob), `WorkBatch::from_ndjson`, `WorkBatch::from_json_array` (each
slices one inbound `Bytes` into per-record views -- no payload copy).

### `TransportReceiver::recv` returns `WorkBatch<Token>` (BREAKING)

| Old | `recv(max) -> TransportResult<RecvBatch<Token>>` (messages + dlq_entries) |
| New | `recv(max) -> TransportResult<WorkBatch<Token>>` |

`recv` now yields a `WorkBatch` natively -- no `RecvBatch` round-trip. The
inbound-filter DLQ entries arrive on `WorkBatch.dlq_entries` alongside the
passing `WorkBatch.records`; the source acks are on `WorkBatch.commit_tokens`.

Filter-dropped records still commit: a record an inbound `drop`/`dlq` filter
removes produces no passing record but WAS handled, so its commit token is
carried into `WorkBatch.commit_tokens`. Drop it and an all-filtered stretch
freezes the Kafka offset / leaks the Redis consumer-group PEL. (`RecvBatch`
survives internally as the build-helper that carries these `filtered_tokens`
into the `WorkBatch` -- see KEPT-but-deferred.)

**Consumer adjustment** -- anywhere you call `recv()`:

```rust
let batch = transport.recv(100).await?;
for entry in batch.dlq_entries {
    dlq.send(DlqEntry::new("filter", entry.reason, entry.payload)).await?;
}
for record in batch.records { /* process */ }
```

Custom `TransportReceiver` impls: change the `recv` signature to return
`WorkBatch` (build via `WorkBatch::new(records, tokens)` /
`WorkBatch::from_records(records)` / `WorkBatch::empty()`).

### Unified driver: `run_governed` / `run_workbatch*` replace the four run loops (BREAKING)

The four legacy `BatchEngine` run loops (`run` / `run_raw` / `run_async` /
`run_raw_async`) are DELETED. One unified driver family replaces them
(`src/worker/engine/driver.rs`):

| New method | Use |
| --- | --- |
| `run_governed` | The default for a self-regulating app. Streams in byte-budget sub-blocks when the governor is on; delegates to `run_workbatch` when off (byte-identical). |
| `run_workbatch` | On-demand parse (default). The driver does not pre-parse; a transform calls `codec::parse` when it needs a field. Pass-through apps pay zero parse. |
| `run_workbatch_parsed` | Opt-in hot path. The driver pre-parses the whole block (SIMD JSON / native MsgPack) on the pool and hands the closure a `ParsedBatch` (records + aligned `ParsedPayload`s + shared `FieldInterner`). Parse failures route to DLQ, no silent drop. |
| `run_workbatch_streaming` | Explicit sub-block streaming with a caller-supplied byte size (peak memory bounded to one sub-block). |

`process` is now `Fn(WorkBatch<Token>) -> Result<WorkBatch<Token>, EngineError>`
(or `Fn(ParsedBatch<'_, Token>) -> Result<WorkBatch<Token>, EngineError>` for
the parsed path). It MUST preserve `commit_tokens` -- use
`WorkBatch::map_records`, which does so automatically. `CommitMode::Auto`
(engine commits after sink `Ok`) vs `CommitMode::SinkManaged` (sink owns the
commit) selects who fires the acks.

Custom in-process callers: `process_mid_tier` / `process_raw` now take a
`Record` (not a `Message`); only the four run LOOPS were removed.

### `TransportSender::send_batch` (additive, default provided)

New trait method `send_batch(&self, records: &[Record]) -> SendResult`. The
default loops `send` per record (using each record's own key + payload
`Bytes`); transports with a native batch RPC (gRPC `RouteBatch`) override it
to send the whole block in one serde-less call. Commit tokens + DLQ entries
are NOT sent -- they are the sender's local concern; pass `&workbatch.records`
and fire the commit tokens locally after `SendResult::Ok`. Existing `send`
callers are unaffected; the default is non-atomic (a mid-block failure leaves
the already-sent prefix on the wire -- at-least-once, retried by the caller).

### Codec consolidation -- native rmpv, JSON bridge removed (BREAKING for codec users)

`src/transport/codec.rs` is the parse-on-demand codec for the WorkBatch spine.
`parse(&Bytes, PayloadFormat) -> ParsedPayload` decodes JSON via `sonic_rs`
(SIMD) and MsgPack via **native `rmpv`** -- NOT the old
`rmp_serde -> serde_json::Value -> serde_json::to_vec -> sonic_rs` bridge
(two parses + a re-serialise per MsgPack record). `ParsedPayload` keeps its
native value (`sonic_rs::Value` / `rmpv::Value`); `field_str` / `field` are
the format-agnostic routing-field accessors; `to_json_bytes` / `to_msgpack_bytes`
/ `ParsedPayload::to_bytes` serialise back to the OWN wire format (no
cross-format bridge). Pass-through contract: an UNMODIFIED record must reuse
its original `Record.payload` -- `to_bytes` is only for a record a transform
actually mutated.

- The read-side `transport/payload.rs` is removed.
- The MsgPack-via-`serde_json` bridge is removed.
- `rmpv` is a new dependency (`transport` feature).
- `ParsedMessage -> ParsedPayload` rename is DEFERRED (see KEPT-but-deferred).

Parse now bounds nesting at depth 64 (`parse_guard::MAX_PARSE_DEPTH`), for
JSON (cheap iterative pre-scan before the recursive SIMD parser) and MsgPack
(`read_value_with_max_depth`). A deeper payload is a per-record `TooDeep`
parse error (routed to DLQ, not a process abort) -- it stops a hostile
deeply-nested payload exhausting the worker stack. Legitimate payloads rarely
nest past a handful of levels, so this is a security floor, not a tuning knob.

`CodecError`, `FieldRef`, `ParsedPayload`, `parse` are re-exported as
`scalo::transport::*`.

### Self-regulation default-ON (BEHAVIOUR CHANGE, opt-out)

A new `self_regulation` config section turns the data-plane governor ON by
default. When the `governor` feature is compiled in, the runtime builds the
pressure governor (memory HARD source), the inbound gate, and the byte-budget
controller, and threads them into the transports + driver. Memory pressure
brakes inbound intake; the byte budget sizes streaming sub-blocks. To opt out
(byte-identical to pre-governor behaviour -- nothing is constructed):

```yaml
self_regulation:
  enabled: false
```

Full tuning surface (profile / pause_above / resume_below / max_hold_secs /
md_factor) in [self-regulation.md](self-regulation.md). `target_rho` is still accepted and has no effect: the byte budget shrinks only under memory pressure, and utilisation and CPU saturation are the autoscaler's signal.
Off-pressure cost is near zero: without memory pressure the budget stays at or above its big start value, so a block is one sub-block with no per-record overhead.

### Originator brake / token wiring (BEHAVIOUR CHANGE)

Data-originator stages get the inbound brake wired into the receive transport:
Kafka pauses ASSIGNED partitions (member stays in group, no rebalance), HTTP/gRPC returns 503 / `UNAVAILABLE`, and the fetcher pauses its poll. Kafka pairs the brake with its offset and the fetcher with its cursor, so a paused intake never advances the source position. HTTP and gRPC have no source position: a refused request is the sender's to retry, and an accepted one is acknowledged once queued, with `commit` a no-op. `SelfRegulationGovernor::attach_kafka_gate` is the one-call form of the gate dance. See [backpressure.md](backpressure.md).

**App adoption is TWO steps, not one.** The default-on governor only
engages end-to-end if the app adopts BOTH the driver method AND the
governed-receiver constructor. `run_governed` alone wires the byte-budget lever
(streaming sub-blocks) but does NOT brake intake; the inbound brake lives on the
receive transport, which the plain factory constructors
(`AnyReceiver::from_config` / `from_transport_config`) do NOT wire. Each of the
six core consumer services MUST:

1. Drive the engine with `run_governed` (not the legacy run loops); AND
2. Build the receive transport through a governor-aware constructor so the
   inbound brake is actually attached.

The one-call path inside a `ServiceRuntime` app (the governor already exists,
built before transports in `ServiceRuntime::build`):

```rust
// run_service(): governor + pressure already constructed by the runtime.
let receiver = runtime.governed_receiver("transport.input").await?;
// Kafka -> pause-partitions gate attached; HTTP/gRPC -> 503/UNAVAILABLE shed;
// brakeless backends (memory/pipe/file/redis) construct as before.
// Falls back to the plain receiver when self_regulation.enabled = false.
```

Outside `ServiceRuntime` (holding a `SelfRegulationGovernor` directly):

```rust
let receiver = AnyReceiver::from_config_with_governor("transport.input", &governor).await?;
// or from_transport_config_with_governor(&cfg, &governor) for an explicit config.
```

Using `from_config` / `from_transport_config` (no governor) is still valid and
byte-identical to before -- but a factory-built receiver wired that way gets NO
inbound brake even when the governor is on. Adopt the `*_with_governor` /
`governed_receiver` path so the default-on governor is not a silent no-op on the
receive side.

### KEPT but deferred

- `Message` / `RecvBatch` remain as internal build-helpers (with
  `From<Message>` / `From<RecvBatch>` conversions into `WorkBatch`). Fully
  retiring them needs a filter-layer rework -- deferred.
- `ParsedMessage -> ParsedPayload` rename deferred (the engine still uses
  `ParsedMessage` for the in-process callers).

### `BatchEngine` filter-DLQ policy (BEHAVIOUR CHANGE)

The generic `BatchEngine` run loops (`run`/`run_raw`/`run_async`/
`run_raw_async`) previously **silently dropped** inbound-filter DLQ entries
after incrementing a metric. They now apply a `FilterDlqPolicy`, defaulting to
`Reject`: if an inbound `action: dlq` filter produces entries and no policy is
set, the run loop returns `EngineError::FilterDlqUnrouted` instead of dropping
data. (Metrics are not delivery.)

**Who is affected:** only apps whose transport has inbound `action: dlq`
filters AND use the generic run loops. Apps with no inbound DLQ filters are
unaffected (the policy never triggers).

**Consumer adjustment** -- pick a policy explicitly:

```rust
use scalo::worker::engine::FilterDlqPolicy;

// Route dead-letters onward (recommended). The sink is FALLIBLE: return Ok on
// success; an Err is a terminal ack-barrier failure (commit skipped, block
// re-delivered) so dead-letters are never silently lost. The SAME route point
// handles inbound-filter entries AND parse/process-generated entries.
let engine = BatchEngine::new(cfg).with_filter_dlq_policy(
    FilterDlqPolicy::Route(std::sync::Arc::new(move |entries| {
        // enqueue / tokio::spawn a DLQ send -- keep it cheap
        Ok(())
    })),
);

// Or deliberately drop with a metric (the old behaviour, now explicit):
let engine = BatchEngine::new(cfg)
    .with_filter_dlq_policy(FilterDlqPolicy::DiscardWithMetric);
```

The metric `dfe_engine_filter_dlq_unrouted_total` is replaced by
`<namespace>_engine_filter_dlq_discarded_total` (emitted only under
`DiscardWithMetric`), where `<namespace>` is the app's metrics namespace.

### `TransportSender::send` takes owned `Bytes` (BREAKING)

| Old | `send(&self, key: &str, payload: &[u8]) -> SendResult` |
| New | `send(&self, key: &str, payload: bytes::Bytes) -> SendResult` |

Owned-bytes send removes the per-send `payload.to_vec()` copy on
the HTTP path (reqwest `Body::from(Bytes)` is zero-copy) and lets a caller that
already holds `Bytes` (the `BatchEngine`) flow it through without re-copying.

**Consumer adjustment** -- at each `send` call, pass owned `Bytes`:

```rust
// Old:
sender.send("topic", &payload_slice).await;
sender.send("topic", b"literal").await;

// New (all conversions from Vec<u8>/String/&'static [u8] are cheap):
sender.send("topic", bytes::Bytes::from(payload_vec)).await;        // Vec<u8>  (free)
sender.send("topic", bytes::Bytes::from_static(b"literal")).await;  // &'static [u8]
sender.send("topic", bytes::Bytes::copy_from_slice(slice)).await;   // &[u8]    (copies)
```

A caller holding `&[u8]` now copies once at the call site (`copy_from_slice`)
instead of inside the transport -- net-neutral. Callers holding `Vec<u8>` or
`Bytes` (the hot path) are now zero-copy. `bytes` is a `transport`-feature dep.

### `transport::FromCascade` trait (additive, non-breaking)

New `transport::FromCascade` trait with a default `from_cascade_key(key)`
consolidates the byte-identical `from_cascade()` bodies the 5 transport configs
(grpc/http/file/pipe/redis) each repeated. Each config's inherent
`from_cascade()` is unchanged in signature -- it just delegates -- so this is
**not** a consumer migration; it only removes internal duplication.

### `AdaptiveWorkerPool::fan_out_async` return type

| Old | `Vec<Result<R, E>>` |
| New | `Vec<Option<Result<R, E>>>` |

Panicked tasks now surface as `None` instead of being silently dropped
(the old `.flatten()` shortened the output and broke the documented
input-order contract). Caller adjustment:

```rust
// Drop panicked slots (old behaviour):
let results: Vec<Result<R, E>> = pool.fan_out_async(items, f)
    .await
    .into_iter()
    .flatten()
    .collect();

// Or destructure explicitly:
for (i, slot) in pool.fan_out_async(items, f).await.into_iter().enumerate() {
    match slot {
        Some(Ok(r)) => { /* success */ }
        Some(Err(e)) => { /* task returned Err */ }
        None => tracing::error!(idx = i, "task panicked"),
    }
}
```

### `DbConnection.password` type

| Old | `String` |
| New | `SensitiveString` |

Construction via `DbConnection { password: "x".into(), ... }` keeps
working (`From<&str> for SensitiveString` exists). Sites that clone
the plaintext need `.expose()`:

```rust
// Before:
let url = format!("postgres://{}:{}@{}/{}", c.user, c.password, c.host, c.db);
// After:
let url = format!("postgres://{}:{}@{}/{}", c.user, c.password.expose(), c.host, c.db);
```

In-crate URL builders already do this. The change exists so `Debug` + `serde` round-trips redact by default.

### `Cache::set` signature

| Old | `fn set(&self, ...) -> ()` |
| New | `fn set(&self, ...) -> Result<(), serde_json::Error>` |

The old form swallowed serialise failures via `Err(_) => return`.
Callers now propagate or `.expect("cache set")` if a panic is the
right escalation:

```rust
cache.set(&key, &value, source).expect("cache set");
// or
cache.set(&key, &value, source)?;
```

### `expose_during` (additive)

New crate-root helper `scalo::expose_during<F, R>(f: F) -> R`
flips a thread-local flag so `SensitiveString` serialises its real
value inside the closure. Wrap any figment / serde round-trip that
must preserve secrets:

```rust
let cfg: Config = expose_during(|| {
    Figment::from(Serialized::defaults(&defaults))
        .merge(Env::prefixed("MYAPP_"))
        .extract()
})?;
```

Required for any consumer using `Figment::from(Serialized::defaults(&Config))`
where `Config` contains `SensitiveString` fields. Symptom of missing
this: secrets land as literal `***REDACTED***` post round-trip and
auth fails.

### `MemoryGuard` reads the cgroup, not a reservation counter (behaviour)

`MemoryGuard::current_bytes()` now returns what the kernel charges this
process -- cgroup v2 `memory.current`, then cgroup v1
`memory.usage_in_bytes`, then `/proc/self/status` `VmRSS`. It used to
return the sum of outstanding `try_reserve`/`add_bytes` reservations
unless the app registered a heap source, which no consumer did, so the
pressure ratio sat near zero while a process held hundreds of MiB and
neither the inbound brake nor the `dfe_scaling_pressure` hard gate ever
engaged.

No consumer code change is required to get the fix. Two things to know:

- **`current_bytes()` no longer starts at zero.** A fresh guard reports
  the process's real usage. Anything asserting `current_bytes() == 0`
  after a release, or treating it as a lease balance, wants the new
  `reserved_bytes()` instead -- that is the `add_bytes` less `release`
  counter, unchanged.
- **`try_reserve(n)` is a projected-admission check** (`usage() + n <=
  limit`) and no longer mutates the reservation counter. Pair it with
  `release` only if you also read `reserved_bytes()`; the kernel
  uncharges freed bytes on its own.

New: `MemoryGuard::reserved_bytes()`, `MemoryGuard::usage_source()`,
`MemoryGuard::with_usage_source(config, source)` and the `UsageSource`
enum. The guard logs which source it resolved at init, and warns when it
resolved to `Reservations` (no kernel accounting readable -- non-Linux).

### `memory::set_heap_source` -- allocator override (additive, opt-in)

Crate hook `scalo::memory::set_heap_source(fn() -> usize)` overrides the
detected `UsageSource` with an allocator statistic. It is now rarely
what you want: it cannot see thread stacks, mmap'd buffers, or pages the
allocator retains after a free, whereas the cgroup default is the number
the OOM killer acts on. Register one only where the allocator figure is
the one you mean to gate on.

`tikv_jemalloc_ctl::stats` needs that crate's `stats` feature, which is off by default: `tikv-jemalloc-ctl = { version = "...", features = ["stats"] }`.

```rust
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    let registered = scalo::memory::set_heap_source(|| {
        tikv_jemalloc_ctl::epoch::advance().ok();
        tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0)
    });
    assert!(registered, "a heap source was already registered");
    // ... ServiceRuntime / MemoryGuard built afterwards pick it up ...
}
```

scalo intentionally takes **no allocator dependency** (the global
allocator is the binary's choice, and scalo is `#![forbid(unsafe_code)]`).

### `SinkDrain::flush_durable` (additive, default no-op)

New trait method; existing impls compile unchanged. A drain with a durability step overrides it. The DLQ drain does, running each backend's durable flush -- see [Kafka DLQ `flush()` waits for broker acks](#kafka-dlq-flush-waits-for-broker-acks-behaviour-change).

### `Dlq` struct (internal layout)

Gained a `cancel: CancellationToken` field. Consumers construct via
`Dlq::spawn` or `Dlq::disabled` and don't touch the struct fields --
no visible change.

### `TransportFilterTierConfig.budget`

New optional field defaulting to permissive values
(`max_ast_nodes=200`, `max_iteration_depth=2`,
`max_payload_bytes=1MiB`). YAML configs without the field continue
to deserialise. To tighten:

```yaml
transport:
  filter_tiers:
    budget:
      max_ast_nodes: 100
      max_iteration_depth: 1
      max_payload_bytes: 524288   # 512 KiB
```

### `StrMatcherSet` (no surface change)

Internal partition into merged-AC + individual matchers. Public API
unchanged (`is_match`, `find`, `find_iter`, `earliest_match`,
`tier_counts`, `len`, `is_empty`).

### `HttpServerConfig.max_connections`

Now actually wired (was silently inert). Default 10,000. Consumers
that set `max_connections: 1` to "disable" filtering will now hit
a hard cap; raise to a realistic number or document the throttling
intent.

### Version check

`version-check` is now opt-in: set `version_check.enabled: true` and a
`version_check.api_url` via config. There is no default endpoint, and the
old opt-out env var is removed (gate via `version_check.enabled`).
`CheckPayload` no longer includes `instance_id` or `deployment`.

### Wave 1 -- Tier-3 single-knob

`transport.filter_tiers.allow_complex_filters_in/out: true` now
implies `expression.allow_regex / allow_iteration / allow_time =
true` for the transport's compile path. Previously operators had
to flip both knobs and they could disagree (filter passes the
transport gate, fails the expression profile). One source of truth.

### Wave 1 -- `WorkerPoolConfig::validate`

`async_concurrency == 0` now rejected at config-load. Previously
passed validation and panicked at `step_by(0)` inside
`fan_out_async`.

### Wave 2 -- `BackgroundSink::flush()` surfaces drain errors

`flush()` now returns `Err(SinkError::Drain(_))` when the underlying
drain's `write_batch` or `flush_durable` failed. Previously acked
`Ok(())` regardless -- callers thought messages were durable when
they were lost. Caller adjustment: handle `Err(SinkError::Drain)`
on `flush().await`.

### Wave 2 -- Kafka DLQ `flush_durable` Err on outstanding

The Kafka backend's durable flush returns `DlqError::Kafka` when its wait for acks expires with messages still in flight, where it used to log at debug and return `Ok(())`. Nothing called it until the DLQ drain began to, so `Dlq::flush()` never saw that error -- see [Kafka DLQ `flush()` waits for broker acks](#kafka-dlq-flush-waits-for-broker-acks-behaviour-change).

### Wave 3 -- `CacheConfig.dir_mode` / `.file_mode`

Two new optional fields default to `Some(0o700)` and `Some(0o600)`.
`None` disables chmod entirely -- required on S3-FUSE / root-
squashed NFS / similar mounts that reject chmod. Operators on
those mounts must own upstream perms.

### Wave 3 -- `dangerous-diagnostics` feature

`config::registry::dump_effective_unredacted()` is now gated by
the `dangerous-diagnostics` cargo feature. Not included in `full`.
Compile with `--features dangerous-diagnostics` only for one-off
operator-driven debugging.

### Wave 3 -- strict CEL `has(<single>)`

Tier-1 `has(<single-field>)` now only matches at JSON depth 1
(immediate child of the root). Previously matched at any depth.
Operators relying on the nested-match behaviour must switch to a
dotted path (`has(some.path.field)`) or to a Tier-2 CEL filter.

### Wave 5 -- bounded metric labels (F7)

`ServiceMetrics` methods that took free-form `&str` for metric labels
now take typed enums. The labels are bounded; cardinality is
fixed at the enum variant count.

| Method | Old | New |
| --- | --- | --- |
| `transport_sent` | `(transport: &str, count)` | `(transport: TransportKind, count)` |
| `transport_send_errors` | `(transport: &str, count)` | `(transport: TransportKind, count)` |
| `auth_failure` | `(reason: &str)` | `(reason: AuthFailureReason)` |
| `validation_failure` | `(reason: &str)` | `(reason: ValidationFailureReason)` |
| `BufferGroup::record_flush` | `(duration, trigger: &str)` | `(duration, trigger: FlushTrigger)` |

```rust
// Before
dfe.auth_failure("token-expired");
dfe.record_flush(0.012, "size");
dfe.transport_sent("kafka", 1);

// After
use scalo::metrics::{AuthFailureReason, FlushTrigger, TransportKind};
dfe.auth_failure(AuthFailureReason::Expired);
dfe.record_flush(0.012, FlushTrigger::Size);
dfe.transport_sent(TransportKind::Kafka, 1);
```

Variant lists:

- `TransportKind`: `Kafka`, `Grpc`, `Memory`, `File`, `Pipe`, `Http`, `Redis`, `Routed`
- `FlushTrigger`: `Size`, `Records`, `Age`, `Eviction`, `Shutdown`, `Manual`
- `AuthFailureReason`: RFC 6749 codes + JWT failure modes
  (`Expired`, `InvalidSignature`, `InvalidClient`, `InvalidGrant`,
  `InvalidScope`, `MalformedToken`, `RevokedToken`, `RateLimited`,
  `Unauthorized`, `AccessDenied`)
- `ValidationFailureReason`: JSON Schema 2020-12 categories
  (`SchemaInvalid`, `FieldMissing`, `TypeMismatch`, `OutOfRange`,
  `PatternMismatch`, `FormatInvalid`, `EnumViolation`,
  `AdditionalProperties`, `NullValue`, `EncodingError`)

No `Other` catch-all. New failure modes require a scalo release
that adds a variant; consumers then bump and recompile. The compiler
flags every site needing the new variant.

### Wave 5 -- `RoutedSender` metric label

`dfe_transport_sent_total{transport="routed",route=...}` now
carries the **configured route name** (or `"default"` for the
fallback), not the inbound message key. Cardinality is bounded by
the routing table size. No consumer code change required -- only
the metric label values change. Dashboards keyed on per-message
keys need rewiring.

### `vault:` path -- the first segment is the mount (BEHAVIOUR CHANGE)

`OpenBaoProvider` now reads the first segment of a vault path as the KV
mount, with the KV v2 `data` segment optional. `vault:secret/x:key` used
to request `secret/data/secret/x`, so the obvious spelling read the wrong
path, and a non-default mount could only be named by writing `data`
yourself.

| Spec | Old | New |
| --- | --- | --- |
| `vault:secret/data/myapp/tls:k` | mount `secret`, path `myapp/tls` | unchanged |
| `vault:kv/data/myapp/tls:k` | mount `kv`, path `myapp/tls` | unchanged |
| `vault:kv/myapp/tls:k` | mount `secret`, path `kv/myapp/tls` | mount `kv`, path `myapp/tls` |
| `vault:myapp:k` | mount `secret`, path `myapp` | unchanged |

**Consumer adjustment** -- ANY path whose second segment is not `data`
changes meaning, at any length: its first segment used to be the first
segment of a path under `secret` and is now the mount. `myapp/tls` was
`secret`/`myapp/tls` and is now `myapp`/`tls`; `kv/dfe-test/runzero` was
`secret`/`kv/dfe-test/runzero` and is now `kv`/`dfe-test/runzero`.
Anything relying on the old reading now asks for a path that does not
exist, so write the mount you mean (`secret/myapp/tls`).

Both surfaces move together, because one parser serves both: `vault:`,
`bao:` and `openbao:` credential specs, AND
`secrets.sources.<name>.path` in mounted YAML. A leading or trailing
`/` is now refused outright rather than reaching OpenBao as a request
that can only 404.

### `secrets.aws.region` is optional (BEHAVIOUR CHANGE)

`AwsConfig.region` is `Option<String>`, defaulting to `None`, and the
client sets a region only when one is configured. An unpinned region
was previously forced to `us-east-1`, which overrode the AWS SDK's own
chain -- the active profile and IMDS included -- so a pod with no
`AWS_REGION` read secrets from the wrong region.

**Consumer adjustment** -- code constructing `AwsConfig` literally now
writes `region: Some("ap-southeast-2".into())`; `AwsConfig::with_region`
and `for_localstack` are unchanged. Anything that relied on the
`us-east-1` default must name the region it means, in config
(`secrets.aws.region`) or in `AWS_REGION` / `AWS_DEFAULT_REGION`.

### Internal Kafka group ids derive from the app's config (BEHAVIOUR CHANGE)

The `KafkaAdmin` offset-query consumer used the fixed group id `__hs_admin_internal`, and a producer-only `KafkaTransport` used `__scalo_producer_only`. A broker granting groups by prefix refused both, logging `GroupAuthorizationFailed` on every start. They are now `<group>-admin` (or `<client_id>-admin`) and `<client_id>-producer-only`. See [kafka-path.md](kafka-path.md), "Internal consumer groups and broker ACLs".

Neither consumer ever joined its group or committed an offset, so no offsets are stranded under the old names. A transport with an empty `group` now subscribes to nothing even when `topics` is set; before, it joined `__scalo_producer_only` on those topics.

**Consumer adjustment** -- none in code. A deployment that granted the two literal ids by name, rather than by the app's group prefix, needs its grant to cover the new names.

### `flush()` reports refused tick, size and shutdown writes (BEHAVIOUR CHANGE)

`BackgroundSink::flush()`, and `Dlq::flush()` built on it, now return `Err` when any batch written since the previous flush was refused. That covers batches written on the `flush_interval` tick, on a full `batch_size`, and during the shutdown drain. Before, a flush reported only the batch the barrier wrote itself, so those losses showed in `dropped()` alone. See [pipeline/dlq.md](pipeline/dlq.md#queue-admission-semantics).

A refusal is reported once: the first flush after it returns the error and the next starts clean. `dropped()` counts as before. For the Kafka DLQ backend, what `Ok` means changes as well -- see the next entry.

**Consumer adjustment** -- a `flush().await?` that used to pass over a lost batch now returns `Err(DlqError::File)`, or `Err(SinkError::Drain)` on a `BackgroundSink` used directly. Read it as "entries written since the last flush were lost". A `dropped()` check around the barrier can stay: it also counts queue overflow, which `flush()` does not cover.

### Kafka DLQ `flush()` waits for broker acks (BEHAVIOUR CHANGE)

`Dlq::flush()` over the Kafka backend now returns once the broker has acknowledged every entry the barrier covers, waiting up to 30 s on tokio's blocking pool. Before, `Ok` meant queued to the producer: the drain never ran a backend's durable flush, and a delivery the broker refused reached neither `flush()` nor `dropped()`. See [pipeline/dlq.md](pipeline/dlq.md#the-kafka-barrier).

- In `KafkaOnly`, `FanOut`, or `Cascade` with nothing after Kafka, a delivery the broker refused since the previous flush fails the flush with `Err(DlqError::File(..))` and is counted in `dropped()` and `dlq_dropped_total{reason="backends_failed"}`, like a refused write. In `Cascade` with a backend after Kafka it goes to that backend instead -- see [Cascade hands a failed Kafka delivery to the next backend](#cascade-hands-a-failed-kafka-delivery-to-the-next-backend-behaviour-change).
- Entries still unacknowledged after 30 s are purged from the producer and handled the same way. The purge adds up to 5 s. An entry in flight to a stalled broker at the purge can still be written, so under a stalled broker `dropped()` is an upper bound and re-placing entries reported lost can duplicate them on the DLQ topic. It never under-reports.
- `Cascade`: when Kafka queues part of a batch and refuses the rest, only the rest goes to the next backend. Before, the whole batch did, so the file held a second copy of the part Kafka took.
- `FanOut`: a Kafka loss counts only for entries no other backend holds.

**Consumer adjustment** -- none in code. A flush over the Kafka backend can now take up to 35 s while the broker is slow or down; a timeout around it shorter than that sees its own timeout, and the failure goes to the next flush. A caller that never calls `flush()` sees Kafka delivery failures in `transport_send_errors_total{transport="kafka"}` only, not in `dropped()`.

The 30 s wait and the `File` variant changed after v2.12.10 -- see [`kafka.send_timeout_ms` is the Kafka ack wait](#kafkasend_timeout_ms-is-the-kafka-ack-wait-behaviour-change) and [A Kafka DLQ loss returns `DlqError::Kafka`](#a-kafka-dlq-loss-returns-dlqerrorkafka-behaviour-change).

### `NdjsonWriter` refuses a write a rotation would panic on (BEHAVIOUR CHANGE)

`file-rotate` 0.8 panics inside a rotation when the output directory is gone and cannot be recreated, or the current file is missing and cannot be created. Services built with `panic = "abort"` died at the next rotation boundary. `NdjsonWriter`, and the DLQ file backend and file output sink built on it, now refuse such a write with an `Err` before calling into `file-rotate`.

- A write while the current file is missing returns `Err` and schedules a reopen, which runs once the directory is usable again. It never recreates a missing directory, at a rotation boundary included.
- `NdjsonWriter::new`, and a reopen, refuse a directory that is not a directory or cannot be listed.
- `NdjsonWriter::new` refuses a filename with no final component (`..`) with `ErrorKind::InvalidInput`.
- `max_age_days` is capped at 1,000,000 days; above about 95 million the rotation's age check panicked.

**Consumer adjustment** -- none in code. A write the writer would previously have lost silently or panicked on is now an `Err`.

### `kafka.send_timeout_ms` is the Kafka ack wait (BEHAVIOUR CHANGE)

The key was parsed and used nowhere. It is now how long a `flush()` or the shutdown waits for the broker's acks, replacing the fixed 30 s v2.12.10 shipped. The default stays 5000, so the default wait is 5 s. `0` skips the ack wait, but the purge after it still waits up to 5 s for the purged entries' delivery reports. See [pipeline/dlq.md](pipeline/dlq.md#the-kafka-barrier).

**Consumer adjustment** -- a flush over the Kafka backend now takes up to `send_timeout_ms` plus 5 s while the broker is slow or down, 10 s at the default. A consumer that exposes `KafkaDlqConfig` to its operators can set `send_timeout_ms: 30000` to keep the 30 s wait; one that builds it from `KafkaDlqConfig::default()` gets 5 s until it exposes the field.

### A Kafka DLQ loss returns `DlqError::Kafka` (BEHAVIOUR CHANGE)

In v2.12.10 a Kafka loss found by `flush()` came back as `Err(DlqError::File("backend: kafka DLQ error: .."))`. It is `Err(DlqError::Kafka(..))` now. A batch every backend refused, whichever backends they were, is still `DlqError::File`.

**Consumer adjustment** -- a caller matching the flush error on `DlqError::File` to spot a lost dead letter matches `DlqError::Kafka` as well.

### `Dlq` shutdown waits for Kafka acks and counts what was lost (BEHAVIOUR CHANGE)

Dropping the Kafka producer discards what it still holds, queued or in flight. The drain used to exit without waiting, so every shutdown threw away the entries the broker had not acked yet, and counted none of them. It now waits for the acks the way a `flush()` does before it exits. Entries only Kafka held that the broker refused or never acked go to the next backend in `Cascade` with a backend after Kafka, and are counted in `dropped()` and `dlq_dropped_total{reason="backends_failed"}` otherwise. See [pipeline/dlq.md](pipeline/dlq.md#shutdown).

**Consumer adjustment** -- none in code. `shutdown()`, or the drain's exit on the cancelled token, can take `kafka.send_timeout_ms` plus 5 s while the broker is down; fit that inside the pod's termination grace period. `shutdown()` still returns `Ok`; read `dropped()` after it, or call `flush()` first for the loss as an `Err`. The count lands only if the drain finishes: a runtime that shuts down under it drops it mid-wait. A caller that never calls `flush()` now sees Kafka delivery failures in `dropped()` once the DLQ has shut down.

### Cascade hands a failed Kafka delivery to the next backend (BEHAVIOUR CHANGE)

In `Cascade` with a backend after Kafka, the default, an entry the producer queued but the broker refused or never acked is no longer counted lost. The DLQ producer keeps its payload, and the drain offers it to the next backend at the next `flush()` or shutdown, which purge what is unacked, or at the next write once librdkafka has failed the delivery (`message.timeout.ms`, 300 s unless set). Before, an unreachable broker lost every such entry with the file backend sitting behind it. See [pipeline/dlq.md](pipeline/dlq.md#modes).

- `dlq_cascade_fallthrough_total{from, to, reason}` counts each entry that falls through and lands. `reason` is `write_refused`, `delivery_failed` or `ack_timeout`.
- `dropped()` and `dlq_dropped_total{reason="backends_failed"}` move when the next backend refuses it too, and for an entry purged at shutdown whose delivery report does not arrive within 5 s, because its payload only comes with the report.
- A purged entry the broker was still writing can land on the topic and in the file. A duplicate, never a gap.
- `KafkaOnly`, `FanOut` and `Cascade` with nothing after Kafka are unchanged.

**Consumer adjustment** -- none in code. A `flush()` in cascade mode that returned `Err(DlqError::Kafka(..))` over an unreachable broker now returns `Ok` once the next backend holds the entries. Alert on `dlq_cascade_fallthrough_total{reason="ack_timeout", to="file"}` for a broker outage the file backend is covering.

### `NdjsonWriter` refuses a write that reached no file (BEHAVIOUR CHANGE)

`file-rotate` reports `Ok` and discards the bytes when it holds no open file, for instance when the current file exists but cannot be opened for writing. The writer checked only that the file existed, so such a write counted as written. It now checks that the file took the bytes, and returns `Err` with `ErrorKind::WriteZero` when it did not. The DLQ file backend counts the entries lost and schedules a reopen, which succeeds once the file is writable again.

**Consumer adjustment** -- none in code. A write that was lost silently is now an `Err`.

### `KafkaProducer::flush` counts messages only (BEHAVIOUR CHANGE)

`flush` returned librdkafka's out-queue length, which also counts the statistics, error and log events the client has still to serve, so it could report messages outstanding after every message had its delivery report. It now returns the messages sent with no delivery report yet. A failed delivery has its report, so it is not counted; `delivery_failures()` counts those.

**Consumer adjustment** -- none in code. `in_flight_count()` and `ProducerMetrics::in_flight` still return the out-queue length.

### `BatchEngine` run loops drain the source at shutdown (BEHAVIOUR CHANGE)

`run_governed`, `run_workbatch`, `run_workbatch_parsed` and `run_workbatch_streaming` returned as soon as the shutdown token was cancelled, leaving the source open. A push source (gRPC, HTTP) acknowledges a record once it is queued, so what it held was lost. They now close the source and run what it still returns through `process`, the sink and the commit until `recv` reports `Closed`. See [pipeline/batch-engine.md](pipeline/batch-engine.md#shutdown).

- The source is closed when the method returns. A second `close()` is harmless on every scalo transport.
- A block the sink refuses transiently (`Backpressure`, `Timeout`) is retried for up to 10 s after the loop sees shutdown, whether it was refused before shutdown or during the drain. Before, retries stopped the moment the token fired, so one busy moment in the sink at shutdown lost everything the source still held.
- A block the sink still refuses after those 10 s stops the drain, uncommitted; one refused since before the drain began closes the source without a drain. A permanent sink error stops it at once and is returned. A source that returns nothing for 5 s without reporting `Closed` stops it too.
- A sink that stays busy therefore holds shutdown for up to 10 s longer than before.
- A Kafka source reports `Closed` at once, so the drain reads nothing from it. A Kafka commit made after the method returns gets one attempt: the transport stops retrying commits once closed.

**Consumer adjustment** -- a service whose sink is the same transport instance as its source builds them separately, since the engine closes the source before the service's final flush. A service that runs a loop again on the same receiver after cancelling a child token builds a new receiver: the first run closed it. A `TransportReceiver` implemented outside scalo returns what it already acknowledged from `recv` after `close()`, then `Closed`, without waiting for new records.

### HTTP server keeps what it acknowledged at `close()` (BEHAVIOUR CHANGE)

The HTTP receive server answers 200 once a record is queued for `recv`. `close()` left the queue open, so a request in flight still got 200, and the next `recv` returned `Closed` with records still queued. See [transport/backends.md](transport/backends.md#http).

- `close()` answers every POST from then on with 503 and `Retry-After: 1`, which a sender retries. `recv` returns the records already queued, then `Closed`.
- `close()` stops the server, so the listener is free when it returns. Open connections finish their in-flight requests.
- A POST refused because the receiver is closed is 503, not 410, and still counts in `transport_refused_total{transport="http"}`.
- The server no longer counts receipts in `transport_sent_total{transport="http"}`. They were counted as sends and as receipts both.

**Consumer adjustment** -- a service that receives over HTTP without a `BatchEngine` run loop shuts down with `close()`, then `recv` until `Closed`, then its final flush. A dashboard that read the server's `transport_sent_total{transport="http"}` as its intake reads `transport_received_events_total{transport="http"}` instead.

### Memory transport keeps what `send` accepted at `close()` (BEHAVIOUR CHANGE)

`recv` after `close()` returned `Closed` with records still queued. It now returns them, then `Closed`, as the HTTP server does.

**Consumer adjustment** -- none.

### Vector-compat `PushEvents` is queued whole or not at all (BEHAVIOUR CHANGE)

The Vector-compat source queued a request's events one at a time, so a request the receiver closed under part-way had its first events queued, and Vector's retry of the whole request delivered them twice. It now converts every event, then reserves room for all of them before queueing any. A refusal because the receiver closed is `Unavailable` with the message `receiver closed`, not `receiver buffer full`, and queues nothing. A request with more events than `recv_buffer_size` is still queued one event at a time. See [transport/backends.md](transport/backends.md#transport-grpc-vector-compat).

**Consumer adjustment** -- none.

### `VectorCompatClient` dials and health checks end at a limit (BEHAVIOUR CHANGE)

The client had no connect timeout, so a dial to a peer that never completed the TCP connect held the caller until the operating system gave up, and every later call queued behind it. A dial whose DNS lookup or TCP connect is unfinished at nine tenths of the gRPC transport's default `send_timeout_ms` (30 s) is now abandoned: the call that started it returns its usual error (`TransportError::Send` from `send_events`, `TransportError::Connection` from `health_check`) and the next call dials afresh. `health_check` is a short probe, so it also gives up at the full 30 s once connected.

`send_events` has no limit once connected. A Vector source with end-to-end acknowledgements holds `PushEvents` open until its own sink has delivered the events, and a sender that cut the RPC off and retried would push the same events again while the first push may still land. A send to a peer that stays connected but never answers waits for as long as the connection stays open.

**Consumer adjustment** -- none in code.

### Transport metric manifest lists every label the transports emit (fix)

The manifest listed only `transport` for every transport series. `transport_sent_total` also carries `path` (gRPC `RouteBatch` sends), and `transport_backpressured_total` carries `reason` (pressure sheds).

**Consumer adjustment** -- none in code. A dashboard or alert generated from the manifest can group by the new keys.

### `log_debounced` lets one concurrent caller through per window (behaviour)

`logger::log_debounced` read the last timestamp and then stored the new one, so callers racing into an open window could all log. It claims the window with a compare-exchange, so exactly one of them does.

**Consumer adjustment** -- none.

### gRPC `close()` keeps what the server acknowledged (BEHAVIOUR CHANGE)

The gRPC server acknowledges a record once it is queued for `recv`. `close()` used to make the next `recv` return `Closed` with records still queued, and kept acknowledging pushes until the serve task noticed the shutdown, so a sender saw `Ok` for records nothing delivered. See [transport/backends.md](transport/backends.md#grpc).

- `close()` refuses every push from then on with `Unavailable`, which a sender retries. `recv` returns the records already queued, then `Closed`.
- `close()` and dropping the transport both stop the server, so the listener is free when `close()` returns. The serve task used to outlive the transport for as long as a client held an RPC open. Open connections still finish their in-flight RPCs.
- `send` and `send_batch` give up at `send_timeout_ms` end to end. The `grpc-timeout` header did not cover DNS, the TCP connect or the TLS handshake, so a send to a receiver that was down could outlive the limit. A dial whose DNS lookup, TCP connect or TLS handshake has not finished by nine tenths of the limit is now abandoned, so the next send dials afresh. Before, only the TCP connect was bounded, and every later send queued behind a TLS handshake the server never answered.
- The server no longer counts receipts in `transport_sent_total{transport="grpc"}`. They were counted as sends and as receipts both.

**Consumer adjustment** -- a service that receives over gRPC shuts down with `close()`, then `recv` until `Closed`, then its final flush. One that stops calling `recv` and closes after its flush still loses what was queued. A dashboard that read the server's `transport_sent_total{transport="grpc"}` as its intake reads `transport_received_events_total{transport="grpc"}` instead.

### gRPC `RouteBatch` larger than `recv_buffer_size` lands whole (BEHAVIOUR CHANGE)

The server reserves queue room for a whole batch before it queues any of it, and the bounded queue refuses a reservation larger than its capacity every time. A batch with more records than the receiver's `recv_buffer_size` was answered `ResourceExhausted` on every try, and the sender retried it forever. Such a batch is now held whole in a slot beside the queue, one batch at a time: it lands in one step, or is refused with nothing queued while an earlier one is still waiting. `recv` hands it over before the queue. See [transport/backends.md](transport/backends.md#grpc).

**Consumer adjustment** -- none. A receiver now holds up to `recv_buffer_size` records plus one such batch.

### gRPC `Cancelled` is backpressure (BEHAVIOUR CHANGE)

A server that cuts an RPC at its deadline answers `Cancelled`, as tonic's server does at the `grpc-timeout` a scalo sender sets. `send` and `send_batch` returned `Fatal` for it, which stops a `BatchEngine` run loop over a slow receiver. They now return `Backpressured`, and the record is retried.

**Consumer adjustment** -- none.

### gRPC clients drop a connection whose peer stops answering (BEHAVIOUR CHANGE)

Neither `GrpcTransport` nor `VectorCompatClient` sent HTTP/2 PINGs, so a peer that stayed connected but stopped answering kept the connection, and every later send rode it. Both now PING a connection that has read nothing for `send_timeout_ms`, and close it when the PING goes unanswered for as long again, so the next send dials afresh. The limit is 30 s for `VectorCompatClient`, and for a `GrpcTransport` with `send_timeout_ms: 0`. A `VectorCompatClient::send_events` on such a connection now returns an error instead of waiting while the connection stays open. See [transport/backends.md](transport/backends.md#grpc).

**Consumer adjustment** -- none.

### Vector-compat `PushEvents` follows the pressure governor and counts receipts (BEHAVIOUR CHANGE)

A Vector-compat push skipped the pressure governor and counted in no `transport_received_*` series. With `GrpcTransport::with_pressure`, a `PushEvents` is now refused with `Unavailable` while the governor holds intake, as a native push is, and the events it queues count in `transport_received_events_total{transport="grpc"}` and `transport_received_bytes_total{transport="grpc"}`, the bytes being the JSON queued for `recv`. A push refused because the receiver closed counts in `transport_refused_total{transport="grpc"}`.

**Consumer adjustment** -- none in code. A dashboard on the gRPC receive counters now includes Vector-compat traffic.

### `RoutedSender` no longer counts sends of its own (BEHAVIOUR CHANGE)

`RoutedSender` counted every record in `transport_sent_total{transport="routed",route=...}` and `transport_sent_bytes_total{transport="routed",route=...}` before handing it on, and the transport it handed it to counted the record again once it landed. `sum(transport_sent_total)` doubled routed traffic, and a record the route refused counted as sent. The routed series are gone: each record counts once, in the series of the transport that sent it, and only once it has landed. The manifest drops the `route` label from both.

**Consumer adjustment** -- none in code. A dashboard that read `transport="routed"` or grouped by `route` reads the sending transport's series instead.

### App info is emitted once (BEHAVIOUR CHANGE)

The service runtime builds the app metric set, with the `info` gauge, and a service that built `AppMetrics` itself as well added a second `info` series with its own `version` and `commit`. `AppMetrics::new` now emits `info` and records the build info only for the first set built on a `MetricsManager`, which is the runtime's, so a scrape carries one `info` series. The runtime's `commit` is the one the app names in `VersionInfo::with_commit`, else the build's `GIT_COMMIT`, else `unknown`.

`worker_pool_scale_events_total` is described with its `direction` label and no longer also emitted as an unlabelled series stuck at 0.

**Consumer adjustment** -- a service that wants its commit in `info` names it in `ServiceApp::version_info` with `with_commit`, or builds with `GIT_COMMIT` set. Its own `AppMetrics::new` call can stay; it no longer changes `info`.

### Source acknowledgements held until delivery (additive, opt-in per run loop)

`BatchEngine::pipeline(&receiver)...run(process, sink)` is a new run loop that holds each block's source acknowledgement until every piece built from the block has reported, then releases it once through the new `TransportReceiver::release`. A Kafka source's commit waits for the sink, and an `Errored` block is never committed past. The key is `acknowledgements.enabled` on the transport's own section (default `true`), read by `AnyReceiver::from_config` from `<key>.kafka.acknowledgements` and set on an explicit transport with `KafkaTransport::with_acknowledgements`. `BatchEngine::with_dlq` makes a dead letter a piece that releases its source only once the DLQ confirms the write. See [pipeline/acknowledgements.md](pipeline/acknowledgements.md).

The new trait methods are provided, so no implementor changes: `TransportReceiver::{ack_control, release, hold_deadline}`, `TransportSender::{confirms_delivery, dead_letter_reason}`. `run_governed` and the other run loops behave as before, except that a push source with acknowledgements on that they run logs one WARN at start, since it still answers its senders at enqueue, and reports `pipeline_delivery_guarantee{guarantee="best_effort",reason="unarmed"}`.

An armed gRPC server answers a push only once its records are released. Build it armed, `GrpcTransport::builder(..).armed(true)` or `AnyReceiver::from_config_armed(key)`, so no push is answered before a pipeline runs. A pipeline with neither `.sender(&sender)` nor `.sink_confirms(..)` logs one WARN at start and reports `best_effort` / `sink_cannot_confirm`: nothing takes a record the sink's transport would dead-letter out of the block.

A `KafkaTransport` armed through `AckControl::arm` commits each partition only up to its lowest offset handed out and not yet released, for `commit` as for `release`. An unarmed one commits as before.

`StatsContext::total_position_lag` and `KafkaTransport::total_position_lag` count records past the consumer's read position, which a held commit does not inflate. `total_consumer_lag` still counts from the committed offset. While the inbound gate holds the assignment paused, both used to stop rising, since librdkafka fetches nothing it has paused and learns the log end only from fetches. The transport now asks the broker for the end once per statistics interval while paused, so both keep rising with what producers write.

**Consumer adjustment** -- none to keep today's behaviour. To hold acknowledgements, move from `run_governed` to `pipeline(..)`, call `.sender(&sender)` for a transport sink, and give the loop a DLQ with `with_dlq`. A hand-rolled loop arms the source before its first `recv` and releases each block through `SourceAck`.

### An `acknowledgements` section under an HTTP or file source warns (behaviour)

`AnyReceiver::from_config` and its armed and governed siblings ignored `<key>.http.acknowledgements` and `<key>.file.acknowledgements` without a word. Each now logs one WARN per process, naming the section and what the source does instead: HTTP answers 200 once a request is queued, and a file source saves the highest read position released, so neither holds its acknowledgement yet. Memory and pipe still warn that they have nothing to hold, now once each, where the two shared one warning.

**Consumer adjustment** -- none in code. The key does nothing under these sources: remove it. An HTTP endpoint that must answer only once its records are delivered is an app's own listener over `Tickets`. See [pipeline/acknowledgements.md](pipeline/acknowledgements.md#which-sources-hold-it).

### gRPC `send_batch` splits a block over `max_message_size` (BEHAVIOUR CHANGE)

2.12 returned `Fatal` for a block over `max_message_size` and sent none of it. `send_batch` now sends it as several requests, each within the limit. A record over the limit on its own no longer fails the block: it is left out, the rest is sent and the result is `Ok`, and the record is dropped, counted in `pipeline_dead_letters_dropped_total{reason="too_large"}`. A block of nothing but such records returns `FilteredDlq`, and its records count there too.

**Consumer adjustment** -- a caller that dead-letters oversize records takes out every record `dead_letter_reason` names before calling `send_batch`, as the pipeline's `.sender(&sender)` does. A caller that retried a `Fatal` block in smaller pieces no longer needs to.

### `Dlq::write_confirmed` and `BackgroundSink::write_confirmed` (additive)

`write_confirmed(entries)` writes one caller's entries as a batch of their own and reports that write to that caller alone. `flush()` keeps its contract: a refusal goes to the first barrier after it, whoever issued it. The pipeline's DLQ piece writes through `write_confirmed`, so one block's refusal never reaches another's. A custom `SinkDrain` may override the new provided `settled()` (default `true`) to fail a confirmed write whose fate it has not heard yet.

`Dlq::refusal(&entry)` names an entry no backend can ever hold: over a Kafka-only DLQ's `message.max.bytes` once base64-encoded. The pipeline drops such an entry, counts it in `pipeline_dead_letters_dropped_total{reason="too_large"}` and releases its piece `Dropped`. Any other failed DLQ write it retries with backoff while the block stays held, where it used to release the block `Errored`. A Kafka partition an `Errored` release pins is counted in the new `transport_ack_withheld` gauge.

**Consumer adjustment** -- a hand-rolled loop that writes dead letters with `write_confirmed` screens each entry with `refusal` first and releases the refused ones `Dropped`, then retries the write on any error but `DlqError::Closed` rather than releasing `Errored`. See [pipeline/dlq.md](pipeline/dlq.md#queue-admission-semantics).

### Kafka producers default to zstd at level 3 (BEHAVIOUR CHANGE)

Every sizing profile now produces with `compression.type=zstd` and `compression.level=3`, where it produced with `lz4`. The profiles still differ in batching and latency. The level is set only while the resolved codec is zstd and no raw map names one, and any other codec runs at librdkafka's own default level for it. See [kafka-path.md](kafka-path.md#wire-compression).

The producer profile constants `PRODUCER_HIGH_THROUGHPUT`, `PRODUCER_EXACTLY_ONCE` and `PRODUCER_LOW_LATENCY` no longer carry `linger.ms` or `compression.type`. Every producer path laid the sizing surface over them, so neither ever took effect there.

**Consumer adjustment** -- every image that runs a scalo producer or consumer needs a librdkafka built with zstd: without it the producer fails at creation, and a consumer cannot read the batches. A stage that should stay on lz4 sets `kafka.sizing.producer.compression_type: lz4`. Code that builds its own producer config from those constants gets librdkafka's own `linger.ms` and codec unless it builds through `producer_client_config`, which lays the sizing surface over them.

### `KafkaProducer` applies `librdkafka_overrides` last (BEHAVIOUR CHANGE)

`KafkaProducer::new` applied `kafka.librdkafka_overrides` before the sizing surface, so a codec, linger or batch key set there was overwritten by the sizing value. It now applies them after, as `KafkaTransport` does. The order on both paths is: profile constants, sizing profile, named sizing knobs, `sizing.producer_librdkafka`, then `librdkafka_overrides`. On every producer and consumer path, and on `KafkaAdmin`, a key also replaces the other librdkafka name for its property from the layers below it (`fetch.message.max.bytes` and `max.partition.fetch.bytes`, `request.required.acks` and `acks`, `compression.codec` and `compression.type`, among others), since rdkafka passes both names to librdkafka in hash order and the value that ran was chance.

**Consumer adjustment** -- a `KafkaProducer` whose `librdkafka_overrides` set a sizing key now runs that value. An override by librdkafka's other name for a property scalo sets, such as `fetch.message.max.bytes`, now always wins where it used to win at random. The Kafka DLQ backend builds on `KafkaProducer`, so its producer follows the same order. A service that builds its own producer config, to own the delivery context, builds it with the new public `transport::kafka::producer_client_config(&config, profile_defaults)`, passing its own keys as `profile_defaults`. Copying the order is not enough: moving `librdkafka_overrides` after `sizing.resolved_producer_map()` still leaves a property set under both librdkafka names to hash order.

### Kafka lag and assignment are published from the first assignment (BEHAVIOUR CHANGE)

`rdkafka_topic_partition_consumer_lag` and `total_consumer_lag` covered only partitions with a committed offset, since librdkafka reports `consumer_lag` only from a commit. A group that had never committed published no lag series and read as idle to a scaler on that lag, backlog or not. Once a `StatsContext` has served a rebalance, a partition with nothing committed counts from the read position: the application's, else librdkafka's fetch position, measured to the end its `isolation.level` reads to. The lag, `total_consumer_lag` and `total_position_lag` then count only the partitions the consumer holds, as its rebalances left them, and a revoked partition's lag series drops to 0. `consumer_partitions_assigned`, which nothing set before, follows the assignment. A context that has served no rebalance counts as before.

**Consumer adjustment** -- none in code. A service that calls `ConsumerMetrics::set_partitions_assigned` for a `KafkaTransport` consumer drops the call, since the transport sets the series for its group. A consumer built on `StatsContext` that takes partitions with `assign()` rather than `subscribe()` serves no rebalance, so it keeps the commit-only rule: lag for every partition with a committed offset, position lag from the application's or committed position, and no `consumer_partitions_assigned`.

### Kafka consumer series all have a writer, and a revoked partition's offsets drop (BEHAVIOUR CHANGE)

`ConsumerMetrics` registered `consumer_lag{topic,partition}` and `consumer_rebalance_total` and nothing set either, so every consumer's manifest listed two series that were always empty. The Kafka transport now fills both from the consumer it owns. `consumer_lag` carries each held partition's lag, the same values as `rdkafka_topic_partition_consumer_lag` and the ones `total_consumer_lag` sums. Only a context that has served a rebalance writes it, as with `consumer_partitions_assigned`. `consumer_rebalance_total` counts each revoke and each assignment, as librdkafka's `rebalance_cnt` does, so an eager rebalance counts 2. `consumer_poll_duration_seconds` stays the service's to record.

A revoked partition kept its last `rdkafka_topic_partition_committed_offset` for good, and stayed in `KafkaMetrics::partition_committed` and `partition_high_watermark`, since librdkafka keeps reporting a stopped partition's last committed offset and high watermark. The recorder cannot drop one series, so a revoke now sets that partition's committed offset and `consumer_lag` to 0 and takes it out of the snapshot. Once a context has served a rebalance, both maps cover only the partitions it holds.

**Consumer adjustment** -- a service that calls `ConsumerMetrics::set_lag` or `record_rebalance` for a scalo `KafkaTransport` consumer drops the call, since the transport writes both series for its group. A committed offset of 0 now also marks a partition the consumer no longer holds. Code that read `partition_committed` or `partition_high_watermark` for a partition the consumer does not hold gets nothing once the context has rebalanced. A context that has served no rebalance reports every partition as before.

### Kafka `rdkafka_` series name their client, and uncommitted lag follows `isolation.level` (BEHAVIOUR CHANGE)

Every `rdkafka_` gauge a `StatsContext` writes now carries `client_id` and `client_type` (`consumer` or `producer`), from librdkafka's statistics. A `KafkaTransport` builds a consumer and a producer on one `client_id`, and both wrote `rdkafka_global_msg_cnt`, `rdkafka_global_msg_size_bytes` and the `rdkafka_broker_*{broker}` series, so the last write won and the value flipped between them. Two consumers in one process reading one partition did the same with the per-partition series. Each client now has its own series, and a revoke zeroes the ones its own client published. No `rdkafka_` series is published before a context's first statistics callback, which names the client.

A held partition with nothing committed measured its lag to the high watermark, where librdkafka measures `consumer_lag` to the last stable offset under `read_committed`, its default. The two differ while a transaction is open upstream. The transport now reads `isolation.level` from the consumer's config and measures an uncommitted partition to the same end librdkafka uses for a committed one: the last stable offset under `read_committed`, the high watermark under `read_uncommitted`. `rdkafka_topic_partition_consumer_lag`, `consumer_lag`, `total_consumer_lag` and `total_position_lag` all follow.

**Consumer adjustment** -- none in code. A query that expected one `rdkafka_` series per name, broker or partition now sees one per client: aggregate over `client_id` and `client_type`, or group by them. Two transports in one process that share a `client_id` still share series, so give each its own. The `consumer_` series carry the consumer group instead, below.

### Kafka `consumer_` series carry the consumer group (BEHAVIOUR CHANGE)

`consumer_lag`, `consumer_partitions_assigned` and `consumer_rebalance_total` now carry `group_id`, the `group.id` a `KafkaTransport`'s consumer joins, `librdkafka_overrides` included. Two consumers in one process in different groups wrote one series of each: the last assignment count won, one group's revoke zeroed the other's `consumer_lag` on a partition both read, and the rebalance count summed the two. Each group now has its own series, and a revoke zeroes only its own group's lag. The group holds across a consumer rebuild and a restart, as `client.id` does, where librdkafka's handle name counts every client the process creates.

`ConsumerMetrics::new` describes the three with `group_id` in the manifest and registers no series for them. It used to register `consumer_partitions_assigned` and `consumer_rebalance_total` with no labels, which would now sit at 0 beside every group's series. Its setters and its `partitions_assigned` and `rebalance` handles still write the series with no `group_id`, from their first write. A `StatsContext` built with `new()` knows no group, and writes them without the label too.

**Consumer adjustment** -- none in code. A query on these series now sees one per group: aggregate over `group_id`, or group by it. No `consumer_partitions_assigned` or `consumer_rebalance_total` series exists before the consumer's first rebalance. Two transports in one process in the same group write one `consumer_partitions_assigned`, so the last write wins.

### Kafka partition leases, and a revoke reaches the caller (additive, BEHAVIOUR CHANGE)

A caller that buffers records before writing them wrote the records of a revoked partition anyway, and the partition's next owner read and wrote them again from the committed offset, so every rebalance under a live buffer duplicated what it moved. `KafkaTransport::lease(topic, partition)` now returns the `PartitionLease` the consumer holds a partition under, and `holds(topic, partition, lease)` says whether it still stands. A revoke or a consumer rebuild ends a lease, and a partition given back is under a new one, so a copy read before an eager revoke never passes for the copy read again. `discarded_after_revoke(n)` counts what the caller discards in the new `transport_revoke_discarded_total{transport="kafka",stage="buffer"}`. `PartitionLease` and the three methods are new public API.

`recv` leaves out a record read before a revoke of its partition in the same poll whether or not acknowledgements are armed, where it did so only armed. Handed out, a caller wrote a copy the next owner writes too. Each one counts in `transport_revoke_discarded_total{stage="receive"}`. The next owner reads it from the committed offset, or, where nothing is committed yet, from where `auto.offset.reset` points, so a consumer on `latest` loses such a record from a partition its group has never committed to. See [transport/backends.md](transport/backends.md#kafka).

**Consumer adjustment** -- none to keep today's behaviour. A caller that holds records across `recv` calls stores each record's lease beside its offset when `recv` returns it, discards the records `holds` rejects right before each write, and counts them with `discarded_after_revoke`. Unarmed, it leaves their offsets out of `commit`. Armed, it releases their tokens as for any record it drops.

### Kafka consumers fetch 1 MiB per partition (BEHAVIOUR CHANGE)

Every sizing profile sets `max.partition.fetch.bytes` to 1 MiB, the new public `transport::kafka::PARTITION_FETCH_BYTES`, where it set 16 MiB. librdkafka grows a fetch for a larger record until the whole record arrives, so a record up to `MESSAGE_MAX_BYTES` still comes through. What changes is memory: a fetch reply carries at most 1 MiB of compressed data per partition, and librdkafka decompresses all of it before the application reads a record. See [kafka-path.md](kafka-path.md#profile--getsend-tuning-table).

**Consumer adjustment** -- none. A stage that wants the larger fetch back sets `kafka.sizing.consumer.max_partition_fetch_bytes`.

### An expired hold keeps the gate open until records arrive (BEHAVIOUR CHANGE)

A hold that reached `self_regulation.max_hold_secs` resumed each `InboundGate` on the latch for one evaluation. A resumed Kafka consumer has to fetch before it returns anything, so the next receive paused it again and the window admitted nothing. The gate now stays open until `InboundGate::note_received` reports a receive that returned records, or 2 s pass, and still resumes once per expired hold. `KafkaTransport` reports every receive.

**Consumer adjustment** -- code that drives its own `InboundGate` calls `note_received(records)` after each receive. Without it each window stays open for 2 s.

### Smaller additions

- `RoutedSender` forwards `dead_letter_reason` to the route a record's key selects, and reports the weakest `confirms_delivery` across its routes, so `.sender(&routed)` screens and reports as the routes do.
- `VectorCompatClient::connect_lazy_within(endpoint, send_timeout_ms)` sets the dial, health-check and PING limit that `connect_lazy` fixes at 30 s.
- `VectorCompatClient::send_events_status` fails with the gRPC status, and `VectorCompatClient::is_permanent_rejection` names the refusals no resend clears (`DataLoss`, `InvalidArgument`, `OutOfRange`), so a transform stops resending events a Vector sink rejected. `send_events` is unchanged. One it drops counts in `pipeline_dead_letters_dropped_total` under `transport::DEAD_LETTER_REJECTED` (`reason="rejected"`).
- `Pipeline::listener(name)` publishes the pipeline's `pipeline_delivery_guarantee` with a `listener` label, for an app that runs several pipelines.
- `BatchEngine::pipeline` writes `self_regulation_recv_block_bytes` for each block it drives under the governor, as `run_governed` does. Nothing wrote it on the pipeline path.
- `EffectiveGuarantee::publish_for(listener)` publishes `pipeline_delivery_guarantee` with a `listener` label, for an app with one source and sink pair per listener.
- `BackgroundSink` counts a push its full queue refuses in `<prefix>_dropped_total{reason="overflow"}`, where the series had no label. For the DLQ that is `dlq_dropped_total`, whose other drops already carry `reason`, so the metric no longer mixes a labelled and an unlabelled series. A query that sums the metric is unchanged. One that matched the unlabelled series by exact labels now needs `reason="overflow"`.

### Kafka config is checked on every client constructor (BEHAVIOUR CHANGE)

`KafkaProducer` (every profile constructor), `KafkaAdmin::new`, `TopicResolver::new` and the Kafka DLQ now apply the `provider` preset and run `KafkaConfig::validate(is_production())` before building a client, as `KafkaTransport::new` already did. The PLAIN floor also matches the mechanism case-insensitively, so `plain` over a plaintext transport is refused like `PLAIN`. `providers::validate` trims both of its arguments, since librdkafka skips leading whitespace in them, so `" PLAIN"` over `sasl_plaintext` is refused like `PLAIN`, and `" SASL_SSL"` with a blank mechanism is refused for want of one.

`validate` also judges the raw librdkafka maps the client builders apply: `librdkafka_overrides`, `sizing.producer_librdkafka`, `sizing.consumer_librdkafka` and `extra_config`. Each value they give `security.protocol`, `sasl.mechanism` or `sasl.mechanisms` is held to the rules above, and a false `enable.ssl.certificate.verification` or an `ssl.endpoint.identification.algorithm` of `none` is refused in production like `ssl_skip_verify`. In production they may not give `sasl.oauthbearer.token.endpoint.url` anything but an `https://` URL, since `http://` or no scheme would send the OAUTHBEARER client secret in cleartext, or give `ssl.cipher.suites` a `NULL`, `eNULL` or `COMPLEMENTOFALL` suite that no leading `!` or `-` excludes, such as `eNULL`, `NULL-SHA` or `ECDHE-RSA-NULL-SHA`. Keys and values match in any case, a value is read past surrounding whitespace, and the refusal names the map and the key. The consumer runs the typed `security_protocol` over an override and the producer runs the override, so both are judged: an override to `ssl` does not lift a typed `plaintext`.

**Consumer adjustment** -- a config these paths used to accept unchecked can now fail construction with `TransportError::Config` (`DlqError::Kafka` for the DLQ):

- in any environment, SASL `PLAIN` (any case) without `security_protocol=sasl_ssl`, or an unknown `provider`;
- in production, `ssl_skip_verify`, or `plaintext` / `sasl_plaintext` without `allow_insecure_transport: true`;
- a config that got past either check through a raw map, such as `librdkafka_overrides: { security.protocol: sasl_plaintext }` beside a typed `sasl_ssl` and `PLAIN`, or `{ enable.ssl.certificate.verification: "false" }` in production, on every constructor including `KafkaTransport::new`. Set the typed field instead, or `allow_insecure_transport: true` for an audited plaintext transport;
- in production, a `sasl.oauthbearer.token.endpoint.url` that is not `https://` or a NULL suite in `ssl.cipher.suites`, from any raw map. Use an `https://` token endpoint, and drop the NULL suite or exclude it with `!eNULL`.

A `provider` preset that these paths silently ignored now takes effect. `producer_client_config` still applies neither step: a caller that builds a producer from it runs `apply_provider` and `validate` itself.

### App environment reads past whitespace and skips blank values (BEHAVIOUR CHANGE)

`get_app_env` trims `APP_ENV`, `ENVIRONMENT` and `ENV`, and treats one that is empty or only whitespace as unset. `APP_ENV=" production"` or `"production\n"` now resolves to `production`, so `is_production()` is true and every production refusal applies where it did not before. `APP_ENV=""` beside `ENVIRONMENT=production` now resolves to `production` rather than to an empty, non-production posture. With every variable blank or unset the posture is `development` and the one-shot warning fires.

**Consumer adjustment** -- a deployment that set one of these to a padded production value was running in development posture and now runs in production posture; check its Kafka, secrets and cache config against the production refusals above before it rolls.

### Contract names and defaults carry no organisation or product name

The names scalo writes into artefacts, and the defaults it falls back on, named one organisation and one product. They are now scalo's own or the app's, and an app that relied on the old value sets it explicitly. Once it does, every name and default below is what it was, except that the secret schema also carries `x-scalo-secret`.

| Old | New | To keep the old value |
| --- | --- | --- |
| Secret schema marker `x-dfe-secret` | `x-scalo-secret`, with `x-dfe-secret` still emitted beside it | Nothing: both are emitted. Move every reader to `x-scalo-secret`, tests that assert the marker included. scalo-py emits both in the same order and reads either -- see [reflectable-config-shape.md](reflectable-config-shape.md#secret-marker) |
| Label and annotation keys `io.hyperi.profile`, `io.hyperi.app`, `io.hyperi.metrics_port`, `io.hyperi.contract.*` | `io.scalo.*`, under the new `OciLabels::label_namespace` | `oci_labels.label_namespace: "io.hyperi".into()` |
| `pub const KEY_PREFIX` (`io.hyperi.contract`) | `pub const KEY_SEGMENT` (`contract`), under the label namespace | -- |
| `ContractIdentity::as_dockerfile_labels()`, `as_yaml_annotations(indent)` | take the namespace: `as_dockerfile_labels(ns)`, `as_yaml_annotations(ns, indent)` | pass `"io.hyperi"` |
| Vendor label default `HYPERI PTY LIMITED` | empty, and an empty vendor writes no label | `oci_labels.vendor: "HYPERI PTY LIMITED".into()` |
| Copyright default `(c) 2026 HYPERI PTY LIMITED`, written to the generated Dockerfile's `# Copyright:` header line | empty, and an empty copyright writes no `# Copyright:` line | `oci_labels.copyright: "(c) 2026 HYPERI PTY LIMITED".into()`, which writes the header exactly as before |
| Licence default `Apache-2.0` (scalo's own), written to the `# License:` header line and the `org.opencontainers.image.licenses` label | empty, and an empty licence writes neither | `oci_labels.licenses: "Apache-2.0".into()`, or the app's own licence. With neither licence nor copyright set, the header drops both lines and the `#` after them |
| `DEFAULT_IMAGE_REGISTRY` (`ghcr.io/hyperi-io`) | removed. `image_registry` is required and `validate()` refuses an empty one | `image_registry: "ghcr.io/hyperi-io".into()`, or `deployment.image_registry` in the cascade |
| `image_registry_from_cascade() -> String` | `-> Option<String>`, `None` when unset | `image_registry_from_cascade().unwrap_or_else(\|\| "ghcr.io/hyperi-io".into())` |
| `argocd_repo_url_from_cascade(app) -> String`, falling back to `https://github.com/hyperi-io/<app>` | `argocd_repo_url_from_cascade() -> Option<String>`. `generate-artefacts` writes no `argocd-application.yaml` without it, and warns | `deployment.argocd.repo_url: https://github.com/hyperi-io/<app>`, in a config file the cascade reads when `generate-artefacts` loads it |
| `ArgocdConfig::default().dest_namespace` `dfe` | empty, which deploys into a namespace named after `app_name`. New cascade key `deployment.argocd.dest_namespace` | `dest_namespace: "dfe".into()`, or `deployment.argocd.dest_namespace: dfe` for `generate-artefacts` |
| `KafkaSource::consumer_group` groups `dfe-{service}-{source}` and `dfe-{service}` | `{service}-{source}` and `{service}`, led by `KafkaSource::with_group_prefix` | `KafkaSource::new(name).with_group_prefix("dfe-")` |
| `FileDlqConfig::default().path` `/var/spool/dfe/dlq` | `/var/spool/scalo/dlq` | `dlq.file.path: /var/spool/dfe/dlq` |
| `FileOutputConfig::default().path` `/var/spool/dfe/output` | `/var/spool/scalo/output` | `path: /var/spool/dfe/output` |
| `FileWriterConfig::default().path` `/var/spool/dfe` | `/var/spool/scalo` (`io::DEFAULT_SPOOL_ROOT`) | `path: /var/spool/dfe` |
| `KafkaDlqConfig.common_topic: String`, default `dfe.dlq` | `Option<String>`, default `None`, which resolves to `<service>.dlq` for the service `Dlq::spawn` is given, or `dlq`. A value that is set, the empty string included, is used as it is | `common_topic: Some("dfe.dlq".into())`, or `dlq.kafka.common_topic: dfe.dlq`. Code assigning a `String` wraps it in `Some` |

**Consumer adjustment** -- set each value the app relied on, in code or config, before the bump. The ones that break something when missed:

- A consumer group that changes resets the group's committed offsets, and a broker that grants groups by prefix refuses the new one. Every `KafkaSource` that derives a group takes `.with_group_prefix(...)` with the prefix it had.
- A contract with no registry fails `validate()`, so `generate_chart`, `generate_container_manifest`, `check_chart_drift` and `generate-artefacts` refuse it.
- `#[serde(default)]` on scalo's `DlqConfig` means a partial `dlq:` block in a config file reverts the fields it leaves out to scalo's defaults, not to the app's. A deployment that mounts its spool volume at the old path sets `dlq.file.path` wherever it writes a `dlq:` block.
- A field-level `#[serde(default)]` on an app's own `dlq: DlqConfig` field fills a config file with no `dlq:` key from `DlqConfig::default()`, not from the app's container `Default`. DLQ values the app sets in its own `Default` then reach only code that builds that default, never a loaded file. Give the field `#[serde(default = "...")]` naming a function that returns the app's values.
- Committed artefacts that `generate_dockerfile`, `generate_runtime_stage`, `generate_container_manifest`, `generate_chart` or `config_schema_json` produced change on regeneration -- the secret marker gains `x-scalo-secret`, and the label and header lines follow the settings above -- so regenerate them in the same change as the bump. `assert_no_config_artifact_drift` and `check_chart_drift` fail until they are. A committed `argocd-application.yaml` stops regenerating unless `deployment.argocd.repo_url` is set.

### CEL moves to cel 0.14 (BEHAVIOUR CHANGE)

The `expression` module and the Tier 2/3 transport filters evaluate on cel 0.14, which matches what the Python `common-expression-language` 0.10 package evaluates. Two things change for expressions written against cel 0.13:

- `contains` is a string function only. `tags.contains("pii")` on a list or map is an evaluation error: `evaluate` returns `ExpressionError::Evaluation` and `evaluate_condition` returns `false`. List membership is `"pii" in tags`, and `"region" in meta` tests a map key. A transport filter of exactly the form `field.contains("literal")` runs on the Tier 1 native path, not through cel, and still searches a non-string field's JSON text.
- `min` and `max` have no default overload. The profile already refused both, so nothing that passed `validate` changes.

The `cel::Program`, `cel::Value` and `cel::Context` types the expression API returns are cel 0.14 types.

**Consumer adjustment** -- an app that declares `cel` itself moves it to `>=0.14.5, <0.15` in the same change as the bump; otherwise it builds a second cel whose types do not match the ones the expression API returns. Check stored routing rules and computed fields for `.contains(` on a list or map field, and rewrite them with `in`.

---

## Known open issues (not fixed on this branch)

Tracked upstream; each needs its own focused commit. Workarounds
applied at the consumer level until then.

### #35 -- Kafka topic auto-discovery race

`KafkaAdmin::list_topics` returns empty when the admin consumer
hasn't finished its bootstrap handshake.

**No longer fatal.** Auto-discovery that matches nothing now logs
"Auto-discovery found no matching topics" and subscribes to nothing
instead of failing startup, and the refresh loop (`topic_refresh_secs`,
60 s by default) subscribes as soon as a matching topic appears. That
covers both the race and the legitimate case of an app deployed
before its first source exists. Set `topic_refresh_secs: 0` and a
transport that discovered nothing consumes nothing until restart.

### #36 -- `KafkaTransport` always allocates both roles

`KafkaTransport::new` builds BOTH a `BaseConsumer` and a `FutureProducer` (the producer from its own `ClientConfig`). A producer-only config (empty `group`) constructs: the idle consumer takes the derived stand-in group `<client_id>-producer-only` and subscribes to nothing. It still connects and looks up that group's coordinator, so the broker has to grant the app's group prefix.

**Workaround:** none needed. Do not set `group.id` in `librdkafka_overrides` on a producer config -- the override replaces the derived stand-in with a group the broker may not grant.

### #37 -- `TransportSender::send(key, payload)` overloads `key` as topic

The Kafka impl passes `key` to `FutureRecord::to(key)`, so the
"key" arg is the destination topic, not a partition key. Callers
can't route to a configured topic AND set a partition key in one
call.

**Workaround:** none. Sites needing partition keys must bypass
the trait and use rdkafka directly.

---

## Older releases

Historical migrations live in agent memory at `project_dfe_*_migration.md` (referenced from `memory.md`) until they graduate here.
