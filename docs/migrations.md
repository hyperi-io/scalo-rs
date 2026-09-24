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
|---|---|
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

Full tuning surface (profile / pause_above / resume_below / target_rho /
md_factor) in [self-regulation.md](self-regulation.md). Off-pressure cost is
near zero: the budget sits at its big start value so a block is one sub-block
with no per-record overhead.

### Originator brake / token wiring (BEHAVIOUR CHANGE)

Data-originator stages get the inbound brake wired into the receive transport:
Kafka pauses ASSIGNED partitions (member stays in group, no rebalance);
HTTP/gRPC returns 503 / `UNAVAILABLE`; the fetcher pauses its poll. Each pairs
with the at-least-once commit token (offset / responder / cursor) so a paused
intake never advances the source position. `SelfRegulationGovernor::attach_kafka_gate`
is the one-call form of the gate dance. See [backpressure.md](backpressure.md).

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

**Consumer adjustment** — pick a policy explicitly:

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

**Consumer adjustment** — at each `send` call, pass owned `Bytes`:

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
instead of inside the transport — net-neutral. Callers holding `Vec<u8>` or
`Bytes` (the hot path) are now zero-copy. `bytes` is a `transport`-feature dep.

### `transport::FromCascade` trait (additive, non-breaking)

New `transport::FromCascade` trait with a default `from_cascade_key(key)`
consolidates the byte-identical `from_cascade()` bodies the 5 transport configs
(grpc/http/file/pipe/redis) each repeated. Each config's inherent
`from_cascade()` is unchanged in signature — it just delegates — so this is
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

In-crate URL builders already do this. The change exists so `Debug`
+ `serde` round-trips redact by default.

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
engaged (dfe-transform-vrl #53).

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

### `memory::set_heap_source` — allocator override (additive, opt-in)

Crate hook `scalo::memory::set_heap_source(fn() -> usize)` overrides the
detected `UsageSource` with an allocator statistic. It is now rarely
what you want: it cannot see thread stacks, mmap'd buffers, or pages the
allocator retains after a free, whereas the cgroup default is the number
the OOM killer acts on. Register one only where the allocator figure is
the one you mean to gate on.

```rust
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    scalo::memory::set_heap_source(|| {
        tikv_jemalloc_ctl::epoch::advance().ok();
        tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0)
    });
    // ... ServiceRuntime / MemoryGuard built afterwards pick it up ...
}
```

scalo intentionally takes **no allocator dependency** (the global
allocator is the binary's choice, and scalo is `#![forbid(unsafe_code)]`).

### `SinkDrain::flush_durable` (additive, default no-op)

New trait method; existing impls compile unchanged. A drain with a durability step overrides it. The DLQ drain does, running each backend's durable flush -- see [Kafka DLQ `flush()` waits for broker acks](#kafka-dlq-flush-waits-for-broker-acks-behaviour-change).

### `Dlq` struct (internal layout)

Gained a `cancel: CancellationToken` field. Consumers construct via
`Dlq::spawn` or `Dlq::disabled` and don't touch the struct fields —
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

### Wave 1 — Tier-3 single-knob

`transport.filter_tiers.allow_complex_filters_in/out: true` now
implies `expression.allow_regex / allow_iteration / allow_time =
true` for the transport's compile path. Previously operators had
to flip both knobs and they could disagree (filter passes the
transport gate, fails the expression profile). One source of truth.

### Wave 1 — `WorkerPoolConfig::validate`

`async_concurrency == 0` now rejected at config-load. Previously
passed validation and panicked at `step_by(0)` inside
`fan_out_async`.

### Wave 2 — `BackgroundSink::flush()` surfaces drain errors

`flush()` now returns `Err(SinkError::Drain(_))` when the underlying
drain's `write_batch` or `flush_durable` failed. Previously acked
`Ok(())` regardless — callers thought messages were durable when
they were lost. Caller adjustment: handle `Err(SinkError::Drain)`
on `flush().await`.

### Wave 2 — Kafka DLQ `flush_durable` Err on outstanding

The Kafka backend's durable flush returns `DlqError::Kafka` when its wait for acks expires with messages still in flight, where it used to log at debug and return `Ok(())`. Nothing called it until the DLQ drain began to, so `Dlq::flush()` never saw that error -- see [Kafka DLQ `flush()` waits for broker acks](#kafka-dlq-flush-waits-for-broker-acks-behaviour-change).

### Wave 3 — `CacheConfig.dir_mode` / `.file_mode`

Two new optional fields default to `Some(0o700)` and `Some(0o600)`.
`None` disables chmod entirely — required on S3-FUSE / root-
squashed NFS / similar mounts that reject chmod. Operators on
those mounts must own upstream perms.

### Wave 3 — `dangerous-diagnostics` feature

`config::registry::dump_effective_unredacted()` is now gated by
the `dangerous-diagnostics` cargo feature. Not included in `full`.
Compile with `--features dangerous-diagnostics` only for one-off
operator-driven debugging.

### Wave 3 — strict CEL `has(<single>)`

Tier-1 `has(<single-field>)` now only matches at JSON depth 1
(immediate child of the root). Previously matched at any depth.
Operators relying on the nested-match behaviour must switch to a
dotted path (`has(some.path.field)`) or to a Tier-2 CEL filter.

### Wave 5 — bounded metric labels (F7)

`ServiceMetrics` methods that took free-form `&str` for metric labels
now take typed enums. The labels are bounded; cardinality is
fixed at the enum variant count.

| Method | Old | New |
|---|---|---|
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

### Wave 5 — `RoutedSender` metric label

`dfe_transport_sent_total{transport="routed",route=...}` now
carries the **configured route name** (or `"default"` for the
fallback), not the inbound message key. Cardinality is bounded by
the routing table size. No consumer code change required — only
the metric label values change. Dashboards keyed on per-message
keys need rewiring.

### `vault:` path -- the first segment is the mount (BEHAVIOUR CHANGE)

`OpenBaoProvider` now reads the first segment of a vault path as the KV
mount, with the KV v2 `data` segment optional. `vault:secret/x:key` used
to request `secret/data/secret/x`, so the obvious spelling read the wrong
path, and a non-default mount could only be named by writing `data`
yourself.

| Spec | Old | New |
|---|---|---|
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

- A delivery the broker refused since the previous flush fails the flush with `Err(DlqError::File(..))` and is counted in `dropped()` and `dlq_dropped_total{reason="backends_failed"}`, like a refused write.
- Entries still unacknowledged after 30 s are purged from the producer, so they cannot land later, and counted the same way. The purge adds up to 5 s.
- `Cascade`: when Kafka queues part of a batch and refuses the rest, only the rest goes to the next backend. Before, the whole batch did, so the file held a second copy of the part Kafka took.
- `FanOut`: a Kafka loss counts only for entries no other backend holds.

**Consumer adjustment** -- none in code. A flush over the Kafka backend can now take up to 35 s while the broker is slow or down; a timeout around it shorter than that sees its own timeout, and the failure goes to the next flush. A caller that never calls `flush()` sees Kafka delivery failures in `transport_send_errors_total{transport="kafka"}` only, not in `dropped()`.

### `NdjsonWriter` refuses a write a rotation would panic on (BEHAVIOUR CHANGE)

`file-rotate` 0.8 panics inside a rotation when the output directory is gone and cannot be recreated, or the current file is missing and cannot be created. Services built with `panic = "abort"` died at the next rotation boundary. `NdjsonWriter`, and the DLQ file backend and file output sink built on it, now refuse such a write with an `Err` before calling into `file-rotate`.

- A write while the current file is missing returns `Err` and schedules a reopen, which runs once the directory is usable again. It never recreates a missing directory, at a rotation boundary included.
- `NdjsonWriter::new`, and a reopen, refuse a directory that is not a directory or cannot be listed.
- `NdjsonWriter::new` refuses a filename with no final component (`..`) with `ErrorKind::InvalidInput`.
- `max_age_days` is capped at 1,000,000 days; above about 95 million the rotation's age check panicked.

**Consumer adjustment** -- none in code. A write the writer would previously have lost silently or panicked on is now an `Err`.

---

## Known open issues (not fixed on this branch)

Tracked upstream; each needs its own focused commit. Workarounds
applied at the consumer level until then.

### #35 — Kafka topic auto-discovery race

`KafkaAdmin::list_topics` returns empty when the admin consumer
hasn't finished its bootstrap handshake.

**No longer fatal.** Auto-discovery that matches nothing now logs
"Auto-discovery found no matching topics" and subscribes to nothing
instead of failing startup, and the refresh loop (`topic_refresh_secs`,
60 s by default) subscribes as soon as a matching topic appears. That
covers both the race and the legitimate case of an app deployed
before its first source exists. Set `topic_refresh_secs: 0` and a
transport that discovered nothing consumes nothing until restart.

### #36 — `KafkaTransport` always allocates both roles

`KafkaTransport::new` builds BOTH a `BaseConsumer` and a `FutureProducer` (the producer from its own `ClientConfig`). A producer-only config (empty `group`) constructs: the idle consumer takes the derived stand-in group `<client_id>-producer-only` and subscribes to nothing. It still connects and looks up that group's coordinator, so the broker has to grant the app's group prefix.

**Workaround:** none needed. Do not set `group.id` in `librdkafka_overrides` on a producer config -- the override replaces the derived stand-in with a group the broker may not grant.

### #37 — `TransportSender::send(key, payload)` overloads `key` as topic

The Kafka impl passes `key` to `FutureRecord::to(key)`, so the
"key" arg is the destination topic, not a partition key. Callers
can't route to a configured topic AND set a partition key in one
call.

**Workaround:** none. Sites needing partition keys must bypass
the trait and use rdkafka directly.

---

## Older releases

Historical migrations live in agent memory at `project_dfe_*_migration.md` (referenced from `memory.md`) until they graduate here.
