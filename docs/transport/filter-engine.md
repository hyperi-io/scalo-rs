# Filter Engine

The transport filter engine drops or DLQs messages on the way in or
the way out of every transport -- Kafka, gRPC, Memory, File, Pipe,
HTTP -- before they reach app code. It's embedded in every
backend, a no-op check when no rules are configured, and tiered so the
common case (field-presence or equality on a top-level field) runs
without invoking the [CEL](https://github.com/cel-expr/cel-spec/blob/master/doc/langdef.md) engine at all.

---

## Why the engine exists

Operators want to:

- Drop noise at the wire (debug events, internal heartbeats) so it
  doesn't burn budget downstream.
- Quarantine poison messages to a DLQ for review without bringing
  down the pipeline.
- Express filters in operator-language (CEL) rather than scattering
  if-statements through transport code.
- Pay nothing for the engine when no filters are configured.

The three-tier design makes that last point work: the fast tier runs
without CEL, and tier classification happens at config-load -- startup
fails fast if a rule lands in a tier the operator hasn't allowed.

---

## Three tiers

Each rule is classified once at config load, and the two CEL tiers start only when their config gate allows them.

```mermaid
flowchart LR
    accTitle: L4 Detail -- Filter tiers
    accDescr: At config load each filter rule is classified. Field presence and absence, equality, inequality, prefix, suffix and contains tests run in tier 1 with SIMD field operations. Compound, numeric and size expressions run in tier 2 as compiled CEL, behind the allow_cel_filters gate. Regex, iteration and time functions run in tier 3 as complex CEL, behind the allow_complex_filters gate.

    classify{"Classify<br/>the rule"}
    tier_1["Tier 1<br/>SIMD field ops"]
    tier_2["Tier 2<br/>compiled CEL"]
    tier_3["Tier 3<br/>complex CEL"]
    gate_2["allow_cel<br/>_filters gate"]
    gate_3["allow_complex<br/>_filters gate"]

    classify -->|field tests| tier_1
    classify -->|compound| tier_2
    classify -->|regex, iteration| tier_3
    tier_2 -.-> gate_2
    tier_3 -.-> gate_3

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef decision fill:#FCF1D6,stroke:#9A6700,stroke-width:1px,color:#3D2A00
    class classify decision
```

*Figure 1. L4 Detail -- Filter tiers. A dotted arrow is the gate a tier needs, under `transport.filter_tiers`.* Up: [Transport](README.md).

| Tier | Operations | Config gate |
| ------ | ------------ | ------------- |
| **1** | `has(field)`, `!has(field)`, `field == "literal"`, `field != "literal"`, `field.startsWith(...)`, `field.endsWith(...)`, `field.contains(...)` -- `field` may be a dotted path such as `a.b` | Always on |
| **2** | Compound CEL (`&&`, `\|\|`), numeric comparison, multi-field access, `size()` | `transport.filter_tiers.allow_cel_filters_in` / `_out` |
| **3** | `matches()` (regex), `exists()`, `filter()`, `map()`, `all()`, `exists_one()` (or `existsOne()`), `timestamp()`, `duration()` | `transport.filter_tiers.allow_complex_filters_in` / `_out` (implies tier-2) |

Each tier costs more than the one before. `benches/filter_benchmark.rs` measures them.

Tier 1 uses `sonic-rs::get_from_slice` for field extraction plus
single-segment field-name lookups via pre-compiled
`memchr::memmem::Finder`. It never invokes the CEL interpreter.

Tier 2 compiles the CEL expression once at startup, then evaluates
against fields extracted via SIMD.

Tier 3 enables CEL's regex / iteration / time profile (which adds
expensive operations and DoS surface -- hence the separate gate).

Tier 2 and Tier 3 need the `expression` Cargo feature. Without it, a rule that classifies above Tier 1 fails at startup.

---

## Classification (no AST walking)

`classify()` decides what tier an expression sits in by **text-pattern
matching**, not AST analysis. `LazyLock` regex patterns for each
tier-1 operation are tried in order of expected frequency
(`has`, `!has`, `==`, `!=`, `startsWith`, `endsWith`, `contains`). If
none match, scan for restricted function names -- match means tier 3.
Otherwise tier 2.

The scan reads literals, comments and backtick-quoted names with the same lexer as the expression profile, so a restricted call it finds is one the profile rejects, and a name inside a literal or comment is neither a call nor a field.

This is conservative on purpose: only expressions that match a
tier-1 pattern whole execute outside the CEL engine. Anything subtle drops
to tier 2 or 3 where the actual CEL interpreter validates the
expression.

See [src/transport/filter/classify.rs](../../src/transport/filter/classify.rs).

---

## Semantics

- First-match wins. Filters are evaluated in declared order, and the
  first match returns its action and stops the loop. No match -> message
  passes.
- `drop` action silently discards. Every match, `drop` or `dlq`, counts
  in the `transport_filtered_total` counter, labelled `direction` and `action`.
  A record a sender's `dead_letter_reason` screens and the send then filters
  counts once: the screen counts only the `dlq` match it takes out. Its
  filters are still evaluated twice when the sender has a `dlq` rule, once by
  the screen and once by the send, a known cost of screening.
- `dlq` action produces a `FilteredDlqEntry` returned **inline** in
  `recv()`'s `WorkBatch.dlq_entries` (alongside the passing
  `WorkBatch.records`) -- the transport does **not** route to a DLQ
  directly. The caller routes `batch.dlq_entries` to the DLQ of its
  choice, and a hand-rolled loop that ignores them loses them. (When the `BatchEngine` run
  loops own the receive, a `FilterDlqPolicy` governs this -- `Reject` by
  default, or `Route`/`DiscardWithMetric`.) The commit token of a dropped
  or DLQ'd record stays in `WorkBatch.commit_tokens`, so the block commit
  moves the source past it.
- Inbound and outbound are independent. Each transport has
  `filters_in` and `filters_out`, evaluated separately.
- Startup fails fast on rules above the allowed tier, on invalid
  CEL syntax, on empty expressions, and on Tier 2/3 rules over the AST
  budget (`transport.filter_tiers.budget`). A pipe built directly with
  `PipeTransport::new` starts but rejects traffic -- see
  [Where it's embedded](#where-its-embedded).

---

## Config

Two distinct cascade keys gate filter behaviour. Operators have to set
both correctly because they apply at different layers.

### `<backend>.filters_{in,out}` -- the rules themselves

Per-transport rule lists, carried on each backend's config struct (`KafkaConfig`, `GrpcConfig`, `MemoryConfig`, `FileTransportConfig`, `PipeTransportConfig`, `HttpTransportConfig`). Each rule has an `expression` (CEL text) and an `action` (`drop`, the default, or `dlq`). Rules apply at the named transport, in the named direction. They sit in the backend section under whatever cascade key the app reads -- here, a receiver built by `AnyReceiver::from_config("transport.input")`:

```yaml
transport:
  input:
    type: kafka
    kafka:
      brokers: ["kafka:9092"]
      topics: ["events"]
      filters_in:
        - expression: 'has(_internal)'
          action: drop
        - expression: 'status == "poison"'
          action: dlq
        - expression: 'severity > 3 && source != "internal"'
          action: dlq
      filters_out:
        - expression: 'has(debug)'
          action: drop
```

### `transport.filter_tiers.*` -- the tier gates

Top-level gate controlling which tiers any transport is allowed to
compile. Defaults to all Tier 2/3 gates closed -- first-time deployments
get Tier 1 only.

```yaml
transport:
  filter_tiers:
    allow_cel_filters_in: false       # Tier 2 (compiled CEL) inbound
    allow_cel_filters_out: false      # Tier 2 outbound
    allow_complex_filters_in: false   # Tier 3 (regex/iteration/time) inbound
    allow_complex_filters_out: false  # Tier 3 outbound
```

If a configured filter rule classifies above the allowed tier, the
transport's constructor returns `TransportError::Config(...)` and the
transport fails to start. **Fail-loud, not fail-silently** -- a
misconfigured drop/dlq rule never runs as an empty filter engine that
lets every message through. `PipeTransport::new` rejects traffic
instead -- see [Where it's embedded](#where-its-embedded).

`transport.filter_tiers.budget` bounds Tier 2/3 cost: `max_ast_nodes` (default 200) and `max_iteration_depth` (default 2) are checked at startup, and `max_payload_bytes` (default 1 MiB) at evaluation. A payload over `max_payload_bytes` skips the CEL rule, which then does not match, and counts in `transport_filter_cel_payload_skip_total` (with the `metrics` feature). A screened record counts its skip once, at the send, or at the screen when a `dlq` rule takes it out.

### `expression.*` -- the app-wide CEL function profile

The top-level `expression` section sets which CEL functions are usable in the app's own expressions (transforms, validators):

```yaml
expression:
  allow_regex: false        # regex matches() function
  allow_iteration: false    # filter/map/all/exists/exists_one/existsOne
  allow_time: false         # time-related functions
```

Transport filters meet it in two ways:

- Tier 2 compiles under this profile.
- Tier 3 compiles with regex, iteration and time all unlocked, so these three flags do not apply to it. The tier gate is the only switch a Tier 3 filter needs.

The profile's function allowlist applies to both tiers.

### The tier gate decides

A filter rule like `'tag.matches("^prod-")'` classifies as Tier 3 and compiles when `transport.filter_tiers.allow_complex_filters_in` is `true`, whatever `expression.allow_regex` says. Setting `expression.allow_regex: false` does not stop regex transport filters -- close the Tier 3 gate for that.

### Reload semantics

The tier-gate config (`transport.filter_tiers.*`) is read **at
transport construction time** via
`TransportFilterTierConfig::from_cascade()`. A `ConfigReloader` update
to those keys does **not** propagate to an already-running transport
-- the old gates remain in effect until the transport is reconstructed.

This is intentional: a misconfigured reload that flips a gate would
otherwise tear down a working transport mid-stream. Operators wanting
the new gate config to take effect should restart the service (or, in
K8s, roll the pod). Rule lists load the same way, so a rule change
needs a restart too.

A first-time deployment should start with all gates off -- only Tier 1
filters work. Flip a gate on once Tier 2 or Tier 3 is needed
and the operator has reviewed the cost (Tier 3 has no CPU bound, see
[Known limitations](#known-limitations)).

---

## API surface

```rust
use scalo::transport::filter::{
    TransportFilterEngine, FilterDisposition,
    FilteredBatch, FilteredDlqEntry,
    FilterAction, FilterDirection, FilterRule, FilterTier,
};

// Built once per transport from config:
let engine = TransportFilterEngine::new(
    &filters_in,
    &filters_out,
    &tier_config,
)?;

// On the hot path:
match engine.apply_inbound(&payload) {
    FilterDisposition::Pass => process(payload),
    FilterDisposition::Drop => continue,
    FilterDisposition::Dlq  => continue,   // a transport's recv() returns it in WorkBatch.dlq_entries
}

// DLQ entries come back inline from recv() -- route them per batch:
let batch = transport.recv(max).await?;
for entry in batch.dlq_entries {
    dlq_sender.send(entry).await?;
}

// A sender's answer for one payload: None sends it, Some(SendResult::Ok)
// for a `drop` match, Some(SendResult::FilteredDlq) for a `dlq` match:
if let Some(settled) = engine.outbound_one(&payload) {
    return settled;
}

// A block: the records to send, borrowed, and the answer once they are sent:
let (to_send, handled) = engine.partition_outbound(&records, |r| &r.payload);

// Cheap fast-paths:
if !engine.has_inbound_filters() {
    /* short-circuit, skip evaluation */
}
```

`has_inbound_filters` and `has_outbound_filters` are marked
`#[inline]` so the no-filter branch becomes a single check on the hot
path.

`FilteredBatch::passthrough` constructs a batch when filters are off,
so transport code doesn't fork for the empty case.

---

## Where it's embedded

Every transport backend builds the engine at construction time from
its own config section's `filters_in` / `filters_out` plus the
`transport.filter_tiers` gates, read through
`TransportFilterTierConfig::from_cascade()`:

| Transport | Source |
| ----------- | -------- |
| Kafka | [src/transport/kafka/mod.rs](../../src/transport/kafka/mod.rs) |
| gRPC | [src/transport/grpc/mod.rs](../../src/transport/grpc/mod.rs) |
| Memory | [src/transport/memory/mod.rs](../../src/transport/memory/mod.rs) |
| File | [src/transport/file.rs](../../src/transport/file.rs) |
| Pipe | [src/transport/pipe.rs](../../src/transport/pipe.rs) |
| HTTP | [src/transport/http.rs](../../src/transport/http.rs) |
| Syslog | [src/transport/syslog/mod.rs](../../src/transport/syslog/mod.rs) |
| OTLP (outbound only) | [src/transport/otlp/mod.rs](../../src/transport/otlp/mod.rs) |

`PipeTransport::new` returns the transport rather than a `Result`, so a rule that fails to compile cannot fail its constructor. The pipe starts unhealthy instead: `send` returns `SendResult::Fatal` and `recv` returns `TransportError::Config`, both carrying the compile error, until it is rebuilt with valid rules. `AnySender` and `AnyReceiver` fail construction on the same rule, as for every other backend.

The engine is a no-op when both filter vectors are empty -- there's no
per-message overhead beyond the inlined `has_*_filters` check.

---

## MsgPack and binary payloads

The engine targets JSON. When `apply_*` sees a payload that looks
like MsgPack (heuristic detection of MsgPack signature bytes), it
short-circuits to `Pass`. The first bypass in each direction logs a
warning, and every bypass counts in
`transport_filter_msgpack_bypass_total{direction}` (with the `metrics`
feature).

This is a deliberate choice -- running JSON-shaped filters against
binary payloads would either falsely match or always reject. The engine
has no MsgPack evaluator, so a pipeline that needs to filter MsgPack
converts it to JSON upstream.

---

## Known limitations

| Limitation | Effect |
| ------ | ------- |
| String comparison is not constant-time | A timing attack on a high-entropy field value is possible, at low risk |
| Expressions are logged as written | Each rule's text is logged at INFO at startup, so treat expressions as non-secret |
| `field == "value"` has no pre-quoted bytes fast path | `has` and `!has` use a pre-compiled `memmem::Finder`, and equality extracts the field and compares strings |
| MsgPack payloads pass unfiltered | A one-shot warning per direction and `transport_filter_msgpack_bypass_total` make the bypass visible |
| Tier 2/3 CEL has no time budget | `transport.filter_tiers.budget` caps AST size and iteration depth at startup and payload size at evaluation. `program.execute(&ctx)` runs with no time cap, so a costly filter within those caps can hold the ingest thread. A time cap needs support in the upstream `cel` crate |

A pipeline that mixes JSON and MsgPack should read the MsgPack row.

---

## Tests and benchmarks

- Unit tests in every file of the module (budget, classify, compiled, config, metrics, mod).
- Integration tests in `tests/transport_filter.rs` -- round-trip,
  adversarial inputs, Unicode, 100-rule lists, MsgPack heuristic.
- Benchmarks: `benches/filter_benchmark.rs` -- criterion groups
  covering the no-filter baseline, tier-1 `has`, `==`, `startsWith` and a
  dotted path, first-match-at-N, tier-2 compound, tier-3 regex.

---

## Related

- [transport/README.md](README.md) -- trait architecture, factory, `AnySender`
- [transport/backends.md](backends.md) -- per-backend wiring
- [pipeline/dlq.md](../pipeline/dlq.md) -- DLQ sink backends
- [core-pillars/config.md](../core-pillars/config.md) -- cascade
- [feature-flags.md](../feature-flags.md) -- `transport`, `expression`
