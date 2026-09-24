# Vector baselining + integration

Project doc. Vector is a first-class part of the DFE picture: DFE already uses
it (2.1 and earlier), and DFE 2.2+ is deliberately swap-in/swap-out integratable
with Vector. This doc is the standing reference for how we BASELINE scalo against
the Vector equivalent (better, or at least not worse) and how we keep clean
INTEGRATION with Vector pipelines - cooperative, not a competitive "bake-off".

It compares the scalo-rs data-plane runtime + the DFE app stack against
vectordotdev/vector (now Datadog) across data model, concurrency,
self-regulation, auto-scaling, transport and efficiency; then specifies the
per-app baselining + integration benches (Section 7) so we get real, repeatable
numbers vs the Vector equivalent instead of marketing claims.

Sources: source read of /projects/scalo-rs, /projects/dfe-* and /projects/vector,
plus web research. Primary-source URLs inline. Design-target (not measured) or
vendor-reported claims are flagged; a consolidated "not verified" list is at the
end. First written 2026-06-26.

---

## Purpose and framing (read this first)

This is a **decision aid**, not a marketing exercise - and it is NOT a
"bake-off". We already USE Vector for a large part of DFE (2.1 and earlier),
and DFE 2.2+ is deliberately built to be easily integratable with Vector -
swap components in and out. So the relationship is cooperative, not
competitive. The benches (Section 7) exist for two things:

- **Baselining**: Vector is the baseline. For each change, answer **"is the
  whole picture better, or at least not worse, than the Vector equivalent?"** -
  not "does scalo beat Vector". Where scalo's specialisation pays off, and where
  it does not, both matter.
- **Integration**: prove scalo interops cleanly with Vector pipelines (swap-in /
  swap-out), so adopting a scalo component never strands a Vector deployment.

It is explicitly NOT a "scalo beats Vector" billboard.

The comparison is also not apples-to-apples by design, and that is the
point. **scalo and Vector are different deployment philosophies:**

- **Vector = a generic swiss-army-knife.** One binary, 30+ sources / 49+
  sinks, glue anything to anything, horizontal scaling delegated entirely
  to k8s, self-regulation limited to sink-side ARC + bounded buffers.
  Brilliant for breadth and "drop it in anywhere".
- **scalo + the DFE stack = an optimised, opinionated data plane.**
  Purpose-built *bookends* (a specialised ingest receiver, specialised
  egress loader/archiver) rather than generic source/sink for everything;
  **built-in horizontal scaling** (a blended KEDA `ScalingPressure`
  signal, not just CPU HPA); **internal self-regulation ON by default**
  (cgroup memory guard + inbound gate + AIMD byte-budget). Narrower on
  purpose, faster and more self-managing on its chosen path.

So when a bench shows scalo ahead on a route/forward workload and at
parity on a full-parse-mutate workload, that is the *expected* shape - it
reflects the deployment-approach difference, not a winner. Use the numbers
to decide which scalo investments earn their keep, and to set honest
expectations with anyone weighing the specialised stack against the
generic tool.

---

## 0. TL;DR

- Both are Rust, Tokio-based, MPL/Apache pipelines. The headline framing
  "row iterator vs batch" is too simple: **Vector also batches**. It moved
  from one-Event-at-a-time to `EventArray` (Vec of ~1000 events) between
  components over v0.20-v0.22 for a reported +10-50% throughput. The real
  difference is *what is in the batch and when it gets parsed*.
- scalo's `WorkBatch<T>` carries **unparsed `bytes::Bytes`** and parses
  **on demand** with SIMD (sonic-rs) only when a field is touched, plus
  field interning and a SIMD pre-route that extracts the routing key
  without a full parse. Vector's `EventArray` carries **already-parsed**
  events in VRL's `Value` model. So scalo's design bet is "don't pay for
  what you don't read"; Vector's is "parse once, rich model, broad
  ecosystem".
- Self-regulation doctrine differs. scalo gates the **inbound source**
  (Kafka pause / HTTP 503 / fetcher-pause) under a cgroup-v2 memory guard
  + AIMD byte-budget, and emits a separate KEDA scale signal. Vector's
  in-process adaptivity is **ARC on the outbound sink** (AIMD on RTT,
  TCP-congestion-control style) and it pushes backpressure upstream via
  bounded buffers. Vector does NOT horizontally self-scale - that is
  delegated entirely to k8s HPA/KEDA + a fronting load balancer.
- Neither side has trustworthy apples-to-apples throughput numbers today.
  scalo has design targets but no committed benches; Vector's headline
  table is ~2019-vintage and self-published. Hence Section 7 (baselining).

---

## 1. Data model - row iterator vs batch (the nuanced version)

### scalo: `WorkBatch<T>` of unparsed bytes, parse-on-demand

- Unit of work: `WorkBatch<T>` (`src/transport/work_batch.rs`) wrapping
  - `Vec<Record>` where each record payload is `bytes::Bytes`
    (Arc<[u8]>; clone = refcount bump, not copy)
  - a **separately sized** `Vec<T>` of commit tokens, deliberately
    decoupled from record count so a fan-out transform (N in -> 2N out)
    still fires exactly N source acks once, after all outputs are sent
  - inline-DLQ entries (`Vec<FilteredDlqEntry>`) for a no-silent-drop
    contract
- Default path is **parse-on-demand**: the `BatchEngine`
  (`src/worker/engine/driver.rs`) drives one batch through
  recv -> filter/DLQ -> ingress lease -> process -> sink -> commit, and
  `codec::parse()` runs per record only when the code reads a field. A
  pure pass-through app pays zero parse cost.
- Opt-in hot path `run_workbatch_parsed()` pre-parses the whole block on
  the rayon pool with SIMD, returning a `ParsedBatch<'a, T>` with aligned
  `records[i]`/`parsed[i]` and a shared `FieldInterner`.
- Parsing is SIMD: **sonic-rs** (AVX2/NEON), MsgPack via `rmpv` mapped
  straight into a sonic value walker (no rmp_serde -> serde_json bridge).
- Within a batch: record-at-a-time, parallelised across the batch on
  rayon. So scalo is "batch as the transport/scheduling unit, record as
  the processing unit", with the batch staying as raw bytes as long as
  possible.

### Vector: `EventArray` of already-parsed events (row-oriented)

- Core type is a tagged enum `Event = Log | Metric | Trace`
  (`lib/vector-core/src/event/mod.rs`). Metrics are first-class, not
  structured logs.
- Between components Vector passes `EventArray`
  (`lib/vector-core/src/event/array.rs`), an enum of
  `Logs(Vec<LogEvent>) | Metrics(Vec<Metric>) | Traces(Vec<TraceEvent>)`
  - type-homogeneous, **row-oriented** (Vec of structs, not columnar),
  ~1000 events per array (`CHUNK_SIZE`,
  `lib/vector-core/src/source_sender/mod.rs`).
- This batching was a deliberate perf evolution: single-Event -> array
  landed Jan 2022, rolled out v0.20.0 (+10-20%) to v0.22.0 (up to +50%
  on common topologies), with a `component_received_events_count`
  histogram added to observe internal batch size distinct from sink
  batching. https://vector.dev/releases/0.20.0/ ,
  https://vector.dev/releases/0.22.0/
- `LogEvent` content is VRL's `Value` (already parsed/typed),
  reportedly `Arc<Inner>` with copy-on-write and cached byte/JSON sizes.
  Not columnar, no Arrow.

### So what

- "Row iterator vs batch" is wrong as stated. Both batch. The contrast:
  - **When you parse**: scalo defers (bytes until touched, SIMD,
    pre-route extract ~50-100 ns without full parse); Vector parses
    eagerly into a rich `Value` model at ingest.
  - **What flows**: scalo flows `Bytes` (zero-copy refcount chains);
    Vector flows typed events (richer, more per-event allocation, but
    every transform sees a uniform model).
  - **Mutation**: Vector's `Value`/CoW model is built for read-many
    mutate-rarely transforms (its core use case); scalo's bytes model is
    built for route/filter/forward where most records are never mutated.
- Neither is columnar/Arrow. That is a genuine third design point both
  have deferred - relevant only if a consumer needs analytical
  scan/aggregate, where vectorized columnar is "one or two orders of
  magnitude" faster than tuple-at-a-time (MonetDB/X100, CIDR 2005,
  https://www.cidrdb.org/cidr2005/papers/P19.pdf). For route/transform/
  ship workloads, row batching is the right call for both.

---

## 2. Concurrency / worker model

### scalo

- `AdaptiveWorkerPool` (`src/worker/pool.rs`): a **rayon** pool for
  CPU-bound work (JSON parse, routing, CEL, compression) sized to
  `std::thread::available_parallelism()` which is **cgroup-aware** on
  Linux (reads `cpu.max`/`cpuset`), plus **Tokio** task spawning for
  async I/O (`fan_out_async()`).
- A custom hysteretic `Semaphore` (parking_lot Mutex+Condvar) holds
  `target` permits; threads park rather than spin when saturated.
- `ScalingDecision` (`src/worker/scaler.rs`) nudges thread count on CPU
  utilisation bands (grow +2 below a low watermark, shrink -1 above a
  high one), with a memory-pressure safety floor to `min_threads`.
- Rationale (and a good one): CPU overload self-corrects via the kernel
  CFS scheduler (graceful throttle), so there is **no cgroup-CPU
  backpressure**; only memory gets a dynamic pressure signal, because
  memory overload is a fatal OOM-kill, not a slowdown.

### Vector

- One Tokio runtime per instance; every component is a task on the
  work-stealing scheduler. No rayon. CPU-bound transform work shares the
  same async runtime (risk: a blocking transform stalls a worker;
  mitigated by component design, not a separate pool).
- Stateless ("function") transforms can be inlined at the source for
  zero-overhead concurrency; stateful ("task") transforms run as their
  own streaming tasks.
- "Automatically scales to take advantage of all vCPUs"
  (https://vector.dev/docs/setup/going-to-prod/sizing/) - vertical only.

### So what

- scalo splits CPU (rayon) from I/O (Tokio) deliberately; Vector runs
  everything on Tokio. scalo's split is better for parse/route/compress
  heavy stages; Vector's single-runtime model is simpler and fine when
  transforms are light or already async.

---

## 3. Self-regulation and backpressure

### scalo - three brains, gate the inbound

- **MemoryGuard** (`src/memory/guard.rs`): hard source of truth from
  cgroup v2 `memory.current`/`memory.max`/`memory.high`; effective limit
  = max * headroom (0.85), capped at memory.high; pressure ratio is the
  never-OOM backstop. App registers its allocator via `set_heap_source`
  so scalo stays allocator-agnostic.
- **ByteBudgetController** (`src/governor/budget.rs`): AIMD on
  rho = EMA(process_time)/EMA(ingest_interval). rho<0.7 additive
  increase, rho>0.7 multiplicative decrease (0.5), memory pressure forces
  immediate decrease. Cold-starts big, collapses to a single sub-block
  under low load (zero overhead).
- **InboundGate** (`src/governor/gate.rs`): pauses the **source** on the
  rising edge (Kafka pause-assigned partitions, HTTP 503, fetcher-pause),
  resumes on the falling edge. Never throttles the sink.
- **ScalingPressure** (`src/scaling/pressure.rs`): a separate, lock-free
  0-100 composite (Kafka lag, queue depth, memory ratio + circuit-breaker
  and memory-high hard gates) emitted for KEDA - a capacity lever, not a
  data-path lever.

This matches the canonical doctrine: gate inbound, never the drain.
Throttling the drain deadlocks the bounded buffer (the drain is the only
party that frees space). External load is push-based so an admission gate
is unavoidable. Reactive Streams / Flink / Kafka pause-resume all agree.
https://github.com/reactive-streams/reactive-streams-jvm ,
https://flink.apache.org/2021/07/07/how-to-identify-the-source-of-backpressure/

### Vector - ARC on the sink, backpressure via bounded buffers

- **Adaptive Request Concurrency (ARC)**
  (`src/sinks/util/adaptive_concurrency/controller.rs`): AIMD on a
  per-sink in-flight limit, driven by EWMA-RTT, "inspired by TCP
  congestion control". +1 additive when RTT flat and 2xx; multiplicative
  decrease (0.9) on rising RTT / 429 / 503 / backpressure. Defaults:
  initial 1, ewma_alpha 0.4, decrease_ratio 0.9, rtt_deviation_scale 2.5,
  max 200. **Default for HTTP-based sinks.**
  https://vector.dev/docs/architecture/arc/ ,
  RFC 1858 (2020-04-06). One observed case: static limits were
  "limiting performance by over 80%".
- **Buffers** (`lib/vector-buffers`): memory (default, 500-event sink
  buffer, ~100-event inter-component slack) or disk v2 (pure-Rust WAL,
  128 MiB files, ~256 MiB min, 500 ms fsync, CRC). `when_full`:
  `block` (backpressure, default) / `drop_newest` (shed) / `overflow`.
- **Backpressure** propagates sink -> transform -> source -> client (HTTP
  429, slow pull), only under `when_full: block`. A source runs at the
  speed of the slowest blocking sink.
- **End-to-end acks** (at-least-once): finalizer/batch-notifier model,
  fan-out takes the worst status. No exactly-once.

### So what - these are complementary, not contradictory

- scalo's "never throttle the drain" and Vector's ARC are NOT in
  conflict. ARC does not throttle the drain to manufacture backpressure;
  it *matches sink concurrency to downstream service capacity* so the
  drain runs as fast as the downstream can take it. scalo has nothing
  equivalent at the sink - it assumes the sink is the broker/storage and
  gates intake. A scalo gap: **no adaptive sink concurrency
  controller** for slow HTTP/gRPC downstreams. ARC is the one piece of
  Vector's self-regulation scalo could learn from.
- scalo's edge over Vector: a hard cgroup memory guard + inbound gate
  that is ON by default, and a first-class KEDA scale signal. Vector
  relies on bounded buffers for memory safety (softer) and externalises
  horizontal scaling entirely.

---

## 4. Auto-scaling

### Internal (within process)

- scalo: vertical via AdaptiveWorkerPool (rayon thread count nudged on
  CPU bands) + AIMD byte-budget (work size shrinks under pressure) +
  inbound gate (intake pauses). ON by default, opt-out.
- Vector: vertical via Tokio work-stealing across vCPUs + ARC per sink.
  No worker-count controller; "scales to all vCPUs" is just the runtime.

### External (k8s)

- scalo: emits `ScalingPressure` 0-100 for the KEDA external-scaler API,
  plus a `deployment` module that generates Dockerfile/Helm/Argo from a
  `DeploymentContract`. So scalo ships an opinion about *how* it should be
  horizontally scaled (custom signal that already blends Kafka lag, queue
  depth, memory).
- Vector: **does not self-scale horizontally** - explicit in the docs
  and the Helm chart (`templates/hpa.yaml` emits a standard
  `autoscaling/v2` HPA only when enabled). Roles: Agent->DaemonSet,
  Aggregator->StatefulSet, Stateless-Aggregator->Deployment. Scale on
  avg CPU ~80-85% with a 5-min window, or KEDA. Partitioning is done in
  front by a load balancer, not by Vector.
  https://vector.dev/docs/setup/going-to-prod/sizing/

### So what

- For Kafka-lag-driven stateful scaling, scalo's blended KEDA signal is
  more direct than Vector's CPU-based HPA (CPU is a lagging indicator;
  queue depth/lag scales before latency rises -
  https://www.datadoghq.com/blog/autoscaling-custom-metrics/). Both hit
  the same hard ceiling for Kafka consumers: **one consumer per
  partition** - partition count caps replicas (KEDA enforces this by
  default), and rebalancing cost is real (cooperative rebalancing,
  KIP-429, mitigates it). https://keda.sh/docs/2.19/scalers/apache-kafka/

---

## 5. Transport, efficiency, allocator

- **Transport**. scalo: config-driven factory (kafka/grpc/http/file/
  pipe/memory) returning `Box<dyn Transport>`, with a `routed`
  sender for per-key dispatch (originators only: receiver, fetcher) and a
  filter engine embedded in every backend. Vector: 30+ sources, 49+
  sinks, native to each integration, connected by the topology DAG. Vector
  wins breadth massively; scalo wins on "one binary, swap backend by
  config" and a uniform filter/route layer. (Reminder from CLAUDE.md:
  scalo transport is config-driven, build with ALL transport features.)
- **Efficiency**. scalo: SIMD parse (sonic-rs), `FieldInterner`
  (DashMap, ~20 ns hit / ~100 ns first-see), SIMD pre-route extract
  (`sonic_rs::get_from_slice`, no full parse), streaming sub-blocks that
  bound peak ingress memory to one sub-block, depth guard against hostile
  nesting. Vector: EventArray batching, sink batching+compression,
  `KeyString` key interning, `simdutf8` for UTF-8 validation (NOT
  simd-json for parse), `bytes` for zero-copy, opt-in per-component
  allocation tracking (~20% cost, hence opt-in).
- **Allocator**. Both lean jemalloc via tikv-jemallocator. Useful
  precedent: Vector adopted jemalloc, **dropped it 2021-06** for the
  system allocator (citing the stale gnzlbg crate), then **re-added** it
  via the maintained tikv binding - exactly the "jemalloc revived" arc in
  our memory. Lesson logged.
  https://vector.dev/highlights/2021-06-02-drop-jemalloc/
- **Scripting/transform perf lesson**. Vector built a VRL bytecode VM
  (v0.21, opt-in), measured +10-15%, then **removed it in v0.23** because
  the optimised tree-walking AST interpreter reached parity. If we ever
  consider a CEL/VRL VM in scalo, this is the cautionary precedent: a
  well-tuned tree-walker matched a bespoke VM for this workload class.
  https://vector.dev/highlights/2022-07-07-0-23-0-upgrade-guide/

---

## 6. Performance estimates and reference points

Treat ALL of these as order-of-magnitude. Vector's headline table is
~2019 (Vector 0.2.0) and self-published; scalo has design targets but no
committed benches.

- Vector self-published (MiB/s): TCP->Blackhole 86, File->TCP 76.7,
  TCP->HTTP 26.7; regex parsing 13.2 (Fluent Bit beats it at 20.5).
  https://github.com/vectordotdev/vector
- Vector sizing guidance (best current first-party): ~10-25 MiB/s per
  vCPU (10 unstructured, ~25 structured), ~2 GiB RAM/vCPU, "almost
  always CPU constrained". https://vector.dev/docs/setup/going-to-prod/sizing/
- Independent (VictoriaMetrics, Mar 2026, competitor - flag bias; 1 core
  / 1 GiB cap): Vector ~25,000 logs/s (3rd), Fluent Bit 31.3k. Vector
  flagged for a silent-loss default and an FD leak under load.
  https://victoriametrics.com/blog/log-collectors-benchmark-2026/
- Kafka per broker (the usual downstream): ~150-360 MB/s on commodity
  NVMe; small clusters ~600 MB/s-1 GB/s aggregate; **3x sync replication
  roughly halves it**. LinkedIn 2014, Confluent.
- Parsing tax: agents drop ~3-5x once regex/structured parsing is
  involved. This is exactly the cost scalo's parse-on-demand + SIMD
  pre-route is designed to dodge for route/forward workloads.
- simd-json / sonic-rs win big on large docs/NDJSON (GB/s) but the
  advantage is **muted on tiny per-message payloads** - serde_json can
  even win on small objects. scalo records are often small, so the SIMD
  win is real mainly via pre-route (avoid full parse) and batch
  amortisation, less via raw parser GB/s. (Langdale/Lemire VLDB 2019;
  simd-lite/simd-json README.)

**Defensible estimate, to be replaced by Section 7 results:** for a
route/filter/forward workload (no mutation), scalo should beat Vector
per-core because it skips the eager parse Vector pays at ingest - perhaps
1.5-3x on MiB/s for pass-through and light-route, narrowing toward parity
once every record is fully parsed and mutated (where Vector's model is
purpose-built). For the *same VRL program*, dfe-transform-vrl and
Vector's `remap` share the VRL crate, so per-event transform cost should
be close - the delta is runtime overhead (in-process vs Tokio task,
batch shape), NOT the language. These are hypotheses. Measure them.

---

## 7. Proposed Vector-baselining + integration benches (the deliverable)

Goal: a **default baseline comparator** in each dfe- app so `cargo bench`
gives real, repeatable numbers vs the equivalent Vector topology, on the
same corpus, on the same box. Use-case-driven, not micro-trivia. Two purposes
(from the framing note at the top): BASELINING (is the whole picture
better-or-not-worse than the Vector equivalent? - a regression/worth-it gate,
not a marketing scoreboard) and INTEGRATION (each app stays swap-in/swap-out
with a Vector pipeline, so a bench also doubles as an interop check).

### 7.0 Shared harness (build once, reuse everywhere)

- New dev-only support crate `scalo-baseline` (or a module under each app's
  `benches/`) providing:
  - **Canonical corpora** generators: small (200 B) and large (4 KiB)
    JSON log lines; structured vs unstructured; an OTLP metric set; a
    syslog stream; a msgpack variant. Fixed seed, committed as fixtures
    so Vector consumes the identical bytes. (No mocks of real deps - use
    real codecs and real transports via the `memory`/`file` backend and
    Vector's `blackhole`/`console` sinks.)
  - **Criterion** harness with throughput mode (MiB/s + events/s) and
    p50/p99 latency where relevant, flat sampling, 10% noise threshold
    (mirrors Vector's own bench config so the numbers are comparable).
  - A **Vector-side runner**: a checked-in `vector.toml` per scenario +
    a script that runs Vector over the same corpus to a `blackhole` sink,
    capturing its `component_received_events_count` and throughput. The
    bench report prints scalo vs Vector side by side.
  - A `BAKEOFF.md` per app capturing the box (vCPU/RAM/instance),
    versions, and the result table, regenerated by the bench.
- Discipline: every bench reports the box and version; never compare
  numbers across machines. Cap to 1 core / 1 GiB in a cgroup for the
  "per-core" table (matches the independent benchmark convention) AND run
  an uncapped "max throughput" table.

### 7.1 GA apps (commit full benches)

**dfe-receiver** - multi-protocol ingest. Use case: accept events at the
edge, normalise, forward.
- Benches: `http_json_ingest`, `grpc_ingest`, `otlp_ingest`,
  `syslog_ingest`, each -> memory/blackhole sink. Variants: 200 B vs
  4 KiB, structured vs unstructured, with-filter vs no-filter.
- Metrics: events/s, MiB/s, p50/p99 ingest-to-forward latency, alloc/event.
- Vector comparator: `http_server`/`opentelemetry`/`syslog` source ->
  `blackhole`. This is the cleanest ingest baseline.

**dfe-loader** - table routing / fan-out dispatch. Use case: route by
`_table` to N topics.
- Benches: `route_by_table_cel` (1->1 by CEL over the SIMD pre-route
  path), `fanout_transform` (N in -> M out, ack accounting),
  `route_cardinality_sweep` (4 / 64 / 1024 destinations).
- Metrics: routing decisions/s, pre-route extract ns/record (the headline
  scalo number - extract key without full parse), MiB/s, ack correctness.
- Vector comparator: `route` (or `exclusive_route`) transform + N sinks.
  Highlights scalo's "route on raw bytes" vs Vector's "parse then route".

**dfe-fetcher** - scheduled pull / poll. Use case: poll an API/container,
enrich, forward, persist cursor.
- Benches: avoid live external APIs (no mocks of real deps either) - use
  the **file** and **container/log extractor** sources over a fixed
  corpus. `poll_cycle_throughput`, `enrichment_cost` (timestamp/tag CEL),
  `cursor_overhead` (incremental fetch bookkeeping).
- Metrics: events/s per poll cycle, enrichment ns/event, cursor I/O cost.
- Vector comparator: `file`/`docker_logs` source -> `remap` (enrich) ->
  blackhole. (Vector has no cursor-pull-API analogue, so cursor_overhead
  is a scalo-only line - report it, do not fake a comparator.)

**dfe-archiver** - Kafka -> object storage. Use case: batch, compress,
roll, ship.
- Benches: `compress_roll_zstd|lz4|snappy|gzip`, `roll_by_size` (1 GB),
  `roll_by_time`, `multi_destination_fanout` (64 hot destinations).
- Metrics: MiB/s in, compressed MiB/s out, compression ratio, CPU/MiB,
  peak memory (hot buffer vs spool).
- Vector comparator: `kafka` source -> `aws_s3`/`gcp_cloud_storage` sink
  with matching batch/compression settings (use a local MinIO/S3-compatible
  endpoint via testcontainers, real backend, no mock).

**dfe-transform-vrl** - in-process VRL. Use case: parse/enrich/drop/remap.
- Benches: a VRL complexity ladder - `vrl_passthrough`, `vrl_parse_json`,
  `vrl_enrich`, `vrl_conditional_drop`, `vrl_heavy_remap` - over the same
  corpus.
- Metrics: events/s and ns/event per tier.
- Vector comparator: Vector `remap` transform with the **identical VRL
  source** (both embed the VRL crate). This is the single most honest
  bake-off in the suite: same language, same program, different runtime.
  The delta isolates scalo's in-process batch runtime vs Vector's Tokio
  task + EventArray. Expect near-parity; any large gap is a finding.

**dfe-transform-vector** - Vector subprocess wrapper. Use case: run
Vector under scalo's lifecycle/metrics.
- Benches: `wrapper_overhead` (scalo wrapper + Vector vs bare Vector,
  same config), `pipe_conversion_cost` (the JSON pipe in/out tax),
  `inprocess_vs_subprocess` (same transform via dfe-transform-vrl vs via
  dfe-transform-vector).
- Metrics: added latency/event, added MiB/s loss, added memory/CPU from
  the subprocess + pipe.
- Comparator: bare Vector is the baseline; the bench *measures the cost of
  the wrapper*, which is the whole point of this app. Likely to show the
  subprocess+pipe model is the slowest transform path - that result is
  itself the justification for preferring transform-vrl/wasm where
  possible.

### 7.2 Beta/alpha/spike apps (microbench + extrapolate)

These are not GA; do not over-invest. Land a single representative
microbench each, then extrapolate from the GA results using a measured
overhead factor. Print extrapolations clearly as estimates.

**dfe-transform-wasm** (spike) - WASM module per event (wasmtime).
- Microbench: `wasm_call_overhead` (host<->guest boundary per event),
  `wasm_instantiate` (module/instance reuse cost), `wasm_passthrough` vs
  `wasm_parse_enrich`.
- Extrapolation: throughput ~= dfe-transform-vrl throughput / wasm_factor,
  where wasm_factor is measured on passthrough then applied to the VRL
  ladder. Expect WASM slower than native VRL but faster than the Vector
  subprocess (no pipe/JSON re-encode, shared address space).
- Comparator: Vector has **no native WASM transform**, so the honest
  baseline is Vector's `lua` transform as the "scripting" reference, plus
  dfe-transform-vrl as the native floor. Position WASM between the two.

**dfe-transform-elastic** (beta) - Elasticsearch shaping/bulk.
- Microbench: `bulk_envelope_shaping` (events -> ES `_bulk` ndjson),
  `mapping_transform`. Real ES via testcontainers for an end-to-end line.
- Extrapolation: shape-only throughput from the microbench; end-to-end
  bounded by ES ingest, so report shaping cost separately from network.
- Comparator: Vector `elasticsearch` sink (same `_bulk`, same batch
  settings). Likely near-parity on shaping; the delta is batching policy.

**dfe-transform-splack** (beta) - Splunk HEC shaping.
- Microbench: `hec_envelope_shaping`, `hec_batch_pack`.
- Extrapolation: as for elastic - shaping cost from the microbench, scaled
  by the GA archiver/transform throughput for the full path.
- Comparator: Vector `splunk_hec` sink with matched batching.

### 7.3 Cross-app rollup

- A top-level `baseline-summary` (script, not a bench) aggregates each
  app's `BASELINE.md` into one table: app, scenario, scalo vs Vector,
  per-core and uncapped, with the box/version header. This becomes the
  baseline artefact we can regenerate per release and watch for regressions
  (scalo-vs-scalo over time AND scalo-vs-the-Vector-baseline).
- Wire it into hyperi-ci as a non-gating bench job first (numbers are
  noisy on shared runners); promote to a regression gate only once we
  have a dedicated, pinned bench box.

---

## 8. Honest caveats / not verified

From scalo side:
- scalo has **no committed benches today**; "PB/day", "PB/s" are design
  targets, not measurements. Section 7 exists precisely to fix this.
- The 1.5-3x pass-through estimate in Section 6 is a reasoned hypothesis
  from architecture, not a measurement.

From Vector / web research side:
- No dedicated EventArray RFC; the perf rationale is code + release notes
  together. ARC "default flipped in v0.17" is synthesis (current default
  IS adaptive, confirmed). Exact EC2 instance for Vector's own benches
  not published. No quantified Datadog at-scale numbers. jemalloc re-add
  PR/date not pinned (drop and current presence both confirmed).
  `LogEvent` Arc/CoW detail came via a secondary (DeepWiki) source. No
  primary VRL-vs-Lua head-to-head number. Vector buffer constants
  (100/500 events, 128 MiB/256 MiB/500 ms) are current-doc values that
  may drift by version.
- General-principle figures (Arrow 10-100x, morsels 30x, X100 multiples,
  Netflix/Envoy AIMD coefficients) are directional, from papers/blogs,
  not guarantees for our workload.
- Vendor/competitor bias flagged on Vector's own table, Confluent's Kafka
  numbers, and the VictoriaMetrics benchmark.

Bottom line: the architecture comparison is solid and cross-checked; the
*numbers* are not trustworthy until Section 7 lands. The benches are the
point.

---

## 9. What scalo should borrow from Vector (grounded gap analysis)

Read against actual source on 2026-06-26. Each item: what scalo has
today, what Vector has, the recommended disposition, and which dfe apps
benefit. Ranked by value x fit, effort noted.

### First, two myths cleared up

- **"scalo only scales vertically / did we drop vCPU autoscaling?"** No.
  scalo's vertical scaling is present and richer than Vector's. Vector
  just rides Tokio work-stealing across vCPUs. scalo sizes both the Tokio
  runtime and the rayon pool to cgroup-aware `available_parallelism()`
  (`worker/config.rs:176`) AND runs an adaptive watermark controller on
  top (`worker/scaler.rs`: grow/steady/down/emergency_down bands + a
  memory-pressure cap), nudging a parking semaphore each interval
  (`worker/pool.rs` `Semaphore`). The "vertical only" line in Section 2
  describes Vector, not scalo.
- **"Is ARC against our gate-inbound doctrine?"** No - they compose. The
  doctrine ("never throttle the drain to manufacture backpressure - it
  deadlocks the bounded buffer") is about *backpressure*. ARC is about
  *matching* drain concurrency to downstream capacity: run the sink as
  fast as the downstream accepts, no faster. One protects memory; the
  other optimises the sink. Wired together they close the loop scalo
  currently leaves open at the sink: ARC slows the drain to the
  downstream's real capacity -> buffers fill -> the existing inbound gate
  pauses the source. Today scalo has no graded sink control - only a
  binary circuit breaker (`tiered_sink/circuit.rs`) and fixed Kafka
  `max_in_flight: 5` (`transport/kafka/config.rs`).

### DO (high value, good fit)

**9.1 Adaptive outbound concurrency + adaptive sink batching (Vector ARC).**
scalo has: AIMD only on the INBOUND byte-budget (`governor/budget.rs`),
fixed sink concurrency, binary circuit breaker. Vector has: AIMD on a
per-sink in-flight limit driven by EWMA-RTT, default-on for HTTP sinks.
Recommendation: add an AIMD sink-concurrency controller in the transport
sender layer (http/grpc first), reusing the AIMD primitive already in
`governor/budget.rs`. Treat the existing circuit breaker as the hard
floor; ARC is the graded controller in front of it. Extend the same RTT
signal to size the outbound batch (Vector batches by size/time; make ours
shrink under rising RTT/errors). Effort: medium. Targets: dfe-fetcher
(SaaS/cloud APIs), dfe-transform-elastic (ES `_bulk`), dfe-transform-
splack (Splunk HEC), dfe-archiver (S3/GCS). This is the single most
material gap.

**9.2 Explicit overflow policy with DLQ-on-overflow (Vector when_full).**
scalo has: inbound gate (lossless) + spool-to-disk where spool-full is
FATAL (`tiered_sink/config.rs` `max_spool_bytes`/`max_spool_items` ->
error). Vector has: `block` / `drop_newest` / `overflow` per buffer.
Recommendation: add an overflow-policy enum to spool/tiered_sink:
`block` (current default, keep lossless) | `drop_newest` | `drop_oldest`
| `dlq`. The `dlq` variant is the on-brand one - overflow routes to the
existing DLQ instead of crashing, preserving no-silent-drop. Opt-in;
default stays lossless. Effort: low. Targets: all, especially archiver
and transforms under burst, and best-effort telemetry workloads where
shedding beats stalling.

**9.3 Active boot-time sink healthcheck (Vector healthcheck per sink).**
scalo has: passive callback health (`health/registry.rs` queries cached
state). Vector has: active probe per sink at boot, fail-fast on
unreachable. Recommendation: add an optional `healthcheck()` to the
Transport trait, called by the factory at startup (opt-out via config,
mirroring Vector's `healthcheck.enabled`). Catches wrong broker / bad
creds / unreachable bucket at boot instead of on first send. Effort: low.
Targets: all.

### CONSIDER (real, but scoped)

**9.4 Parse-once mutable transform lane (Vector Value + CoW) - scoped,
NOT a core change.** The user's flagged contrast is right: bytes-first is
the correct default for route/filter/forward (the 80%). Do NOT adopt
Vector's always-parsed core - it would tax the pass-through case scalo is
built to win. The genuine sub-gap is narrower:
  - Across apps, the Kafka boundary is bytes, so a 3-app transform chain
    re-parses 3x. That re-parse is the *price of durability/replay* and
    is a deliberate feature, not a bug - leave it.
  - Within a single app, a multi-step transform stage should parse ONCE
    and mutate a shared typed view, writing bytes back ONCE. Verify
    `ParsedBatch`/`run_workbatch_parsed` (`worker/engine`) already does
    this for sequential VRL files; if any path re-parses per step, fix
    it. Add copy-on-write on the parsed `Value` (Vector uses
    `Arc::make_mut`) so a mutating transform doesn't deep-copy untouched
    fields. Effort: medium. Targets: dfe-transform-vrl, -wasm, -elastic,
    -splack.

**9.5 Explicit rate limiter / throttle (Vector throttle transform).**
scalo has: none on the sink. Vector has: throttle transform + per-sink
rate caps. Partly subsumed by 9.1 (ARC handles *dynamic* capacity), but a
hard token-bucket cap is still needed for downstreams with a *contractual*
limit ("this API allows 100 req/s"). Recommendation: a token-bucket
limiter on the sender, configurable per route. Effort: low. Targets:
dfe-fetcher, -splack, -elastic.

### DECLINE (conscious non-goals - document them)

**9.6 First-class Metric/Trace data types (Vector's typed Metric).** scalo
converts OTLP metrics/traces to JSON and routes them as opaque records
(`dfe-receiver/src/server/otlp`). Vector keeps Metric first-class with
typed counter/gauge/histogram + arithmetic. Implementing this is a
product-surface expansion (metrics pipeline) that cuts against scalo's
"generic, unopinionated, bytes" positioning. Decline unless a consumer
needs in-pipeline metric aggregation; if so, do it as a metrics-aware
transform, not a core type. Record as a deliberate non-goal.

**9.7 Per-component MEMORY attribution (Vector allocation-tracing).**
Vector's per-component allocator costs ~20% throughput (hence opt-in).
scalo already has per-stage event/byte metric groups (`metrics/groups/`).
Keep those; decline the per-component memory tracker - the cost is not
worth it for a hot-path runtime. Global cgroup memory + PSI is enough.

### Already covered (no action - Vector parity or better)

- End-to-end acks / at-least-once with fan-out accounting: scalo's
  WorkBatch commit-token model (`transport/work_batch.rs`) already
  decouples acks from record count and is fan-out-safe. Equal to Vector.
- Restart durability: in the Kafka-mediated mode, Kafka IS the WAL
  (replay from offset; offset commits only after sink success). Vector
  needs a disk buffer to get this; scalo gets it free on the Kafka path.
  Only the direct-gRPC path lacks restart durability - note for that mode.
- Disk-backed buffering: scalo's spool (`spool/queue.rs`, yaque) +
  tiered_sink already spill to disk on sink failure. The gap there is the
  overflow *policy* (9.2), not the persistence itself.

### Suggested sequencing

1. 9.3 healthcheck + 9.2 overflow policy (both low-effort, high-operability,
   independent).
2. 9.1 ARC (the big one; land the AIMD sink controller, then fold in
   adaptive sink batching 9.x and the rate-limiter 9.5 on the same
   sender seam).
3. 9.4 verify/patch the within-app parse-once lane + CoW.
4. Bake the before/after into the Section 7 benches so each is measured,
   not asserted.
