# Memory

`MemoryGuard` is the cgroup-aware OOM-prevention layer. It tracks
process memory against a detected or configured limit and exposes a
fast atomic `under_pressure()` check that hot-path code reads to
decide whether to accept more work or shed load.

It is a *guard*, not a *limit*. scalo never OOM-kills its own
process and takes no allocator dependency (`#![forbid(unsafe_code)]`).
It surfaces pressure; the caller decides what to do with it.

---

## Why guard, not limit

The kernel OOM-kills the process when the cgroup limit is hit -- the
worst outcome: in-flight requests vanish, on-disk state can tear, K8s
restarts the pod with no graceful drain. `MemoryGuard` lets the
process refuse new work *before* the kernel reaches for the axe.

Memory pressure brakes INBOUND intake only -- never the outbound
drain. Gating the drain deadlocks (you stop the thing that frees
memory). See the backpressure doctrine in SELF-REGULATION.

| Consumer | Behaviour on `under_pressure()` |
|----------|--------------------------------|
| HTTP server | 503 Service Unavailable |
| Kafka receiver | Pause partition assignment |
| `TieredSink` | Spill in-memory buffer to disk |
| `BatchEngine` | Smaller batches, more frequent flushes |
| `ScalingPressure` | Bump KEDA signal toward 1.0 |

The guard publishes the ratio; it owns none of these policies.

---

## Limit detection

`MemoryGuard::new(config)` reads `config.limit_bytes`. Zero (the
default) auto-detects via `detect_memory_limit`:

| Priority | Source | File |
|----------|--------|------|
| 1 | cgroup v2 | `/sys/fs/cgroup/memory.max` |
| 2 | cgroup v1 | `/sys/fs/cgroup/memory/memory.limit_in_bytes` |
| 3 | system memory | `sysinfo::System::total_memory()` |

Detected limit is scaled by `cgroup_headroom` (default 0.85) to leave
room for code, stack, allocator fragmentation, and cgroup-attributed
page cache. Rust has no GC, so no spike headroom is needed -- the 15%
gap is just the cost of being a process.

A 4 GiB cgroup with defaults -> ~3.4 GiB effective limit; backpressure
at 80% of that (~2.72 GiB, ~68% of the real cgroup limit). Matches the
OTel Collector's `limit_percentage: 80` philosophy.

---

## Usage detection

Both halves of the pressure ratio come from the kernel. `UsageSource`
is resolved once per guard, in this order:

| Priority | Source | File | Name in the init log |
|----------|--------|------|----------------------|
| 1 | cgroup v2 | `/sys/fs/cgroup/memory.current` | `cgroup-v2` |
| 2 | cgroup v1 | `/sys/fs/cgroup/memory/memory.usage_in_bytes` | `cgroup-v1` |
| 3 | procfs | `/proc/self/status` `VmRSS` | `proc-status` |
| 4 | reservations | -- | `reservations` |

Rung 1 is the number `memory.max` is compared against, so the guard
sheds on the same figure the OOM killer acts on, whatever allocator the
binary installed. Rung 3 covers a Linux host running no cgroup memory
controller; it reads kB, so no page-size constant can be wrong on a 16K
or 64K kernel. Rung 4 is the last resort where no kernel accounting is
readable at all (non-Linux): the guard then sees only bytes callers
reserved by hand, and says so with a warn at init.

The file read is cached for 50 ms. The guard is sampled per payload on
the receive path, and the kernel charges memory in per-CPU batches, so
a reading is approximate below that interval anyway.

Bytes admitted since that reading are added to it, so a burst inside one
window is charged against the limit instead of against a reading that
predates it. The ledger is cleared whenever a fresh sample is taken --
before the file is read, so an admission racing the sample is
double-counted rather than dropped.

`current_bytes()` is that estimate -- what the kernel charges, plus what
has been admitted against the current reading. `reserved_bytes()` is the
outstanding byte leases from `try_reserve`/`add_bytes` less `release`.
They are different numbers and both are exposed.

### Heap source (allocator override, opt-in)

`memory::set_heap_source(fn() -> usize)` registers a process-wide
total-live-heap reader (set once at startup) that overrides the
detected source, for a service that wants to gate on the allocator's
own figure. It is narrower than the cgroup: it cannot see thread
stacks, mmap'd buffers, or pages the allocator retains after a free.

The source is allocator-agnostic: pass `cap::Cap::allocated`, a
jemalloc `stats.allocated` reader (advance the epoch inside the
closure), or any `fn() -> usize`. scalo itself depends on no
allocator -- the choice is the binary's.

`try_reserve(n)` is a projected-admission check (`usage() + n <=
limit`) against whichever source is in force, and does NOT mutate the
reservation counter -- the kernel uncharges the bytes when they are
freed, so no `release` is needed to keep the check honest. It does
charge the ledger, so callers behind it in the same cache window see the
admission.

---

## Allocator

scalo picks no allocator. The binary does, and jemalloc is the usual pick for a long-running, multi-threaded data-plane service: one set of heap stats and one profiling story.

The guard does not read the allocator. In a container it reads the kernel's `memory.current`, which counts every page the process holds, arenas the allocator keeps after a free included. An allocator figure such as jemalloc's `stats.allocated` is read only when the binary registers it with `set_heap_source`, and it then replaces the kernel figure. A binary that wants the kernel figure in a container and the allocator figure elsewhere registers the source only when no cgroup usage file is readable.

Why the `tikv-` names? The original `jemallocator` crate stopped at 0.5.4 on 2023-07-27. The TiKV project carries it on as `tikv-jemallocator`, from the same repo: https://github.com/tikv/jemallocator. As of 2026-09-26 `tikv-jemalloc-sys` is at 0.7.1, published 2026-05-25. So `tikv-` is the maintained line, not a side fork.

Three crates, three jobs:

- `tikv-jemalloc-sys` -- 'build jemalloc and link it in'. It compiles jemalloc's C source and statically links it into the binary. Nothing depends on it directly, it comes in under the other two. The version names the upstream build: `0.7.1+5.3.1-0-g81034ce1` is jemalloc 5.3.1 at that commit.
- `tikv-jemallocator` -- the `#[global_allocator]` shim.
- `tikv-jemalloc-ctl` -- 'ask jemalloc how much it holds'. Turn on its `stats` feature or there are no stats to read.

Why not a native Rust allocator? Rust ships none of its own. Without jemalloc a binary gets the platform's C `malloc`, glibc on Linux. So the real choice is which C allocator, and jemalloc wins on heap stats and profiling, which mimalloc and snmalloc do not match.

---

## Thresholds

```yaml
memory:
  limit_bytes: 0           # 0 = auto-detect
  pressure_threshold: 0.80 # backpressure at 80% of effective limit
  cgroup_headroom: 0.85    # use 85% of detected cgroup limit
```

```rust
pub enum MemoryPressure {
    Low,     // ratio < 0.5
    Medium,  // 0.5 <= ratio < pressure_threshold
    High,    // ratio >= pressure_threshold -- apply backpressure
}
```

`pressure_threshold` is the only knob the hot path cares about. There
is no separate warn/soft/hard tier: the hot-path API is binary
(`under_pressure() -> bool`); `pressure()` is for log/metric labels.

---

## Hot-path API

Lock-free atomics throughout, plus the usage file read at most once per
50 ms.

```rust
let guard = Arc::new(MemoryGuard::new(MemoryGuardConfig::from_env("MYAPP")));

// On data arrival -- atomic check, rolls back if it would exceed:
if !guard.try_reserve(payload_len) {
    return Err(BackpressureError::MemoryFull);   // 503 / pause / spill
}

// After data is flushed/sent/dropped (lease accounting; admission reads the kernel):
guard.release(payload_len);

// Cheap hot-path probe:
if guard.under_pressure() {
    return shed_load();
}
```

| Operation | Cost |
|-----------|------|
| `try_reserve(n)` | one cached usage read + compare + `fetch_add` on the ledger (rollback on the reservation counter only on rung 4) |
| `add_bytes(n)` | two `fetch_add` + threshold update |
| `release(n)` | two saturating `fetch_update` -- over-release floors at zero |
| `under_pressure()` | one cached usage read + compare; one file read per 50 ms |
| `pressure_ratio()` | the same read + one float division; >1.0 means misconfigured limit |

---

## Self-regulation

Self-regulation (the `governor` feature) is ON by default; opt out via
`self_regulation.enabled = false`, after which nothing is constructed
and the data path is byte-identical to pre-governor. It consumes the
guard's pressure to drive the inbound brake and an AIMD byte budget.
Memory the process already holds can keep the ratio above the brake's
release point with nothing coming in, so one hold lasts at most
`self_regulation.max_hold_secs` (default 30). Its metrics are namespaced
`self_regulation_*`. See SELF-REGULATION.

`ScalingPressure` consumes the guard too: memory is a hard gate. When
the ratio exceeds ~0.9 the autoscaler signal jumps straight to maximum
to force scale-up before OOM-kill, bypassing the weighted composite.

---

## ServiceRuntime wiring

`ServiceRuntime::build` constructs the guard from the env prefix and
hands the `Arc<MemoryGuard>` to:

- `AdaptiveWorkerPool` via `set_memory_guard(...)` -- scales down under
  pressure.
- `BatchEngine` via `auto_wire(..., Some(&memory_guard))` -- reduces
  batch size under pressure.
- The self-regulation governor (built from the same guard).

Apps read `runtime.memory_guard` directly. Env-var overrides
(`MYAPP_MEMORY_LIMIT_BYTES`) work without bridging because
`build` uses `from_env(env_prefix)`.

Env vars:

- `{PREFIX}_MEMORY_LIMIT_BYTES` -- explicit override
- `{PREFIX}_MEMORY_PRESSURE_THRESHOLD` -- float, default 0.80
- `{PREFIX}_MEMORY_CGROUP_HEADROOM` -- float, default 0.85

---

## API surface

| Item | Purpose |
|------|---------|
| `memory::set_heap_source(fn() -> usize) -> bool` | Override the detected source with an allocator reader (set-once; returns false if already set) |
| `MemoryGuard::new(config)` | Construct; detects the usage source, and the limit if `limit_bytes == 0` |
| `MemoryGuard::with_usage_source(config, source)` | Construct reading usage from a pinned `UsageSource` |
| `MemoryGuard::try_reserve(n) -> bool` | Projected-admission check against current usage |
| `MemoryGuard::add_bytes(n)` | Unchecked lease tracking -- data already accepted |
| `MemoryGuard::release(n)` | Saturating subtract on the reservation counter and the ledger |
| `MemoryGuard::under_pressure() -> bool` | Hot-path probe |
| `MemoryGuard::pressure() -> MemoryPressure` | Three-level enum for logs/labels |
| `MemoryGuard::pressure_ratio() -> f64` | Usage as fraction of effective limit |
| `MemoryGuard::current_bytes() -> u64` | What the kernel charges, plus bytes admitted since that reading |
| `MemoryGuard::reserved_bytes() -> u64` | Outstanding byte leases, not process usage |
| `MemoryGuard::usage_source() -> &'static str` | Which source is in force |
| `MemoryGuard::limit_bytes() -> u64` | Effective limit (after headroom) |
| `UsageSource` | `CgroupV2` / `CgroupV1` / `ProcStatus` / `Reservations` |
| `MemoryGuardConfig` | Serde-deserialisable config struct |
| `MemoryGuardConfig::from_cascade()` | Load from the 7-layer cascade |
| `MemoryGuardConfig::from_env(prefix)` | Build from `{PREFIX}_MEMORY_*` env vars |
| `MemoryPressure` | `Low` / `Medium` / `High` |
| `cgroup::detect_memory_limit() -> u64` | Standalone limit detection |
| `cgroup::detect_memory_pressure() -> Option<f64>` | This container's `current/limit` |

---

## Two-layer model

| Layer | Default | Behaviour |
|-------|---------|-----------|
| 1 -- cap allocator | opt-in | Hard cap; last-resort crash via `handle_alloc_error` instead of OOM-kill |
| 2 -- `MemoryGuard` | on | Cgroup-aware tracking + backpressure signal |

Layer 2 is what 99% of services need. Layer 1 is a seatbelt for
binaries that can't trust every dependency to honour backpressure.

---

## Related

- [self-regulation.md](../self-regulation.md) -- governor, inbound brake, AIMD budget
- [runtime-context.md](runtime-context.md) -- cgroup limit detection
- [service-runtime.md](service-runtime.md) -- `ServiceRuntime` holds the guard
- [../feature-flags.md](../feature-flags.md) -- `memory`, `governor`
- Source: [../../src/memory/guard.rs](../../src/memory/guard.rs),
  [../../src/memory/cgroup.rs](../../src/memory/cgroup.rs),
  [../../src/memory/usage.rs](../../src/memory/usage.rs)
