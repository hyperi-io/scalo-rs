# Spool

Disk-backed async FIFO queue built on
[yaque](https://crates.io/crates/yaque). Crash-safe, persistent, with
optional zstd compression and bounded size. Used by `TieredSink` as
the disk-spillover tier; available as a standalone primitive for any
pipeline that needs durable store-and-forward.

For most workloads you want `TieredSink` (transport + spool + circuit
breaker + drain). Reach for `Spool` directly only when you're building
something `TieredSink` doesn't cover — e.g. an out-of-band replay
buffer, a checkpoint store, or a custom drainer.

---

## What it gives you

| Property | Notes |
| ---------- | ------- |
| FIFO order | Strict — yaque is a single-producer / single-consumer log |
| Persistent | Survives restarts; segment files in the queue directory |
| Crash-safe | Receiver position persisted in `recv-metadata`; commit-then-advance semantics |
| Async-native | Built on yaque's async receiver — `recv()` awaits when empty |
| Optional compression | zstd at construction-time level (1–22, clamped) |
| Bounded | `max_items` (count) and `max_size_bytes` (directory size) |

---

## Storage layout

Inside the configured `path`:

```text
spool.queue/
|-- 0.q                # segment file — [4-byte Hamming header][payload] ...
|-- 1.q
|-- ...
|-- recv-metadata      # 16 bytes: (segment u64 BE, position u64 BE)
`-- send-metadata
```

Segments roll over as they fill. The receiver position is two
big-endian u64s pointing into the segment file at the next byte to
read. On `recv` / `pop_front` the guard returned by yaque is
explicitly `commit()`'d — the position advances; on drop without
commit, the read rolls back and the item stays in the queue (this is
how `peek` works).

`Spool::open` rescans the directory on construction to recover the
item count after a restart — yaque doesn't expose a length API.

---

## Durability and recovery

- yaque writes durable per-message — a successful `push().await`
  means the bytes are in the segment file. (Whether the OS has
  fsync'd to disk depends on yaque's internal policy; for absolute
  durability the caller should `fsync` the directory out-of-band or
  use a journalled filesystem.)
- On restart, the receiver position is read from `recv-metadata` and
  scanning starts from there. Items consumed before the crash stay
  consumed; items not yet committed reappear.
- `clear()` walks the queue and commits every item — empties without
  touching the filesystem directly.

### Locks after a hard kill

yaque keeps `send.lock`, `recv.lock` and `version/lock` in the queue directory, each holding `pid=<pid>` and a random per-process `token`. A clean stop removes them. A kill -9 leaves them behind, and without recovery every restart would refuse the queue.

`Spool::open` and `TieredSink::new` (shared code in `src/spool_codec.rs`) handle each leftover lock before opening:

| Lock file | Action |
| ----------- | -------- |
| Owner pid not running, or equal to this process's pid with another token (a restarted container reusing its pid) | Removed, counted in `spool_stale_locks_cleared_total{lock,reason="dead_owner"}`, one warn with the path |
| Empty or unparseable (killed between create and write) | Re-read after 500 ms if younger than that, then removed as `reason="unparseable"` |
| Owner pid running, or this process's own open queue | Open refused with `Open` / `SpoolOpen` naming the lock. Never quarantined |

Two sinks or spools on one path in one process, or two processes sharing one volume, therefore fail at start instead of writing one queue twice. Give each its own path.

The liveness check reads the process table of the current pid namespace. Two pods sharing a `ReadWriteMany` volume cannot see each other's processes, so they must not share a spool path.

### Quarantine

When a queue still will not open after the lock check, or a CRC check fails on read, `on_corruption` decides (default `quarantine`):

- `quarantine` moves the queue's files into a new `corrupt-YYYYMMDD-HHMMSS-<nanos>-<n>/` subdirectory of the spool path and opens a fresh queue in place. The path itself is never renamed, so this works when the path is a mount point (an `emptyDir` or PVC mounted exactly at `spool_path`). Counted in `spool_quarantined_total{trigger="open_failed|crc_mismatch"}`, one warn with the path.
- `fail` returns the error.

Permission, full-disk, read-only and quota errors are returned under either policy, because moving the queue aside would orphan records that are still readable. Quarantined subdirectories are kept for forensics and are not counted against `max_size_bytes`. Remove them by hand once inspected.

---

## Compression

When `compress = true`, every payload is zstd-compressed before
`sender.send` and decompressed inside `recv` / `pop_front` /
`pop_front_async`. Compression level is config-controlled (default 3
— fast). Use higher levels (10+) for archival queues; default for
hot-path spool.

The choice is a one-shot at construction — there's no per-message
override.

---

## Bounded size

| Limit | Behaviour on exceeded |
| ------- | ---------------------- |
| `max_items: Some(n)` | `push` returns `Err(MaxItemsReached { max })` |
| `max_size_bytes: Some(b)` | `push` returns `Err(MaxSizeReached { max_bytes })` |

Both checks happen pre-write. The size check uses `file_size()` which
sums every regular file in the queue directory — exact for fresh
opens, slightly stale between segment rolls. Callers should treat
these as soft bounds; downstream pressure (DLQ, drop, throttle) is
the right response when they fire.

There's no built-in "drop oldest" mode — yaque is append-only and
removing oldest would require rewriting segment files. If you need
ring-buffer semantics, build it on top by combining `pop_front` (oldest)
with `push` (newest) under your own lock.

---

## When to use `TieredSink` instead

Use `TieredSink` if the answer to all of these is yes:

- The primary destination is a network sink (Kafka, gRPC, HTTP, S3).
- You want automatic spillover on transport failure.
- You want a circuit breaker and background drain back to primary.

Use `Spool` directly when:

- You're not retrying against an upstream sink — the spool **is** the
  destination (replay buffer, audit log, deferred-work queue).
- You need to peek or clear the queue, which `TieredSink` doesn't
  expose.
- You're building a custom drainer with non-standard semantics.

---

## Configuration

```yaml
spool:
  path: /var/spool/myapp/replay
  compress: true
  compression_level: 3
  max_items: 1000000
  max_size_bytes: 10737418240   # 10 GiB
```

Builder methods on `SpoolConfig` cover the common shapes —
`SpoolConfig::new(path)`, `SpoolConfig::with_compression(path)`,
`.compress(bool)`, `.compression_level(i)`, `.max_items(n)`,
`.max_size_bytes(b)`.

---

## Usage

```rust
use scalo::spool::{Spool, SpoolConfig};

let cfg = SpoolConfig::new("/var/spool/myapp")
    .compress(true)
    .max_items(1_000_000);

let mut spool = Spool::open(cfg).await?;

spool.push(b"event-1").await?;
spool.push(b"event-2").await?;

while let Some(data) = spool.pop_front().await? {
    process(&data).await?;
}
```

`recv()` is the async-await variant — it blocks when the queue is
empty (useful for a consumer task that should idle until work
arrives). `pop_front` is the try-style alternative that returns
`Ok(None)` instead of blocking.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `Spool::open(config)` | Open or create the queue; recovers item count from disk |
| `Spool::create(path)` | Convenience for `open(SpoolConfig::new(path))` |
| `Spool::create_compressed(path)` | Convenience for `open(SpoolConfig::with_compression(path))` |
| `push(data).await` | Append; checks `max_items` / `max_size_bytes` |
| `recv().await` | Wait for next item; commits on success |
| `pop_front().await` | Non-blocking pop; `Ok(None)` if empty |
| `peek().await` | Read without removing (guard rollback) |
| `pop().await` | Remove front without returning value |
| `len() / is_empty()` | Item count (tracked internally) |
| `file_size()` | Directory size in bytes |
| `clear()` | Drain every item; resets count |
| `config() -> &SpoolConfig` | Active config |
| `SpoolError` | `Open / Queue / Compression / Decompression / Io / MaxItemsReached / MaxSizeReached / Corrupted` |

---

## Source

- [`../../src/spool/mod.rs`](../../src/spool/mod.rs)
- [`../../src/spool/queue.rs`](../../src/spool/queue.rs) — `Spool`, yaque wrapper, item count recovery
- [`../../src/spool/config.rs`](../../src/spool/config.rs)
- [`../../src/spool/error.rs`](../../src/spool/error.rs)

---

## Related

- [tiered-sink.md](tiered-sink.md) — the primary consumer; handles the retry / circuit / drain semantics on top
- [dlq.md](dlq.md) — where to send messages when spool is full
- [../feature-flags.md](../feature-flags.md) — `spool` (pulls `zstd`)
- [../architecture.md](../architecture.md)
