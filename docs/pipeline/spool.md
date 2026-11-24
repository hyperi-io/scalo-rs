# Spool

Disk-backed async FIFO queue. Crash-safe, persistent, with optional zstd compression and bounded size, for any pipeline that needs durable store-and-forward. `TieredSink` does not use `Spool`: it runs its own queue for disk spillover and shares the locks, quarantine, sync and full-disk handling below.

The queue started as yaque 0.6.6 (<https://github.com/tokahuke/yaque>, Apache-2.0), copied into scalo as the private module `src/yaque` because upstream looked abandoned: no release and no commit to its default branch since 2023-11. scalo has since replaced its locks, its wake-up and its read path. The entry format is still yaque's, so a spool an earlier scalo wrote opens as before.

For most workloads you want `TieredSink` (transport + spool + circuit
breaker + drain). Reach for `Spool` directly only when you're building
something `TieredSink` doesn't cover -- e.g. an out-of-band replay
buffer, a checkpoint store, or a custom drainer.

---

## What it gives you

| Property | Notes |
| ---------- | ------- |
| FIFO order | Strict -- the queue is a single-producer / single-consumer log |
| Persistent | Survives restarts, segment files in the queue directory |
| Crash-safe | A record is committed only once read whole. The committed position is saved in `recv-metadata` every 250 records or 350 ms, and by each timed sync |
| Async-native | `recv()` awaits when empty, and the push in the same process wakes it. No file watcher, no thread |
| Optional compression | zstd at construction-time level (1-22, clamped) |
| Bounded | `max_items` (count) and `max_size_bytes` (bytes queued) |

---

## Storage layout

Directly in the configured `path`:

```text
<path>/
|-- 0.q                # segment file -- [4-byte header][record] ... [end marker]
|-- 1.q
|-- ...
|-- recv-metadata      # 16 bytes: (segment u64 BE, position u64 BE)
|-- send.lock          # OS-locked by the open spool
|-- recv.lock
`-- version/contents   # the format version
```

A segment takes records until it passes `segment_bytes` (16 MiB), then an end marker closes it and the next record starts a new file. A record bigger than a segment gets one to itself. The receiver position is two big-endian u64s pointing at the next record. A read is a transaction: `pop_front` and `recv` commit it, and `peek` hands the record back. A segment file is deleted once every record in it is committed.

`Spool::open` counts what is queued by walking the segments' headers from the saved position, seeking past each record rather than reading it.

---

## Durability and recovery

- A successful `push().await` has written the record with one write, so it survives the process being killed (`tests/spool_crash.rs` kills a writer with SIGKILL and reads every acknowledged record back). The receiver reads only records the sender has finished writing.
- A write that fails part way, on a full disk or an I/O error, is cut back to the last whole record before the error returns. A record a kill left torn at the end of the newest segment is cut off when the spool next opens, and a segment a kill left closed by its end marker is followed by a new one, so no record is ever written behind a torn one or a marker.
- The timed sync runs every `sync_interval_ms` (500 ms, 0 turns it off): it flushes the open segment, the directory when a segment was created since the last sync, and saves the read position. A power loss or a node crash loses at most about one interval of pushed records. A process kill loses none. `sync().await` does the same at once, and `sync_on_push: true` on every push, at the cost of one disk sync each. `close().await` runs a last sync.
- On restart, reading resumes from the saved position. Records committed since it was saved come again: up to 250, or 350 ms of them.
- The position is saved through a temporary file and a rename, so a kill leaves the old position or the new one. A power loss can still leave it naming a segment that is gone or an offset past the end of one. `Spool::open` and `TieredSink::new` detect either and replay from the first segment, with a warn, so records consumed from it before the crash come again. The queue is not quarantined.
- `clear()` reads and commits every record in bounded batches.

### Locks after a hard kill

The sending and receiving halves each hold an operating-system lock on a file in the queue directory, `send.lock` and `recv.lock` (`flock` on Linux and macOS, `LockFileEx` on Windows). The OS drops a lock with the process that held it, so a kill -9 leaves no lock behind and a restart opens the queue at once. The files stay and are reused. A queue another open holds, in this process or another, is refused with `Open` / `SpoolOpen`, and is never quarantined.

Two sinks or spools on one path in one process, or two processes sharing one volume, therefore fail at start instead of writing one queue twice. Give each its own path. A lock on a network filesystem holds only as far as that filesystem's locking does, so two pods sharing a `ReadWriteMany` volume must not share a spool path.

### Quarantine

When a queue will not open, or a CRC check fails on read, `on_corruption` decides (default `quarantine`). A queue will not open when its `version/contents` does not hold a version or names a format this build does not read, or the directory holds a `.q` file that is not a segment number. Each is an open failure naming the file, never a panic:

- `quarantine` moves the queue's files into a new `corrupt-YYYYMMDD-HHMMSS-<nanos>-<n>/` subdirectory of the spool path and opens a fresh queue in place. The path itself is never renamed, so this works when the path is a mount point (an `emptyDir` or PVC mounted exactly at `spool_path`). Counted in `spool_quarantined_total{trigger="open_failed|crc_mismatch|damaged_header"}`, one warn with the path.
- `fail` returns the error.

A record's 4-byte header carries parity bits. One that fails them is damaged, and no record after it can be found, so a read that meets it quarantines the queue under either policy, counted as `trigger="damaged_header"`.

Permission, full-disk, read-only and quota errors are returned under either policy, because moving the queue aside would orphan records that are still readable. Quarantined subdirectories are kept for forensics and are not counted against `max_size_bytes`. Remove them by hand once inspected.

### Full disk

A push the disk has no room for (`StorageFull`, a quota, or a file size limit) is cut back, writes nothing, and returns `Err(SpoolError::DiskFull)`. The caller backs off as it does at `max_size_bytes`. Pushes are then refused without touching the disk until a retry is due, the wait doubling from 100 ms to 5 s, and the first push that fits ends the wait. The spool waits on the space it already holds: popping deletes consumed segments. While the disk is full the `spool_disk` health component is Degraded. `spool_disk_full_refused_bytes_total` counts the refused bytes, and `spool_disk_full_wait_milliseconds_total` the time spent waiting.

### Disk use

A segment file goes as soon as every record in it is committed. Once the spool is empty, a segment holding a sixteenth of `segment_bytes` or more is closed and deleted, so a drained spool stops holding its disk. Nothing is preallocated. `spool_disk_bytes` reports what the segment files hold.

### Memory

A push copies nothing: the record goes to the disk from the caller's bytes, or from the compressor's reused buffer. The write side holds no records. A pop reads each record into a buffer kept for the next one, up to `read_buffer_bytes`, and `pop_front_into(&mut buf)` decodes into the caller's buffer, so a loop of pops allocates nothing. With `with_memory_guard(guard)`, each record's length is reserved before it is read and released once it is handed off. A refused reservation never refuses the pop itself.

---

## Compression

When `compress = true`, every payload is zstd-compressed before it is
written and decompressed inside `recv`, `pop_front` and `peek`, through
scalo's compression layer with its context reused. Compression level is
config-controlled (default 3 -- fast). Use higher levels (10+) for
archival queues, and the default for a hot-path spool.

The choice is a one-shot at construction -- there's no per-message
override.

---

## Bounded size

| Limit | Behaviour on exceeded |
| ------- | ---------------------- |
| `max_items: Some(n)` | `push` returns `Err(MaxItemsReached { max })` |
| `max_size_bytes: Some(b)` | `push` returns `Err(MaxSizeReached { max_bytes })` |

Both checks happen pre-write. The size check adds the record about to be
written, after compression and framing, to `queued_bytes()`: the bytes of
the records queued, counted from the segment files once at open and moved
by every push and take, so a push never walks the directory. A drained
spool has room again even while its segment file still holds the records
it consumed. Downstream pressure (DLQ, drop, throttle) is the right
response when either check fires.

There's no built-in "drop oldest" mode -- the queue is append-only and
removing oldest would require rewriting segment files. If you need
ring-buffer semantics, build it on top by combining `pop_front` (oldest)
with `push` (newest) under your own lock.

---

## When to use `TieredSink` instead

Use `TieredSink` if the answer to all of these is yes:

- The primary destination is a `TransportSender` (Kafka, gRPC, HTTP, file, pipe).
- You want automatic spillover on transport failure.
- You want a circuit breaker and background drain back to primary.

Use `Spool` directly when:

- You're not retrying against an upstream sink -- the spool **is** the
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
  sync_on_push: false           # true syncs to disk before every push returns
  sync_interval_ms: 500         # 0 turns the timed sync off
  segment_bytes: 16777216       # 16 MiB
  read_buffer_bytes: 65536      # 64 KiB
```

Builder methods on `SpoolConfig` cover the common shapes --
`SpoolConfig::new(path)`, `SpoolConfig::with_compression(path)`,
`.compress(bool)`, `.compression_level(i)`, `.max_items(n)`,
`.max_size_bytes(b)`, `.sync_on_push(bool)`, `.sync_interval_ms(ms)`,
`.segment_bytes(b)`, `.read_buffer_bytes(b)`.

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

`recv()` is the async-await variant -- it blocks when the queue is
empty (useful for a consumer task that should idle until work
arrives). `pop_front` is the try-style alternative that returns
`Ok(None)` instead of blocking.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `Spool::open(config)` | Open or create the queue, recovering the item count from disk |
| `Spool::create(path)` | Convenience for `open(SpoolConfig::new(path))` |
| `Spool::create_compressed(path)` | Convenience for `open(SpoolConfig::with_compression(path))` |
| `push(data).await` | Append, checking `max_items` / `max_size_bytes`, and syncing first under `sync_on_push` |
| `sync().await` | Sync every record pushed so far to disk, on tokio's blocking pool |
| `recv().await` | Wait for next item, committing on success |
| `pop_front().await` | Non-blocking pop, `Ok(None)` if empty |
| `pop_front_into(&mut buf).await` | `pop_front` into the caller's buffer, `Ok(false)` if empty. Allocates nothing once the buffers are warm |
| `peek().await` | Read without removing (guard rollback) |
| `pop().await` | Remove front without returning value |
| `len() / is_empty()` | Item count (tracked internally) |
| `queued_bytes()` | Bytes of the records queued, the count `max_size_bytes` caps |
| `file_size()` | Directory size in bytes, consumed records included. Walks the directory |
| `clear()` | Drain every item and reset the count |
| `close().await` | Stop the timed sync after one last sync |
| `with_memory_guard(guard)` | Reserve each record's length before reading it (`memory` feature) |
| `config() -> &SpoolConfig` | Active config |
| `SpoolError` | `Open / Queue / Compression / Decompression / Io / MaxItemsReached / MaxSizeReached / Corrupted / DiskFull` |

---

## Metrics

With the `metrics` feature, a spool reports `spool_queue_depth`, and with `store="spool"` the series the tiered sink's spill reports under `store="tiered_sink"`: `spool_bytes_in_total`, `spool_bytes_written_total`, `spool_codec_microseconds_total`, `spool_disk_bytes`, `spool_disk_full_refused_bytes_total`, `spool_disk_full_wait_milliseconds_total`, `spool_sync_seconds`, `spool_memory_reserved_bytes`, `spool_position_save_failures_total`, `spool_segment_delete_failures_total`, `spool_torn_entries_cut_total` and `spool_quarantined_total`. [tiered-sink.md](tiered-sink.md#metrics) describes each.

---

## Source

- [`../../src/spool/mod.rs`](../../src/spool/mod.rs)
- [`../../src/spool/queue.rs`](../../src/spool/queue.rs) -- `Spool` over the queue, item count recovery
- [`../../src/spool/config.rs`](../../src/spool/config.rs)
- [`../../src/spool/error.rs`](../../src/spool/error.rs)
- [`../../src/yaque/`](../../src/yaque/) -- the queue, derived from yaque 0.6.6
- [`../../src/spool_codec.rs`](../../src/spool_codec.rs) -- open, quarantine, sync, full disk and metrics, shared with `TieredSink`

---

## Related

- [tiered-sink.md](tiered-sink.md) -- transport, its own spill queue, circuit breaker and drain in one
- [dlq.md](dlq.md) -- where to send messages when spool is full
- [../feature-flags.md](../feature-flags.md) -- `spool` (pulls `compression`)
- [../architecture.md](../architecture.md)
