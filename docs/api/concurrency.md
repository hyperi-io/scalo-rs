# Concurrency

Three async primitives for long-lived module code:
`BackgroundSink`, `PeriodicWorker`, `ActorHandle`. None of them are
novel -- they're the conventional answers to fire-and-forget durable
writes, timer-driven loops, and command-queue actors. Centralising
them here keeps the implementations consistent across subsystems and
keeps modules from rolling their own variants.

If you're writing a new long-lived module, pick from this menu before
reaching for `tokio::spawn` + a custom channel.

---

## Decision matrix

| Workload shape | Use |
| ---------------- | ----- |
| Stateless function | Just call the function. No primitive needed. |
| Per-request short-lived work | `tokio::spawn` + `JoinSet` |
| Pure CPU batch | `rayon::par_iter` |
| Read-heavy shared state | `Arc<RwLock<T>>` |
| Fire-and-forget durable writes | [`BackgroundSink`](#backgroundsink) |
| Timer-driven loop | [`PeriodicWorker`](#periodicworker) |
| Long-lived state behind a command queue | [`ActorHandle`](#actorhandle) |
| Pipeline stage with backpressure | `tokio::mpsc::channel(N)` |

The three primitives below cover the cases the rest of the rows leave
to "build it yourself". Use them.

---

## `BackgroundSink`

Fire-and-forget durable write. Caller `try_push`es items, and an actor task
drains them through a `SinkDrain` impl that writes durably (disk file,
DLQ queue, Kafka topic). Caller returns immediately -- the actor does
the I/O.

The [DLQ](../pipeline/dlq.md) queues its entries through one. It suits any
subsystem that needs durable side-channel writes without blocking the
hot path.

```rust
use scalo::concurrency::{BackgroundSink, BackgroundSinkConfig, DrainError, Overflow, SinkDrain};
use tokio::io::AsyncWriteExt;

struct NdjsonDrain { path: PathBuf }

impl SinkDrain<Event> for NdjsonDrain {
    async fn write_batch(&mut self, batch: Vec<Event>) -> Result<(), DrainError> {
        let mut buf = Vec::new();
        for event in &batch {
            serde_json::to_writer(&mut buf, event).map_err(|e| DrainError::Backend(Box::new(e)))?;
            buf.push(b'\n');
        }
        // async file I/O only -- a std::fs call here stalls the actor's runtime thread
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(&buf).await?;
        Ok(())
    }
}

// spawn returns a cloneable sink plus a single-owner join handle.
let (sink, handle) = BackgroundSink::spawn(
    NdjsonDrain { path: "/var/log/events.ndjson".into() },
    BackgroundSinkConfig {
        queue_capacity: 10_000,
        overflow: Overflow::Drop,
        batch_size: 256,
        flush_interval: Duration::from_millis(500),
        ..Default::default()
    },
    shutdown_token.clone(),
);

// Hot path -- non-blocking:
sink.try_push(event)?;

// Periodic stats:
let dropped = sink.dropped();
let pending = sink.pending();

// On shutdown:
sink.flush().await?;
shutdown_token.cancel();
handle.join().await?;
```

Overflow modes: `Drop` (the default -- `try_push` returns
`Err(Overflow)` immediately and bumps the dropped counter, so pick this
for telemetry) and `Block` (`try_push` is rejected unconditionally,
forcing callers to use `push_blocking`/`flush`, so pick this when the
sink must never silently drop, e.g. billing).

`push_blocking()` waits for capacity (use sparingly, since the point of this
primitive is `try_push` returning immediately). In `Block` mode it is
the only push path -- `try_push` always returns `Err(Overflow)`.

---

## `PeriodicWorker`

Timer-driven loop. The task implements a `PeriodicTask` trait, and the worker drives
the schedule with `MissedTickBehavior::Delay` (if the task takes longer
than the interval, the next fire is delayed, not piled up).

It suits any subsystem with a "check every N seconds" requirement.

```rust
use scalo::concurrency::{PeriodicTask, PeriodicWorker, TickError};

struct Refresh;

impl PeriodicTask for Refresh {
    async fn tick(&mut self) -> Result<(), TickError> {
        refresh_lookup_table().await;
        Ok(())
    }
}

let worker = PeriodicWorker::spawn(
    Refresh,
    Duration::from_secs(5),
    shutdown_token.clone(),
);

// On shutdown:
worker.join().await?;
```

The first tick fires one interval after `spawn`, so a fleet that starts
together doesn't hit a dependency at once. `spawn_immediate` ticks at
spawn as well, for a gauge or pool size that is read from startup.

The shutdown token argument is mandatory -- periodic workers always
take the global cancellation token so they drain cleanly on SIGTERM.

---

## `ActorHandle`

Command-queue actor. It owns mutable state, readers send commands through
an `mpsc` channel, and the actor task processes them serially. Use when
state needs serialised access AND lives for the process lifetime.

It suits anywhere "one writer, many readers, long-lived" applies and a
lock won't do (e.g. multi-step command sequences, side-effects between
commands).

```rust
use scalo::concurrency::{Actor, ActorConfig, ActorHandle};

enum Cmd {
    AddRule(Rule),
    RemoveRule(RuleId),
    Snapshot(oneshot::Sender<RuleSet>),
}

struct RulesActor { state: RuleSet }

impl Actor for RulesActor {
    type Command = Cmd;
    async fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::AddRule(r) => self.state.insert(r),
            Cmd::RemoveRule(id) => self.state.remove(&id),
            Cmd::Snapshot(reply) => { let _ = reply.send(self.state.clone()); }
        }
    }
}

// spawn returns a cloneable handle plus a single-owner join handle.
let (handle, join) = ActorHandle::<Cmd>::spawn(
    RulesActor { state: RuleSet::new() },
    ActorConfig { queue_capacity: 256, ..Default::default() },
    shutdown_token.clone(),
);

handle.send(Cmd::AddRule(rule)).await?;
let (tx, rx) = oneshot::channel();
handle.send(Cmd::Snapshot(tx)).await?;
let snapshot = rx.await?;

// On shutdown:
join.join().await?;
```

`try_send` for non-blocking pushes, `send` for back-pressure. The
`oneshot` reply pattern is how you do request-response.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `BackgroundSink::spawn(drain, config, shutdown)` | Spawn the actor task, returning `(BackgroundSink, BackgroundSinkHandle)` |
| `.try_push(msg)` | Hot-path push, returns immediately |
| `.push_blocking(msg)` | Await capacity |
| `.flush()` | Wait until every message queued before the call is written, then run `flush_durable`. `Err(SinkError::Drain)` with the first failure since the previous flush -- a size, tick, shutdown or barrier batch write, or `flush_durable`. Reported once: the next flush starts clean |
| `.dropped() -> u64` | Count of dropped messages (overflow) |
| `.pending() -> usize` | Current queue depth |
| `BackgroundSinkHandle::join()` | Await the actor task exit (single-owner) |
| `PeriodicTask` trait | Implement `tick()` |
| `PeriodicWorker::spawn(task, interval, shutdown)` | Drive the schedule, first tick one interval in, returning `PeriodicWorker` |
| `PeriodicWorker::spawn_immediate(task, interval, shutdown)` | As `spawn`, first tick at once |
| `PeriodicWorker::join()` | Await worker exit |
| `Actor` trait | Implement `handle(&mut self, Command)` |
| `ActorHandle::spawn(actor, config, shutdown)` | Spawn an actor, returning `(ActorHandle, ActorJoinHandle)` |
| `.send(cmd)` | Await capacity, deliver command |
| `.try_send(cmd)` | Non-blocking variant |
| `ActorJoinHandle::join()` | Await the actor task exit (single-owner) |

---

## Related

- [../pipeline/dlq.md](../pipeline/dlq.md) -- `BackgroundSink` consumer
- [../core-pillars/shutdown.md](../core-pillars/shutdown.md) -- `CancellationToken` discipline
- [../feature-flags.md](../feature-flags.md) -- `concurrency`
- Source: [../../src/concurrency/](../../src/concurrency/)
