# Acknowledgements

A source's acknowledgement is what it is told once its records are safe: a Kafka offset commit, a gRPC answer. The `BatchEngine` pipeline builder releases it only once every piece built from a block is delivered, dead-lettered with the DLQ's confirmation, or dropped by policy. A failed piece withholds it, so the record is delivered again: duplicates are possible, loss is not.

```rust
engine
    .pipeline(&receiver)
    .shutdown(shutdown)
    .sender(&sender) // what the sink confirms, and what it would refuse
    .run(|batch| Ok(batch), |out| send(out))
    .await?;
```

---

## Pieces and release

A **piece** is one downstream delivery covering part of a block: each sink call (the whole block, or a sub-block while the governor streams), the block's dead letters, and any piece the sink takes itself. `run_with_pieces` hands the sink a `BlockPieces` for that: a sink that fans out to several destinations, or confirms a write later, takes a piece per destination and reports it when the write lands. A piece dropped without a report counts as `Errored`.

Once every piece has reported, the loop calls `TransportReceiver::release` once with the merged status:

| Merged status | Source |
|---------------|--------|
| `Delivered`, `Dropped`, `Rejected` (dead-lettered, and the DLQ confirmed it) | Released: Kafka commits, a held push is answered OK |
| `Errored` | Withheld. Kafka does not commit, so the block is read again after a restart, and the loop stops. A push source answers `UNAVAILABLE`, its sender retries, and the loop goes on |

A push source gives each block a hold deadline (`TransportReceiver::hold_deadline`). A block the sink is still refusing 100 ms before it is released `Errored`, so the sender is answered before its own deadline and retries, and the loop moves to the next block. A pull source has no deadline and keeps the hold-and-retry of the other run loops.

A block the loop stops holding before it is released -- a panic in `process` or the sink, or the run future dropped mid-block by a `select!` or an aborted task -- is released `Errored` as it goes. A push source answers its sender at once and frees the bytes the request held, rather than keeping them until the transport is dropped. That release runs from a drop, which cannot await, so it is polled once: a custom receiver's `release` does an `Errored` release before its first `.await`, or the block stays unreleased and a WARN says so.

---

## The acknowledgements key

```yaml
kafka:
  acknowledgements:
    enabled: true   # the default
```

The key sits beside the transport's own section: `AnyReceiver::from_config(key)` reads `<key>.kafka.acknowledgements` or `<key>.grpc.acknowledgements`, and a transport built from an explicit config takes it with `KafkaTransport::with_acknowledgements` or `GrpcTransport::builder(..).acknowledgements(..)`. Pipe and memory have no acknowledgement to hold, and the factory warns once when the key sits under either.

- **On:** the loop arms the source (`AckControl::arm`) before the first `recv`. Kafka then commits each partition only up to its lowest offset not yet released, whatever order releases arrive in, and an `Errored` offset holds the commit below it. A push source answers its sender only on release, once armed. Before that it answers at enqueue, so a push that arrives between the server starting and the loop's `arm` is acknowledged with nothing to deliver it. Build a push source armed instead: `GrpcTransport::builder(..).armed(true)`, or `AnyReceiver::from_config_armed(key)` and `from_config_with_governor_armed(key, governor)`. The loop's own `arm` then changes nothing.
- **Off:** the source is released at receipt, before the block is processed. A crash or a failed delivery loses what was released.
- **A source with no acknowledgement:** released after the pieces, as the other run loops commit.

---

## Dead letters

With `BatchEngine::with_dlq(Arc<Dlq>)`, a block's dead letters -- inbound filter matches, entries `process` adds, and records the sink would refuse -- are one piece, written with `Dlq::write_confirmed`. It reports `Rejected` once a DLQ backend holds them and `Errored` when the DLQ refuses them, so the source is never released for a dead letter the DLQ does not hold. The answer covers this block's own write, so a refusal of another writer's entries never reaches it ([dlq.md](dlq.md#queue-admission-semantics)). A disabled DLQ reports `Dropped` and counts each in `pipeline_dead_letters_dropped_total{reason}`, where `reason` is `dead_letter` for one an inbound filter or `process` produced.

A sender names the records it would dead-letter rather than send (`TransportSender::dead_letter_reason`): Kafka names a record over `message.max.bytes` less 128 bytes of framing, gRPC one over `max_message_size` on its own, and both an outbound `dlq` filter match. The sender answers such a record `FilteredDlq`, or leaves it out of a gRPC block, without writing it anywhere, and `send_batch` counts it handled, so `.sender(&sender)` has the loop take them out of the block before the sink is called. Without a DLQ they go through `FilterDlqPolicy::Route` when one is set. Otherwise they are dropped, counted in `pipeline_dead_letters_dropped_total{reason}`, and the block releases `Dropped`, never `Delivered`.

An app whose sink writes to a scalo transport passes that transport with `.sender(&sender)`. Without it nothing screens the block: `send_batch` drops such a record and the block releases `Delivered`. A sink that is not a transport -- a database writer that refuses a record itself -- has no transport screen to miss, and declares what its `Ok` proves with `.sink_confirms(..)` instead. A pipeline with neither logs one WARN at start and reports `best_effort` / `sink_cannot_confirm` (below).

Without a DLQ, entries from filters and `process` go through the `FilterDlqPolicy` as in the other run loops. `Route` reports `Rejected` once its closure returns `Ok`, which does not prove the entry is durable. `with_dlq` does.

---

## The guarantee, as a metric

At start the loop sets `pipeline_delivery_guarantee{guarantee, reason}` to 1:

| `guarantee` | `reason` | When |
|-------------|----------|------|
| `at_least_once` | `confirmed` | The source holds its ack and the sink confirms remotely (`SinkConfirmation::Remote`: Kafka, gRPC) |
| `at_least_once_local` | `sink_confirms_locally` | The sink confirms a durable local write |
| `best_effort` | `sink_cannot_confirm` | The sink's `Ok` proves nothing more, including a pipeline with neither `.sender(&sender)` nor `.sink_confirms(..)`. `.sink_confirms(..)` declares a custom sink that does |
| `best_effort` | `acks_disabled` | `acknowledgements.enabled: false` |
| `best_effort` | `source_cannot_ack` | Pipe or memory source |
| `best_effort` | `unarmed` | A push source with acknowledgements on, run by a loop that does not arm it, so it answers at enqueue |

A write to a sink that cannot confirm still counts as delivered: the metric reports the weaker guarantee rather than refusing to run. The other run loops set only the `unarmed` row.

---

## A hand-rolled loop

An app with its own receive loop arms the source once before the first `recv`, then releases each block through `SourceAck`:

```rust
use scalo::transport::{DeliveryStatus, SourceAck};

if let Some(control) = receiver.ack_control() {
    control.arm();
}
let batch = receiver.recv(1_000).await?;
let ack = SourceAck::new(&receiver, batch.commit_tokens);
let piece = ack.piece(); // one per table, file or destination
piece.report(DeliveryStatus::Delivered);
ack.release().await?; // seals, awaits the pieces, releases the source
```

A `SourceAck` dropped before its `release` completes -- a panic, or the loop's future dropped mid-block -- releases its block `Errored`, as the pipeline does.

An app's own listener uses `scalo::transport::ack::Tickets`:

- `admit(bytes, deadline)` before queuing a request, refused past the held-byte ceiling unless nothing is held
- `ticket.piece()` for each destination send
- `ticket.outcome().await` for the answer

---

## Source

- [`../../src/worker/engine/pipeline.rs`](../../src/worker/engine/pipeline.rs)
- [`../../src/transport/ack.rs`](../../src/transport/ack.rs)
- [`../../src/transport/kafka/acks.rs`](../../src/transport/kafka/acks.rs)

## Related

- [batch-engine.md](batch-engine.md) -- the run loops
- [dlq.md](dlq.md) -- the DLQ and its `flush` barrier
- [../transport/README.md](../transport/README.md) -- `release` and the receive traits
