# Delivery path

How a caller's batch reaches its destinations, the alternatives that were benched, and what the numbers decided. Today's behaviour is the first approach below, with the fan-in buffer in front of each gRPC destination. Parallel fan-out is a parked design.

---

## The model

Each caller's batch is delivered inline. `send_batch` groups the records by route, hands each group to its destination's sender, and returns when the destinations have answered. A multi-destination batch goes to its destinations in turn (see [Fan-out](routing.md#fan-out)).

- No queue sits between the caller and a destination, so a record is never acknowledged before its destination has taken it. Backpressure from a destination reaches the caller as `Backpressured`.
- Nothing spools to disk, and memory held is bounded by the calls in flight under the memory guard.
- A queue per sink, answering the caller at enqueue, would add a buffer that must be sized, bounded and drained, and would release records as delivered before they are. That shape was not benched.

---

## Method

The harness is `benches/delivery_path`. Each approach is one type behind one trait, picked by name. Each (approach, load, run) cell runs in its own child process, so peak RSS and CPU time are its own.

```text
cargo bench --bench delivery_path --features transport-test-support -- \
    --approaches inline_sequential,per_record_futures --loads W1,W4 \
    --tier 1 --seconds 5 --runs 5 --control
```

- Tier 1 runs on in-process shaped senders. It is the fast loop and the regression gate, comparing an approach with its own saved baseline (`--save`, `--compare`, `--max-regression 0.05`).
- Tier 2 adds the features `testcontainers` and `transport-grpc`. It starts its own Kafka broker container and removes it on exit, and runs real gRPC listeners in the process. Decisions are made here, on a quiet host with the runner and broker pinned to separate CPUs.
- Runs interleave (A, B, A, B) with the order rotated each round. `--control` runs the first approach twice, so every table carries a same-code noise floor.
- Throughput comes from closed-loop callers. Latency comes from open-loop callers at a fixed offered rate (`--open-loop 0.5,0.8`), timed from the scheduled offer so a slow approach cannot hide its tail by offering less.
- CPU per record is the whole bench process over records sent, including the fake callers, librdkafka and the in-process listener. It is not a sender's own cost.

---

## Loads and the decision rule

The loads are defined in `benches/delivery_path/loads.rs`.

| Id | Pattern |
| --- | --- |
| W1 | many callers, 1 small record each, 2 destinations |
| W2 | few callers, 1000 records of 1 KiB each, 3 destinations |
| W3 | W1 and W2 together |
| W4 | W3 with one destination 100 ms slow |
| W5 | W3 with one destination down |
| W6 | W3 with the Kafka queue filling in bursts |
| W7 | W2 with one refused record per batch |
| W1g | 512 callers, 1 record of 200 B each, one gRPC destination |
| W2g | 8 callers, 1000 records of 1 KiB each, one gRPC destination |
| W3g | W1g and W2g together |

The rule, fixed before the runs: a change replaces today's send when multi-destination p99 improves and no load regresses more than 5%. A merge stage is adopted for gRPC only when W1 gains at least 20%, W2 loses no more than 5% and peak held bytes grow no more than 10%. A difference inside the run-to-run spread decides nothing. The no-regression half was later judged on a paired 95% interval over 30 rounds, which is tighter than the spread test.

The approaches below were benched from a harness branch and are not all in the tree yet. The in-tree harness has `inline_sequential` and `per_record_futures`. Differences from today's send (`inline_sequential`), 10 rounds a cell. "ns" is inside the spread.

| Approach | W1 closed | W2 closed | W3 closed | W4 open@0.8 | W7 closed |
| --- | --- | --- | --- | --- | --- |
| today's send, rec/s and p99 ms | 15131, 47.6 | 206381, 63.6 | 105492, 63.6 | 52270, 128.1 | 145589, 104.9 |
| `batch_per_destination` | ns | ns | ns | ns | ns |
| `parallel_fanout` | +51.5% rec/s, -38.1% p99 | ns | ns | p99 -8.6% | ns |
| `merge_channel` | +37.1% rec/s | p99 +75.1% | rec/s +54.0%, p99 +84.1% | rec/s -26.3%, p99 +458.5% | rec/s -72.4%, p99 +249.0% |

- `batch_per_destination` sends one `send_batch` per destination with an outcome per record. It does the same work as today's send without stopping at the first failure, and its W7 gain did not hold at 30 rounds (+0.5%).
- `merge_channel` merges concurrent callers' blocks per gRPC destination in a bounded buffer. A mutex-swapped double buffer (`merge_swap`) behaves the same. A sweep on W1 and W2 of windows (0.5, 1, 5 ms) and buffers (0 to 4 MiB) found no buffered config that meets the rule. A zero buffer meets it, but that is the stage switched off, which is plain fan-out.
- Merge fails on W2 because a block is about 1 MiB, so a 4 MiB buffer admits 4 callers at a time and the rest queue. On W4 one send at a time to the slow destination caps the stage, and held bytes rose from 7.8 to 73.7 MiB. On W7 the per-record fallback for the refused record runs while the stage is held.
- Merge's W1 closed p99 is 44.8 ms against 29.4 for parallel fan-out, at every buffer size, which fits each caller waiting out a send that carries other callers' blocks. The bench does not record per-caller waits, so that is inferred from the numbers. A drainer variant would not address W2, W4 or W7 and was not built. Revisiting merge needs concurrent sends per destination and a buffer of at least callers times block size.
- Merge is not adopted, and no buffer-size or window key exists.

---

## Parallel fan-out (parked)

Status: not built. Most deploys pick one transport, so a route has one destination and nothing to overlap.

It would send a batch to all its destinations at once, which helps where one batch goes to 2 or more destinations, such as a dual write to Kafka and gRPC or a bulk request whose records route differently. In turn the waits add, and at once the caller waits only for the slowest.

Collecting the send futures with `join_all` copies each future whole. A send future is 8,736 bytes, so that was about 17 KB a batch at 2 destinations, and perf put about 70% of the extra CPU in that memmove. Pinning each future where it sits cut that memmove excess by about 78%, and the one-destination CPU excess from +38% to +21%. The re-run on tier 2, 10 runs a cell, judged by the spread test:

| Load | Approach | rec/s | p99 ms | CPU us/rec |
| --- | --- | --- | --- | --- |
| 1 destination, closed | today's send | 22118 | 22.25 | 6.78 |
| | auto | 23017 | 22.25 | 6.83 (+0.6%, inside the spread) |
| 2 destinations, closed | today's send | 15069 | 47.59 | 121 |
| | auto | 23126 (+53.5%) | 28.58 (-40%) | 162 |
| Mixed, closed | today's send | 103696 | 63.63 | 47.1 |
| | auto | 99020 (-4.5%, inside the spread) | 60.51 | 50.0 |
| 2 destinations, half load | today's send | 7457 | 43.93 | 139 |
| | auto | 7496 | 23.16 (-47%) | 178 (+28%) |
| Mixed, half load | today's send | 48821 | 43.06 | 21.7 |
| | auto | 48977 | 29.15 (-32%) | 27.0 (+24%) |

Auto shows no cost in throughput or p99 at one destination, a p99 cut at 2 or 3 destinations, and no throughput loss outside the spread. The earlier by-value build did lose 6% on W3 and W5 closed by the paired interval, and the pinned build was not re-run on W5, W6 or with that interval. The price is 24-34% more CPU per record on 2-destination loads.

The design:

- `fanout: auto` is the only key, with `sequential` for an operator who has measured a loss. There is no `parallel` value, since `auto` already sends at once.
- One destination takes today's path, untouched. Two or more start every send together, each future pinned in place, and merge the answers by the existing rules. The first `Backpressured` or `Fatal` wins and `FilteredDlq` does not stop the others.
- No thread, queue, buffer, background task or lock. The mode is read once at build.
- No load switch is needed: the full-load loss on mixed traffic falls inside the spread.

For delivery, at-least-once holds. A retry after a partial failure re-sends to destinations that already accepted, so there are more duplicates than today's send, which stops at the first failed destination. Per-destination order and Kafka per-key order are unchanged.

It comes back for a deploy that routes one batch to 2 or more destinations and has p99 to gain. That changes `send_batch_fanout` and `send_fanout` in `src/transport/routed.rs` and adds `fanout` to the config schema. The bench is then re-run with the paired interval on W1, W3, W5 and W6, closed and at half load.

---

## Fan-in buffer for gRPC destinations

Status: built, on by default. The keys are under `<key>.grpc.coalesce` (see [gRPC](grpc.md)).

Fan-in is many callers each sending a small block to one gRPC destination, such as a receiver taking many small HTTP requests. Without the buffer each `send_batch` is its own `RouteBatch` call.

The buffer sends what is queued the moment a call slot frees:

- Up to `in_flight` (4) calls run at once per destination.
- A freed slot takes the next block and every block queued behind it, up to `max_batch_bytes` (64 KiB) or `max_message_size`, whichever is less, encoded. There's no timer.
- A block at or over that cap goes straight out.
- At low load a slot is free, so a block goes out at once. Under load blocks queue while the slots are busy, and batch that way.

It differs from the rejected merge stage in two ways: several calls run per destination at once, and no block waits on a window.

Delivery stays at-least-once. A caller is answered when the call carrying its records returns. A call that fails for now fails every caller in it, and each sends again: copies, never loss. A call refused for good, or answered `ResourceExhausted`, sends each block again on its own. Queued payload is capped at 4 x `max_batch_bytes` per destination. A caller waits for room, and a block still untaken at `send_timeout_ms` is withdrawn and answered `Backpressured`. Dead letters come out of each block before it joins, so `FilteredDlq` goes only to a caller whose block had them.

Tier 2, the listener in its own process, loopback plaintext, medians of 10 runs with a control run each:

| Load | today's send | buffer (64 KiB, 4 in flight) |
| --- | --- | --- |
| W1g closed, rec/s | 32597 | 545674 |
| W1g closed, p99 ms | 17.16 | 1.50 |
| W1g half load, p99 ms | 2.89 | 2.32 |
| W2g closed, rec/s | 697503 | 716606 (ns, control 716295) |
| W3g closed, rec/s | 222024 | 673148 |
| W3g closed, p99 ms | 36.33 | 15.07 |
| W1g closed, listener CPU us/rec | 53.1 | 2.2 |

The same runs set the defaults:

- 8 calls in flight gave 490491 rec/s on W1g closed, and 128 KiB gave 519114. Both are below 4 and 64 KiB.
- A 0.5 ms linger raised W1g half-load p99 to 4.07 ms, because tokio rounds a timer up to its next 1 ms tick. It did better on W3g closed (701303 rec/s, p99 12.09 ms), but low load is the common case, so there is no linger key.

Not measured: TLS, a network round trip, and the blocks per call. The batching under load is read from the throughput and listener CPU figures.

`coalesce.enabled: false` sends each `send_batch` as its own call.
