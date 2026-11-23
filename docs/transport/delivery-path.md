# Delivery path

How a batch gets from a caller to its destinations, and why. We benched four ways to do it, and the numbers picked.

- Each batch is sent inline. No queue, no spool, and nothing acknowledged before its destination has it.
- A gRPC destination fed by many small callers gets a fan-in buffer, on by default. About 17x the throughput on small blocks.
- Parallel fan-out to several destinations works, but it's parked. Most deploys have one destination, so there's nothing to overlap.
- A merge stage in front of gRPC lost on four of five loads.

---

## The model

`send_batch` groups a batch's records by route, hands each group to its destination's sender, and returns when they've answered. Several destinations are sent in turn (see [Fan-out](routing.md#fan-out)).

- No queue sits between caller and destination. A record is never acknowledged before its destination takes it, and backpressure reaches the caller as `Backpressured`.
- Nothing spools to disk. Memory held is bounded by the calls in flight, under the memory guard.
- A queue per sink that answers at enqueue would need sizing, bounding and draining. Worse, it would call records delivered before they are. We didn't bench it.

---

## Method

The harness is `benches/delivery_path`. Each approach is one type behind one trait, picked by name. Each (approach, load, run) cell runs in a separate child process, so each cell's peak RSS and CPU stand alone.

```text
cargo bench --bench delivery_path --features transport-test-support -- \
    --approaches inline_sequential,per_record_futures --loads W1,W4 \
    --tier 1 --seconds 5 --runs 5 --control
```

- Tier 1 runs in-process shaped senders. It's the fast loop and the regression gate: an approach against its saved baseline (`--save`, `--compare`, `--max-regression 0.05`).
- Tier 2 adds `testcontainers` and `transport-grpc`. It starts a Kafka broker container, removes it on exit, and runs real gRPC listeners. Decisions are made here, on a quiet host with runner and broker pinned to separate CPUs.
- Runs interleave (A, B, A, B), rotated each round. `--control` runs the first approach twice, so every table has a same-code noise floor.
- Throughput comes from closed-loop callers. Latency comes from open-loop callers at a fixed offered rate (`--open-loop 0.5,0.8`), timed from the scheduled offer, so a slow approach can't hide its tail by offering less.
- CPU per record is the whole bench process over records sent: fake callers, librdkafka and the in-process listener included. It's not the sender's cost alone.

---

## Loads and the rule

The loads live in `benches/delivery_path/loads.rs`.

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

The rule was fixed before the runs. A change replaces the inline send when multi-destination p99 improves and no load regresses more than 5%. A merge stage for gRPC also needs W1 up at least 20%, W2 down no more than 5%, and peak held bytes up no more than 10%.

A difference inside the run-to-run spread decides nothing. Where a call was close, no-regression was checked on a paired 95% interval over 30 rounds, which is tighter than the spread.

---

## Four approaches

Differences from the inline send (`inline_sequential`), 10 rounds a cell. "ns" is inside the spread. The in-tree harness has `inline_sequential` and `per_record_futures`. The others ran from a harness branch.

| Approach | W1 closed | W2 closed | W3 closed | W4 open@0.8 | W7 closed |
| --- | --- | --- | --- | --- | --- |
| inline send, rec/s and p99 ms | 15131, 47.6 | 206381, 63.6 | 105492, 63.6 | 52270, 128.1 | 145589, 104.9 |
| `batch_per_destination` | ns | ns | ns | ns | ns |
| `parallel_fanout` | +51.5% rec/s, -38.1% p99 | ns | ns | p99 -8.6% | ns |
| `merge_channel` | +37.1% rec/s | p99 +75.1% | rec/s +54.0%, p99 +84.1% | rec/s -26.3%, p99 +458.5% | rec/s -72.4%, p99 +249.0% |

- `batch_per_destination` sends one `send_batch` per destination, with an outcome per record. It's the inline send's work without the stop at the first failure. Its W7 gain didn't hold at 30 rounds (+0.5%).
- `merge_channel` merges concurrent callers' blocks per gRPC destination in a bounded buffer. A mutex-swapped double buffer (`merge_swap`) behaves the same. We swept windows of 0.5, 1 and 5 ms and buffers of 0 to 4 MiB on W1 and W2. No buffered config met the rule. A zero buffer did, but that's the stage switched off.
- Merge fails W2 because a block is about 1 MiB, so a 4 MiB buffer admits 4 callers and the rest queue. On W4 one send at a time to the slow destination caps the stage, and held bytes went from 7.8 to 73.7 MiB. On W7 the per-record fallback for the refused record runs while the stage is held.
- Merge's W1 closed p99 is 44.8 ms against 29.4 for parallel fan-out, at every buffer size. That fits each caller waiting out a send that carries other callers' blocks. That's inferred: the bench doesn't record per-caller waits.
- A drainer variant wouldn't fix W2, W4 or W7, so we didn't build it. Merge isn't adopted, and there's no buffer or window key. Bringing it back needs concurrent sends per destination and a buffer of at least callers x block size.

---

## Parallel fan-out (parked)

Not built. Most deploys pick one transport, so a route has one destination and nothing to overlap.

It sends a batch to all its destinations at once. That helps where one batch goes to 2 or more: a dual write to Kafka and gRPC, or a bulk request whose records route differently. In turn the waits add up. At once the caller waits only for the slowest.

Collecting the send futures with `join_all` copies each one whole. A send future is 8,736 bytes, about 17 KB a batch at 2 destinations, and perf put about 70% of the extra CPU in that memmove. Pinning each future where it sits cut the memmove excess by about 78%, and the one-destination CPU excess from +38% to +21%.

The pinned build, tier 2, 10 runs a cell, judged by the spread:

| Load | Approach | rec/s | p99 ms | CPU us/rec |
| --- | --- | --- | --- | --- |
| 1 destination, closed | inline send | 22118 | 22.25 | 6.78 |
| | auto | 23017 | 22.25 | 6.83 (+0.6%, inside the spread) |
| 2 destinations, closed | inline send | 15069 | 47.59 | 121 |
| | auto | 23126 (+53.5%) | 28.58 (-40%) | 162 |
| Mixed, closed | inline send | 103696 | 63.63 | 47.1 |
| | auto | 99020 (-4.5%, inside the spread) | 60.51 | 50.0 |
| 2 destinations, half load | inline send | 7457 | 43.93 | 139 |
| | auto | 7496 | 23.16 (-47%) | 178 (+28%) |
| Mixed, half load | inline send | 48821 | 43.06 | 21.7 |
| | auto | 48977 | 29.15 (-32%) | 27.0 (+24%) |

No cost at one destination. A p99 cut at 2 or 3. No throughput loss outside the spread. The price is 24-34% more CPU per record on 2-destination loads.

One gap. The by-value build lost 6% on W3 and W5 closed by the paired interval, and the pinned build hasn't been re-run on W5, W6 or with that interval.

The design:

- `fanout: auto` is the only key, with `sequential` for an operator who has seen a loss. There's no `parallel` value, since `auto` already sends at once.
- One destination takes the inline path, untouched. Two or more start every send together, each future pinned in place, and merge the answers by the existing rules: the first `Backpressured` or `Fatal` wins, and `FilteredDlq` doesn't stop the others.
- No thread, queue, buffer, background task or lock. The mode is read once at build.
- No load switch. The full-load loss on mixed traffic is inside the spread.

At-least-once holds. A retry after a partial failure re-sends to destinations that already took the batch, so there are more duplicates than with the inline send, which stops at the first failed destination. Per-destination order and Kafka per-key order don't change.

It comes back for a deploy that routes one batch to 2 or more destinations and has p99 to gain. That touches `send_batch_fanout` and `send_fanout` in `src/transport/routed.rs` and adds `fanout` to the config schema. Then the bench re-runs with the paired interval on W1, W3, W5 and W6, closed and at half load.

---

## gRPC fan-in buffer

Built, and on by default. The keys are under `<key>.grpc.coalesce` (see [gRPC](grpc.md)).

Fan-in is many callers each sending a small block to one gRPC destination. Think a receiver taking lots of small HTTP requests. Without the buffer every `send_batch` is a separate `RouteBatch` call.

The buffer sends what's queued the moment a call slot frees:

- Up to `in_flight` (4) calls run at once per destination.
- A freed slot takes the next block and every block queued behind it, up to `max_batch_bytes` (64 KiB) or `max_message_size`, whichever is less, encoded. No timer.
- A block at or over that cap goes straight out.
- At low load a slot is free, so a block goes out at once. Under load, blocks queue while the slots are busy, and batch that way.

It differs from the merge stage that lost in two ways: several calls run per destination at once, and no block waits on a window.

Delivery stays at-least-once:

- A caller is answered when the call carrying its records returns.
- A call that fails for now fails every caller in it, and each sends again. Copies, never loss.
- A call refused for good, or answered `ResourceExhausted`, re-sends each block separately.
- Queued payload is capped at 4 x `max_batch_bytes` per destination, and a caller waits for room. A block still untaken at `send_timeout_ms` is withdrawn and answered `Backpressured`.
- Dead letters come out of each block before it joins, so `FilteredDlq` only goes to a caller whose block had them.

Tier 2, listener in a separate process, loopback plaintext, medians of 10 runs, each with a control run:

| Load | inline send | buffer (64 KiB, 4 in flight) |
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
- A 0.5 ms linger pushed W1g half-load p99 to 4.07 ms, because tokio rounds a timer up to its next 1 ms tick. It did win W3g closed (701303 rec/s, p99 12.09 ms). But low load is the common case. Hence no linger key.

The bench didn't cover TLS, a real network round trip, or blocks per call. The batching under load is read off the throughput and listener CPU.

`coalesce.enabled: false` turns it off, and each `send_batch` is a separate call again.
