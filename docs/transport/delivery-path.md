# Delivery path

How a caller's batch reaches its destinations, the alternatives that were benched, and what the numbers decided. Today's behaviour is the first approach below. Parallel fan-out is a parked design, and the fan-in buffer for gRPC destinations is being benched.

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

The merge stage above is one fan-in design, and it was rejected. A separate fan-in buffer for gRPC destinations is being benched, and its results and decision go here when that bench lands.
