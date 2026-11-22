# gRPC -- joined calls

`send_batch` joins the small blocks headed for one gRPC endpoint into one `RouteBatch` call, so a receiver or fetcher taking many small requests sends few calls. It is on by default, and the keys sit beside the other gRPC keys in [backends.md](backends.md#grpc):

```yaml
transport:
  output:
    type: grpc
    grpc:
      endpoint: "http://my-app:6000"
      coalesce:
        enabled: true          # false sends every send_batch as its own call
        max_batch_bytes: 65536
        in_flight: 4
```

- Joining: the cap is the least of `max_batch_bytes` and `max_message_size`, encoded. A block under it waits for one of `in_flight` call slots, then goes out with every block queued behind it, up to the cap, as one request. There is no timer: at low load a block goes out at once, and under load blocks queue while the slots are busy. A block at the cap or over it goes straight out on its own, split within `max_message_size`, and takes no slot.
- Answers: a caller gets the result of the call carrying its records once it returns. A `Backpressured` call fails every block in it, and each caller sends again: duplicates, never loss. A `Fatal` call of several blocks sends each block again on its own, through the same slots, so one caller's bad record fails no one else. A `dlq` filter match, or a record over `max_message_size` on its own, comes out of its caller's block first.
- Bounds: queued blocks are capped at 4 x `max_batch_bytes` per sender, counted encoded and at least 64 bytes a block. A caller that finds no room waits, and nothing is dropped. A cancelled caller's block is not sent unless a call has taken it. `close()`, or dropping the transport, stops its calls in flight and answers every waiting caller `Backpressured`.
- Deadline: one `send_timeout_ms` from the start of `send_batch` covers the wait, the call and any resend, then the answer is `Backpressured`. A block in a call is still sent, so the resend can duplicate it. Blocks sent on their own keep `send_timeout_ms` per request, as before.
- Limits: `in_flight` or `max_batch_bytes` of 0 is refused at build, unless `enabled` is false.
- Counters: `transport_coalesced_calls_total`, and the `_blocks_total` and `_records_total` they carried. `transport_coalesced_timeouts_total` counts blocks answered at the deadline. A failed call counts once, however many blocks it carried.

Source: [../../src/transport/grpc/coalesce.rs](../../src/transport/grpc/coalesce.rs).
