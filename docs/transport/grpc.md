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

- Joining: the cap is the least of `max_batch_bytes` and `max_message_size`, encoded. A block under it waits for one of `in_flight` call slots, then goes out with every block queued behind it, up to the cap, as one request. There is no timer: a block that finds a slot free goes out at once. A block at the cap or over it goes straight out on its own, split within `max_message_size`, and takes no slot.
- Answers: a caller gets the result of the call carrying its records once it returns. A `Backpressured` call fails every block in it, and each caller sends again: duplicates, never loss. A call of several blocks answered `Fatal` or `ResourceExhausted` sends each block again on its own, through the same slots, so neither one caller's bad record nor a receiver with a lower limit fails the rest. A `dlq` filter match, or a record over `max_message_size` on its own, comes out of its caller's block first.
- Bounds: queued blocks are capped at 4 x `max_batch_bytes` per sender, counted encoded and at least 64 bytes a block. A caller that finds no room waits, and nothing is dropped. A cancelled caller's block is not sent unless a call has taken it. `close()`, or dropping the transport, stops its calls in flight and answers every waiting caller `Backpressured`.
- Deadline: `send_timeout_ms` from the start of `send_batch` bounds the wait for room and a slot, a resend's included. A block no call has taken by then is withdrawn unsent and answered `Backpressured`. A block in a call gets that call's answer, which `send_timeout_ms` bounds per request, so a joined `send_batch` can take up to about twice `send_timeout_ms`.
- Limits: `in_flight` or `max_batch_bytes` of 0 is refused at build, unless `enabled` is false.
- Counters: `transport_coalesced_calls_total`, and the `_blocks_total` and `_records_total` they carried. `transport_coalesced_timeouts_total` counts blocks withdrawn at the deadline. A failed call counts once, however many blocks it carried.

Source: [../../src/transport/grpc/coalesce.rs](../../src/transport/grpc/coalesce.rs).
