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

- Joining: a block under `max_batch_bytes` of payload waits for one of `in_flight` call slots. When one is free, the next block goes out with every block queued behind it, up to `max_batch_bytes` of payload and `max_message_size` encoded, so a joined call is one request a receiver with the same `max_message_size` takes. There is no timer: at low load a slot is free and a block goes out at once, and under load blocks queue while the slots are busy. A block of `max_batch_bytes` or more goes straight out on its own and takes no slot.
- Answers: a caller is answered when the call carrying its records returns, with that call's result. A failed call fails every block in it and each caller sends again, so a retry can duplicate records and never loses them. A record a `dlq` filter matches, or one over `max_message_size` on its own, comes out of its own caller's block first, so only that caller sees `FilteredDlq`.
- Bounds: queued payload is capped at 4 x `max_batch_bytes` per sender. A caller that finds no room waits for it, and nothing is dropped. A block whose caller is cancelled before a call takes it is not sent.
- Deadline: `send_timeout_ms` bounds the whole `send_batch`, the wait for a slot included. A caller still waiting at the limit is answered `Backpressured`, as a call past its deadline is, and sends again. A block already in a call is still sent, so that resend can duplicate it.
- Limits: `in_flight` and `max_batch_bytes` of 0 are refused when the transport is built, unless `enabled` is false.
- Counters: `transport_coalesced_calls_total` counts the joined calls, and `transport_coalesced_blocks_total` and `transport_coalesced_records_total` what they carried. Divided by the calls, they give the blocks and records per call.

Source: [../../src/transport/grpc/coalesce.rs](../../src/transport/grpc/coalesce.rs).
