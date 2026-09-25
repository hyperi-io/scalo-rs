# Delivery conformance

`scalo::deployment::test_support::conformance` checks the one property at-least-once delivery promises: every record whose source acknowledgement was released either arrived at the sink or was counted as dead-lettered or dropped. It runs in `cargo test` with no broker. Add `scalo` to `[dev-dependencies]` with `deployment-test-support`, `transport` and `worker-batch`.

A plain "did it arrive" check cannot tell a lost record from one that was never acknowledged, and it passes a pipeline that counts a dropped ack handle as delivered. The harness keeps a `Ledger` of what was acknowledged, arrived, dead-lettered and dropped, keyed by a marker in each JSON record (`conformance_marker`). `Verdict::lost` names every acknowledged marker that is unaccounted for. Duplicates are counted in `Verdict::duplicates` and never fail a run.

## Calling it

Wrap the app's pipeline in a closure the harness calls once per instance. A restart is a new call. The closure gets the source, a fault-injecting sink, the ledger, and a shutdown token to stop on:

```rust,ignore
use scalo::deployment::test_support::conformance::{Case, Fault};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_acknowledged_record_arrives() {
    for fault in Fault::ALL {
        let verdict = Case::new(fault)
            .run_pull(|source, sink, ledger, shutdown| async move {
                my_app::run(&*source, &*sink, &ledger, shutdown).await;
            })
            .await;
        verdict.assert_lossless();
        assert_eq!(verdict.unacknowledged, 0, "{fault:?}: {verdict:?}");
    }
}
```

`run_pull` feeds a `PullSource`: a partitioned log with cumulative per-partition commits, the shape of a Kafka topic. A restarted instance reads from the committed offsets. `run_push` feeds a `PushSource`, the shape of a gRPC or HTTP server. Its client resends each request until it is answered with success. The source holds each answer until `release` once armed, and answers at enqueue when it is not armed. An app whose dead-letter or filter path accounts for a record records it with `ledger.dead_lettered(marker)` or `ledger.dropped(marker, reason)`.

## Faults

Each fault is keyed on the middle marker, so it lands at the same point however fast the pipeline runs. A case whose fault never landed panics, because it proved nothing.

| `Fault` | What happens at the middle marker |
|---|---|
| `GracefulStop` | shutdown fires while that send is in flight; the send then completes |
| `KillMidBatch` | the pipeline task is aborted while that send is in flight |
| `DownstreamRefusing` | the sink refuses that send and every send for 300 ms after it |
| `RejectMidStream` | the sink rejects that record for good, after the records before it landed, until the restart |
| `TwoInstances` | two instances share the source; the one holding that send is killed and the others take over |

After the fault the harness restarts one instance and waits until everything is acknowledged or 30 s pass (`Case::drain_within`). `Verdict::unacknowledged` counts what the source still holds at the end, which is not loss.

`FaultSink`, `PullLog` and `PushSource` are public, so an app can build its own case from the same pieces. scalo's own run is `tests/integration/conformance.rs`: the engine pipeline builder over a pull source, and a hand-rolled `SourceAck` loop over a push source.
