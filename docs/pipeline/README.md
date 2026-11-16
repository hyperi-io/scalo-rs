# Pipeline

The hot path: what happens to a batch between arriving on a transport and
landing at its destination. Everything here is opt-in behind a feature flag --
a service takes the stages it needs.

The through-line is that no stage drops data silently. A stage that cannot keep
up applies backpressure upstream, a sink that fails spills to disk, and a record
no sink can take goes to the DLQ rather than being discarded.

The batch engine hands processed records to the app's sink, a tiered sink or a sink stack around a transport sender, and dotted arrows carry spool writes and dead letters.

```mermaid
flowchart LR
    accTitle: L4 Detail -- Pipeline stages
    accDescr: A transport receiver hands batches to the BatchEngine, which runs the transform on the worker pool. The app's sink is a TieredSink or a sink stack around a transport sender, which delivers to the destination. The TieredSink spills to the spool when its sink fails, and the engine writes dead letters to the DLQ.

    receiver["Transport<br/>receiver"]
    engine[["BatchEngine"]]
    tiered[["TieredSink"]]
    stack[["Sink stack"]]
    dlq[("DLQ")]
    sender["Transport<br/>sender"]
    spool[("Spool")]
    destination["Destination"]

    receiver --> engine
    engine --> tiered
    engine --> stack
    engine -.-> dlq
    tiered --> sender
    tiered -.-> spool
    stack --> sender
    sender --> destination

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef store fill:#DDF3EC,stroke:#00785A,stroke-width:1px,color:#0B3327
    classDef error fill:#FBE3D6,stroke:#C2410C,stroke-width:1px,color:#4A1A05
    classDef ext fill:#EEF0F2,stroke:#6B7280,stroke-width:1px,stroke-dasharray:4 3,color:#1F2937
    class spool store
    class dlq error
    class destination ext
```

*Figure 1. L4 Detail -- Pipeline stages. Dotted arrows are the spill to the spool and the dead letters `with_dlq` writes.* Up: [L3 Components -- scalo](../README.md). Down: [L4 Detail -- Batch engine stages](batch-engine.md), [L4 Detail -- Tiered sink delivery](tiered-sink.md), [L4 Detail -- Sink stack layer order](sink-stack.md).

The engine runs the transform on the `AdaptiveWorkerPool`. A tiered sink and a sink stack are two ways to wrap the sender: the tiered sink adds the spill and a circuit breaker, the sink stack timeouts, limits and retries.

| Doc | Covers |
| --- | --- |
| [batch-engine.md](batch-engine.md) | SIMD parse, pre-route filter, field interning |
| [worker-pool.md](worker-pool.md) | `AdaptiveWorkerPool`, pressure-based scaling |
| [tiered-sink.md](tiered-sink.md) | resilient delivery, disk spillover, circuit breaker |
| [sink-stack.md](sink-stack.md) | timeout / load-shed / concurrency / retry / rate-limit |
| [spool.md](spool.md) | disk-backed async FIFO |
| [dlq.md](dlq.md) | file, Kafka and HTTP backends |
| [strmatch.md](strmatch.md) | 4-tier regex to fast-path matcher |
| [rules.md](rules.md) | ordered first-match rules that set target fields, clamp, hit counters, hot reload |
| [scaling.md](scaling.md) | `ScalingPressure`, KEDA external scaler signal |

For why the pipeline throttles the way it does, read
[../backpressure.md](../backpressure.md) first.
