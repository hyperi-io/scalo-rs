# Pipeline

The hot path: what happens to a batch between arriving on a transport and
landing at its destination. Everything here is opt-in behind a feature flag --
a service takes only the stages it needs.

The through-line is that no stage drops data silently. A stage that cannot keep
up applies backpressure upstream, a sink that fails spills to disk, and a record
no sink can take goes to the DLQ rather than being discarded.

```mermaid
flowchart LR
    T["Transport"] --> BE["BatchEngine"]
    BE --> WP["WorkerPool"]
    WP --> TS["TieredSink"]
    TS --> SS["sink-stack"]
    SS --> Out["destination"]
    TS -.-> SP[(&quot;Spool&quot;)]
    BE -.-> DLQ[(&quot;DLQ&quot;)]
```

Solid arrows are the happy path, and dotted ones carry data only off it: the
`TieredSink` spill when the sink fails, and the dead letters the pipeline
builder writes with `with_dlq`.

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
