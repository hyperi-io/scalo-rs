# KEDA

KEDA (Kubernetes Event-driven Autoscaling) scales pods on triggers the standard HPA can't see -- Kafka consumer-group lag, Prometheus queries, cron schedules, queue depth. `KedaContract` is the deployment-side declaration, and the [scalo-service](../../charts/scalo-service/README.md) chart renders it into a ScaledObject that scales on Kafka consumer-group lag and CPU. `ScalingPressure` is a runtime-side signal that chart does not read: a deployment that wants scale-out to track pipeline pressure adds a trigger for it, as the `ScalingPressure` section below describes.

---

## `KedaContract`

The contract subset the scalo-service chart renders into its `ScaledObject`;
the chart's `keda.*` values override each field per deployment. Built from
`KedaConfig` (cascade-loaded runtime config) via
`KedaContract::from_config(&cfg)`.

```rust
pub struct KedaContract {
    pub enabled: bool,                // false renders what `keda: None` does
    pub min_replicas: u32,
    pub max_replicas: u32,
    pub polling_interval: u32,        // seconds between KEDA polls
    pub cooldown_period: u32,         // seconds before scale-down after load drops
    pub kafka_lag_threshold: u64,     // scale when lag > N per partition
    pub activation_lag_threshold: u64,// wake from zero when lag > N
    pub cpu_enabled: bool,
    pub cpu_threshold: u32,           // % utilisation
    pub kafka_trigger: KafkaLagTrigger, // where the lag trigger reads brokers/group/topics
}
```

`KedaContract::default()` shape:

| Field | Default |
| ------- | --------- |
| `enabled` | `true` |
| `min_replicas` | `1` |
| `max_replicas` | `10` |
| `polling_interval` | `15` |
| `cooldown_period` | `300` |
| `kafka_lag_threshold` | `1000` |
| `activation_lag_threshold` | `0` |
| `cpu_enabled` | `true` |
| `cpu_threshold` | `80` |
| `kafka_trigger` | `KafkaLagTrigger::under("config.kafka")` |

`min_replicas: 0` enables scale-to-zero -- pods spin down entirely
when there's nothing to do, KEDA spins them back up when lag exceeds
`activation_lag_threshold`. It needs the Kafka lag trigger: KEDA's CPU
scaler cannot wake a workload from zero on its own, so a contract with
the Kafka trigger off and `min_replicas: 0` is refused with
`InvalidContract` on `keda.min_replicas` by `validate()`, and the chart
refuses to render the same combination when values set it.

`enabled` comes across from `KedaConfig::enabled`. Off renders exactly
what `keda: None` does -- no ScaledObject -- so either way of saying it
gives the same result.

### `KafkaLagTrigger` -- where the lag trigger looks

The Kafka trigger needs the broker list, the consumer group and a topic.
Where those live in `values.yaml` depends on the app's config shape, so
the contract says. Each path is dotted and `.Values`-relative:

| Field | Default | Accepts |
| ------- | --------- | --------- |
| `enabled` | `true` | `false` drops the Kafka trigger, leaving CPU |
| `brokers_path` | `config.kafka.brokers` | a list or a comma-separated string |
| `group_path` | `config.kafka.group_id` | a string |
| `topics_path` | `config.kafka.topics` | a list or a comma-separated string; KEDA watches the first |

An app whose config keeps Kafka under a `source` section points the
trigger there:

```rust
let keda = KedaContract::from_config(&cfg)
    .with_kafka_trigger(KafkaLagTrigger::under("config.source"));
```

`KafkaLagTrigger::under(base)` reads `base.brokers`, `base.group_id` and
`base.topics`. The chart reads each path against the config the app ends
up running -- the contract's `default_config`, then `config`, then
`configOverrides` -- and refuses to render when one has no value. Set it,
or turn the trigger off for that deployment with `keda.kafka.enabled: false`.

`KafkaLagTrigger::disabled()` is for an app that does not consume from
Kafka. The ScaledObject then carries only the CPU trigger. Turning off
the Kafka trigger AND `cpu_enabled` is an error -- KEDA would have
nothing to scale on, so `validate()` refuses the contract and the chart
refuses a ScaledObject with no trigger. Use `keda: None` for that. So is
the Kafka trigger off with `min_replicas: 0`, since CPU alone cannot
scale from zero.

`KafkaLagTrigger` is `#[non_exhaustive]`: build it with `under()` or
`disabled()`, then set any path that sits elsewhere on the value they
return.

`DeploymentContract::unresolved_values_paths()` lists every trigger path
that `default_config` does not set (or sets to null). Each one is a path
every deployment has to set in values, or the chart refuses to render, so
assert the list is empty in a test.

---

## When the chart renders a ScaledObject

The scalo-service chart renders a ScaledObject named `<fullname>-scaler`
**while KEDA is on**: `contract.keda` is `Some` with `enabled: true`,
unless `keda.enabled` in values says otherwise. A singleton never gets
one. Non-autoscaling services (one-shot jobs, singleton coordinators)
set `keda: None`, or build the contract from a `KedaConfig` with
`enabled: false`.

With KEDA off, `autoscaling.enabled: true` renders a CPU
HorizontalPodAutoscaler instead, so clusters without the KEDA operator
still scale on CPU by setting `keda.enabled: false` and
`autoscaling.enabled: true`.

The Deployment sets `replicas: <replicaCount>` exactly when neither KEDA
nor the HPA is on, because whichever is on owns the replica count and a
GitOps sync must not revert a scale.

---

## The triggers it derives

1. **CPU** (unless `cpu_enabled` or `keda.cpu.enabled` turns it off) --
   utilisation target `cpu_threshold`, `metricType: Utilization`;
   `keda.cpu.targetUtilization` overrides the target.
2. **Kafka** (unless `kafka_trigger` or `keda.kafka.enabled` turns it
   off) -- `lagThreshold` per partition and `activationLagThreshold` for
   wake-from-zero, from the contract or `keda.kafka.*`. Brokers, consumer
   group and topic come from the `kafka_trigger` paths: a broker list is
   joined with commas, and the first topic is taken whether the topics are
   a list or a comma-separated string.

`keda.triggers` replaces both, and `keda.extraTriggers` is appended after
them. With the default trigger paths and `config.kafka` set to one broker,
group `event-stage` and topic `events`, the triggers render as:

```yaml
triggers:
  - type: cpu
    metricType: Utilization
    metadata:
      value: "80"
  - type: kafka
    metadata:
      bootstrapServers: kafka:9092
      consumerGroup: event-stage
      topic: events
      lagThreshold: "1000"
      activationLagThreshold: "0"
```

KEDA scales to the **max** of all triggers -- high lag OR high CPU
grows the pool; both must subside to shrink.

The chart renders a TriggerAuthentication named `<fullname>-trigger-auth`
only when `keda.triggerAuthentication.secretTargetRef` is set, and the
Kafka trigger then references it. The trigger carries no `sasl` or `tls`:
KEDA's Kafka scaler takes both as authentication parameters, so the
operator sets them in that `secretTargetRef` beside the credentials, to
match the listener. With neither set KEDA uses no SASL and no TLS.

---

## `ScalingPressure` -- app-level signal

KEDA's built-in scalers see infrastructure metrics (Kafka lag, CPU),
not *internal* pipeline state -- buffer depth, batch formation rate,
memory headroom, circuit-breaker status. `ScalingPressure` lets the app publish a composite 0.0-100.0 score for an autoscaler to read.

```rust
use scalo::scaling::{ScalingPressure, ScalingPressureConfig, ScalingComponent};

let pressure = ScalingPressure::new(
    ScalingPressureConfig::default(),
    vec![
        ScalingComponent::new("kafka_lag",    0.35, 100_000.0),
        ScalingComponent::new("buffer_depth", 0.25,  10_000.0),
        ScalingComponent::new("memory",       0.40,        1.0),
    ],
);

// Lock-free updates from anywhere
pressure.set_component("kafka_lag", 50_000.0);
pressure.set_memory(400_000_000, 1_000_000_000);
```

Two **hard gates** short-circuit the weighted composite. They are
checked in order -- circuit breaker first, so it wins over the memory
gate when both fire:

| Gate | Trigger | Result |
| ------ | --------- | -------- |
| Circuit-breaker open | Downstream sink unreachable | `0.0` -- scaling won't help |
| Memory >= threshold | Pod approaching OOM | `100.0` -- scale before kill |

Outside the gates, components are weighted (sum to 1.0) and each saturates at its configured ceiling. The scalo-service chart does not read the score. It reaches the `scaling_pressure` gauge, with the `metrics.namespace` prefix when one is set: `ServiceRuntime` writes it once a second when `service-metrics` is on, and a service outside the runtime writes it through the `ServiceMetrics` helper. A deployment can read the gauge with a KEDA `metrics-api` scaler through an adapter that serves it; [../pipeline/scaling.md](../pipeline/scaling.md#how-keda-reads-it) shows the trigger.

---

## `/scaling/pressure` endpoint

Attach `ScalingPressure` to the metrics manager via `MetricsManager::set_scaling_pressure(...)`. The metrics HTTP server then answers `/scaling/pressure` with the current value as plain text, whether `start_server` or `start_server_with_routes` started it, and 404 until a pressure is attached. `ServiceRuntime` attaches its own, so every service built on it serves the route. Each pod answers with its own value; the deployment-side trigger above reads the gauge.

See [../../src/metrics/mod.rs](../../src/metrics/mod.rs) for the attach
API and [../../src/scaling/mod.rs](../../src/scaling/mod.rs) for the
pressure pipeline.

---

## CPU split

CPU is **not** part of the `ScalingPressure` composite. KEDA's native
CPU trigger reads container-level CPU from the K8s metrics-server --
the right source, since the app shouldn't measure its own CPU.
Configure both triggers independently in the `ScaledObject`:

- pressure gauge -> `metrics-api` scaler through an adapter (app-level signals, added by the deployment)
- CPU utilisation -> CPU scaler (container-level, via metrics-server)

KEDA takes the max; either fires scale-out independently.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `KedaConfig` | Cascade-loaded runtime config (`enabled`, thresholds) |
| `KedaContract` | Deployment-time subset; `enabled: false` renders what `keda: None` does |
| `KedaContract::from_config(&cfg)` | Build contract from config, `enabled` included |
| `KedaContract::with_kafka_trigger(t)` | Point the Kafka lag trigger at another values layout, or turn it off |
| `KedaContract::default()` | Defaults table above |
| `KafkaLagTrigger::under(base)` / `::disabled()` | Trigger paths under `base`, or no Kafka trigger |
| `DeploymentContract::unresolved_values_paths()` | Trigger paths `default_config` never sets |
| `ScalingPressure` | Composite pressure source -- see `scaling/` module |
| `ScalingPressureConfig` | Gate thresholds (`memory_gate_threshold`, `enabled`) |
| `ScalingComponent` | Single weighted component |
| `PressureSnapshot` / `ComponentSnapshot` / `GateType` | Introspection types |

---

## Related

- [contract.md](contract.md) -- `keda: Option<KedaContract>` field
- [../../charts/scalo-service/README.md](../../charts/scalo-service/README.md) -- every `keda.*` value the chart reads
- [../core-pillars/metrics.md](../core-pillars/metrics.md) -- metric
  exposition pipeline that powers Prometheus triggers
- Source: [../../src/deployment/keda.rs](../../src/deployment/keda.rs),
  [../../src/scaling/mod.rs](../../src/scaling/mod.rs),
  [../../src/scaling/pressure.rs](../../src/scaling/pressure.rs)
