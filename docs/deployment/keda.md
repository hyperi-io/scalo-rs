# KEDA

KEDA (Kubernetes Event-driven Autoscaling) scales pods on triggers
the standard HPA can't see -- Kafka consumer-group lag, Prometheus
queries, cron schedules, queue depth. `KedaContract` is the
deployment-side declaration; `ScalingPressure` is the runtime-side
signal source. Together they make scale-out track pipeline pressure,
not container CPU.

---

## `KedaContract`

The contract subset that lands in `values.yaml.keda` and the generated
`ScaledObject`. Built from `KedaConfig` (cascade-loaded runtime config)
via `KedaContract::from_config(&cfg)`.

```rust
pub struct KedaContract {
    pub enabled: bool,                // false generates what `keda: None` does
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
|-------|---------|
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
`activation_lag_threshold`.

`enabled` comes across from `KedaConfig::enabled`. Off produces exactly
the chart `keda: None` does, so either way of saying it gives the same
result.

### `KafkaLagTrigger` -- where the lag trigger looks

The Kafka trigger needs the broker list, the consumer group and a topic.
Where those live in `values.yaml` depends on the app's config shape, so
the contract says. Each path is dotted and `.Values`-relative:

| Field | Default | Accepts |
|-------|---------|---------|
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
`base.topics`. Every segment must be a Go identifier (letters, digits,
`_`), because the chart addresses it with dot syntax. Anything else and
`generate_chart()` returns `DeploymentError::InvalidContract` rather than
writing a chart that cannot render.

`KafkaLagTrigger::disabled()` is for an app that does not consume from
Kafka. The ScaledObject then carries only the CPU trigger and renders
only while `keda.cpu.enabled` is true, `values.yaml` drops `keda.kafka`,
and `keda-triggerauth.yaml` is written as a one-line comment so the
chart's file set stays the same. Turning off the Kafka trigger AND
`cpu_enabled` is an error -- KEDA would have nothing to scale on. Use
`keda: None` for that.

`DeploymentContract::unresolved_values_paths()` lists every trigger path
that `default_config` does not set (or sets to null), and
`validate_helm_values()` reports each one as a mismatch. A path the
chart reads but the config never sets renders empty, and an empty
`bootstrapServers` is admitted by KEDA and then never scales.

---

## When templates are generated

`generate_chart()` writes `keda-scaledobject.yaml` and
`keda-triggerauth.yaml` **only when `contract.keda` is `Some` with
`enabled: true`**. Non-autoscaling services (one-shot jobs, singleton
coordinators) set `keda: None`, or build the contract from a
`KedaConfig` with `enabled: false`, and get just the HPA fallback.

`hpa.yaml` is always written. It guards itself with
`{{- if and .Values.autoscaling.enabled (not .Values.keda.enabled) }}`
-- mutually exclusive with KEDA at runtime, so clusters without the
KEDA operator still scale on CPU by setting `autoscaling.enabled: true`
and `keda.enabled: false`.

The Deployment sets `replicas: <replicaCount>` exactly when neither the
ScaledObject nor the HPA renders, because whichever renders owns the
replica count and a Deployment without `replicas` runs one pod. That
includes a CPU-only chart installed with `keda.cpu.enabled: false`,
where no ScaledObject renders even though `keda.enabled` is true.

---

## Generated `ScaledObject`

Triggers KEDA gets:

1. **Kafka** (unless `kafka_trigger` is disabled) -- `lagThreshold`
   per partition, `activationLagThreshold` for wake-from-zero. Brokers,
   topic and consumer group come from the `kafka_trigger` paths
   (`config.kafka.*` by default); `keda.kafka.topic` and
   `keda.kafka.consumerGroup` override them per deployment.
2. **CPU** (optional, when `keda.cpu.enabled`) -- utilisation
   percentage via `metricType: Utilization`.

With the default trigger paths:

```yaml
{{- if .Values.keda.enabled }}
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{ include "dfe-loader.fullname" . }}
spec:
  scaleTargetRef:
    name: {{ include "dfe-loader.fullname" . }}
  minReplicaCount: {{ .Values.keda.minReplicaCount }}
  maxReplicaCount: {{ .Values.keda.maxReplicaCount }}
  pollingInterval: {{ .Values.keda.pollingInterval }}
  cooldownPeriod: {{ .Values.keda.cooldownPeriod }}
  triggers:
    - type: kafka
      authenticationRef:
        name: {{ include "dfe-loader.fullname" . }}-kafka-auth
      metadata:
        bootstrapServers: {{ join "," ((.Values.config).kafka).brokers | quote }}
        consumerGroup: {{ .Values.keda.kafka.consumerGroup | default ((.Values.config).kafka).group_id | quote }}
        {{- $topics := join "," ((.Values.config).kafka).topics }}
        {{- if .Values.keda.kafka.topic }}
        topic: {{ .Values.keda.kafka.topic | quote }}
        {{- else if $topics }}
        topic: {{ splitList "," $topics | first | quote }}
        {{- else }}
        topic: ""
        {{- end }}
        lagThreshold: {{ .Values.keda.kafka.lagThreshold | quote }}
        activationLagThreshold: {{ .Values.keda.kafka.activationLagThreshold | quote }}
        sasl: scram_sha512
        tls: disable
    {{- if .Values.keda.cpu.enabled }}
    - type: cpu
      metricType: Utilization
      metadata:
        value: {{ .Values.keda.cpu.threshold | quote }}
    {{- end }}
{{- end }}
```

Why it is shaped like that:

- `join ","` before `quote`, because `quote` on a list renders
  `"[kafka:9092]"`, which KEDA reads as a host called `[kafka`. `join`
  also passes a comma-separated string through untouched and turns null
  into an empty string.
- The parenthesised lookups (`((.Values.config).kafka).brokers`) render
  empty when a parent key is missing or null, instead of failing the
  render with a nil pointer.
- The topics are joined and split rather than indexed, because `index`
  on a string topic gives back a single byte.

KEDA scales to the **max** of all triggers -- high lag OR high CPU
grows the pool; both must subside to shrink.

The `TriggerAuthentication` template wires SASL credentials from the
`kafka` secret group. With no `kafka` group, no `TriggerAuthentication`
is written, and both `authenticationRef` and the `sasl` line are
omitted -- a SASL mechanism with no credentials cannot authenticate.
That suits clusters where Kafka auth is bypassed via mesh mTLS. `tls`
is still the literal `disable` whatever the listener uses.

---

## `ScalingPressure` -- app-level signal

KEDA's built-in scalers see infrastructure metrics (Kafka lag, CPU),
not *internal* pipeline state -- buffer depth, batch formation rate,
memory headroom, circuit-breaker status. `ScalingPressure` lets the
app publish a composite 0.0-100.0 score that a Prometheus-trigger KEDA
scaler reads.

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
|------|---------|--------|
| Circuit-breaker open | Downstream sink unreachable | `0.0` -- scaling won't help |
| Memory >= threshold | Pod approaching OOM | `100.0` -- scale before kill |

Outside the gates, components are weighted (sum to 1.0) and each
saturates at its configured ceiling. The app surfaces the score as a
Prometheus gauge (the `ServiceMetrics` helper exposes it as
`dfe_scaling_pressure`) for KEDA's Prometheus trigger to consume.

---

## `/scaling/pressure` endpoint

Attach `ScalingPressure` to the metrics manager via
`MetricsManager::set_scaling_pressure(...)`. The metrics HTTP server
(started with `start_server_with_routes`) then mounts a
`/scaling/pressure` route returning the current value as plain text --
useful as a KEDA `metrics-api` trigger source without standing up a
Prometheus query.

See [../../src/metrics/mod.rs](../../src/metrics/mod.rs) for the attach
API and [../../src/scaling/mod.rs](../../src/scaling/mod.rs) for the
pressure pipeline.

---

## CPU split

CPU is **not** part of the `ScalingPressure` composite. KEDA's native
CPU trigger reads container-level CPU from the K8s metrics-server --
the right source, since the app shouldn't measure its own CPU.
Configure both triggers independently in the `ScaledObject`:

- pressure gauge -> Prometheus scaler (app-level signals)
- CPU utilisation -> CPU scaler (container-level, via metrics-server)

KEDA takes the max; either fires scale-out independently.

---

## API surface

| Item | Purpose |
|------|---------|
| `KedaConfig` | Cascade-loaded runtime config (`enabled`, thresholds) |
| `KedaContract` | Deployment-time subset; `enabled: false` generates what `keda: None` does |
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
- [artefacts.md](artefacts.md) -- when KEDA templates are written
- [../core-pillars/metrics.md](../core-pillars/metrics.md) -- metric
  exposition pipeline that powers Prometheus triggers
- Source: [../../src/deployment/keda.rs](../../src/deployment/keda.rs),
  [../../src/scaling/mod.rs](../../src/scaling/mod.rs),
  [../../src/scaling/pressure.rs](../../src/scaling/pressure.rs)
