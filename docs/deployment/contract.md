# Contract

`DeploymentContract` is the one struct each service fills in.
From it, scalo derives every deployment artefact -- Dockerfile, Helm
chart, Compose fragment, ArgoCD `Application`, container manifest,
runtime-stage fragment -- with no YAML templates in the app.

Fill in the 20% that's app-specific, get the 80% boilerplate for free.
`validate_*` catches contract-vs-artefact drift in CI.

---

## Why a contract

Hand-maintained Dockerfiles and Helm charts rot: ports added to the
binary but never the chart, healthcheck paths change, new secrets land
in code while the `Secret` template references old key names. The
contract makes the binary the source of truth -- the app declares its
surface, generation produces matching artefacts, validation guards the
boundary.

---

## Schema versioning

`DeploymentContract::schema_version` is checked by CI, giving a
fail-fast hook before generation runs against a stale contract.
Current version is **3** (the field defaults to 3). Bump it when the
struct shape changes in a way that breaks downstream consumers.

| Version | Notes |
|---------|-------|
| 1 | Initial shape -- no `image_profile`, no `oci_labels` |
| 2 | `ImageProfile`, `OciLabels`, `SecretGroupContract` |
| 3 | Current - adds `config_schema` + `capabilities` |

v3 is back-compatible in both directions. Reading FORWARD, a v2 consumer
tolerates a v3 contract because `DeploymentContract` does not set
`deny_unknown_fields`, so serde ignores the fields it does not know -- that
holds whether or not the new fields carry content. Reading BACKWARD, a v3
consumer accepts a v2 contract because both fields carry `#[serde(default)]`.
They are also `skip_serializing_if`, so an app that provides neither emits the
same bytes it did under v2.

---

## Producer tiers

```mermaid
flowchart LR
    R[Rust service<br/>uses scalo] -->|generate-artefacts| C[deployment-contract.json]
    P[Python service<br/>uses scalo-py] -->|generate-artefacts| C
    O[bash / TS / Go service<br/>via hyperi-ci templater] -.roadmap.-> C
    C --> D[Dockerfile]
    C --> H[Helm chart/]
    C --> A[argocd-application.yaml]
    C --> CM[container-manifest.json]
    C --> CS[compose.yaml fragment]
```

| Tier | Producer | Status |
|------|----------|--------|
| 1 | scalo (this crate) -- Rust services emit the contract from their config struct | **Shipped** |
| 2 | scalo-py -- Python services emit the same contract shape | **Shipped** |
| 3 | hyperi-ci templater -- bash/TS/Go services emit the contract via templating | **Roadmap** |

Tier 3 is aspirational. The contract is JSON-serialisable and
language-neutral by design; the Rust (`scalo`) and Python (`scalo-py`)
producers both exist today. Cross-language consumers read
`deployment-contract.json` (the serialised form), not this crate.

---

## Struct shape

```rust
use scalo::deployment::*;

let contract = DeploymentContract {
    schema_version: 3,
    app_name: "dfe-loader".into(),
    binary_name: "dfe-loader".into(),
    description: "Kafka -> ClickHouse data loader".into(),
    metrics_port: 9090,
    health: HealthContract::default(),     // /livez, /readyz, /metrics
    env_prefix: "DFE_LOADER".into(),
    metric_prefix: "loader".into(),
    config_mount_path: "/etc/dfe/loader.yaml".into(),
    image_registry: image_registry_from_cascade(),   // org registry
    base_image: base_image_from_cascade(),           // org base image
    extra_ports: vec![],
    entrypoint_args: vec!["--config".into(), "/etc/dfe/loader.yaml".into()],
    secrets: vec![
        SecretGroupContract {
            group_name: "kafka".into(),
            env_vars: vec![
                SecretEnvContract {
                    env_var: "DFE_LOADER__KAFKA__USERNAME".into(),
                    key_name: "username".into(),
                    secret_key: "kafka-username".into(),
                },
                SecretEnvContract {
                    env_var: "DFE_LOADER__KAFKA__PASSWORD".into(),
                    key_name: "password".into(),
                    secret_key: "kafka-password".into(),
                },
            ],
        },
    ],
    default_config: None,
    depends_on: vec!["kafka".into(), "clickhouse".into()],
    keda: Some(KedaContract::default()),
    native_deps: NativeDepsContract::for_rustlib_features(
        &["transport-kafka", "spool", "tiered-sink"],
        &base_image_from_cascade(),
    ),
    image_profile: ImageProfile::Production,
    oci_labels: OciLabels::default(),
    // v3 fields -- both optional
    config_schema: Some(config_schema_json::<Config>()),
    capabilities: vec![
        Capability::transport("kafka")
            .description("Kafka source and sink")
            .maturity("stable"),
    ],
};
```

`config_schema` is the JSON Schema (draft 2020-12) of the app's own
`Config`, derived by schemars - `None` when the app does not derive
`JsonSchema`. `capabilities` is the hand-authored catalog of runtime-data
surface schemars cannot see (service names and their knobs); empty when
the app supplies none. Both are also written out as
`config-schema.{json,yaml}` and `capability-catalog.{json,yaml}` by
`emit_config_artifacts`.

The field that bites people is `secrets`: it's `Vec<SecretGroupContract>`,
not flat. Each group bundles env vars sharing one K8s `Secret` (one
Secret per backend -- Kafka credentials, ClickHouse password, Vault
token).

| Field | Purpose |
|-------|---------|
| `group_name` | Section name in `values.yaml`, helper template suffix (`kafkaSecretName`) |
| `env_vars[].env_var` | The full env var name injected into the pod (`DFE_LOADER__KAFKA__PASSWORD`) |
| `env_vars[].key_name` | Field name in `values.yaml.<group>.secretKeys.<key_name>` |
| `env_vars[].secret_key` | Default K8s Secret data key (`kafka-password`) |

---

## Dev profile derivation

`ImageProfile::Development` is a one-line variant: same binary, same
linking, plus diagnostic tools (`bash`, `strace`, `tcpdump`, `procps`,
`dnsutils`, `net-tools`, `less`, `jq`) and a `-dev` image tag suffix.

```rust
let prod = build_contract();
let dev  = prod.with_dev_profile();   // ImageProfile::Development

generate_dockerfile(&prod, None);    // base_image + runtime libs only
generate_dockerfile(&dev, None);     // + strace, tcpdump, ...
```

CI produces both: `:1.15.0` (prod) and `:1.15.0-dev` (dev). Operators
pull the dev image into a debug pod for forensic work without
rebuilding.

---

## Cascade-driven defaults

These read from the config cascade so ops can change them org-wide
without rebuilding each app. Wire them into the contract builder to pull
registry and base image from `settings.yaml` rather than baking them into
source.

| Function | Cascade key | Default |
|----------|-------------|---------|
| `image_registry_from_cascade()` | `deployment.image_registry` | `ghcr.io/hyperi-io` |
| `base_image_from_cascade()` | `deployment.base_image` | `debian:trixie-slim` |
| `resolve_base_distro(base_image)` | `deployment.base_distro` | derived from `base_image` |
| `argocd_repo_url_from_cascade(app)` | `deployment.argocd.repo_url` | `https://github.com/hyperi-io/<app>` |

Overriding `base_image`? Keep `glibc(runtime) >= glibc(build host)` and
stay off musl (alpine) -- see [native-deps.md](native-deps.md#glibc-keep-runtime--build).
Pinning it to a digest? Set `base_distro` too - runtime package names are
release-specific and a digest carries no codename.

---

## API surface

| Item | Purpose |
|------|---------|
| `DeploymentContract` | Top-level contract struct |
| `DeploymentContract::with_dev_profile()` | Clone with `ImageProfile::Development` |
| `DeploymentContract::to_json()` / `to_yaml()` | Serialise for CI consumption |
| `DeploymentContract::binary()` | Effective binary name (falls back to `app_name`) |
| `DeploymentContract::config_filename()` / `config_dir()` | Split `config_mount_path` |
| `ImageProfile::{Production, Development}` | Profile enum |
| `HealthContract` | `/livez` / `/readyz` / `/metrics` paths |
| `PortContract` | Extra container port beyond `metrics_port` |
| `SecretGroupContract` | One K8s Secret's worth of env vars |
| `SecretEnvContract` | Single env var sourced from a Secret key |
| `OciLabels` | Static OCI labels (`title`, `description`, `vendor`, `licenses`, `copyright`) |
| `NativeDepsContract` | Runtime APT packages -- see [native-deps.md](native-deps.md) |
| `KedaContract` | Autoscaling thresholds -- see [keda.md](keda.md) |
| `ArgocdConfig` | ArgoCD `Application` repo / path / namespace |
| `DEFAULT_IMAGE_REGISTRY` / `DEFAULT_BASE_IMAGE` | Defaults used when cascade is silent |
| `image_registry_from_cascade()` / `base_image_from_cascade()` / `argocd_repo_url_from_cascade()` | Cascade readers |
| `Capability` / `FieldSpec` | Capability-catalog entry and its config fields |
| `config_schema_json::<T>()` | Derive the JSON Schema for the app's `Config` |

`oci_labels.licenses` and `oci_labels.copyright` do double duty: they set the
OCI labels AND the generated Dockerfile's `# License` / `# Copyright` header,
so a non-Apache consumer does not get scalo's licence stamped into its repo.
Both default to scalo's own values.

---

## CI integration

`<app> generate-artefacts --output-dir ci/` (a `cli`-feature
subcommand) emits the contract. CI then runs `validate_*` to confirm
the repo's chart/Dockerfile still match the contract.

See [artefacts.md](artefacts.md) for what generation writes and
[../integration.md](../integration.md) for the `ServiceApp::deployment_contract`
hook that exposes the contract to the CLI.

---

## Related

- [artefacts.md](artefacts.md) -- what `generate-artefacts` writes
- [native-deps.md](native-deps.md) -- auto-detected APT packages
- [keda.md](keda.md) -- autoscaling contract
- [../auto-wiring.md](../auto-wiring.md) -- singleton pattern
- [../integration.md](../integration.md) -- `ServiceApp` trait
- [../feature-flags.md](../feature-flags.md) -- `deployment`, `cli`
- Source: [../../src/deployment/contract.rs](../../src/deployment/contract.rs)
