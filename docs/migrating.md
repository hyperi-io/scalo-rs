# Migrating from hyperi-rustlib to scalo

`hyperi-rustlib` was renamed and relicensed as **`scalo`** -- a generic,
Apache-2.0 runtime for self-regulating data-plane services. The
`hyperi-rustlib` repo is archived, and development continues in `scalo`. This
guide covers the move.

> The renamed public types shipped as deprecated aliases for one release and
> are gone, so code still on an old name no longer compiles. The sections
> below give each rename.

## 1. Swap the dependency

```diff
# Cargo.toml
- hyperi-rustlib = { version = "2.8", features = ["config", "logger", "metrics"] }
+ scalo          = { version = "2.9", features = ["config", "logger", "metrics"] }
```

```diff
# imports
- use hyperi_rustlib::config;
+ use scalo::config;
```

The version line continues unbroken: scalo `2.9.x` is the direct successor to
hyperi-rustlib `2.8.x`.

## 2. Renamed public types (`Dfe*` -> `Service*`)

Every `Dfe`-prefixed public type now has a brand-neutral `Service`-prefixed
name. The old names shipped as `#[deprecated]` aliases for one release and are
now gone:

```diff
- let metrics = DfeMetrics::register(&manager)?;
+ let metrics = ServiceMetrics::register(&manager);
```

## 3. Environment variables are now bare by default

scalo imposes **no** brand prefix on environment variables. Where
hyperi-rustlib baked in `HYPERI_*` fallbacks, scalo reads bare keys -- or the
prefix *you* declare via `ConfigOptions::env_prefix` (e.g. set `DFE` to read
`DFE_KAFKA__BROKERS`).

Removed with no in-lib replacement:

| Removed | Use instead |
| --------- | ------------- |
| `HYPERI_LIB_APP_NAME` | bare `APP_NAME` |
| `HYPERI_E2E_CLUSTER` | removed -- scalo reads no cluster variable |
| `HYPERI_TELEMETRY` | removed -- gate version checks via `version_check.enabled` |

If you relied on the `HYPERI_` prefix, set your own prefix once at startup and
all keys follow it.

## 4. Dropped features

| Removed | Replacement |
| --------- | ------------- |
| `database` | none in-lib -- bring your own client |
| `config-postgres` (Postgres config source) | none -- the config cascade is **7-layer** (was 8). Use the YAML, env and CLI layers |
| `cache` | use `spool` / `tiered-sink` for durable buffering |

If your `Cargo.toml` enabled any of these features, drop them.

## 5. Version check has no baked-in endpoint

hyperi-rustlib ran a startup version check by default against a baked-in
endpoint. scalo bakes in no endpoint: `enabled` defaults to `true`, and the
check stays inert until `api_url` is set. To run it:

```yaml
version_check:
  api_url: "https://<your-version-endpoint>"
```

Set `enabled: false` to turn it off outright.

## 6. Metric and wire-protocol names

**Metric names are now bare with an optional namespace** (mirroring the env
rule -- bare unless a prefix is supplied). The hardcoded `dfe_` prefix is gone:

- Default: bare names with domain sub-prefixes, e.g. `dfe_transport_sent_total`
  -> `transport_sent_total`.
- To KEEP your old `dfe_*` names unchanged, set the metrics namespace to `dfe`:
  `MetricsManager::new("dfe")` (or `MetricsConfig { namespace: "dfe".into(), .. }`).
  Every metric then renders `dfe_<name>` exactly as before.
- Set it to your app name for a unified namespace (`myapp_transport_sent_total`).
  Pod/app/instance differentiation comes from scrape/OTel labels, not the name.
- One namespace covers every family. Before, `ServiceMetrics` carried a fixed
  `dfe_` and the metric groups carried the namespace, so an app on namespace
  `dfe_loader` had `dfe_records_received_total` from `ServiceMetrics` and
  `dfe_loader_records_received_total` from `AppMetrics`. Now both are one series.
  An app that counted a record through both counts it twice: count it through
  `ServiceMetrics::records_received` only. No namespace keeps both old spellings.

**Feature renamed:** `metrics-dfe` -> `service-metrics`.

**Modules renamed** (only matters if you imported them directly):
`scalo::metrics::dfe` -> `scalo::metrics::service`, and
`scalo::metrics::dfe_groups` -> `scalo::metrics::groups`.

**Types:** `DfeMetrics` -> `ServiceMetrics` (the alias shipped for one release and is gone).

**Wire protocol (BREAKING -- rebuild required):** the gRPC proto package
`dfe.transport.v1` -> `scalo.transport.v1`, service `DfeTransport` -> `Transport`.
Wire-incompatible: every gRPC producer and consumer must rebuild against the new
package in lockstep. The messages (`PushRequest`, `Batch`, `Record`, ...) are unchanged.

## 7. Licence

scalo is **Apache-2.0** (hyperi-rustlib was BUSL-1.1). Review your own
distribution terms if the licence change affects you.

## Getting help

- scalo source and issues: <https://github.com/hyperi-io/scalo-rs>
- scalo on crates.io: <https://crates.io/crates/scalo>
