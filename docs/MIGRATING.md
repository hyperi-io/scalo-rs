# Migrating from hyperi-rustlib to scalo

`hyperi-rustlib` has been renamed and relicensed as **`scalo`** -- a generic,
Apache-2.0 runtime for self-regulating data-plane services. `hyperi-rustlib` is
now **deprecated**; all future development happens in `scalo`. This guide covers
everything you need to move across.

> Most code keeps compiling after the dependency swap -- the renamed public
> types ship as deprecated aliases for one release, so you get warnings, not
> errors. Work through the sections below to clear them.

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
name. The old names remain as `#[deprecated]` aliases for one release, so
existing code compiles with warnings:

```diff
- let metrics = DfeMetrics::register(&manager)?;
+ let metrics = ServiceMetrics::register(&manager)?;
```

Fix the warnings at your own pace; the aliases will be removed in a later scalo
release.

## 3. Environment variables are now bare by default

scalo imposes **no** brand prefix on environment variables. Where
hyperi-rustlib baked in `HYPERI_*` fallbacks, scalo reads bare keys -- or the
prefix *you* declare via `ConfigOptions::env_prefix` (e.g. set `DFE` to read
`DFE_KAFKA__BROKERS`).

Removed with no in-lib replacement:

| Removed | Use instead |
|---------|-------------|
| `HYPERI_LIB_APP_NAME` | bare `APP_NAME` (or `<YOUR_PREFIX>_APP_NAME`) |
| `HYPERI_E2E_CLUSTER` | bare `E2E_CLUSTER` |
| `HYPERI_TELEMETRY` | removed -- gate version checks via `version_check.enabled` |

If you relied on the `HYPERI_` prefix, set your own prefix once at startup and
all keys follow it.

## 4. Dropped features

| Removed | Replacement |
|---------|-------------|
| `database` | none in-lib -- bring your own client |
| `config-postgres` (Postgres config source) | none -- config cascade is now **7-layer** (was 8); use YAML/env/CLI layers |
| `cache` | use `spool` / `tiered-sink` for durable buffering |

If your `Cargo.toml` enabled any of these features, drop them.

## 5. Version check is opt-in

hyperi-rustlib ran a startup version check by default against a baked-in
endpoint. scalo ships it **off**. To keep it, set both:

```yaml
version_check:
  enabled: true
  api_url: "https://<your-version-endpoint>"
```

With `enabled: false` (the default) it is a no-op. No endpoint is baked in.

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

**Feature renamed:** `metrics-dfe` -> `service-metrics`.

**Modules renamed** (only matters if you imported them directly):
`scalo::metrics::dfe` -> `scalo::metrics::service`;
`scalo::metrics::dfe_groups` -> `scalo::metrics::groups`.

**Types:** `DfeMetrics` -> `ServiceMetrics` (deprecated alias kept one release).

**Wire protocol (BREAKING -- rebuild required):** the gRPC proto package
`dfe.transport.v1` -> `scalo.transport.v1`, service `DfeTransport` -> `Transport`.
Wire-incompatible: every gRPC producer and consumer must rebuild against the new
package in lockstep. Message shapes (PushRequest, Batch, Record, ...) unchanged.

## 7. Licence

scalo is **Apache-2.0** (hyperi-rustlib was BUSL-1.1). Review your own
distribution terms if the licence change affects you.

## Getting help

- scalo source and issues: <https://github.com/hyperi-io/scalo-rs>
- scalo on crates.io: <https://crates.io/crates/scalo>
