# Dependency duplication report

> Snapshot 2026-10-06.
> Regenerate with `scripts/dep-dup-check.sh` (warning-only).

`cargo tree -d --features full -e normal` reports **54 duplicated crate versions**. Each scalo direct dependency resolves to one version; the other copy always arrives transitively. Duplicates fall into three buckets.

## 1. Actionable (transitive, upgrade path exists)

| Duplicate | Pulled by | Action |
| ----------- | ----------- | -------- |
| `reqwest 0.12` (alongside our `0.13`) | `opentelemetry-otlp 0.31` -> `opentelemetry-http 0.31` | Clears with the OpenTelemetry stack bump, held at 0.31 until metrics-exporter-opentelemetry publishes (scalo-rs#281). |
| `tower-http 0.6` (alongside our `0.7`) | `reqwest 0.12` and `reqwest 0.13` | Clears when reqwest moves to tower-http 0.7. |
| `base64 0.22` (alongside our `0.23`) | `tonic 0.14`, `cel 0.14`, `metrics-exporter-prometheus 0.18`, `reqwest 0.12` | Clears as those crates move to base64 0.23. |
| `sha2 0.11`, `hmac 0.13` (alongside our `sha2 0.10`, `hkdf 0.12`) | `aws-sigv4 1.6` | Ours: move `aes-gcm`, `hkdf`, `sha2` and `rand_core` (the `secrets` disk-cache crypto) to the RustCrypto 0.11 line together. |
| `sysinfo 0.28` (alongside our `0.39`) | `yaque 0.6.6` (spool/disk-queue) | yaque pins old sysinfo. Track yaque updates, or revisit the spool backend. Low impact (compiled once, not on the hot path). |

## 2. Ecosystem transitional duplicates (unavoidable, no action)

These are mid-migration splits across the whole Rust ecosystem; pinning is not in our control and forcing a single version is infeasible:

- `thiserror 1 / 2`, `thiserror-impl 1 / 2`
- `syn 1 / 2 / 3`, `synstructure 0.12 / 0.14`
- `mio 0.8 / 1`
- `rand 0.8 / 0.9`, `rand_chacha 0.3 / 0.9`, `rand_core 0.6 / 0.9`
- `hashbrown 0.14 / 0.16 / 0.17`, `bitflags 1 / 2`
- `getrandom 0.2 / 0.3 / 0.4`
- `toml_datetime`, `toml_edit`, `winnow 0.7 / 1.0`
- crypto family mid-bump: `digest 0.10 / 0.11`, `block-buffer`, `crypto-common`, `cpufeatures`
- `either 1.18.0` is listed twice at one version: two builds with different feature sets.

## 3. Notes / watch items

- **Yanked `metrics 0.24.5`:** RESOLVED -- `cargo update -p metrics` lands on
  `0.24.6` (off the yank). Cargo.lock is gitignored for this library, so fresh
  CI/consumer resolution selects 0.24.6 automatically (cargo never picks a
  yanked version unless pinned in a committed lock). No action needed.
- **Minimal feature builds:** the AWS, Vault, TUI, and git stacks are behind
  `secrets-aws` / `secrets-vault` / `cli-service` / `directory-config-git` and
  must NOT appear in a default or transport-only build. Spot-check with
  `cargo tree --no-default-features --features transport-kafka`.

## CI

`scripts/dep-dup-check.sh` runs `cargo tree -d` and prints the count
**warning-only** (never fails the build) -- duplication is tracked, not gated,
because most entries are ecosystem-transitional and outside our control.
