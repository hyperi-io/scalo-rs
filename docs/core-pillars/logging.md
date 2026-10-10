# Logging

`logger::setup_default()` installs a `tracing-subscriber` once at startup. Every module then uses `tracing::info!` / `error!` / `#[instrument]` with no handle passing. Output is line-delimited JSON or human-readable text, both with RFC 3339 UTC timestamps. With no format set, an OTEL endpoint gives JSON, a CI runner gives text, a terminal gives text, and anything else (containers, pipes) gets JSON. Text is coloured only on a terminal unless `LOG_COLOR`, `NO_COLOR` or `logger.color` says otherwise. See [Format and colour](#format-and-colour).

The subscriber wraps stderr in a `MaskingWriter` that redacts sensitive field
values at the write boundary -- both `password=secret123` and
`"password":"secret123"` come out `[REDACTED]`. Masking is on by default; disable
it explicitly if you must. JSON lines are also enriched with `service` /
`version` (from `SERVICE_NAME` / `SERVICE_VERSION` or `ServiceApp`) and K8s context
(`pod_name`, `namespace`, `node_name`) from
[`env::runtime_context`](../../src/env.rs); those K8s fields are absent on bare
metal.

---

## Setup

```rust
use scalo::logger;
logger::setup_default()?;                       // env-driven -- what ServiceApp calls

// or explicit
use scalo::logger::{setup, LoggerOptions, LogFormat};
setup(LoggerOptions {
    level: tracing::Level::DEBUG,
    format: LogFormat::Json,
    enable_masking: true,
    ..Default::default()
})?;
```

`setup_default()` reads:

| Var | Effect |
| --- | --- |
| `LOG_LEVEL` / `RUST_LOG` | Level filter; falls back to `EnvFilter` for per-module filters (`hyper=warn,my_app=debug`) |
| `LOG_FORMAT` | `json` / `text` / `auto` (default). Unset, blank, `auto` or unrecognised defers to `logger.format`, then to the derived default |
| `LOG_COLOR` | Text-mode colour: `true` / `1` / `yes` (any case) is on, any other value off. Outranks `NO_COLOR` and `logger.color` |
| `NO_COLOR` | Disable ANSI colour, even on a TTY, when `LOG_COLOR` is unset |
| `LOG_THROTTLE_ENABLED` | Global `tracing-throttle` token bucket (default off) |
| `LOG_THROTTLE_BURST` | Burst capacity (default 50) |
| `LOG_THROTTLE_RATE` | Recovery tokens/sec (default 1.0) |
| `SERVICE_NAME` / `SERVICE_VERSION` | Injected into JSON lines |

When config is loaded, `setup_default()` reads `logger.format` and `setup()` reads `logger.color` from the cascade.

---

## Format and colour

These rules match scalo-py's logger.

The format is the first selector that names `json` or `text` (`pretty` and `human` are aliases of `text`):

1. `--log-format` on a [`CommonArgs`](../../src/cli/args.rs) CLI
2. `LOG_FORMAT`
3. `logger.format` in config

`auto` and blank defer to the next selector. An unrecognised value fails `CommonArgs::to_logger_options` with an invalid-argument error, and defers in `setup_default()`. With nothing concrete set, the format is derived, first match wins:

1. `OTEL_EXPORTER_OTLP_ENDPOINT` set and not blank: JSON
2. A CI runner (`CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI` or `TRAVIS` equal to `true`, or `JENKINS_URL` present): text
3. stderr is a terminal: text
4. Anything else: JSON

`LogFormat::Auto` resolves by the same derivation. `setup(opts)` takes `opts.format` as given and reads neither `LOG_FORMAT` nor `logger.format`: an explicit `Json` or `Text` is used as is, and `Auto` goes straight to the derivation.

Colour applies to text output only; JSON is never coloured. First match wins:

1. `LOG_COLOR`: `true`, `1` or `yes` in any case is on, any other value (empty included) is off
2. `NO_COLOR` present, with any value: off
3. `logger.color` in config: a bool, a number (non-zero is on), or the strings `LOG_COLOR` accepts
4. Whether stderr is a terminal

So text piped to a file or another process carries no ANSI escapes unless one of the first three turns colour on.

---

## Sensitive-field masking

Applied at the write boundary, so it is the catch-all regardless of how a value
reaches a log line. Default list (case-insensitive substring match on field name)
covers password / token / api_key / secret / credential / auth / bearer /
private_key / client_secret / refresh_token / access_token / ssn / credit_card /
cvv / pin and their common spellings. See
[`default_sensitive_fields()`](../../src/logger/masking.rs).

JSON mode walks the object tree and redacts at any depth; text mode scans for
`name=value` and `name="value"`. Both write `[REDACTED]` in place.

Extend rather than replace:

```rust
let mut fields = logger::default_sensitive_fields();
fields.push("internal_session_id".into());
setup(LoggerOptions { sensitive_fields: fields, ..Default::default() })?;
```

For values with no recognisable field name (e.g. a token in a URL), use
`SensitiveString` from [config](config.md) -- it serialises as `***REDACTED***`
regardless of caller.

Masking reads field names, never values. An email address, literal or percent-encoded (`sentinel.user%40example.com` in a URL), or an opaque provider user id such as Okta's `00u1sentinel2abc3`, is written as-is unless the field carrying it is on the list, and `email` is not on the default list. scalo-py's logger redacts email addresses by value; scalo-rs has no value scrubber yet (see [Planned](../README.md#planned-not-in-current-release)).

---

## Flood-control helpers

Per-call-site rate limiting on lock-free atomics in
[`logger/helpers.rs`](../../src/logger/helpers.rs); all three are ~5 ns when
suppressed.

```rust
use std::sync::atomic::{AtomicBool, AtomicU64};
use scalo::logger::{log_state_change, log_sampled, log_debounced};

// 1. Sustained conditions -- log only on the transition
static PRESSURE_HIGH: AtomicBool = AtomicBool::new(false);
if log_state_change(&PRESSURE_HIGH, current > threshold) {
    tracing::warn!(current, threshold, "memory pressure crossed threshold");
}

// 2. Hot-path errors -- log first + every Nth
static SEND_ERRORS: AtomicU64 = AtomicU64::new(0);
if log_sampled(&SEND_ERRORS, 1000) { tracing::error!("kafka send failed"); }

// 3. Tight poll loops -- log at most once per N ms
static LAST_WARN: AtomicU64 = AtomicU64::new(0);
if log_debounced(&LAST_WARN, 5_000) { tracing::warn!("udp recv backlog"); }
```

Pair sampled / debounced calls with a metric counter -- the helper gates
emission, the metric carries the count.

For service-wide dedup of identical events (different sites, same signature), set
`LOG_THROTTLE_ENABLED=1`. The `tracing-throttle` layer dedups by event signature
with a token bucket; high-cardinality fields (`request_id`, `trace_id`,
`span_id`) are excluded from the signature by default so per-request lines don't
collapse into one.

---

## API surface

| Item | Purpose |
| --- | --- |
| `logger::setup_default()` | Env-driven install -- `ServiceApp` calls this |
| `logger::setup(opts)` | Explicit install |
| `LoggerOptions` | `level`, `format`, `add_source`, `enable_masking`, `sensitive_fields`, `span_events`, `throttle`, `service_name`, `service_version` |
| `LogFormat::{Json, Text, Auto}` | Format; `Auto` resolves on `setup` |
| `LoggerSettings` | `level`, `format`, `color` under the `logger` config key; `LoggerSettings::from_cascade()` |
| `ThrottleConfig` | `enabled`, `burst`, `rate`, `max_signatures`, `excluded_fields` |
| `logger::default_sensitive_fields() -> Vec<String>` | Baseline mask list -- extend, don't replace |
| `logger::mask_sensitive_string(input, patterns) -> String` | Ad-hoc redaction outside the logger |
| `MaskingLayer` / `MaskingWriter<W>` | Detector + the writer wrapper `setup` installs |
| `log_state_change(&AtomicBool, new) -> bool` | Transition gate |
| `log_sampled(&AtomicU64, every_n) -> bool` | Nth-occurrence gate |
| `log_debounced(&AtomicU64, period_ms) -> bool` | Time-window gate |
| `SecurityEvent` / `SecurityOutcome` | Audit-log event types in [`logger/security.rs`](../../src/logger/security.rs) |

---

## Testing

The subscriber installs globally, once per process. Tests needing a fresh logger
should not call `setup_default` -- capture a `tracing` `MakeWriter`, or skip
logger init (the macros are no-ops with no subscriber). For env-var mutation use
`temp-env` -- `std::env::set_var` is `unsafe` in edition 2024 and this crate
forbids unsafe.

---

## Related

- [config.md](config.md) -- `SensitiveString` for value-level redaction
- [metrics.md](metrics.md) -- sampled log + metric counter is the standard pair
- [tracing.md](tracing.md) -- spans and `#[instrument]` feed OTel
- [../auto-wiring.md](../auto-wiring.md) -- singleton install pattern
- [../feature-flags.md](../feature-flags.md) -- `logger`
- Source: [`src/logger/mod.rs`](../../src/logger/mod.rs), [`src/logger/helpers.rs`](../../src/logger/helpers.rs), [`src/logger/masking.rs`](../../src/logger/masking.rs)
