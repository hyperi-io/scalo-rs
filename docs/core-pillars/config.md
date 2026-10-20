# Config

Call `config::setup` once at startup; every module then reads one typed view of
the whole cascade through the same API.

The cascade is **7 layers** -- one typed view assembled from CLI args, env vars,
`.env`, the YAML files, and hard-coded defaults.

---

## Cascade order (highest priority first)

| # | Source | Notes |
| --- | -------- | ------- |
| 1 | CLI args | `Config::merge_cli(args)` after `Config::new` |
| 2 | Environment variables | Prefix from `ConfigOptions::env_prefix`, double-underscore for nesting (`DFE_LOADER__KAFKA__BROKERS`) |
| 3 | `.env` file | `./.env` (plus `~/.env` beneath it with `load_home_dotenv`), loaded by `dotenvy` into env vars -- same precedence as layer 2. Parent directories are not searched |
| 4 | `settings.{env}.yaml` | `{env}` from `APP_ENV` / `ENVIRONMENT` / `ENV` (default `development`) |
| 5 | `settings.yaml` | Team defaults, committed |
| 6 | `defaults.yaml` | Fallback baseline |
| 7 | Hard-coded defaults | `log_level=info`, `log_format=auto` |

Each YAML layer reads every location below that holds the file and merges them in this order, so a later location overrides an earlier one. Within one location `.yaml` beats `.yml`.

```text
./<name>.yaml                         ./<name>.yml
./config/<name>.yaml                  ./config/<name>.yml
/config/<name>.yaml                   /config/<name>.yml
~/.config/<app_name>/<name>.yaml      ~/.config/<app_name>/<name>.yml
<config_paths dir>/<name>.yaml        <config_paths dir>/<name>.yml
```

A `ConfigOptions::config_paths` entry that is a directory is searched like the locations above. An entry that is a file, which is what `--config` passes, joins the `settings` layer only and merges after every other `settings.yaml`, so it outranks those and `defaults.yaml`. `settings.{env}.yaml` and env vars still outrank it.

`<app_name>` resolves from `ConfigOptions::app_name`, then `APP_NAME`, with no other fallback. No name -> the home-config path is skipped.

---

## YAML parsing rules

Every YAML file the cascade reads, every file `load_typed` reads, and every table the directory store reads goes through `scalo::yaml` (feature `yaml`, on with `config`). The rules are the same everywhere:

| YAML | Reads as |
| --- | --- |
| `yes`, `no`, `on`, `off` | Text. Only `true` and `false` are booleans |
| `key:` or `key: ~` into a `String` field | An error at that line. `Option<String>` reads it as `None` and `SensitiveString` as empty. `#[serde(deserialize_with = "scalo::yaml::de::null_as_empty")]` opts a `String` field in |
| A key repeated in one mapping | An error |
| `<<: *anchor` | Merged into the mapping |
| `.inf`, `.nan` in the cascade or an untyped value | An error. A typed `f64` field reads them |
| `0123`, `1_000` in the cascade or an untyped value | The numbers `123` and `1000`, which a `String` field read through the cascade refuses. A file parsed straight into a struct keeps the text in a `String` field. Quote values that must stay text |
| More than one document (`---`) | An error |

A config file may be up to 16 MiB, 100,000 nodes and 32 levels deep. An error names the file, line and column and quotes nothing, so it is safe to log. `YamlError::render_snippet` shows the failing line with a secret-looking value masked, for a person at a terminal.

`scalo::yaml::from_str_reporting` also returns the keys the target type did not take and lints: indentation that is not a multiple of two, and plain leading-zero or `_`-separated numbers.

---

## Usage

### Setup + ad-hoc reads

```rust
use scalo::config::{self, ConfigOptions};

config::setup(ConfigOptions { env_prefix: "DFE_LOADER".into(), ..Default::default() })?;

let cfg = config::get();
let brokers = cfg.get_string_list("kafka.brokers").unwrap();
let timeout = cfg.get_duration("kafka.session_timeout").unwrap_or(Duration::from_secs(30));
```

`get_string` / `get_int` / `get_bool` / `get_duration` / `get_string_list` cover
the primitive lookups.

### Typed deserialisation (dominant pattern)

```rust
let cfg: LoaderConfig = config::get().unmarshal()?;          // whole cascade
let kafka: KafkaConfig = config::get().unmarshal_key("kafka")?;  // one section
```

### An app's whole config

`config::load_typed(path, prefix)` is the loader a `ServiceApp::load_config` calls. With `--config <file>` it parses that file into the app's struct and logs every key the struct does not take: a misspelt nested key at WARN, a top-level key at INFO, since it may be a runtime section. With no file it reads the cascade. Either way it then merges `{PREFIX}_` env vars with `__` nesting (empty values count as unset), applies the flat spellings through `ApplyFlatEnv`, and runs `Normalize`. It also sets up the global cascade with the file as its settings layer, so runtime sections (`metrics`, `version_check`) resolve from the same file.

```rust
fn load_config(&self, path: Option<&str>) -> Result<LoaderConfig, CliError> {
    scalo::config::load_typed(path, self.env_prefix()).map_err(CliError::from)
}
```

The config type derives `Serialize` as well as `Deserialize`, because the env merge round-trips it. A `#[serde(skip)]` field comes back at its default, so set it after the call.

`run_app` loads the config before it sets up logging, because the logger reads its own settings from that config. So `load_typed` holds the lines it would log until a subscriber exists, and `logger::setup` emits them. An app that installs its own subscriber calls `config::log_deferred()` after doing so.

### Registered sections

`unmarshal_key_registered::<T>(key)` deserialises a section AND auto-registers it
in the global section registry, so the `/config` endpoint can dump it with no
per-section wiring.

```rust
let kafka: KafkaConfig = config::get().unmarshal_key_registered::<KafkaConfig>("kafka")?;
```

Modules with sensible defaults expose a `from_cascade` constructor wrapping this
call: `TransportFilterEngine::from_cascade("filters")?`.

---

## Sensitive fields

Masking is applied at the serialisation boundary, not at the call site. Three
concentric defences:

1. `#[serde(skip_serializing)]` on secret fields -- explicit but easy to forget.
2. Name-based redaction -- the registry dump masks a field whose name holds
   `password`, `secret`, `token`, `key`, `credential`, `auth`, `private`, `cert`,
   `encryption`, `connection_string` or `dsn`; the logger, and the config dump
   `config-check` prints, judge a name by `logger::is_credential_name` (see
   [logging.md](logging.md#sensitive-field-masking)).
3. `SensitiveString` newtype -- always serialises as `"***REDACTED***"`, prints
   the same in `Debug` / `Display`. Only `.expose()` reveals the value, and call
   sites are grep-able for review.

Use `SensitiveString` for anything crossing a serialisation boundary:

```rust
#[derive(Deserialize)]
struct KafkaConfig {
    pub brokers: Vec<String>,
    pub username: Option<SensitiveString>,
    pub password: Option<SensitiveString>,
}

let creds = format!("{}:{}",
    cfg.kafka.username.as_ref().unwrap().expose(),
    cfg.kafka.password.as_ref().unwrap().expose());
```

See [sensitive.rs](../../src/config/sensitive.rs).

---

## Flat-env bridge (Kubernetes)

K8s `ConfigMap` / `env:` blocks emit flat env vars (`DFE_LOADER_KAFKA_BROKERS=...`),
but Figment's nested convention is double-underscore (`DFE_LOADER__KAFKA__BROKERS`).
Without bridging, K8s overrides silently fail to apply.

`ApplyFlatEnv` is implemented on the config struct, and names each flat variable
it reads:

```rust
use scalo::config::flat_env::{ApplyFlatEnv, flat_env_list};

impl ApplyFlatEnv for LoaderConfig {
    fn apply_flat_env(&mut self, prefix: &str) {
        if let Some(brokers) = flat_env_list(prefix, "KAFKA_BROKERS") {
            self.kafka.brokers = brokers;
        }
    }
}

let mut cfg: LoaderConfig = config::get().unmarshal()?;
cfg.apply_flat_env("DFE_LOADER");
```

This reads `DFE_LOADER_KAFKA_BROKERS` into `kafka.brokers`, so apps accept both
flat (K8s) and nested (double-underscore) forms. `config::load_typed` and
`flat_env::load_config` call it for you. See [flat_env.rs](../../src/config/flat_env.rs).

---

## Hot-reload

`ConfigReloader<T>` watches the config file (and/or SIGHUP / periodic timer),
rebuilds the typed view on change, and publishes it through `SharedConfig<T>`.
The reloader is built with a reload closure, then started:

```rust
let reloader = ConfigReloader::new(reloader_config, shared.clone(), reload_fn)
    .with_registry_update("kafka");   // fire registry::update("kafka") after reload
let _handle = reloader.start();        // -> JoinHandle<()>; spawns the watch task

// Reader path -- lock-free:
let current = shared.get();            // clone; or shared.read() for a guard
```

`with_registry_update(key)` makes the reloader call `registry::update(key)` after a
successful reload, firing any `registry::on_change(key, cb)` subscribers:

```rust
config::registry::on_change("kafka", |new_section| {
    tracing::info!("kafka config changed, reconnecting");
});
```

See [reloader.rs](../../src/config/reloader.rs) and [shared.rs](../../src/config/shared.rs).
`scalo::rules::reloader` builds one for a rule set in a section of the config file ([../pipeline/rules.md](../pipeline/rules.md#hot-reload)), and the contract's `hot_paths` keeps an edit there from rolling the pods ([../deployment/contract.md](../deployment/contract.md#hot-paths)).

---

## `/config` admin endpoint

Set `enable_config_endpoint = true` (in the HTTP server options) and the runtime
mounts `GET /config` on the metrics server, returning JSON with every registered
section, secrets redacted via `SensitiveString`'s `Serialize` impl.

Opt-in, **off by default** -- it would otherwise expose config on a port reachable
inside the cluster. See [http_server/config.rs](../../src/http_server/config.rs).

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `config::setup(opts)` | One-shot init -- builds the cascade into the global `OnceLock` |
| `config::get() -> &Config` | Reader, panics if not initialised |
| `config::try_get() -> Option<&Config>` | Reader, `None` if not initialised |
| `Config::new(opts)` / `Config::new_async(opts)` | Build without installing globally (`_async` for `config-postgres`) |
| `Config::merge_cli(args)` | Append CLI args as the highest-priority layer |
| `Config::get_*(key)` | Typed primitive lookups (`String`, `i64`, `f64`, `bool`, `Duration`, `Vec<String>`) |
| `Config::unmarshal::<T>()` | Deserialise the whole cascade into `T` |
| `Config::unmarshal_key::<T>(key)` | Deserialise one section |
| `Config::unmarshal_key_registered::<T>(key)` | Same, plus registers the section |
| `config::load_typed(path, prefix)` | An app's config struct: the named file or the cascade, then nested env, flat env and `Normalize` |
| `config::log_deferred()` | Log what `load_typed` noted before a subscriber existed (`logger::setup` calls it) |
| `config::flat_env::ApplyFlatEnv` | K8s flat-env bridge trait |
| `config::flat_env::load_config(path, prefix)` | Cascade with `path` as the settings layer, then flat env and `Normalize` |
| `yaml::from_str` / `from_path` / `from_str_reporting` / `to_value` / `to_string` | Parse and emit YAML by the rules above |
| `config::registry::on_change(key, cb)` | Subscribe to reloads of a section |
| `config::registry::update(key, &value)` | Re-register a section + notify subscribers |
| `config::registry::sections() -> Vec<...>` | Dump the registry (powers `/config`) |
| `config::SensitiveString` | Redacted-on-serialise wrapper; `.expose()` to read |
| `ConfigReloader::new(...).start() -> JoinHandle<()>` | Hot-reload driver (builder-style) |
| `SharedConfig<T>::get()` / `read()` | Lock-free reader for the reloader's current value |

---

## Testing

Build a `Config` directly to avoid touching the global:

```rust
let cfg = Config::new(ConfigOptions {
    env_prefix: "TEST".into(),
    config_paths: vec!["tests/fixtures/config".into()],
    load_dotenv: false,
    ..Default::default()
})?;
```

For tests mutating env vars, use the `temp-env` crate -- `std::env::set_var` is
`unsafe` in edition 2024 and this crate forbids unsafe. The `setup_test_env`
helper is in [tests/common](../../tests/common/).

---

## Related

- [auto-wiring.md](../auto-wiring.md) -- config in the singleton model
- [integration.md](../integration.md) -- `ServiceApp::load_config` recipe
- [core-pillars/logging.md](logging.md) -- sensitive-field masking in logs
- [feature-flags.md](../feature-flags.md) -- `config`, `config-reload`, `config-postgres`
- Source: [`src/config/mod.rs`](../../src/config/mod.rs), [`src/config/registry.rs`](../../src/config/registry.rs), [`src/config/reloader.rs`](../../src/config/reloader.rs)
