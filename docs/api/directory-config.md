# Directory config

A YAML directory-backed config store -- distinct from the [7-layer
config cascade](../core-pillars/config.md). Use this for ops-managed
config that changes between deploys without a code change: detection
rules, scrub patterns, routing tables, allow/deny lists, any artefact
that ops curates as a set of YAML files in a directory.

The cascade is for *application config* (set at startup, optionally
hot-reloaded). This is for *operational data* (read continuously,
written via tooling or git push).

---

## When to use which

| Need | Use |
| ------ | ----- |
| App config (Kafka brokers, ports, log level) | [Config cascade](../core-pillars/config.md) |
| Per-environment overrides of app config | Cascade -- `settings.{env}.yaml` |
| One YAML per logical object, ops adds/removes them | This module |
| File-system-shaped data store ops can edit by hand | This module |
| Git as the audit trail for changes | This module with `directory-config-git` |

If you find yourself growing the cascade into a "rules engine", you
want this module instead.

---

## Usage

```rust
use scalo::directory_config::{DirectoryConfigStore, DirectoryConfigStoreConfig};

let mut store = DirectoryConfigStore::new(DirectoryConfigStoreConfig {
    directory: "/etc/myapp/rules".into(),
    git_enabled: false,
    ..Default::default()
}).await?;

store.start().await?;

// List every YAML file in the directory (one per "table"):
let tables = store.list_tables().await;
// tables == ["allowlist", "routing_rules", "scrub_patterns", ...]

// Read one file as an untyped value or a typed struct:
let raw: serde_json::Value = store.get("scrub_patterns").await?;
let typed: ScrubPatterns = store.get_as("scrub_patterns").await?;

// Look up a key within a table:
let v = store.get_key("routing_rules", "kafka.events.high").await?;
```

`start()` begins the background refresh, which re-reads the directory every
`refresh_interval`. `stop()` ends it. A file that fails to parse keeps its last
good contents and logs a warning.

Tables parse with [`scalo::yaml`](../core-pillars/config.md#yaml-parsing-rules)'s
`Data` profile: up to 1 GiB per file, `yes` and `no` are text, and a repeated
key is an error. A plain `0123` or `1_000` reads as a number in an untyped
table, so quote a value that must stay text.

---

## Writes and locking

The store detects its write mode when it opens the directory: `ReadOnly` when
the directory is not writable, `GitCommit` when `git_enabled` is set and the
directory is a git repository, `DirectWrite` otherwise.

```rust
store.set("routing_rules", "kafka.events.high", serde_json::json!({ "topic": "events_high" }), None).await?;
store.delete_key("routing_rules", "kafka.events.low", None).await?;
```

A write rewrites the whole file and keeps the order of its keys. Comments in
the file are not kept. A key path through a value that is not a mapping is
refused with `KeyPathConflict`, and the file is left as it was.

File-level locking via `std::fs::File::{lock,unlock}` (stable since
Rust 1.89) -- no third-party lock crate. Concurrent writes from the
same process serialise; cross-process writes are also safe.

`ReadOnly` rejects writes at the API.

---

## Git integration (optional)

With the `directory-config-git` feature and a directory that is a git
repository, each `set` / `delete_key` commits the file through `libgit2`, and
pushes when `git_push` is set. The optional `message` argument is the commit
message, `config: update <table>` by default.

`is_git()`, `current_branch()`, `list_branches()` and `switch_branch()` expose
the backing repository.

---

## Change notifications

`on_change()` returns a `broadcast::Receiver<ChangeEvent>` consumers
can listen to:

```rust
let mut rx = store.on_change();
while let Ok(event) = rx.recv().await {
    tracing::info!(table = %event.table, "directory config changed");
    rebuild_rules_engine(event.table).await;
}
```

Events fire on writes and on files the refresh finds added, changed or
removed. Use this for hot-reload of derived data structures.

---

## Config shape

`DirectoryConfigStoreConfig` derives `Deserialize` with every field defaulted,
so an app can read it from a section of its own config:

```yaml
rules_store:
  directory: /etc/myapp/rules
  refresh_interval: { secs: 30, nanos: 0 }
  git_enabled: true
  git_push: false
  git_author_name: DirectoryConfigStore
  git_author_email: directory-config@localhost
```

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `DirectoryConfigStore::new(config)` | Build a store (async -- loads every table) |
| `.start()` | Start the background refresh |
| `.stop()` | Stop the background refresh |
| `.list_tables() -> Vec<String>` | Every YAML file name in the directory |
| `.get(table) -> serde_json::Value` | Read one table as an untyped value |
| `.get_as::<T>(table) -> T` | Read and deserialise into a typed struct |
| `.get_key(table, key) -> serde_json::Value` | Read one nested key |
| `.set(table, key, value: serde_json::Value, message)` | Set one nested key (needs a writable store) |
| `.delete_key(table, key, message)` | Remove one nested key |
| `.on_change() -> broadcast::Receiver<ChangeEvent>` | Subscribe to writes and refreshes |
| `.write_mode() -> WriteMode` | The detected write mode |
| `.is_git() -> bool` | True if writes commit to git |
| `.current_branch() -> Option<String>` | Backing repo branch name (`directory-config-git`) |

---

## Related

- [../core-pillars/config.md](../core-pillars/config.md) -- app config cascade
- [../feature-flags.md](../feature-flags.md) -- `directory-config`, `directory-config-git`
- Source: [../../src/directory_config/](../../src/directory_config/)
