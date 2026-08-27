# Version check

Non-blocking startup probe that asks a configured releases endpoint
whether a newer version of the service is available, and logs the result.
Spawned as a tokio task, fire-and-forget -- never blocks startup, never
panics, never affects exit code. Feature flag: `version-check`.

The check is OPT-OUT: wiring it into a binary is the opt-in, so `enabled`
defaults true -- and it stays inert until an `api_url` is supplied, so
nothing is ever sent unless the binary (or config) names an endpoint. An
explicit `version_check.enabled: false` in any config layer is the
opt-out and always wins. Don't want a check at all? Don't compile the
feature in.

## Quick start

`ServiceApp` binaries get the check for free from `ServiceRuntime::build`;
the app supplies only its endpoint via the trait hook:

```rust
fn version_check_defaults(&self) -> scalo::version_check::VersionCheckConfig {
    scalo::version_check::VersionCheckConfig {
        api_url: "https://releases.example.com/api/v1/check".into(),
        ..Default::default()
    }
}
```

Outside the runtime, call it directly:

```rust
use scalo::version_check::{VersionCheck, VersionCheckConfig};

let config = VersionCheckConfig::from_cascade_or(
    "my-service",
    env!("CARGO_PKG_VERSION"),
    VersionCheckConfig {
        api_url: "https://releases.example.com/api/v1/check".into(),
        ..Default::default()
    },
);
VersionCheck::new(config).check_on_startup();
```

## Configuration

All settings live under the `version_check` cascade key. The env form
nests with a double underscore through the app's prefix, e.g.
`DFE_LOADER_VERSION_CHECK__ENABLED=false`.

```yaml
version_check:
  enabled: true
  api_url: "https://releases.example.com/api/v1/check"
  timeout: 5
  send_instance_id: true   # false = no identifier in the payload
  instance_id: ""          # explicit override of the derived id
```

| Setting | Library default | Purpose |
|---------|-----------------|---------|
| `enabled` | `true` | The kill switch. An explicit `false` wins over any app default. |
| `api_url` | empty | Endpoint. No baked-in default -- the binary supplies its own; without one the check is inert. |
| `timeout` | `5` | HTTP timeout in seconds. |
| `send_instance_id` | `true` | `false` strips the instance id from the payload. |
| `instance_id` | empty | Explicit id, sent verbatim -- overrides the derived one. |

`from_cascade_or` overlays the cascade on the caller's defaults: a key
the cascade sets wins, an unset key falls to the given default. Plain
`from_cascade` is the same overlay on this type's own defaults.

## Payload

POST body, matching scalo-py field for field:

```json
{
  "product": "dfe-loader",
  "current_version": "1.8.0",
  "os": "linux",
  "arch": "x86_64",
  "instance_id": "4f9f9577-e391-5835-8236-3e88e902b11b"
}
```

`deployment` is never sent: the payload struct has no such field, so
free-form deployment strings cannot leak. `instance_id` is derived from
what the app runs on -- first hit wins: explicit config id; Kubernetes
(UUIDv5 over the serviceaccount cluster CA + namespace); `/etc/machine-id`
app-scoped outside containers; a UUID persisted under `~/.config/scalo/`;
an ephemeral UUID. The UUIDv5 namespace is shared with scalo-py, so both
chassis derive the same id on the same platform, and the derived forms are
one-way -- nothing about the host is recoverable.

## Failure handling

Everything inside the spawned check -- DNS failure, timeout, HTTP error,
malformed JSON -- is caught and logged once at WARN as
`version check failed (non-fatal)`. Air-gapped sites set
`version_check.enabled: false`; without it the cost is one WARN line and
one connect timeout per boot, nothing more.
