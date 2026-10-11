# Secrets

`SecretsManager` is the runtime-side reader for secrets the app needs at
runtime -- passwords, API tokens, TLS keys. Backends include a file
provider that's always available, OpenBao/Vault, and AWS Secrets
Manager. The manager caches values with a TTL and broadcasts rotation
events when a refresh finds that a source's version has changed.

Where `SensitiveString` (from `config`) covers values that arrive
through the config cascade, `SecretsManager` covers values that need
out-of-band fetching -- typically because they're rotated independently
of deploys.

---

## Backends

| Backend | Feature | Backed by | Use when |
| --------- | --------- | ----------- | ---------- |
| File | `secrets` (default) | Plain files on disk | Local dev, K8s `Secret` volume mounts |
| OpenBao / Vault | `secrets-vault` | [`vaultrs`](https://crates.io/crates/vaultrs) | Centralised secret store, rotation enabled |
| AWS Secrets Manager | `secrets-aws` | [`aws-sdk-secretsmanager`](https://crates.io/crates/aws-sdk-secretsmanager) | AWS-native deployments |
| `secrets-all` | -- | Vault + AWS | When you need both |

The file backend is always wired when the `secrets` feature is on.
Backend-specific providers are opt-in.

---

## Usage

```rust
use scalo::secrets::{SecretsConfig, SecretsManager};

let mgr = SecretsManager::new(SecretsConfig::from_cascade())?;

let pwd = mgr.get("db_password").await?;
let key = mgr.get_file("/var/secrets/tls/tls.key").await?;

let creds = format!("{}:{}", username, pwd.as_str()?);
```

`get()` takes a source name from `secrets.sources` and reads it from the backend that entry names (see [Config shape](#config-shape)).
`get_file()` reads from a local path -- useful for
K8s `Secret` volume mounts that come through as files.

---

## Credential specs

`resolve()` / `resolve_optional()` turn a short spec string into a
plaintext value, so config knobs and data-plane services share one
syntax instead of each hand-rolling its own env/vault lookup:

| Spec | Resolves to |
| ------ | ------------- |
| `vault:<mount>/<path>:<key>` | OpenBao lookup via `SecretsManager`: the first segment is the KV mount, the KV v2 `data` segment is optional (needs `secrets-vault`) |
| `vault:<name>:<key>` | the same lookup on the default `secret` mount, for a path with no mount segment |
| `bao:<mount>/<path>:<key>`, `openbao:<mount>/<path>:<key>` | the same lookup, spelled as the OpenBao tooling spells it |
| `file:path` | the contents of a local file, read fresh every time so a rotated mount is seen; a zero-byte file is refused |
| `aws:secret_id`, `aws:secret_id:key` | AWS Secrets Manager lookup, optionally one key out of a JSON secret (needs `secrets-aws`). An ARN is refused -- it carries colons of its own, so use the secret name or an `aws` source in `secrets.sources`. So is an empty key (`aws:name:`) |
| `env:VAR` | the environment variable `VAR` (hard error if unset) |
| anything else | the literal string |

```rust
use scalo::secrets::resolve;

// "env:KAFKA_PASSWORD" -> the env value; "s3kr3t" -> "s3kr3t" verbatim
let password = resolve(spec).await?;
```

`resolve_optional(Option<&str>)` returns `None` for `None` or an empty
string, otherwise resolves the inner spec. Failures surface as
`CredentialError` -- missing env var, unreadable file, vault or AWS
lookup failure, malformed spec, or a spec naming a provider whose
feature is off.

---

## Caching

`get()` serves a cached value until `ttl_secs` passes, then goes back to the backend. If the backend fails, `get()` serves the expired value for up to `stale_grace_secs` more before returning the error. The disk tier is written only with an `encryption_key`, or with `allow_plaintext_disk_cache` outside production. Without either the cache is memory-only.

`refresh_all()` re-fetches every source in `secrets.sources` from its backend, fresh entries included, and replaces the cached values, so the next `get()` returns them. A source that fails is named in the returned `SecretsError::RefreshFailed` and keeps its cached value, and the other sources still refresh. Unlike `get()`, `refresh_all()` never answers a failed source from the stale cache. Paths read with `get_file()` are not refreshed. `clear_cache()` drops the cache without re-fetching.

scalo runs no background refresh. Call `refresh_all()` on your own schedule, a `tokio::time::interval` loop for example.

```rust
let stats = mgr.cache_stats();    // memory and disk entries, hits, misses, stale hits
mgr.clear_cache();                // e.g. after a known rotation
mgr.refresh_all().await?;         // re-fetch every configured source
```

### Turning caching off

`cache.enabled: false` turns off both tiers. Nothing is held in memory, nothing is written to disk and no cache directory is created. With it off:

- every `get()` and `get_file()` reads from the backend
- a backend failure is returned to the caller, never answered from a stale value
- `refresh_all()` re-reads every source but sends no rotation events, because there is no previous version to compare against

The TTL, grace period, directory, encryption and file-mode settings have no effect while it is off.

---

## Rotation events

`refresh_all()` sends a `RotationEvent` when the version it fetches for a source differs from the version the cache last held for that source, expired or not. The event carries both versions. `get()` never sends one.

Only backends that report a version can rotate: OpenBao (the KV v2 version number) and AWS Secrets Manager (the version id). A file source reports none, and with caching off there is no previous version, so neither produces an event.

```rust
use tokio::sync::broadcast::error::RecvError;

let mut rx = mgr.subscribe_rotations();
loop {
    match rx.recv().await {
        Ok(event) => {
            tracing::info!(name = %event.name, version = %event.new_version, "secret rotated");
            // tear down and rebuild any client that uses the old value
        }
        // The channel holds 16 events, so a slow subscriber skips the oldest.
        Err(RecvError::Lagged(_)) => continue,
        Err(RecvError::Closed) => break,
    }
}
```

Use this for client builders that hold a credential (Kafka client,
database pool) -- react to rotation by rebuilding the client.

---

## Health checks

`health_check()` calls into each enabled backend and returns a map of
backend name to up/down status. Wire into the `HealthRegistry` so
`/readyz` reflects backend availability:

```rust
HealthRegistry::register("secrets", move || async move {
    let status = mgr.health_check().await;
    if status.values().all(|&ok| ok) { HealthStatus::Healthy }
    else { HealthStatus::Unhealthy("one or more backends down".into()) }
});
```

---

## Config shape

`SecretsConfig::from_cascade()` reads the `secrets` key. Every block is optional, and every `cache` value below is its default except `directory`.

```yaml
secrets:
  cache:
    enabled: true
    ttl_secs: 3600
    stale_grace_secs: 86400
    directory: /var/cache/myapp/secrets
    allow_plaintext_disk_cache: false
  openbao:                    # needs the secrets-vault feature
    address: https://openbao.example.com:8200
    auth:
      method: kubernetes      # token | app_role | kubernetes
      role: myapp
    ca_cert: /etc/ssl/openbao-ca.pem
  aws:                        # needs the secrets-aws feature
    region: ap-southeast-2
  sources:
    db_password:
      provider: open_bao
      path: secret/data/myapp/db
      key: password
    tls_key:
      provider: file
      path: /var/secrets/tls/tls.key
    api_token:
      provider: aws
      secret_id: myapp/api
      key: token
```

Each entry under `sources` is a name `get()` takes, routed to one backend:

| `provider` | Fields | Feature |
| ---------- | ------ | ------- |
| `file` | `path` | `secrets` |
| `open_bao` | `path` as `<mount>/<path>` (the KV v2 `data` segment optional), `key` | `secrets-vault` |
| `aws` | `secret_id`, plus an optional `key` that picks one key out of a JSON secret | `secrets-aws` |

A source whose backend's feature is off fails with `SecretsError::ProviderNotConfigured`.

`cache`:

| Field | Default | Effect |
| ----- | ------- | ------ |
| `enabled` | `true` | `false` turns the cache off, see [Turning caching off](#turning-caching-off) |
| `ttl_secs` | `3600` | Seconds a cached value is served without going to the backend |
| `stale_grace_secs` | `86400` | Seconds past the TTL that `get()` still serves a cached value when the backend fails |
| `directory` | the platform cache directory (`~/.cache` on Linux) + `scalo/secrets` | Where the disk tier writes, created when the manager is built |
| `encryption_key` | unset | Encrypts the disk tier with AES-256-GCM. Unset, the disk tier is skipped and the cache is memory-only |
| `allow_plaintext_disk_cache` | `false` | Writes the disk tier unencrypted when no `encryption_key` is set. Rejected under a production profile |
| `dir_mode` | `0o700` | Unix mode forced on `directory`. `null` skips the chmod |
| `file_mode` | `0o600` | Unix mode for cache files. `null` skips the chmod |
| `refresh_interval_secs` | `1800` | Parsed, read by nothing: there is no background refresh |
| `refresh_jitter_secs` | `300` | Parsed, read by nothing, as above |

`openbao` takes `address`, `auth`, and optionally `namespace`, `ca_cert` (a PEM CA file) and `skip_verify` (default `false`, rejected under a production profile). `auth.method` picks the fields that go with it:

| `method` | Fields |
| -------- | ------ |
| `token` | `token` |
| `app_role` | `role_id`, `secret_id`, `mount` (default `approle`) |
| `kubernetes` | `role`, `token_path` (default the pod's service account token), `mount` (default `kubernetes`) |

`aws` takes `region` and `endpoint_url`, both optional. An unset region is left to the AWS SDK's own resolution chain.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `SecretsManager::new(config)` | Build a manager with configured backends |
| `SecretsConfig::from_cascade()` | Build config from the global cascade (pass to `SecretsManager::new`) |
| `.get(name) -> SecretValue` | Fetch a secret by name through routed backend |
| `.get_file(path) -> SecretValue` | Read a secret from a local file |
| `.refresh_all()` | Re-fetch every configured source, report rotated versions |
| `.clear_cache()` | Drop the cache without re-fetching |
| `.cache_stats() -> CacheStats` | Memory and disk entries, hits, misses, stale hits |
| `.subscribe_rotations() -> broadcast::Receiver<RotationEvent>` | Subscribe to rotation notifications |
| `.health_check() -> HashMap<String, bool>` | Per-backend up/down status |
| `SecretProvider` trait | The interface the built-in backends implement. `SecretsManager` takes no custom backend |
| `SecretValue::as_str() -> SecretsResult<&str>` | Reveal the raw value as UTF-8 (grep-able call site) |
| `SecretValue::as_bytes() -> &[u8]` | Reveal the raw value as bytes |
| `resolve(spec) -> String` | Resolve a `vault:<mount>/<path>:<key>`, `bao:`, `openbao:`, `file:`, `aws:`, `env:` or literal credential spec |
| `resolve_optional(Option<spec>) -> Option<String>` | As `resolve`, but `None`/empty in -> `None` out |
| `CredentialError` | Failure modes for `resolve` / `resolve_optional` |

---

## Related

- [../core-pillars/config.md](../core-pillars/config.md) -- `SensitiveString` for config-time secrets
- [../core-pillars/health.md](../core-pillars/health.md) -- wiring backend health into `/readyz`
- [../auto-wiring.md](../auto-wiring.md) -- singleton model
- [../feature-flags.md](../feature-flags.md) -- `secrets`, `secrets-vault`, `secrets-aws`, `secrets-all`
- Source: [../../src/secrets/](../../src/secrets/)
