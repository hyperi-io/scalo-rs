# Secrets

`SecretsManager` is the runtime-side reader for secrets the app needs at
runtime -- passwords, API tokens, TLS keys. Backends include a file
provider that's always available, OpenBao/Vault, and AWS Secrets
Manager. The manager caches values with a TTL and broadcasts rotation
events when an underlying source changes.

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

let mgr = SecretsManager::new(SecretsConfig::from_cascade()?)?;

let pwd = mgr.get("kafka/password").await?;
let key = mgr.get_file("/var/secrets/tls/tls.key").await?;

let creds = format!("{}:{}", username, pwd.as_str()?);
```

`get()` resolves through whichever backend the secret name routes to
(based on config). `get_file()` reads from a local path -- useful for
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
| `aws:secret_id`, `aws:secret_id:key` | AWS Secrets Manager lookup, optionally one key out of a JSON secret (needs `secrets-aws`). An ARN is refused -- it carries colons of its own, so use the secret name or an `aws` source in `secrets.sources` |
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

Results are cached with a configurable TTL. Cache hits skip the
network. `refresh_all()` forces re-fetch of every cached secret;
`clear_cache()` drops the cache without re-fetching.

```rust
let stats = mgr.cache_stats();    // hits, misses, size
mgr.clear_cache();                // e.g. after a known rotation
mgr.refresh_all().await?;         // force re-fetch
```

---

## Rotation events

Subscribers see a `RotationEvent` when the manager detects a backend
secret has changed (next refresh after the cache TTL expires, or after
an explicit `refresh_all`):

```rust
let mut rx = mgr.subscribe_rotations();
while let Ok(event) = rx.recv().await {
    tracing::info!(name = %event.name, "secret rotated");
    // tear down and rebuild any client that uses the old value
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

```yaml
secrets:
  default_backend: file       # file | vault | aws
  cache_ttl: 300s
  file:
    base_path: /var/secrets
  vault:
    address: https://vault.internal:8200
    auth: kubernetes          # token | kubernetes | approle
    mount: secret/
  aws:
    region: ap-southeast-2
```

Per-secret backend routing is configurable -- e.g. send
`kafka/password` to Vault and leave `local/dev/token` on the file
backend.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `SecretsManager::new(config)` | Build a manager with configured backends |
| `SecretsConfig::from_cascade()` | Build config from the global cascade (pass to `SecretsManager::new`) |
| `.get(name) -> SecretValue` | Fetch a secret by name through routed backend |
| `.get_file(path) -> SecretValue` | Read a secret from a local file |
| `.refresh_all()` | Re-fetch every cached secret |
| `.clear_cache()` | Drop the cache without re-fetching |
| `.cache_stats() -> CacheStats` | Hits, misses, current size |
| `.subscribe_rotations() -> broadcast::Receiver<RotationEvent>` | Subscribe to rotation notifications |
| `.health_check() -> HashMap<String, bool>` | Per-backend up/down status |
| `SecretProvider` trait | Implement to add a custom backend |
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
