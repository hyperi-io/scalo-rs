# GeoIP database provisioning

`geoip_download` gets MMDB files onto disk. That is all it does -- there
is no lookup engine and no MMDB reader here. It resolves which databases
a service wants, checks whether the local copies are still fresh, and
downloads replacements when they are not. What reads the files is the
caller's business: an MMDB reader crate, a VRL enrichment table, a
sidecar.

Feature: `geoip-download`. Off by default.

```toml
[dependencies.scalo]
version = "2"
features = ["geoip-download"]
```

---

## Non-fatal by contract

`ensure_databases` returns `Ok` with `None` paths when a download fails.
It never stops a service from starting. GeoIP going missing degrades
enrichment; it is not a startup fault, and treating it as one turns a
third-party outage into an outage of yours.

The fallback ladder, per database:

1. Local file inside `max_age_days` -- used as is, no request.
2. Otherwise download; on success, that file.
3. On failure, the stale local file if there is one (logged at `warn`).
4. Otherwise `None`.

---

## Usage

```rust
use scalo::geoip_download::{GeoIpConfig, ensure_databases};

let paths = ensure_databases(&GeoIpConfig::from_cascade()).await?;

if let Some(city) = paths.city {
    // hand the path to your reader
}
```

`from_cascade()` reads the `geoip` config section. Pass an explicit
`GeoIpConfig` when the app owns the values itself.

---

## Providers

| Provider (`provider:`) | Auth | City | ASN | Format |
|---|---|---|---|---|
| `db_ip_lite` (default) | none | yes | yes | gzip |
| `max_mind_geo_lite2` | account id + licence key | yes | yes | tar.gz |
| `ip_locate` | none | no | yes | raw |
| `ip_info_lite` | token | yes | no | raw |
| `sapics` | none | no | yes | raw |
| `custom` | n/a | explicit path | explicit path | n/a |

Set `city_db_path` or `asn_db_path` and the provider is bypassed
entirely: those paths are returned verbatim and nothing is downloaded or
checked. That is the escape hatch for an operator who mounts the
databases themselves.

---

## Config shape

```yaml
geoip:
  enabled: true
  provider: db_ip_lite
  city_db_path: null
  asn_db_path: null
  auto_download:
    enabled: true
    data_dir: /var/lib/geoip
    max_age_days: 30
    maxmind_account_id: null
    maxmind_license_key: null
    ipinfo_token: null
```

The three credentials are `SensitiveString`: redacted in `Debug`, in
`Serialize`, and in the emitted JSON schema. Supply them through the
secrets layer rather than as literals in a config file.

`enabled: false` makes `ensure_databases` a no-op, so an app can wire the
call unconditionally and let config decide.

---

## API surface

| Item | Purpose |
|---|---|
| `ensure_databases(&GeoIpConfig) -> Result<DatabasePaths, GeoIpDownloadError>` | Resolve and, if needed, download |
| `DatabasePaths { city, asn }` | `Option<PathBuf>` each |
| `GeoIpConfig` / `GeoIpConfig::from_cascade()` | Provisioning config |
| `AutoDownloadConfig` | Download and credential settings |
| `GeoIpProvider` | Provider enum |
| `GeoIpDownloadError` | Error type, absorbed by `ensure_databases` |

---

## What it reuses

- `HttpClient` (`http` feature) for the transfer, including its
  retry/backoff and request metrics. There is no second HTTP stack.
- `SensitiveString` for the credentials.
- The config cascade for `from_cascade()`.

The only additions are `flate2` and `tar`, the two archive formats the
providers publish in.

Bodies stream to a sibling temp file rather than into memory -- a city
MMDB runs to hundreds of megabytes and a memory-capped pod cannot hold
the compressed and decompressed copies at once. Decompression and tar
extraction run on `spawn_blocking`. The final move into place is a
same-directory rename, so a reader never sees a half-written database.

---

## Migrating from a local copy

An app that carried its own `geoip_download` maps across like this:

| App-side | scalo |
|---|---|
| `GeoIpConfig.enabled` | same field, now honoured by `ensure_databases`, and defaults to `true` |
| `GeoIpConfig.cache_capacity` | **stays in the app** -- a lookup-cache size, not a provisioning input |
| `city_db_path` / `asn_db_path`: `Option<String>` | `Option<PathBuf>` (same YAML) |
| `auto_download.data_dir`: `String` | `PathBuf` (same YAML) |
| `auto_download.maxmind_account_id` / `_license_key`: `Option<String>` | `Option<SensitiveString>`; `.as_deref()` becomes `.map(SensitiveString::expose)` |
| `auto_download.ipinfo_token` | unchanged (`Option<SensitiveString>`) |
| default `data_dir` | `/var/lib/geoip` -- set it explicitly if the old value matters |
| `GeoIpDownloadError::Http(reqwest::Error)` | `Http(scalo::http_client::HttpError)`, plus `UnexpectedStatus` |
| `GeoIpDownloadError::NoDatabases(String)` | `NoDatabases { provider, kind }` |
| tar member missing, as `Io(NotFound)` | `ArchiveMemberMissing { member }` |

An app keeping its own `enabled` and `cache_capacity` nests the scalo
type:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GeoIpConfig {
    pub cache_capacity: usize,
    #[serde(flatten)]
    pub databases: scalo::geoip_download::GeoIpConfig,
}
```

The YAML shape is unchanged; call sites become
`ensure_databases(&config.geoip.databases)`.

---

## Related

- [http-client.md](http-client.md) -- the client this rides on
- [secrets.md](secrets.md) -- where the credentials should come from
- [../feature-flags.md](../feature-flags.md) -- `geoip-download`
- Source: [../../src/geoip_download/](../../src/geoip_download/)
