# Native Deps

A release build links every C library the Kafka transport and the spool use -- librdkafka, OpenSSL, zlib, zstd -- statically, so the image installs none of them. libgit2 (`directory-config-git`) is the one runtime package left.

`NativeDepsContract` is the bookkeeping. The Dockerfile generator reads
it and emits the right `apt-get` block, with no per-app hand-coding.

rdkafka-sys runs with `dynamic-linking`, which finds librdkafka through pkg-config. The release build points pkg-config at a static-only prefix that hyperi-ci builds once per library release (hyperi-io/hyperi-ci#742): `librdkafka.a` with SSL and zstd, OpenSSL on the 3.5 LTS line, zstd and zlib, each with its `.pc` file. The build sets:

| Variable | Value |
| --- | --- |
| `PKG_CONFIG_PATH` | the prefix's `lib/pkgconfig` |
| `RDKAFKA_STATIC` | `1`, so pkg-config emits the static link line |
| `OPENSSL_STATIC` | `1` |
| `OPENSSL_DIR` | the prefix |

The static librdkafka has no libcurl and no libsasl2, so `sasl.oauthbearer.method: oidc` and GSSAPI are unavailable. SCRAM and PLAIN over TLS need neither.

That OpenSSL is built with `--openssldir=/etc/ssl`, so it reads CA certificates from `/etc/ssl/cert.pem` and `/etc/ssl/certs`. Every generated image installs `ca-certificates`, which fills `/etc/ssl/certs`.

A local dev build with none of those set links the system `librdkafka-dev` dynamically.

librdkafka, OpenSSL, `rdkafka` and `rdkafka-sys` take each release with no 7-day cooldown: they are mature, very widely used libraries, and staying current outweighs the delay. The versions are still pinned with checksums.

---

## Auto-detection from features

The usual path is `for_scalo_features()`. Pass the same feature flags
the app enables on scalo, get back the runtime packages and
any custom APT repos:

```rust
use scalo::deployment::NativeDepsContract;

let deps = NativeDepsContract::for_scalo_features(
    &["transport-kafka", "spool", "secrets", "directory-config-git"],
    "debian:trixie-slim",
);
// deps.apt_repos    = []
// deps.apt_packages = ["libgit2-1.9"]
```

Runtime package names are release-specific, so the mapping needs to know
which distro release the base image is. `for_features()` takes that release
as a `BaseDistro` directly; `for_scalo_features()` resolves it, in order:

1. `deployment.base_distro` in the config cascade, if set and recognised.
2. The env var `DEPLOYMENT__BASE_DISTRO` - the same key's ENV-layer spelling.
3. The base image tag, if it positively names a release.
4. Otherwise the default (`trixie`), recorded in
   `unresolved_base_image` so the generated Dockerfile carries a warning.

Step 2 is not redundant. Artefact generation runs on a CLI path that never
loads the config cascade, so the YAML lookup always misses there - use the env
var when setting the release for a `generate-artefacts` run, or the YAML key
when the app is running normally. Both spell the same thing.

An unrecognised value at step 1 or 2 is treated as unset and falls through, so a
typo cannot pin the release to something nobody asked for, and cannot suppress a
correctly-spelled value in the layer below it.

If an explicit release CONTRADICTS a base image that names its own - say
`base_distro: noble` against `base_image: debian:trixie-slim` - the explicit
value still wins, because config beats a string. But the result is a Dockerfile
whose `FROM` and whose package names are for different releases, which is
almost never intended, so the generator stamps a second warning naming both.
Fix one of the two rather than shipping it.

Tag derivation strips any digest, ignores a registry port, and tests each
hyphen-separated component - so `debian:trixie-slim`, `rust:1-trixie` and
`ubuntu:24.04` all resolve. A codename wins wherever it appears; a bare version
number counts only on the `debian` and `ubuntu` images themselves, because
`postgres:13-bookworm` is postgres 13 on bookworm, not Debian 13. A
digest-pinned or rolling tag (`ghcr.io/org/base@sha256:...`, `:stable`), or a
version tag on some other image (`ghcr.io/org/base:13`), names no release and
reaches step 4. The default base, `debian:trixie-slim@sha256:...`, keeps its
tag beside the digest, so it resolves at step 3. Pinning a digest is what the
container standard asks for, so with a digest-only reference state the release
alongside it:

```yaml
deployment:
  base_image: ghcr.io/org/base@sha256:...
  base_distro: trixie      # trixie|bookworm|noble|jammy|focal
```

```bash
# Equivalent, and the form that works for `<app> generate-artefacts`.
DEPLOYMENT__BASE_DISTRO=trixie <app> generate-artefacts --output-dir ci-artefacts
```

What the release decides:

| Release | libgit2 |
| --------- | --------- |
| `trixie` (default) | `libgit2-1.9` |
| `bookworm` | `libgit2-1.5` |
| `noble` | `libgit2-1.7` |
| `jammy` | `libgit2-1.1` |
| `focal` | `libgit2-28` |

---

## Feature -> package map

| Feature(s) | Runtime packages |
| ------------ | ------------------- |
| `directory-config-git` | libgit2, from the release table above |
| `transport-kafka`, `dlq-kafka*`, `spool`, `tiered-sink`, `http`, `secrets*`, `otel*` | none: linked statically, or no C library |
| Pure-Rust features (`cli`, `logger`, `deployment`, `metrics`, ...) | none |

Only librdkafka brings OpenSSL: `cargo tree --all-features -i openssl-sys` finds no other path.

`NativeDepsContract::default()` is empty too. A contract that doesn't populate `native_deps` gives a production Dockerfile with `ca-certificates` alone.

---

## Custom APT repos

An app that needs a vendor's package adds an `AptRepoContract` to `apt_repos`. An empty `codename` takes the release resolved above, and a set one is used as written, for a vendor whose suites do not follow the distro's:

```rust
deps.apt_repos.push(AptRepoContract {
    key_url: "https://apt.vendor.example/archive.key".into(),
    keyring: "/usr/share/keyrings/vendor.gpg".into(),
    url: "https://apt.vendor.example/deb".into(),
    codename: String::new(),
    key_fingerprint: "0123456789ABCDEF0123456789ABCDEF01234567".into(),
    packages: vec!["vendor-runtime".into()],
});
```

The build asserts the key's OpenPGP fingerprint before it dearmors the key into the keyring, so a compromised mirror cannot substitute its own key. A rotated key fails the build: re-derive the fingerprint rather than dropping the check. Pinning makes trust-on-first-use a one-time event. It does not prove the key was legitimate to begin with.

The generated fragment, on trixie:

```dockerfile
# Runtime shared libraries for dynamically-linked Rust crates.
# Apt versions are unpinned because Debian drops superseded ones, so the digest-pinned base is what fixes the release.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg \
    && curl -fsSL https://apt.vendor.example/archive.key -o /tmp/repo-key.asc \
    && gpg --show-keys --with-colons --with-fingerprint /tmp/repo-key.asc \
       > /tmp/repo-key.info \
    && grep -q "^fpr:::::::::0123456789ABCDEF0123456789ABCDEF01234567:" /tmp/repo-key.info \
    && rm -f /tmp/repo-key.info \
    && gpg --dearmor -o /usr/share/keyrings/vendor.gpg /tmp/repo-key.asc \
    && rm -f /tmp/repo-key.asc \
    && echo "deb [signed-by=/usr/share/keyrings/vendor.gpg] \
       https://apt.vendor.example/deb trixie main" \
       > /etc/apt/sources.list.d/vendor.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       vendor-runtime libgit2-1.9 \
    && apt-get purge -y --auto-remove curl gnupg \
    && rm -rf /var/lib/apt/lists/*
```

`gnupg` is pulled in automatically whenever a custom repo is needed
(for the fingerprint check and `gpg --dearmor`).

Package versions are deliberately not pinned, and the `# hadolint ignore=DL3008`
pragma says so to the linter. Debian's archive drops a superseded version, so a
pinned one breaks the build within weeks. What makes the install reproducible is
the digest-pinned base image, which fixes the release.

When the release could not be derived, the same block is preceded by a
`# WARNING:` comment naming the base image and the release assumed, so a
wrong guess shows up in the artefact rather than as "Unable to locate
package" in a build log.

---

## Build host vs runtime host

| Where | Needs |
| ------- | ------- |
| **Release build** (CI runner doing `cargo build`) | the static prefix above, plus `libgit2-dev` for `directory-config-git` |
| **Local dev build** | `librdkafka-dev` and `libgit2-dev` |
| **Runtime host** (the container image) | the release-specific libgit2 from the table above for `directory-config-git` (on trixie: `libgit2-1.9`), nothing else |

`NativeDepsContract` describes the **runtime** side only -- what ships
in the image. hyperi-ci handles build-host packages separately by
sniffing `Cargo.lock` for `-sys` crates and installing matching `-dev`
packages on the runner.

### glibc: keep runtime >= build

scalo links glibc dynamically, so the rule is **glibc(runtime image)
>= glibc(build host)**. A binary built against a newer glibc fails at
startup on an older one (`version 'GLIBC_x.yz' not found`). The default
`base_image` is `debian:trixie-slim`, pinned by digest, and the CI builders run
debian trixie too, so build and runtime glibc are identical -- it just works.

That mapping is not taken on trust. `tier_a_dockerfile_with_native_deps_builds`
(`tests/e2e/contract_artefacts.rs`) generates a Dockerfile for the default base
with the kafka, spool, secrets and git features plus a real signed vendor repo,
BUILDS it, and then asserts inside the image that the repo's package and
`libgit2.so` are present. A package search would not prove this either way -
Debian's package pages do not surface `Provides:`, so a package that resolves
perfectly well can look absent. Only a build settles it.

If you OVERRIDE `deployment.base_image`, keep its glibc >= the build
host's (debian trixie):

| Runtime image | Safe on a debian-trixie builder? |
| --- | --- |
| `debian:trixie-slim@sha256:...` (default) | yes -- same release |
| a newer Debian release | yes -- newer glibc |
| an OLDER Debian, or Ubuntu | no -- older glibc; build on that base too |
| distroless `cc-debian*` | no -- see below |

Distroless is not a glibc problem: `cc-debian13` is trixie, so the glibc
matches. It has no apt, so the contract cannot install a package into it, and
the generator only builds `apt-get` images.

musl images (alpine) are **not supported**: the native deps above
(rdkafka, libgit2, openssl, `aws-lc-sys`) link glibc.

---

## Reading from `Cargo.toml`

`from_cargo_toml()` parses the `scalo` features array from the
app's `Cargo.toml` and runs the same mapping -- for tooling that won't
hard-code the feature list:

```rust
let deps = NativeDepsContract::from_cargo_toml(
    Path::new("Cargo.toml"),
    &base_image_from_cascade(),
);
```

Single-line and multi-line `features = [...]` forms are both
recognised. Returns empty (`is_empty() == true`) on parse failure or
when the dependency is absent -- no panic, no surprise build break.

---

## Validation

No `validate_native_deps()` -- the generator is the source of truth.
To catch drift between an app's features and its installed packages,
run `generate-artefacts` in CI and diff the produced `Dockerfile.runtime`
against the committed copy. See [artefacts.md](artefacts.md) for the
drift-detection pattern.

---

## API surface

| Item | Purpose |
| ------ | --------- |
| `NativeDepsContract` | The contract -- `apt_repos`, `apt_packages`, `distro`, `unresolved_base_image` |
| `NativeDepsContract::for_features(&[..], BaseDistro)` | Build for a stated release, nothing inferred |
| `NativeDepsContract::for_scalo_features(&[..], base)` | Build from feature names, resolving the release |
| `NativeDepsContract::from_cargo_toml(path, base)` | Parse features out of `Cargo.toml` |
| `NativeDepsContract::is_empty()` | True if no packages to install |
| `BaseDistro` | The distro release the package names target |
| `base_distro_from_cascade()` / `resolve_base_distro(base)` | Cascade reader / full resolution |
| `DEFAULT_BASE_DISTRO` | The assumed release when nothing answers (`trixie`) |
| `AptRepoContract` | One custom APT repo (`key_url`, `keyring`, `url`, `codename`, `key_fingerprint`, `packages`) |

---

## Related

- [contract.md](contract.md) -- `native_deps` field on the contract
- [artefacts.md](artefacts.md) -- the generated APT block in `Dockerfile.runtime`
- [../feature-flags.md](../feature-flags.md) -- which features pull which deps
- README -- full build-host vs runtime-host package tables
- Source: [../../src/deployment/native_deps.rs](../../src/deployment/native_deps.rs)
