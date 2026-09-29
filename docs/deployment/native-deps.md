# Native Deps

scalo dynamically links against system C libraries -- rdkafka,
libgit2, zstd, openssl, zlib -- instead of static compilation. Saves
~30 minutes of C++ build per CI run. Cost: the runtime container needs
the `.so` files present.

`NativeDepsContract` is the bookkeeping. The Dockerfile generator reads
it and emits the right `apt-get` block -- no per-app hand-coding, no "I
forgot to add `libssl3`" outages.

---

## Auto-detection from features

The usual path is `for_scalo_features()`. Pass the same feature flags
the app enables on scalo, get back the runtime packages and
any custom APT repos:

```rust
use scalo::deployment::NativeDepsContract;

let deps = NativeDepsContract::for_scalo_features(
    &["transport-kafka", "spool", "tiered-sink", "secrets"],
    "debian:trixie-slim",
);
// deps.apt_repos    = [Confluent repo (librdkafka1, suite=bookworm)]
// deps.apt_packages = ["libssl3t64", "zlib1g", "libzstd1"]
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
  base_image: ghcr.io/example-org/app-base@sha256:...
  base_distro: trixie      # trixie|bookworm|noble|jammy|focal
```

```bash
# Equivalent, and the form that works for `<app> generate-artefacts`.
DEPLOYMENT__BASE_DISTRO=trixie <app> generate-artefacts --output-dir ci-artefacts
```

What the release decides:

| Release | Confluent suite | libgit2 | libssl |
|---------|-----------------|---------|--------|
| `trixie` (default) | `bookworm` | `libgit2-1.9` | `libssl3t64` |
| `bookworm` | `bookworm` | `libgit2-1.5` | `libssl3` |
| `noble` | `noble` | `libgit2-1.7` | `libssl3t64` |
| `jammy` | `jammy` | `libgit2-1.1` | `libssl3` |
| `focal` | `focal` | `libgit2-28` | `libssl1.1` |

The Confluent column is the clients-repo APT SUITE, not the release's own
codename: Confluent publishes no trixie suite, so trixie takes `bookworm`.
The libssl split is the 64-bit `time_t` transition, which renamed `libssl3`
to `libssl3t64` on trixie and noble.

---

## Feature -> package map

| Feature(s) | APT repo | Runtime packages |
|------------|----------|-------------------|
| `transport-kafka`, `dlq-kafka` (or any `dlq-kafka-*`) | Confluent (`packages.confluent.io/clients/deb`) | `librdkafka1`, libssl, `zlib1g` |
| `spool`, `tiered-sink` | -- | `libzstd1` |
| `http`, `secrets*`, `transport*`, `otel*` | -- | libssl, `zlib1g` |
| `directory-config-git` | -- | libgit2 |
| Pure-Rust features (`cli`, `logger`, `deployment`, `metrics`, ...) | -- | none |

The libssl and libgit2 package names come from the release table above.
Deduplication is automatic: enabling both `transport-kafka` and `http`
adds libssl once.

---

## Confluent repo auto-add

`librdkafka` in the Debian/Ubuntu repos lags the protocol, so we never
use the distro package. The Confluent clients repo carries the current
build and is added UNCONDITIONALLY whenever a Kafka feature is present -
there is no distro for which we fall back to the native package. Generated
Dockerfile fragment (suite `bookworm`, from a trixie base):

```dockerfile
# Runtime shared libraries for dynamically-linked Rust crates.
# Apt versions are unpinned because Debian drops superseded ones, so the digest-pinned base is what fixes the release.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd iputils-ping gnupg \
    && curl -fsSL https://packages.confluent.io/clients/deb/archive.key -o /tmp/repo-key.asc \
    && gpg --show-keys --with-colons --with-fingerprint /tmp/repo-key.asc \
       > /tmp/repo-key.info \
    && grep -q "^fpr:::::::::CBBB821E8FAF364F79835C438B1DA6120C2BF624:" /tmp/repo-key.info \
    && rm -f /tmp/repo-key.info \
    && gpg --dearmor -o /usr/share/keyrings/confluent-clients.gpg /tmp/repo-key.asc \
    && rm -f /tmp/repo-key.asc \
    && echo "deb [signed-by=/usr/share/keyrings/confluent-clients.gpg] \
       https://packages.confluent.io/clients/deb bookworm main" \
       > /etc/apt/sources.list.d/confluent-clients.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       librdkafka1 libssl3t64 zlib1g \
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
|-------|-------|
| **Build host** (CI runner doing `cargo build`) | `-dev` packages: `librdkafka-dev`, `libgit2-dev`, `libzstd-dev`, `libssl-dev`, `zlib1g-dev` |
| **Runtime host** (the container image) | `.so` runtimes: `librdkafka1`, `libzstd1`, `zlib1g`, plus the release-specific libgit2 / libssl from the table above (on trixie: `libgit2-1.9`, `libssl3t64`) |

`NativeDepsContract` describes the **runtime** side only -- what ships
in the image. A CI wrapper handles build-host packages separately by
sniffing `Cargo.lock` for `-sys` crates and installing matching `-dev`
packages on the runner.

### glibc: keep runtime >= build

scalo links glibc dynamically, so the rule is **glibc(runtime image)
>= glibc(build host)**. A binary built against a newer glibc fails at
startup on an older one (`version 'GLIBC_x.yz' not found`). The default
`base_image` is `debian:trixie-slim`, pinned by digest, and the CI builders run
debian trixie too, so build and runtime glibc are identical -- it just works.
`librdkafka1` always comes from the Confluent clients repo, on trixie as
everywhere else. Confluent publishes no trixie suite, so trixie maps to the
`bookworm` one, and its `librdkafka1` installs cleanly on trixie because the
libssl / libsasl2 / zlib deps are satisfied by trixie's newer versions.

The repo's signing key is PINNED. The generated build downloads the key, asserts
its OpenPGP fingerprint with `gpg` and only then dearmors it into the keyring,
so a compromised mirror or an intercepted TLS session cannot substitute its own
key and have `signed-by` happily validate the resulting repo. If Confluent
rotates the key the build fails loudly - re-derive the fingerprint and update
`confluent_repo()` rather than removing the check.

Pinning converts an every-build trust-on-first-use into a one-time one. It does
not establish that the key was legitimate to begin with.

That mapping is not taken on trust. `tier_a_dockerfile_with_native_deps_builds`
(`tests/e2e/contract_artefacts.rs`) generates a Dockerfile for the default base
with the kafka, spool, secrets and git features, BUILDS it, and then asserts
inside the image that `librdkafka.so`, `libssl.so`, `libgit2.so` and
`libzstd.so` are present. A package search would not prove this either way -
Debian's package pages do not surface `Provides:`, so a package that resolves
perfectly well can look absent. Only a build settles it.

If you OVERRIDE `deployment.base_image`, keep its glibc >= the build
host's (debian trixie):

| Runtime image | Safe on a debian-trixie builder? |
|---|---|
| `debian:trixie-slim@sha256:...` (default) | yes -- same release |
| a newer Debian release | yes -- newer glibc |
| an OLDER Debian, or Ubuntu | no -- older glibc; build on that base too |
| distroless `cc-debian*` | no -- see below |

Distroless is not a glibc problem: `cc-debian13` is trixie, so the glibc
matches. It fails on the dependency closure. Our binaries need shared
objects `cc-debian13` does not carry, and it has no apt to add them - on the
build recorded in scalo-rs#7 the Confluent `librdkafka1` alone pulled
`libcurl3t64-gnutls`, `libngtcp2-16` and `libngtcp2-crypto-gnutls8`. You
would have to copy each `.so` in by hand and keep that list current as
librdkafka's deps move, which is exactly the outage the contract exists to
prevent.

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

## Empty by default

`NativeDepsContract::default()` is empty. A contract that doesn't
populate `native_deps` gives a Dockerfile with only base packages
(`ca-certificates`, `curl`, `netcat-openbsd`, `iputils-ping`). Apps
opt in by populating the field, usually via `for_scalo_features()`.

A pure-Rust-feature service shouldn't carry unused system libraries.
Opt-in keeps the image lean.

---

## Codename override

For your own APT repo, set `AptRepoContract::codename` directly. Leave it
empty and it is derived from the base image at generation time; set it and
the generator uses your value as-is. This is the per-repo lever. For the
auto-added Confluent repo, the operator-facing lever is
`deployment.base_distro` in the cascade (above) - the derived suite is
already filled in on that entry.

```rust
let repo = AptRepoContract {
    key_url: "https://example.com/key.asc".into(),
    keyring: "/usr/share/keyrings/example.gpg".into(),
    url: "https://example.com/apt".into(),
    codename: "trixie".into(),       // explicit, no derivation
    packages: vec!["libexample0".into()],
};
```

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
|------|---------|
| `NativeDepsContract` | The contract -- `apt_repos`, `apt_packages`, `distro`, `unresolved_base_image` |
| `NativeDepsContract::for_features(&[..], BaseDistro)` | Build for a stated release, nothing inferred |
| `NativeDepsContract::for_scalo_features(&[..], base)` | Build from feature names, resolving the release |
| `NativeDepsContract::from_cargo_toml(path, base)` | Parse features out of `Cargo.toml` |
| `NativeDepsContract::is_empty()` | True if no packages to install |
| `BaseDistro` | The distro release the package names target |
| `base_distro_from_cascade()` / `resolve_base_distro(base)` | Cascade reader / full resolution |
| `DEFAULT_BASE_DISTRO` | The assumed release when nothing answers (`trixie`) |
| `AptRepoContract` | One custom APT repo (`key_url`, `keyring`, `url`, `codename`, `packages`) |

---

## Related

- [contract.md](contract.md) -- `native_deps` field on the contract
- [artefacts.md](artefacts.md) -- the generated APT block in `Dockerfile.runtime`
- [../feature-flags.md](../feature-flags.md) -- which features pull which deps
- README -- full build-host vs runtime-host package tables
- Source: [../../src/deployment/native_deps.rs](../../src/deployment/native_deps.rs)
