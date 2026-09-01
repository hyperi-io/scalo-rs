// Project:   scalo
// File:      src/deployment/native_deps.rs
// Purpose:   Runtime native dependency declarations for container images
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Runtime native dependency contracts for Dockerfile generation.
//!
//! Maps scalo Cargo features to the system packages needed at runtime
//! in the container image. The Dockerfile generator uses this to emit APT repo
//! setup and `apt-get install` commands automatically.

use serde::{Deserialize, Serialize};

/// Runtime native dependencies for a container image.
///
/// Populated via [`NativeDepsContract::for_features`] (explicit distro) or
/// [`NativeDepsContract::for_scalo_features`] (distro resolved from the
/// cascade or the base image) -- pass the list of scalo features your app
/// enables, get back the runtime packages and any custom APT repos needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
// Adding a field must not break downstream literal construction -- consumers
// build this through the constructors below, never by struct literal.
#[non_exhaustive]
pub struct NativeDepsContract {
    /// Custom APT repositories to add before installing packages.
    #[serde(default)]
    pub apt_repos: Vec<AptRepoContract>,

    /// APT packages to install from default repos.
    #[serde(default)]
    pub apt_packages: Vec<String>,

    /// The distro release the package names above were resolved for.
    ///
    /// `None` on a contract deserialised from an older emitter. Package names
    /// are release-specific (soname-versioned packages do not alias), so this
    /// records WHICH release the names are valid for rather than leaving it
    /// implicit in the base-image string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distro: Option<BaseDistro>,

    /// Set when the distro could not be derived from the base image and
    /// [`DEFAULT_BASE_DISTRO`](crate::deployment::DEFAULT_BASE_DISTRO) was
    /// assumed instead.
    ///
    /// Digest-pinned images (`debian@sha256:...`) carry no codename, so this is
    /// the expected state whenever the pinning rule is followed without also
    /// setting `deployment.base_distro`. The Dockerfile generator stamps a
    /// warning into its output while this is set, so the assumption cannot ship
    /// silently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unresolved_base_image: Option<String>,

    /// Set when an explicit `deployment.base_distro` CONTRADICTED a base image
    /// that names its own release.
    ///
    /// The explicit value still wins -- config beats a string -- but the result
    /// is a Dockerfile whose `FROM` and whose package names are for different
    /// releases, which is almost always a mistake rather than an intention. It
    /// is the one disagreement the generator cannot resolve for you, so it says
    /// so in the artefact rather than quietly picking a side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contradicted_base_image: Option<String>,
}

/// A custom APT repository (e.g., Confluent for librdkafka).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AptRepoContract {
    /// GPG key URL for the repo.
    pub key_url: String,

    /// Local keyring file path (e.g., `/usr/share/keyrings/confluent-clients.gpg`).
    pub keyring: String,

    /// Repository base URL (e.g., `https://packages.confluent.io/clients/deb`).
    pub url: String,

    /// Distribution codename (e.g., `noble`, `bookworm`).
    /// If empty, derived from the base image at generation time.
    #[serde(default)]
    pub codename: String,

    /// Expected OpenPGP fingerprint of `key_url`, uppercase hex, no spaces.
    ///
    /// When set, the generated build asserts the downloaded key matches before
    /// trusting the repo. Without it the key is trust-on-first-use, re-fetched
    /// on every build: a compromised mirror or a CA-level intercept serves its
    /// own key, `signed-by` then validates the attacker's repo, and we install
    /// their `librdkafka1` into a binary the data plane dynamically links.
    ///
    /// Pinning locks in the key as observed. It does NOT establish that the key
    /// was legitimate in the first place -- it converts an every-build TOFU into
    /// a one-time one, which is the part worth having.
    #[serde(default)]
    pub key_fingerprint: String,

    /// APT packages to install from this specific repo.
    pub packages: Vec<String>,
}

/// Confluent APT repository for librdkafka.
fn confluent_repo(codename: &str) -> AptRepoContract {
    AptRepoContract {
        key_url: "https://packages.confluent.io/clients/deb/archive.key".into(),
        keyring: "/usr/share/keyrings/confluent-clients.gpg".into(),
        url: "https://packages.confluent.io/clients/deb".into(),
        codename: codename.into(),
        // Fingerprint of the key served at key_url, verified by build: the
        // generated image asserts it with gpg before the repo is trusted. If
        // Confluent rotates the key this fails loudly at build time, which is
        // the intended behaviour -- re-derive and update rather than drop it.
        key_fingerprint: "CBBB821E8FAF364F79835C438B1DA6120C2BF624".into(),
        packages: vec!["librdkafka1".into()],
    }
}

/// A distro release we can generate runtime package names for.
///
/// Runtime package names are release-specific: soname-versioned packages bake
/// the library version into the package name (`libgit2-1.9` vs `libgit2-1.7`)
/// and do not alias, so installing the wrong one fails the build outright.
///
/// This is an explicit contract value, NOT something inferred from the base
/// image on every use. [`from_base_image`](Self::from_base_image) is a
/// best-effort convenience for the recognisable cases only: a digest-pinned
/// reference (`debian@sha256:...`) carries no codename at all, and a rolling
/// alias (`debian:stable-slim`) means a different release every couple of
/// years. Both return `None` so the caller falls back to a stated default
/// instead of silently borrowing another distro's package names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
// Adding a release is not a breaking change -- a future Debian 14 must not
// break every downstream exhaustive match.
#[non_exhaustive]
pub enum BaseDistro {
    /// Debian 13 "trixie" -- the org default.
    #[default]
    Trixie,
    /// Debian 12 "bookworm".
    Bookworm,
    /// Ubuntu 24.04 "noble".
    Noble,
    /// Ubuntu 22.04 "jammy".
    Jammy,
    /// Ubuntu 20.04 "focal".
    Focal,
}

impl BaseDistro {
    /// The release codename, as APT and the config cascade spell it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trixie => "trixie",
            Self::Bookworm => "bookworm",
            Self::Noble => "noble",
            Self::Jammy => "jammy",
            Self::Focal => "focal",
        }
    }

    /// Parse a codename, an ID we accept in `deployment.base_distro`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "trixie" | "debian13" | "debian-13" => Some(Self::Trixie),
            "bookworm" | "debian12" | "debian-12" => Some(Self::Bookworm),
            "noble" | "ubuntu24.04" | "ubuntu-24.04" => Some(Self::Noble),
            "jammy" | "ubuntu22.04" | "ubuntu-22.04" => Some(Self::Jammy),
            "focal" | "ubuntu20.04" | "ubuntu-20.04" => Some(Self::Focal),
            _ => None,
        }
    }

    /// Best-effort derivation from a base-image reference.
    ///
    /// Returns `None` for anything not positively recognised -- a digest-only
    /// reference, a rolling alias (`stable`, `latest`), or an unknown release.
    /// The caller decides what to do about that; guessing here is what breaks
    /// digest-pinned builds.
    #[must_use]
    pub fn from_base_image(base_image: &str) -> Option<Self> {
        // Strip any digest, then split off the tag -- but only if what follows
        // the last ':' is really a tag and not a registry port
        // (`localhost:5000/x`).
        let without_digest = base_image
            .split_once('@')
            .map_or(base_image, |(reference, _)| reference);
        let (repository, tag) = without_digest.rsplit_once(':')?;
        if tag.contains('/') || tag.is_empty() {
            return None;
        }

        // A tag is commonly compound (`trixie-slim`, `1-trixie`), so test each
        // hyphen-separated component rather than the whole string. A codename
        // is unambiguous wherever it appears, so it wins outright.
        if let Some(distro) = tag.split('-').find_map(Self::from_codename) {
            return Some(distro);
        }

        // A bare version number only names a Debian/Ubuntu release on the
        // Debian/Ubuntu images themselves. `postgres:13-bookworm` is postgres
        // 13 on bookworm, NOT Debian 13 -- reading a version out of an
        // arbitrary repository is the same class of mistake as matching on the
        // whole image string, so we do not.
        let image_name = repository.rsplit('/').next().unwrap_or(repository);
        if image_name != "debian" && image_name != "ubuntu" {
            return None;
        }
        tag.split('-')
            .find_map(|part| Self::from_version(image_name, part))
    }

    /// Match a single tag component against a release codename.
    fn from_codename(part: &str) -> Option<Self> {
        match part {
            "trixie" => Some(Self::Trixie),
            "bookworm" => Some(Self::Bookworm),
            "noble" => Some(Self::Noble),
            "jammy" => Some(Self::Jammy),
            "focal" => Some(Self::Focal),
            _ => None,
        }
    }

    /// Match a single tag component against a release version, for the
    /// `debian` and `ubuntu` images only.
    fn from_version(image_name: &str, part: &str) -> Option<Self> {
        match image_name {
            // Debian publishes point-release tags (`13.2-slim`); the minor does
            // not change the release, so compare on the major alone.
            "debian" => match part.split('.').next().unwrap_or(part) {
                "13" => Some(Self::Trixie),
                "12" => Some(Self::Bookworm),
                _ => None,
            },
            // Ubuntu versions ARE `major.minor` -- do not truncate these.
            "ubuntu" => match part {
                "24.04" => Some(Self::Noble),
                "22.04" => Some(Self::Jammy),
                "20.04" => Some(Self::Focal),
                _ => None,
            },
            _ => None,
        }
    }

    /// The Confluent clients-repo APT suite to pull `librdkafka1` from.
    ///
    /// This is the suite used for `packages.confluent.io/clients/deb`, NOT
    /// necessarily the release's own codename. Confluent's clients repo has no
    /// `trixie` (debian 13) suite -- its newest debian suite is `bookworm`
    /// (debian 12), whose `librdkafka1` .deb installs cleanly on trixie (the
    /// libssl/libsasl2/zlib deps are satisfied by trixie's newer versions), so
    /// trixie maps to `bookworm`.
    #[must_use]
    pub const fn confluent_suite(self) -> &'static str {
        match self {
            Self::Trixie | Self::Bookworm => "bookworm",
            Self::Noble => "noble",
            Self::Jammy => "jammy",
            Self::Focal => "focal",
        }
    }

    /// The libgit2 runtime package (`directory-config-git`).
    ///
    /// The library version is baked into the package name and there is no
    /// virtual provider, so this has to track each release: trixie 1.9,
    /// bookworm 1.5, noble 1.7, jammy 1.1, focal 0.28.
    #[must_use]
    pub const fn libgit2_package(self) -> &'static str {
        match self {
            Self::Trixie => "libgit2-1.9",
            Self::Bookworm => "libgit2-1.5",
            Self::Noble => "libgit2-1.7",
            Self::Jammy => "libgit2-1.1",
            Self::Focal => "libgit2-28",
        }
    }

    /// The OpenSSL runtime package.
    ///
    /// Releases that went through the 64-bit `time_t` transition renamed
    /// `libssl3` to `libssl3t64`. The old name still resolves there via
    /// `Provides:`, but naming the real package is clearer and does not depend
    /// on that `Provides` staying in place. Focal predates OpenSSL 3.
    #[must_use]
    pub const fn libssl_package(self) -> &'static str {
        match self {
            Self::Trixie | Self::Noble => "libssl3t64",
            Self::Bookworm | Self::Jammy => "libssl3",
            Self::Focal => "libssl1.1",
        }
    }
}

impl std::fmt::Display for BaseDistro {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl NativeDepsContract {
    /// Build runtime native deps from a list of scalo feature names, for an
    /// explicitly stated distro release.
    ///
    /// Prefer this over [`for_scalo_features`](Self::for_scalo_features)
    /// when the app already knows its base release: nothing is inferred from a
    /// display string, so it cannot be wrong-footed by a digest pin.
    ///
    /// Pass the same feature strings you use in `Cargo.toml` (e.g.,
    /// `"transport-kafka"`, `"spool"`, `"secrets-aws"`).
    ///
    /// # Example
    ///
    /// ```rust
    /// use scalo::deployment::{BaseDistro, NativeDepsContract};
    ///
    /// let deps = NativeDepsContract::for_features(
    ///     &["transport-kafka", "spool", "tiered-sink", "secrets"],
    ///     BaseDistro::Trixie,
    /// );
    /// assert!(!deps.apt_packages.is_empty());
    /// assert!(!deps.apt_repos.is_empty());
    /// ```
    #[must_use]
    pub fn for_features(features: &[&str], distro: BaseDistro) -> Self {
        Self::packages_for(features, distro)
    }

    /// Build runtime native deps from a list of scalo feature names, resolving
    /// the distro release from the config cascade or the base image.
    ///
    /// The precedence itself lives in
    /// [`resolve_base_distro`](crate::deployment::resolve_base_distro) -- one
    /// home, so a future layer added there cannot disagree with this. When it
    /// answers `None` (typically a digest-pinned base image, which carries no
    /// codename) this falls back to
    /// [`DEFAULT_BASE_DISTRO`](crate::deployment::DEFAULT_BASE_DISTRO) and
    /// records the base image in
    /// [`unresolved_base_image`](Self::unresolved_base_image), so the generated
    /// Dockerfile can say the release was assumed.
    ///
    /// Separately, if config states a release and the base image names a
    /// DIFFERENT one, config still wins but the disagreement is recorded in
    /// [`contradicted_base_image`](Self::contradicted_base_image).
    ///
    /// # Example
    ///
    /// ```rust
    /// use scalo::deployment::NativeDepsContract;
    ///
    /// let deps = NativeDepsContract::for_scalo_features(
    ///     &["transport-kafka", "spool", "tiered-sink", "secrets"],
    ///     "debian:trixie-slim",
    /// );
    /// assert!(!deps.apt_packages.is_empty());
    /// assert!(!deps.apt_repos.is_empty());
    /// assert!(deps.unresolved_base_image.is_none());
    /// ```
    #[must_use]
    pub fn for_scalo_features(features: &[&str], base_image: &str) -> Self {
        if let Some(distro) = crate::deployment::registry::resolve_base_distro(base_image) {
            let mut deps = Self::for_features(features, distro);
            // Config won over an image that names a different release. It is
            // still config's call, but the two disagreeing is almost always a
            // mistake, so record it.
            if BaseDistro::from_base_image(base_image).is_some_and(|d| d != distro) {
                deps.contradicted_base_image = Some(base_image.to_string());
            }
            deps
        } else {
            // Nothing answered: assume the org default and say that we did.
            let mut deps =
                Self::for_features(features, crate::deployment::registry::DEFAULT_BASE_DISTRO);
            deps.unresolved_base_image = Some(base_image.to_string());
            deps
        }
    }

    /// The actual feature-to-package mapping, for one stated release.
    ///
    /// Both constructors land here. It knows nothing about where the release
    /// came from -- resolution and the "we assumed" / "these disagree" flags are
    /// the callers' business, which is what keeps this a pure mapping.
    fn packages_for(features: &[&str], distro: BaseDistro) -> Self {
        let mut apt_repos = Vec::new();
        let mut packages: Vec<String> = Vec::new();

        // At most a handful of packages, so a linear scan beats a HashSet and
        // its extra String per entry.
        let mut add = |pkg: &str| {
            if !packages.iter().any(|p| p == pkg) {
                packages.push(pkg.into());
            }
        };

        let needs_kafka = features
            .iter()
            .any(|f| *f == "transport-kafka" || f.starts_with("dlq-kafka"));

        if needs_kafka {
            // Always source librdkafka1 from the Confluent clients repo so the
            // image ships the LATEST librdkafka -- the debian/ubuntu distro
            // packages lag the protocol, which is why we do NOT use them. The
            // binary dynamic-links librdkafka, so a container gets the current
            // Confluent build supplied here, while a manual run on a host uses
            // that host's librdkafka (older is fine). Trixie has no Confluent
            // suite, so it maps to bookworm (see BaseDistro::confluent_suite).
            apt_repos.push(confluent_repo(distro.confluent_suite()));
            add(distro.libssl_package());
            add("zlib1g");
        }

        let needs_zstd = features
            .iter()
            .any(|f| *f == "spool" || *f == "tiered-sink");
        if needs_zstd {
            add("libzstd1");
        }

        // openssl is a transitive dep for many features (http, secrets, transport)
        let needs_ssl = features.iter().any(|f| {
            *f == "http"
                || f.starts_with("secrets")
                || f.starts_with("transport")
                || f.starts_with("otel")
        });
        if needs_ssl {
            add(distro.libssl_package());
            add("zlib1g");
        }

        // directory-config-git needs libgit2 (soname pkg differs per release)
        let needs_git2 = features.contains(&"directory-config-git");
        if needs_git2 {
            add(distro.libgit2_package());
        }

        Self {
            apt_repos,
            apt_packages: packages,
            distro: Some(distro),
            unresolved_base_image: None,
            contradicted_base_image: None,
        }
    }

    /// Auto-detect native deps from the app's Cargo.toml.
    ///
    /// Reads `[dependencies.scalo]` features from the given Cargo.toml
    /// and maps them to runtime packages. Falls back to empty deps if parsing fails.
    #[must_use]
    pub fn from_cargo_toml(cargo_toml_path: &std::path::Path, base_image: &str) -> Self {
        let Ok(content) = std::fs::read_to_string(cargo_toml_path) else {
            return Self::default();
        };

        // Parse features from the scalo dependency line
        // Matches: features = ["transport-kafka", "spool", ...]
        let features = extract_scalo_features(&content);
        if features.is_empty() {
            return Self::default();
        }

        let feature_refs: Vec<&str> = features.iter().map(String::as_str).collect();
        Self::for_scalo_features(&feature_refs, base_image)
    }

    /// Returns true if there are no native deps to install.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.apt_repos.is_empty() && self.apt_packages.is_empty()
    }
}

/// Extract scalo feature names from Cargo.toml content.
///
/// Parses the `features = [...]` array from the `scalo` dependency.
/// Returns empty vec if not found or parsing fails.
fn extract_scalo_features(content: &str) -> Vec<String> {
    // Find the scalo dependency line
    let mut in_scalo = false;
    let mut features = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Single-line: scalo = { version = "...", features = [...] }
        if trimmed.starts_with("scalo")
            && trimmed.contains("features")
            && let Some(start) = trimmed.find("features = [")
        {
            let after = &trimmed[start + 12..];
            if let Some(end) = after.find(']') {
                let feature_str = &after[..end];
                for feat in feature_str.split(',') {
                    let f = feat.trim().trim_matches('"').trim();
                    if !f.is_empty() {
                        features.push(f.to_string());
                    }
                }
                return features;
            }
        }

        // Multi-line: features = [\n"transport-kafka",\n...\n]
        if trimmed.starts_with("scalo") {
            in_scalo = true;
            continue;
        }
        if in_scalo {
            if trimmed.starts_with(']') {
                return features;
            }
            if trimmed.starts_with('"') {
                let f = trimmed.trim_matches('"').trim_end_matches(',').trim();
                if !f.is_empty() {
                    features.push(f.to_string());
                }
            }
            // End of dependency block
            if trimmed.starts_with('[') && !trimmed.starts_with("[dependencies") {
                return features;
            }
        }
    }

    features
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_features_add_confluent_repo() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(&["transport-kafka"], "ubuntu:24.04");
        assert_eq!(deps.apt_repos.len(), 1);
        assert!(deps.apt_repos[0].url.contains("confluent"));
        assert!(deps.apt_repos[0].packages.contains(&"librdkafka1".into()));
        assert_eq!(deps.apt_repos[0].codename, "noble");
        assert!(deps.apt_packages.contains(&"libssl3t64".into()));
        assert!(deps.apt_packages.contains(&"zlib1g".into()));
    }

    #[test]
    fn test_spool_adds_zstd() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(&["spool"], "ubuntu:24.04");
        assert!(deps.apt_packages.contains(&"libzstd1".into()));
    }

    #[test]
    fn test_tiered_sink_adds_zstd() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(&["tiered-sink"], "ubuntu:24.04");
        assert!(deps.apt_packages.contains(&"libzstd1".into()));
    }

    #[test]
    fn test_no_features_empty() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(&[], "ubuntu:24.04");
        assert!(deps.is_empty());
    }

    #[test]
    fn test_pure_rust_features_empty() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(
            &["cli", "deployment", "logger"],
            "ubuntu:24.04",
        );
        assert!(deps.is_empty());
    }

    #[test]
    fn test_bookworm_codename() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps =
            NativeDepsContract::for_scalo_features(&["transport-kafka"], "debian:bookworm-slim");
        assert_eq!(deps.apt_repos[0].codename, "bookworm");
    }

    #[test]
    fn test_trixie_kafka_uses_confluent_bookworm() {
        let _env = crate::deployment::env_test_lock::guard();
        // Trixie has no Confluent suite, so it pulls the LATEST librdkafka1 from
        // the Confluent clients repo via the bookworm suite (debian's native
        // package lags the protocol). librdkafka1 comes from the repo, NOT
        // apt_packages.
        let deps =
            NativeDepsContract::for_scalo_features(&["transport-kafka"], "debian:trixie-slim");
        assert_eq!(deps.apt_repos.len(), 1);
        assert!(deps.apt_repos[0].url.contains("confluent"));
        assert_eq!(deps.apt_repos[0].codename, "bookworm");
        assert!(deps.apt_repos[0].packages.contains(&"librdkafka1".into()));
        assert!(!deps.apt_packages.contains(&"librdkafka1".into()));
        // Trixie went through the 64-bit time_t transition -- libssl3t64.
        assert!(deps.apt_packages.contains(&"libssl3t64".into()));
        assert!(deps.apt_packages.contains(&"zlib1g".into()));
    }

    #[test]
    fn test_trixie_git2_soname() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps =
            NativeDepsContract::for_scalo_features(&["directory-config-git"], "debian:trixie-slim");
        assert!(deps.apt_packages.contains(&"libgit2-1.9".into()));
    }

    #[test]
    fn test_no_duplicate_packages() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(
            &["transport-kafka", "http", "secrets"],
            "ubuntu:24.04",
        );
        let ssl_count = deps
            .apt_packages
            .iter()
            .filter(|p| *p == "libssl3t64")
            .count();
        assert_eq!(ssl_count, 1);
    }

    #[test]
    fn test_dlq_kafka_adds_confluent() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(&["dlq-kafka"], "ubuntu:24.04");
        assert_eq!(deps.apt_repos.len(), 1);
    }

    #[test]
    fn test_git2_feature() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps =
            NativeDepsContract::for_scalo_features(&["directory-config-git"], "ubuntu:24.04");
        assert!(deps.apt_packages.contains(&"libgit2-1.7".into()));
    }

    #[test]
    fn test_full_receiver_features() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(
            &[
                "config",
                "config-reload",
                "logger",
                "metrics",
                "http-server",
                "transport-kafka",
                "transport-grpc",
                "dlq-kafka",
                "spool",
                "tiered-sink",
                "runtime",
                "secrets",
                "scaling",
                "cli",
                "deployment",
            ],
            "ubuntu:24.04",
        );
        assert_eq!(deps.apt_repos.len(), 1); // confluent only
        assert!(!deps.apt_packages.contains(&"librdkafka1".to_string())); // in repo packages
        assert!(deps.apt_repos[0].packages.contains(&"librdkafka1".into()));
        assert!(deps.apt_packages.contains(&"libssl3t64".into()));
        assert!(deps.apt_packages.contains(&"libzstd1".into()));
        assert!(deps.apt_packages.contains(&"zlib1g".into()));
    }

    // ------------------------------------------------------------------
    // BaseDistro derivation
    //
    // The substring match this replaced only ever recognised a base image
    // spelled with the word "trixie", and quietly served Ubuntu package names
    // to everything else. Every reference below is a legitimate way to name a
    // Debian 13 image.
    // ------------------------------------------------------------------

    #[test]
    fn distro_from_codename_tags() {
        assert_eq!(
            BaseDistro::from_base_image("debian:trixie-slim"),
            Some(BaseDistro::Trixie)
        );
        assert_eq!(
            BaseDistro::from_base_image("debian:bookworm-slim"),
            Some(BaseDistro::Bookworm)
        );
        assert_eq!(
            BaseDistro::from_base_image("ubuntu:noble"),
            Some(BaseDistro::Noble)
        );
        assert_eq!(
            BaseDistro::from_base_image("rust:1-trixie"),
            Some(BaseDistro::Trixie)
        );
    }

    #[test]
    fn distro_from_version_tags() {
        // The tag most likely to be typed, and the one the old substring match
        // silently sent to Ubuntu.
        assert_eq!(
            BaseDistro::from_base_image("debian:13-slim"),
            Some(BaseDistro::Trixie)
        );
        assert_eq!(
            BaseDistro::from_base_image("debian:12"),
            Some(BaseDistro::Bookworm)
        );
        assert_eq!(
            BaseDistro::from_base_image("ubuntu:24.04"),
            Some(BaseDistro::Noble)
        );
        assert_eq!(
            BaseDistro::from_base_image("ubuntu:22.04"),
            Some(BaseDistro::Jammy)
        );
    }

    #[test]
    fn distro_version_tags_only_count_on_the_debian_and_ubuntu_images() {
        // A version in an arbitrary repository is that software's version, not
        // a Debian release. Reading it as one is the same class of mistake as
        // matching on the whole image string.
        assert_eq!(
            BaseDistro::from_base_image("postgres:13-bookworm"),
            Some(BaseDistro::Bookworm),
            "the codename must win over the leading version"
        );
        assert_eq!(
            BaseDistro::from_base_image("postgres:12-trixie"),
            Some(BaseDistro::Trixie)
        );
        // A curated org base tagged with a bare number tells us nothing -- and
        // registry.rs anticipates exactly such an image.
        assert_eq!(
            BaseDistro::from_base_image("ghcr.io/hyperi-io/dfe-base:13"),
            None
        );
        assert_eq!(BaseDistro::from_base_image("postgres:13"), None);
    }

    #[test]
    fn distro_from_debian_point_release_tags() {
        // Published Docker Hub tags. The minor does not change the release.
        assert_eq!(
            BaseDistro::from_base_image("debian:13.2-slim"),
            Some(BaseDistro::Trixie)
        );
        assert_eq!(
            BaseDistro::from_base_image("debian:12.11-slim"),
            Some(BaseDistro::Bookworm)
        );
        // Ubuntu versions ARE major.minor -- truncating would break them.
        assert_eq!(
            BaseDistro::from_base_image("ubuntu:24.04-slim"),
            Some(BaseDistro::Noble)
        );
        assert_eq!(BaseDistro::from_base_image("ubuntu:24"), None);
    }

    #[test]
    fn distro_unresolvable_references_return_none() {
        // Digest-only: the codename is gone, which is what the container
        // standard's pinning rule produces.
        assert_eq!(BaseDistro::from_base_image("debian@sha256:abc123"), None);
        // Rolling aliases mean a different release every couple of years.
        assert_eq!(BaseDistro::from_base_image("debian:stable-slim"), None);
        assert_eq!(BaseDistro::from_base_image("debian:latest"), None);
        // No tag at all.
        assert_eq!(BaseDistro::from_base_image("debian"), None);
        // A registry port is not a tag.
        assert_eq!(BaseDistro::from_base_image("localhost:5000/debian"), None);
    }

    #[test]
    fn distro_survives_a_pinned_digest_beside_a_tag() {
        assert_eq!(
            BaseDistro::from_base_image("debian:trixie-slim@sha256:abc123"),
            Some(BaseDistro::Trixie)
        );
    }

    #[test]
    fn digest_pinned_base_records_the_assumption() {
        let _env = crate::deployment::env_test_lock::guard();
        // No cascade config in a unit test, so this exercises the fallback:
        // default distro assumed, base image recorded so the generator can say
        // so in the artefact.
        let deps = NativeDepsContract::for_scalo_features(
            &["transport-kafka", "directory-config-git"],
            "debian@sha256:abc123",
        );
        assert_eq!(
            deps.unresolved_base_image.as_deref(),
            Some("debian@sha256:abc123")
        );
        assert_eq!(deps.distro, Some(crate::deployment::DEFAULT_BASE_DISTRO));
        // Trixie names, not the Ubuntu ones the old fallback would have served.
        assert!(deps.apt_packages.contains(&"libgit2-1.9".into()));
        assert!(deps.apt_packages.contains(&"libssl3t64".into()));
    }

    #[test]
    fn explicit_distro_contradicting_the_base_image_is_recorded() {
        let _env = crate::deployment::env_test_lock::guard();
        // Config wins, but a Dockerfile whose FROM and whose package names are
        // for different releases is almost never intended -- so say so rather
        // than quietly picking a side. This is the one case that used to have
        // NO signal at all.
        temp_env::with_var("DEPLOYMENT__BASE_DISTRO", Some("noble"), || {
            let deps =
                NativeDepsContract::for_scalo_features(&["transport-kafka"], "debian:trixie-slim");
            assert_eq!(deps.distro, Some(BaseDistro::Noble), "explicit config wins");
            assert_eq!(
                deps.contradicted_base_image.as_deref(),
                Some("debian:trixie-slim")
            );
            // Not the same thing as an unresolvable base.
            assert!(deps.unresolved_base_image.is_none());
        });
    }

    #[test]
    fn explicit_distro_agreeing_with_the_base_image_is_not_flagged() {
        let _env = crate::deployment::env_test_lock::guard();
        temp_env::with_var("DEPLOYMENT__BASE_DISTRO", Some("trixie"), || {
            let deps =
                NativeDepsContract::for_scalo_features(&["transport-kafka"], "debian:trixie-slim");
            assert_eq!(deps.distro, Some(BaseDistro::Trixie));
            assert!(deps.contradicted_base_image.is_none());
        });
    }

    #[test]
    fn recognised_base_records_no_assumption() {
        let _env = crate::deployment::env_test_lock::guard();
        let deps = NativeDepsContract::for_scalo_features(&["transport-kafka"], "debian:13-slim");
        assert!(deps.unresolved_base_image.is_none());
        assert_eq!(deps.distro, Some(BaseDistro::Trixie));
        assert_eq!(deps.apt_repos[0].codename, "bookworm");
    }

    #[test]
    fn for_features_takes_the_distro_verbatim() {
        // No string anywhere in this path.
        let deps = NativeDepsContract::for_features(
            &["transport-kafka", "directory-config-git"],
            BaseDistro::Bookworm,
        );
        assert_eq!(deps.distro, Some(BaseDistro::Bookworm));
        assert!(deps.unresolved_base_image.is_none());
        assert_eq!(deps.apt_repos[0].codename, "bookworm");
        assert!(deps.apt_packages.contains(&"libgit2-1.5".into()));
        assert!(deps.apt_packages.contains(&"libssl3".into()));
    }

    #[test]
    fn distro_parse_accepts_cascade_spellings() {
        assert_eq!(BaseDistro::parse("trixie"), Some(BaseDistro::Trixie));
        assert_eq!(BaseDistro::parse("  Trixie "), Some(BaseDistro::Trixie));
        assert_eq!(BaseDistro::parse("ubuntu24.04"), Some(BaseDistro::Noble));
        assert_eq!(BaseDistro::parse("plucky"), None);
    }

    #[test]
    fn distro_package_names_are_release_specific() {
        // Soname-versioned packages do not alias, so a wrong release is a hard
        // build failure rather than a silent downgrade.
        assert_eq!(BaseDistro::Trixie.libgit2_package(), "libgit2-1.9");
        assert_eq!(BaseDistro::Bookworm.libgit2_package(), "libgit2-1.5");
        assert_eq!(BaseDistro::Noble.libgit2_package(), "libgit2-1.7");
        assert_eq!(BaseDistro::Jammy.libgit2_package(), "libgit2-1.1");
        assert_eq!(BaseDistro::Focal.libgit2_package(), "libgit2-28");
        // Only the time_t-transitioned releases carry the t64 suffix.
        assert_eq!(BaseDistro::Trixie.libssl_package(), "libssl3t64");
        assert_eq!(BaseDistro::Noble.libssl_package(), "libssl3t64");
        assert_eq!(BaseDistro::Bookworm.libssl_package(), "libssl3");
        assert_eq!(BaseDistro::Focal.libssl_package(), "libssl1.1");
    }

    #[test]
    fn confluent_has_no_trixie_suite() {
        assert_eq!(BaseDistro::Trixie.confluent_suite(), "bookworm");
        assert_eq!(BaseDistro::Bookworm.confluent_suite(), "bookworm");
        assert_eq!(BaseDistro::Noble.confluent_suite(), "noble");
    }
}
