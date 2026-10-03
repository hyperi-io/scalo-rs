// Project:   scalo
// File:      src/deployment/registry.rs
// Purpose:   Config-cascade-driven container registry resolution
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Container registry resolution for deployment contracts.
//!
//! [`BaseDistro`] lives in [`native_deps`](super::native_deps); this module
//! only resolves WHICH one applies.
//!
//! The publish-target registry (where the built image is pushed) and the
//! base image (the `FROM` line) are org-wide decisions, not per-app. This
//! module reads them from the config cascade so they live in YAML config
//! rather than being hardcoded in each app's contract source.
//!
//! The same applies to the distro release those artefacts install packages
//! for -- see [`base_distro_from_cascade`].
//!
//! # Cascade keys
//!
//! ```yaml
//! deployment:
//!   image_registry: registry.example.com/team          # no default -- required
//!   base_image: debian:trixie-slim@sha256:<digest>     # default: DEFAULT_BASE_IMAGE
//!   base_distro: trixie                                # default: derived from base_image
//!   argocd:
//!     repo_url: https://git.example.com/team/my-app    # no default -- no Application without it
//!     dest_namespace: my-namespace                     # default: the app name
//! ```
//!
//! # Defaults
//!
//! - `image_registry` has none: an organisation's registry is not something a
//!   library can guess, and [`DeploymentContract::validate`](super::DeploymentContract::validate)
//!   refuses a contract that names none
//! - [`DEFAULT_BASE_IMAGE`] = `debian:trixie-slim@sha256:...` -- what the runtime stage builds on
//! - [`DEFAULT_BASE_DISTRO`] = `trixie` -- which release's package names to emit
//!
//! `base_distro` exists because runtime package names are release-specific and
//! a base image does not always say which release it is. A digest-only
//! reference (`debian@sha256:...`) carries no codename at all. Keep the tag
//! beside the digest (`debian:trixie-slim@sha256:...`) and the release still
//! derives, or set `base_distro` alongside the pin and the generator has no
//! guessing to do.
//!
//! When (eventually) a curated GHCR base image lands at
//! `ghcr.io/org/base:trixie`, ops can override
//! `deployment.base_image` in the cascade without rebuilding the apps.

use super::native_deps::BaseDistro;

/// Default base image for the runtime stage.
///
/// Pulled from Docker Hub (no registry prefix). Debian "trixie" slim: smaller
/// and fewer CVEs than a full ubuntu base, and its glibc matches the debian
/// trixie CI builders so binaries built on CI run as-is. Override via
/// `deployment.base_image` in the YAML cascade (keep glibc(runtime) >=
/// glibc(build); musl/alpine unsupported -- see docs/deployment/native-deps.md).
///
/// Pinned to the multi-arch index digest so every build of one scalo release
/// gets the same bytes on amd64 and arm64. Renovate refreshes the digest.
// renovate: datasource=docker depName=debian
pub const DEFAULT_BASE_IMAGE: &str =
    "debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a";

/// Default distro release the generated runtime package names target.
///
/// Kept in step with [`DEFAULT_BASE_IMAGE`]: change one, change the other.
pub const DEFAULT_BASE_DISTRO: BaseDistro = BaseDistro::Trixie;

/// Read the publish-target image registry from the config cascade.
///
/// Reads `deployment.image_registry` from the YAML cascade. `None` when it is
/// not set or empty, when config isn't loaded, or when the `config` feature is
/// disabled -- there is no default registry, so the caller names its own.
///
/// # Example
///
/// ```rust,no_run
/// use scalo::deployment::{DeploymentContract, image_registry_from_cascade};
/// # fn dummy() -> DeploymentContract { unimplemented!() }
/// let mut contract = dummy();
/// contract.image_registry =
///     image_registry_from_cascade().unwrap_or_else(|| "registry.example.com/team".into());
/// ```
#[must_use]
pub fn image_registry_from_cascade() -> Option<String> {
    cascade_string("deployment.image_registry")
}

/// The non-empty string the cascade holds at `key`, if config is loaded.
fn cascade_string(key: &str) -> Option<String> {
    #[cfg(feature = "config")]
    {
        crate::config::try_get()
            .and_then(|cfg| cfg.get_string(key))
            .filter(|s| !s.is_empty())
    }
    #[cfg(not(feature = "config"))]
    {
        let _ = key;
        None
    }
}

/// Read the runtime base image from the config cascade.
///
/// Reads `deployment.base_image` from the YAML cascade. Falls back to
/// [`DEFAULT_BASE_IMAGE`] when not set.
#[must_use]
pub fn base_image_from_cascade() -> String {
    cascade_string("deployment.base_image").unwrap_or_else(|| DEFAULT_BASE_IMAGE.to_string())
}

/// Read the explicit distro release from the config cascade.
///
/// Reads `deployment.base_distro` from the YAML cascade, then falls back to
/// that key's own ENV-layer spelling, `DEPLOYMENT__BASE_DISTRO`.
///
/// The env fallback is not redundant. A Dockerfile generated where the app's
/// config did not load, or by a caller that never loads it, finds no cascade
/// and the YAML lookup misses -- which would make the remedy named in the
/// generated Dockerfile's warning ("set `deployment.base_distro`") inert there.
/// Reading the ENV layer's key directly gives the same answer whether or not
/// config happens to be loaded.
///
/// Returns `None` when unset or unrecognised. An unrecognised value is treated
/// as unset rather than silently substituted, so [`resolve_base_distro`] can
/// still fall through to the env layer and then to the base image -- a typo
/// must not be able to pin the release to something nobody asked for, nor to
/// suppress a correctly-spelled value in the layer below it.
#[must_use]
pub fn base_distro_from_cascade() -> Option<BaseDistro> {
    #[cfg(feature = "config")]
    {
        if let Some(cfg) = crate::config::try_get()
            && let Some(s) = cfg.get_string("deployment.base_distro")
            && !s.is_empty()
            && let Some(distro) = BaseDistro::parse(&s)
        {
            return Some(distro);
        }
    }
    std::env::var("DEPLOYMENT__BASE_DISTRO")
        .ok()
        .filter(|s| !s.is_empty())
        .as_deref()
        .and_then(BaseDistro::parse)
}

/// Resolve which distro release the runtime package names should target.
///
/// Explicit config wins, then the base image where it is positively
/// recognisable. `None` means neither answered -- typically a digest-pinned
/// base image with no `deployment.base_distro` set. Callers fall back to
/// [`DEFAULT_BASE_DISTRO`] and record that they did; nothing here guesses at a
/// different distro's package names.
#[must_use]
pub fn resolve_base_distro(base_image: &str) -> Option<BaseDistro> {
    base_distro_from_cascade().or_else(|| BaseDistro::from_base_image(base_image))
}

/// Read the git repo URL for ArgoCD generation from the config cascade.
///
/// Reads `deployment.argocd.repo_url` from the YAML cascade. `None` when it is
/// not set: where an app's chart lives is not something a library can guess,
/// so `generate-artefacts` writes no ArgoCD `Application` without it.
#[must_use]
pub fn argocd_repo_url_from_cascade() -> Option<String> {
    cascade_string("deployment.argocd.repo_url")
}

/// Read the ArgoCD destination namespace from the config cascade.
///
/// Reads `deployment.argocd.dest_namespace` from the YAML cascade. `None` when
/// it is not set, which leaves [`ArgocdConfig::dest_namespace`](super::ArgocdConfig::dest_namespace)
/// empty and deploys the app into a namespace named after it.
#[must_use]
pub fn argocd_dest_namespace_from_cascade() -> Option<String> {
    cascade_string("deployment.argocd.dest_namespace")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_base_image_is_pinned_by_digest() {
        // Asserts the shape rather than the digest, which Renovate moves.
        let digest = DEFAULT_BASE_IMAGE
            .strip_prefix("debian:trixie-slim@sha256:")
            .unwrap_or_else(|| panic!("not a digest-pinned trixie-slim: {DEFAULT_BASE_IMAGE}"));
        assert_eq!(digest.len(), 64, "sha256 digest is 64 hex chars: {digest}");
        assert!(
            digest
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
            "digest is not lower-case hex: {digest}"
        );
    }

    #[test]
    fn cascade_falls_back_to_defaults_when_no_config() {
        // No config setup -> the base image has a default, and the registry and
        // the ArgoCD source and namespace have none.
        assert_eq!(image_registry_from_cascade(), None);
        assert_eq!(argocd_repo_url_from_cascade(), None);
        assert_eq!(argocd_dest_namespace_from_cascade(), None);
        assert_eq!(base_image_from_cascade(), DEFAULT_BASE_IMAGE);
    }

    #[test]
    fn default_distro_matches_default_base_image() {
        // These two are a pair: the default package names must be for the
        // release the default base image actually is.
        assert_eq!(
            BaseDistro::from_base_image(DEFAULT_BASE_IMAGE),
            Some(DEFAULT_BASE_DISTRO)
        );
    }

    #[test]
    fn resolve_falls_through_to_the_base_image() {
        // No cascade config in a unit test, so the image string answers.
        assert_eq!(resolve_base_distro("ubuntu:24.04"), Some(BaseDistro::Noble));
    }

    #[test]
    fn resolve_gives_up_on_a_digest_pin() {
        // The caller falls back to DEFAULT_BASE_DISTRO and records that it did
        // -- see NativeDepsContract::unresolved_base_image.
        assert_eq!(resolve_base_distro("debian@sha256:abc123"), None);
    }

    #[test]
    fn env_answers_when_the_cascade_is_not_loaded() {
        let _env = crate::deployment::env_test_lock::guard();
        // The remedy the generated Dockerfile names has to work in the command
        // that GENERATES Dockerfiles -- and artefact generation never calls
        // load_config, so the YAML cascade is not initialised there. Reading the
        // ENV layer's own spelling is what makes the advice true. A digest pin
        // is the case that reaches it.
        temp_env::with_var("DEPLOYMENT__BASE_DISTRO", Some("bookworm"), || {
            assert_eq!(
                resolve_base_distro("debian@sha256:abc123"),
                Some(BaseDistro::Bookworm)
            );
        });
    }

    #[test]
    fn explicit_distro_beats_the_base_image() {
        let _env = crate::deployment::env_test_lock::guard();
        // Config is explicit, the image string is a guess -- explicit wins.
        temp_env::with_var("DEPLOYMENT__BASE_DISTRO", Some("noble"), || {
            assert_eq!(
                resolve_base_distro("debian:trixie-slim"),
                Some(BaseDistro::Noble)
            );
        });
    }

    #[test]
    fn an_unparseable_or_empty_env_value_is_treated_as_unset() {
        let _env = crate::deployment::env_test_lock::guard();
        temp_env::with_var("DEPLOYMENT__BASE_DISTRO", Some("plucky"), || {
            // Falls through to the image rather than substituting something.
            assert_eq!(
                resolve_base_distro("debian:trixie-slim"),
                Some(BaseDistro::Trixie)
            );
            assert_eq!(resolve_base_distro("debian@sha256:abc"), None);
        });
        temp_env::with_var("DEPLOYMENT__BASE_DISTRO", Some(""), || {
            assert_eq!(resolve_base_distro("debian@sha256:abc"), None);
        });
    }
}
