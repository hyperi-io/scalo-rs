// Project:   scalo
// File:      tests/e2e/contract_artefacts.rs
// Purpose:   E2E tests for generated container contract artefacts (TEMPLATE)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! ============================================================================
//! TEMPLATE -- copy this file into your consumer's `tests/e2e/` and
//! adapt the FIXTURE section. The probe and skip helpers live in
//! `scalo::deployment::test_support` so each consumer's copy
//! stays short and benefits from any bug fixes pushed to scalo.
//!
//! Bring the test_support helpers into scope by adding a dev-dependency
//! on scalo version 2.7.3 or higher with the
//! `deployment-test-support` feature enabled.
//!
//! Then in your consumer's tests/e2e/contract_artefacts.rs, replace this
//! file's `test_contract()` / `test_identity()` fixtures with your own
//! `Config::deployment_contract()` (or whatever your app uses to build
//! its `DeploymentContract`) and `ContractIdentity::detect(image_ref)`.
//! Everything else -- the Tier A test bodies -- stays the same.
//! ============================================================================
//!
//! E2E tests for the Dockerfile `crate::deployment` emits, without a cluster:
//!
//! - `docker build` + `docker run --rm <img> --help` -- proves the image
//!   actually starts.
//! - `hadolint` -- proves the Dockerfile and runtime stage lint clean.
//!
//! The Kubernetes objects come from the scalo-service library chart, which
//! `charts/scalo-service/tests/run.sh` renders and validates.
//!
//! # Skip policy
//!
//! Every test that needs an external tool or daemon probes first
//! via `scalo::deployment::test_support` and skips cleanly when
//! the dependency is absent. Skip emissions use the canonical prefix
//! `HYPERCI-SKIP[contract-e2e][tier-a]:` so downstream test
//! runners can grep, count, and emit a summary line at the end of a CI
//! run.
//!
//! # Why a mock binary?
//!
//! `generate_dockerfile()` produces a Dockerfile that `COPY`s the
//! consumer app's binary into the image. scalo itself isn't an app, so
//! for Tier A this template drops a tiny POSIX shell script into the
//! build context that responds to `--help`. Real consumer copies of this
//! template should REPLACE the mock with `cargo build --release --bin
//! <name>` and copy the produced binary into the docker build context.
//! See a receiver's adaptation for the canonical example.

#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::io::Write;
use std::path::Path;
use std::process::Command;

use scalo::deployment::test_support::{
    docker_available, docker_empty_creds_json, docker_host, hadolint_available, skip,
};
use scalo::deployment::{
    ContractIdentity, DeploymentContract, HealthContract, ImageProfile, OciLabels,
    generate_dockerfile, generate_runtime_stage,
};

// ============================================================================
// FIXTURE -- replace `test_contract()` and `test_identity()` in your
// consumer's copy with calls to your app's real contract / identity.
// ============================================================================

fn test_contract() -> DeploymentContract {
    DeploymentContract {
        app_name: "hyperi-contract-test".into(),
        binary_name: "hyperi-contract-test".into(),
        description: "Throwaway test app for scalo contract e2e".into(),
        metrics_port: 9090,
        health: HealthContract::default(),
        env_prefix: "HCT".into(),
        metric_prefix: "hct".into(),
        config_mount_path: "/etc/hct/config.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        extra_ports: vec![],
        unbound_listen_paths: vec![],
        entrypoint_args: vec![],
        secrets: vec![],
        base_image: "ubuntu:24.04".into(),
        native_deps: scalo::deployment::NativeDepsContract::default(),
        image_profile: ImageProfile::Production,
        oci_labels: OciLabels::default(),
        schema_version: 3,
        config_schema: None,
        capabilities: vec![],
        writable_paths: vec![],
        termination_grace_seconds: 45,
        resources: scalo::deployment::ResourcesContract::default(),
        security: scalo::deployment::SecurityContract::default(),
        singleton: false,
        service_account: scalo::deployment::ServiceAccount::Own,
        hot_paths: vec![],
        keda: None,
        default_config: None,
        depends_on: vec![],
    }
}

fn test_identity() -> ContractIdentity {
    ContractIdentity::new(
        "0123456789abcdef0123456789abcdef01234567",
        "ghcr.io/hyperi-io/hyperi-contract-test:test",
    )
    .expect("fixture identity must validate")
}

/// Drop a tiny POSIX script into the build context to stand in for the
/// real consumer binary. Real consumer copies of this template should
/// REPLACE this with `cargo build` of their actual binary.
fn write_mock_binary(build_ctx: &Path, binary_name: &str) -> std::io::Result<()> {
    let path = build_ctx.join(binary_name);
    let mut f = std::fs::File::create(&path)?;
    f.write_all(
        b"#!/bin/sh\n\
          # Mock binary for scalo contract-artefact e2e test.\n\
          # The real consumer's binary is replaced by this stub during testing.\n\
          if [ \"$1\" = \"--help\" ] || [ \"$1\" = \"-h\" ]; then\n\
          \x20 echo \"hyperi-contract-test: ok\"\n\
          \x20 exit 0\n\
          fi\n\
          echo \"hyperi-contract-test: started (mock)\"\n\
          exit 0\n",
    )?;
    drop(f);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms)?;
    }
    Ok(())
}

// ============================================================================
// Tier A -- Dockerfile: docker build + docker run --help
// ============================================================================

/// An image built from a contract's generated Dockerfile, removed on drop.
///
/// Dropping rather than cleaning up at the end of the test matters: an assert
/// that fires unwinds past any trailing `docker rmi`, so a failing test would
/// otherwise leak its image every run.
struct BuiltImage {
    tag: String,
    docker_config: tempfile::TempDir,
}

impl BuiltImage {
    /// Generate the Dockerfile for `contract`, build it, and return the handle.
    /// Panics with the build output if the image does not build.
    fn build(
        contract: &DeploymentContract,
        identity: Option<&ContractIdentity>,
        tag_suffix: &str,
    ) -> Self {
        let dockerfile = generate_dockerfile(contract, identity).expect("the contract is valid");

        let ctx = tempfile::tempdir().expect("tempdir");
        let dockerfile_path = ctx.path().join("Dockerfile");
        std::fs::write(&dockerfile_path, &dockerfile).expect("write Dockerfile");
        write_mock_binary(ctx.path(), contract.binary()).expect("write mock binary");

        let docker_config = tempfile::tempdir().expect("docker config tempdir");
        std::fs::write(
            docker_config.path().join("config.json"),
            docker_empty_creds_json(),
        )
        .expect("write empty docker config");

        let tag = format!(
            "hyperi-contract-test:e2e-{suffix}-{pid}",
            suffix = tag_suffix,
            pid = std::process::id(),
        );

        let build = Self::docker_cmd(docker_config.path())
            .args(["build", "--quiet", "-t", &tag, "-f"])
            .arg(&dockerfile_path)
            .arg(ctx.path())
            .output()
            .expect("docker build invocation");
        assert!(
            build.status.success(),
            "docker build failed on base {base}: stdout={stdout} stderr={stderr}",
            base = contract.base_image,
            stdout = String::from_utf8_lossy(&build.stdout),
            stderr = String::from_utf8_lossy(&build.stderr),
        );

        Self { tag, docker_config }
    }

    /// A `docker` command pointed at a throwaway credential store.
    ///
    /// The throwaway `DOCKER_CONFIG` keeps credential helpers out of the test,
    /// but it also hides the context store, so the daemon endpoint has to be
    /// carried across explicitly or docker falls back to a socket path that is
    /// only correct on Linux CI.
    fn docker_cmd(docker_config: &Path) -> Command {
        let mut cmd = Command::new("docker");
        cmd.env("DOCKER_CONFIG", docker_config);
        if let Some(host) = docker_host() {
            cmd.env("DOCKER_HOST", host);
        }
        cmd
    }

    /// A `docker` command bound to this image's throwaway credential store.
    fn docker(&self) -> Command {
        Self::docker_cmd(self.docker_config.path())
    }
}

impl Drop for BuiltImage {
    fn drop(&mut self) {
        let _ = self.docker().args(["rmi", "-f", &self.tag]).output();
    }
}

/// `USER` is numeric, so the image's uid must still resolve to `appuser` and
/// its own group, or file ownership inside the image is wrong.
fn assert_runs_as_appuser(image: &BuiltImage) {
    let id = image
        .docker()
        .args(["run", "--rm", "--entrypoint", "id", &image.tag])
        .output()
        .expect("docker run invocation");
    let stdout = String::from_utf8_lossy(&id.stdout);
    assert!(
        id.status.success() && stdout.starts_with("uid=1000(appuser) gid=1000(appuser)"),
        "image does not run as appuser: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&id.stderr),
    );
}

#[test]
fn tier_a_dockerfile_builds_and_image_runs() {
    if !docker_available() {
        skip(
            "tier-a",
            "tier_a_dockerfile_builds_and_image_runs",
            "docker daemon not reachable",
        );
        return;
    }

    let contract = test_contract();
    let identity = test_identity();
    let image = BuiltImage::build(&contract, Some(&identity), "plain");

    let entrypoint = format!("/usr/local/bin/{}", contract.binary());
    let run = image
        .docker()
        .args([
            "run",
            "--rm",
            "--entrypoint",
            &entrypoint,
            &image.tag,
            "--help",
        ])
        .output()
        .expect("docker run invocation");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "docker run failed: stdout={stdout} stderr={stderr}",
    );
    assert!(
        stdout.contains("hyperi-contract-test: ok"),
        "container ran but did not produce expected output: stdout={stdout} stderr={stderr}",
    );
    assert_runs_as_appuser(&image);

    let inspect = image
        .docker()
        .args(["inspect", "--format", "{{json .Config.Labels}}", &image.tag])
        .output()
        .expect("docker inspect invocation");
    let labels = String::from_utf8_lossy(&inspect.stdout);
    assert!(
        labels.contains("io.scalo.contract.version")
            && labels.contains("\"v1\"")
            && labels.contains("io.scalo.contract.source-commit")
            && labels.contains("0123456789abcdef0123456789abcdef01234567")
            && labels.contains("io.scalo.contract.image-ref"),
        "docker inspect did not show all three io.scalo.contract.* labels: {labels}",
    );
}

/// The apt block is the part that can be wrong in a way only a build catches.
///
/// The fixture above deliberately carries no native deps, so it never installs
/// anything. This one uses the real default base, the release-specific
/// `libgit2-1.9` and a real signed vendor repo (Confluent's clients repo, which
/// has no trixie suite), so the build checks the key fingerprint, the
/// `signed-by` line and every package name. A package search is not proof a
/// package exists (it does not surface `Provides:`). A build is.
#[test]
fn tier_a_dockerfile_with_native_deps_builds() {
    if !docker_available() {
        skip(
            "tier-a",
            "tier_a_dockerfile_with_native_deps_builds",
            "docker daemon not reachable",
        );
        return;
    }

    let mut contract = test_contract();
    contract.base_image = scalo::deployment::DEFAULT_BASE_IMAGE.to_string();
    contract.native_deps = scalo::deployment::NativeDepsContract::for_features(
        &[
            "transport-kafka",
            "spool",
            "secrets",
            "directory-config-git",
        ],
        scalo::deployment::DEFAULT_BASE_DISTRO,
    );
    contract
        .native_deps
        .apt_repos
        .push(scalo::deployment::AptRepoContract {
            key_url: "https://packages.confluent.io/clients/deb/archive.key".into(),
            keyring: "/usr/share/keyrings/confluent-clients.gpg".into(),
            url: "https://packages.confluent.io/clients/deb".into(),
            codename: "bookworm".into(),
            key_fingerprint: "CBBB821E8FAF364F79835C438B1DA6120C2BF624".into(),
            packages: vec!["librdkafka1".into()],
        });
    // A failure here means a generated package name does not resolve on this
    // release -- the assertion inside `build` prints the apt output.
    let image = BuiltImage::build(&contract, None, "deps");

    // The .so files have to be present, not just the packages installed.
    let run = image
        .docker()
        .args([
            "run",
            "--rm",
            "--entrypoint",
            "/bin/sh",
            &image.tag,
            "-c",
            "ldconfig -p | grep -E 'librdkafka|libgit2'",
        ])
        .output()
        .expect("docker run invocation");
    let listed = String::from_utf8_lossy(&run.stdout);
    assert!(
        run.status.success(),
        "could not list shared objects in the image: stderr={}",
        String::from_utf8_lossy(&run.stderr),
    );
    for so in ["librdkafka.so", "libgit2.so"] {
        assert!(
            listed.contains(so),
            "{so} missing from the runtime image -- the package installed but the \
             shared object is not there: {listed}",
        );
    }
    assert_runs_as_appuser(&image);
}

// ============================================================================
// Tier A -- Dockerfile lint: hadolint
// ============================================================================

/// hadolint gates the build under the container standard, so the generated
/// Dockerfile and runtime fragment must lint clean with no config relaxing it.
/// The contract carries native deps so the apt block, the one with a pragma,
/// is in the output.
#[test]
fn tier_a_generated_dockerfiles_lint_clean() {
    if !hadolint_available() {
        skip(
            "tier-a",
            "tier_a_generated_dockerfiles_lint_clean",
            "hadolint not installed",
        );
        return;
    }

    let mut contract = test_contract();
    contract.base_image = scalo::deployment::DEFAULT_BASE_IMAGE.to_string();
    contract.native_deps = scalo::deployment::NativeDepsContract::for_features(
        &["directory-config-git"],
        scalo::deployment::DEFAULT_BASE_DISTRO,
    );

    // Run from an empty directory so no .hadolint.yaml in the checkout applies.
    let dir = tempfile::tempdir().expect("tempdir");
    for (file, text) in [
        (
            "Dockerfile",
            generate_dockerfile(&contract, None).expect("valid contract"),
        ),
        (
            "Dockerfile.runtime",
            generate_runtime_stage(&contract).expect("valid contract"),
        ),
    ] {
        let path = dir.path().join(file);
        std::fs::write(&path, &text).expect("write Dockerfile");
        let lint = Command::new("hadolint")
            .arg("--no-color")
            .arg(&path)
            .current_dir(dir.path())
            .output()
            .expect("hadolint invocation");
        let findings = String::from_utf8_lossy(&lint.stdout);
        assert!(
            lint.status.success() && findings.trim().is_empty(),
            "hadolint findings on generated {file}:\n{findings}{}\n{text}",
            String::from_utf8_lossy(&lint.stderr),
        );
    }
}
