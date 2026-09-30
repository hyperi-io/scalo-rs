// Project:   scalo
// File:      tests/e2e/contract_artefacts.rs
// Purpose:   E2E tests for generated container contract artefacts (TEMPLATE)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! ============================================================================
//! TEMPLATE -- copy this file into your consumer's `tests/e2e/` and
//! adapt the FIXTURE section. The probe/skip/cluster helpers live in
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
//! Everything else -- the Tier A + Tier B test bodies -- stays the same.
//! ============================================================================
//!
//! E2E tests for the artefacts emitted by `crate::deployment` -- the
//! Dockerfile, Helm chart, and ArgoCD Application. Two tiers:
//!
//! - **Tier A** (default): light-weight checks that exercise the artefact
//!   without a real cluster.
//!   - Dockerfile: `docker build` + `docker run --rm <img> --help` -- proves
//!     the image actually starts.
//!   - Helm chart: `helm lint` + `helm template` -- proves the chart
//!     renders. (Without a cluster, deployment manifests can't "execute".)
//!   - ArgoCD Application: `kubeconform` -- proves the manifest is
//!     schema-valid.
//! - **Tier B** (env-gated by `E2E_CLUSTER=1`): heavy-weight checks
//!   that bring up a local kind cluster.
//!   - Helm: `helm install` on the kind cluster, assert the release lands.
//!   - ArgoCD: install ArgoCD into the cluster, apply the generated
//!     Application, verify the live object carries the identity annotations.
//!
//! # Skip policy
//!
//! Every test that needs an external tool / daemon / cluster probes first
//! via `scalo::deployment::test_support` and skips cleanly when
//! the dependency is absent. Skip emissions use the canonical prefix
//! `HYPERCI-SKIP[contract-e2e][tier-a|tier-b]:` so downstream test
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
use std::time::Duration;

use scalo::deployment::test_support::{
    docker_available, docker_empty_creds_json, docker_host, ensure_kind_cluster,
    hadolint_available, helm_available, kubeconform_available, skip, tier_b_enabled, wait_until,
};
use scalo::deployment::{
    ArgocdConfig, ContractIdentity, DeploymentContract, HealthContract, ImageProfile,
    KafkaLagTrigger, KedaContract, OciLabels, PortContract, generate_argocd_application,
    generate_chart, generate_dockerfile, generate_runtime_stage,
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
        let dockerfile = generate_dockerfile(contract, identity);

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
        labels.contains("io.hyperi.contract.version")
            && labels.contains("\"v1\"")
            && labels.contains("io.hyperi.contract.source-commit")
            && labels.contains("0123456789abcdef0123456789abcdef01234567")
            && labels.contains("io.hyperi.contract.image-ref"),
        "docker inspect did not show all three io.hyperi.contract.* labels: {labels}",
    );
}

/// The apt block is the part that can be wrong in a way only a build catches.
///
/// The fixture above deliberately carries no native deps, so it never installs
/// anything. This one uses the real default base and a real feature set, which
/// makes the build resolve every generated package name for that release --
/// the Confluent repo import, the trixie-to-bookworm suite mapping, and the
/// release-specific `libssl3t64` / `libgit2-1.9`. A package search is not proof
/// a package exists (it does not surface `Provides:`); a build is.
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
            "ldconfig -p | grep -E 'librdkafka|libssl|libgit2|libzstd'",
        ])
        .output()
        .expect("docker run invocation");
    let listed = String::from_utf8_lossy(&run.stdout);
    assert!(
        run.status.success(),
        "could not list shared objects in the image: stderr={}",
        String::from_utf8_lossy(&run.stderr),
    );
    for so in ["librdkafka.so", "libssl.so", "libgit2.so", "libzstd.so"] {
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
        &["transport-kafka", "spool"],
        scalo::deployment::DEFAULT_BASE_DISTRO,
    );

    // Run from an empty directory so no .hadolint.yaml in the checkout applies.
    let dir = tempfile::tempdir().expect("tempdir");
    for (file, text) in [
        ("Dockerfile", generate_dockerfile(&contract, None)),
        ("Dockerfile.runtime", generate_runtime_stage(&contract)),
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

// ============================================================================
// Tier A -- Helm chart: helm lint + helm template
// ============================================================================

#[test]
fn tier_a_chart_lint_and_template() {
    if !helm_available() {
        skip(
            "tier-a",
            "tier_a_chart_lint_and_template",
            "helm CLI not available",
        );
        return;
    }

    let contract = test_contract();
    let identity = test_identity();

    let tmp = tempfile::tempdir().expect("tempdir");
    let chart_dir = tmp.path().join("chart");
    std::fs::create_dir_all(&chart_dir).expect("create chart dir");
    generate_chart(&contract, &chart_dir, Some(&identity)).expect("generate_chart");

    let lint = Command::new("helm")
        .arg("lint")
        .arg(&chart_dir)
        .output()
        .expect("helm lint invocation");
    assert!(
        lint.status.success(),
        "helm lint failed: stdout={} stderr={}",
        String::from_utf8_lossy(&lint.stdout),
        String::from_utf8_lossy(&lint.stderr),
    );

    let template = Command::new("helm")
        .args(["template", "test-release"])
        .arg(&chart_dir)
        .output()
        .expect("helm template invocation");
    assert!(
        template.status.success(),
        "helm template failed: stdout={} stderr={}",
        String::from_utf8_lossy(&template.stdout),
        String::from_utf8_lossy(&template.stderr),
    );
    let rendered = String::from_utf8_lossy(&template.stdout);
    assert!(
        rendered.contains("hyperi-contract-test"),
        "rendered template missing app name: {rendered}",
    );

    let chart_yaml =
        std::fs::read_to_string(chart_dir.join("Chart.yaml")).expect("read Chart.yaml");
    assert!(chart_yaml.contains("io.hyperi.contract.version: \"v1\""));
    assert!(chart_yaml.contains(
        "io.hyperi.contract.source-commit: \"0123456789abcdef0123456789abcdef01234567\""
    ));
    assert!(
        chart_yaml.contains(
            "io.hyperi.contract.image-ref: \"ghcr.io/hyperi-io/hyperi-contract-test:test\""
        )
    );
}

// ============================================================================
// Tier A -- KEDA trigger: assert the RENDERED values, not that helm exited 0
// ============================================================================

/// The fixture contract with KEDA on and `default_config` as the app's config.
fn keda_contract(default_config: serde_json::Value) -> DeploymentContract {
    DeploymentContract {
        keda: Some(KedaContract::default()),
        default_config: Some(default_config),
        ..test_contract()
    }
}

/// Render only the ScaledObject and return the kafka trigger's metadata.
/// Panics with helm's stderr if the chart does not render.
fn rendered_kafka_trigger(
    contract: &DeploymentContract,
    extra_args: &[&str],
) -> serde_yaml_ng::Value {
    let tmp = tempfile::tempdir().expect("tempdir");
    let chart_dir = tmp.path().join("chart");
    generate_chart(contract, &chart_dir, None).expect("generate_chart");

    let out = Command::new("helm")
        .args(["template", "test-release"])
        .arg(&chart_dir)
        .args(["--show-only", "templates/keda-scaledobject.yaml"])
        .args(extra_args)
        .output()
        .expect("helm template invocation");
    assert!(
        out.status.success(),
        "helm template failed with {extra_args:?}: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_slice(&out.stdout).expect("rendered ScaledObject is YAML");
    let triggers = doc["spec"]["triggers"]
        .as_sequence()
        .expect("ScaledObject has a triggers list");
    triggers
        .iter()
        .find(|t| t["type"].as_str() == Some("kafka"))
        .expect("ScaledObject has a kafka trigger")["metadata"]
        .clone()
}

/// A rendered metadata value as a string; an empty render comes back as null.
fn rendered_str<'a>(metadata: &'a serde_yaml_ng::Value, key: &str) -> &'a str {
    metadata[key].as_str().unwrap_or_default()
}

/// KEDA admits a trigger with an unparseable broker list and simply never
/// scales, so the rendered values are what has to be checked.
#[test]
fn tier_a_keda_trigger_renders_usable_values() {
    if !helm_available() {
        skip(
            "tier-a",
            "tier_a_keda_trigger_renders_usable_values",
            "helm CLI not available",
        );
        return;
    }

    let contract = keda_contract(serde_json::json!({
        "kafka": {
            "brokers": ["kafka:9092", "kafka2:9092"],
            "group_id": "hct-group",
            "topics": ["events", "audit"],
        }
    }));

    let metadata = rendered_kafka_trigger(&contract, &[]);
    assert_eq!(
        rendered_str(&metadata, "bootstrapServers"),
        "kafka:9092,kafka2:9092"
    );
    assert_eq!(rendered_str(&metadata, "consumerGroup"), "hct-group");
    assert_eq!(rendered_str(&metadata, "topic"), "events");
}

/// An app that keeps its Kafka settings under `config.source` has no
/// `config.kafka` key, and operators set brokers and topics as a list or a
/// comma-separated string, or null the section out, so every shape must render.
#[test]
fn tier_a_keda_trigger_follows_a_source_shaped_config() {
    if !helm_available() {
        skip(
            "tier-a",
            "tier_a_keda_trigger_follows_a_source_shaped_config",
            "helm CLI not available",
        );
        return;
    }

    let mut contract = keda_contract(serde_json::json!({
        "source": {
            "brokers": ["kafka:9092", "kafka2:9092"],
            "group_id": "hct-group",
            "topics": ["events", "audit"],
        },
        "sink": { "brokers": ["sink:9092"], "topic": "out" },
    }));
    contract.keda = contract
        .keda
        .map(|k| k.with_kafka_trigger(KafkaLagTrigger::under("config.source")));

    let tmp = tempfile::tempdir().expect("tempdir");
    let chart_dir = tmp.path().join("chart");
    generate_chart(&contract, &chart_dir, None).expect("generate_chart");
    let lint = Command::new("helm")
        .arg("lint")
        .arg(&chart_dir)
        .output()
        .expect("helm lint invocation");
    assert!(
        lint.status.success(),
        "helm lint failed: stdout={} stderr={}",
        String::from_utf8_lossy(&lint.stdout),
        String::from_utf8_lossy(&lint.stderr),
    );

    let metadata = rendered_kafka_trigger(&contract, &[]);
    assert_eq!(
        rendered_str(&metadata, "bootstrapServers"),
        "kafka:9092,kafka2:9092"
    );
    assert!(
        !rendered_str(&metadata, "consumerGroup").is_empty(),
        "consumerGroup rendered empty: {metadata:?}"
    );
    assert_eq!(rendered_str(&metadata, "consumerGroup"), "hct-group");
    assert_eq!(rendered_str(&metadata, "topic"), "events");

    let string_brokers = rendered_kafka_trigger(
        &contract,
        &[
            "--set-json",
            r#"config.source.brokers="kafka:9092,kafka2:9092""#,
        ],
    );
    assert_eq!(
        rendered_str(&string_brokers, "bootstrapServers"),
        "kafka:9092,kafka2:9092"
    );

    let string_topics = rendered_kafka_trigger(
        &contract,
        &["--set-json", r#"config.source.topics="events,audit""#],
    );
    assert_eq!(rendered_str(&string_topics, "topic"), "events");

    let no_source = rendered_kafka_trigger(&contract, &["--set-json", "config.source=null"]);
    assert_eq!(rendered_str(&no_source, "bootstrapServers"), "");
    assert_eq!(rendered_str(&no_source, "topic"), "");
}

// ============================================================================
// Tier A -- gated ports: a port renders only while its listener is on
// ============================================================================

/// Render one template of `chart_dir` with `extra_args` and parse it.
/// Panics with helm's stderr if the chart does not render.
fn render_template(chart_dir: &Path, template: &str, extra_args: &[&str]) -> serde_yaml_ng::Value {
    let out = Command::new("helm")
        .args(["template", "test-release"])
        .arg(chart_dir)
        .args(["--show-only", template])
        .args(extra_args)
        .output()
        .expect("helm template invocation");
    assert!(
        out.status.success(),
        "helm template {template} failed with {extra_args:?}: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    serde_yaml_ng::from_slice(&out.stdout).expect("rendered template is YAML")
}

/// The port names the Deployment's container and the Service each render.
fn rendered_port_names(chart_dir: &Path, extra_args: &[&str]) -> (Vec<String>, Vec<String>) {
    let names = |ports: &serde_yaml_ng::Value| -> Vec<String> {
        ports
            .as_sequence()
            .expect("a ports list")
            .iter()
            .map(|p| p["name"].as_str().expect("a port name").to_string())
            .collect()
    };
    let deployment = render_template(chart_dir, "templates/deployment.yaml", extra_args);
    let service = render_template(chart_dir, "templates/service.yaml", extra_args);
    (
        names(&deployment["spec"]["template"]["spec"]["containers"][0]["ports"]),
        names(&service["spec"]["ports"]),
    )
}

/// A gate on a key the config lacks, or sets to something else, renders
/// nothing and does not fail the render; setting the key renders the port.
#[test]
fn tier_a_chart_renders_a_gated_port_only_when_its_listener_is_on() {
    if !helm_available() {
        skip(
            "tier-a",
            "tier_a_chart_renders_a_gated_port_only_when_its_listener_is_on",
            "helm CLI not available",
        );
        return;
    }

    let contract = DeploymentContract {
        extra_ports: vec![
            PortContract::tcp("http", 8080),
            PortContract::tcp("grpc", 6001).when_enabled("config.grpc.enabled"),
            PortContract::tcp("push", 6000).when_equals("config.source.transport", "direct"),
            PortContract::tcp("relay", 6002)
                .when_one_of("config.source.transport", ["direct", "grpc"]),
        ],
        // No `source` key at all, and the grpc switch off.
        default_config: Some(serde_json::json!({ "grpc": { "enabled": false } })),
        ..test_contract()
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let chart_dir = tmp.path().join("chart");
    generate_chart(&contract, &chart_dir, None).expect("generate_chart");

    let lint = Command::new("helm")
        .arg("lint")
        .arg(&chart_dir)
        .output()
        .expect("helm lint invocation");
    assert!(
        lint.status.success(),
        "helm lint failed: stdout={} stderr={}",
        String::from_utf8_lossy(&lint.stdout),
        String::from_utf8_lossy(&lint.stderr),
    );

    let cases: [(&[&str], &[&str]); 5] = [
        (&[], &["metrics", "http"]),
        (
            &["--set", "config.source.transport=direct"],
            &["metrics", "http", "push", "relay"],
        ),
        (
            &["--set", "config.source.transport=grpc"],
            &["metrics", "http", "relay"],
        ),
        (
            &["--set", "config.grpc.enabled=true"],
            &["metrics", "http", "grpc"],
        ),
        (&["--set-json", "config=null"], &["metrics", "http"]),
    ];
    for (args, expected) in cases {
        let (deployment, service) = rendered_port_names(&chart_dir, args);
        assert_eq!(deployment, expected, "Deployment ports with {args:?}");
        assert_eq!(service, expected, "Service ports with {args:?}");
    }
}

// ============================================================================
// Tier A -- replicas: exactly one owner of the replica count
// ============================================================================

/// What owns the replica count in one render of a chart.
#[derive(Debug, PartialEq)]
struct ReplicaOwners {
    scaled_object: bool,
    hpa: bool,
    /// The Deployment's own `replicas`, when it sets one.
    replicas: Option<u64>,
}

/// Render the whole chart with `extra_args` and report what owns the replica count.
fn replica_owners(chart_dir: &Path, extra_args: &[&str]) -> ReplicaOwners {
    use serde::Deserialize;

    let out = Command::new("helm")
        .args(["template", "test-release"])
        .arg(chart_dir)
        .args(extra_args)
        .output()
        .expect("helm template invocation");
    assert!(
        out.status.success(),
        "helm template failed with {extra_args:?}: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    let docs: Vec<serde_yaml_ng::Value> = serde_yaml_ng::Deserializer::from_slice(&out.stdout)
        .map(|doc| serde_yaml_ng::Value::deserialize(doc).expect("rendered document is YAML"))
        .collect();
    let of_kind = |kind: &str| docs.iter().find(|d| d["kind"].as_str() == Some(kind));
    ReplicaOwners {
        scaled_object: of_kind("ScaledObject").is_some(),
        hpa: of_kind("HorizontalPodAutoscaler").is_some(),
        replicas: of_kind("Deployment").expect("a Deployment")["spec"]["replicas"].as_u64(),
    }
}

/// A Deployment without `replicas` runs one pod, so whenever neither a
/// ScaledObject nor the HPA renders the Deployment must set `replicaCount`.
#[test]
fn tier_a_chart_sets_replicas_whenever_no_scaler_renders() {
    if !helm_available() {
        skip(
            "tier-a",
            "tier_a_chart_sets_replicas_whenever_no_scaler_renders",
            "helm CLI not available",
        );
        return;
    }

    let kafka = keda_contract(serde_json::json!({
        "kafka": { "brokers": ["kafka:9092"], "group_id": "g", "topics": ["events"] }
    }));
    let mut cpu_only = kafka.clone();
    cpu_only.keda = cpu_only
        .keda
        .map(|k| k.with_kafka_trigger(KafkaLagTrigger::disabled()));
    let no_keda = test_contract();

    let owned_by = |scaled_object, hpa, replicas| ReplicaOwners {
        scaled_object,
        hpa,
        replicas,
    };
    let keda_off = ["--set", "keda.enabled=false"];
    let hpa_on = ["--set", "autoscaling.enabled=true"];
    let cases: Vec<(&str, &DeploymentContract, Vec<&str>, ReplicaOwners)> = vec![
        ("kafka", &kafka, vec![], owned_by(true, false, None)),
        (
            "kafka",
            &kafka,
            keda_off.to_vec(),
            owned_by(false, false, Some(3)),
        ),
        (
            "kafka",
            &kafka,
            [keda_off, hpa_on].concat(),
            owned_by(false, true, None),
        ),
        ("cpu-only", &cpu_only, vec![], owned_by(true, false, None)),
        (
            "cpu-only",
            &cpu_only,
            vec!["--set", "keda.cpu.enabled=false"],
            owned_by(false, false, Some(3)),
        ),
        (
            "cpu-only",
            &cpu_only,
            vec![
                "--set",
                "keda.cpu.enabled=false",
                "--set",
                "autoscaling.enabled=true",
            ],
            owned_by(false, false, Some(3)),
        ),
        (
            "cpu-only",
            &cpu_only,
            [keda_off, hpa_on].concat(),
            owned_by(false, true, None),
        ),
        ("no keda", &no_keda, vec![], owned_by(false, false, Some(3))),
        (
            "no keda",
            &no_keda,
            hpa_on.to_vec(),
            owned_by(false, true, None),
        ),
        (
            "no keda",
            &no_keda,
            vec!["--set", "keda.enabled=true"],
            owned_by(false, false, Some(3)),
        ),
    ];

    for (label, contract, args, expected) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let chart_dir = tmp.path().join("chart");
        generate_chart(contract, &chart_dir, None).expect("generate_chart");
        let args = [&["--set", "replicaCount=3"][..], &args[..]].concat();
        assert_eq!(
            replica_owners(&chart_dir, &args),
            expected,
            "{label} contract with {args:?}"
        );
    }
}

// ============================================================================
// Tier A -- service-account files: the instance id survives a restart
// ============================================================================

/// The version check derives its instance id from `ca.crt` and `namespace` in
/// the service-account directory, so the pod carries both without the token,
/// whether the chart creates the account or the operator supplies one.
#[test]
fn tier_a_chart_projects_the_service_account_files_without_the_token() {
    if !helm_available() {
        skip(
            "tier-a",
            "tier_a_chart_projects_the_service_account_files_without_the_token",
            "helm CLI not available",
        );
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let chart_dir = tmp.path().join("chart");
    generate_chart(&test_contract(), &chart_dir, None).expect("generate_chart");

    let account = render_template(&chart_dir, "templates/serviceaccount.yaml", &[]);
    assert_eq!(
        account["automountServiceAccountToken"].as_bool(),
        Some(false)
    );

    let expected_sources: serde_yaml_ng::Value = serde_yaml_ng::from_str(
        r"
- configMap:
    name: kube-root-ca.crt
    items:
      - key: ca.crt
        path: ca.crt
- downwardAPI:
    items:
      - path: namespace
        fieldRef:
          fieldPath: metadata.namespace
",
    )
    .expect("expected sources are YAML");

    for args in [&[][..], &["--set", "serviceAccount.create=false"][..]] {
        let deployment = render_template(&chart_dir, "templates/deployment.yaml", args);
        let pod = &deployment["spec"]["template"]["spec"];
        assert_eq!(
            pod["automountServiceAccountToken"].as_bool(),
            Some(false),
            "pod automount with {args:?}"
        );

        let mount = pod["containers"][0]["volumeMounts"]
            .as_sequence()
            .expect("a volumeMounts list")
            .iter()
            .find(|m| {
                m["mountPath"].as_str() == Some("/var/run/secrets/kubernetes.io/serviceaccount")
            })
            .expect("a mount at the service-account path");
        assert_eq!(mount["readOnly"].as_bool(), Some(true), "{args:?}");

        let volume = pod["volumes"]
            .as_sequence()
            .expect("a volumes list")
            .iter()
            .find(|v| v["name"] == mount["name"])
            .expect("the volume the mount names");
        // Exactly these two sources, so no serviceAccountToken source rides along.
        assert_eq!(volume["projected"]["sources"], expected_sources, "{args:?}");
    }
}

// ============================================================================
// Tier A -- ArgoCD Application: kubeconform
// ============================================================================

#[test]
fn tier_a_argocd_application_kubeconform() {
    if !kubeconform_available() {
        skip(
            "tier-a",
            "tier_a_argocd_application_kubeconform",
            "kubeconform not on PATH",
        );
        return;
    }

    let contract = test_contract();
    let identity = test_identity();
    let argo = ArgocdConfig::default();
    let yaml = generate_argocd_application(&contract, &argo, Some(&identity));

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("application.yaml");
    std::fs::write(&path, &yaml).expect("write application.yaml");

    let out = Command::new("kubeconform")
        .args(["-strict", "-summary", "-ignore-missing-schemas"])
        .arg(&path)
        .output()
        .expect("kubeconform invocation");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "kubeconform failed: stdout={stdout} stderr={stderr}",
    );
    assert!(
        stdout.contains("0 errors") || stdout.contains("Valid:"),
        "kubeconform summary missing 0-error line: {stdout}",
    );

    let raw = std::fs::read_to_string(&path).unwrap();
    assert_eq!(raw.matches("io.hyperi.contract").count(), 3);
}

// ============================================================================
// Tier B -- kind cluster + real helm install. Env-gated.
// ============================================================================

#[test]
fn tier_b_helm_install_on_kind() {
    if !tier_b_enabled() {
        skip(
            "tier-b",
            "tier_b_helm_install_on_kind",
            "E2E_CLUSTER env var not set (skipping cluster-based tests)",
        );
        return;
    }
    if !helm_available() {
        skip(
            "tier-b",
            "tier_b_helm_install_on_kind",
            "helm CLI not available",
        );
        return;
    }

    let Some(cluster) = ensure_kind_cluster("tier_b_helm_install_on_kind") else {
        return;
    };

    let contract = test_contract();
    let identity = test_identity();

    let tmp = tempfile::tempdir().expect("tempdir");
    let chart_dir = tmp.path().join("chart");
    std::fs::create_dir_all(&chart_dir).expect("create chart dir");
    generate_chart(&contract, &chart_dir, Some(&identity)).expect("generate_chart");

    let install = Command::new("helm")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args([
            "install",
            "test-release",
            chart_dir.to_str().unwrap(),
            "--namespace",
            "default",
            "--set",
            "image.repository=public.ecr.aws/docker/library/nginx",
            "--set",
            "image.tag=alpine",
            "--wait",
            "--timeout",
            "120s",
        ])
        .output()
        .expect("helm install invocation");
    let stdout = String::from_utf8_lossy(&install.stdout);
    let stderr = String::from_utf8_lossy(&install.stderr);
    if !install.status.success() {
        assert!(
            !(stderr.contains("Error: INSTALLATION FAILED") && stderr.contains("template:")),
            "helm install failed at template stage: {stderr}",
        );
        eprintln!("note: helm install completed admission but pod did not become ready ({stderr})");
    }
    assert!(
        !stdout.is_empty() || !stderr.is_empty(),
        "helm install produced no output -- something is very wrong",
    );

    let list = Command::new("helm")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args(["list", "--all-namespaces", "-o", "json"])
        .output()
        .expect("helm list");
    let out = String::from_utf8_lossy(&list.stdout);
    assert!(
        out.contains("test-release"),
        "helm release 'test-release' not found post-install: {out}",
    );

    let _ = Command::new("helm")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args(["uninstall", "test-release"])
        .output();
}

#[test]
fn tier_b_argocd_application_sync_on_kind() {
    if !tier_b_enabled() {
        skip(
            "tier-b",
            "tier_b_argocd_application_sync_on_kind",
            "E2E_CLUSTER env var not set (skipping cluster-based tests)",
        );
        return;
    }

    let Some(cluster) = ensure_kind_cluster("tier_b_argocd_application_sync_on_kind") else {
        return;
    };

    // Install ArgoCD into the cluster.
    let ns_yaml = Command::new("kubectl")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args([
            "create",
            "namespace",
            "argocd",
            "--dry-run=client",
            "-o",
            "yaml",
        ])
        .output()
        .expect("kubectl ns yaml");
    let ns_apply = Command::new("kubectl")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.as_mut().unwrap().write_all(&ns_yaml.stdout)?;
            child.wait_with_output()
        })
        .expect("kubectl apply namespace");
    assert!(ns_apply.status.success());

    let install = Command::new("kubectl")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args([
            "apply",
            "-n",
            "argocd",
            "-f",
            "https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml",
        ])
        .output()
        .expect("kubectl apply argocd");
    if !install.status.success() {
        skip(
            "tier-b",
            "tier_b_argocd_application_sync_on_kind",
            &format!(
                "ArgoCD manifest fetch/apply failed (network?): {}",
                String::from_utf8_lossy(&install.stderr).trim()
            ),
        );
        return;
    }

    // Wait for argocd-server Deployment to become Available.
    let kubeconfig = cluster.kubeconfig.clone();
    let server_ready = wait_until(Duration::from_mins(5), Duration::from_secs(5), || {
        Command::new("kubectl")
            .env("KUBECONFIG", &kubeconfig)
            .args([
                "-n",
                "argocd",
                "wait",
                "--for=condition=Available",
                "--timeout=10s",
                "deploy/argocd-server",
            ])
            .output()
            .is_ok_and(|o| o.status.success())
    });
    if !server_ready {
        skip(
            "tier-b",
            "tier_b_argocd_application_sync_on_kind",
            "argocd-server did not become Available within 300s",
        );
        return;
    }

    // Apply the generated Application + verify identity annotations on the
    // live object.
    let contract = test_contract();
    let identity = test_identity();
    let argo = ArgocdConfig::default();
    let app_yaml = generate_argocd_application(&contract, &argo, Some(&identity));

    let tmp = tempfile::tempdir().expect("tempdir");
    let app_path = tmp.path().join("application.yaml");
    std::fs::write(&app_path, &app_yaml).expect("write app yaml");

    let apply = Command::new("kubectl")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args(["apply", "-f"])
        .arg(&app_path)
        .output()
        .expect("kubectl apply application");
    assert!(
        apply.status.success(),
        "kubectl apply application failed: {}",
        String::from_utf8_lossy(&apply.stderr),
    );

    let get = Command::new("kubectl")
        .env("KUBECONFIG", &cluster.kubeconfig)
        .args([
            "-n",
            "argocd",
            "get",
            "application",
            &contract.app_name,
            "-o",
            "jsonpath={.metadata.annotations}",
        ])
        .output()
        .expect("kubectl get application");
    let annotations = String::from_utf8_lossy(&get.stdout);
    assert!(
        annotations.contains("io.hyperi.contract.version")
            && annotations.contains("v1")
            && annotations.contains("io.hyperi.contract.source-commit")
            && annotations.contains("io.hyperi.contract.image-ref"),
        "applied Application missing identity annotations: {annotations}",
    );
}
