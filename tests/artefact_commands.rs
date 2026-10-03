// Project:   scalo
// File:      tests/artefact_commands.rs
// Purpose:   metrics-manifest and generate-artefacts, read off a real process
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `metrics-manifest` and `generate-artefacts` run the way a service binary
//! runs them: in a process of their own, so what they print is read off real
//! stdout and stderr, and the config cascade installs fresh for each command.
//!
//! The test binary re-runs itself as that process. A test finding
//! [`PROBE_ARGS`] in its environment is the re-run: it runs the probe service
//! on those arguments and returns.

#![cfg(all(feature = "cli-service", feature = "deployment"))]

use std::path::Path;
use std::process::Command;

use clap::Parser;
use scalo::cli::{CliError, CommonArgs, ServiceApp, ServiceRuntime, StandardCommand, VersionInfo};
use scalo::config::{ConfigError, ConfigOptions};
use scalo::deployment::{
    DEFAULT_BASE_IMAGE, DeploymentContract, HealthContract, ImageProfile, NativeDepsContract,
    OciLabels, base_image_from_cascade,
};

/// The probe's command line, one argument per line, set only in the re-run.
const PROBE_ARGS: &str = "ARTEFACT_PROBE_ARGS";

/// A base image no default produces, so finding it proves the cascade was read.
const CONFIGURED_BASE_IMAGE: &str = "registry.example/base:configured";

#[derive(Parser)]
#[command(name = "artefact-probe")]
struct ProbeCli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<StandardCommand>,
}

/// A service whose contract reads its base image from the config cascade, as a
/// real one does.
struct ProbeApp {
    cli: ProbeCli,
}

impl ServiceApp for ProbeApp {
    type Config = ();

    fn name(&self) -> &'static str {
        "artefact-probe"
    }

    fn env_prefix(&self) -> &'static str {
        "ARTEFACT_PROBE"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("artefact-probe", "1.2.3")
    }

    fn common_args(&self) -> &CommonArgs {
        &self.cli.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        self.cli.command.as_ref()
    }

    /// Installs the cascade, then reads it whole, which is where a settings
    /// file that is not YAML fails.
    fn load_config(&self, path: Option<&str>) -> Result<(), CliError> {
        let dir =
            path.ok_or_else(|| CliError::Config("the probe is always given --config".into()))?;
        match scalo::config::setup(ConfigOptions {
            config_paths: vec![dir.into()],
            ..ConfigOptions::default()
        }) {
            Ok(()) | Err(ConfigError::AlreadyInitialised) => {}
            Err(e) => return Err(CliError::Config(e.to_string())),
        }
        scalo::config::get()
            .unmarshal::<serde_json::Value>()
            .map(drop)
            .map_err(|e| CliError::Config(e.to_string()))
    }

    fn run_service(
        &self,
        _config: (),
        _runtime: ServiceRuntime,
    ) -> impl Future<Output = Result<(), CliError>> + Send {
        std::future::ready(Ok(()))
    }

    fn deployment_contract(&self) -> Option<DeploymentContract> {
        Some(DeploymentContract {
            schema_version: 3,
            app_name: "artefact-probe".into(),
            binary_name: String::new(),
            description: String::new(),
            metrics_port: 9090,
            health: HealthContract::default(),
            env_prefix: "ARTEFACT_PROBE".into(),
            metric_prefix: String::new(),
            config_mount_path: "/etc/artefact-probe/config.yaml".into(),
            image_registry: "ghcr.io/example".into(),
            extra_ports: vec![],
            unbound_listen_paths: vec![],
            entrypoint_args: vec![],
            secrets: vec![],
            default_config: None,
            depends_on: vec![],
            keda: None,
            base_image: base_image_from_cascade(),
            native_deps: NativeDepsContract::default(),
            image_profile: ImageProfile::Production,
            oci_labels: OciLabels::default(),
            config_schema: None,
            capabilities: vec![],
        })
    }
}

/// In the re-run, run the probe on the arguments it was handed and say so;
/// anywhere else, do nothing.
fn ran_as_probe() -> bool {
    let Ok(args) = std::env::var(PROBE_ARGS) else {
        return false;
    };
    let command_line = std::iter::once("artefact-probe").chain(args.lines());
    let app = ProbeApp {
        cli: ProbeCli::try_parse_from(command_line).expect("the probe's own arguments parse"),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime for the probe")
        .block_on(scalo::cli::run_app(app))
        .expect("the command succeeds");
    true
}

/// What one probe process wrote to its two streams.
struct Streams {
    stdout: String,
    stderr: String,
}

/// Re-run this test binary as the probe, on `config` then `args`.
fn probe(test: &str, config: &Path, args: &[&str]) -> Streams {
    let command_line: Vec<&str> = ["--config", path_str(config)]
        .into_iter()
        .chain(args.iter().copied())
        .collect();
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(PROBE_ARGS, command_line.join("\n"))
        .output()
        .expect("the test binary re-runs");
    let streams = Streams {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    };
    assert!(
        output.status.success(),
        "the probe failed:\n{}\n{}",
        streams.stdout,
        streams.stderr
    );
    streams
}

/// The pretty JSON document in a probe's stdout, from its opening brace to its
/// closing line, with the trailing newline an artefact file ends in.
///
/// The harness prints the test's name on the line the document starts on, so
/// the document is found by its first brace rather than by a line of its own.
fn printed_json(stdout: &str) -> String {
    let start = stdout
        .find('{')
        .unwrap_or_else(|| panic!("no JSON document on stdout:\n{stdout}"));
    let mut json = String::new();
    for line in stdout[start..].lines() {
        json.push_str(line);
        json.push('\n');
        if line == "}" {
            return json;
        }
    }
    panic!("an unterminated JSON document on stdout:\n{stdout}");
}

fn settings_dir(settings: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("config tempdir");
    std::fs::write(dir.path().join("settings.yaml"), settings).expect("write settings.yaml");
    dir
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("a UTF-8 tempdir")
}

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// `metrics-manifest` prints exactly the file `generate-artefacts` writes, and
/// a second run prints the same bytes: nothing in it is stamped per run.
#[test]
fn metrics_manifest_prints_what_generate_artefacts_writes() {
    const TEST: &str = "metrics_manifest_prints_what_generate_artefacts_writes";
    if ran_as_probe() {
        return;
    }
    let config = settings_dir("metrics:\n  namespace: probe\n");
    let out = tempfile::tempdir().expect("artefact tempdir");

    let first = printed_json(&probe(TEST, config.path(), &["metrics-manifest"]).stdout);
    let second = printed_json(&probe(TEST, config.path(), &["metrics-manifest"]).stdout);
    probe(
        TEST,
        config.path(),
        &["generate-artefacts", "--output-dir", path_str(out.path())],
    );
    let written = read(out.path(), "metrics-manifest.json");

    assert_eq!(first, second, "two runs printed different manifests");
    assert_eq!(first, written, "stdout and the written file differ");
    let manifest: serde_json::Value = serde_json::from_str(&written).expect("JSON");
    assert_eq!(
        manifest["registered_at"], "",
        "an offline manifest is not stamped"
    );
    assert_eq!(manifest["namespace"], "probe", "the cascade was read");
}

/// The contract is built after the config loads, so what it reads from the
/// cascade is the service's config rather than the defaults.
#[test]
fn the_contract_follows_the_config_cascade() {
    const TEST: &str = "the_contract_follows_the_config_cascade";
    if ran_as_probe() {
        return;
    }
    let config = settings_dir(&format!(
        "deployment:\n  base_image: \"{CONFIGURED_BASE_IMAGE}\"\n"
    ));
    let out = tempfile::tempdir().expect("artefact tempdir");

    probe(
        TEST,
        config.path(),
        &["generate-artefacts", "--output-dir", path_str(out.path())],
    );

    let contract: serde_json::Value =
        serde_json::from_str(&read(out.path(), "deployment-contract.json")).expect("JSON");
    assert_eq!(contract["base_image"], CONFIGURED_BASE_IMAGE);
    assert!(
        read(out.path(), "Dockerfile.runtime").contains(CONFIGURED_BASE_IMAGE),
        "the runtime stage builds on the configured base"
    );
}

/// A config that does not load is named on stderr, and every artefact is still
/// written from the defaults.
#[test]
fn a_config_that_does_not_load_is_warned_about_and_artefacts_still_written() {
    const TEST: &str = "a_config_that_does_not_load_is_warned_about_and_artefacts_still_written";
    if ran_as_probe() {
        return;
    }
    let config = settings_dir("deployment: [unclosed\n");
    let out = tempfile::tempdir().expect("artefact tempdir");

    let streams = probe(
        TEST,
        config.path(),
        &["generate-artefacts", "--output-dir", path_str(out.path())],
    );

    assert!(
        streams.stderr.contains("[warn] config did not load"),
        "no warning on stderr:\n{}",
        streams.stderr
    );
    assert_eq!(
        streams.stderr.matches("config did not load").count(),
        1,
        "the config is loaded once per command:\n{}",
        streams.stderr
    );
    for name in [
        "metrics-manifest.json",
        "deployment-contract.json",
        "container-manifest.json",
        "Dockerfile.runtime",
    ] {
        assert!(out.path().join(name).is_file(), "{name} not written");
    }
    let contract: serde_json::Value =
        serde_json::from_str(&read(out.path(), "deployment-contract.json")).expect("JSON");
    assert_eq!(contract["base_image"], DEFAULT_BASE_IMAGE);
}

/// With no repo named for it, the ArgoCD Application is not written and the
/// reason is on stderr; every other artefact still is.
#[test]
fn no_argocd_application_without_a_repo() {
    const TEST: &str = "no_argocd_application_without_a_repo";
    if ran_as_probe() {
        return;
    }
    let config = settings_dir("metrics:\n  namespace: probe\n");
    let out = tempfile::tempdir().expect("artefact tempdir");

    let streams = probe(
        TEST,
        config.path(),
        &["generate-artefacts", "--output-dir", path_str(out.path())],
    );

    assert!(
        !out.path().join("argocd-application.yaml").exists(),
        "an Application was written with no repo to sync"
    );
    assert!(
        streams
            .stderr
            .contains("deployment.argocd.repo_url is not set"),
        "no warning on stderr:\n{}",
        streams.stderr
    );
    assert!(out.path().join("Dockerfile.runtime").is_file());
}

/// The repo and destination namespace the cascade names are the ones the
/// Application carries.
#[test]
fn the_argocd_application_follows_the_config_cascade() {
    const TEST: &str = "the_argocd_application_follows_the_config_cascade";
    if ran_as_probe() {
        return;
    }
    let config = settings_dir(
        "deployment:\n  argocd:\n    repo_url: https://git.example.com/team/artefact-probe\n    \
         dest_namespace: probes\n",
    );
    let out = tempfile::tempdir().expect("artefact tempdir");

    probe(
        TEST,
        config.path(),
        &["generate-artefacts", "--output-dir", path_str(out.path())],
    );

    let app = read(out.path(), "argocd-application.yaml");
    assert!(
        app.contains("repoURL: https://git.example.com/team/artefact-probe\n"),
        "{app}"
    );
    assert!(app.contains("    namespace: probes\n"), "{app}");
}
