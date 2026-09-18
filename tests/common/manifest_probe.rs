// Project:   scalo
// File:      tests/common/manifest_probe.rs
// Purpose:   A service app that reports the manifest from both run_app paths
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service app driven through the real `run_app`, once as
//! `generate-artefacts` and once as `run`, over one settings file.
//!
//! It overrides nothing but what a service must supply, so what the two paths
//! report is exactly what scalo itself describes. Config installs once per
//! process, so each namespace case lives in its own test file.

use std::path::Path;
use std::sync::{Arc, Mutex};

use clap::Parser;
use scalo::cli::{CliError, CommonArgs, ServiceApp, ServiceRuntime, StandardCommand, VersionInfo};
use scalo::config::{ConfigError, ConfigOptions};
use scalo::metrics::ManifestResponse;

pub const APP_NAME: &str = "manifest-probe";

#[derive(Parser)]
#[command(name = "manifest-probe")]
struct ProbeCli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<StandardCommand>,
}

struct ProbeApp {
    cli: ProbeCli,
    /// What the running service's own registry held once the runtime was built.
    served: Arc<Mutex<Option<ManifestResponse>>>,
}

impl ServiceApp for ProbeApp {
    type Config = ();

    fn name(&self) -> &'static str {
        APP_NAME
    }

    fn env_prefix(&self) -> &'static str {
        "MANIFEST_PROBE"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new(APP_NAME, "1.2.3")
    }

    fn common_args(&self) -> &CommonArgs {
        &self.cli.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        self.cli.command.as_ref()
    }

    fn load_config(&self, path: Option<&str>) -> Result<(), CliError> {
        let dir =
            path.ok_or_else(|| CliError::Config("the probe is always given --config".into()))?;
        match scalo::config::setup(ConfigOptions {
            config_paths: vec![dir.into()],
            ..ConfigOptions::default()
        }) {
            Ok(()) | Err(ConfigError::AlreadyInitialised) => Ok(()),
            Err(e) => Err(CliError::Config(e.to_string())),
        }
    }

    fn run_service(
        &self,
        _config: (),
        runtime: ServiceRuntime,
    ) -> impl Future<Output = Result<(), CliError>> + Send {
        *self.served.lock().unwrap() = Some(runtime.metrics.registry().manifest());
        std::future::ready(Ok(()))
    }
}

/// What each path produced over one settings file.
pub struct Manifests {
    /// `generate-artefacts`'s `metrics-manifest.json`, byte for byte.
    pub written: String,
    /// The same file, parsed.
    pub generated: ManifestResponse,
    /// The registry of the service `run` built.
    pub served: ManifestResponse,
}

/// Write `settings` as the service's `settings.yaml`, then run the service's
/// `generate-artefacts` and `run` over it.
pub async fn manifests_for(settings: &str) -> Manifests {
    let config = tempfile::tempdir().expect("config tempdir");
    std::fs::write(config.path().join("settings.yaml"), settings).expect("write settings.yaml");
    let out = tempfile::tempdir().expect("artefact tempdir");
    let served = Arc::new(Mutex::new(None));

    run(
        &served,
        config.path(),
        &["generate-artefacts", "--output-dir", path_str(out.path())],
    )
    .await;
    run(
        &served,
        config.path(),
        &["--metrics-addr", "127.0.0.1:0", "run"],
    )
    .await;

    let written = std::fs::read_to_string(out.path().join("metrics-manifest.json"))
        .expect("generate-artefacts wrote a metrics manifest");
    let generated = serde_json::from_str(&written).expect("the manifest is JSON");
    let served = served
        .lock()
        .unwrap()
        .take()
        .expect("run reached run_service");
    Manifests {
        written,
        generated,
        served,
    }
}

async fn run(served: &Arc<Mutex<Option<ManifestResponse>>>, config: &Path, args: &[&str]) {
    let command_line = ["manifest-probe", "--config", path_str(config)]
        .into_iter()
        .chain(args.iter().copied());
    let app = ProbeApp {
        cli: ProbeCli::try_parse_from(command_line).expect("the probe's own arguments parse"),
        served: Arc::clone(served),
    };
    scalo::cli::run_app(app)
        .await
        .expect("the command succeeds");
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("a UTF-8 tempdir")
}

/// The names in a manifest, sorted.
pub fn names(manifest: &ManifestResponse) -> Vec<String> {
    let mut names: Vec<String> = manifest.metrics.iter().map(|m| m.name.clone()).collect();
    names.sort();
    names
}

/// Names that appear more than once in a manifest.
pub fn repeated(manifest: &ManifestResponse) -> Vec<String> {
    let mut sorted = names(manifest);
    sorted.dedup();
    let mut seen = names(manifest);
    for name in &sorted {
        if let Some(at) = seen.iter().position(|n| n == name) {
            seen.remove(at);
        }
    }
    seen
}
