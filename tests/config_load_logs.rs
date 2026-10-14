// Project:   scalo
// File:      tests/config_load_logs.rs
// Purpose:   What load_typed logs about a config file reaches a service's log
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `run_app` loads the config before it sets up the logger, so anything
//! `load_typed` logs about the file happens with no subscriber. These tests
//! run a service the way its binary runs, in a process of its own, and read
//! the log off its stderr.
//!
//! The test binary re-runs itself as that process. A test finding
//! [`PROBE_ARGS`] in its environment is the re-run: it runs the probe service
//! on those arguments and returns.

#![cfg(all(feature = "cli-service", feature = "config", feature = "logger"))]

use std::path::Path;
use std::process::Command;

use clap::Parser;
use scalo::cli::{CliError, CommonArgs, ServiceApp, ServiceRuntime, StandardCommand, VersionInfo};
use scalo::config::flat_env::{ApplyFlatEnv, Normalize};
use serde::{Deserialize, Serialize};

/// The probe's command line, one argument per line, set only in the re-run.
const PROBE_ARGS: &str = "CONFIG_LOG_PROBE_ARGS";

/// A config with a misspelt nested key, which no field of [`ProbeConfig`] takes.
const CONFIG: &str = "kafka:\n  brokers: [kafka:9092]\n  brokerz: [kafka:9093]\n";

const WARNING: &str = "unknown config key ignored";

#[derive(Parser)]
#[command(name = "config-log-probe")]
struct ProbeCli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<StandardCommand>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct ProbeConfig {
    kafka: Kafka,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct Kafka {
    brokers: Vec<String>,
}

impl ApplyFlatEnv for ProbeConfig {
    fn apply_flat_env(&mut self, _prefix: &str) {}
}

impl Normalize for ProbeConfig {}

/// A service whose config loads through `load_typed`, as a real one's does.
struct ProbeApp {
    cli: ProbeCli,
}

impl ServiceApp for ProbeApp {
    type Config = ProbeConfig;

    fn name(&self) -> &'static str {
        "config-log-probe"
    }

    fn env_prefix(&self) -> &'static str {
        "CONFIG_LOG_PROBE"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("config-log-probe", "1.2.3")
    }

    fn common_args(&self) -> &CommonArgs {
        &self.cli.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        self.cli.command.as_ref()
    }

    fn load_config(&self, path: Option<&str>) -> Result<ProbeConfig, CliError> {
        scalo::config::load_typed(path, self.env_prefix()).map_err(CliError::from)
    }

    fn run_service(
        &self,
        _config: ProbeConfig,
        _runtime: ServiceRuntime,
    ) -> impl Future<Output = Result<(), CliError>> + Send {
        std::future::ready(Ok(()))
    }
}

/// In the re-run, run the probe on the arguments it was handed and return
/// true. Anywhere else, do nothing.
fn ran_as_probe() -> bool {
    let Ok(args) = std::env::var(PROBE_ARGS) else {
        return false;
    };
    let command_line = std::iter::once("config-log-probe").chain(args.lines());
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

/// Re-run this test binary as the probe on `config` then `args`, and return
/// its stderr, where the logger writes.
fn probe_stderr(test: &str, config: &Path, args: &[&str]) -> String {
    let command_line: Vec<&str> = ["--config", config.to_str().expect("a UTF-8 path")]
        .into_iter()
        .chain(args.iter().copied())
        .collect();
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(PROBE_ARGS, command_line.join("\n"))
        .env("LOG_FORMAT", "json")
        .env_remove("LOG_LEVEL")
        .env_remove("RUST_LOG")
        .output()
        .expect("the test binary re-runs");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "the probe failed:\n{stdout}\n{stderr}"
    );
    stderr
}

fn config_file() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("config tempdir");
    let path = dir.path().join("config-log-probe.yaml");
    std::fs::write(&path, CONFIG).expect("write the config");
    (dir, path)
}

/// The one log line naming `key`, asserted to appear exactly once.
fn the_warning_for(stderr: &str, key: &str) -> serde_json::Value {
    let lines: Vec<serde_json::Value> = stderr
        .lines()
        .filter(|line| line.contains(WARNING))
        .map(|line| serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}")))
        .collect();
    assert_eq!(lines.len(), 1, "one warning, logged once:\n{stderr}");
    let line = lines.into_iter().next().unwrap_or_default();
    assert_eq!(line["fields"]["key"], key, "{line}");
    line
}

/// `run` sets up the logger after the config loads, and the warning about the
/// misspelt key is in the service's log, at WARN, naming the file.
#[test]
fn run_logs_an_unknown_key_found_before_the_logger_existed() {
    const TEST: &str = "run_logs_an_unknown_key_found_before_the_logger_existed";
    if ran_as_probe() {
        return;
    }
    let (_dir, path) = config_file();

    let stderr = probe_stderr(TEST, &path, &["--metrics-addr", "127.0.0.1:0", "run"]);

    let line = the_warning_for(&stderr, "kafka.brokerz");
    assert_eq!(line["level"], "WARN", "{line}");
    assert_eq!(
        line["fields"]["path"],
        path.to_str().expect("a UTF-8 path"),
        "{line}"
    );
}

/// `config-check` loads the config the same way, so it reports the same key.
#[test]
fn config_check_logs_an_unknown_key() {
    const TEST: &str = "config_check_logs_an_unknown_key";
    if ran_as_probe() {
        return;
    }
    let (_dir, path) = config_file();

    let stderr = probe_stderr(TEST, &path, &["config-check"]);

    the_warning_for(&stderr, "kafka.brokerz");
}
