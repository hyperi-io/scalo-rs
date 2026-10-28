// Project:   scalo
// File:      tests/crypto_provider.rs
// Purpose:   install_default_provider, called from main, keeps server TLS working
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service built with both rustls providers linked, whose `main` calls
//! [`scalo::tls::install_default_provider`] before `run_app`, builds a server
//! TLS config. Without the call it panics the way an unconfigured service
//! does, and a provider installed before the call is kept.
//!
//! The process default is process-global, so each test re-runs this binary
//! as a probe service in a process of its own. The test build links ring
//! through the `testcontainers` dev-dependency and aws-lc-rs through scalo,
//! the pair that leaves rustls with no default of its own.

#![cfg(all(feature = "cli-service", feature = "tls"))]

use std::process::Command;

use clap::Parser;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use scalo::cli::{CliError, CommonArgs, ServiceApp, ServiceRuntime, StandardCommand, VersionInfo};

/// Set only in the re-run, to what the probe's `main` does before `run_app`:
/// `nothing`, `helper`, or `ring-then-helper`.
const PROBE_MAIN: &str = "CRYPTO_PROVIDER_PROBE_MAIN";

/// The probe prints this, then the key provider its server config was built
/// with.
const MARKER: &str = "server config built with key provider ";

#[derive(Parser)]
#[command(name = "crypto-provider-probe")]
struct ProbeCli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Option<StandardCommand>,
}

/// A service that builds a rustls server config the way a TLS listener does.
struct ProbeApp {
    cli: ProbeCli,
}

impl ServiceApp for ProbeApp {
    type Config = ();

    fn name(&self) -> &'static str {
        "crypto-provider-probe"
    }

    fn env_prefix(&self) -> &'static str {
        "CRYPTO_PROVIDER_PROBE"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("crypto-provider-probe", "1.2.3")
    }

    fn common_args(&self) -> &CommonArgs {
        &self.cli.common
    }

    fn command(&self) -> Option<&StandardCommand> {
        self.cli.command.as_ref()
    }

    fn load_config(&self, _path: Option<&str>) -> Result<(), CliError> {
        Ok(())
    }

    fn run_service(
        &self,
        _config: (),
        _runtime: ServiceRuntime,
    ) -> impl Future<Output = Result<(), CliError>> + Send {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("a self-signed certificate");
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        ));
        let built = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certified.cert.der().clone()], key)
            .map(|config| {
                println!("{MARKER}{:?}", config.crypto_provider().key_provider);
            })
            .map_err(|e| CliError::Service(format!("server config: {e}")));
        std::future::ready(built)
    }
}

/// In the re-run, do what the probe's `main` is set to, run the service and
/// return true. Anywhere else, do nothing.
fn ran_as_probe() -> bool {
    let Ok(main) = std::env::var(PROBE_MAIN) else {
        return false;
    };
    assert!(
        CryptoProvider::get_default().is_none(),
        "a fresh process has no default until something installs one"
    );
    match main.as_str() {
        "nothing" => {}
        "helper" => assert!(scalo::tls::install_default_provider(), "installs aws-lc-rs"),
        "ring-then-helper" => {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("the first install in the process");
            assert!(
                !scalo::tls::install_default_provider(),
                "an installed provider is kept"
            );
        }
        other => panic!("unknown probe main {other}"),
    }
    let command_line = [
        "crypto-provider-probe",
        "--metrics-addr",
        "127.0.0.1:0",
        "run",
    ];
    let app = ProbeApp {
        cli: ProbeCli::try_parse_from(command_line).expect("the probe's own arguments parse"),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime for the probe")
        .block_on(scalo::cli::run_app(app))
        .expect("the service runs");
    true
}

/// Re-run this test binary as the probe with `main`, and return its exit
/// status, stdout and stderr.
fn run_probe(test: &str, main: &str) -> (bool, String, String) {
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(PROBE_MAIN, main)
        .env_remove("RUST_LOG")
        .output()
        .expect("the test binary re-runs");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// The key provider the probe's server config was built with.
fn built_with(test: &str, main: &str) -> String {
    let (ok, stdout, stderr) = run_probe(test, main);
    assert!(ok, "the probe failed:\n{stdout}\n{stderr}");
    stdout
        .lines()
        .find_map(|line| line.split_once(MARKER))
        .map_or_else(
            || panic!("no server config line:\n{stdout}\n{stderr}"),
            |(_, provider)| provider.trim().to_owned(),
        )
}

/// Proves both providers are linked: without that, rustls would pick the one
/// it has and the tests below would pass with or without the helper.
#[test]
fn a_service_whose_main_installs_nothing_panics_building_a_server_config() {
    const TEST: &str = "a_service_whose_main_installs_nothing_panics_building_a_server_config";
    if ran_as_probe() {
        return;
    }
    let (ok, stdout, stderr) = run_probe(TEST, "nothing");
    assert!(!ok, "the probe built a server config:\n{stdout}");
    assert!(
        stderr.contains("Could not automatically determine the process-level CryptoProvider"),
        "{stderr}"
    );
}

#[test]
fn install_default_provider_in_main_lets_the_service_build_a_server_config() {
    const TEST: &str = "install_default_provider_in_main_lets_the_service_build_a_server_config";
    if ran_as_probe() {
        return;
    }
    assert_eq!(built_with(TEST, "helper"), "AwsLcRs");
}

#[test]
fn install_default_provider_keeps_a_provider_installed_before_it() {
    const TEST: &str = "install_default_provider_keeps_a_provider_installed_before_it";
    if ran_as_probe() {
        return;
    }
    assert_eq!(built_with(TEST, "ring-then-helper"), "Ring");
}
