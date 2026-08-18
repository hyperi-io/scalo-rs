// Project:   scalo
// File:      tests/logger_cascade_level.rs
// Purpose:   Log level resolved from the config cascade
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The log level an app runs at can come from its settings file.
//!
//! Only reachable because config now loads before the logger. The precedence
//! is the cascade's: `--verbose`/`--quiet`, then `--log-level` / `LOG_LEVEL`,
//! then `logger.level`, then the hard-coded default.
//!
//! The global config installs once per process, so this file owns it.

#![cfg(all(feature = "cli", feature = "config", feature = "logger"))]

use scalo::cli::CommonArgs;

fn bare_args() -> CommonArgs {
    CommonArgs {
        config: None,
        log_level: None,
        log_format: None,
        metrics_addr: "0.0.0.0:9090".to_string(),
        verbose: false,
        quiet: false,
    }
}

#[test]
fn settings_yaml_supplies_the_level_and_the_cli_still_outranks_it() {
    let dir = tempfile::tempdir().expect("config tempdir");
    std::fs::write(
        dir.path().join("settings.yaml"),
        "logger:\n  level: warn\n  format: json\n",
    )
    .expect("write settings.yaml");

    temp_env::with_vars(
        [
            ("LOG_LEVEL", None::<&str>),
            ("LOG_FORMAT", None::<&str>),
            ("APP_ENV", Some("test")),
        ],
        || {
            scalo::config::setup(scalo::config::ConfigOptions {
                config_paths: vec![dir.path().to_path_buf()],
                ..scalo::config::ConfigOptions::default()
            })
            .expect("config setup");

            assert_eq!(
                bare_args().effective_log_level(),
                "warn",
                "logger.level from settings.yaml must beat the hard-coded default"
            );
            assert_eq!(
                bare_args().effective_log_format(),
                "json",
                "logger.format from settings.yaml must beat the hard-coded default"
            );

            let explicit = CommonArgs {
                log_level: Some("error".to_string()),
                ..bare_args()
            };
            assert_eq!(
                explicit.effective_log_level(),
                "error",
                "an explicit --log-level must outrank config"
            );

            let verbose = CommonArgs {
                verbose: true,
                ..bare_args()
            };
            assert_eq!(
                verbose.effective_log_level(),
                "debug",
                "--verbose must outrank config"
            );
        },
    );
}
