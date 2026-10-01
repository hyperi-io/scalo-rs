// Project:   scalo
// File:      src/cli/args.rs
// Purpose:   Standard CLI arguments for data-plane services
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Common CLI arguments shared across all data-plane services.
//!
//! Use `#[command(flatten)]` to embed these in your application's Clap parser:
//!
//! ```rust,ignore
//! use clap::Parser;
//! use scalo::cli::CommonArgs;
//!
//! #[derive(Parser)]
//! struct App {
//!     #[command(flatten)]
//!     common: CommonArgs,
//! }
//! ```

/// Standard CLI arguments for data-plane services.
///
/// Provides the 80% of flags that every service needs:
/// config path, log level/format, metrics address, verbose/quiet modes.
///
/// Embed in your Clap parser with `#[command(flatten)]`.
#[derive(Debug, Clone, clap::Args)]
pub struct CommonArgs {
    /// Path to configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: Option<String>,

    /// Log level (trace, debug, info, warn, error).
    ///
    /// Unset here falls through to `logger.level` in the config cascade, then
    /// to `info`. Carrying no clap default is what makes that fall-through
    /// possible: a default is indistinguishable from an explicit flag.
    #[arg(short = 'l', long = "log-level", env = "LOG_LEVEL")]
    pub log_level: Option<String>,

    /// Log output format (json, text, auto).
    ///
    /// Unset or `auto` falls through to `LOG_FORMAT`, then `logger.format`,
    /// then derives: `json` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else
    /// `text` under a CI runner or when stderr is a terminal, else `json`.
    #[arg(long = "log-format", env = "LOG_FORMAT")]
    pub log_format: Option<String>,

    /// Metrics server bind address.
    ///
    /// Unset falls through to `metrics.address` in the config cascade, then
    /// to `0.0.0.0:9090`. Carrying no clap default is what makes that
    /// fall-through possible: a default is indistinguishable from an
    /// explicit flag.
    #[arg(long = "metrics-addr", env = "METRICS_ADDR")]
    pub metrics_addr: Option<String>,

    /// Enable verbose output (sets log level to debug).
    #[arg(short = 'v', long, conflicts_with = "quiet")]
    pub verbose: bool,

    /// Suppress all output except errors.
    #[arg(short = 'q', long, conflicts_with = "verbose")]
    pub quiet: bool,
}

impl CommonArgs {
    /// Hard-coded log level, used when neither the CLI, the environment, nor
    /// config supplies one.
    pub const DEFAULT_LOG_LEVEL: &'static str = "info";

    /// Hard-coded log format, used on the same terms.
    ///
    /// An unset format resolves exactly as `auto` does, so
    /// [`effective_log_format`](Self::effective_log_format) returns the
    /// concrete `json` or `text` it derives rather than this value.
    pub const DEFAULT_LOG_FORMAT: &'static str = "auto";

    /// Hard-coded metrics bind address, used when neither the CLI, the
    /// environment, nor config supplies one.
    pub const DEFAULT_METRICS_ADDR: &'static str = "0.0.0.0:9090";

    /// Resolve the effective log level, accounting for --verbose and --quiet flags.
    ///
    /// Precedence is the config cascade's: `--verbose`/`--quiet`, then
    /// `--log-level` or `LOG_LEVEL`, then `logger.level` from config, then
    /// [`DEFAULT_LOG_LEVEL`](Self::DEFAULT_LOG_LEVEL).
    #[must_use]
    pub fn effective_log_level(&self) -> String {
        if self.verbose {
            return "debug".to_string();
        }
        if self.quiet {
            return "error".to_string();
        }
        if let Some(level) = &self.log_level {
            return level.clone();
        }
        #[cfg(feature = "logger")]
        if let Some(level) = crate::logger::LoggerSettings::from_cascade().level {
            return level;
        }
        Self::DEFAULT_LOG_LEVEL.to_string()
    }

    /// Resolve the effective log format.
    ///
    /// The first selector that is neither blank nor `auto` wins: `--log-format`,
    /// then `LOG_FORMAT`, then `logger.format` from config. A value that is not
    /// a known format is returned as given, so
    /// [`to_logger_options`](Self::to_logger_options) rejects it. With none
    /// set, `json` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else `text`
    /// under a CI runner or when stderr is a terminal, else `json`.
    #[must_use]
    pub fn effective_log_format(&self) -> String {
        self.effective_log_format_in(crate::env::ConsoleContext::detect())
    }

    /// [`effective_log_format`](Self::effective_log_format) with the console
    /// context supplied rather than detected.
    fn effective_log_format_in(&self, ctx: crate::env::ConsoleContext) -> String {
        let from_env = std::env::var("LOG_FORMAT").ok();
        if let Some(format) = [self.log_format.as_deref(), from_env.as_deref()]
            .into_iter()
            .find_map(concrete_format)
        {
            return format;
        }
        #[cfg(feature = "logger")]
        if let Some(format) = concrete_format(
            crate::logger::LoggerSettings::from_cascade()
                .format
                .as_deref(),
        ) {
            return format;
        }
        if ctx.wants_json() { "json" } else { "text" }.to_string()
    }

    /// The `json` / `text` choice from OTEL presence alone.
    ///
    /// [`effective_log_format`](Self::effective_log_format) applies this rule
    /// first, then the CI and terminal checks this function does not make.
    #[must_use]
    pub fn derive_log_format(otel_endpoint_set: bool) -> &'static str {
        if otel_endpoint_set { "json" } else { "text" }
    }

    /// Resolve the effective metrics bind address.
    ///
    /// `--metrics-addr` or `METRICS_ADDR`, then `metrics.address` from
    /// config, then [`DEFAULT_METRICS_ADDR`](Self::DEFAULT_METRICS_ADDR).
    #[must_use]
    pub fn effective_metrics_addr(&self) -> String {
        if let Some(addr) = &self.metrics_addr {
            return addr.clone();
        }
        #[cfg(feature = "metrics")]
        if let Some(addr) = crate::metrics::MetricsSettings::from_cascade().address {
            return addr;
        }
        Self::DEFAULT_METRICS_ADDR.to_string()
    }

    /// Convert to `LoggerOptions` for use with `logger::setup()`.
    ///
    /// Parses the log level and format strings into their typed equivalents.
    ///
    /// # Errors
    ///
    /// Returns `CliError::InvalidArgument` if the log level or format is invalid.
    #[cfg(feature = "logger")]
    pub fn to_logger_options(&self) -> Result<crate::logger::LoggerOptions, super::CliError> {
        use std::str::FromStr;

        let resolved_level = self.effective_log_level();
        let level: tracing::Level = resolved_level.to_uppercase().parse().map_err(|_| {
            super::CliError::InvalidArgument(format!("invalid log level: {resolved_level}"))
        })?;

        let format = crate::logger::LogFormat::from_str(&self.effective_log_format())
            .map_err(|e| super::CliError::InvalidArgument(format!("invalid log format: {e}")))?;

        Ok(crate::logger::LoggerOptions {
            level,
            format,
            ..Default::default()
        })
    }

    /// Convert to `ConfigOptions` for use with `config::setup()`.
    #[cfg(feature = "config")]
    #[must_use]
    pub fn to_config_options(&self, env_prefix: &str) -> crate::config::ConfigOptions {
        let mut opts = crate::config::ConfigOptions {
            env_prefix: env_prefix.to_string(),
            ..Default::default()
        };
        if let Some(ref path) = self.config {
            opts.config_paths.push(path.into());
        }
        opts
    }
}

/// The selector trimmed, or `None` when it is unset, blank or `auto` so the next one decides.
fn concrete_format(selector: Option<&str>) -> Option<String> {
    let selector = selector?.trim();
    (!selector.is_empty() && !selector.eq_ignore_ascii_case("auto")).then(|| selector.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::ConsoleContext;

    /// Args with nothing supplied on the command line.
    fn bare_args() -> CommonArgs {
        CommonArgs {
            config: None,
            log_level: None,
            log_format: None,
            metrics_addr: None,
            verbose: false,
            quiet: false,
        }
    }

    #[test]
    fn test_effective_log_level_default() {
        assert_eq!(
            bare_args().effective_log_level(),
            CommonArgs::DEFAULT_LOG_LEVEL
        );
    }

    #[test]
    fn test_effective_log_level_verbose() {
        let args = CommonArgs {
            verbose: true,
            ..bare_args()
        };
        assert_eq!(args.effective_log_level(), "debug");
    }

    #[test]
    fn test_effective_log_level_quiet() {
        let args = CommonArgs {
            quiet: true,
            ..bare_args()
        };
        assert_eq!(args.effective_log_level(), "error");
    }

    #[test]
    fn test_effective_log_level_custom() {
        let args = CommonArgs {
            log_level: Some("warn".to_string()),
            ..bare_args()
        };
        assert_eq!(args.effective_log_level(), "warn");
    }

    #[test]
    fn verbose_and_quiet_outrank_an_explicit_level() {
        let args = CommonArgs {
            log_level: Some("warn".to_string()),
            verbose: true,
            ..bare_args()
        };
        assert_eq!(
            args.effective_log_level(),
            "debug",
            "--verbose must win over --log-level"
        );
    }

    #[test]
    fn explicit_format_flag_wins() {
        let args = CommonArgs {
            log_format: Some("json".to_string()),
            ..bare_args()
        };
        assert_eq!(args.effective_log_format(), "json");
    }

    #[test]
    fn format_derives_from_otel_presence() {
        // The deployment seam: shipping telemetry -> json, not shipping -> lines.
        assert_eq!(CommonArgs::derive_log_format(true), "json");
        assert_eq!(CommonArgs::derive_log_format(false), "text");
    }

    #[test]
    fn unset_format_resolves_to_a_derived_value_not_auto() {
        temp_env::with_var("LOG_FORMAT", None::<&str>, || {
            let resolved = bare_args().effective_log_format();
            assert!(
                resolved == "json" || resolved == "text",
                "expected a derived concrete format, got {resolved}"
            );
        });
    }

    /// Nothing set: no OTEL endpoint, no CI runner, stderr not a terminal.
    const PIPED: ConsoleContext = ConsoleContext {
        otel_endpoint: false,
        ci: false,
        tty: false,
    };

    fn with_format(format: Option<&str>) -> CommonArgs {
        CommonArgs {
            log_format: format.map(str::to_string),
            ..bare_args()
        }
    }

    #[test]
    fn an_explicit_format_outranks_log_format() {
        temp_env::with_var("LOG_FORMAT", Some("text"), || {
            assert_eq!(
                with_format(Some("json")).effective_log_format_in(PIPED),
                "json"
            );
            assert_eq!(
                with_format(Some("pretty")).effective_log_format_in(PIPED),
                "pretty"
            );
        });
    }

    #[test]
    fn an_explicit_auto_defers_to_log_format() {
        for env in ["json", "text"] {
            temp_env::with_var("LOG_FORMAT", Some(env), || {
                assert_eq!(
                    with_format(Some("auto")).effective_log_format_in(PIPED),
                    env
                );
            });
        }
    }

    #[test]
    fn blank_and_auto_defer_to_the_derived_format() {
        for selector in ["auto", "AUTO", "", "  "] {
            temp_env::with_var("LOG_FORMAT", Some(selector), || {
                assert_eq!(
                    with_format(Some(selector)).effective_log_format_in(PIPED),
                    "json",
                    "{selector:?} on the flag and in LOG_FORMAT"
                );
            });
        }
    }

    #[test]
    fn unset_format_derives_from_otel_then_ci_then_the_terminal() {
        temp_env::with_var("LOG_FORMAT", None::<&str>, || {
            let unset = bare_args();
            assert_eq!(unset.effective_log_format_in(PIPED), "json", "piped");
            let tty = ConsoleContext { tty: true, ..PIPED };
            assert_eq!(unset.effective_log_format_in(tty), "text", "terminal");
            let ci = ConsoleContext { ci: true, ..PIPED };
            assert_eq!(unset.effective_log_format_in(ci), "text", "CI, piped");
            let otel = ConsoleContext {
                otel_endpoint: true,
                ci: true,
                tty: true,
            };
            assert_eq!(unset.effective_log_format_in(otel), "json", "OTEL on a TTY");
        });
    }

    #[cfg(feature = "logger")]
    #[test]
    fn an_unknown_format_still_fails() {
        temp_env::with_var("LOG_FORMAT", None::<&str>, || {
            assert!(with_format(Some("yaml")).to_logger_options().is_err());
        });
        temp_env::with_var("LOG_FORMAT", Some("yaml"), || {
            assert!(with_format(Some("auto")).to_logger_options().is_err());
        });
    }

    #[test]
    fn metrics_addr_falls_back_to_the_hard_coded_default() {
        assert_eq!(
            bare_args().effective_metrics_addr(),
            CommonArgs::DEFAULT_METRICS_ADDR
        );
        let args = CommonArgs {
            metrics_addr: Some("127.0.0.1:9191".to_string()),
            ..bare_args()
        };
        assert_eq!(args.effective_metrics_addr(), "127.0.0.1:9191");
    }
}
