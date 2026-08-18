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
    /// Unset falls through to `logger.format`, then to `auto`.
    #[arg(long = "log-format", env = "LOG_FORMAT")]
    pub log_format: Option<String>,

    /// Metrics server bind address.
    #[arg(
        long = "metrics-addr",
        env = "METRICS_ADDR",
        default_value = "0.0.0.0:9090"
    )]
    pub metrics_addr: String,

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
    pub const DEFAULT_LOG_FORMAT: &'static str = "auto";

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
    /// `--log-format` or `LOG_FORMAT`, then `logger.format` from config, then
    /// [`DEFAULT_LOG_FORMAT`](Self::DEFAULT_LOG_FORMAT).
    #[must_use]
    pub fn effective_log_format(&self) -> String {
        if let Some(format) = &self.log_format {
            return format.clone();
        }
        #[cfg(feature = "logger")]
        if let Some(format) = crate::logger::LoggerSettings::from_cascade().format {
            return format;
        }
        Self::DEFAULT_LOG_FORMAT.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Args with nothing supplied on the command line.
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
    fn format_falls_back_to_the_hard_coded_default() {
        assert_eq!(
            bare_args().effective_log_format(),
            CommonArgs::DEFAULT_LOG_FORMAT
        );
        let args = CommonArgs {
            log_format: Some("json".to_string()),
            ..bare_args()
        };
        assert_eq!(args.effective_log_format(), "json");
    }
}
