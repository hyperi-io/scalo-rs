// Project:   scalo
// File:      src/cli/commands.rs
// Purpose:   Standard CLI subcommands for data-plane services
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Standard subcommands shared across all data-plane services.
//!
//! Every data-plane service gets `run`, `version`, and `config-check` for free.
//! The `top` subcommand is available when the `top` feature is enabled.

/// Standard subcommands provided by scalo.
///
/// Apps embed these via `#[command(flatten)]` in their own subcommand enum:
///
/// ```rust,ignore
/// use clap::Subcommand;
/// use scalo::cli::StandardCommand;
///
/// #[derive(Subcommand)]
/// enum Commands {
///     #[command(flatten)]
///     Standard(StandardCommand),
///     // App-specific subcommands here
/// }
/// ```
#[derive(Debug, Clone, clap::Subcommand)]
pub enum StandardCommand {
    /// Start the service (default if no subcommand given).
    Run,

    /// Print version information and exit.
    Version,

    /// Validate configuration and exit.
    #[command(name = "config-check")]
    ConfigCheck,

    /// Print metrics manifest JSON and exit.
    ///
    /// Outputs the full metric catalogue (names, types, labels, groups, buckets)
    /// for this service. Use in CI to generate `docs/metrics-manifest.json`.
    #[command(name = "metrics-manifest")]
    MetricsManifest,

    /// Generate all CI artefacts and exit.
    ///
    /// Produces metrics manifest, deployment contract, and container spec
    /// in the specified output directory. Use in CI post-build:
    /// `dfe-loader generate-artefacts --output-dir docs/`
    #[command(name = "generate-artefacts")]
    GenerateArtefacts(GenerateArtefactsArgs),

    /// Emit the reflectable config artefacts and exit.
    ///
    /// Writes `config-schema.{json,yaml}` + `capability-catalog.{json,yaml}`
    /// derived from the app's `Config` and capability catalog into the target
    /// directory (scalo-rs#6). Also produced by `generate-artefacts`. Use to
    /// refresh the committed contract; a drift test then keeps it honest:
    /// `dfe-loader config-schema --dir docs/`
    #[command(name = "config-schema")]
    ConfigSchema(ConfigSchemaArgs),

    /// Live metrics dashboard (like `vector top`).
    #[cfg(feature = "top")]
    Top(TopArgs),
}

/// Arguments for the `generate-artefacts` subcommand.
#[derive(Debug, Clone, clap::Args)]
pub struct GenerateArtefactsArgs {
    /// Output directory for generated artefacts.
    #[arg(long = "output-dir", default_value = "docs")]
    pub output_dir: String,
}

/// Arguments for the `config-schema` subcommand.
#[derive(Debug, Clone, clap::Args)]
pub struct ConfigSchemaArgs {
    /// Output directory for the config artefacts (`config-schema.*`,
    /// `capability-catalog.*`).
    #[arg(long = "dir", default_value = "docs")]
    pub dir: String,
}

/// Arguments for the `top` subcommand.
#[cfg(feature = "top")]
#[derive(Debug, Clone, clap::Args)]
pub struct TopArgs {
    /// Metrics endpoint URL to poll.
    #[arg(
        long = "metrics-url",
        env = "METRICS_URL",
        default_value = "http://127.0.0.1:9090/metrics"
    )]
    pub metrics_url: String,

    /// Poll interval in seconds.
    #[arg(long = "interval", default_value = "2")]
    pub interval: u64,

    /// Single scrape: print metrics to stdout and exit (no TUI).
    #[arg(long = "once")]
    pub once: bool,

    /// Output as JSON (implies --once).
    #[arg(long = "json")]
    pub json: bool,

    /// Filter metrics by name (regex pattern).
    #[arg(long = "filter", short = 'f')]
    pub filter: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_command_variants() {
        // Verify the enum variants exist and are constructible
        let _ = StandardCommand::Run;
        let _ = StandardCommand::Version;
        let _ = StandardCommand::ConfigCheck;
    }
}
