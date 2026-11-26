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
    /// `my-service generate-artefacts --output-dir docs/`
    #[command(name = "generate-artefacts")]
    GenerateArtefacts(GenerateArtefactsArgs),

    /// Emit the reflectable config artefacts and exit.
    ///
    /// Writes `config-schema.{json,yaml}` + `capability-catalog.{json,yaml}`
    /// derived from the app's `Config` and capability catalog into the target
    /// directory (scalo-rs#6). Also produced by `generate-artefacts`. Use to
    /// refresh the committed contract. A drift test then keeps it honest:
    /// `my-service config-schema --dir docs/`
    #[command(name = "config-schema")]
    ConfigSchema(ConfigSchemaArgs),

    /// Measure the service's memory under the standard load and exit.
    ///
    /// Runs the app's `ServiceApp::bench_pipeline` between an in-memory source
    /// offering the load and a sink that discards what arrives, samples the
    /// process's memory over the window, and writes the profile JSON that
    /// `generate-artefacts --memory-profile` puts in the contract. Needs the
    /// `bench-memory` feature, and exits non-zero for an app with no bench
    /// pipeline: `my-service bench-memory --output ci/memory-profile.json`
    #[command(name = "bench-memory")]
    BenchMemory(BenchMemoryArgs),

    /// Live metrics dashboard in the terminal: a sortable table polled from the
    /// service's Prometheus `/metrics` endpoint.
    #[cfg(feature = "top")]
    Top(TopArgs),
}

/// Arguments for the `generate-artefacts` subcommand.
#[derive(Debug, Clone, clap::Args)]
pub struct GenerateArtefactsArgs {
    /// Output directory for generated artefacts.
    #[arg(long = "output-dir", default_value = "docs")]
    pub output_dir: String,

    /// A profile `bench-memory` wrote, put in the contract as its `memory`
    /// block. Without it the contract has none.
    #[arg(long = "memory-profile")]
    pub memory_profile: Option<String>,
}

/// Arguments for the `bench-memory` subcommand. The defaults are the standard
/// load, and the profile records whatever a run used.
#[derive(Debug, Clone, clap::Args)]
pub struct BenchMemoryArgs {
    /// JSON records a second offered to the pipeline, across all sources.
    #[arg(
        long = "records-per-second",
        default_value_t = 5000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub records_per_second: u64,

    /// Bytes in each record, 16 to 16 MiB.
    #[arg(
        long = "record-bytes",
        default_value_t = 1024,
        value_parser = clap::value_parser!(u64).range(16..=16_777_216)
    )]
    pub record_bytes: u64,

    /// Producers feeding the source at once, each offering an even share of
    /// the rate. No more than the records a second.
    #[arg(
        long = "sources",
        default_value_t = 1,
        value_parser = clap::value_parser!(u32).range(1..=1024)
    )]
    pub sources: u32,

    /// Seconds the load runs before the first sample.
    #[arg(long = "warmup-seconds", default_value_t = 30)]
    pub warmup_seconds: u32,

    /// Seconds the samples span.
    #[arg(
        long = "window-seconds",
        default_value_t = 120,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub window_seconds: u32,

    /// Milliseconds between samples, no more than the window.
    #[arg(
        long = "sample-interval-ms",
        default_value_t = 1000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub sample_interval_ms: u64,

    /// File the profile JSON is written to.
    #[arg(long = "output", default_value = "memory-profile.json")]
    pub output: String,
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
