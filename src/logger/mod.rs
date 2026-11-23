// Project:   scalo
// File:      src/logger/mod.rs
// Purpose:   Structured logging with JSON output and sensitive data masking
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Structured logging with JSON output and sensitive data masking.
//!
//! Provides production-ready logging matching scalo-py.
//! With no format set, picks JSON or text from the OTEL endpoint, a CI runner
//! and whether stderr is a terminal.
//!
//! ## Features
//!
//! - RFC 3339 timestamps with timezone
//! - JSON or human-readable text, coloured only on a terminal unless configured
//! - Sensitive data masking (passwords, tokens, API keys, URL passwords),
//!   judged by [`is_credential_name`]
//! - Environment variable overrides (LOG_LEVEL, LOG_FORMAT, LOG_COLOR, NO_COLOR)
//!
//! ## Example
//!
//! ```rust,no_run
//! use scalo::logger;
//!
//! // Initialise with defaults (auto-detects format)
//! logger::setup_default().unwrap();
//!
//! // Use tracing macros
//! tracing::info!(user_id = 123, "User logged in");
//! tracing::error!(error = "connection failed", "Database error");
//! ```

mod credential;
pub mod format;
pub mod helpers;
mod masking;
pub mod security;

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use thiserror::Error;
use tracing::Level;
use tracing_subscriber::Layer as _;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, reload};

use tracing_throttle::{Policy, TracingRateLimitLayer};

use crate::env::ConsoleContext;

pub use credential::{is_credential_name, mask_url_userinfo, url_passwords};
pub use helpers::{log_debounced, log_sampled, log_state_change};
pub use masking::{MaskingLayer, MaskingWriter, default_sensitive_fields, mask_sensitive_string};
pub use security::{SecurityEvent, SecurityOutcome};

/// Global flag to track initialisation.
static LOGGER_INIT: OnceLock<()> = OnceLock::new();

/// The level handle of the logger [`setup`] started.
static LEVEL_HANDLE: OnceLock<LevelHandle> = OnceLock::new();

/// Changes the level of a running logger.
///
/// The `rdkafka=warn` floor and the `RUST_LOG` directives read at [`setup`]
/// stay layered over the new level, as they are at start.
///
/// ```rust,no_run
/// scalo::logger::setup_default()?;
/// if let Some(level) = scalo::logger::level_handle() {
///     level.set("debug")?;
/// }
/// # Ok::<(), scalo::logger::LoggerError>(())
/// ```
#[derive(Clone)]
pub struct LevelHandle {
    filter: reload::Handle<EnvFilter, Registry>,
    rust_log: Option<String>,
    level: Arc<Mutex<Level>>,
}

impl LevelHandle {
    /// A filter at `level`, with `rust_log` layered over it, and the handle
    /// that changes it.
    fn new(level: Level, rust_log: Option<String>) -> (reload::Layer<EnvFilter, Registry>, Self) {
        let (filter, handle) = reload::Layer::new(env_filter(level, rust_log.as_deref()));
        let handle = Self {
            filter: handle,
            rust_log,
            level: Arc::new(Mutex::new(level)),
        };
        (filter, handle)
    }

    /// Run at `level`, written as `LOG_LEVEL` takes it: a `tracing` name or
    /// `1`-`5`, or `warning`, `critical` or `fatal`, in any case.
    ///
    /// # Errors
    ///
    /// [`LoggerError::InvalidLevel`] for any other text, and
    /// [`LoggerError::SetGlobalError`] when the subscriber is gone. Either
    /// leaves the level as it was.
    pub fn set(&self, level: &str) -> Result<(), LoggerError> {
        let wanted =
            parse_level(level).ok_or_else(|| LoggerError::InvalidLevel(level.to_string()))?;
        let mut current = self.level.lock().unwrap_or_else(PoisonError::into_inner);
        self.filter
            .reload(env_filter(wanted, self.rust_log.as_deref()))
            .map_err(|e| LoggerError::SetGlobalError(e.to_string()))?;
        let previous = std::mem::replace(&mut *current, wanted);
        drop(current);
        tracing::info!(from = %previous, to = %wanted, "log level changed");
        Ok(())
    }

    /// The level the logger runs at.
    #[must_use]
    pub fn current(&self) -> Level {
        *self.level.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for LevelHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelHandle")
            .field("level", &self.current())
            .field("rust_log", &self.rust_log)
            .finish_non_exhaustive()
    }
}

/// The handle on the level of the logger [`setup`] started, or `None` before
/// it has run.
#[must_use]
pub fn level_handle() -> Option<LevelHandle> {
    LEVEL_HANDLE.get().cloned()
}

/// The filter at `level`, with `rdkafka` held at warn and `rust_log` layered
/// over both.
///
/// rdkafka logs client lifecycle at info, which floods service logs. A later
/// directive wins per target, so `RUST_LOG=rdkafka=info` turns it back on.
fn env_filter(level: Level, rust_log: Option<&str>) -> EnvFilter {
    let quiet_defaults = format!("{level},rdkafka=warn");
    match rust_log {
        Some(rust_log) => EnvFilter::new(format!("{quiet_defaults},{rust_log}")),
        None => EnvFilter::new(quiet_defaults),
    }
}

/// Logger errors.
#[derive(Debug, Error)]
pub enum LoggerError {
    /// Logger already initialised.
    #[error("logger already initialised")]
    AlreadyInitialised,

    /// The `logger` or `otel_tracing` config section does not parse.
    #[error(transparent)]
    Config(#[from] crate::cascade::CascadeError),

    /// Failed to set global subscriber.
    #[error("failed to set global subscriber: {0}")]
    SetGlobalError(String),

    /// Invalid log level.
    #[error("invalid log level: {0}")]
    InvalidLevel(String),

    /// Invalid log format (expected json, text, or auto).
    #[error("invalid log format: {0}")]
    InvalidFormat(String),
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// JSON output (for containers/log aggregators).
    Json,
    /// Human-readable coloured text.
    Text,
    /// Resolve from the environment: JSON when an OTEL endpoint is set, text in
    /// CI or on a terminal, JSON otherwise.
    #[default]
    Auto,
}

impl LogFormat {
    /// Resolve `Auto` to `Json` or `Text`. Other formats are returned unchanged.
    ///
    /// `Auto` gives `Json` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else
    /// `Text` under a CI runner or when stderr is a terminal, else `Json`.
    #[must_use]
    pub fn resolve(self) -> Self {
        match self {
            Self::Auto => Self::derived(ConsoleContext::detect()),
            other => other,
        }
    }

    /// The concrete format `Auto` resolves to in `ctx`.
    fn derived(ctx: ConsoleContext) -> Self {
        if ctx.wants_json() {
            Self::Json
        } else {
            Self::Text
        }
    }
}

impl std::str::FromStr for LogFormat {
    type Err = LoggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "text" | "pretty" | "human" => Ok(Self::Text),
            "auto" => Ok(Self::Auto),
            _ => Err(LoggerError::InvalidFormat(s.to_string())),
        }
    }
}

/// Log throttle configuration.
///
/// Controls global rate limiting via `tracing-throttle`. Disabled by default.
/// When enabled, identical log events are deduplicated using a token bucket policy.
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    /// Enable log throttling.
    pub enabled: bool,
    /// Token bucket burst capacity (max events before throttling starts).
    pub burst: f64,
    /// Token recovery rate (tokens per second).
    pub rate: f64,
    /// Maximum number of distinct event signatures to track.
    pub max_signatures: usize,
    /// High-cardinality fields to exclude from signature matching.
    /// Events differing only in these fields will be treated as identical.
    pub excluded_fields: Vec<String>,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            burst: 50.0,
            rate: 1.0,
            max_signatures: 10_000,
            excluded_fields: vec![
                "request_id".to_string(),
                "trace_id".to_string(),
                "span_id".to_string(),
            ],
        }
    }
}

/// Logger configuration options.
#[derive(Debug, Clone)]
pub struct LoggerOptions {
    /// Log level (DEBUG, INFO, WARN, ERROR).
    pub level: Level,
    /// Output format.
    pub format: LogFormat,
    /// Include source file and line in output.
    pub add_source: bool,
    /// Enable sensitive data masking.
    pub enable_masking: bool,
    /// Field names to mask.
    pub sensitive_fields: Vec<String>,
    /// Include span events.
    pub span_events: bool,
    /// Log throttle configuration (deduplicate identical events).
    pub throttle: ThrottleConfig,
    /// Service name injected into JSON log output.
    /// Auto-populated by ServiceApp. Falls back to SERVICE_NAME env var.
    pub service_name: Option<String>,
    /// Service version injected into JSON log output.
    /// Auto-populated by ServiceApp. Falls back to SERVICE_VERSION env var.
    pub service_version: Option<String>,
}

impl Default for LoggerOptions {
    fn default() -> Self {
        Self {
            level: Level::INFO,
            format: LogFormat::Auto,
            add_source: true,
            enable_masking: true,
            sensitive_fields: default_sensitive_fields(),
            span_events: false,
            throttle: ThrottleConfig::default(),
            service_name: None,
            service_version: None,
        }
    }
}

/// Cascade-loadable logger settings, under the `logger` config key.
///
/// Every field is optional so an absent key is distinguishable from a set
/// one: a CLI flag or environment variable outranks config, and can only do
/// so if "not given" is representable.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LoggerSettings {
    /// Log level (trace, debug, info, warn, error).
    #[serde(default)]
    pub level: Option<String>,

    /// Output format (json, text, auto).
    #[serde(default)]
    pub format: Option<String>,

    /// Colour in text mode: a bool, a number (non-zero is on), or a string
    /// (`true`, `1` or `yes` is on). `LOG_COLOR` and `NO_COLOR` outrank it.
    #[serde(default, deserialize_with = "deserialize_flag")]
    pub color: Option<bool>,
}

/// Read a bool, number or string as on or off, the way scalo-py reads `logging.color`.
fn deserialize_flag<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum Flag {
        Bool(bool),
        Int(i64),
        Text(String),
    }
    let flag: Option<Flag> = serde::Deserialize::deserialize(deserializer)?;
    Ok(flag.map(|flag| match flag {
        Flag::Bool(on) => on,
        Flag::Int(n) => n != 0,
        Flag::Text(text) => is_truthy(&text),
    }))
}

impl LoggerSettings {
    /// Load from the config cascade under the `logger` key.
    ///
    /// Returns the default (nothing set) when config is not initialised, so
    /// a logger built before the cascade exists still works.
    ///
    /// # Errors
    ///
    /// [`CascadeError`](crate::cascade::CascadeError) when the `logger` section
    /// is present but does not parse.
    pub fn from_cascade() -> Result<Self, crate::cascade::CascadeError> {
        crate::cascade::section("logger")
    }
}

/// Initialise the global logger with custom options.
///
/// # Errors
///
/// Returns an error if the logger is already initialised.
pub fn setup(opts: LoggerOptions) -> Result<(), LoggerError> {
    if LOGGER_INIT.get().is_some() {
        return Err(LoggerError::AlreadyInitialised);
    }

    let format = opts.format.resolve();

    let rust_log = std::env::var("RUST_LOG")
        .ok()
        .filter(|rust_log| !rust_log.trim().is_empty());
    let (filter, level_handle) = LevelHandle::new(opts.level, rust_log);

    // RFC 3339 timestamp format
    let timer = UtcTime::rfc_3339();

    let span_events = if opts.span_events {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    // Build sensitive fields set for masking writer
    let sensitive: std::collections::HashSet<String> = if opts.enable_masking {
        opts.sensitive_fields
            .iter()
            .map(|s| s.to_lowercase())
            .collect()
    } else {
        std::collections::HashSet::new()
    };

    // Build optional throttle filter
    let throttle_filter = build_throttle_filter(&opts.throttle);

    // Span export shares the subscriber, so it has to be composed in here.
    // `None` is a no-op layer, which is what an opted-out or unbuildable
    // exporter resolves to.
    #[cfg(feature = "otel-tracing")]
    let otel = {
        let mut config = crate::otel_tracing::OtelTracingConfig::from_cascade()?;
        if config.service_name.is_empty() {
            config.service_name = opts
                .service_name
                .clone()
                .or_else(|| std::env::var("SERVICE_NAME").ok())
                .unwrap_or_default();
        }
        crate::otel_tracing::layer_if_active(&config)
    };
    #[cfg(not(feature = "otel-tracing"))]
    let otel: Option<tracing_subscriber::layer::Identity> = None;

    match format {
        LogFormat::Json => {
            let writer = masking::make_masking_writer(
                sensitive,
                true,
                opts.service_name.clone(),
                opts.service_version.clone(),
            );
            let layer = tracing_subscriber::fmt::layer()
                .json()
                .with_timer(timer)
                .with_file(opts.add_source)
                .with_line_number(opts.add_source)
                .with_target(true)
                .with_span_events(span_events)
                .with_writer(writer);

            if let Some(throttle) = throttle_filter {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(otel)
                    .with(layer.with_filter(throttle))
                    .try_init()
                    .map_err(|e| LoggerError::SetGlobalError(e.to_string()))?;
            } else {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(otel)
                    .with(layer)
                    .try_init()
                    .map_err(|e| LoggerError::SetGlobalError(e.to_string()))?;
            }
        }
        LogFormat::Text => {
            let writer = masking::make_masking_writer(sensitive, false, None, None);
            let ansi = colour_enabled(
                std::env::var("LOG_COLOR").ok().as_deref(),
                is_no_color(),
                LoggerSettings::from_cascade()?.color,
                ConsoleContext::detect().tty,
            );
            let formatter = format::ColouredFormatter::new(ansi)
                .with_file(opts.add_source)
                .with_line_number(opts.add_source);
            let layer = tracing_subscriber::fmt::layer()
                .with_ansi(ansi)
                .with_span_events(span_events)
                .event_format(formatter)
                .with_writer(writer);

            if let Some(throttle) = throttle_filter {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(otel)
                    .with(layer.with_filter(throttle))
                    .try_init()
                    .map_err(|e| LoggerError::SetGlobalError(e.to_string()))?;
            } else {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(otel)
                    .with(layer)
                    .try_init()
                    .map_err(|e| LoggerError::SetGlobalError(e.to_string()))?;
            }
        }
        LogFormat::Auto => unreachable!("Auto should be resolved"),
    }

    let _ = LOGGER_INIT.set(());
    let _ = LEVEL_HANDLE.set(level_handle);

    #[cfg(feature = "otel-tracing")]
    crate::otel_tracing::log_init_status();

    #[cfg(feature = "config")]
    crate::config::log_deferred();

    Ok(())
}

/// A log level as an operator writes it: the `tracing` names or `1`-`5`, and
/// the `warning`, `critical` and `fatal` that scalo-py accepts, in any case.
/// `None` for a blank or unknown name.
pub(crate) fn parse_level(name: &str) -> Option<Level> {
    let name = name.trim();
    if name.eq_ignore_ascii_case("warning") {
        return Some(Level::WARN);
    }
    if name.eq_ignore_ascii_case("critical") || name.eq_ignore_ascii_case("fatal") {
        return Some(Level::ERROR);
    }
    name.parse().ok()
}

/// Initialise the global logger with default settings.
///
/// Respects environment variables:
/// - `LOG_LEVEL` or `RUST_LOG`: Log level
/// - `LOG_FORMAT`: Output format (json, text, auto). Unset, blank, `auto` or
///   unrecognised defers to `logger.format` in config, then to
///   [`LogFormat::Auto`]
/// - `LOG_COLOR`: Colour in text mode (`true`, `1` or `yes` is on, anything
///   else off). Unset defers to `NO_COLOR`, then `logger.color`, then the
///   terminal check
/// - `NO_COLOR`: Disable coloured output when `LOG_COLOR` is unset
/// - `LOG_THROTTLE_ENABLED`: Enable log deduplication (default: false)
/// - `LOG_THROTTLE_BURST`: Token bucket burst capacity (default: 50)
/// - `LOG_THROTTLE_RATE`: Token recovery rate per second (default: 1.0)
///
/// # Errors
///
/// Returns an error if the logger is already initialised.
pub fn setup_default() -> Result<(), LoggerError> {
    let level = std::env::var("LOG_LEVEL")
        .or_else(|_| std::env::var("RUST_LOG"))
        .ok()
        .and_then(|s| parse_level(&s))
        .unwrap_or(Level::INFO);

    let format = first_concrete_format([
        std::env::var("LOG_FORMAT").ok(),
        LoggerSettings::from_cascade()?.format,
    ]);

    let throttle_enabled = std::env::var("LOG_THROTTLE_ENABLED")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));

    let throttle_burst = std::env::var("LOG_THROTTLE_BURST")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50.0);

    let throttle_rate = std::env::var("LOG_THROTTLE_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1.0);

    let service_name = std::env::var("SERVICE_NAME").ok();
    let service_version = std::env::var("SERVICE_VERSION").ok();

    setup(LoggerOptions {
        level,
        format,
        throttle: ThrottleConfig {
            enabled: throttle_enabled,
            burst: throttle_burst,
            rate: throttle_rate,
            ..Default::default()
        },
        service_name,
        service_version,
        ..Default::default()
    })
}

/// Build an optional throttle filter from configuration.
fn build_throttle_filter(config: &ThrottleConfig) -> Option<TracingRateLimitLayer> {
    if !config.enabled {
        return None;
    }

    let policy = Policy::token_bucket(config.burst, config.rate)
        .unwrap_or_else(|_| Policy::token_bucket(50.0, 1.0).expect("default policy is valid"));

    let mut builder = TracingRateLimitLayer::builder()
        .with_policy(policy)
        .with_max_signatures(config.max_signatures);

    if !config.excluded_fields.is_empty() {
        builder = builder.with_excluded_fields(config.excluded_fields.clone());
    }

    match builder.build() {
        Ok(layer) => Some(layer),
        Err(e) => {
            eprintln!("Failed to build log throttle layer: {e}");
            None
        }
    }
}

/// The first selector naming `json` or `text`, else `Auto`. Blank, `auto` and unrecognised ones defer.
fn first_concrete_format<const N: usize>(selectors: [Option<String>; N]) -> LogFormat {
    selectors
        .into_iter()
        .flatten()
        .filter_map(|selector| selector.trim().parse::<LogFormat>().ok())
        .find(|format| *format != LogFormat::Auto)
        .unwrap_or(LogFormat::Auto)
}

/// Whether text output carries ANSI colour.
///
/// `LOG_COLOR` decides when set, then a set `NO_COLOR` turns colour off, then
/// `logger.color`, then whether stderr is a terminal.
fn colour_enabled(
    log_color: Option<&str>,
    no_color: bool,
    configured: Option<bool>,
    tty: bool,
) -> bool {
    if let Some(value) = log_color {
        return is_truthy(value);
    }
    if no_color {
        return false;
    }
    configured.unwrap_or(tty)
}

/// `true` for `true`, `1` or `yes` in any case, the values scalo-py reads as on.
fn is_truthy(value: &str) -> bool {
    ["true", "1", "yes"]
        .iter()
        .any(|on| value.eq_ignore_ascii_case(on))
}

/// Check if NO_COLOR environment variable is set.
fn is_no_color() -> bool {
    std::env::var("NO_COLOR").is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_str() {
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert_eq!("pretty".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert_eq!("auto".parse::<LogFormat>().unwrap(), LogFormat::Auto);
        // A bad format string reports InvalidFormat, not InvalidLevel.
        assert!(matches!(
            "yaml".parse::<LogFormat>(),
            Err(LoggerError::InvalidFormat(_))
        ));
    }

    #[test]
    fn test_log_format_resolve() {
        // Json and Text should stay as-is
        assert_eq!(LogFormat::Json.resolve(), LogFormat::Json);
        assert_eq!(LogFormat::Text.resolve(), LogFormat::Text);

        // Auto resolves based on environment
        let resolved = LogFormat::Auto.resolve();
        assert!(matches!(resolved, LogFormat::Json | LogFormat::Text));
    }

    #[test]
    fn auto_is_json_off_a_terminal_and_text_on_one() {
        let piped = ConsoleContext::default();
        let tty = ConsoleContext { tty: true, ..piped };
        assert_eq!(LogFormat::derived(piped), LogFormat::Json);
        assert_eq!(LogFormat::derived(tty), LogFormat::Text);
    }

    #[test]
    fn auto_is_json_with_an_otel_endpoint_even_on_a_terminal() {
        let ctx = ConsoleContext {
            otel_endpoint: true,
            ci: true,
            tty: true,
        };
        assert_eq!(LogFormat::derived(ctx), LogFormat::Json);
    }

    #[test]
    fn auto_is_text_in_ci_even_when_piped() {
        let ctx = ConsoleContext {
            ci: true,
            ..ConsoleContext::default()
        };
        assert_eq!(LogFormat::derived(ctx), LogFormat::Text);
    }

    #[test]
    fn the_first_concrete_selector_wins() {
        let some = |s: &str| Some(s.to_string());
        assert_eq!(
            first_concrete_format([some("json"), some("text")]),
            LogFormat::Json,
            "LOG_FORMAT outranks logger.format"
        );
        assert_eq!(
            first_concrete_format([some(" Pretty "), None]),
            LogFormat::Text,
            "aliases and padding are read as scalo-py reads them"
        );
        for deferring in ["auto", "AUTO", "", "  ", "yaml"] {
            assert_eq!(
                first_concrete_format([some(deferring), some("text")]),
                LogFormat::Text,
                "LOG_FORMAT={deferring:?} defers to logger.format"
            );
        }
        assert_eq!(
            first_concrete_format([None, some("auto")]),
            LogFormat::Auto,
            "nothing concrete leaves Auto for setup to resolve"
        );
        assert_eq!(first_concrete_format([None, None]), LogFormat::Auto);
    }

    #[test]
    fn log_color_outranks_everything() {
        for (log_color, on) in [
            ("true", true),
            ("TRUE", true),
            ("1", true),
            ("yes", true),
            ("false", false),
            ("0", false),
            ("no", false),
            ("", false),
        ] {
            assert_eq!(
                colour_enabled(Some(log_color), true, Some(!on), !on),
                on,
                "LOG_COLOR={log_color:?} beats NO_COLOR, config and the terminal"
            );
        }
    }

    #[test]
    fn no_color_outranks_config_and_the_terminal() {
        assert!(!colour_enabled(None, true, Some(true), true));
    }

    #[test]
    fn config_outranks_the_terminal() {
        assert!(colour_enabled(None, false, Some(true), false));
        assert!(!colour_enabled(None, false, Some(false), true));
    }

    #[test]
    fn colour_follows_the_terminal_when_nothing_is_set() {
        assert!(colour_enabled(None, false, None, true), "terminal");
        assert!(!colour_enabled(None, false, None, false), "piped");
    }

    #[test]
    fn logger_color_reads_bools_numbers_and_strings() {
        for (json, expected) in [
            (r#"{"color": true}"#, Some(true)),
            (r#"{"color": false}"#, Some(false)),
            (r#"{"color": 1}"#, Some(true)),
            (r#"{"color": 0}"#, Some(false)),
            (r#"{"color": "yes"}"#, Some(true)),
            (r#"{"color": "TRUE"}"#, Some(true)),
            (r#"{"color": "off"}"#, Some(false)),
            (r#"{"color": null}"#, None),
            ("{}", None),
        ] {
            let settings: LoggerSettings = serde_json::from_str(json).expect(json);
            assert_eq!(settings.color, expected, "{json}");
        }
    }

    #[test]
    fn a_string_color_leaves_level_and_format_intact() {
        let settings: LoggerSettings =
            serde_json::from_str(r#"{"level": "warn", "format": "json", "color": "no"}"#)
                .expect("settings");
        assert_eq!(settings.level.as_deref(), Some("warn"));
        assert_eq!(settings.format.as_deref(), Some("json"));
        assert_eq!(settings.color, Some(false));
    }

    #[test]
    fn test_logger_options_default() {
        let opts = LoggerOptions::default();
        assert_eq!(opts.level, Level::INFO);
        assert_eq!(opts.format, LogFormat::Auto);
        assert!(opts.add_source);
        assert!(opts.enable_masking);
        assert_ne!(opts.sensitive_fields, [] as [std::string::String; 0]);
    }

    #[test]
    fn test_is_no_color() {
        temp_env::with_var("NO_COLOR", None::<&str>, || assert!(!is_no_color()));
        temp_env::with_var("NO_COLOR", Some("1"), || assert!(is_no_color()));
    }

    /// Log lines a test subscriber writes.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The handle changes what a running subscriber writes and keeps the
    /// rdkafka floor. Text it cannot read leaves the level as it was.
    #[test]
    fn the_level_handle_changes_the_level_live() {
        let captured = Captured::default();
        let writer = captured.clone();
        let (filter, handle) = LevelHandle::new(Level::INFO, None);
        let subscriber = tracing_subscriber::registry().with(filter).with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("first debug line");
            handle.set("debug").unwrap();
            tracing::debug!("second debug line");
            tracing::info!(target: "rdkafka", "rdkafka info line");
            for bad in ["loud", "", "debug,my_app=trace"] {
                assert!(
                    matches!(handle.set(bad), Err(LoggerError::InvalidLevel(_))),
                    "{bad:?} must not parse"
                );
            }
            assert_eq!(handle.current(), Level::DEBUG);
            tracing::debug!("third debug line");
            handle.set("Warning").unwrap();
            tracing::info!("info line at warn");
        });
        assert_eq!(handle.current(), Level::WARN);

        let text = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        for (line, written) in [
            ("first debug line", false),
            ("second debug line", true),
            ("rdkafka info line", false),
            ("third debug line", true),
            ("info line at warn", false),
        ] {
            assert_eq!(text.contains(line), written, "{line}:\n{text}");
        }
    }

    /// `setup` hands out the handle on the logger it starts.
    #[test]
    fn setup_hands_out_the_level_handle() {
        temp_env::with_var("RUST_LOG", None::<&str>, || {
            setup(LoggerOptions {
                format: LogFormat::Json,
                ..LoggerOptions::default()
            })
            .unwrap();
        });
        let handle = level_handle().unwrap();
        assert_eq!(handle.current(), Level::INFO);
        assert!(!tracing::enabled!(Level::DEBUG));
        handle.set("debug").unwrap();
        assert!(tracing::enabled!(Level::DEBUG));
    }
}
