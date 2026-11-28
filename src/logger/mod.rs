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
//! - One writer thread behind a bounded queue, so a stalled stderr drops
//!   lines rather than blocking the thread that logs. See [`flush`]
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
mod queue;
pub mod security;

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use thiserror::Error;
use tracing::Level;
use tracing_subscriber::Layer;
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

/// The queue the logger [`setup`] started writes its lines through.
static LINE_QUEUE: OnceLock<queue::LineQueue> = OnceLock::new();

/// How long [`flush`] waits for the queued lines to be written.
const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Wait up to one second for the lines logged so far to reach stderr.
///
/// The logger runs this itself at process exit and before a panic message.
/// A process that ends by `abort`, `_exit` or a fatal signal skips both, so
/// call it first there. It returns at once before [`setup`] has run.
pub fn flush() {
    if let Some(queue) = LINE_QUEUE.get() {
        queue.flush(FLUSH_TIMEOUT);
    }
}

/// The queue to stderr, started on first use.
fn line_queue() -> Result<&'static queue::LineQueue, LoggerError> {
    if let Some(queue) = LINE_QUEUE.get() {
        return Ok(queue);
    }
    let queue = queue::LineQueue::spawn(std::io::stderr(), queue::CAPACITY)
        .map_err(|e| LoggerError::SetGlobalError(format!("log writer thread: {e}")))?;
    let mut started = false;
    let queue = LINE_QUEUE.get_or_init(|| {
        started = true;
        queue
    });
    if started {
        flush_on_exit();
    }
    Ok(queue)
}

/// Flush before a panic message and at process exit, so the lines that
/// explain a failure reach stderr ahead of it.
fn flush_on_exit() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        flush();
        previous(info);
    }));
    #[cfg(unix)]
    flush_at_exit();
}

/// Register [`flush`] with the C runtime, which runs it when `main` returns
/// and on `std::process::exit`.
#[cfg(unix)]
#[allow(unsafe_code)]
fn flush_at_exit() {
    extern "C" fn at_exit() {
        flush();
    }
    // SAFETY: `at_exit` takes no arguments and never unwinds, since `flush`
    // recovers a poisoned lock instead of panicking.
    let registered = unsafe { libc::atexit(at_exit) };
    if registered != 0 {
        eprintln!("logger: atexit registration failed, call logger::flush() before exit");
    }
}

/// Changes the level of a running logger: what it prints and the spans it
/// exports. Layers passed to [`setup_with_layers`] filter for themselves.
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
    filters: Vec<reload::Handle<EnvFilter, Registry>>,
    rust_log: Option<String>,
    level: Arc<Mutex<Level>>,
}

impl LevelHandle {
    /// A handle at `level`, with `rust_log` layered over it, that changes no
    /// filter until [`filter`](Self::filter) hands one out.
    fn new(level: Level, rust_log: Option<String>) -> Self {
        Self {
            filters: Vec::new(),
            rust_log,
            level: Arc::new(Mutex::new(level)),
        }
    }

    /// A filter at the current level, which [`set`](Self::set) changes from
    /// then on. Each filtered layer takes its own, since a filter keeps
    /// per-span state.
    fn filter(&mut self) -> reload::Layer<EnvFilter, Registry> {
        let (filter, handle) =
            reload::Layer::new(env_filter(self.current(), self.rust_log.as_deref()));
        self.filters.push(handle);
        filter
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
        for filter in &self.filters {
            filter
                .reload(env_filter(wanted, self.rust_log.as_deref()))
                .map_err(|e| LoggerError::SetGlobalError(e.to_string()))?;
        }
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
    setup_with_layers(opts, Vec::new())
}

/// [`setup`], with `layers` added beside the printed output.
///
/// The level filter sits on the printed output and the span export, so each
/// of `layers` sees every event and span whatever the level, and filters for
/// itself. Give a layer its own filter (`layer.with_filter(LevelFilter::WARN)`)
/// rather than dropping events inside it: an unfiltered layer is handed every
/// event at every level, so every `tracing` call in the process builds one.
///
/// With any layer here, each `tracing` call up to the most verbose level a
/// layer wants runs every layer's filter, because a cached callsite verdict
/// drops events after an `enabled!` query (tokio-rs/tracing#2519).
///
/// ```rust,no_run
/// use tracing_subscriber::Layer;
/// use tracing_subscriber::filter::LevelFilter;
///
/// let warnings = tracing_subscriber::fmt::layer()
///     .with_writer(std::io::stdout)
///     .with_filter(LevelFilter::WARN);
/// scalo::logger::setup_with_layers(
///     scalo::logger::LoggerOptions::default(),
///     vec![warnings.boxed()],
/// )?;
/// # Ok::<(), scalo::logger::LoggerError>(())
/// ```
///
/// # Errors
///
/// Returns an error if the logger is already initialised.
pub fn setup_with_layers(
    opts: LoggerOptions,
    layers: Vec<Box<dyn Layer<Registry> + Send + Sync>>,
) -> Result<(), LoggerError> {
    if LOGGER_INIT.get().is_some() {
        return Err(LoggerError::AlreadyInitialised);
    }

    let format = opts.format.resolve();

    let rust_log = std::env::var("RUST_LOG")
        .ok()
        .filter(|rust_log| !rust_log.trim().is_empty());
    let mut level_handle = LevelHandle::new(opts.level, rust_log);

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
        crate::otel_tracing::layer_if_active::<Registry>(&config)
    };
    #[cfg(not(feature = "otel-tracing"))]
    let otel: Option<tracing_subscriber::layer::Identity> = None;

    let queue = line_queue()?;
    let stack = match format {
        LogFormat::Json => {
            let writer = masking::make_masking_writer(
                || queue.line(),
                sensitive,
                true,
                opts.service_name.clone(),
                opts.service_version.clone(),
            );
            let printed = tracing_subscriber::fmt::layer()
                .json()
                .with_timer(timer)
                .with_file(opts.add_source)
                .with_line_number(opts.add_source)
                .with_target(true)
                .with_span_events(span_events)
                .with_writer(writer);
            layer_stack(printed, throttle_filter, otel, layers, &mut level_handle)
        }
        LogFormat::Text => {
            let writer =
                masking::make_masking_writer(|| queue.line(), sensitive, false, None, None);
            let ansi = colour_enabled(
                std::env::var("LOG_COLOR").ok().as_deref(),
                is_no_color(),
                LoggerSettings::from_cascade()?.color,
                ConsoleContext::detect().tty,
            );
            let formatter = format::ColouredFormatter::new(ansi)
                .with_file(opts.add_source)
                .with_line_number(opts.add_source);
            let printed = tracing_subscriber::fmt::layer()
                .with_ansi(ansi)
                .with_span_events(span_events)
                .event_format(formatter)
                .with_writer(writer);
            layer_stack(printed, throttle_filter, otel, layers, &mut level_handle)
        }
        LogFormat::Auto => unreachable!("Auto should be resolved"),
    };

    install(stack)?;

    let _ = LOGGER_INIT.set(());
    let _ = LEVEL_HANDLE.set(level_handle);

    #[cfg(feature = "otel-tracing")]
    crate::otel_tracing::log_init_status();

    #[cfg(feature = "config")]
    crate::config::log_deferred();

    Ok(())
}

/// Has every callsite ask the per-layer filters on each call.
///
/// A per-layer filter's verdict waits in a thread-local until the event is
/// recorded. `tracing::enabled!` and `log::log_enabled!` ask without
/// recording, so the next event whose callsite skips the filters reads that
/// verdict and a layer drops it (tokio-rs/tracing#2519). Asking every call
/// overwrites it.
struct AskEveryCall;

impl<S> tracing_subscriber::layer::Filter<S> for AskEveryCall {
    fn enabled(
        &self,
        _: &tracing::Metadata<'_>,
        _: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        true
    }

    fn callsite_enabled(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    /// Adds no level to the most verbose one the other layers want.
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::OFF)
    }
}

/// The layers [`setup_with_layers`] installs on the registry.
///
/// With no `extra` layer, one filter from `level` wraps `otel` and `printed`
/// and rejects an event before either sees it, as `setup` always has. That
/// clears the per-layer state a query leaves behind (see [`AskEveryCall`]).
/// With `extra` layers, `otel` and `printed` each sit behind their own filter
/// from `level`, then `extra` as given, then [`AskEveryCall`].
///
/// The throttle sits inside the level filter, so it counts only events the
/// level lets through.
fn layer_stack<P, O>(
    printed: P,
    throttle: Option<TracingRateLimitLayer>,
    otel: Option<O>,
    extra: Vec<Box<dyn Layer<Registry> + Send + Sync>>,
    level: &mut LevelHandle,
) -> Vec<Box<dyn Layer<Registry> + Send + Sync>>
where
    P: Layer<Registry> + Send + Sync + 'static,
    O: Layer<Registry> + Send + Sync + 'static,
{
    if extra.is_empty() {
        // Outermost, because an `and_then` on `Registry` takes its hint from
        // the outer layer alone and turns an inner `never` into `sometimes`.
        let filter = level.filter();
        return vec![match throttle {
            Some(throttle) => Layer::and_then(otel, printed.with_filter(throttle))
                .and_then(filter)
                .boxed(),
            None => Layer::and_then(otel, printed).and_then(filter).boxed(),
        }];
    }
    let printed = match throttle {
        Some(throttle) => printed
            .with_filter(throttle)
            .with_filter(level.filter())
            .boxed(),
        None => printed.with_filter(level.filter()).boxed(),
    };
    let otel = otel.map(|otel| otel.with_filter(level.filter()).boxed());
    let ask_every_call = tracing_subscriber::layer::Identity::new()
        .with_filter(AskEveryCall)
        .boxed();
    otel.into_iter()
        .chain([printed])
        .chain(extra)
        .chain([ask_every_call])
        .collect()
}

/// Install `stack` as the global subscriber.
fn install(stack: Vec<Box<dyn Layer<Registry> + Send + Sync>>) -> Result<(), LoggerError> {
    tracing_subscriber::registry()
        .with(stack)
        .try_init()
        .map_err(|e| LoggerError::SetGlobalError(e.to_string()))
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
    use tracing_subscriber::filter::LevelFilter;

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

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    /// A plain text layer writing every event it is handed to `captured`.
    fn printing_to<S>(captured: Captured) -> impl Layer<S> + Send + Sync + 'static
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + 'static,
    {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_writer(move || captured.clone())
    }

    /// A stand-in for span export, with a per-layer filter as the real one has.
    fn exporting_to<S>(captured: Captured) -> impl Layer<S> + Send + Sync + 'static
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + 'static,
    {
        printing_to(captured).with_filter(tracing_subscriber::filter::filter_fn(|meta| {
            meta.target() != "rdkafka"
        }))
    }

    /// No span export, for a stack built without one.
    const NO_OTEL: Option<tracing_subscriber::layer::Identity> = None;

    const LEVELS: [Level; 5] = [
        Level::ERROR,
        Level::WARN,
        Level::INFO,
        Level::DEBUG,
        Level::TRACE,
    ];

    /// What `tracing::enabled!` says of each level, then one event at each
    /// level, a warning inside a debug span, and an rdkafka info line.
    fn emit_each_level() -> [bool; 5] {
        let enabled = [
            tracing::enabled!(Level::ERROR),
            tracing::enabled!(Level::WARN),
            tracing::enabled!(Level::INFO),
            tracing::enabled!(Level::DEBUG),
            tracing::enabled!(Level::TRACE),
        ];
        tracing::error!("error event");
        tracing::warn!("warn event");
        tracing::info!("info event");
        tracing::debug!("debug event");
        tracing::trace!("trace event");
        tracing::debug_span!("debug_span").in_scope(|| tracing::warn!("warn in a span"));
        tracing::info!(target: "rdkafka", "rdkafka info event");
        enabled
    }

    /// With no extra layers the stack prints, exports and enables exactly what
    /// the registry-wide level filter did, a `RUST_LOG` span directive
    /// included. Its hint is the level filter's, where the registry-wide stack
    /// gave none once span export had a per-layer filter.
    #[test]
    fn no_extra_layers_filter_as_a_registry_wide_level_did() {
        let cases = LEVELS.map(|level| (level, None)).into_iter().chain([
            (Level::WARN, Some("rdkafka=info")),
            (Level::INFO, Some("[probe]=debug")),
        ]);
        for (level, rust_log) in cases {
            let (printed_before, exported_before) = (Captured::default(), Captured::default());
            let subscriber = tracing_subscriber::registry()
                .with(env_filter(level, rust_log))
                .with(exporting_to(exported_before.clone()))
                .with(printing_to(printed_before.clone()));
            let enabled_before = tracing::subscriber::with_default(subscriber, emit_each_level);
            let filter_hint =
                <EnvFilter as Layer<Registry>>::max_level_hint(&env_filter(level, rust_log));

            let (printed, exported) = (Captured::default(), Captured::default());
            let mut handle = LevelHandle::new(level, rust_log.map(str::to_string));
            let stack = layer_stack(
                printing_to(printed.clone()),
                None,
                Some(exporting_to(exported.clone())),
                Vec::new(),
                &mut handle,
            );
            let subscriber = tracing_subscriber::registry().with(stack);
            let hint = tracing::Subscriber::max_level_hint(&subscriber);
            let enabled = tracing::subscriber::with_default(subscriber, emit_each_level);

            let case = format!("{level} {rust_log:?}");
            assert_eq!(printed.text(), printed_before.text(), "printed, {case}");
            assert_eq!(exported.text(), exported_before.text(), "exported, {case}");
            assert_eq!(enabled, enabled_before, "enabled, {case}");
            assert_eq!(hint, filter_hint, "hint, {case}");
        }
    }

    /// An extra layer sees every event at every level, while the printed
    /// layer and the span export see only what the level lets through.
    #[test]
    fn extra_layers_see_every_level_and_the_level_filters_the_rest() {
        let events = [
            (Level::ERROR, "error event"),
            (Level::WARN, "warn event"),
            (Level::INFO, "info event"),
            (Level::DEBUG, "debug event"),
            (Level::TRACE, "trace event"),
        ];
        for level in LEVELS {
            let (printed, exported, extra) = (
                Captured::default(),
                Captured::default(),
                Captured::default(),
            );
            let mut handle = LevelHandle::new(level, None);
            let stack = layer_stack(
                printing_to(printed.clone()),
                None,
                Some(printing_to(exported.clone())),
                vec![printing_to(extra.clone()).boxed()],
                &mut handle,
            );
            let enabled = tracing::subscriber::with_default(
                tracing_subscriber::registry().with(stack),
                emit_each_level,
            );
            assert_eq!(enabled, [true; 5], "an extra layer enables every level");

            let (printed, exported, extra) = (printed.text(), exported.text(), extra.text());
            for (at, line) in events {
                let shown = at <= level;
                assert_eq!(printed.contains(line), shown, "printed {line} at {level}");
                assert_eq!(exported.contains(line), shown, "exported {line} at {level}");
                assert!(extra.contains(line), "extra {line} at {level}:\n{extra}");
            }
        }
    }

    /// A query that records nothing, such as `tracing::enabled!` or
    /// `log::log_enabled!`, costs no layer the next event.
    #[test]
    fn an_enabled_query_costs_no_layer_the_next_event() {
        /// The level, the extra layer's filter, the query, the next event.
        type Case = (Level, Option<LevelFilter>, fn() -> bool, fn());
        let cases: [Case; 2] = [
            (
                Level::ERROR,
                None,
                || tracing::enabled!(Level::WARN),
                || tracing::error!("next event"),
            ),
            (
                Level::INFO,
                Some(LevelFilter::WARN),
                || tracing::enabled!(Level::INFO),
                || tracing::warn!("next event"),
            ),
        ];
        for (level, extra_filter, query, next_event) in cases {
            let (printed, extra) = (Captured::default(), Captured::default());
            let extra_layer = match extra_filter {
                Some(filter) => printing_to(extra.clone()).with_filter(filter).boxed(),
                None => printing_to(extra.clone()).boxed(),
            };
            let mut handle = LevelHandle::new(level, None);
            let stack = layer_stack(
                printing_to(printed.clone()),
                None,
                NO_OTEL,
                vec![extra_layer],
                &mut handle,
            );
            tracing::subscriber::with_default(tracing_subscriber::registry().with(stack), || {
                assert!(query(), "one layer wants it, so the query says yes");
                next_event();
            });
            let case = format!("{level} {extra_filter:?}");
            assert!(printed.text().contains("next event"), "printed, {case}");
            assert!(extra.text().contains("next event"), "extra, {case}");
        }
    }

    /// The throttle holds back repeats on the printed layer only, with or
    /// without an extra layer.
    #[test]
    fn the_throttle_holds_back_printed_repeats_only() {
        for with_extra in [false, true] {
            let throttle = build_throttle_filter(&ThrottleConfig {
                enabled: true,
                burst: 1.0,
                rate: 1.0,
                ..ThrottleConfig::default()
            });
            assert!(throttle.is_some());
            let (printed, extra) = (Captured::default(), Captured::default());
            let extra_layers = if with_extra {
                vec![printing_to(extra.clone()).boxed()]
            } else {
                Vec::new()
            };
            let mut handle = LevelHandle::new(Level::INFO, None);
            let stack = layer_stack(
                printing_to(printed.clone()),
                throttle,
                NO_OTEL,
                extra_layers,
                &mut handle,
            );
            tracing::subscriber::with_default(tracing_subscriber::registry().with(stack), || {
                for _ in 0..5 {
                    tracing::warn!("repeated line");
                }
            });
            let repeats = |captured: &Captured| captured.text().matches("repeated line").count();
            assert_eq!(repeats(&printed), 1, "printed, extra layer {with_extra}");
            assert_eq!(repeats(&extra), if with_extra { 5 } else { 0 });
        }
    }

    /// The handle changes what the printed layer and the span export write,
    /// live, with or without an extra layer, and keeps the rdkafka floor. An
    /// extra layer sees every line throughout. Text it cannot read leaves the
    /// level as it was.
    #[test]
    fn the_level_handle_changes_the_level_live() {
        for with_extra in [false, true] {
            let (printed, exported, extra) = (
                Captured::default(),
                Captured::default(),
                Captured::default(),
            );
            let extra_layers = if with_extra {
                vec![printing_to(extra.clone()).boxed()]
            } else {
                Vec::new()
            };
            let mut handle = LevelHandle::new(Level::INFO, None);
            let stack = layer_stack(
                printing_to(printed.clone()),
                None,
                Some(exporting_to(exported.clone())),
                extra_layers,
                &mut handle,
            );
            let subscriber = tracing_subscriber::registry().with(stack);
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

            let (printed, exported, extra) = (printed.text(), exported.text(), extra.text());
            for (line, written) in [
                ("first debug line", false),
                ("second debug line", true),
                ("rdkafka info line", false),
                ("third debug line", true),
                ("info line at warn", false),
            ] {
                let case = format!("{line}, extra layer {with_extra}");
                assert_eq!(printed.contains(line), written, "{case}:\n{printed}");
                assert_eq!(exported.contains(line), written, "{case}:\n{exported}");
                assert_eq!(extra.contains(line), with_extra, "{case}:\n{extra}");
            }
        }
    }

    /// `setup_with_layers` hands out the handle on the logger it starts, and
    /// a filtered extra layer keeps callsites no layer wants switched off.
    #[test]
    fn setup_hands_out_the_level_handle_and_adds_extra_layers() {
        let extra = Captured::default();
        temp_env::with_var("RUST_LOG", None::<&str>, || {
            setup_with_layers(
                LoggerOptions {
                    format: LogFormat::Json,
                    ..LoggerOptions::default()
                },
                vec![
                    printing_to(extra.clone())
                        .with_filter(LevelFilter::WARN)
                        .boxed(),
                ],
            )
            .unwrap();
        });
        let handle = level_handle().unwrap();
        assert_eq!(handle.current(), Level::INFO);
        assert!(!tracing::enabled!(Level::DEBUG));
        tracing::info!("info line");
        tracing::warn!("warn line");
        handle.set("debug").unwrap();
        assert!(tracing::enabled!(Level::DEBUG));

        let extra = extra.text();
        assert!(extra.contains("warn line"), "{extra}");
        assert!(!extra.contains("info line"), "{extra}");
    }

    /// The `log` crate's max level `try_init` gives `subscriber`: the `tracing`
    /// max level once `subscriber` is the one registered dispatcher.
    #[cfg(feature = "otel-tracing")]
    fn log_max_level_at_install<S>(subscriber: S) -> log::LevelFilter
    where
        S: tracing::Subscriber + Send + Sync + 'static,
    {
        let dispatch = tracing::Dispatch::new(subscriber);
        let level: log::LevelFilter = LevelFilter::current().to_string().parse().unwrap();
        drop(dispatch);
        level
    }

    /// A runtime to build span export in, which is off outside one.
    #[cfg(feature = "otel-tracing")]
    fn export_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    /// The `log` crate's max level at install is the level filter's, with the
    /// real span export and the throttle each on and off. The reload handle
    /// moves it on each level change (tracing-subscriber reload.rs).
    #[cfg(feature = "otel-tracing")]
    #[test]
    fn install_starts_the_log_max_level_at_the_filter_level() {
        let runtime = export_runtime();
        let _entered = runtime.enter();
        let config = crate::otel_tracing::OtelTracingConfig::default();
        let mut rows = Vec::new();
        for (otel, throttle) in [(false, false), (true, false), (false, true), (true, true)] {
            let throttle_config = ThrottleConfig {
                enabled: throttle,
                ..ThrottleConfig::default()
            };
            let export = otel
                .then(|| crate::otel_tracing::layer_if_active::<Registry>(&config))
                .flatten();
            assert_eq!(export.is_some(), otel, "span export builds");
            let mut handle = LevelHandle::new(Level::INFO, None);
            let stack = layer_stack(
                printing_to(Captured::default()),
                build_throttle_filter(&throttle_config),
                export,
                Vec::new(),
                &mut handle,
            );
            let level = log_max_level_at_install(tracing_subscriber::registry().with(stack));
            rows.push(((otel, throttle), level));
        }
        assert!(
            rows.iter()
                .all(|(_, level)| *level == log::LevelFilter::Info),
            "((span export, throttle), log max level): {rows:?}"
        );
    }

    /// With span export on, the real install starts `log` at the filter's
    /// level, and the reload behind a raise moves it, so the raise reaches
    /// `log` records below the start level.
    #[cfg(feature = "otel-tracing")]
    #[test]
    fn a_live_raise_reaches_log_records_with_span_export_on() {
        let runtime = export_runtime();
        let _entered = runtime.enter();
        let export = crate::otel_tracing::layer_if_active::<Registry>(
            &crate::otel_tracing::OtelTracingConfig::default(),
        );
        assert!(export.is_some(), "span export builds");
        let printed = Captured::default();
        let mut handle = LevelHandle::new(Level::INFO, None);
        let stack = layer_stack(
            printing_to(printed.clone()),
            None,
            export,
            Vec::new(),
            &mut handle,
        );
        install(stack).unwrap();
        assert_eq!(log::max_level(), log::LevelFilter::Info);
        log::debug!("log record at info");
        handle.set("debug").unwrap();
        assert_eq!(log::max_level(), log::LevelFilter::Debug);
        log::debug!("log record at debug");

        let printed = printed.text();
        assert!(!printed.contains("log record at info"), "{printed}");
        assert!(printed.contains("log record at debug"), "{printed}");
    }
}
