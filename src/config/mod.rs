// Project:   scalo
// File:      src/config/mod.rs
// Purpose:   7-layer configuration cascade
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Configuration management with 7-layer cascade.
//!
//! Provides a hierarchical configuration system matching scalo-py.
//! Configuration is loaded from multiple sources with clear priority
//! ordering.
//!
//! ## Cascade Priority (highest to lowest)
//!
//! 1. CLI arguments (via clap integration)
//! 2. Environment variables (with configurable prefix)
//! 3. `.env` file (loaded via dotenvy)
//! 4. `settings.{env}.yaml` (environment-specific)
//! 5. `settings.yaml` (base settings)
//! 6. `defaults.yaml`
//! 7. Hard-coded defaults
//!
//! An app that names a [`ManagedSource`] adds a layer between 3 and 4: the
//! settings Group Policy or an MDM pushes.
//!
//! ## How .env Files Work in the Cascade
//!
//! `.env` in the working directory (parent directories are not searched) is
//! loaded into the process environment, so `.env` values are read at layer 2
//! alongside real env vars. Real env vars win: `dotenvy` does NOT overwrite
//! existing variables.
//!
//! ## Environment Variable Naming
//!
//! Use the `env_compat` module for standardised environment variable names
//! with legacy alias support and deprecation warnings.
//!
//! ## Environment Variable Values
//!
//! A value is parsed before any field reads it: `8080` is a number, `true` a
//! bool, `[a, b]` a list. A field that refuses the parsed value gets the text
//! the variable was set to, trimmed, so `0123` reaches a `String` field as
//! `0123` and a `u16` field as 123.
//!
//! ## A Cascade Without the Global
//!
//! [`setup`] and [`load_typed`] keep the first cascade set up in a process.
//! [`Config::new`] and [`load_typed_cascade`] build one into a value instead,
//! holding the environment it was built in, so tests sharing a process each
//! read their own. `setup` and `load_typed` build theirs the same way.
//!
//! ## Example
//!
//! ```rust,no_run
//! use scalo::config::{self, ConfigOptions};
//!
//! // Initialise with env prefix
//! config::setup(ConfigOptions {
//!     env_prefix: "MYAPP".into(),
//!     ..Default::default()
//! }).unwrap();
//!
//! // Access configuration
//! let cfg = config::get();
//! let host = cfg.get_string("database.host").unwrap_or_default();
//! let port = cfg.get_int("database.port").unwrap_or(5432);
//! ```

pub mod env_compat;
mod env_text;
pub mod flat_env;
mod load;
mod managed;
pub mod registry;
pub mod sensitive;
mod yaml_file;

pub use load::{Loaded, load_typed, load_typed_cascade, load_typed_managed, log_deferred};
pub use managed::ManagedSource;

#[cfg(feature = "config-reload")]
pub mod reloader;

#[cfg(feature = "config-reload")]
pub mod shared;

#[cfg(any(feature = "config-reload", feature = "lifecycle"))]
pub mod watch;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Serialized};
use serde::de::DeserializeOwned;
use thiserror::Error;

use self::env_text::EnvText;
use self::managed::ManagedLayer;
use self::yaml_file::YamlFile;
use crate::env::get_app_env;

/// Global configuration singleton.
static CONFIG: OnceLock<Config> = OnceLock::new();

/// Configuration errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Failed to load configuration file.
    #[error("failed to load config file '{path}': {message}")]
    LoadError { path: PathBuf, message: String },

    /// Failed to extract configuration value.
    #[error("failed to extract config: {0}")]
    ExtractError(#[from] figment::Error),

    /// A config file could not be read or parsed.
    #[error(transparent)]
    Yaml(#[from] crate::yaml::YamlError),

    /// Missing required configuration key.
    #[error("missing required config key: {0}")]
    MissingKey(String),

    /// Invalid configuration value.
    #[error("invalid config value for '{key}': {reason}")]
    InvalidValue { key: String, reason: String },

    /// Configuration already initialised.
    #[error("configuration already initialised")]
    AlreadyInitialised,

    /// Configuration not initialised.
    #[error("configuration not initialised - call config::setup() first")]
    NotInitialised,
}

/// Configuration options.
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    /// Environment variable prefix (e.g., "MYAPP" for MYAPP_DATABASE_HOST).
    pub env_prefix: String,

    /// The app's own default app environment (dev, staging, prod), below
    /// `APP_ENV`, `ENVIRONMENT`, `ENV` and the cascade's `app_env` key. It
    /// picks `settings.{env}.yaml` and is what `get_app_env` returns when none
    /// of those names the environment.
    pub app_env: Option<String>,

    /// Application name for user-scoped config discovery.
    ///
    /// When set, enables searching `~/.config/{app_name}/` for config files,
    /// unless `search_standard_paths` is off.
    /// Falls back to the bare `APP_NAME` environment variable if not
    /// explicitly provided.
    ///
    /// Default: None (user config directory not searched)
    pub app_name: Option<String>,

    /// Additional paths to search for config files: a directory searched for
    /// `defaults.yaml`, `settings.yaml` and `settings.{env}.yaml`, or a file
    /// that is the `settings` layer outright.
    pub config_paths: Vec<PathBuf>,

    /// Whether to search the standard locations for config files: the current
    /// directory, `./config/`, `/config/` and `~/.config/{app_name}/`.
    ///
    /// Turn it off for a service that must read only the files it was pointed
    /// at, such as one running with privileges, so nothing in the working
    /// directory or a user's home reaches its config. `config_paths` is
    /// searched either way. `cli::CommonArgs::to_config_options` and
    /// [`load_typed`] turn it off when a file is named, as they do `.env`.
    ///
    /// Default: true
    pub search_standard_paths: bool,

    /// Whether to load `.env` files.
    ///
    /// When enabled, loads `.env` files in this order (lowest to highest priority):
    /// 1. `~/.env` (home directory - global defaults)
    /// 2. Project `.env` (current directory - project overrides)
    ///
    /// Later files override earlier ones. Real environment variables always
    /// take precedence over `.env` values.
    pub load_dotenv: bool,

    /// Whether to load home directory `.env` file (`~/.env`).
    ///
    /// Only applies when `load_dotenv` is true.
    /// Default: false (opt-in, matching scalo-py)
    pub load_home_dotenv: bool,

    /// The OS store an administrator manages this app's settings through:
    /// Group Policy on Windows, MDM managed preferences on macOS. It merges
    /// after every config file and before env vars. See [`ManagedSource`].
    ///
    /// Default: None, so nothing is read.
    pub managed: Option<ManagedSource>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self {
            env_prefix: String::new(),
            app_env: None,
            app_name: None,
            config_paths: Vec::new(),
            search_standard_paths: true,
            load_dotenv: true,
            load_home_dotenv: false,
            managed: None,
        }
    }
}

/// Where [`Config::find_config_files`] looks.
struct SearchPaths<'a> {
    /// `ConfigOptions::config_paths`.
    extra: &'a [PathBuf],
    /// Whether to search the standard locations.
    standard: bool,
    /// The name `~/.config/` is searched under, when there is one.
    app_name: Option<&'a str>,
}

/// Configuration manager wrapping Figment.
#[derive(Debug)]
pub struct Config {
    figment: Figment,
    env_prefix: String,
    /// `ConfigOptions::app_env`, when it holds a value.
    app_env_default: Option<String>,
    /// The env layers' text, for a field that refuses a parsed env value.
    env_text: EnvText,
}

impl Config {
    /// Resolve the effective app name from explicit value or environment.
    ///
    /// Order: explicit -> bare `APP_NAME`. No brand prefix is imposed and no
    /// legacy fallback -- a consumer needing an old name maps it in its config.
    fn resolve_app_name(explicit: Option<&str>) -> Option<String> {
        explicit
            .map(String::from)
            .or_else(|| std::env::var("APP_NAME").ok())
    }

    /// Create a new configuration with the given options.
    ///
    /// # Errors
    ///
    /// [`ConfigError::LoadError`] when a `config_paths` entry does not exist,
    /// or names a file that cannot be read or parsed, or the managed store
    /// can't be read. [`ConfigError::InvalidValue`] when the managed store
    /// holds a secret.
    pub fn new(opts: ConfigOptions) -> Result<Self, ConfigError> {
        let layer = managed::read(opts.managed.as_ref())?;
        Self::build(opts, layer.as_ref())
    }

    /// The `app_env` key of `files` with the managed policy and the prefixed
    /// variables over it, as the finished cascade gives it.
    fn app_env_key(
        files: &Figment,
        managed: Option<&ManagedLayer>,
        env_prefix: &str,
    ) -> Option<String> {
        let mut probe = files.clone();
        if let Some(layer) = managed {
            probe = probe.merge(layer);
        }
        if !env_prefix.is_empty() {
            probe = probe
                .merge(Env::prefixed(&format!("{env_prefix}__")).split("__"))
                .merge(Env::prefixed(&format!("{env_prefix}_")).split("__"));
        }
        let app_env: String = probe.extract_inner(crate::env::APP_ENV_KEY).ok()?;
        crate::env::non_blank(&app_env)
    }

    /// [`Config::new`] with the managed layer already read.
    fn build(opts: ConfigOptions, managed: Option<&ManagedLayer>) -> Result<Self, ConfigError> {
        for path in &opts.config_paths {
            Self::check_named_path(path)?;
        }
        let resolved_app_name = Self::resolve_app_name(opts.app_name.as_deref());
        let app_name_ref = resolved_app_name.as_deref();

        // Load .env files (home = global defaults, project = overrides).
        // Real env vars always win -- dotenvy doesn't overwrite.
        if opts.load_dotenv {
            Self::load_dotenv_cascade(Path::new("."), opts.load_home_dotenv);
        }

        // Build the cascade (lowest to highest priority)
        let mut figment = Figment::new();

        // 7. Hard-coded defaults (lowest priority)
        figment = figment.merge(Serialized::defaults(HardcodedDefaults::default()));

        let search = SearchPaths {
            extra: &opts.config_paths,
            standard: opts.search_standard_paths,
            app_name: app_name_ref,
        };

        // 6. defaults.yaml
        for path in Self::find_config_files("defaults", &search) {
            figment = figment.merge(YamlFile::new(path));
        }

        // 5. settings.yaml
        for path in Self::find_config_files("settings", &search) {
            figment = figment.merge(YamlFile::new(path));
        }

        // 4. settings.{env}.yaml, for the environment `get_app_env` resolves
        // once the cascade is set up.
        let app_env_default = opts.app_env.as_deref().and_then(crate::env::non_blank);
        let app_env = crate::env::app_env_from_vars()
            .or_else(|| Self::app_env_key(&figment, managed, &opts.env_prefix))
            .or_else(|| app_env_default.clone())
            .unwrap_or_else(|| crate::env::DEFAULT_APP_ENV.to_string());
        let env_settings = format!("settings.{app_env}");
        for path in Self::find_config_files(&env_settings, &search) {
            figment = figment.merge(YamlFile::new(path));
        }

        // Managed policy beats every file, and env vars beat it.
        if let Some(layer) = managed {
            figment = figment.merge(layer);
        }

        // 3. .env file values are already loaded into env vars

        // 2. Environment variables (with prefix)
        // Keys are lowercased: TEST_DATABASE_HOST -> database_host
        // Use double underscore for nesting: TEST_DATABASE__HOST -> database.host
        //
        // Both separators after the prefix are accepted. Stripping only
        // `{prefix}_` leaves a leading underscore on the nested spelling --
        // TEST__DATABASE__HOST becomes `_database.host`, a key nothing reads --
        // and that is the spelling charts emit for secret env vars.
        let mut env_text = EnvText::default();
        if !opts.env_prefix.is_empty() {
            let nested = Env::prefixed(&format!("{}__", opts.env_prefix)).split("__");
            figment = env_text.merge(figment, nested);
            let single = Env::prefixed(&format!("{}_", opts.env_prefix)).split("__");
            figment = env_text.merge(figment, single);
        }

        // 1. CLI args would be merged by the application via merge_cli()

        Ok(Self {
            figment,
            env_prefix: opts.env_prefix,
            app_env_default,
            env_text,
        })
    }

    /// `ConfigOptions::app_env`, the app's own default app environment, when
    /// it holds a value.
    pub(crate) fn app_env_default(&self) -> Option<&str> {
        self.app_env_default.as_deref()
    }

    /// Load `.env` files: `~/.env` (global defaults) then `project_dir/.env`
    /// (overrides). `dotenvy` doesn't overwrite, so load in reverse --
    /// project first wins, home only fills gaps. Real env vars beat both.
    ///
    /// Only `project_dir` itself is read. `dotenvy::dotenv()` would search
    /// every parent up to `/` and load the first `.env` it found.
    fn load_dotenv_cascade(project_dir: &Path, load_home: bool) {
        use tracing::debug;

        // Project .env first -- these values take precedence.
        let project_env = project_dir.join(".env");
        match dotenvy::from_path(&project_env) {
            Ok(()) => {
                debug!(path = %project_env.display(), "Loaded project .env file");
            }
            Err(dotenvy::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                // No project .env, that's fine
            }
            Err(e) => {
                debug!(path = %project_env.display(), error = %e, "Failed to load project .env file");
            }
        }

        // Load home directory .env (only fills in missing values)
        if load_home && let Some(home) = dirs::home_dir() {
            let home_env = home.join(".env");
            if home_env.exists() {
                match dotenvy::from_path(&home_env) {
                    Ok(()) => {
                        debug!(path = %home_env.display(), "Loaded home .env file");
                    }
                    Err(e) => {
                        debug!(path = %home_env.display(), error = %e, "Failed to load home .env file");
                    }
                }
            }
        }
    }

    /// Refuse a path the operator named that the cascade cannot use: a typo or
    /// an unreadable file would otherwise be an empty layer, and the service
    /// would run on hard-coded defaults.
    fn check_named_path(path: &Path) -> Result<(), ConfigError> {
        let refused = |message: String| ConfigError::LoadError {
            path: path.to_path_buf(),
            message,
        };
        if !path.exists() {
            return Err(refused("no such file or directory".to_string()));
        }
        if path.is_file() {
            figment::Provider::data(&YamlFile::new(path.to_path_buf()))
                .map_err(|e| refused(e.to_string()))?;
        }
        Ok(())
    }

    /// Find config files with the given base name.
    ///
    /// Search order (all locations merged, later overrides earlier within
    /// the same cascade layer):
    /// 1. Current directory: `./{name}.yaml`
    /// 2. Project config subdir: `./config/{name}.yaml`
    /// 3. Container mount: `/config/{name}.yaml` (always checked)
    /// 4. User config: `~/.config/{app_name}/{name}.yaml` (when app_name set)
    /// 5. Extra paths from `ConfigOptions::config_paths`
    ///
    /// Locations 1 to 4 are the standard ones, searched only when
    /// `search.standard` is set.
    fn find_config_files(base_name: &str, search: &SearchPaths<'_>) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let extensions = ["yaml", "yml"];

        if search.standard {
            // 1. Current directory
            for ext in &extensions {
                let path = PathBuf::from(format!("{base_name}.{ext}"));
                if path.exists() {
                    files.push(path);
                    break;
                }
            }

            // 2. Project config subdirectory
            for ext in &extensions {
                let path = PathBuf::from(format!("config/{base_name}.{ext}"));
                if path.exists() {
                    files.push(path);
                    break;
                }
            }

            // 3. Container config mount (/config/)
            let container_config = PathBuf::from("/config");
            if container_config.is_dir() {
                for ext in &extensions {
                    let path = container_config.join(format!("{base_name}.{ext}"));
                    if path.exists() {
                        files.push(path);
                        break;
                    }
                }
            }

            // 4. User config directory (~/.config/{app_name}/)
            if let Some(name) = search.app_name
                && let Some(config_dir) = dirs::config_dir()
            {
                let user_config = config_dir.join(name);
                if user_config.is_dir() {
                    for ext in &extensions {
                        let path = user_config.join(format!("{base_name}.{ext}"));
                        if path.exists() {
                            files.push(path);
                            break;
                        }
                    }
                }
            }
        }

        // 5. Extra paths: a DIRECTORY to search, or a FILE named outright
        // (`--config` passes a file, which no base name can be joined onto).
        // A named file belongs to the settings layer only -- it is one document,
        // and it outranks defaults.yaml because the operator chose it.
        for base in search.extra {
            if base.is_file() {
                if base_name == "settings" {
                    files.push(base.clone());
                }
                continue;
            }
            for ext in &extensions {
                let path = base.join(format!("{base_name}.{ext}"));
                if path.exists() {
                    files.push(path);
                    break;
                }
            }
        }

        files
    }

    /// Merge CLI arguments into the configuration.
    ///
    /// Call this after parsing CLI args to add them as highest priority.
    #[must_use]
    pub fn merge_cli<T: serde::Serialize>(mut self, cli_args: T) -> Self {
        self.figment = self.figment.merge(Serialized::defaults(cli_args));
        self
    }

    /// Deserialise `T` at `key`, or the whole cascade.
    fn extract<T: DeserializeOwned>(&self, key: Option<&str>) -> Result<T, figment::Error> {
        self.env_text.extract(&self.figment, key)
    }

    /// Get a string value.
    #[must_use]
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.extract(Some(key)).ok()
    }

    /// Get an integer value.
    #[must_use]
    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.extract(Some(key)).ok()
    }

    /// Get a float value.
    #[must_use]
    pub fn get_float(&self, key: &str) -> Option<f64> {
        self.extract(Some(key)).ok()
    }

    /// Get a boolean value.
    #[must_use]
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.extract(Some(key)).ok()
    }

    /// Get a duration value (parses strings like "30s", "5m", "1h").
    #[must_use]
    pub fn get_duration(&self, key: &str) -> Option<Duration> {
        let value = self.get_string(key)?;
        parse_duration(&value)
    }

    /// Get a list of strings.
    #[must_use]
    pub fn get_string_list(&self, key: &str) -> Option<Vec<String>> {
        self.extract(Some(key)).ok()
    }

    /// Check if a key exists.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.figment.find_value(key).is_ok()
    }

    /// Deserialise the entire configuration into a typed struct.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialisation fails.
    pub fn unmarshal<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        self.extract(None).map_err(ConfigError::ExtractError)
    }

    /// Deserialise a specific key into a typed struct.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialisation fails.
    pub fn unmarshal_key<T: DeserializeOwned>(&self, key: &str) -> Result<T, ConfigError> {
        self.extract(Some(key)).map_err(ConfigError::ExtractError)
    }

    /// Deserialise a specific key and auto-register it in the config registry.
    ///
    /// Same as [`unmarshal_key`](Self::unmarshal_key) but also records the
    /// section in the global [`registry`] for reflection. The type must
    /// implement `Serialize + Default` so the registry can capture both
    /// the effective and default values.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialisation fails. The section is only
    /// registered on success.
    pub fn unmarshal_key_registered<T>(&self, key: &str) -> Result<T, ConfigError>
    where
        T: DeserializeOwned + serde::Serialize + Default + 'static,
    {
        let value: T = self.unmarshal_key(key)?;
        registry::register::<T>(key, &value);
        Ok(value)
    }

    /// Get the environment variable prefix.
    #[must_use]
    pub fn env_prefix(&self) -> &str {
        &self.env_prefix
    }
}

/// Hard-coded default values (lowest priority in cascade).
#[derive(Debug, serde::Serialize)]
struct HardcodedDefaults {
    log_level: String,
    log_format: String,
}

impl Default for HardcodedDefaults {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            log_format: "auto".to_string(),
        }
    }
}

/// Parse a duration string like "30s", "5m", "1h", "2h30m".
fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();

    // Try simple formats first
    if let Some(secs) = s.strip_suffix('s') {
        return secs.parse::<u64>().ok().map(Duration::from_secs);
    }
    if let Some(mins) = s.strip_suffix('m') {
        return mins
            .parse::<u64>()
            .ok()
            .map(|m| Duration::from_secs(m * 60));
    }
    if let Some(hours) = s.strip_suffix('h') {
        return hours
            .parse::<u64>()
            .ok()
            .map(|h| Duration::from_secs(h * 3600));
    }

    // Try parsing as seconds
    s.parse::<u64>().ok().map(Duration::from_secs)
}

// Global singleton functions

/// Initialise the global configuration.
///
/// # Errors
///
/// Returns an error if configuration loading fails or if already initialised.
pub fn setup(opts: ConfigOptions) -> Result<(), ConfigError> {
    let config = Config::new(opts)?;
    CONFIG
        .set(config)
        .map_err(|_| ConfigError::AlreadyInitialised)?;
    // Resolved once the cascade can name it, so a default posture is logged at load.
    let _ = get_app_env();
    Ok(())
}

/// The global configuration once [`setup`] has run, with no warning when it
/// has not.
pub(crate) fn loaded() -> Option<&'static Config> {
    CONFIG.get()
}

/// Get the global configuration.
///
/// # Panics
///
/// Panics if configuration has not been initialised.
#[must_use]
pub fn get() -> &'static Config {
    CONFIG
        .get()
        .expect("configuration not initialised - call config::setup() first")
}

/// Try to get the global configuration.
///
/// Warns once per process when the cascade was never initialised. Every
/// `from_cascade()` reader falls back to its own default in that case, which is
/// indistinguishable from a cascade that genuinely says "default" -- so without
/// this line an app runs entirely on hard-coded values and reports nothing.
#[must_use]
pub fn try_get() -> Option<&'static Config> {
    let cfg = CONFIG.get();
    if cfg.is_none() && !CASCADE_ABSENT_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::warn!(
            "config cascade not initialised: config::setup() has not been called, so every \
             from_cascade() reader is falling back to its own default and no file or \
             environment value can reach it"
        );
    }
    cfg
}

/// Guards the one-shot warning in [`try_get`].
static CASCADE_ABSENT_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("1s"), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m"), Some(Duration::from_mins(5)));
        assert_eq!(parse_duration("1m"), Some(Duration::from_mins(1)));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h"), Some(Duration::from_hours(1)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_hours(2)));
    }

    #[test]
    fn test_parse_duration_plain_number() {
        assert_eq!(parse_duration("60"), Some(Duration::from_mins(1)));
    }

    #[test]
    fn test_config_options_default() {
        let opts = ConfigOptions::default();
        assert_eq!(opts.env_prefix, "");
        assert!(opts.app_env.is_none());
        assert!(opts.app_name.is_none());
        assert_eq!(opts.config_paths, [] as [std::path::PathBuf; 0]);
        assert!(opts.search_standard_paths);
        assert!(opts.load_dotenv);
        assert!(!opts.load_home_dotenv);
        assert!(opts.managed.is_none());
    }

    #[test]
    fn test_config_new() {
        let config = Config::new(ConfigOptions::default());
        assert!(config.is_ok());
    }

    #[test]
    fn test_config_hardcoded_defaults() {
        let config = Config::new(ConfigOptions::default()).unwrap();

        // Should have hardcoded defaults
        assert_eq!(config.get_string("log_level"), Some("info".to_string()));
        assert_eq!(config.get_string("log_format"), Some("auto".to_string()));
    }

    #[test]
    fn test_config_env_override() {
        // Env vars use double underscore for nesting: PREFIX_KEY__SUBKEY -> key.subkey
        // For flat keys, just use PREFIX_KEY -> key
        temp_env::with_var("TEST_HOST", Some("testhost"), || {
            let config = Config::new(ConfigOptions {
                env_prefix: "TEST".into(),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(config.get_string("host"), Some("testhost".to_string()));
        });
    }

    /// `app_env` in the files, the managed policy or a prefixed variable picks
    /// `settings.{env}.yaml` and names the environment, and the bare variables
    /// win over it.
    #[test]
    fn app_env_picks_the_settings_file_below_the_variables() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "app_env: staging\nmarker: base\n",
        )
        .unwrap();
        for env in ["staging", "production"] {
            std::fs::write(
                dir.path().join(format!("settings.{env}.yaml")),
                format!("marker: {env}\n"),
            )
            .unwrap();
        }
        let unset: [(&str, Option<&str>); 4] = [
            ("APP_ENV", None),
            ("ENVIRONMENT", None),
            ("ENV", None),
            ("APPENVTEST_APP_ENV", None),
        ];
        // (variable set, marker loaded and app_env the cascade gives)
        let cases = [
            (None, "staging", "staging"),
            (Some(("APP_ENV", "production")), "production", "staging"),
            (
                Some(("APPENVTEST_APP_ENV", "production")),
                "production",
                "production",
            ),
            (Some(("ENV", "production")), "production", "staging"),
        ];
        for (var, marker, key) in cases {
            let vars = unset.map(|(name, _)| {
                (
                    name,
                    var.and_then(|(set, value)| (set == name).then_some(value)),
                )
            });
            temp_env::with_vars(vars, || {
                let config = Config::new(ConfigOptions {
                    env_prefix: "APPENVTEST".into(),
                    config_paths: vec![dir.path().to_path_buf()],
                    search_standard_paths: false,
                    load_dotenv: false,
                    ..ConfigOptions::default()
                })
                .unwrap();
                assert_eq!(
                    config.get_string("marker").as_deref(),
                    Some(marker),
                    "{var:?}"
                );
                assert_eq!(
                    config.get_string("app_env").as_deref(),
                    Some(key),
                    "{var:?}"
                );
            });
        }
        temp_env::with_vars(unset, || {
            setup(ConfigOptions {
                env_prefix: "APPENVTEST".into(),
                config_paths: vec![dir.path().to_path_buf()],
                search_standard_paths: false,
                load_dotenv: false,
                ..ConfigOptions::default()
            })
            .unwrap();
            assert_eq!(get_app_env(), "staging", "the loaded cascade names it");
        });
    }

    /// The absent-cascade warning fires at most once per process.
    ///
    /// A reader consulted in a hot path would otherwise flood the log, and a
    /// flooded warning is ignored as reliably as a missing one.
    #[test]
    fn cascade_absent_warning_is_one_shot() {
        CASCADE_ABSENT_WARNED.store(false, std::sync::atomic::Ordering::Relaxed);

        // First call claims the warning, every later call must not.
        let claimed_first = !CASCADE_ABSENT_WARNED.load(std::sync::atomic::Ordering::Relaxed);
        let _ = try_get();
        let armed_after_first = CASCADE_ABSENT_WARNED.load(std::sync::atomic::Ordering::Relaxed);
        let _ = try_get();
        let armed_after_second = CASCADE_ABSENT_WARNED.load(std::sync::atomic::Ordering::Relaxed);

        assert!(
            claimed_first,
            "flag must start clear for this test to mean anything"
        );
        assert!(
            armed_after_first || try_get().is_some(),
            "a try_get with no cascade must arm the one-shot flag"
        );
        assert!(
            armed_after_second,
            "the flag must stay armed, so the warning cannot repeat"
        );
    }

    /// The nested env spelling must reach the key it names.
    ///
    /// `Env::prefixed` strips exactly `{prefix}_`, so `TEST__DATABASE__HOST`
    /// resolved to `_database.host` -- a leading-underscore key nothing reads
    /// and serde drops. That is the spelling the contract's secret groups use for
    /// env vars, so Kafka SASL credentials, ClickHouse passwords and cloud
    /// credentials all arrived at a key no app consults.
    #[test]
    fn nested_env_spelling_reaches_the_key_it_names() {
        temp_env::with_var("TEST__DATABASE__HOST", Some("nested-host"), || {
            let config = Config::new(ConfigOptions {
                env_prefix: "TEST".into(),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(
                config.get_string("database.host"),
                Some("nested-host".to_string()),
                "the double-underscore spelling did not reach database.host"
            );
        });
    }

    /// The single-underscore spelling keeps working.
    #[test]
    fn single_underscore_nesting_still_reaches_its_key() {
        temp_env::with_var("TEST_DATABASE__HOST", Some("single-host"), || {
            let config = Config::new(ConfigOptions {
                env_prefix: "TEST".into(),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(
                config.get_string("database.host"),
                Some("single-host".to_string()),
                "the single-underscore spelling stopped resolving"
            );
        });
    }

    /// Every reader gives a field that refuses an env var's parsed value the
    /// variable's text, at a leaf key, a section and the root. A value a
    /// higher layer set is not replaced by the env text beneath it.
    #[test]
    fn every_reader_gives_a_string_field_the_env_text() {
        #[derive(Debug, Default, serde::Deserialize)]
        #[serde(default)]
        struct Kafka {
            password: String,
            port: u16,
        }
        #[derive(Debug, Default, serde::Deserialize)]
        #[serde(default)]
        struct Root {
            kafka: Kafka,
        }

        let vars = [
            ("READTEST__KAFKA__PASSWORD", Some("0123")),
            ("READTEST_KAFKA__PORT", Some("0123")),
        ];
        temp_env::with_vars(vars, || {
            let config = Config::new(ConfigOptions {
                env_prefix: "READTEST".into(),
                search_standard_paths: false,
                load_dotenv: false,
                ..Default::default()
            })
            .unwrap();
            assert_eq!(config.get_string("kafka.password").as_deref(), Some("0123"));
            assert_eq!(config.get_int("kafka.port"), Some(123));
            let kafka: Kafka = config.unmarshal_key("kafka").unwrap();
            assert_eq!((kafka.password.as_str(), kafka.port), ("0123", 123));
            let root: Root = config.unmarshal().unwrap();
            assert_eq!(
                (root.kafka.password.as_str(), root.kafka.port),
                ("0123", 123)
            );

            let cli = std::collections::BTreeMap::from([(
                "kafka",
                std::collections::BTreeMap::from([("password", 7)]),
            )]);
            let config = config.merge_cli(cli);
            assert_eq!(config.get_string("kafka.password"), None, "a CLI number");
        });
    }

    /// `--config <file>` must reach the cascade.
    ///
    /// `CommonArgs::to_config_options` pushes the named FILE into
    /// `config_paths`, and this function joined a base name onto every entry --
    /// so a file was searched as `.../config.yaml/settings.yaml`, which cannot
    /// exist. Every container entrypoint passes `--config`, so the file never
    /// loaded and the cascade silently answered from hard-coded defaults.
    #[test]
    fn named_config_file_reaches_the_cascade() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, "host: from-the-named-file\n").unwrap();

        let config = Config::new(ConfigOptions {
            config_paths: vec![file],
            ..Default::default()
        })
        .unwrap();

        assert_eq!(
            config.get_string("host"),
            Some("from-the-named-file".to_string()),
            "a file passed as --config did not reach the cascade"
        );
    }

    /// A named config path the cascade cannot read refuses the setup, rather
    /// than leaving every reader on its hard-coded default.
    #[cfg(unix)]
    #[test]
    fn a_named_config_path_it_cannot_read_is_an_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let unreadable = dir.path().join("unreadable.yaml");
        std::fs::write(&unreadable, "host: x\n").unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
        let malformed = dir.path().join("malformed.yaml");
        std::fs::write(&malformed, "a: 1\na: 2\n").unwrap();

        let mut cases = vec![
            ("missing file", dir.path().join("typo.yaml")),
            ("missing directory", dir.path().join("no-such-dir")),
            ("malformed file", malformed),
        ];
        // Root reads a mode-000 file, so the case proves nothing there.
        if std::fs::read(&unreadable).is_err() {
            cases.push(("unreadable file", unreadable));
        }
        for (name, path) in cases {
            let result = Config::new(ConfigOptions {
                config_paths: vec![path.clone()],
                ..Default::default()
            });
            match result {
                Err(ConfigError::LoadError { path: got, .. }) => assert_eq!(got, path, "{name}"),
                Err(other) => panic!("{name}: expected LoadError, got {other:?}"),
                Ok(_) => panic!("{name}: {} loaded", path.display()),
            }
        }
    }

    /// A `.env` above the project directory is never read.
    ///
    /// `dotenvy::dotenv()` searched every parent up to `/`, so an app started
    /// anywhere under `$HOME` exported every secret in `~/.env`.
    #[test]
    fn dotenv_above_the_project_dir_is_not_loaded() {
        let root = tempfile::tempdir().unwrap();
        let project = root.path().join("project");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(root.path().join(".env"), "SCALO_DOTENV_PARENT=loaded\n").unwrap();

        temp_env::with_var_unset("SCALO_DOTENV_PARENT", || {
            Config::load_dotenv_cascade(&project, false);
            assert_eq!(
                std::env::var("SCALO_DOTENV_PARENT").ok(),
                None,
                "a .env in a parent directory was loaded"
            );
        });
    }

    /// The project directory's own `.env` still loads.
    #[test]
    fn dotenv_in_the_project_dir_is_loaded() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".env"), "SCALO_DOTENV_PROJECT=loaded\n").unwrap();

        temp_env::with_var_unset("SCALO_DOTENV_PROJECT", || {
            Config::load_dotenv_cascade(dir.path(), false);
            assert_eq!(
                std::env::var("SCALO_DOTENV_PROJECT").ok().as_deref(),
                Some("loaded"),
                "the project .env did not load"
            );
        });
    }

    /// A directory entry keeps searching for {base_name}.yaml inside it.
    #[test]
    fn directory_config_path_still_searches_for_base_names() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("settings.yaml"), "host: from-the-dir\n").unwrap();

        let config = Config::new(ConfigOptions {
            config_paths: vec![dir.path().to_path_buf()],
            ..Default::default()
        })
        .unwrap();

        assert_eq!(
            config.get_string("host"),
            Some("from-the-dir".to_string()),
            "a directory in config_paths stopped resolving settings.yaml"
        );
    }
}
