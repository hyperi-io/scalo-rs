// Project:   scalo
// File:      src/config/load.rs
// Purpose:   Typed app-config loader: YAML file or cascade, env overrides, normalise
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`load_typed`]: the app's own config struct, from the file it was started
//! with or from the cascade, with environment overrides applied.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use figment::Figment;
use figment::providers::{Env, Serialized};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::flat_env::{ApplyFlatEnv, Normalize};
use super::{Config, ConfigError, ConfigOptions, setup};
use crate::yaml::{self, Profile};

/// Load an app's config struct.
///
/// With `path`, the named YAML file is the config. It is parsed with
/// [`yaml::Profile::Config`], and every key no field of `T` takes is logged:
/// a nested key at WARN, since a misspelt key does nothing, and a top-level
/// key at INFO, since it may be a section the scalo runtime reads. Called
/// before logging is set up, these lines are held until [`log_deferred`] runs,
/// which `logger::setup` does. With no `path`, `T` is read from the 7-layer
/// cascade.
///
/// Then, in order:
///
/// 1. `{env_prefix}_` variables nest with `__`, after either separator:
///    `APP__KAFKA__BROKERS` and `APP_KAFKA__BROKERS` both set
///    `kafka.brokers`. An empty variable counts as unset.
/// 2. [`ApplyFlatEnv`] applies the flat spellings (`APP_KAFKA_BROKERS`).
/// 3. [`Normalize`] fills in implied settings.
///
/// The global cascade is also set up, with the file as its settings layer, so
/// the sections the scalo runtime reads (`metrics`, `version_check`, ...)
/// resolve from the same file. A later call, such as a reload, keeps the
/// first cascade. `.env` is read only when no `path` is given, so a working
/// tree's `.env` never reaches a deployment that names its file.
///
/// Step 1 round-trips `T` through `Serialize` when a prefixed variable is set,
/// with secrets exposed for the round trip. A `#[serde(skip)]` field comes back
/// at its default, so set such fields after this call.
///
/// # Errors
///
/// [`ConfigError::Yaml`] when the named file is missing, unreadable or does
/// not parse into `T`. [`ConfigError::ExtractError`] when the cascade or an
/// environment override does not fit `T`.
pub fn load_typed<T>(path: Option<&str>, env_prefix: &str) -> Result<T, ConfigError>
where
    T: DeserializeOwned + Serialize + ApplyFlatEnv + Normalize,
{
    let options = ConfigOptions {
        env_prefix: env_prefix.to_string(),
        config_paths: path.map(PathBuf::from).into_iter().collect(),
        load_dotenv: path.is_none(),
        ..ConfigOptions::default()
    };
    // The file is read first so its errors are `Yaml`, located, before the
    // cascade refuses the same file.
    let from_file: Option<T> = path.map(|path| read_file(Path::new(path))).transpose()?;
    match setup(options.clone()) {
        Ok(()) | Err(ConfigError::AlreadyInitialised) => {}
        Err(e) => return Err(e),
    }

    let mut config: T = match from_file {
        Some(config) => config,
        None => Config::new(options)?.unmarshal()?,
    };
    merge_nested_env(&mut config, env_prefix)?;
    config.apply_flat_env(env_prefix);
    config.normalize();
    Ok(config)
}

/// Parse the named file into `T`, logging the keys it skipped and the lints.
fn read_file<T: DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
    let loaded = yaml::from_path_reporting::<T>(path, Profile::Config)?;
    let file = path.display().to_string();
    for key in loaded.unknown_keys {
        let path = file.clone();
        if key.contains('.') {
            FileNote::UnknownKey { key, path }
        } else {
            FileNote::RuntimeKey { key, path }
        }
        .log_or_hold();
    }
    for lint in loaded.lints {
        FileNote::Lint {
            path: file.clone(),
            lint,
        }
        .log_or_hold();
    }
    Ok(loaded.value)
}

/// Notes [`load_typed`] made before any subscriber existed, for [`log_deferred`].
static HELD: Mutex<Vec<FileNote>> = Mutex::new(Vec::new());

/// A log line about the config file [`load_typed`] read.
enum FileNote {
    /// A nested key no field took.
    UnknownKey { key: String, path: String },
    /// A top-level key no field took, which a runtime section may read.
    RuntimeKey { key: String, path: String },
    /// A lint the parse raised.
    Lint { path: String, lint: yaml::Lint },
}

impl FileNote {
    fn log(&self) {
        match self {
            Self::UnknownKey { key, path } => {
                tracing::warn!(
                    key,
                    path,
                    "unknown config key ignored -- check its spelling"
                );
            }
            Self::RuntimeKey { key, path } => {
                tracing::info!(key, path, "top-level config key left to the runtime");
            }
            Self::Lint { path, lint } => {
                tracing::warn!(
                    path,
                    line = lint.line,
                    column = lint.column,
                    "{}",
                    lint.message
                );
            }
        }
    }

    /// Log now when a subscriber exists, else hold the note for [`log_deferred`].
    fn log_or_hold(self) {
        let subscribed = tracing::dispatcher::get_default(|current| {
            !current.is::<tracing::subscriber::NoSubscriber>()
        });
        if subscribed {
            self.log();
        } else {
            HELD.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(self);
        }
    }
}

/// Log what [`load_typed`] noted before a subscriber existed.
///
/// A service loads its config before it sets up logging, because the logger
/// reads its own settings from that config. `logger::setup` calls this once
/// its subscriber is installed. An app that installs a subscriber of its
/// own calls it after doing so.
pub fn log_deferred() {
    let held = std::mem::take(&mut *HELD.lock().unwrap_or_else(PoisonError::into_inner));
    for note in &held {
        note.log();
    }
}

/// Merge `{prefix}_...` variables onto `config` with `__` nesting.
///
/// figment's `prefixed` strips exactly `{prefix}_`, so `{prefix}__KAFKA__X`
/// arrives as `_kafka.x`, a key nothing reads. Trimming the separator left
/// behind makes both spellings reach `kafka.x`, and no key starts with `_`.
fn merge_nested_env<T>(config: &mut T, env_prefix: &str) -> Result<(), ConfigError>
where
    T: DeserializeOwned + Serialize,
{
    let prefix = format!("{env_prefix}_");
    let set = std::env::vars_os().any(|(key, value)| {
        !value.is_empty() && key.to_str().is_some_and(|key| key.starts_with(&prefix))
    });
    if !set {
        return Ok(());
    }

    // Secrets serialise as the mask outside an expose window, which would
    // overwrite them with it.
    let merged = crate::sensitive::expose_during(|| {
        let env = Env::raw()
            .filter_map(move |key| {
                if !key.starts_with(&prefix)
                    || std::env::var(key.as_str()).is_ok_and(|v| v.is_empty())
                {
                    return None;
                }
                Some(key.as_str()[prefix.len()..].trim_start_matches('_').into())
            })
            .split("__");
        Figment::from(Serialized::defaults(&*config))
            .merge(env)
            .extract::<T>()
    })?;
    *config = merged;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::SensitiveString;

    #[derive(Debug, Default, Deserialize, Serialize)]
    #[serde(default)]
    struct App {
        name: String,
        kafka: Kafka,
        sasl_enabled: bool,
    }

    #[derive(Debug, Default, Deserialize, Serialize)]
    #[serde(default)]
    struct Kafka {
        brokers: Vec<String>,
        username: String,
        password: SensitiveString,
    }

    impl ApplyFlatEnv for App {
        fn apply_flat_env(&mut self, prefix: &str) {
            if let Some(name) = super::super::flat_env::flat_env_string(prefix, "NAME") {
                self.name = name;
            }
        }
    }

    impl Normalize for App {
        fn normalize(&mut self) {
            self.sasl_enabled = !self.kafka.username.is_empty();
        }
    }

    fn file(text: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.yaml");
        std::fs::write(&path, text).unwrap();
        (dir, path.display().to_string())
    }

    /// The file, nested env, flat env and normalise all reach the struct, and a
    /// secret from the file survives the env round trip.
    #[test]
    fn file_then_nested_then_flat_then_normalise() {
        let (_dir, path) = file("name: from-file\nkafka:\n  brokers: [a]\n  password: hunter2\n");
        temp_env::with_vars(
            [
                ("LOADTEST__KAFKA__USERNAME", Some("svc")),
                ("LOADTEST_KAFKA__BROKERS", Some("[b, c]")),
                ("LOADTEST_NAME", Some("from-flat-env")),
                ("LOADTEST__KAFKA__PASSWORD", Some("")),
            ],
            || {
                let app: App = load_typed(Some(&path), "LOADTEST").unwrap();
                assert_eq!(app.name, "from-flat-env");
                assert_eq!(app.kafka.brokers, ["b", "c"]);
                assert_eq!(app.kafka.username, "svc");
                assert_eq!(
                    app.kafka.password.expose(),
                    "hunter2",
                    "an empty variable is unset"
                );
                assert!(app.sasl_enabled, "normalize ran last");
            },
        );
    }

    #[test]
    fn a_missing_named_file_is_an_error() {
        let err = load_typed::<App>(Some("/nonexistent/app.yaml"), "LOADTEST").unwrap_err();
        assert!(matches!(err, ConfigError::Yaml(_)), "{err:?}");
        assert!(err.to_string().contains("/nonexistent/app.yaml"), "{err}");
    }

    #[test]
    fn a_parse_error_is_located_not_defaulted() {
        let (_dir, path) = file("name: a\nkafka:\n  brokers: 7\n");
        let ConfigError::Yaml(err) = load_typed::<App>(Some(&path), "LOADTEST").unwrap_err() else {
            panic!("expected a YAML error");
        };
        assert_eq!(err.line, Some(3));
    }

    /// No `path` reads the cascade, nested env included.
    #[test]
    fn no_path_reads_the_cascade() {
        temp_env::with_var("LOADTEST__KAFKA__USERNAME", Some("cascade-user"), || {
            let app: App = load_typed(None, "LOADTEST").unwrap();
            assert_eq!(app.kafka.username, "cascade-user");
            assert!(app.sasl_enabled);
        });
    }

    /// A subscriber writing into a buffer the test reads back.
    #[cfg(feature = "logger")]
    fn capture() -> (
        impl tracing::Subscriber + Send + Sync,
        std::sync::Arc<Mutex<Vec<u8>>>,
    ) {
        use std::sync::Arc;
        use tracing_subscriber::layer::SubscriberExt;

        struct Sink(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buf);
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || Sink(Arc::clone(&writer))),
        );
        (subscriber, buf)
    }

    /// With no subscriber the note is held, and `log_deferred` logs it once a
    /// subscriber exists.
    #[cfg(feature = "logger")]
    #[test]
    fn a_note_made_before_any_subscriber_is_logged_by_log_deferred() {
        let (_dir, path) = file("name: a\nkafka:\n  brokerz: [b]\n");
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            load_typed::<App>(Some(&path), "LOADTEST").unwrap();
        });
        assert_eq!(HELD.lock().unwrap().len(), 1, "the note is held");

        let (subscriber, buf) = capture();
        tracing::subscriber::with_default(subscriber, log_deferred);

        let out = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        assert!(out.contains(" WARN "), "{out}");
        assert!(out.contains("unknown config key ignored"), "{out}");
        assert!(out.contains("kafka.brokerz"), "{out}");
        assert!(HELD.lock().unwrap().is_empty(), "the held note is spent");
    }

    /// With a subscriber in place, as on a reload, the note is logged at once.
    #[cfg(feature = "logger")]
    #[test]
    fn a_note_made_with_a_subscriber_is_logged_at_once() {
        let (_dir, path) = file("name: a\nkafka:\n  brokerz: [b]\n");
        let (subscriber, buf) = capture();
        tracing::subscriber::with_default(subscriber, || {
            load_typed::<App>(Some(&path), "LOADTEST").unwrap();
        });

        let out = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        assert!(out.contains("kafka.brokerz"), "{out}");
        assert!(HELD.lock().unwrap().is_empty(), "nothing is held");
    }
}
