// Project:   scalo
// File:      src/runtime.rs
// Purpose:   Container-aware runtime path management
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Runtime path management.
//!
//! Provides container-aware path resolution that works identically in
//! Kubernetes, Docker, and local development environments.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use crate::env::Environment;

/// Default container base path.
const CONTAINER_BASE_PATH: &str = "/app";

/// App name for the bare-metal directories when no explicit name, `APP_NAME` or program name is found.
const DEFAULT_APP_NAME: &str = "app";

/// Standard application paths based on runtime environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    /// Read-only configuration directory (ConfigMap in K8s, ~/.config locally)
    pub config_dir: PathBuf,
    /// Read-only secrets directory (Secret in K8s, ~/.{app}/secrets locally)
    pub secrets_dir: PathBuf,
    /// Persistent data directory (PVC in K8s, ~/.local/share locally)
    pub data_dir: PathBuf,
    /// Ephemeral temporary directory (EmptyDir in K8s, /tmp locally)
    pub temp_dir: PathBuf,
    /// Application logs directory
    pub logs_dir: PathBuf,
    /// Cache directory
    pub cache_dir: PathBuf,
    /// Runtime directory (PID files, sockets)
    pub run_dir: PathBuf,
}

impl RuntimePaths {
    /// Discover paths based on auto-detected environment.
    #[must_use]
    pub fn discover() -> Self {
        Self::discover_for(Environment::detect())
    }

    /// Discover paths for a specific environment.
    ///
    /// On bare metal the directories are named for `APP_NAME`, else the
    /// program name, else `app`; see [`discover_for_app`](Self::discover_for_app).
    #[must_use]
    pub fn discover_for(env: Environment) -> Self {
        Self::discover_for_app(env, None)
    }

    /// Discover paths for `env`, naming the bare-metal directories `app_name`.
    ///
    /// The name resolves from `app_name`, then `APP_NAME`, then the program
    /// name (the file stem of `argv[0]`, underscores turned into hyphens),
    /// then `app`. A blank value counts as unset, and an empty `argv[0]` or
    /// one starting with `-` names no program. Container paths take no name.
    #[must_use]
    pub fn discover_for_app(env: Environment, app_name: Option<&str>) -> Self {
        match env {
            Environment::Kubernetes | Environment::Docker | Environment::Container => {
                Self::container_paths()
            }
            Environment::BareMetal => Self::local_paths(&resolve_app_name(app_name)),
        }
    }

    /// Get paths for container environments.
    fn container_paths() -> Self {
        let base = std::env::var("CONTAINER_BASE_PATH")
            .unwrap_or_else(|_| CONTAINER_BASE_PATH.to_string());
        let base_path = PathBuf::from(&base);

        Self {
            config_dir: base_path.join("config"),
            secrets_dir: base_path.join("secrets"),
            data_dir: base_path.join("data"),
            temp_dir: base_path.join("tmp"),
            logs_dir: base_path.join("logs"),
            cache_dir: base_path.join("cache"),
            run_dir: base_path.join("run"),
        }
    }

    /// Get paths for local development (XDG-compliant).
    fn local_paths(app_name: &str) -> Self {
        // Use dirs crate for XDG-compliant paths
        let config_dir = dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("~/.config"))
            .join(app_name);

        let data_dir = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("~/.local/share"))
            .join(app_name);

        let cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("~/.cache"))
            .join(app_name);

        let home_dir = dirs::home_dir().unwrap_or_else(|| PathBuf::from("~"));

        Self {
            config_dir,
            secrets_dir: home_dir.join(format!(".{app_name}")).join("secrets"),
            data_dir: data_dir.clone(),
            temp_dir: std::env::temp_dir().join(app_name),
            logs_dir: data_dir.join("logs"),
            cache_dir,
            run_dir: dirs::runtime_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(app_name),
        }
    }

    /// Create all directories if they don't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails.
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.config_dir)?;
        std::fs::create_dir_all(&self.secrets_dir)?;
        std::fs::create_dir_all(&self.data_dir)?;
        std::fs::create_dir_all(&self.temp_dir)?;
        std::fs::create_dir_all(&self.logs_dir)?;
        std::fs::create_dir_all(&self.cache_dir)?;
        std::fs::create_dir_all(&self.run_dir)?;
        Ok(())
    }

    /// Check if all required directories exist.
    #[must_use]
    pub fn all_exist(&self) -> bool {
        self.config_dir.exists()
            && self.secrets_dir.exists()
            && self.data_dir.exists()
            && self.temp_dir.exists()
            && self.logs_dir.exists()
            && self.cache_dir.exists()
            && self.run_dir.exists()
    }
}

impl Default for RuntimePaths {
    fn default() -> Self {
        Self::discover()
    }
}

/// The bare-metal app name: `explicit`, then `APP_NAME`, then the program name, then [`DEFAULT_APP_NAME`].
fn resolve_app_name(explicit: Option<&str>) -> String {
    app_name_from(
        explicit,
        std::env::var("APP_NAME").ok().as_deref(),
        std::env::args_os().next().as_deref(),
    )
}

/// [`resolve_app_name`] over given inputs; a blank name counts as unset.
fn app_name_from(
    explicit: Option<&str>,
    app_name_var: Option<&str>,
    argv0: Option<&OsStr>,
) -> String {
    [explicit, app_name_var]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|name| !name.is_empty())
        .map(str::to_string)
        .or_else(|| argv0.and_then(program_name))
        .unwrap_or_else(|| DEFAULT_APP_NAME.to_string())
}

/// The file stem of `argv0` with underscores as hyphens; `None` when it is empty, starts with `-` or has no UTF-8 stem.
fn program_name(argv0: &OsStr) -> Option<String> {
    if matches!(argv0.as_encoded_bytes().first(), None | Some(b'-')) {
        return None;
    }
    let stem = Path::new(argv0).file_stem()?.to_str()?;
    (!stem.is_empty()).then(|| stem.replace('_', "-"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_paths() {
        let paths = RuntimePaths::discover_for(Environment::Kubernetes);

        assert_eq!(paths.config_dir, PathBuf::from("/app/config"));
        assert_eq!(paths.secrets_dir, PathBuf::from("/app/secrets"));
        assert_eq!(paths.data_dir, PathBuf::from("/app/data"));
        assert_eq!(paths.temp_dir, PathBuf::from("/app/tmp"));
        assert_eq!(paths.logs_dir, PathBuf::from("/app/logs"));
        assert_eq!(paths.cache_dir, PathBuf::from("/app/cache"));
        assert_eq!(paths.run_dir, PathBuf::from("/app/run"));
    }

    #[test]
    fn test_docker_uses_container_paths() {
        let docker_paths = RuntimePaths::discover_for(Environment::Docker);
        let k8s_paths = RuntimePaths::discover_for(Environment::Kubernetes);

        // Docker and K8s should use same container paths
        assert_eq!(docker_paths, k8s_paths);
    }

    #[test]
    fn test_local_paths_use_xdg() {
        let paths = RuntimePaths::discover_for(Environment::BareMetal);

        // Local paths should be in home directory, not /app
        assert!(!paths.config_dir.starts_with("/app"));
        assert!(!paths.data_dir.starts_with("/app"));
    }

    #[test]
    fn test_custom_container_base_path() {
        temp_env::with_var("CONTAINER_BASE_PATH", Some("/custom"), || {
            let paths = RuntimePaths::discover_for(Environment::Docker);
            assert_eq!(paths.config_dir, PathBuf::from("/custom/config"));
        });
    }

    #[test]
    fn app_name_resolves_explicit_then_app_name_then_program() {
        let program = Some(OsStr::new("/usr/local/bin/data_sync"));
        assert_eq!(
            app_name_from(Some("explicit"), Some("from-env"), program),
            "explicit"
        );
        assert_eq!(app_name_from(None, Some("from-env"), program), "from-env");
        assert_eq!(app_name_from(None, None, program), "data-sync");
        assert_eq!(
            app_name_from(None, None, Some(OsStr::new("my-app"))),
            "my-app"
        );
        assert_eq!(app_name_from(None, None, None), "app");
    }

    #[test]
    fn a_blank_name_falls_through_to_the_next_source() {
        let program = Some(OsStr::new("/opt/bin/worker"));
        assert_eq!(
            app_name_from(Some("  "), Some("from-env"), program),
            "from-env"
        );
        assert_eq!(app_name_from(Some(""), Some(" \n"), program), "worker");
        assert_eq!(app_name_from(None, Some(" padded "), program), "padded");
    }

    /// An argv[0] that names no program falls back to the default, never to a flag or an empty name.
    #[test]
    fn an_argv0_that_names_no_program_gives_the_default() {
        for value in ["", "-", "-bash", "--flag", "/", "."] {
            assert_eq!(
                app_name_from(None, None, Some(OsStr::new(value))),
                "app",
                "argv[0] {value:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_non_utf8_program_name_gives_the_default() {
        use std::os::unix::ffi::OsStrExt as _;
        let argv0 = OsStr::from_bytes(b"/usr/bin/\xff\xfe");
        assert_eq!(app_name_from(None, None, Some(argv0)), "app");
    }

    #[test]
    fn the_local_paths_carry_the_resolved_name() {
        let paths = RuntimePaths::discover_for_app(Environment::BareMetal, Some("my-app"));
        assert!(paths.config_dir.ends_with("my-app"), "{paths:?}");
        assert!(paths.data_dir.ends_with("my-app"), "{paths:?}");
        assert!(paths.secrets_dir.ends_with(".my-app/secrets"), "{paths:?}");
        assert_eq!(paths.temp_dir, std::env::temp_dir().join("my-app"));

        temp_env::with_var("APP_NAME", Some("from-env"), || {
            let paths = RuntimePaths::discover_for(Environment::BareMetal);
            assert!(paths.config_dir.ends_with("from-env"), "{paths:?}");
        });
    }

    /// With nothing set, the directories take the program's own name, never a fixed one.
    #[test]
    fn unnamed_local_paths_take_the_program_name() {
        temp_env::with_var("APP_NAME", None::<&str>, || {
            let expected = std::env::args_os()
                .next()
                .as_deref()
                .and_then(program_name)
                .unwrap_or_else(|| "app".to_string());
            let paths = RuntimePaths::discover_for(Environment::BareMetal);
            assert_eq!(paths.temp_dir, std::env::temp_dir().join(&expected));
            assert!(paths.config_dir.ends_with(&expected), "{paths:?}");
        });
    }

    #[test]
    fn an_explicit_name_leaves_the_container_paths_alone() {
        assert_eq!(
            RuntimePaths::discover_for_app(Environment::Kubernetes, Some("my-app")),
            RuntimePaths::discover_for(Environment::Kubernetes)
        );
    }
}
