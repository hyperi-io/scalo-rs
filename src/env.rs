// Project:   scalo
// File:      src/env.rs
// Purpose:   Runtime environment detection (K8s, Docker, container, bare metal)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Runtime environment detection.
//!
//! Detects whether the application is running in Kubernetes, Docker,
//! a generic container, or on bare metal. This information is used
//! to configure paths, logging format, and other runtime behaviour.

use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Runtime environment types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Environment {
    /// Running in Kubernetes
    Kubernetes,
    /// Running in Docker (but not K8s)
    Docker,
    /// Running in a generic container (detected via cgroups, the root mount or a container variable)
    Container,
    /// Running on bare metal / local development
    BareMetal,
}

/// cgroup path segments only a containerised process sits under; host daemons such as `docker.service` do not match.
const CONTAINER_CGROUP_MARKERS: [&str; 4] = ["/docker/", "/kubepods", "/lxc/", "/containerd/"];

/// Runtime names found in the mount options of a container's own root filesystem.
const CONTAINER_ROOTFS_MARKERS: [&str; 3] = ["docker", "containerd", "kubelet"];

/// Variables a container runtime or orchestrator sets inside the container.
const CONTAINER_ENV_VARS: [&str; 3] = [
    "container",
    "DOCKER_CONTAINER",
    "ECS_CONTAINER_METADATA_URI",
];

impl Environment {
    /// Detect the current runtime environment.
    ///
    /// Only container evidence counts, highest confidence first. A host
    /// directory such as `/cache` or `/data`, a container daemon's own cgroup
    /// (`docker.service`), and the container mounts a docker host lists in its
    /// mountinfo do not make the host a container.
    ///
    /// 1. `Kubernetes`: the `/var/run/secrets/kubernetes.io/serviceaccount`
    ///    directory exists, or `KUBERNETES_SERVICE_HOST` is set
    /// 2. `Docker`: `/.dockerenv` exists
    /// 3. `Container`: `/proc/1/cgroup` or `/proc/self/cgroup` names a container
    ///    cgroup (`/docker/`, `/kubepods`, `/lxc/`, `/containerd/` or a
    ///    `docker-<64 hex>.scope`), the mount at `/` in `/proc/self/mountinfo`
    ///    is an overlay whose options name `docker`, `containerd` or `kubelet`,
    ///    or `container`, `DOCKER_CONTAINER` or `ECS_CONTAINER_METADATA_URI` is set
    /// 4. `BareMetal`: none of the above
    ///
    /// A variable set to an empty value counts as unset.
    #[must_use]
    pub fn detect() -> Self {
        Self::detect_at(Path::new("/"))
    }

    /// Detect from the files under `root` and the process environment.
    fn detect_at(root: &Path) -> Self {
        if root
            .join("var/run/secrets/kubernetes.io/serviceaccount")
            .exists()
            || env_is_set("KUBERNETES_SERVICE_HOST")
        {
            return Self::Kubernetes;
        }

        if root.join(".dockerenv").exists() {
            return Self::Docker;
        }

        let in_container_cgroup = ["proc/1/cgroup", "proc/self/cgroup"].iter().any(|file| {
            std::fs::read_to_string(root.join(file)).is_ok_and(|body| cgroup_names_container(&body))
        });
        if in_container_cgroup
            || std::fs::read_to_string(root.join("proc/self/mountinfo"))
                .is_ok_and(|body| rootfs_is_container_overlay(&body))
            || CONTAINER_ENV_VARS.iter().any(|name| env_is_set(name))
        {
            return Self::Container;
        }

        Self::BareMetal
    }

    /// Check if running in any container environment.
    #[must_use]
    pub const fn is_container(&self) -> bool {
        matches!(self, Self::Kubernetes | Self::Docker | Self::Container)
    }

    /// Check if running in Kubernetes.
    #[must_use]
    pub const fn is_kubernetes(&self) -> bool {
        matches!(self, Self::Kubernetes)
    }

    /// Check if running in Docker (but not K8s).
    #[must_use]
    pub const fn is_docker(&self) -> bool {
        matches!(self, Self::Docker)
    }

    /// Check if running on bare metal.
    #[must_use]
    pub const fn is_bare_metal(&self) -> bool {
        matches!(self, Self::BareMetal)
    }
}

/// Whether `name` is set to a non-empty value.
fn env_is_set(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

/// Whether a `/proc/<pid>/cgroup` body places the process inside a container.
fn cgroup_names_container(body: &str) -> bool {
    CONTAINER_CGROUP_MARKERS
        .iter()
        .any(|marker| body.contains(marker))
        || has_docker_scope(body)
}

/// Whether `body` holds a `/docker-<64 lowercase hex>.scope` segment, the systemd cgroup driver's name for a container.
fn has_docker_scope(body: &str) -> bool {
    const PREFIX: &str = "/docker-";
    const ID_LEN: usize = 64;
    body.match_indices(PREFIX).any(|(at, _)| {
        let rest = body.as_bytes().get(at + PREFIX.len()..).unwrap_or_default();
        rest.get(..ID_LEN)
            .is_some_and(|id| id.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
            && rest
                .get(ID_LEN..)
                .is_some_and(|tail| tail.starts_with(b".scope"))
    })
}

/// Whether the mount at `/` in a mountinfo body is an overlay a container runtime built; only the root mount counts, as a host that runs containers lists their overlays too.
fn rootfs_is_container_overlay(mountinfo: &str) -> bool {
    mountinfo.lines().any(|line| {
        if line.split(' ').nth(4) != Some("/") {
            return false;
        }
        let Some((_, filesystem)) = line.split_once(" - ") else {
            return false;
        };
        let fs_type = filesystem.split(' ').next().unwrap_or_default();
        fs_type.contains("overlay")
            && CONTAINER_ROOTFS_MARKERS
                .iter()
                .any(|marker| filesystem.contains(marker))
    })
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kubernetes => write!(f, "kubernetes"),
            Self::Docker => write!(f, "docker"),
            Self::Container => write!(f, "container"),
            Self::BareMetal => write!(f, "bare_metal"),
        }
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self::detect()
    }
}

// =============================================================================
// RuntimeContext -- rich runtime metadata detected once at startup
// =============================================================================

/// Rich runtime context detected once at startup, immutable after.
///
/// Provides all K8s/container metadata in one place. Modules read from this
/// instead of doing their own env var lookups. Detected lazily on first access
/// via [`runtime_context()`].
///
/// On bare metal, most fields are `None` -- features that read them become no-ops.
#[derive(Debug, Clone)]
pub struct RuntimeContext {
    /// Detected runtime environment.
    pub environment: Environment,
    /// K8s pod name (from `POD_NAME` or `HOSTNAME` env var).
    pub pod_name: Option<String>,
    /// K8s namespace (from `POD_NAMESPACE` env var or service account).
    pub namespace: Option<String>,
    /// K8s node name (from `NODE_NAME` env var).
    pub node_name: Option<String>,
    /// Container ID (from `HOSTNAME` in container environments).
    pub container_id: Option<String>,
    /// cgroup memory limit in bytes (`None` if unlimited or bare metal).
    pub memory_limit_bytes: Option<u64>,
    /// cgroup CPU quota in cores (`None` if unlimited or bare metal).
    pub cpu_quota_cores: Option<f64>,
}

impl RuntimeContext {
    /// Detect the full runtime context.
    ///
    /// Reads environment variables and filesystem signals. Safe to call
    /// on bare metal -- fields will be `None` when not in a container.
    #[must_use]
    pub fn detect() -> Self {
        let environment = Environment::detect();

        let pod_name = std::env::var("POD_NAME").ok().or_else(|| {
            if environment.is_container() {
                std::env::var("HOSTNAME").ok()
            } else {
                None
            }
        });

        let namespace = std::env::var("POD_NAMESPACE").ok().or_else(|| {
            // Fall back to reading the K8s service account namespace file
            std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .ok()
                .map(|s| s.trim().to_string())
        });

        let node_name = std::env::var("NODE_NAME").ok();

        let container_id = if environment.is_container() {
            std::env::var("HOSTNAME").ok()
        } else {
            None
        };

        // cgroup resource limits (container environments only)
        let memory_limit_bytes = if environment.is_container() {
            read_cgroup_memory_limit()
        } else {
            None
        };

        let cpu_quota_cores = if environment.is_container() {
            read_cgroup_cpu_quota()
        } else {
            None
        };

        Self {
            environment,
            pod_name,
            namespace,
            node_name,
            container_id,
            memory_limit_bytes,
            cpu_quota_cores,
        }
    }

    /// Convenience: is this running in Kubernetes?
    #[must_use]
    pub fn is_kubernetes(&self) -> bool {
        self.environment.is_kubernetes()
    }

    /// Convenience: is this running in any container?
    #[must_use]
    pub fn is_container(&self) -> bool {
        self.environment.is_container()
    }

    /// Convenience: is this bare metal / local dev?
    #[must_use]
    pub fn is_bare_metal(&self) -> bool {
        self.environment.is_bare_metal()
    }
}

impl Default for RuntimeContext {
    fn default() -> Self {
        Self::detect()
    }
}

static RUNTIME_CONTEXT: std::sync::OnceLock<RuntimeContext> = std::sync::OnceLock::new();

/// Get the global runtime context (detected lazily on first call).
///
/// All modules should use this instead of reading env vars directly.
/// The context is immutable after first detection.
#[must_use]
pub fn runtime_context() -> &'static RuntimeContext {
    RUNTIME_CONTEXT.get_or_init(RuntimeContext::detect)
}

// =============================================================================
// cgroup resource limit helpers
// =============================================================================

/// Read cgroup v2 memory limit (returns None if unlimited or not in a cgroup).
fn read_cgroup_memory_limit() -> Option<u64> {
    let content = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let trimmed = content.trim();
    if trimmed == "max" {
        return None; // No limit set
    }
    trimmed.parse::<u64>().ok()
}

/// Read cgroup v2 CPU quota as fractional cores (returns None if unlimited).
///
/// Reads `/sys/fs/cgroup/cpu.max` which contains `quota period` (e.g. "200000 100000" = 2 cores).
fn read_cgroup_cpu_quota() -> Option<f64> {
    let content = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() < 2 || parts[0] == "max" {
        return None; // No limit
    }
    let quota: f64 = parts[0].parse().ok()?;
    let period: f64 = parts[1].parse().ok()?;
    if period > 0.0 {
        Some(quota / period)
    } else {
        None
    }
}

// =============================================================================
// Helm detection and app env helpers
// =============================================================================

/// Check if the application was deployed via Helm.
///
/// Looks for Helm-specific labels in Kubernetes downward API.
#[must_use]
pub fn is_helm() -> bool {
    // Check for Helm release name env var (commonly set)
    if std::env::var("HELM_RELEASE_NAME").is_ok() {
        return true;
    }

    // Check for Helm labels via downward API
    let labels_path = Path::new("/etc/podinfo/labels");
    if labels_path.exists()
        && let Ok(content) = std::fs::read_to_string(labels_path)
    {
        return content.contains("helm.sh/chart")
            || content.contains("app.kubernetes.io/managed-by=\"Helm\"");
    }

    false
}

/// App environment used when none of `APP_ENV`, `ENVIRONMENT` or `ENV` is set.
const DEFAULT_APP_ENV: &str = "development";

/// The variables that name the app environment, highest precedence first.
const APP_ENV_VARS: [&str; 3] = ["APP_ENV", "ENVIRONMENT", "ENV"];

/// Guards the one-shot default-posture warning in [`get_app_env`].
static DEFAULT_POSTURE_WARNED: AtomicBool = AtomicBool::new(false);

/// Get the current application environment name (dev, staging, prod).
///
/// Returns the first of `APP_ENV`, `ENVIRONMENT` and `ENV` that holds a
/// value, trimmed of surrounding whitespace, else "development". A variable
/// that is empty or only whitespace counts as unset, so resolution moves on
/// to the next one.
///
/// Falling back to the default turns off every [`is_production`] check, so
/// the first fallback a log subscriber would record emits one warning per
/// process. Setting any of the three variables to a non-blank value silences
/// it.
#[must_use]
pub fn get_app_env() -> String {
    app_env_or_default(&DEFAULT_POSTURE_WARNED)
}

/// Resolve the app environment, claiming `warned` to log the default once.
fn app_env_or_default(warned: &AtomicBool) -> String {
    if let Some(app_env) = APP_ENV_VARS.iter().find_map(|name| {
        let value = posture_var(name)?;
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }) {
        return app_env;
    }
    // Claimed only once a subscriber would record it: config can load before the logger.
    #[cfg(feature = "tracing")]
    if tracing::enabled!(tracing::Level::WARN)
        && !warned.swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        tracing::warn!(
            app_env = DEFAULT_APP_ENV,
            "none of APP_ENV, ENVIRONMENT or ENV is set, so the app environment defaults to \
             development and production-only safety checks are disabled -- set \
             APP_ENV=production on a production deployment"
        );
    }
    #[cfg(not(feature = "tracing"))]
    let _ = warned;
    DEFAULT_APP_ENV.to_string()
}

/// Whether the current app environment is production-like.
///
/// True when [`get_app_env`] resolves (case-insensitively) to `production`
/// or `prod`, so ` production` and `production\n` count too. Used by config
/// `validate(is_production)` methods to reject insecure-by-design settings
/// (e.g. TLS `skip_verify`, plaintext disk caches) outside of dev/test. With
/// none of the three variables set to a non-blank value this is `false`, and
/// [`get_app_env`] logs that once.
#[must_use]
pub fn is_production() -> bool {
    matches!(
        get_app_env().to_ascii_lowercase().as_str(),
        "production" | "prod"
    )
}

/// The posture variable `name` from the process environment.
#[cfg(not(test))]
fn posture_var(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The posture variable `name`, from the calling test's [`test_posture`] scope when one names it.
#[cfg(test)]
fn posture_var(name: &str) -> Option<String> {
    test_posture::read(name)
}

/// Posture variables scoped to one test thread.
///
/// The process environment is shared by every test in the process, so a
/// production posture set there makes Kafka clients that tests build on other
/// threads refuse as if in production. A test that needs a production posture
/// sets it here instead.
#[cfg(test)]
pub(crate) mod test_posture {
    use std::cell::RefCell;

    /// A posture variable set (`Some`) or unset (`None`).
    pub(crate) type Var = (&'static str, Option<&'static str>);

    thread_local! {
        /// Every scope open on this thread, innermost last.
        static SCOPES: RefCell<Vec<Var>> = const { RefCell::new(Vec::new()) };
    }

    /// `name` as the innermost scope on this thread sets it, else from the process environment.
    pub(super) fn read(name: &str) -> Option<String> {
        SCOPES.with_borrow(|scopes| {
            scopes
                .iter()
                .rev()
                .find(|(var, _)| *var == name)
                .map_or_else(
                    || std::env::var(name).ok(),
                    |(_, value)| value.map(str::to_string),
                )
        })
    }

    /// Run `f` with each of `vars` set or unset for posture reads on this thread alone.
    pub(crate) fn with_vars<R>(vars: impl AsRef<[Var]>, f: impl FnOnce() -> R) -> R {
        let vars = vars.as_ref();
        SCOPES.with_borrow_mut(|scopes| scopes.extend_from_slice(vars));
        let _close = Close(vars.len());
        f()
    }

    /// Closes a scope on drop, so a panic in its closure closes it too.
    struct Close(usize);

    impl Drop for Close {
        fn drop(&mut self) {
            SCOPES.with_borrow_mut(|scopes| scopes.truncate(scopes.len().saturating_sub(self.0)));
        }
    }
}

// =============================================================================
// Console context for the default log format and colour
// =============================================================================

/// What the default log format and colour are decided from.
///
/// [`ConsoleContext::detect`] reads it from the process; tests build one directly.
#[cfg(any(feature = "logger", feature = "cli"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ConsoleContext {
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` is set and not blank.
    pub(crate) otel_endpoint: bool,
    /// A CI runner is detected, by the variables [`is_ci`] reads.
    pub(crate) ci: bool,
    /// stderr, where the logger writes, is a terminal.
    pub(crate) tty: bool,
}

#[cfg(any(feature = "logger", feature = "cli"))]
impl ConsoleContext {
    /// Read the context from the environment and stderr.
    pub(crate) fn detect() -> Self {
        use std::io::IsTerminal;
        Self {
            otel_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .is_ok_and(|v| !v.trim().is_empty()),
            ci: is_ci(),
            tty: std::io::stderr().is_terminal(),
        }
    }

    /// `true` when an unset or `auto` log format resolves to JSON rather than text.
    ///
    /// An OTEL endpoint gives JSON, then a CI run gives text, then a terminal
    /// gives text and anything else JSON, the order scalo-py applies.
    pub(crate) fn wants_json(self) -> bool {
        self.otel_endpoint || !(self.ci || self.tty)
    }
}

/// `true` under a CI runner, by the variables scalo-py checks.
#[cfg(any(feature = "logger", feature = "cli"))]
fn is_ci() -> bool {
    ["CI", "GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "TRAVIS"]
        .iter()
        .any(|name| std::env::var(name).is_ok_and(|v| v == "true"))
        || std::env::var_os("JENKINS_URL").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_environment_display() {
        assert_eq!(Environment::Kubernetes.to_string(), "kubernetes");
        assert_eq!(Environment::Docker.to_string(), "docker");
        assert_eq!(Environment::Container.to_string(), "container");
        assert_eq!(Environment::BareMetal.to_string(), "bare_metal");
    }

    #[test]
    fn test_environment_is_container() {
        assert!(Environment::Kubernetes.is_container());
        assert!(Environment::Docker.is_container());
        assert!(Environment::Container.is_container());
        assert!(!Environment::BareMetal.is_container());
    }

    #[test]
    fn test_environment_is_kubernetes() {
        assert!(Environment::Kubernetes.is_kubernetes());
        assert!(!Environment::Docker.is_kubernetes());
        assert!(!Environment::Container.is_kubernetes());
        assert!(!Environment::BareMetal.is_kubernetes());
    }

    #[test]
    fn test_environment_is_bare_metal() {
        assert!(!Environment::Kubernetes.is_bare_metal());
        assert!(!Environment::Docker.is_bare_metal());
        assert!(!Environment::Container.is_bare_metal());
        assert!(Environment::BareMetal.is_bare_metal());
    }

    #[test]
    fn test_get_app_env_default() {
        temp_env::with_vars(
            [
                ("APP_ENV", None::<&str>),
                ("ENVIRONMENT", None),
                ("ENV", None),
            ],
            || assert_eq!(get_app_env(), "development"),
        );
    }

    /// Reads the process environment, with a value no production check acts on.
    #[test]
    fn test_get_app_env_from_app_env() {
        temp_env::with_var("APP_ENV", Some("staging"), || {
            assert_eq!(get_app_env(), "staging");
        });
    }

    /// All three posture variables cleared, so resolution reaches the default.
    const UNSET: [(&str, Option<&str>); 3] =
        [("APP_ENV", None), ("ENVIRONMENT", None), ("ENV", None)];

    /// Substring of the default-posture warning that names all three variables.
    #[cfg(feature = "logger")]
    const NAMES_THE_VARIABLES: &str = "none of APP_ENV, ENVIRONMENT or ENV is set";

    /// `UNSET` with `var` set to `value`.
    fn only(var: &str, value: &'static str) -> [(&'static str, Option<&'static str>); 3] {
        UNSET.map(|(name, _)| (name, (name == var).then_some(value)))
    }

    #[test]
    fn is_production_accepts_only_production_and_prod() {
        for (value, expected) in [
            ("production", true),
            ("prod", true),
            ("PRODUCTION", true),
            ("Prod", true),
            (" production", true),
            ("production\n", true),
            ("\tprod ", true),
            ("staging", false),
            ("development", false),
            ("production-eu", false),
            ("pro duction", false),
            ("", false),
            ("  ", false),
        ] {
            test_posture::with_vars(only("APP_ENV", value), || {
                assert_eq!(is_production(), expected, "APP_ENV={value:?}");
            });
        }
    }

    /// A padded value resolves as the name inside it, so the config cascade
    /// picks `settings.staging.yaml` for ` staging `.
    #[test]
    fn get_app_env_trims_the_value() {
        for (value, expected) in [
            (" production", "production"),
            ("production\n", "production"),
            (" staging ", "staging"),
            ("\ttest\r\n", "test"),
        ] {
            test_posture::with_vars(only("APP_ENV", value), || {
                assert_eq!(get_app_env(), expected, "APP_ENV={value:?}");
            });
        }
    }

    /// An empty or blank variable is skipped, so it cannot mask a production
    /// posture set further down the order.
    #[test]
    fn a_blank_variable_counts_as_unset() {
        for blank in ["", " ", "\n", " \t "] {
            let masked = [
                ("APP_ENV", Some(blank)),
                ("ENVIRONMENT", Some("production")),
                ("ENV", None),
            ];
            test_posture::with_vars(masked, || {
                assert_eq!(get_app_env(), "production", "APP_ENV={blank:?}");
                assert!(is_production(), "APP_ENV={blank:?} over ENVIRONMENT");
            });

            let deepest = [
                ("APP_ENV", Some(blank)),
                ("ENVIRONMENT", Some(blank)),
                ("ENV", Some("prod")),
            ];
            test_posture::with_vars(deepest, || {
                assert_eq!(get_app_env(), "prod", "blank APP_ENV and ENVIRONMENT");
            });

            let all_blank = UNSET.map(|(name, _)| (name, Some(blank)));
            test_posture::with_vars(all_blank, || {
                assert_eq!(get_app_env(), "development", "all three {blank:?}");
                assert!(!is_production());
            });
        }
    }

    #[test]
    fn is_production_reads_each_variable_in_turn() {
        for var in ["APP_ENV", "ENVIRONMENT", "ENV"] {
            test_posture::with_vars(only(var, "prod"), || {
                assert!(is_production(), "{var}=prod");
            });
        }
        test_posture::with_vars(UNSET, || {
            assert!(!is_production(), "unset resolves to development");
        });
    }

    /// The scope sets this thread's posture, and another thread reads the process environment.
    #[test]
    fn a_scoped_posture_reaches_no_other_thread() {
        temp_env::with_vars(UNSET, || {
            test_posture::with_vars(only("APP_ENV", "production"), || {
                assert!(is_production(), "the scope sets this thread's posture");
                let elsewhere = std::thread::spawn(is_production)
                    .join()
                    .expect("reader thread");
                assert!(
                    !elsewhere,
                    "another thread read the scoped production posture"
                );
            });
            assert!(!is_production(), "the scope outlived its closure");
        });
    }

    #[test]
    fn a_scoped_posture_closes_when_its_closure_panics() {
        let unwound = std::panic::catch_unwind(|| {
            test_posture::with_vars(only("APP_ENV", "production"), || panic!("inside the scope"));
        });
        assert!(unwound.is_err());
        temp_env::with_vars(UNSET, || {
            assert!(!is_production(), "the panic left the scope open");
        });
    }

    /// Subscriber recording WARN and above as text, and the buffer it writes to.
    #[cfg(feature = "logger")]
    fn capture() -> (
        impl tracing::Subscriber + Send + Sync + 'static,
        std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    ) {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt as _;

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
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::WARN)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || Sink(Arc::clone(&writer))),
            );
        (subscriber, buf)
    }

    #[cfg(feature = "logger")]
    fn captured(buf: &std::sync::Mutex<Vec<u8>>) -> String {
        String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
    }

    #[cfg(feature = "logger")]
    #[test]
    fn default_posture_warns_once_naming_the_variables() {
        let warned = AtomicBool::new(false);
        let (subscriber, buf) = capture();
        temp_env::with_vars(UNSET, || {
            tracing::subscriber::with_default(subscriber, || {
                for _ in 0..3 {
                    assert_eq!(app_env_or_default(&warned), "development");
                }
            });
        });

        let out = captured(&buf);
        assert_eq!(out.matches(NAMES_THE_VARIABLES).count(), 1, "{out}");
        assert!(out.contains("WARN"), "{out}");
        assert!(
            out.contains("production-only safety checks are disabled"),
            "{out}"
        );
    }

    #[cfg(feature = "logger")]
    #[test]
    fn any_set_variable_silences_the_warning() {
        for var in ["APP_ENV", "ENVIRONMENT", "ENV"] {
            for value in ["staging", "production"] {
                let warned = AtomicBool::new(false);
                let (subscriber, buf) = capture();
                test_posture::with_vars(only(var, value), || {
                    tracing::subscriber::with_default(subscriber, || {
                        assert_eq!(app_env_or_default(&warned), value);
                    });
                });
                assert!(captured(&buf).is_empty(), "{var}={value} must not warn");
                assert!(
                    !warned.load(std::sync::atomic::Ordering::Relaxed),
                    "{var}={value} must leave the warning unclaimed"
                );
            }
        }
    }

    /// A blank variable is unset, so it cannot hide the development default.
    #[cfg(feature = "logger")]
    #[test]
    fn blank_variables_still_warn() {
        for blank in ["", "  ", "\n"] {
            let warned = AtomicBool::new(false);
            let (subscriber, buf) = capture();
            temp_env::with_vars(UNSET.map(|(name, _)| (name, Some(blank))), || {
                tracing::subscriber::with_default(subscriber, || {
                    assert_eq!(app_env_or_default(&warned), "development");
                });
            });
            let out = captured(&buf);
            assert_eq!(
                out.matches(NAMES_THE_VARIABLES).count(),
                1,
                "all three {blank:?}: {out}"
            );
            assert!(warned.load(std::sync::atomic::Ordering::Relaxed));
        }
    }

    /// A default resolved before the logger is up must not use up the warning.
    #[cfg(feature = "logger")]
    #[test]
    fn default_posture_waits_for_a_subscriber() {
        let warned = AtomicBool::new(false);
        let (subscriber, buf) = capture();
        temp_env::with_vars(UNSET, || {
            tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
                assert_eq!(app_env_or_default(&warned), "development");
            });
            assert!(!warned.load(std::sync::atomic::Ordering::Relaxed));

            tracing::subscriber::with_default(subscriber, || {
                assert_eq!(app_env_or_default(&warned), "development");
            });
        });
        assert_eq!(captured(&buf).matches(NAMES_THE_VARIABLES).count(), 1);
    }

    /// The public resolver goes through the process-wide one-shot, not a local flag.
    #[cfg(feature = "logger")]
    #[test]
    fn get_app_env_arms_the_process_warning() {
        let (subscriber, _buf) = capture();
        temp_env::with_vars(UNSET, || {
            tracing::subscriber::with_default(subscriber, || assert!(!is_production()));
        });
        assert!(DEFAULT_POSTURE_WARNED.load(std::sync::atomic::Ordering::Relaxed));
    }

    // --- Container detection from a fake filesystem root ---

    /// The variables that decide detection on their own, all cleared so the runner's environment (a CI pod) cannot.
    const NO_CONTAINER_VARS: [(&str, Option<&str>); 4] = [
        ("KUBERNETES_SERVICE_HOST", None),
        ("container", None),
        ("DOCKER_CONTAINER", None),
        ("ECS_CONTAINER_METADATA_URI", None),
    ];

    const DOCKER_ID: &str = "0ddaeccc84c6fcf3cadf246caa3f5a6578f505691c1923b1517a23b4a3107959";

    const HOST_PID1_CGROUP: &str = "0::/init.scope\n";
    const HOST_SELF_CGROUP: &str =
        "0::/user.slice/user-1000.slice/user@1000.service/app.slice/terminal.scope\n";

    /// A bare-metal docker host: ext4 root, with its containers' overlays and netns mounts on other paths.
    fn docker_host_mountinfo() -> String {
        format!(
            "27 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro\n\
             176 27 8:17 / /cache rw,relatime shared:94 - ext4 /dev/sdb1 rw\n\
             423 176 0:65 / /cache/docker/rootfs/overlayfs/{DOCKER_ID} rw,relatime shared:220 - overlay overlay \
             rw,lowerdir=/cache/docker/containerd/daemon/io.containerd.snapshotter.v1.overlayfs/snapshots/12/fs,\
             upperdir=/cache/docker/containerd/daemon/io.containerd.snapshotter.v1.overlayfs/snapshots/13/fs\n\
             431 27 0:66 / /var/lib/docker/overlay2/1/merged rw,relatime - overlay overlay \
             rw,lowerdir=/var/lib/docker/overlay2/l/ABC,upperdir=/var/lib/docker/overlay2/1/diff\n\
             439 34 0:5 net:[4026533674] /run/docker/netns/026949dfeea6 rw shared:225 - nsfs nsfs rw\n"
        )
    }

    /// Inside a docker container: the root mount is the overlay docker assembled.
    const DOCKER_CONTAINER_MOUNTINFO: &str = "612 540 0:61 / / rw,relatime master:220 - overlay overlay \
        rw,lowerdir=/var/lib/docker/overlay2/l/ABC:/var/lib/docker/overlay2/l/DEF,\
        upperdir=/var/lib/docker/overlay2/123/diff,workdir=/var/lib/docker/overlay2/123/work\n\
        613 612 0:64 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw\n";

    /// Inside a Kubernetes pod on containerd: the root mount is a containerd snapshot overlay.
    const CONTAINERD_CONTAINER_MOUNTINFO: &str = "1462 1360 0:310 / / rw,relatime - overlay overlay \
        rw,lowerdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/88/fs,\
        upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/90/fs\n";

    /// An image-based desktop distro: the root is a composefs overlay that no container runtime built.
    const COMPOSEFS_MOUNTINFO: &str = "28 1 0:31 / / ro,relatime shared:1 - overlay composefs \
        ro,lowerdir+=/run/ostree/.private/cfsroot-lower,datadir+=/sysroot/ostree/repo/objects\n";

    /// A host root on LVM whose volume group is named for docker, which is not an overlay.
    const DOCKER_NAMED_EXT4_ROOT_MOUNTINFO: &str = "27 1 253:0 / / rw,relatime shared:1 - ext4 /dev/mapper/docker--vg-root rw,errors=remount-ro\n";

    /// Write the `/proc` files detection reads under a fresh fake root.
    fn fake_root(mountinfo: &str, pid1_cgroup: &str, self_cgroup: &str) -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("fake root");
        for dir in ["proc/1", "proc/self"] {
            std::fs::create_dir_all(root.path().join(dir)).expect("fake /proc dir");
        }
        for (file, body) in [
            ("proc/1/cgroup", pid1_cgroup),
            ("proc/self/cgroup", self_cgroup),
            ("proc/self/mountinfo", mountinfo),
        ] {
            std::fs::write(root.path().join(file), body).expect("fake /proc file");
        }
        root
    }

    fn docker_host_root() -> tempfile::TempDir {
        fake_root(&docker_host_mountinfo(), HOST_PID1_CGROUP, HOST_SELF_CGROUP)
    }

    /// Detection under `root` with every container variable cleared.
    fn detect_in(root: &Path) -> Environment {
        temp_env::with_vars(NO_CONTAINER_VARS, || Environment::detect_at(root))
    }

    #[test]
    fn a_docker_host_with_host_dirs_and_container_mounts_is_bare_metal() {
        let root = docker_host_root();
        for host_dir in ["cache", "data", "app/config", "config"] {
            std::fs::create_dir_all(root.path().join(host_dir)).expect("host dir");
        }
        assert_eq!(detect_in(root.path()), Environment::BareMetal);
    }

    #[test]
    fn a_root_that_is_not_a_container_overlay_is_bare_metal() {
        for mountinfo in [COMPOSEFS_MOUNTINFO, DOCKER_NAMED_EXT4_ROOT_MOUNTINFO] {
            let root = fake_root(mountinfo, HOST_PID1_CGROUP, HOST_SELF_CGROUP);
            assert_eq!(
                detect_in(root.path()),
                Environment::BareMetal,
                "{mountinfo}"
            );
        }
    }

    /// A root line with no ` - ` separator names no filesystem, so it is not evidence.
    #[test]
    fn a_malformed_root_mount_line_is_bare_metal() {
        let root = fake_root(
            "612 540 0:61 / / rw,relatime overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/ABC\n",
            HOST_PID1_CGROUP,
            HOST_SELF_CGROUP,
        );
        assert_eq!(detect_in(root.path()), Environment::BareMetal);
    }

    /// A container daemon's own cgroup, and names that only look like a container scope, are host evidence.
    #[test]
    fn host_service_cgroups_are_bare_metal() {
        let short_scope = format!("0::/system.slice/docker-{}.scope\n", &DOCKER_ID[..12]);
        let upper_scope = format!(
            "0::/system.slice/docker-{}.scope\n",
            DOCKER_ID.to_ascii_uppercase()
        );
        for cgroup in [
            "0::/system.slice/containerd.service\n",
            "0::/system.slice/docker.service\n",
            "0::/user.slice/user-1000.slice/user@1000.service/app.slice/app-docker\\x2ddesktop-1234.scope\n",
            "0::/system.slice/docker-backup.service\n",
            "12:memory:/system.slice/lxcfs.service\n",
            short_scope.as_str(),
            upper_scope.as_str(),
        ] {
            let in_pid1 = fake_root(&docker_host_mountinfo(), cgroup, HOST_SELF_CGROUP);
            assert_eq!(
                detect_in(in_pid1.path()),
                Environment::BareMetal,
                "pid 1 {cgroup}"
            );
            let in_self = fake_root(&docker_host_mountinfo(), HOST_PID1_CGROUP, cgroup);
            assert_eq!(
                detect_in(in_self.path()),
                Environment::BareMetal,
                "self {cgroup}"
            );
        }
    }

    #[test]
    fn missing_proc_files_are_bare_metal() {
        let root = tempfile::tempdir().expect("empty root");
        assert_eq!(detect_in(root.path()), Environment::BareMetal);
    }

    /// A container variable set to an empty value counts as unset.
    #[test]
    fn empty_container_variables_are_bare_metal() {
        let root = docker_host_root();
        for name in [
            "KUBERNETES_SERVICE_HOST",
            "container",
            "DOCKER_CONTAINER",
            "ECS_CONTAINER_METADATA_URI",
        ] {
            let vars = NO_CONTAINER_VARS.map(|(var, _)| (var, (var == name).then_some("")));
            let detected = temp_env::with_vars(vars, || Environment::detect_at(root.path()));
            assert_eq!(detected, Environment::BareMetal, "{name}=\"\"");
        }
    }

    #[test]
    fn an_overlay_root_a_runtime_built_is_a_container() {
        for mountinfo in [DOCKER_CONTAINER_MOUNTINFO, CONTAINERD_CONTAINER_MOUNTINFO] {
            let root = fake_root(mountinfo, "0::/\n", "0::/\n");
            assert_eq!(
                detect_in(root.path()),
                Environment::Container,
                "{mountinfo}"
            );
        }
    }

    #[test]
    fn container_cgroups_are_containers() {
        let v1_docker = format!("12:memory:/docker/{DOCKER_ID}\n");
        let systemd_docker = format!("0::/system.slice/docker-{DOCKER_ID}.scope\n");
        for cgroup in [
            v1_docker.as_str(),
            systemd_docker.as_str(),
            "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1.slice/cri-containerd-1.scope\n",
            "11:devices:/kubepods/besteffort/pod1/abc\n",
            "11:devices:/lxc/web01\n",
            "0::/containerd/default/web01\n",
        ] {
            let in_pid1 = fake_root(&docker_host_mountinfo(), cgroup, HOST_SELF_CGROUP);
            assert_eq!(
                detect_in(in_pid1.path()),
                Environment::Container,
                "pid 1 {cgroup}"
            );
            let in_self = fake_root(&docker_host_mountinfo(), HOST_PID1_CGROUP, cgroup);
            assert_eq!(
                detect_in(in_self.path()),
                Environment::Container,
                "self {cgroup}"
            );
        }
    }

    #[test]
    fn dockerenv_is_docker() {
        let root = docker_host_root();
        std::fs::write(root.path().join(".dockerenv"), "").expect("dockerenv");
        assert_eq!(detect_in(root.path()), Environment::Docker);
    }

    /// The service-account directory counts on its own, whether or not a token is mounted in it.
    #[test]
    fn the_service_account_directory_is_kubernetes() {
        let root = docker_host_root();
        std::fs::create_dir_all(
            root.path()
                .join("var/run/secrets/kubernetes.io/serviceaccount"),
        )
        .expect("service account dir");
        assert_eq!(detect_in(root.path()), Environment::Kubernetes);
    }

    #[test]
    fn container_variables_are_detected() {
        let root = docker_host_root();
        for (name, expected) in [
            ("KUBERNETES_SERVICE_HOST", Environment::Kubernetes),
            ("container", Environment::Container),
            ("DOCKER_CONTAINER", Environment::Container),
            ("ECS_CONTAINER_METADATA_URI", Environment::Container),
        ] {
            let vars = NO_CONTAINER_VARS.map(|(var, _)| (var, (var == name).then_some("set")));
            let detected = temp_env::with_vars(vars, || Environment::detect_at(root.path()));
            assert_eq!(detected, expected, "{name}=set");
        }
    }

    #[test]
    fn test_environment_detect_returns_valid() {
        // Just ensure detect() doesn't panic and returns a valid variant
        let env = Environment::detect();
        assert!(matches!(
            env,
            Environment::Kubernetes
                | Environment::Docker
                | Environment::Container
                | Environment::BareMetal
        ));
    }

    // --- RuntimeContext tests ---

    #[test]
    fn test_runtime_context_detect_does_not_panic() {
        let ctx = RuntimeContext::detect();
        // Environment is always set
        assert!(matches!(
            ctx.environment,
            Environment::Kubernetes
                | Environment::Docker
                | Environment::Container
                | Environment::BareMetal
        ));
    }

    #[test]
    fn test_runtime_context_bare_metal_has_no_k8s_fields() {
        // On a dev machine (bare metal), K8s fields should be None
        // unless POD_NAME etc. env vars happen to be set
        let ctx = RuntimeContext::detect();
        if ctx.environment.is_bare_metal() {
            assert!(
                ctx.node_name.is_none(),
                "node_name should be None on bare metal"
            );
            // pod_name might come from HOSTNAME, so we don't assert it's None
        }
    }

    #[test]
    fn test_runtime_context_reads_pod_name_env() {
        temp_env::with_vars(
            [
                ("POD_NAME", Some("test-pod-123")),
                ("KUBERNETES_SERVICE_HOST", Some("10.0.0.1")),
            ],
            || {
                let ctx = RuntimeContext::detect();
                assert_eq!(ctx.pod_name.as_deref(), Some("test-pod-123"));
            },
        );
    }

    #[test]
    fn test_runtime_context_reads_namespace_env() {
        temp_env::with_var("POD_NAMESPACE", Some("production"), || {
            let ctx = RuntimeContext::detect();
            assert_eq!(ctx.namespace.as_deref(), Some("production"));
        });
    }

    #[test]
    fn test_runtime_context_reads_node_name_env() {
        temp_env::with_var("NODE_NAME", Some("node-1"), || {
            let ctx = RuntimeContext::detect();
            assert_eq!(ctx.node_name.as_deref(), Some("node-1"));
        });
    }

    #[test]
    fn test_runtime_context_global_singleton() {
        // runtime_context() should return the same instance every time
        let ctx1 = runtime_context();
        let ctx2 = runtime_context();
        assert_eq!(ctx1.environment, ctx2.environment);
        assert_eq!(ctx1.pod_name, ctx2.pod_name);
    }

    #[test]
    fn test_runtime_context_is_kubernetes_convenience() {
        let mut ctx = RuntimeContext::detect();
        ctx.environment = Environment::Kubernetes;
        assert!(ctx.is_kubernetes());
        assert!(ctx.is_container());
        assert!(!ctx.is_bare_metal());
    }

    #[test]
    fn test_runtime_context_is_bare_metal_convenience() {
        let mut ctx = RuntimeContext::detect();
        ctx.environment = Environment::BareMetal;
        assert!(!ctx.is_kubernetes());
        assert!(!ctx.is_container());
        assert!(ctx.is_bare_metal());
    }

    // --- cgroup helper tests ---

    #[test]
    fn test_read_cgroup_memory_limit_returns_option() {
        // On bare metal, returns None (no cgroup). On container, returns Some.
        let limit = read_cgroup_memory_limit();
        // Just verify it doesn't panic -- result depends on environment
        let _ = limit;
    }

    #[test]
    fn test_read_cgroup_cpu_quota_returns_option() {
        let quota = read_cgroup_cpu_quota();
        let _ = quota;
    }

    #[test]
    fn test_prestop_delay_default_bare_metal() {
        // On bare metal, default pre-stop delay should be 0
        temp_env::with_var("PRESTOP_DELAY_SECS", None::<&str>, || {
            let ctx = RuntimeContext::detect();
            if ctx.environment.is_bare_metal() {
                // The prestop_delay_secs function is in shutdown.rs,
                // but we can verify the RuntimeContext is bare metal
                assert!(!ctx.is_kubernetes());
            }
        });
    }

    // --- ConsoleContext tests ---

    /// The CI variables `is_ci` reads, all cleared.
    #[cfg(any(feature = "logger", feature = "cli"))]
    const NO_CI: [(&str, Option<&str>); 6] = [
        ("CI", None),
        ("GITHUB_ACTIONS", None),
        ("GITLAB_CI", None),
        ("CIRCLECI", None),
        ("TRAVIS", None),
        ("JENKINS_URL", None),
    ];

    #[cfg(any(feature = "logger", feature = "cli"))]
    #[test]
    fn unset_format_is_json_with_otel_then_text_in_ci_or_on_a_tty() {
        for (otel_endpoint, ci, tty, json) in [
            (true, false, false, true),
            (true, true, true, true),
            (false, true, false, false),
            (false, false, true, false),
            (false, true, true, false),
            (false, false, false, true),
        ] {
            let ctx = ConsoleContext {
                otel_endpoint,
                ci,
                tty,
            };
            assert_eq!(ctx.wants_json(), json, "{ctx:?}");
        }
    }

    #[cfg(any(feature = "logger", feature = "cli"))]
    #[test]
    fn is_ci_reads_each_runner_variable() {
        temp_env::with_vars(NO_CI, || assert!(!is_ci(), "nothing set"));
        for name in ["CI", "GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "TRAVIS"] {
            let set = NO_CI.map(|(n, _)| (n, (n == name).then_some("true")));
            temp_env::with_vars(set, || assert!(is_ci(), "{name}=true"));
            let other = NO_CI.map(|(n, _)| (n, (n == name).then_some("1")));
            temp_env::with_vars(other, || assert!(!is_ci(), "{name}=1 is not true"));
        }
        let jenkins = NO_CI.map(|(n, _)| (n, (n == "JENKINS_URL").then_some("")));
        temp_env::with_vars(jenkins, || assert!(is_ci(), "JENKINS_URL present"));
    }

    #[cfg(any(feature = "logger", feature = "cli"))]
    #[test]
    fn detect_reads_the_otel_endpoint_and_ignores_a_blank_one() {
        temp_env::with_var(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            Some("http://otel:4317"),
            || {
                assert!(ConsoleContext::detect().otel_endpoint);
            },
        );
        temp_env::with_var("OTEL_EXPORTER_OTLP_ENDPOINT", Some("  "), || {
            assert!(!ConsoleContext::detect().otel_endpoint);
        });
        temp_env::with_var("OTEL_EXPORTER_OTLP_ENDPOINT", None::<&str>, || {
            assert!(!ConsoleContext::detect().otel_endpoint);
        });
    }
}
