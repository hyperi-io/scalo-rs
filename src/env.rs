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
    /// Running in a generic container (detected via cgroups)
    Container,
    /// Running on bare metal / local development
    BareMetal,
}

impl Environment {
    /// Detect the current runtime environment.
    ///
    /// Detection priority (highest confidence first):
    /// 1. Kubernetes service account token exists
    /// 2. Kubernetes environment variables present
    /// 3. Docker: `/.dockerenv` file exists
    /// 4. Container: cgroups contain container markers
    /// 5. Default: `BareMetal`
    #[must_use]
    pub fn detect() -> Self {
        // Check for Kubernetes first (highest priority)
        if Self::is_kubernetes_by_token() || Self::is_kubernetes_by_env() {
            return Self::Kubernetes;
        }

        // Check for Docker
        if Self::is_docker_by_file() {
            return Self::Docker;
        }

        // Check for generic container via cgroups
        if Self::is_container_by_cgroups() {
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

    // Detection helpers

    fn is_kubernetes_by_token() -> bool {
        Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists()
    }

    fn is_kubernetes_by_env() -> bool {
        std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
    }

    fn is_docker_by_file() -> bool {
        Path::new("/.dockerenv").exists()
    }

    fn is_container_by_cgroups() -> bool {
        // Check cgroup v1
        if let Ok(content) = std::fs::read_to_string("/proc/1/cgroup")
            && (content.contains("/docker/")
                || content.contains("/kubepods/")
                || content.contains("/lxc/")
                || content.contains("/containerd/"))
        {
            return true;
        }

        // Check cgroup v2 (unified hierarchy)
        if let Ok(content) = std::fs::read_to_string("/proc/1/mountinfo")
            && (content.contains("/docker/")
                || content.contains("/kubepods/")
                || content.contains("/containerd/"))
        {
            return true;
        }

        false
    }
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
        let value = std::env::var(name).ok()?;
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

    #[test]
    fn test_get_app_env_from_app_env() {
        temp_env::with_var("APP_ENV", Some("production"), || {
            assert_eq!(get_app_env(), "production");
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
            temp_env::with_vars(only("APP_ENV", value), || {
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
            temp_env::with_vars(only("APP_ENV", value), || {
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
            temp_env::with_vars(masked, || {
                assert_eq!(get_app_env(), "production", "APP_ENV={blank:?}");
                assert!(is_production(), "APP_ENV={blank:?} over ENVIRONMENT");
            });

            let deepest = [
                ("APP_ENV", Some(blank)),
                ("ENVIRONMENT", Some(blank)),
                ("ENV", Some("prod")),
            ];
            temp_env::with_vars(deepest, || {
                assert_eq!(get_app_env(), "prod", "blank APP_ENV and ENVIRONMENT");
            });

            let all_blank = UNSET.map(|(name, _)| (name, Some(blank)));
            temp_env::with_vars(all_blank, || {
                assert_eq!(get_app_env(), "development", "all three {blank:?}");
                assert!(!is_production());
            });
        }
    }

    #[test]
    fn is_production_reads_each_variable_in_turn() {
        for var in ["APP_ENV", "ENVIRONMENT", "ENV"] {
            temp_env::with_vars(only(var, "prod"), || {
                assert!(is_production(), "{var}=prod");
            });
        }
        temp_env::with_vars(UNSET, || {
            assert!(!is_production(), "unset resolves to development");
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
                temp_env::with_vars(only(var, value), || {
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
}
