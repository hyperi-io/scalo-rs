// Project:   scalo
// File:      tests/smoke.rs
// Purpose:   Startup smoke test -- catches init panics before production
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Startup smoke test.
//!
//! Boots the library on default configuration and asserts the state each step
//! leaves behind. Several downstream apps take this library, so a default boot that
//! panics, or quietly produces nothing, has to fail here rather than in a
//! consumer.
//!
//! Every assertion here has to be one a regression can fail. An assertion the
//! type system already guarantees is not coverage, whatever it looks like in a
//! green run.
//!
//! One boot step per test, because the runner is nextest and each test gets
//! its own process. The logger subscriber, the config cell, the metrics
//! recorder and the signal handlers are all process-global and install once.

/// The documented one-call boot: logger subscriber, then the config cascade.
/// This is a consumer's first line, so a panic or a silent no-op here is a
/// panic or a silent no-op in all six of them.
#[cfg(all(feature = "config", feature = "logger"))]
#[test]
fn smoke_init_boots_the_library_on_defaults() {
    scalo::init("SCALO_SMOKE").expect("default boot must succeed");

    // config::get() panics when setup() never ran, so reaching the cascade is
    // half the check. The other half is that it answers -- a cascade that
    // builds but serves nothing leaves every from_cascade() reader in the
    // library on its own hard-coded fallback.
    let cfg = scalo::config::get();
    assert_eq!(cfg.env_prefix(), "SCALO_SMOKE");
    assert_eq!(
        cfg.get_string("log_level").as_deref(),
        Some("info"),
        "the lowest cascade layer must still answer"
    );
    assert_eq!(
        cfg.get_string("log_format").as_deref(),
        Some("auto"),
        "the lowest cascade layer must still answer"
    );

    // A second boot refuses rather than installing a second logger and a
    // second config over the top of the live ones.
    assert!(
        scalo::init("SCALO_SMOKE").is_err(),
        "a second boot must report the conflict"
    );
}

/// `detect()` reads `/var/run/secrets`, `/.dockerenv` and the cgroup files, so
/// calling it is itself the check -- a panic there is a boot panic everywhere.
#[test]
fn smoke_environment_detection_is_self_consistent() {
    use scalo::Environment;

    let detected = Environment::detect();
    assert_eq!(
        detected,
        Environment::detect(),
        "detection must be stable within a process, got {detected:?} then another answer"
    );

    // The predicates must agree with the variant for all four, not only the one
    // this host detects as, or the check goes unexercised on every other host.
    // Container answers false to all three specific predicates, so the rule is
    // at-most-one rather than exactly-one.
    for env in [
        Environment::Kubernetes,
        Environment::Docker,
        Environment::Container,
        Environment::BareMetal,
    ] {
        let specific = [env.is_kubernetes(), env.is_docker(), env.is_bare_metal()];
        assert!(
            specific.iter().filter(|t| **t).count() <= 1,
            "at most one of is_kubernetes/is_docker/is_bare_metal may hold for {env:?}"
        );
        assert_eq!(
            env.is_container(),
            !env.is_bare_metal(),
            "every variant but BareMetal is a container, {env:?} disagrees"
        );
    }
}

/// The metrics manager installs a process-global recorder and a manifest
/// registry. A counter that never reaches the manifest is the wiring break
/// that leaves a consumer's `/metrics/manifest` empty.
#[cfg(feature = "metrics")]
#[test]
fn smoke_metrics_manager_boots_and_registers() {
    let manager = scalo::MetricsManager::new("smoke");
    let counter = manager.counter("boot_total", "Boots observed by the smoke test");
    counter.increment(1);

    // Callers pass the BARE name and the registry applies `{namespace}_` once.
    // Asserting the prefixed name is what catches a double prefix or a dropped
    // one, either of which renames every metric a consumer dashboards on.
    let manifest = manager.registry().manifest();
    assert_eq!(manifest.namespace, "smoke");
    assert!(
        manifest
            .metrics
            .iter()
            .any(|m| m.name == "smoke_boot_total"),
        "a counter created on the manager must reach the manifest under its \
         namespaced name, got {:?}",
        manifest.metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
    );
}

/// A process that has registered nothing is ready. A component defaulting to
/// unhealthy would hold every consumer's `/readyz` at 503 through a rollout.
#[cfg(feature = "health")]
#[test]
fn smoke_health_registry_boots_ready() {
    assert!(
        scalo::HealthRegistry::is_ready(),
        "an empty registry is ready"
    );
    assert!(
        scalo::HealthRegistry::is_healthy(),
        "an empty registry is healthy"
    );

    // Register an unhealthy component to prove the two calls above are reading
    // live state rather than a constant.
    scalo::HealthRegistry::register("smoke", || scalo::HealthStatus::Unhealthy);
    assert!(
        !scalo::HealthRegistry::is_ready(),
        "an unhealthy component must drop readiness"
    );
}

/// The shutdown token is what every drain path waits on. One that arrives
/// already cancelled would shut a pod down the moment it starts.
#[cfg(feature = "shutdown")]
#[tokio::test]
async fn smoke_shutdown_token_boots_uncancelled() {
    let token = scalo::shutdown::install_signal_handler();
    assert!(
        !token.is_cancelled(),
        "a freshly installed token must not be cancelled"
    );
}

/// Every discovered path has to be absolute. A relative one resolves against
/// whatever directory the container happened to start in.
#[cfg(feature = "runtime")]
#[test]
fn smoke_runtime_paths_are_absolute() {
    let paths = scalo::runtime::RuntimePaths::discover();

    for (name, path) in [
        ("config_dir", &paths.config_dir),
        ("secrets_dir", &paths.secrets_dir),
        ("data_dir", &paths.data_dir),
        ("temp_dir", &paths.temp_dir),
        ("logs_dir", &paths.logs_dir),
        ("cache_dir", &paths.cache_dir),
        ("run_dir", &paths.run_dir),
    ] {
        assert!(
            path.is_absolute(),
            "{name} must be absolute, got {}",
            path.display()
        );
    }
}
