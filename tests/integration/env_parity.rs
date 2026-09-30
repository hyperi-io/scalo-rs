// Project:   scalo
// File:      tests/integration/env_parity.rs
// Purpose:   Environment detection parity tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Environment detection parity tests.
//!
//! These tests verify that environment detection behaves identically
//! to scalo-py's env handling.

use scalo::env::{Environment, get_app_env};

/// Test that Environment::detect() returns valid enum.
#[test]
fn test_detect_returns_valid_environment() {
    let env = Environment::detect();
    assert!(matches!(
        env,
        Environment::Kubernetes
            | Environment::Docker
            | Environment::Container
            | Environment::BareMetal
    ));
}

/// Test is_container() matches Go behaviour.
#[test]
fn test_is_container_parity() {
    // K8s, Docker, Container should return true
    assert!(Environment::Kubernetes.is_container());
    assert!(Environment::Docker.is_container());
    assert!(Environment::Container.is_container());

    // BareMetal should return false
    assert!(!Environment::BareMetal.is_container());
}

/// Test get_app_env() priority: APP_ENV > ENVIRONMENT > ENV > "development".
#[test]
fn test_get_app_env_priority() {
    // [APP_ENV, ENVIRONMENT, ENV], and the name each set resolves to.
    let cases = [
        ([None, None, None], "development"),
        ([None, None, Some("staging")], "staging"),
        ([None, Some("production"), Some("staging")], "production"),
        (
            [Some("testing"), Some("production"), Some("staging")],
            "testing",
        ),
    ];
    for ([app_env, environment, env], expected) in cases {
        temp_env::with_vars(
            [
                ("APP_ENV", app_env),
                ("ENVIRONMENT", environment),
                ("ENV", env),
            ],
            || assert_eq!(get_app_env(), expected),
        );
    }
}

/// Test Display implementation matches Go string output.
#[test]
fn test_environment_display_parity() {
    assert_eq!(Environment::Kubernetes.to_string(), "kubernetes");
    assert_eq!(Environment::Docker.to_string(), "docker");
    assert_eq!(Environment::Container.to_string(), "container");
    assert_eq!(Environment::BareMetal.to_string(), "bare_metal");
}
