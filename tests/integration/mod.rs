// Project:   scalo
// File:      tests/integration/mod.rs
// Purpose:   Integration test module declarations
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

// Always-compiled integration tests (core modules only)
mod doc_ascii;
mod docs_rs_features;
mod env;
mod env_parity;

// Feature-gated integration tests. Each is gated on the library module it
// exercises so a partial `--features` run compiles instead of erroring (CI runs
// `features: all`, so all of these are on there). Matches env.rs's per-mod gating.
#[cfg(feature = "config")]
mod config_parity;

#[cfg(feature = "logger")]
mod logger_output;

#[cfg(feature = "metrics")]
mod metrics;

#[cfg(feature = "directory-config")]
mod directory_config;

#[cfg(feature = "expression")]
mod expression;

#[cfg(feature = "http")]
mod http_client_signed;

#[cfg(feature = "auth")]
mod auth;

#[cfg(all(
    feature = "deployment-test-support",
    feature = "transport",
    feature = "worker-batch"
))]
mod conformance;
