// Project:   scalo
// File:      tests/secrets_vault_resolve.rs
// Purpose:   End-to-end `vault:` credential resolution against a real OpenBao
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `vault:path:key` resolution against a real OpenBao server.
//!
//! The unit tests in `src/secrets/resolve.rs` can only prove the provider gets
//! built and the lookup leaves the process. Whether a secret actually comes back
//! needs a server, and this is the only test that proves `vault:` specs work at
//! all -- every consumer (fetcher credentials, transport SASL passwords)
//! goes through the same `resolve()`.
//!
//! Skips when Docker is absent, and fails instead of skipping under CI, where a
//! container runtime is promised.

#![cfg(all(feature = "secrets-vault", feature = "config"))]

mod common;

use common::openbao::{ROOT_TOKEN, put_secret, start_openbao};

#[tokio::test]
async fn vault_spec_resolves_a_real_secret() {
    let Some((_container, address)) = start_openbao().await else {
        return;
    };
    put_secret(&address, "app/creds", "api-key", "super-secret-value").await;

    let resolved = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", Some(address.as_str())),
            ("VAULT_TOKEN", Some(ROOT_TOKEN)),
        ],
        async { scalo::secrets::resolve("vault:secret/data/app/creds:api-key").await },
    )
    .await
    .unwrap_or_else(|e| panic!("resolve against {address}: {e}"));

    assert_eq!(resolved, "super-secret-value");
}

/// A path that does not exist must fail for the right reason. `is_err()` alone
/// is satisfied by an unconfigured provider, which says nothing about the path.
#[tokio::test]
async fn vault_spec_for_a_missing_path_fails_on_the_lookup() {
    let Some((_container, address)) = start_openbao().await else {
        return;
    };

    let err = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", Some(address.as_str())),
            ("VAULT_TOKEN", Some(ROOT_TOKEN)),
        ],
        async {
            scalo::secrets::resolve("vault:secret/data/nope/absent:api-key")
                .await
                .expect_err("a missing path must not resolve")
                .to_string()
        },
    )
    .await;

    assert!(
        !err.contains("provider not configured"),
        "the lookup never reached OpenBao, so this says nothing about the \
         missing path: {err}"
    );
    assert!(err.contains("lookup failed"), "unexpected error: {err}");
}

/// A wrong token must fail on authentication, not silently fall back to some
/// other credential source.
#[tokio::test]
async fn vault_spec_with_a_bad_token_fails() {
    let Some((_container, address)) = start_openbao().await else {
        return;
    };
    put_secret(&address, "app/creds", "api-key", "super-secret-value").await;

    let err = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", Some(address.as_str())),
            ("VAULT_TOKEN", Some("not-the-root-token")),
        ],
        async {
            scalo::secrets::resolve("vault:secret/data/app/creds:api-key")
                .await
                .expect_err("a bad token must not resolve the secret")
                .to_string()
        },
    )
    .await;

    assert!(
        !err.contains("provider not configured"),
        "the lookup never reached OpenBao: {err}"
    );
}
