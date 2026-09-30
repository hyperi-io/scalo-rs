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

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

/// renovate: datasource=docker depName=openbao/openbao
const OPENBAO_TAG: &str = "2.6.1";

const ROOT_TOKEN: &str = "root";

/// Fail in CI rather than skip: a skipped container test reports green while
/// exercising nothing, and CI does provide a container runtime.
fn skip_or_fail(reason: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "no OpenBao available in CI: {reason}. This test must RUN here."
    );
    eprintln!("skipping: {reason}");
}

/// Start a dev-mode OpenBao and return `(container, address)`.
///
/// The `BAO_` env spellings are required. With `VAULT_DEV_ROOT_TOKEN_ID` the
/// server issues a RANDOM root token instead, so every request 403s and the
/// cause is never named.
async fn start_openbao() -> Option<(testcontainers::ContainerAsync<GenericImage>, String)> {
    let image = GenericImage::new("openbao/openbao", OPENBAO_TAG)
        .with_exposed_port(8200u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("OpenBao server started"))
        .with_env_var("BAO_DEV_ROOT_TOKEN_ID", ROOT_TOKEN)
        .with_env_var("BAO_DEV_LISTEN_ADDRESS", "0.0.0.0:8200")
        .with_cmd(["server", "-dev"]);

    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            skip_or_fail(&format!("container start failed: {e}"));
            return None;
        }
    };
    let host = match container.get_host().await {
        Ok(h) => h,
        Err(e) => {
            skip_or_fail(&format!("get_host failed: {e}"));
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(8200u16).await {
        Ok(p) => p,
        Err(e) => {
            skip_or_fail(&format!("get_host_port failed: {e}"));
            return None;
        }
    };
    Some((container, format!("http://{host}:{port}")))
}

/// Write a kv-v2 secret over the HTTP API, so the fixture does not depend on
/// the code under test.
async fn put_secret(address: &str, path: &str, key: &str, value: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{address}/v1/secret/data/{path}"))
        .header("X-Vault-Token", ROOT_TOKEN)
        .json(&serde_json::json!({ "data": { key: value } }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("write {path} to {address}: {e}"));
    assert!(
        resp.status().is_success(),
        "write {path} returned {}",
        resp.status()
    );
}

#[tokio::test]
async fn vault_spec_resolves_a_real_secret() {
    let Some((_container, address)) = start_openbao().await else {
        return;
    };
    put_secret(&address, "dfe/creds", "api-key", "super-secret-value").await;

    let resolved = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", Some(address.as_str())),
            ("VAULT_TOKEN", Some(ROOT_TOKEN)),
        ],
        async { scalo::secrets::resolve("vault:secret/data/dfe/creds:api-key").await },
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
    put_secret(&address, "dfe/creds", "api-key", "super-secret-value").await;

    let err = temp_env::async_with_vars(
        [
            ("VAULT_ADDR", Some(address.as_str())),
            ("VAULT_TOKEN", Some("not-the-root-token")),
        ],
        async {
            scalo::secrets::resolve("vault:secret/data/dfe/creds:api-key")
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
