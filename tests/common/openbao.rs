// Project:   scalo
// File:      tests/common/openbao.rs
// Purpose:   A real OpenBao dev server for the secrets integration tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A dev-mode OpenBao in a container, and a fixture writer that does not go
//! through the code under test.

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use super::skip_or_fail;

/// renovate: datasource=docker depName=openbao/openbao
pub const OPENBAO_TAG: &str = "2.6.1";

pub const ROOT_TOKEN: &str = "root";

/// Start a dev-mode OpenBao and return `(container, address)`.
///
/// The `BAO_` env spellings are required. With `VAULT_DEV_ROOT_TOKEN_ID` the
/// server issues a RANDOM root token instead, so every request 403s and the
/// cause is never named.
pub async fn start_openbao() -> Option<(ContainerAsync<GenericImage>, String)> {
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

/// Write a kv-v2 secret on the `secret` mount over the HTTP API, so the
/// fixture does not depend on the code under test. Each write is a new KV
/// version, numbered from 1.
pub async fn put_secret(address: &str, path: &str, key: &str, value: &str) {
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
