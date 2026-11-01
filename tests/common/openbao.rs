// Project:   scalo
// File:      tests/common/openbao.rs
// Purpose:   The run's shared OpenBao dev server for the secrets integration tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The run's one dev-mode OpenBao, shared by every test process, and a fixture
//! writer that does not go through the code under test. Each test writes
//! under a path of its own ([`kv_path`]), since KV v2 numbers versions per
//! path.

use std::sync::OnceLock;
use std::time::Duration;

use scalo::deployment::test_support::shared_service::{self, Ready, ServiceSpec, SharedContainer};

use super::{SUITE, docker_answers, skip_or_fail};

/// renovate: datasource=docker depName=openbao/openbao
pub const OPENBAO_TAG: &str = "2.6.1";

/// The multi-arch index digest of [`OPENBAO_TAG`].
const OPENBAO_DIGEST: &str =
    "sha256:5b2486ab0fb90bbc788cc345b0a08616dfb375873ee8be5df3a2fd4d378a67e0";

pub const ROOT_TOKEN: &str = "root";

const OPENBAO_PORT: u16 = 8200;

/// A dev-mode OpenBao.
///
/// The `BAO_` env spellings are required. With `VAULT_DEV_ROOT_TOKEN_ID` the
/// server issues a RANDOM root token instead, so every request 403s and the
/// cause is never named.
fn openbao_spec() -> ServiceSpec {
    ServiceSpec {
        ports: vec![OPENBAO_PORT],
        env: vec![
            ("BAO_DEV_ROOT_TOKEN_ID".into(), ROOT_TOKEN.into()),
            ("BAO_DEV_LISTEN_ADDRESS".into(), "0.0.0.0:8200".into()),
        ],
        args: vec!["server".into(), "-dev".into()],
        ready: Ready::Log("OpenBao server started"),
        ..ServiceSpec::new(
            "openbao",
            format!("openbao/openbao:{OPENBAO_TAG}@{OPENBAO_DIGEST}"),
            Duration::from_secs(120),
        )
    }
}

/// The run's OpenBao and its address, or `None` after a skip when no Docker
/// daemon answers outside CI. The unit value stands where a container guard
/// stood: the run owns the server.
pub async fn start_openbao() -> Option<((), String)> {
    static SHARED: OnceLock<SharedContainer> = OnceLock::new();
    if !docker_answers() {
        skip_or_fail("no Docker daemon answers `docker info`");
        return None;
    }
    let container = match SHARED.get() {
        Some(container) => container.clone(),
        None => tokio::task::spawn_blocking(|| {
            SHARED
                .get_or_init(|| shared_service::acquire(&SUITE, &openbao_spec()))
                .clone()
        })
        .await
        .expect("acquire the shared OpenBao"),
    };
    Some(((), format!("http://{}", container.addr(OPENBAO_PORT))))
}

/// The KV path of the test named `test` (pass `scalo::test_name!()`), unique
/// across the tests of one process and the processes of one run.
pub fn kv_path(test: &str) -> String {
    super::unique(&format!("app/{test}"))
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
