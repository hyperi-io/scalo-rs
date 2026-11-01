// Project:   scalo
// File:      tests/secrets_refresh.rs
// Purpose:   SecretsManager refresh, rotation, failure paths and documented config
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `SecretsManager::refresh_all`, rotation events and provider failures,
//! against real files and a real OpenBao, plus the config example
//! `docs/api/secrets.md` documents.
//!
//! A file source carries no version, so rotation is exercised against OpenBao
//! (KV v2), which numbers every write: the run's shared server, each test under
//! a path of its own. The OpenBao tests skip when Docker is absent, and fail
//! instead of skipping under CI.

#![cfg(feature = "secrets")]

mod common;

use std::path::{Path, PathBuf};

use scalo::secrets::{CacheConfig, SecretSource, SecretsConfig, SecretsError, SecretsManager};
use tempfile::TempDir;

const NAME: &str = "api_key";

/// A cache that holds entries for `ttl_secs` and serves them stale for a day
/// after, in a directory the test owns.
fn caching(dir: &Path, ttl_secs: u64) -> CacheConfig {
    CacheConfig {
        enabled: true,
        directory: Some(dir.to_path_buf()),
        ttl_secs,
        stale_grace_secs: 86_400,
        ..Default::default()
    }
}

fn no_caching() -> CacheConfig {
    CacheConfig {
        enabled: false,
        ..Default::default()
    }
}

fn file_manager(path: &Path, cache: CacheConfig) -> SecretsManager {
    let config = SecretsConfig {
        cache,
        sources: [(
            NAME.to_string(),
            SecretSource::File {
                path: path.to_str().expect("utf-8 temp path").to_string(),
            },
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    SecretsManager::new(config).expect("build the manager")
}

fn secret_file(dir: &TempDir, value: &str) -> PathBuf {
    let path = dir.path().join("api-key");
    std::fs::write(&path, value).expect("write the secret file");
    path
}

async fn read(manager: &SecretsManager) -> String {
    manager
        .get(NAME)
        .await
        .expect("get the secret")
        .as_str()
        .expect("utf-8 secret")
        .to_string()
}

/// A fresh entry still goes back to the source on refresh, so a value changed
/// at the source is what the next `get` returns.
#[tokio::test]
async fn refresh_rereads_a_fresh_file_entry() {
    let secrets = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let path = secret_file(&secrets, "first");
    let manager = file_manager(&path, caching(cache.path(), 3600));
    assert_eq!(read(&manager).await, "first");

    std::fs::write(&path, "second").unwrap();
    manager.refresh_all().await.expect("refresh");

    assert_eq!(
        read(&manager).await,
        "second",
        "refresh_all left the pre-refresh value in the cache"
    );
}

/// A source that fails on refresh is reported, not answered from the stale
/// entry the cache still holds.
#[tokio::test]
async fn a_failed_refresh_is_reported_not_served_stale() {
    let secrets = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let path = secret_file(&secrets, "first");
    let manager = file_manager(&path, caching(cache.path(), 0));
    assert_eq!(read(&manager).await, "first");

    std::fs::remove_file(&path).unwrap();

    match manager.refresh_all().await {
        Err(SecretsError::RefreshFailed(detail)) => assert!(
            detail.contains(NAME),
            "the failure must name the source: {detail}"
        ),
        other => panic!("a refresh whose source is gone must fail, got {other:?}"),
    }

    assert_eq!(
        read(&manager).await,
        "first",
        "get still falls back to the stale value the failed refresh left in place"
    );
}

/// With caching off, a provider failure on `get` reaches the caller.
#[tokio::test]
async fn caching_off_raises_a_provider_failure() {
    let secrets = TempDir::new().unwrap();
    let path = secret_file(&secrets, "first");
    let manager = file_manager(&path, no_caching());
    assert_eq!(read(&manager).await, "first");

    std::fs::remove_file(&path).unwrap();

    let result = manager.get(NAME).await;
    assert!(
        matches!(result, Err(SecretsError::NotFound(_))),
        "caching off must not serve a value the source no longer has: {result:?}"
    );
}

/// The control for the test above: the same steps with caching on and every
/// entry already expired serve the stale value, so that test's assertion can
/// tell the two configurations apart.
#[tokio::test]
async fn caching_on_serves_the_stale_value_on_a_provider_failure() {
    let secrets = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let path = secret_file(&secrets, "first");
    let manager = file_manager(&path, caching(cache.path(), 0));
    assert_eq!(read(&manager).await, "first");

    std::fs::remove_file(&path).unwrap();

    assert_eq!(read(&manager).await, "first");
}

#[cfg(all(feature = "secrets-vault", feature = "deployment-test-support", unix))]
mod openbao {
    use scalo::secrets::{
        CacheConfig, OpenBaoConfig, RotationEvent, SecretSource, SecretsConfig, SecretsManager,
    };
    use tempfile::TempDir;
    use tokio::sync::broadcast::Receiver;
    use tokio::sync::broadcast::error::TryRecvError;

    use super::common::openbao::{ROOT_TOKEN, kv_path, put_secret, start_openbao};
    use super::{NAME, caching, no_caching, read};

    const KEY: &str = "api-key";

    fn manager(address: &str, path: &str, cache: CacheConfig) -> SecretsManager {
        let config = SecretsConfig {
            cache,
            openbao: Some(OpenBaoConfig::with_token(address, ROOT_TOKEN)),
            sources: [(
                NAME.to_string(),
                SecretSource::OpenBao {
                    path: format!("secret/data/{path}"),
                    key: KEY.to_string(),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        SecretsManager::new(config).expect("build the manager")
    }

    fn assert_no_event(rx: &mut Receiver<RotationEvent>, after: &str) {
        let next = rx.try_recv();
        assert!(
            matches!(next, Err(TryRecvError::Empty)),
            "no rotation event expected after {after}, got {next:?}"
        );
    }

    fn assert_rotated(rx: &mut Receiver<RotationEvent>, old: &str, new: &str) {
        let event = rx.try_recv().expect("a rotation event");
        assert_eq!(event.name, NAME);
        assert_eq!(event.old_version.as_deref(), Some(old));
        assert_eq!(event.new_version, new);
    }

    /// KV v2 numbers every write, and a plain `get` carries that number.
    #[tokio::test]
    async fn a_get_carries_the_kv_version() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let manager = manager(&address, &path, no_caching());

        put_secret(&address, &path, KEY, "first").await;
        let first = manager.get(NAME).await.expect("get v1");
        assert_eq!(first.metadata.version.as_deref(), Some("1"));

        put_secret(&address, &path, KEY, "second").await;
        let second = manager.get(NAME).await.expect("get v2");
        assert_eq!(second.as_str().unwrap(), "second");
        assert_eq!(second.metadata.version.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn refresh_refetches_a_fresh_entry() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 3600));
        put_secret(&address, &path, KEY, "first").await;
        assert_eq!(read(&manager).await, "first");

        put_secret(&address, &path, KEY, "second").await;
        manager.refresh_all().await.expect("refresh");

        assert_eq!(
            read(&manager).await,
            "second",
            "refresh_all left the pre-refresh value in the cache"
        );
    }

    /// A real change is reported once, with both versions, and a second
    /// refresh with nothing new reports nothing.
    #[tokio::test]
    async fn a_rotation_is_reported_once_with_both_versions() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 3600));
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        put_secret(&address, &path, KEY, "second").await;
        manager.refresh_all().await.expect("first refresh");
        assert_rotated(&mut rx, "1", "2");
        assert_no_event(&mut rx, "the rotation event");

        manager.refresh_all().await.expect("second refresh");
        assert_no_event(&mut rx, "a refresh with nothing new");
    }

    #[tokio::test]
    async fn refresh_reports_nothing_when_nothing_changed() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 3600));
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        manager.refresh_all().await.expect("refresh");
        assert_no_event(&mut rx, "a refresh of an unchanged secret");
    }

    /// An entry past its TTL still holds the version it was fetched at, so
    /// expiry alone is not a rotation.
    #[tokio::test]
    async fn an_expired_entry_is_not_reported_as_rotated() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 0));
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        manager.refresh_all().await.expect("refresh");
        assert_no_event(&mut rx, "a refresh of an expired, unchanged entry");
    }

    #[tokio::test]
    async fn a_rotation_after_expiry_reports_the_old_version() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 0));
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        put_secret(&address, &path, KEY, "second").await;
        manager.refresh_all().await.expect("refresh");
        assert_rotated(&mut rx, "1", "2");
    }

    /// A `get` that reaches the provider reports a rotation the same way
    /// `refresh_all` does, once, so a subscriber hears of it before any refresh.
    #[tokio::test]
    async fn a_get_past_expiry_reports_a_rotation_once() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 0));
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        put_secret(&address, &path, KEY, "second").await;
        assert_eq!(read(&manager).await, "second");
        assert_rotated(&mut rx, "1", "2");

        read(&manager).await;
        assert_no_event(&mut rx, "a second get of the same version");
        manager.refresh_all().await.expect("refresh");
        assert_no_event(&mut rx, "a refresh after the get already reported it");
    }

    /// A fresh cache hit never reaches the provider, so it reports nothing
    /// even after the store moved on.
    #[tokio::test]
    async fn a_fresh_cache_hit_reports_nothing() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let cache = TempDir::new().unwrap();
        let manager = manager(&address, &path, caching(cache.path(), 3600));
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        put_secret(&address, &path, KEY, "second").await;
        assert_eq!(read(&manager).await, "first");
        assert_no_event(&mut rx, "a fresh cache hit");
    }

    /// With caching off there is no previous version to compare against, so
    /// refresh reports no rotation, whether or not the secret changed.
    #[tokio::test]
    async fn caching_off_reports_no_rotation() {
        let Some((_container, address)) = start_openbao().await else {
            return;
        };
        let path = kv_path(scalo::test_name!());
        let manager = manager(&address, &path, no_caching());
        put_secret(&address, &path, KEY, "first").await;
        read(&manager).await;
        let mut rx = manager.subscribe_rotations();

        manager.refresh_all().await.expect("first refresh");
        assert_no_event(&mut rx, "a refresh with caching off");

        put_secret(&address, &path, KEY, "second").await;
        manager.refresh_all().await.expect("second refresh");
        assert_no_event(&mut rx, "a refresh of a changed secret with caching off");
    }
}

/// The YAML under "Config shape" in `docs/api/secrets.md` parses into
/// `SecretsConfig` and means what the doc says it means.
#[cfg(all(feature = "config", feature = "secrets-vault", feature = "secrets-aws"))]
mod documented_config {
    use scalo::secrets::{CacheConfig, OpenBaoAuth, SecretSource, SecretsConfig};

    #[derive(serde::Deserialize)]
    struct Root {
        secrets: SecretsConfig,
    }

    /// The YAML block that follows `heading`, or `None` outside a checkout,
    /// since the published crate leaves `docs/` out.
    fn documented_yaml(heading: &str) -> Option<String> {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/docs/api/secrets.md");
        let Ok(doc) = std::fs::read_to_string(path) else {
            assert!(
                std::env::var_os("CI").is_none(),
                "{path} must be readable in CI"
            );
            eprintln!("skipping: {path} is not in this tree");
            return None;
        };
        let section = doc
            .split(heading)
            .nth(1)
            .expect("the heading is in the doc");
        let block = section.split("```yaml\n").nth(1).expect("a yaml block");
        Some(
            block
                .split("```")
                .next()
                .expect("a closed block")
                .to_string(),
        )
    }

    #[test]
    fn the_documented_config_example_parses_as_described() {
        let Some(yaml) = documented_yaml("## Config shape") else {
            return;
        };
        let secrets = scalo::yaml::from_str::<Root>(
            &yaml,
            "docs/api/secrets.md",
            scalo::yaml::Profile::Config,
        )
        .expect("the documented example parses")
        .secrets;

        let defaults = CacheConfig::default();
        assert_eq!(secrets.cache.enabled, defaults.enabled);
        assert_eq!(secrets.cache.ttl_secs, defaults.ttl_secs);
        assert_eq!(secrets.cache.stale_grace_secs, defaults.stale_grace_secs);
        assert_eq!(
            secrets.cache.allow_plaintext_disk_cache,
            defaults.allow_plaintext_disk_cache
        );
        assert!(secrets.cache.directory.is_some());

        let openbao = secrets.openbao.expect("the openbao block");
        assert!(matches!(openbao.auth, OpenBaoAuth::Kubernetes { .. }));
        assert!(openbao.ca_cert.is_some());
        let aws = secrets.aws.expect("the aws block");
        assert_eq!(aws.region.as_deref(), Some("ap-southeast-2"));

        assert!(matches!(
            secrets.sources.get("db_password"),
            Some(SecretSource::OpenBao { .. })
        ));
        assert!(matches!(
            secrets.sources.get("tls_key"),
            Some(SecretSource::File { .. })
        ));
        assert!(matches!(
            secrets.sources.get("api_token"),
            Some(SecretSource::Aws { key: Some(_), .. })
        ));
    }

    /// The `auth.method` spellings and the `null` file mode the doc's tables
    /// give.
    #[test]
    fn the_documented_spellings_parse() {
        let token: OpenBaoAuth = scalo::yaml::from_str(
            "method: token\ntoken: t\n",
            "test",
            scalo::yaml::Profile::Config,
        )
        .unwrap();
        assert!(matches!(token, OpenBaoAuth::Token { .. }));
        let approle: OpenBaoAuth = scalo::yaml::from_str(
            "method: app_role\nrole_id: r\nsecret_id: s\n",
            "test",
            scalo::yaml::Profile::Config,
        )
        .unwrap();
        assert!(matches!(approle, OpenBaoAuth::AppRole { ref mount, .. } if mount == "approle"));

        let cache: CacheConfig =
            scalo::yaml::from_str("dir_mode: null\n", "test", scalo::yaml::Profile::Config)
                .unwrap();
        assert!(cache.dir_mode.is_none());
    }
}
