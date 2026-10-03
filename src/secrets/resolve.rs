// Project:   scalo
// File:      src/secrets/resolve.rs
// Purpose:   Credential spec resolution (env, file, vault, aws, literal) over the secrets module
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential specification resolution.
//!
//! Resolves credential specs in these formats:
//! - `vault:mount/path:key` -- fetch from OpenBao via [`super::SecretsManager`]
//!   (requires the `secrets-vault` feature); the KV v2 `data` segment is
//!   optional
//! - `bao:mount/path:key` / `openbao:mount/path:key` -- the same lookup under
//!   the names the OpenBao tooling uses
//! - `file:path` -- read a local file, typically a mounted Kubernetes Secret
//! - `aws:secret_id` or `aws:secret_id:key` -- fetch from AWS Secrets Manager
//!   (requires the `secrets-aws` feature)
//! - `env:VAR_NAME`   -- read from the environment; hard error if unset
//! - any other string -- used as a literal value
//!
//! Folded in from the former `credential` module so data-plane services
//! (fetcher credentials, etc.) share one spec syntax via `scalo::secrets`.

use thiserror::Error;

/// Errors that can arise resolving a credential spec.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// An `env:` spec referenced a variable that is not set.
    #[error("environment variable '{name}' is not set")]
    MissingEnvVar {
        /// Name of the missing environment variable.
        name: String,
    },

    /// A `vault:` lookup failed.
    #[error("vault resolution failed: {0}")]
    Vault(String),

    /// A `file:` read failed.
    #[error("file credential '{path}' could not be read: {message}")]
    File {
        /// Path the spec named.
        path: String,
        /// What the file provider reported.
        message: String,
    },

    /// An `aws:` lookup failed.
    #[error("aws resolution failed: {0}")]
    Aws(String),

    /// The spec was malformed.
    #[error("invalid credential spec: {0}")]
    BadSpec(String),

    /// A `vault:` spec was used without the `secrets-vault` feature.
    #[error("vault: spec requires the `secrets-vault` feature to be enabled")]
    VaultUnsupported,

    /// An `aws:` spec was used without the `secrets-aws` feature.
    #[error("aws: spec requires the `secrets-aws` feature to be enabled")]
    AwsUnsupported,
}

/// Resolve a credential spec to its plaintext value.
///
/// - `vault:mount/path:key`, `bao:mount/path:key` and `openbao:mount/path:key`
///   resolve via OpenBao (needs `secrets-vault`)
/// - `file:path` reads a local file
/// - `aws:secret_id` or `aws:secret_id:key` resolves via AWS Secrets Manager
///   (needs `secrets-aws`)
/// - `env:VAR` reads the environment
/// - anything else is returned as a literal
///
/// # Errors
/// Returns [`CredentialError`] if an `env:` variable is unset, a `file:` path
/// cannot be read, a `vault:` or `aws:` lookup fails, the spec is malformed, or
/// the spec names a provider whose feature is not enabled.
pub async fn resolve(spec: &str) -> Result<String, CredentialError> {
    match spec.split_once(':') {
        Some(("vault" | "bao" | "openbao", rest)) => {
            let (path, key) = parse_vault_spec(rest)?;
            resolve_vault(spec, path, key).await
        }
        Some(("file", path)) => resolve_file(spec, parse_file_spec(path)?).await,
        Some(("aws", rest)) => {
            let (secret_id, key) = parse_aws_spec(rest)?;
            resolve_aws(spec, secret_id, key).await
        }
        Some(("env", var_name)) => resolve_env(var_name),
        _ => Ok(spec.to_string()),
    }
}

/// Split the path and key out of a `vault:` spec.
///
/// Shape is checked before any provider work so a malformed spec reads the same
/// whether or not the `secrets-vault` feature is compiled in.
fn parse_vault_spec(rest: &str) -> Result<(&str, &str), CredentialError> {
    match rest.split_once(':') {
        Some((path, key)) if !path.is_empty() && !key.is_empty() => Ok((path, key)),
        _ => Err(CredentialError::BadSpec(format!(
            "invalid vault spec '{rest}', expected 'mount/path:key' with both parts present"
        ))),
    }
}

/// Check the path of a `file:` spec.
fn parse_file_spec(path: &str) -> Result<&str, CredentialError> {
    if path.is_empty() {
        return Err(CredentialError::BadSpec(
            "invalid file spec, expected 'file:path'".to_string(),
        ));
    }
    Ok(path)
}

/// Split the secret id and the optional JSON key out of an `aws:` spec.
///
/// An ARN is refused rather than split: it carries colons of its own, so the
/// `aws:secret_id[:key]` shape cannot say where the id ends.
fn parse_aws_spec(rest: &str) -> Result<(&str, Option<&str>), CredentialError> {
    if rest.starts_with("arn:") {
        return Err(CredentialError::BadSpec(format!(
            "aws spec '{rest}' names an ARN, which this shape cannot express because an \
             ARN contains colons -- use the secret name, or declare an `aws` source in \
             `secrets.sources` to look one up by ARN"
        )));
    }
    let (secret_id, key) = match rest.split_once(':') {
        Some((id, key)) => (id, Some(key)),
        None => (rest, None),
    };
    if secret_id.is_empty() {
        return Err(CredentialError::BadSpec(
            "invalid aws spec, expected 'aws:secret_id' or 'aws:secret_id:key'".to_string(),
        ));
    }
    if key == Some("") {
        return Err(CredentialError::BadSpec(format!(
            "invalid aws spec '{rest}', the key after the last ':' is empty -- drop the \
             trailing ':' for the whole secret, or name the JSON key"
        )));
    }
    Ok((secret_id, key))
}

/// The source name a one-off lookup registers its spec under.
///
/// [`super::SecretCache`] keys on the source name and binds the disk tier's
/// AEAD to it, and that disk tier is shared by every scalo process running as
/// this user, so a name fixed per provider would serve one spec's secret to the
/// next spec that asks.
fn one_off_source_name(spec: &str) -> String {
    format!("credential-spec:{spec}")
}

/// Turn the cache off for a one-off lookup.
///
/// The manager is discarded after the single read, so a cache entry can only be
/// read back by a different spec or a different process.
fn without_cache(config: &mut super::SecretsConfig) {
    config.cache = super::CacheConfig {
        enabled: false,
        ..super::CacheConfig::default()
    };
}

/// Resolve an optional credential spec -- returns `None` for `None`/empty.
///
/// # Errors
/// Returns [`CredentialError`] if a non-empty inner spec fails to resolve
/// (see [`resolve`]).
pub async fn resolve_optional(spec: Option<&str>) -> Result<Option<String>, CredentialError> {
    match spec {
        Some("") | None => Ok(None),
        Some(s) => Ok(Some(resolve(s).await?)),
    }
}

fn resolve_env(var_name: &str) -> Result<String, CredentialError> {
    std::env::var(var_name).map_err(|_| CredentialError::MissingEnvVar {
        name: var_name.to_string(),
    })
}

/// The `SecretsConfig` a one-off `file:` read runs against.
///
/// Nothing is cached: a mounted Kubernetes Secret is rewritten in place when it
/// rotates, so a cached copy would keep serving the retired value.
fn file_lookup_config() -> super::SecretsConfig {
    let mut config = super::SecretsConfig::default();
    without_cache(&mut config);
    config
}

/// Read a `file:` credential straight from disk.
async fn resolve_file(spec: &str, path: &str) -> Result<String, CredentialError> {
    use super::{SecretSource, SecretsManager};

    let failed = |message: String| CredentialError::File {
        path: path.to_string(),
        message,
    };

    let name = one_off_source_name(spec);
    let mut config = file_lookup_config();
    config.sources.insert(
        name.clone(),
        SecretSource::File {
            path: path.to_string(),
        },
    );

    let secrets = SecretsManager::new(config)
        .map_err(|e| failed(format!("failed to initialise secrets manager: {e}")))?;
    let value = secrets
        .get(&name)
        .await
        .map_err(|e| failed(e.to_string()))?;
    if value.as_bytes().is_empty() {
        return Err(failed(
            "file is empty, which is a mount or rotation failure rather than a credential"
                .to_string(),
        ));
    }
    let text = value
        .as_str()
        .map_err(|e| failed(format!("file secret not valid UTF-8: {e}")))?;
    tracing::debug!(path = path, "resolved file credential");
    Ok(text.to_string())
}

/// The `SecretsConfig` a one-off `vault:` lookup runs against, with an OpenBao
/// connection resolved from the `secrets` config section or the environment.
///
/// Resolving it is the whole point. `SecretsConfig::default()` leaves `openbao`
/// as `None`, `SecretsManager::new` then builds no vault provider, and every
/// lookup is refused with `provider not configured: openbao` before an address
/// or token is read -- so `VAULT_ADDR` could not influence it and no `vault:`
/// spec ever resolved.
#[cfg(all(feature = "secrets-vault", feature = "config"))]
fn secrets_config_for_lookup() -> Result<super::SecretsConfig, CredentialError> {
    let mut config = super::SecretsConfig::from_cascade();
    if config.openbao.is_none() {
        config.openbao = super::OpenBaoConfig::from_env();
    }
    if config.openbao.is_none() {
        return Err(CredentialError::Vault(
            "no OpenBao connection configured. Set VAULT_ADDR plus one of \
             VAULT_TOKEN, VAULT_ROLE_ID + VAULT_SECRET_ID or VAULT_K8S_ROLE \
             (OPENBAO_* and BAO_* are accepted as legacy fallbacks), or declare \
             a `secrets.openbao` section in the config"
                .to_string(),
        ));
    }
    without_cache(&mut config);
    Ok(config)
}

/// Without the `config` feature there is nothing to read the OpenBao address
/// from, so say that rather than failing later as an unconfigured provider.
#[cfg(all(feature = "secrets-vault", not(feature = "config")))]
fn secrets_config_for_lookup() -> Result<super::SecretsConfig, CredentialError> {
    Err(CredentialError::Vault(
        "vault: specs need the `config` feature enabled to read the OpenBao \
         connection from the environment or the config cascade"
            .to_string(),
    ))
}

#[cfg(feature = "secrets-vault")]
async fn resolve_vault(spec: &str, path: &str, key: &str) -> Result<String, CredentialError> {
    use super::{SecretSource, SecretsManager};

    let name = one_off_source_name(spec);
    let mut config = secrets_config_for_lookup()?;
    config.sources.insert(
        name.clone(),
        SecretSource::OpenBao {
            path: path.to_string(),
            key: key.to_string(),
        },
    );

    let secrets = SecretsManager::new(config).map_err(|e| {
        CredentialError::Vault(format!("failed to initialise secrets manager: {e}"))
    })?;
    let value = secrets
        .get(&name)
        .await
        .map_err(|e| CredentialError::Vault(format!("lookup failed for {path}:{key}: {e}")))?;
    let text = value
        .as_str()
        .map_err(|e| CredentialError::Vault(format!("vault secret not valid UTF-8: {e}")))?;
    tracing::debug!(path = path, key = key, "resolved vault credential");
    Ok(text.to_string())
}

#[cfg(not(feature = "secrets-vault"))]
#[allow(clippy::unused_async)] // signature must mirror the secrets-vault variant
async fn resolve_vault(_spec: &str, _path: &str, _key: &str) -> Result<String, CredentialError> {
    Err(CredentialError::VaultUnsupported)
}

/// The `SecretsConfig` a one-off `aws:` lookup runs against.
///
/// `SecretsManager::new` builds the AWS provider only when `aws` is set, so an
/// absent section is filled from the standard `AWS_*` variables rather than
/// left to fail later as an unconfigured provider.
#[cfg(feature = "secrets-aws")]
fn secrets_config_for_aws_lookup() -> super::SecretsConfig {
    let mut config = super::SecretsConfig::from_cascade();
    if config.aws.is_none() {
        #[cfg(feature = "config")]
        {
            config.aws = Some(super::AwsConfig::from_env());
        }
        #[cfg(not(feature = "config"))]
        {
            config.aws = Some(super::AwsConfig::default());
        }
    }
    without_cache(&mut config);
    config
}

#[cfg(feature = "secrets-aws")]
async fn resolve_aws(
    spec: &str,
    secret_id: &str,
    key: Option<&str>,
) -> Result<String, CredentialError> {
    use super::{SecretSource, SecretsManager};

    let name = one_off_source_name(spec);
    let mut config = secrets_config_for_aws_lookup();
    config.sources.insert(
        name.clone(),
        SecretSource::Aws {
            secret_id: secret_id.to_string(),
            key: key.map(str::to_string),
        },
    );

    // `AwsProvider::new` loads the SDK config with `Handle::block_on`, which
    // panics on a thread that is driving tasks (`src/secrets/aws.rs`).
    let secrets = tokio::task::spawn_blocking(move || SecretsManager::new(config))
        .await
        .map_err(|e| CredentialError::Aws(format!("secrets manager task failed: {e}")))?
        .map_err(|e| CredentialError::Aws(format!("failed to initialise secrets manager: {e}")))?;
    let value = secrets
        .get(&name)
        .await
        .map_err(|e| CredentialError::Aws(format!("lookup failed for {secret_id}: {e}")))?;
    let text = value
        .as_str()
        .map_err(|e| CredentialError::Aws(format!("aws secret not valid UTF-8: {e}")))?;
    tracing::debug!(secret_id = secret_id, "resolved aws credential");
    Ok(text.to_string())
}

#[cfg(not(feature = "secrets-aws"))]
#[allow(clippy::unused_async)] // signature must mirror the secrets-aws variant
async fn resolve_aws(
    _spec: &str,
    _secret_id: &str,
    _key: Option<&str>,
) -> Result<String, CredentialError> {
    Err(CredentialError::AwsUnsupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_literal() {
        let v = resolve("my-secret-value").await.unwrap();
        assert_eq!(v, "my-secret-value");
    }

    #[test]
    fn resolve_env_set() {
        temp_env::with_var("SCALO_TEST_CRED", Some("value-123"), || {
            assert_eq!(resolve_env("SCALO_TEST_CRED").unwrap(), "value-123");
        });
    }

    #[test]
    fn resolve_env_missing() {
        temp_env::with_var("SCALO_NONEXISTENT_XYZ", None::<&str>, || {
            let err = resolve_env("SCALO_NONEXISTENT_XYZ").unwrap_err();
            match err {
                CredentialError::MissingEnvVar { name } => {
                    assert_eq!(name, "SCALO_NONEXISTENT_XYZ");
                }
                other => panic!("expected MissingEnvVar, got {other:?}"),
            }
        });
    }

    #[tokio::test]
    async fn resolve_optional_none_returns_none() {
        assert!(resolve_optional(None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn resolve_optional_empty_returns_none() {
        assert!(resolve_optional(Some("")).await.unwrap().is_none());
    }

    #[tokio::test]
    #[cfg(not(feature = "secrets-vault"))]
    async fn vault_without_feature_returns_clear_error() {
        let err = resolve("vault:secret/x:k").await.unwrap_err();
        assert!(matches!(err, CredentialError::VaultUnsupported));
    }

    /// A `vault:` spec with no address configured must say which variables to
    /// set. It used to report `provider not configured: openbao`, which reads
    /// as a scalo build problem rather than "you have not told me where OpenBao
    /// is", and no amount of `VAULT_ADDR` changed it.
    #[tokio::test]
    #[cfg(all(feature = "secrets-vault", feature = "config"))]
    async fn vault_without_an_address_names_the_variables_to_set() {
        let err = temp_env::async_with_vars(
            [
                ("VAULT_ADDR", None::<&str>),
                ("OPENBAO_ADDR", None),
                ("BAO_ADDR", None),
            ],
            async { resolve("vault:secret/x:k").await.unwrap_err().to_string() },
        )
        .await;

        assert!(
            !err.contains("provider not configured"),
            "the unconfigured-provider dead end is back: {err}"
        );
        assert!(
            err.contains("VAULT_ADDR"),
            "the error must name VAULT_ADDR: {err}"
        );
    }

    #[tokio::test]
    async fn file_spec_reads_the_file_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "mounted-secret").unwrap();

        let spec = format!("file:{}", path.display());
        assert_eq!(resolve(&spec).await.unwrap(), "mounted-secret");
    }

    /// A mounted Kubernetes Secret is rewritten in place when it rotates, so the
    /// one-off read must not cache. The config here is the one a `file:` spec
    /// runs against, given a live directory and the plaintext opt-in, so both
    /// cache tiers would answer the second read if `enabled` ever flipped to
    /// true and the rotated value would be missed.
    #[tokio::test]
    async fn file_lookups_do_not_cache_a_rotated_value() {
        use super::super::{SecretSource, SecretsManager};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "before-rotation").unwrap();

        let name = one_off_source_name("file:token");
        let mut config = file_lookup_config();
        config.cache.directory = Some(dir.path().join("cache"));
        config.cache.allow_plaintext_disk_cache = true;
        config.sources.insert(
            name.clone(),
            SecretSource::File {
                path: path.display().to_string(),
            },
        );
        let secrets = SecretsManager::new(config).unwrap();

        let before = secrets.get(&name).await.unwrap();
        assert_eq!(before.as_str().unwrap(), "before-rotation");

        std::fs::write(&path, "after-rotation").unwrap();
        let after = secrets.get(&name).await.unwrap();
        assert_eq!(
            after.as_str().unwrap(),
            "after-rotation",
            "a rotated mount must not be answered from cache"
        );
    }

    /// The disk cache tier is shared by every scalo process running as this
    /// user and is keyed on the source name, so two one-off lookups must not
    /// register under the same name or the second spec reads the first spec's
    /// secret. `SecretSource::File` stands in for the providers that need a
    /// server.
    #[tokio::test]
    async fn one_off_lookups_do_not_share_a_cache_entry() {
        use super::super::CacheConfig;

        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        std::fs::write(&first, "first-secret").unwrap();
        std::fs::write(&second, "second-secret").unwrap();

        let cache = CacheConfig {
            enabled: true,
            directory: Some(dir.path().join("cache")),
            encryption_key: Some(crate::SensitiveString::from("cache-encryption-key")),
            ..CacheConfig::default()
        };

        assert_eq!(
            read_through_shared_cache(&cache, "file:first", &first).await,
            "first-secret"
        );
        assert_eq!(
            read_through_shared_cache(&cache, "file:second", &second).await,
            "second-secret",
            "the second spec must read its own secret, not the first spec's cached one"
        );
    }

    /// One spec's read through a fresh manager over a shared cache directory,
    /// as two scalo processes resolving two specs would see it.
    async fn read_through_shared_cache(
        cache: &super::super::CacheConfig,
        spec: &str,
        path: &std::path::Path,
    ) -> String {
        use super::super::{SecretSource, SecretsConfig, SecretsManager};

        let name = one_off_source_name(spec);
        let config = SecretsConfig {
            cache: cache.clone(),
            sources: [(
                name.clone(),
                SecretSource::File {
                    path: path.display().to_string(),
                },
            )]
            .into_iter()
            .collect(),
            ..SecretsConfig::default()
        };
        let secrets = SecretsManager::new(config).unwrap();
        let value = secrets.get(&name).await.unwrap();
        value.as_str().unwrap().to_string()
    }

    /// A zero-byte secret file is a mount or rotation failure, and resolving it
    /// to an empty password hands the failure to whatever authenticates next.
    #[tokio::test]
    async fn file_spec_refuses_a_zero_byte_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-token");
        std::fs::write(&path, "").unwrap();

        let err = resolve(&format!("file:{}", path.display()))
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains(&path.display().to_string()),
            "the refusal must name the path: {err}"
        );
    }

    /// An empty path is a malformed spec whatever the feature set, and reaching
    /// the filesystem with it would only produce a confusing read error.
    #[tokio::test]
    async fn file_spec_with_no_path_is_refused() {
        let err = resolve("file:").await.unwrap_err();
        assert!(
            matches!(err, CredentialError::BadSpec(_)),
            "expected BadSpec, got {err:?}"
        );
    }

    /// Spec shape is checked before provider dispatch, so an empty secret id is
    /// refused under every feature set rather than reported as a missing
    /// feature.
    #[tokio::test]
    async fn aws_spec_with_no_secret_id_is_refused() {
        let err = resolve("aws:").await.unwrap_err();
        assert!(
            matches!(err, CredentialError::BadSpec(_)),
            "expected BadSpec, got {err:?}"
        );
    }

    /// An `aws:` ARN cannot be split into id and key, so refuse it and name the
    /// way around it rather than looking up the leading fragment.
    #[tokio::test]
    async fn aws_spec_refuses_an_arn() {
        let err = resolve("aws:arn:aws:secretsmanager:ap-southeast-2:1:secret:x")
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("ARN"), "the refusal must name the ARN: {err}");
        assert!(
            err.contains("secrets.sources"),
            "the refusal must name the way around it: {err}"
        );
    }

    /// A trailing ':' with nothing after it is a typo, not a request for the
    /// whole secret, so refuse it by name rather than look up an empty JSON key.
    #[tokio::test]
    async fn aws_spec_with_an_empty_key_is_refused() {
        let err = resolve("aws:name:").await.unwrap_err();
        assert!(
            matches!(err, CredentialError::BadSpec(_)),
            "expected BadSpec, got {err:?}"
        );
        assert!(
            err.to_string().contains("'name:'"),
            "the refusal must name the spec: {err}"
        );
        assert_eq!(parse_aws_spec("name").unwrap(), ("name", None));
        assert_eq!(parse_aws_spec("name:key").unwrap(), ("name", Some("key")));
    }

    /// A `vault:` spec with no key cannot be served, so refuse it before the
    /// round trip rather than reading the whole secret and finding no key.
    #[tokio::test]
    async fn vault_spec_with_no_key_is_refused() {
        for spec in ["vault:secret/x:", "vault:secret/x"] {
            let err = resolve(spec).await.unwrap_err();
            assert!(
                matches!(err, CredentialError::BadSpec(_)),
                "{spec} must be refused as malformed, got {err:?}"
            );
        }
    }

    /// A one-off vault lookup throws its manager away after the read, so a
    /// cache entry can only be read back by a different spec or a different
    /// process.
    #[test]
    #[cfg(all(feature = "secrets-vault", feature = "config"))]
    fn vault_lookup_config_turns_the_cache_off() {
        let config = temp_env::with_vars(
            [
                ("VAULT_ADDR", Some("http://127.0.0.1:1")),
                ("VAULT_TOKEN", Some("not-a-real-token")),
            ],
            || secrets_config_for_lookup().unwrap(),
        );

        assert!(
            !config.cache.enabled,
            "a one-off vault lookup must not cache"
        );
    }

    /// As for vault: nothing reads the entry back except another spec or
    /// another process.
    #[test]
    #[cfg(feature = "secrets-aws")]
    fn aws_lookup_config_turns_the_cache_off() {
        assert!(
            !secrets_config_for_aws_lookup().cache.enabled,
            "a one-off aws lookup must not cache"
        );
    }

    /// With no region in the environment the lookup must leave it unset, so the
    /// SDK chain (profile, IMDS) answers instead of a pinned default.
    #[test]
    #[cfg(feature = "secrets-aws")]
    fn aws_lookup_leaves_an_unset_region_to_the_sdk() {
        let region = temp_env::with_vars(
            [
                ("AWS_REGION", None::<&str>),
                ("AWS_DEFAULT_REGION", None),
                ("AWS_ENDPOINT_URL", None),
            ],
            || secrets_config_for_aws_lookup().aws.unwrap().region,
        );

        assert!(
            region.is_none(),
            "an unset region must stay unset, got {region:?}"
        );
    }

    #[tokio::test]
    async fn file_spec_for_a_missing_path_names_the_path() {
        let err = resolve("file:/nonexistent/scalo-credential-spec")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("/nonexistent/scalo-credential-spec"),
            "the error must name the path it could not read: {err}"
        );
    }

    /// `bao:` and `openbao:` are spellings of the same provider, so a spec must
    /// fail identically whichever one a deployment writes.
    #[tokio::test]
    #[cfg(all(feature = "secrets-vault", feature = "config"))]
    async fn bao_and_openbao_resolve_as_vault_does() {
        let errors = temp_env::async_with_vars(
            [
                ("VAULT_ADDR", None::<&str>),
                ("OPENBAO_ADDR", None),
                ("BAO_ADDR", None),
            ],
            async {
                let mut out = Vec::new();
                for spec in ["vault:secret/x:k", "bao:secret/x:k", "openbao:secret/x:k"] {
                    out.push(resolve(spec).await.unwrap_err().to_string());
                }
                out
            },
        )
        .await;

        assert_eq!(errors[0], errors[1], "bao: must fail as vault: does");
        assert_eq!(errors[0], errors[2], "openbao: must fail as vault: does");
    }

    #[tokio::test]
    #[cfg(not(feature = "secrets-vault"))]
    async fn bao_aliases_share_the_vault_feature_refusal() {
        for spec in ["bao:secret/x:k", "openbao:secret/x:k"] {
            let err = resolve(spec).await.unwrap_err();
            assert!(
                matches!(err, CredentialError::VaultUnsupported),
                "{spec} must be refused as vault: is, got {err:?}"
            );
        }
    }

    #[tokio::test]
    #[cfg(not(feature = "secrets-aws"))]
    async fn aws_without_the_feature_names_the_feature() {
        let err = resolve("aws:prod/auth/tokens:bearer")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("secrets-aws"),
            "the refusal must name the feature to enable: {err}"
        );
    }

    /// With the feature on, an `aws:` spec must reach the SDK -- the point being
    /// that the provider was constructed at all. A refused connection to a closed
    /// port is the expected outcome.
    #[tokio::test]
    #[cfg(all(feature = "secrets-aws", feature = "config"))]
    async fn aws_spec_attempts_the_lookup() {
        let err = temp_env::async_with_vars(
            [
                ("AWS_ENDPOINT_URL", Some("http://127.0.0.1:1")),
                ("AWS_DEFAULT_REGION", Some("ap-southeast-2")),
                ("AWS_ACCESS_KEY_ID", Some("not-a-real-key")),
                ("AWS_SECRET_ACCESS_KEY", Some("not-a-real-secret")),
            ],
            async {
                resolve("aws:prod/auth/tokens:bearer")
                    .await
                    .unwrap_err()
                    .to_string()
            },
        )
        .await;

        assert!(
            !err.contains("provider not configured"),
            "an AWS region was configured, so the provider must have been built: {err}"
        );
        assert!(
            err.contains("lookup failed"),
            "expected a failed lookup against the unreachable endpoint: {err}"
        );
    }

    /// An unrecognised prefix stays a literal: passwords contain colons, and the
    /// resolver must not claim a vocabulary it cannot read.
    #[tokio::test]
    async fn an_unknown_prefix_stays_a_literal() {
        assert_eq!(
            resolve("gcp:projects/p/secrets/s").await.unwrap(),
            "gcp:projects/p/secrets/s"
        );
        assert_eq!(resolve("p4ss:w0rd").await.unwrap(), "p4ss:w0rd");
    }

    /// With an address set, the lookup must reach the network -- the point being
    /// that provider construction happened at all. A refused connection to a
    /// closed port is the expected outcome.
    #[tokio::test]
    #[cfg(all(feature = "secrets-vault", feature = "config"))]
    async fn vault_with_an_address_attempts_the_lookup() {
        let err = temp_env::async_with_vars(
            [
                // Port 1 on loopback: reserved, never listening.
                ("VAULT_ADDR", Some("http://127.0.0.1:1")),
                ("VAULT_TOKEN", Some("not-a-real-token")),
            ],
            async { resolve("vault:secret/x:k").await.unwrap_err().to_string() },
        )
        .await;

        assert!(
            !err.contains("provider not configured"),
            "an address was configured, so the provider must have been built: {err}"
        );
        assert!(
            err.contains("lookup failed"),
            "expected a failed lookup against the unreachable address: {err}"
        );
    }
}
