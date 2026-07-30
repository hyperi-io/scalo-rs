// Project:   scalo
// File:      src/secrets/resolve.rs
// Purpose:   Credential spec resolution (env, vault, literal) over the secrets module
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Credential specification resolution.
//!
//! Resolves credential specs in three formats:
//! - `vault:path:key` -- fetch from OpenBao via [`super::SecretsManager`]
//!   (requires the `secrets-vault` feature)
//! - `env:VAR_NAME`   -- read from the environment; hard error if unset
//! - any other string -- used as a literal value
//!
//! Folded in from the former `credential` module so data-plane services
//! (dfe-fetcher, etc.) share one spec syntax via `scalo::secrets`.

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

    /// The spec was malformed.
    #[error("invalid credential spec: {0}")]
    BadSpec(String),

    /// A `vault:` spec was used without the `secrets-vault` feature.
    #[error("vault: spec requires the `secrets-vault` feature to be enabled")]
    VaultUnsupported,
}

/// Resolve a credential spec to its plaintext value.
///
/// - `vault:path:key` resolves via OpenBao (needs `secrets-vault`)
/// - `env:VAR` reads the environment
/// - anything else is returned as a literal
///
/// # Errors
/// Returns [`CredentialError`] if an `env:` variable is unset, a `vault:`
/// lookup fails, the vault spec is malformed, or `vault:` is used without
/// the `secrets-vault` feature.
pub async fn resolve(spec: &str) -> Result<String, CredentialError> {
    if let Some(rest) = spec.strip_prefix("vault:") {
        resolve_vault(rest).await
    } else if let Some(var_name) = spec.strip_prefix("env:") {
        resolve_env(var_name)
    } else {
        Ok(spec.to_string())
    }
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
async fn resolve_vault(path_key: &str) -> Result<String, CredentialError> {
    use super::{SecretSource, SecretsManager};

    let parts: Vec<&str> = path_key.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(CredentialError::BadSpec(format!(
            "invalid vault spec '{path_key}', expected 'path:key'"
        )));
    }
    let path = parts[0];
    let key = parts[1];

    let mut config = secrets_config_for_lookup()?;
    config.sources.insert(
        "_vault_lookup".to_string(),
        SecretSource::OpenBao {
            path: path.to_string(),
            key: key.to_string(),
        },
    );

    let secrets = SecretsManager::new(config).map_err(|e| {
        CredentialError::Vault(format!("failed to initialise secrets manager: {e}"))
    })?;
    let value = secrets
        .get("_vault_lookup")
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
async fn resolve_vault(_path_key: &str) -> Result<String, CredentialError> {
    Err(CredentialError::VaultUnsupported)
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
