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

#[cfg(feature = "secrets-vault")]
async fn resolve_vault(path_key: &str) -> Result<String, CredentialError> {
    use super::{SecretSource, SecretsConfig, SecretsManager};
    use std::collections::HashMap;

    let parts: Vec<&str> = path_key.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(CredentialError::BadSpec(format!(
            "invalid vault spec '{path_key}', expected 'path:key'"
        )));
    }
    let path = parts[0];
    let key = parts[1];

    let mut sources = HashMap::new();
    sources.insert(
        "_vault_lookup".to_string(),
        SecretSource::OpenBao {
            path: path.to_string(),
            key: key.to_string(),
        },
    );
    let config = SecretsConfig {
        sources,
        ..Default::default()
    };

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
}
