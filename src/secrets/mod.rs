// Project:   scalo
// File:      src/secrets/mod.rs
// Purpose:   Secrets management with multi-provider support and caching
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Secrets management with multi-provider support and resilient caching.
//!
//! Provides a unified interface for loading certificates, credentials, and other
//! sensitive data from multiple sources with automatic caching for resilience.
//!
//! ## Providers
//!
//! - **File**: Local filesystem (always available)
//! - **OpenBao/Vault**: HashiCorp Vault API (requires `secrets-vault` feature)
//! - **AWS Secrets Manager**: AWS SDK (requires `secrets-aws` feature)
//!
//! ## Features
//!
//! - Multi-provider support with unified API
//! - Local disk cache with TTL for resilience
//! - Stale cache fallback when providers are unavailable
//! - Refresh of every configured source, on demand (`refresh_all`) or on a
//!   jittered timer (`spawn_refresh`)
//! - Rotation events for application notification
//!
//! ## Credential specs
//!
//! [`resolve`](fn@resolve) / [`resolve_optional`] turn a short credential spec string
//! into a plaintext value, so data-plane services share one syntax:
//!
//! - `vault:mount/path:key` -- fetch from OpenBao (needs the `secrets-vault`
//!   feature); the first segment is the KV mount and the KV v2 `data` segment
//!   is optional
//! - `bao:mount/path:key` / `openbao:mount/path:key` -- the same lookup,
//!   spelled as the OpenBao tooling spells it
//! - `file:path` -- read a local file, typically a mounted Kubernetes Secret
//! - `aws:secret_id` or `aws:secret_id:key` -- fetch from AWS Secrets Manager
//!   (needs the `secrets-aws` feature)
//! - `env:VAR` -- read an environment variable (hard error if unset)
//! - any other string -- used as a literal value
//!
//! Folded in from the former `credential` module - see [`CredentialError`]
//! for the failure modes.
//!
//! ## Example
//!
//! ```rust,no_run
//! use scalo::secrets::{SecretsManager, SecretsConfig, SecretSource};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Simple file-based usage
//!     let secrets = SecretsManager::new(SecretsConfig::default())?;
//!     let cert = secrets.get_file("/etc/ssl/cert.pem").await?;
//!
//!     // With named sources
//!     let config = SecretsConfig {
//!         sources: vec![
//!             ("tls_cert".into(), SecretSource::File { path: "/etc/ssl/cert.pem".into() }),
//!         ].into_iter().collect(),
//!         ..Default::default()
//!     };
//!     let secrets = SecretsManager::new(config)?;
//!     let cert = secrets.get("tls_cert").await?;
//!
//!     Ok(())
//! }
//! ```

mod cache;
mod crypto;
mod error;
mod provider;
mod resolve;
mod types;

#[cfg(feature = "secrets-vault")]
mod vault;

#[cfg(feature = "secrets-aws")]
mod aws;

pub use cache::SecretCache;
pub use error::{SecretsError, SecretsResult};
pub use provider::{FileProvider, SecretProvider};
pub use resolve::{CredentialError, resolve, resolve_optional};
pub use types::{
    CacheConfig, RotationEvent, SecretMetadata, SecretSource, SecretValue, SecretsConfig,
};

#[cfg(feature = "secrets-vault")]
pub use vault::{OpenBaoAuth, OpenBaoConfig, OpenBaoProvider};

#[cfg(feature = "secrets-aws")]
pub use aws::{AwsConfig, AwsProvider};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Which providers are mid-outage, so a failing health check is logged once when
/// it starts and once when it ends, not on every poll.
#[cfg(any(feature = "secrets-vault", feature = "secrets-aws"))]
#[derive(Default)]
struct HealthLog {
    down: parking_lot::Mutex<std::collections::HashSet<&'static str>>,
}

#[cfg(any(feature = "secrets-vault", feature = "secrets-aws"))]
impl HealthLog {
    /// Log the outcome of one check and return whether the provider is healthy.
    fn record(&self, provider: &'static str, result: &SecretsResult<()>) -> bool {
        let mut down = self.down.lock();
        match result {
            Ok(()) => {
                if down.remove(provider) {
                    info!(provider, "Secrets provider is healthy again");
                }
                true
            }
            Err(e) => {
                if down.insert(provider) {
                    warn!(provider, error = %e, "Secrets provider health check failed");
                }
                false
            }
        }
    }
}

/// Secrets manager that coordinates providers and caching.
pub struct SecretsManager {
    config: SecretsConfig,
    cache: Arc<RwLock<SecretCache>>,
    file_provider: FileProvider,
    #[cfg(feature = "secrets-vault")]
    vault_provider: Option<OpenBaoProvider>,
    #[cfg(feature = "secrets-aws")]
    aws_provider: Option<AwsProvider>,
    #[cfg(any(feature = "secrets-vault", feature = "secrets-aws"))]
    health_log: HealthLog,
    rotation_tx: broadcast::Sender<RotationEvent>,
    /// One lock per cache key, held across a fetch, so a slow read of an old
    /// value never lands after a newer one. An entry lives only while a fetch
    /// holds it or waits on it.
    in_flight: InFlight,
}

type InFlight = parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

/// A held fetch lock. Dropping it releases the lock and removes the map
/// entry when no other caller holds or waits on it.
struct Fetching<'a> {
    in_flight: &'a InFlight,
    key: &'a str,
    lock: Arc<tokio::sync::Mutex<()>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Fetching<'_> {
    async fn acquire<'a>(in_flight: &'a InFlight, key: &'a str) -> Fetching<'a> {
        let lock = Arc::clone(in_flight.lock().entry(key.to_string()).or_default());
        let mut fetching = Fetching {
            in_flight,
            key,
            lock,
            guard: None,
        };
        fetching.guard = Some(Arc::clone(&fetching.lock).lock_owned().await);
        fetching
    }
}

impl Drop for Fetching<'_> {
    fn drop(&mut self) {
        // Under the map's lock, so no caller can clone the entry between the
        // count and the removal. The map and `self.lock` are the two left.
        let mut locks = self.in_flight.lock();
        drop(self.guard.take());
        if Arc::strong_count(&self.lock) == 2
            && locks
                .get(self.key)
                .is_some_and(|l| Arc::ptr_eq(l, &self.lock))
        {
            locks.remove(self.key);
        }
    }
}

impl SecretsManager {
    /// Create a new secrets manager from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if provider initialization fails.
    pub fn new(config: SecretsConfig) -> SecretsResult<Self> {
        let cache = SecretCache::new(&config.cache)?;

        #[cfg(feature = "secrets-vault")]
        let vault_provider = config
            .openbao
            .as_ref()
            .map(OpenBaoProvider::new)
            .transpose()?;

        #[cfg(feature = "secrets-aws")]
        let aws_provider = config.aws.as_ref().map(AwsProvider::new).transpose()?;

        let (rotation_tx, _) = broadcast::channel(16);

        Ok(Self {
            config,
            cache: Arc::new(RwLock::new(cache)),
            file_provider: FileProvider::new(),
            #[cfg(feature = "secrets-vault")]
            vault_provider,
            #[cfg(feature = "secrets-aws")]
            aws_provider,
            #[cfg(any(feature = "secrets-vault", feature = "secrets-aws"))]
            health_log: HealthLog::default(),
            rotation_tx,
            in_flight: parking_lot::Mutex::default(),
        })
    }

    /// Get a secret by name (from configured sources).
    ///
    /// Looks up the named source in configuration and fetches from the appropriate provider.
    /// A read that reaches the provider sends a [`RotationEvent`] when the version changed
    /// since the cache last held the secret, as [`refresh_all`](Self::refresh_all) does.
    ///
    /// # Errors
    ///
    /// Returns an error if the secret cannot be fetched.
    pub async fn get(&self, name: &str) -> SecretsResult<SecretValue> {
        let source = self
            .config
            .sources
            .get(name)
            .ok_or_else(|| SecretsError::NotFound(format!("unknown secret source: {name}")))?
            .clone();

        self.get_from_source(name, &source).await
    }

    /// Get a secret directly from a file path.
    ///
    /// This bypasses the configured sources and reads directly from the filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read.
    pub async fn get_file(&self, path: &str) -> SecretsResult<SecretValue> {
        // Use path as cache key
        let cache_key = format!("file:{path}");

        // Check cache first
        if let Some(cached) = self.cached(&cache_key).await {
            debug!(path = %path, "Secret loaded from cache");
            #[cfg(feature = "metrics")]
            metrics::counter!("secrets_cache_hits_total").increment(1);
            return Ok(cached);
        }

        #[cfg(feature = "metrics")]
        metrics::counter!("secrets_cache_misses_total").increment(1);

        let _fetching = Fetching::acquire(&self.in_flight, &cache_key).await;
        // A concurrent read may have filled the cache while this one waited.
        if let Some(cached) = self.cached(&cache_key).await {
            return Ok(cached);
        }

        let value = self.file_provider.get(path).await?;

        #[cfg(feature = "metrics")]
        metrics::counter!("secrets_fetch_total").increment(1);

        // Update cache
        if let Err(e) = self.store(&cache_key, &value).await {
            warn!(error = %e, "Failed to cache secret");
        }

        Ok(value)
    }

    /// Get a secret from a specific source: a fresh cache entry, else the
    /// provider, else a stale entry within the grace period.
    async fn get_from_source(
        &self,
        cache_key: &str,
        source: &SecretSource,
    ) -> SecretsResult<SecretValue> {
        // Check cache first
        if let Some(cached) = self.cached(cache_key).await {
            debug!(key = %cache_key, "Secret loaded from cache");
            #[cfg(feature = "metrics")]
            metrics::counter!("secrets_cache_hits_total").increment(1);
            return Ok(cached);
        }

        #[cfg(feature = "metrics")]
        metrics::counter!("secrets_cache_misses_total").increment(1);

        let _fetching = Fetching::acquire(&self.in_flight, cache_key).await;
        // A concurrent read or refresh may have filled the cache while this
        // one waited.
        if let Some(cached) = self.cached(cache_key).await {
            return Ok(cached);
        }

        match self.fetch(cache_key, source).await {
            Ok(value) => Ok(value),
            Err(e) => {
                // Try stale cache on provider failure
                if let Some(stale) = self.stale(cache_key).await {
                    warn!(
                        key = %cache_key,
                        error = %e,
                        "Provider failed, using stale cached secret"
                    );
                    return Ok(stale);
                }
                Err(e)
            }
        }
    }

    /// Fetch `source` from its provider, skipping the cache, cache the value
    /// on success, and send a [`RotationEvent`] when its version differs from
    /// the one the cache last held for `cache_key`, expired or not.
    async fn fetch(&self, cache_key: &str, source: &SecretSource) -> SecretsResult<SecretValue> {
        let old_version = self.last_version(cache_key).await;
        let result = match source {
            SecretSource::File { path } => self.file_provider.get(path).await,

            #[cfg(feature = "secrets-vault")]
            SecretSource::OpenBao { path, key } => {
                let provider = self
                    .vault_provider
                    .as_ref()
                    .ok_or_else(|| SecretsError::ProviderNotConfigured("openbao".into()))?;
                provider.get(path, key).await
            }

            #[cfg(feature = "secrets-aws")]
            SecretSource::Aws { secret_id, key } => {
                let provider = self
                    .aws_provider
                    .as_ref()
                    .ok_or_else(|| SecretsError::ProviderNotConfigured("aws".into()))?;
                provider.get(secret_id, key.as_deref()).await
            }

            #[cfg(not(feature = "secrets-vault"))]
            SecretSource::OpenBao { .. } => {
                return Err(SecretsError::ProviderNotConfigured(
                    "openbao (enable secrets-vault feature)".into(),
                ));
            }

            #[cfg(not(feature = "secrets-aws"))]
            SecretSource::Aws { .. } => {
                return Err(SecretsError::ProviderNotConfigured(
                    "aws (enable secrets-aws feature)".into(),
                ));
            }
        };

        #[cfg(feature = "metrics")]
        metrics::counter!("secrets_fetch_total").increment(1);

        let value = result?;
        if let Err(e) = self.store(cache_key, &value).await {
            warn!(key = %cache_key, error = %e, "Failed to cache secret");
        }
        if let (Some(old_version), Some(new_version)) = (old_version, &value.metadata.version)
            && &old_version != new_version
        {
            info!(
                name = %cache_key,
                old_version = %old_version,
                new_version = %new_version,
                "Secret rotated"
            );
            let event = RotationEvent {
                name: cache_key.to_string(),
                old_version: Some(old_version),
                new_version: new_version.clone(),
                rotated_at: std::time::SystemTime::now(),
            };
            // No subscriber is not an error.
            let _ = self.rotation_tx.send(event);
        }
        Ok(value)
    }

    /// A fresh cached value: memory under the lock, then disk on the blocking
    /// pool with no lock held, so a slow cache volume stalls only this lookup.
    async fn cached(&self, key: &str) -> Option<SecretValue> {
        let disk = {
            let cache = self.cache.read();
            if let Some(value) = cache.fresh_in_memory(key) {
                return Some(value);
            }
            cache.disk_tier()
        };
        let found = match disk {
            Some(disk) => {
                let key = key.to_string();
                off_runtime(move || disk.load_fresh(&key)).await.flatten()
            }
            None => None,
        };
        self.cache.read().count_lookup(found.is_some());
        found
    }

    /// A cached value within the grace period, read as [`cached`](Self::cached) reads.
    async fn stale(&self, key: &str) -> Option<SecretValue> {
        let disk = {
            let cache = self.cache.read();
            if let Some(value) = cache.stale_in_memory(key) {
                return Some(value);
            }
            cache.disk_tier()?
        };
        let key = key.to_string();
        let found = off_runtime(move || disk.load_within_grace(&key))
            .await
            .flatten();
        if found.is_some() {
            self.cache.read().count_stale_hit();
        }
        found
    }

    /// The version last cached for `key`, fresh or not: memory, then disk.
    /// TTL and grace period are ignored, and no hit or miss is counted.
    async fn last_version(&self, key: &str) -> Option<String> {
        let disk = {
            let cache = self.cache.read();
            if let Some(value) = cache.in_memory(key) {
                return value.metadata.version.clone();
            }
            cache.disk_tier()?
        };
        let key = key.to_string();
        off_runtime(move || disk.load(&key))
            .await
            .flatten()
            .and_then(|value| value.metadata.version)
    }

    /// Cache `value`: memory under the lock, then disk on the blocking pool.
    async fn store(&self, key: &str, value: &SecretValue) -> SecretsResult<()> {
        let disk = {
            let mut cache = self.cache.write();
            cache.remember(key, value);
            cache.disk_tier()
        };
        if let Some(disk) = disk {
            let key = key.to_string();
            let value = value.clone();
            off_runtime(move || disk.save(&key, &value))
                .await
                .unwrap_or_else(|| {
                    Err(SecretsError::CacheError(
                        "disk cache write task failed".into(),
                    ))
                })?;
        }
        debug!(key = %key, "Secret cached");
        Ok(())
    }

    /// Subscribe to rotation events.
    ///
    /// Returns a receiver that will receive events when secrets are rotated.
    #[must_use]
    pub fn subscribe_rotations(&self) -> broadcast::Receiver<RotationEvent> {
        self.rotation_tx.subscribe()
    }

    /// Re-fetch every configured source from its provider, fresh cache
    /// entries included.
    ///
    /// Each value fetched replaces the cached one, so the next
    /// [`get`](Self::get) returns it. A [`RotationEvent`] is sent when the
    /// version fetched differs from the version the cache last held for that
    /// source, expired or not -- the same check a [`get`](Self::get) that
    /// reaches the provider makes. With caching disabled the cache holds no
    /// previous version, so no event is sent, and a source whose provider
    /// reports no version (a file) never produces one.
    ///
    /// Fetches of one source run one at a time across `get` and
    /// `refresh_all`, so the cache ends on the value read last. A `get` that
    /// waited on a fetch returns what that fetch cached.
    ///
    /// # Errors
    ///
    /// Returns [`SecretsError::RefreshFailed`] naming every source that
    /// failed, after refreshing the rest. A failed source is not answered
    /// from the stale cache, and its cached value is left as it was.
    pub async fn refresh_all(&self) -> SecretsResult<()> {
        let mut errors = Vec::new();

        for (name, source) in &self.config.sources {
            let _fetching = Fetching::acquire(&self.in_flight, name).await;
            if let Err(e) = self.fetch(name, source).await {
                warn!(name = %name, error = %e, "Failed to refresh secret");
                errors.push(format!("{name}: {e}"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(SecretsError::RefreshFailed(errors.join("; ")))
        }
    }

    /// Run [`refresh_all`](Self::refresh_all) on a timer until `shutdown`
    /// fires.
    ///
    /// Each wait is `cache.refresh_interval_secs` plus up to
    /// `cache.refresh_jitter_secs`, drawn afresh for every wait, and the
    /// first refresh comes after one wait, not at start. A failed refresh is
    /// logged per source by `refresh_all`, and the next one runs on
    /// schedule. Returns `None`, and starts nothing, when the interval is
    /// `0`, caching is off (a refresh would hold nothing) or no source is
    /// configured.
    ///
    /// ```no_run
    /// # async fn wire(shutdown: tokio_util::sync::CancellationToken) -> scalo::secrets::SecretsResult<()> {
    /// use std::sync::Arc;
    ///
    /// use scalo::secrets::{SecretsConfig, SecretsManager};
    ///
    /// let mgr = Arc::new(SecretsManager::new(SecretsConfig::from_cascade()?)?);
    /// let refresher = mgr.spawn_refresh(shutdown.clone());
    /// // ... run the service, then on shutdown:
    /// shutdown.cancel();
    /// if let Some(task) = refresher {
    ///     let _ = task.await;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use = "the refresh stops only when `shutdown` fires; keep the handle to await it"]
    pub fn spawn_refresh(self: &Arc<Self>, shutdown: CancellationToken) -> Option<JoinHandle<()>> {
        let cache = &self.config.cache;
        if !cache.enabled || self.config.sources.is_empty() {
            return None;
        }
        let interval = Duration::from_secs(cache.refresh_interval_secs);
        let jitter = Duration::from_secs(cache.refresh_jitter_secs);
        refresh_wait(interval, jitter, 0)?;
        let manager = Arc::clone(self);
        Some(tokio::spawn(async move {
            loop {
                let Some(wait) = refresh_wait(interval, jitter, clock_seed()) else {
                    return;
                };
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    // refresh_all logs every source that failed.
                    _ = manager.refresh_all() => {}
                }
            }
        }))
    }

    /// Check health of all configured providers.
    ///
    /// Returns a map of provider names to their health status. A provider whose
    /// check fails is logged at `warn` with the reason once per outage, and at
    /// `info` once when it recovers. A
    /// `HealthRegistry` check is synchronous, so poll this from a task and let
    /// the check read the last answer:
    ///
    /// ```no_run
    /// # #[cfg(feature = "health")]
    /// # async fn wire(mgr: std::sync::Arc<scalo::secrets::SecretsManager>) {
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicBool, Ordering};
    /// use std::time::Duration;
    ///
    /// use scalo::{HealthRegistry, HealthStatus};
    ///
    /// let backends_up = Arc::new(AtomicBool::new(true));
    /// let poller = (Arc::clone(&mgr), Arc::clone(&backends_up));
    /// tokio::spawn(async move {
    ///     let (mgr, backends_up) = poller;
    ///     let mut tick = tokio::time::interval(Duration::from_secs(30));
    ///     loop {
    ///         tick.tick().await;
    ///         let up = mgr.health_check().await.values().all(|&ok| ok);
    ///         backends_up.store(up, Ordering::Relaxed);
    ///     }
    /// });
    /// HealthRegistry::register("secrets", move || {
    ///     if backends_up.load(Ordering::Relaxed) {
    ///         HealthStatus::Healthy
    ///     } else {
    ///         HealthStatus::Unhealthy
    ///     }
    /// });
    /// # }
    /// ```
    pub async fn health_check(&self) -> HashMap<String, bool> {
        let mut health = HashMap::new();

        // File provider is always healthy
        health.insert("file".into(), true);

        #[cfg(feature = "secrets-vault")]
        if let Some(ref provider) = self.vault_provider {
            let result = provider.health_check().await;
            health.insert("openbao".into(), self.health_log.record("openbao", &result));
        }

        #[cfg(feature = "secrets-aws")]
        if let Some(ref provider) = self.aws_provider {
            let result = provider.health_check().await;
            health.insert("aws".into(), self.health_log.record("aws", &result));
        }

        health
    }

    /// Clear all cached secrets.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics.
    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.read().stats()
    }
}

/// The wait before the next background refresh: `interval` plus `seed`
/// folded into `0..=jitter` milliseconds. `None` for a zero interval.
fn refresh_wait(interval: Duration, jitter: Duration, seed: u64) -> Option<Duration> {
    if interval.is_zero() {
        return None;
    }
    let span = u64::try_from(jitter.as_millis()).unwrap_or(u64::MAX);
    let extra = seed % span.saturating_add(1);
    Some(interval.saturating_add(Duration::from_millis(extra)))
}

/// Sub-second clock nanoseconds: replicas only need different offsets.
fn clock_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()))
}

/// Cache statistics.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of entries in memory cache.
    pub memory_entries: usize,
    /// Number of entries in disk cache.
    pub disk_entries: usize,
    /// Total cache hits.
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
    /// Total stale hits (fallback).
    pub stale_hits: u64,
}

/// Run blocking disk-cache I/O on the blocking pool. `None` if the task
/// panicked.
async fn off_runtime<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(f).await.ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secrets_config_default() {
        let config = SecretsConfig::default();
        assert!(config.cache.enabled);
        assert_eq!(config.cache.ttl_secs, 3600);
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_secrets_manager_new() {
        let manager = SecretsManager::new(SecretsConfig::default());
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_file_provider_missing_file() {
        let manager = SecretsManager::new(SecretsConfig::default()).unwrap();
        let result = manager.get_file("/nonexistent/path/secret.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_file_provider_read_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let secret_path = temp_dir.path().join("test-secret.txt");
        std::fs::write(&secret_path, "my-secret-value").unwrap();

        let manager = SecretsManager::new(SecretsConfig::default()).unwrap();
        let result = manager.get_file(secret_path.to_str().unwrap()).await;

        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value.as_str().unwrap(), "my-secret-value");
    }

    #[tokio::test]
    async fn test_named_source_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let secret_path = temp_dir.path().join("api-key.txt");
        std::fs::write(&secret_path, "super-secret-key").unwrap();

        let config = SecretsConfig {
            sources: [(
                "api_key".into(),
                SecretSource::File {
                    path: secret_path.to_str().unwrap().into(),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let manager = SecretsManager::new(config).unwrap();
        let value = manager.get("api_key").await.unwrap();
        assert_eq!(value.as_str().unwrap(), "super-secret-key");
    }

    #[tokio::test]
    async fn test_unknown_source() {
        let manager = SecretsManager::new(SecretsConfig::default()).unwrap();
        let result = manager.get("nonexistent").await;
        assert!(matches!(result, Err(SecretsError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_health_check() {
        let manager = SecretsManager::new(SecretsConfig::default()).unwrap();
        let health = manager.health_check().await;
        assert!(health.get("file").copied().unwrap_or(false));
    }

    #[test]
    fn refresh_waits_add_jitter_within_its_bound() {
        let secs = Duration::from_secs;
        let ms = Duration::from_millis;
        // (case, interval, jitter, seed, wait)
        let cases = [
            ("interval off", secs(0), secs(300), 7, None),
            ("no jitter", secs(1800), secs(0), 123_456, Some(secs(1800))),
            (
                "seed inside the jitter",
                secs(60),
                secs(5),
                4_999,
                Some(ms(64_999)),
            ),
            (
                "seed at the bound",
                secs(60),
                secs(5),
                5_000,
                Some(secs(65)),
            ),
            (
                "seed past the bound wraps",
                secs(60),
                secs(5),
                5_001,
                Some(secs(60)),
            ),
            ("seed wraps twice", secs(1), secs(1), 2_002, Some(secs(1))),
            ("huge jitter", secs(1), Duration::MAX, 9, Some(ms(1_009))),
        ];
        for (case, interval, jitter, seed, want) in cases {
            assert_eq!(refresh_wait(interval, jitter, seed), want, "{case}");
        }
    }

    /// A disk-cache read that never returns stalls only its own lookup: the
    /// cache lock is not held across it, so another secret is fetched and
    /// cached meanwhile. The stalled read is a FIFO with no writer.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn a_stalled_cache_read_does_not_stall_other_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let mut sources = std::collections::HashMap::new();
        for name in ["a", "b"] {
            let path = dir.path().join(name);
            std::fs::write(&path, name).unwrap();
            sources.insert(
                name.to_string(),
                SecretSource::File {
                    path: path.to_str().unwrap().to_string(),
                },
            );
        }
        let config = SecretsConfig {
            cache: CacheConfig {
                directory: Some(cache_dir.clone()),
                ttl_secs: 3600,
                allow_plaintext_disk_cache: true,
                ..Default::default()
            },
            sources,
            ..Default::default()
        };
        // The disk-cache file for "a" (its key, base64url, `.json`).
        let fifo = cache_dir.join("YQ.json");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("run mkfifo");
        assert!(made.success(), "mkfifo failed");

        let mgr = Arc::new(SecretsManager::new(config).unwrap());
        let stalled = tokio::spawn({
            let mgr = Arc::clone(&mgr);
            async move { mgr.get("a").await }
        });
        // Long enough for "a" to reach its cache read. Too short only lets "b"
        // finish first, which passes the check below whatever the fix.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // On a task, so a lookup that blocks its thread fails the timeout
        // instead of hanging the test.
        let other = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::spawn({
                let mgr = Arc::clone(&mgr);
                async move { mgr.get("b").await }
            }),
        )
        .await;

        // Open the FIFO's write end for the blocked read, unlink it so no later
        // lookup finds it, then close: "a" reads EOF and fetches from source.
        std::thread::spawn(move || {
            let writer = std::fs::OpenOptions::new().write(true).open(&fifo);
            let _ = std::fs::remove_file(&fifo);
            drop(writer);
        });
        let unstalled = tokio::time::timeout(Duration::from_secs(5), stalled).await;

        assert!(
            matches!(&other, Ok(Ok(Ok(value))) if value.as_bytes() == b"b"),
            "a stalled cache read for one secret held up another: {other:?}"
        );
        assert!(
            matches!(&unstalled, Ok(Ok(Ok(value))) if value.as_bytes() == b"a"),
            "the stalled lookup completes once its read returns: {unstalled:?}"
        );
    }

    fn file_sources(
        dir: &std::path::Path,
        cache: CacheConfig,
    ) -> (SecretsConfig, std::path::PathBuf) {
        let path = dir.join("token");
        std::fs::write(&path, "first").unwrap();
        let config = SecretsConfig {
            cache,
            sources: [(
                "token".to_string(),
                SecretSource::File {
                    path: path.to_str().unwrap().to_string(),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        (config, path)
    }

    /// A cache in `dir` that holds entries for an hour, refreshed every
    /// `interval_secs` with no jitter.
    fn refreshing(dir: &std::path::Path, interval_secs: u64) -> CacheConfig {
        CacheConfig {
            directory: Some(dir.join("cache")),
            ttl_secs: 3600,
            refresh_interval_secs: interval_secs,
            refresh_jitter_secs: 0,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn spawn_refresh_starts_nothing_when_it_could_hold_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let off = |enabled: bool, interval: u64| CacheConfig {
            enabled,
            ..refreshing(dir.path(), interval)
        };
        for (case, cache, with_source) in [
            ("interval 0", off(true, 0), true),
            ("caching off", off(false, 1), true),
            ("no source", off(true, 1), false),
        ] {
            let (mut config, _) = file_sources(dir.path(), cache);
            if !with_source {
                config.sources.clear();
            }
            let manager = Arc::new(SecretsManager::new(config).unwrap());
            assert!(
                manager.spawn_refresh(CancellationToken::new()).is_none(),
                "{case}: a refresh task started"
            );
        }
    }

    /// The timer re-reads a fresh entry the cache would otherwise serve for
    /// an hour, and the task ends once shutdown fires.
    #[tokio::test]
    async fn spawn_refresh_rereads_on_its_interval_and_stops_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let (config, path) = file_sources(dir.path(), refreshing(dir.path(), 1));
        let manager = Arc::new(SecretsManager::new(config).unwrap());
        assert_eq!(
            manager.get("token").await.unwrap().as_str().unwrap(),
            "first"
        );

        let shutdown = CancellationToken::new();
        let task = manager
            .spawn_refresh(shutdown.clone())
            .expect("a refresh task");
        std::fs::write(&path, "second").unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let seen = manager.get("token").await.unwrap();
            if seen.as_str().unwrap() == "second" {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no refresh within 30 s of a 1 s interval"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the refresh task outlived shutdown by 10 s")
            .expect("the refresh task panicked");
    }

    /// A slow read of the old value never lands over a newer one: a `get`
    /// whose read of v1 is held open, then a refresh that reads v2, leaves the
    /// cache on v2.
    ///
    /// The source is a FIFO, so the `get`'s read blocks until the test writes
    /// v1. While it blocks, the path is swapped for a file holding v2. Unlocked,
    /// the refresh reads v2 and caches it, then the `get` caches v1 over it.
    /// A fetch lock lives only while a caller holds or waits on it, so reads
    /// of ever-changing paths leave nothing behind, failed reads included.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetch_locks_are_dropped_once_no_caller_needs_them() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(SecretsManager::new(SecretsConfig::default()).unwrap());
        for i in 0..10_000 {
            let path = dir.path().join(format!("missing-{i}"));
            assert!(manager.get_file(path.to_str().unwrap()).await.is_err());
        }
        assert_eq!(manager.in_flight.lock().len(), 0, "sequential reads");

        let path = dir.path().join("missing-shared");
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let manager = Arc::clone(&manager);
            let path = path.to_str().unwrap().to_string();
            readers.spawn(async move { manager.get_file(&path).await.is_err() });
        }
        while let Some(failed) = readers.join_next().await {
            assert!(failed.unwrap());
        }
        assert_eq!(
            manager.in_flight.lock().len(),
            0,
            "concurrent reads of one path"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_slow_read_of_the_old_value_never_overwrites_a_newer_one() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().unwrap();
        let (config, path) = file_sources(dir.path(), refreshing(dir.path(), 0));
        std::fs::remove_file(&path).unwrap();
        let made = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("run mkfifo");
        assert!(made.success(), "mkfifo {}", path.display());
        let writer_path = dir.path().join("token-writer");
        std::fs::hard_link(&path, &writer_path).unwrap();
        let manager = Arc::new(SecretsManager::new(config).unwrap());

        let get = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.get("token").await }
        });
        // Opening a FIFO for writing returns once the get's read has opened it.
        let mut writer = tokio::task::spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(writer_path)
                .expect("open the FIFO")
        })
        .await
        .unwrap();

        let newer = dir.path().join("token-v2");
        std::fs::write(&newer, "v2").unwrap();
        std::fs::rename(&newer, &path).unwrap();
        let mut refresh = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.refresh_all().await }
        });
        // Unlocked, the refresh finishes here on v2. Locked, it waits on the get.
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut refresh).await;

        writer.write_all(b"v1").unwrap();
        drop(writer);
        let read = get.await.unwrap().unwrap();
        assert_eq!(read.as_str().unwrap(), "v1", "the get read what it opened");
        if !refresh.is_finished() {
            refresh.await.unwrap().expect("refresh");
        }

        let cached = manager.get("token").await.unwrap();
        assert_eq!(
            cached.as_str().unwrap(),
            "v2",
            "the get's late v1 replaced the refresh's v2 in the cache"
        );
    }

    #[cfg(all(
        feature = "logger",
        any(feature = "secrets-vault", feature = "secrets-aws")
    ))]
    mod health_log {
        use std::sync::{Arc, Mutex};

        use tracing_subscriber::layer::SubscriberExt;

        use super::*;

        struct Sink(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        fn capture() -> (impl tracing::Subscriber + Send + Sync, Arc<Mutex<Vec<u8>>>) {
            let buf = Arc::new(Mutex::new(Vec::new()));
            let writer = Arc::clone(&buf);
            let subscriber = tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || Sink(Arc::clone(&writer))),
            );
            (subscriber, buf)
        }

        fn logged(buf: &Arc<Mutex<Vec<u8>>>) -> String {
            String::from_utf8_lossy(&buf.lock().unwrap()).into_owned()
        }

        fn refused() -> SecretsResult<()> {
            Err(SecretsError::ProviderError(
                "Vault health check failed: connection refused".into(),
            ))
        }

        #[test]
        fn an_outage_logs_why_once_and_the_recovery_once() {
            let log = HealthLog::default();
            let (subscriber, buf) = capture();
            tracing::subscriber::with_default(subscriber, || {
                for _ in 0..50 {
                    assert!(!log.record("openbao", &refused()));
                }
                assert!(log.record("openbao", &Ok(())));
                assert!(log.record("openbao", &Ok(())));
                assert!(!log.record("openbao", &refused()));
            });

            let out = logged(&buf);
            let count = |needle: &str| out.matches(needle).count();
            assert_eq!(count(" WARN "), 2, "one warning per outage: {out}");
            assert_eq!(count("connection refused"), 2, "{out}");
            assert_eq!(
                count("provider=\"openbao\"") + count("provider=openbao"),
                3,
                "{out}"
            );
            assert_eq!(count(" INFO "), 1, "one line on recovery: {out}");
        }

        #[test]
        fn providers_are_tracked_apart() {
            let log = HealthLog::default();
            let (subscriber, buf) = capture();
            tracing::subscriber::with_default(subscriber, || {
                log.record("openbao", &refused());
                log.record("aws", &refused());
                log.record("openbao", &refused());
            });
            assert_eq!(
                logged(&buf).matches(" WARN ").count(),
                2,
                "{}",
                logged(&buf)
            );
        }

        #[cfg(feature = "secrets-vault")]
        #[tokio::test]
        async fn health_check_warns_once_with_the_reason_while_a_provider_stays_down() {
            let config = SecretsConfig {
                openbao: Some(OpenBaoConfig {
                    // Port 1 refuses the connection at once, so the check fails without a server.
                    address: "http://127.0.0.1:1".into(),
                    auth: OpenBaoAuth::Token {
                        token: "unused".to_string().into(),
                    },
                    namespace: None,
                    ca_cert: None,
                    skip_verify: false,
                    timeout_secs: 60,
                }),
                ..SecretsConfig::default()
            };
            let manager = SecretsManager::new(config).unwrap();
            let (subscriber, buf) = capture();
            let _guard = tracing::subscriber::set_default(subscriber);

            for _ in 0..3 {
                let health = manager.health_check().await;
                assert_eq!(health.get("openbao"), Some(&false));
            }

            let out = logged(&buf);
            assert_eq!(out.matches(" WARN ").count(), 1, "{out}");
            assert!(
                out.contains("health check failed"),
                "the reason is logged: {out}"
            );
            assert!(
                !out.contains("unused"),
                "the token never reaches the log: {out}"
            );
        }
    }
}
