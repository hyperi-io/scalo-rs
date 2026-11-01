// Project:   scalo
// File:      src/health/registry.rs
// Purpose:   Global health registry singleton for component health tracking
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Global health registry for unified service health state.
//!
//! Modules register health check callbacks at construction. The registry
//! aggregates component status to determine overall service health.
//!
//! # Design
//!
//! - Global singleton via `OnceLock` (consistent with config registry pattern)
//! - Components register a closure that returns their current [`HealthStatus`]
//! - [`is_healthy`](HealthRegistry::is_healthy) requires ALL components healthy
//! - [`is_ready`](HealthRegistry::is_ready) requires NO components unhealthy
//!   (degraded is acceptable for readiness)
//! - Empty registry is considered healthy (vacuously true)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Health status of a registered component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Component is fully operational.
    Healthy,
    /// Component is operational but impaired (e.g., circuit half-open,
    /// elevated latency, fallback active).
    Degraded,
    /// Component is not operational. Service should not receive traffic.
    Unhealthy,
}

impl HealthStatus {
    /// String representation for JSON serialisation and endpoint output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }
}

/// Health check callback -- returns current component status.
type HealthCheck = Arc<dyn Fn() -> HealthStatus + Send + Sync>;

/// A registered health check entry.
struct HealthEntry {
    id: u64,
    name: String,
    check: HealthCheck,
}

/// Source of [`HealthEntry::id`], unique for the process.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// A component's place in the registry, removed when this is dropped.
///
/// Returned by [`HealthRegistry::register_scoped`] for a component that can be
/// retired before the process ends, a transport say, so a closed and dropped
/// one stops counting against readiness.
#[must_use = "dropping the registration unregisters the component at once"]
#[derive(Debug)]
pub struct HealthRegistration {
    id: u64,
}

impl HealthRegistration {
    /// The registry entry this handle owns.
    #[cfg(test)]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for HealthRegistration {
    fn drop(&mut self) {
        if let Ok(mut components) = HealthRegistry::global().components.lock() {
            components.retain(|c| c.id != self.id);
        }
    }
}

/// Global health registry singleton.
///
/// Modules register health check callbacks at construction. The registry
/// aggregates all component statuses to determine overall service health.
///
/// # Thread Safety
///
/// The registry uses `Mutex<Vec<_>>` for registration (infrequent, at
/// init time) and read access (health checks). For the typical service
/// with 3-8 registered components, lock contention is negligible.
pub struct HealthRegistry {
    components: Mutex<Vec<HealthEntry>>,
}

/// Global singleton instance.
static REGISTRY: OnceLock<HealthRegistry> = OnceLock::new();

impl HealthRegistry {
    /// Create a new empty registry.
    fn new() -> Self {
        Self {
            components: Mutex::new(Vec::new()),
        }
    }

    /// Get or initialise the global registry.
    fn global() -> &'static Self {
        REGISTRY.get_or_init(Self::new)
    }

    /// Register a health check callback.
    ///
    /// Called by modules at construction time. The callback is invoked
    /// each time health is queried, so it should be cheap (e.g., read
    /// an `AtomicBool` or check a cached state).
    ///
    /// # Duplicate Names
    ///
    /// Multiple components may register with the same name. Each
    /// registration is independent -- the registry does not deduplicate.
    pub fn register(
        name: impl Into<String>,
        check: impl Fn() -> HealthStatus + Send + Sync + 'static,
    ) {
        Self::insert(name.into(), Arc::new(check));
    }

    /// Register a health check that lasts as long as the returned handle.
    ///
    /// As [`register`](Self::register), but dropping the
    /// [`HealthRegistration`] removes the check. A component that can be
    /// closed and replaced holds the handle, so it leaves the registry with
    /// the component rather than reporting unhealthy for the rest of the
    /// process.
    pub fn register_scoped(
        name: impl Into<String>,
        check: impl Fn() -> HealthStatus + Send + Sync + 'static,
    ) -> HealthRegistration {
        HealthRegistration {
            id: Self::insert(name.into(), Arc::new(check)),
        }
    }

    /// Add `check` under `name` and return its entry id.
    fn insert(name: String, check: HealthCheck) -> u64 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut components) = Self::global().components.lock() {
            components.push(HealthEntry { id, name, check });
        }
        id
    }

    /// Whether the entry `id` is still registered.
    #[cfg(test)]
    pub(crate) fn is_registered(id: u64) -> bool {
        Self::global()
            .components
            .lock()
            .is_ok_and(|components| components.iter().any(|c| c.id == id))
    }

    /// Snapshot every registered check, drop the registry lock, then run
    /// the checks. A check callback that re-enters
    /// [`HealthRegistry::register`] (or any other path that takes the
    /// registry mutex) would otherwise deadlock against itself; this
    /// keeps the lock critical-section to a clone of the `Arc<dyn Fn>`
    /// handles.
    fn snapshot_checks() -> Vec<HealthCheck> {
        let registry = Self::global();
        registry
            .components
            .lock()
            .ok()
            .map(|components| components.iter().map(|c| Arc::clone(&c.check)).collect())
            .unwrap_or_default()
    }

    /// Check if ALL components are healthy.
    ///
    /// Returns `true` if the registry is empty (vacuously true) or
    /// every registered component reports [`HealthStatus::Healthy`].
    #[must_use]
    pub fn is_healthy() -> bool {
        Self::snapshot_checks()
            .iter()
            .all(|check| check() == HealthStatus::Healthy)
    }

    /// Check if the service is ready to receive traffic.
    ///
    /// Ready means no component is [`HealthStatus::Unhealthy`]. Degraded
    /// components are acceptable -- the service can still serve requests,
    /// just with reduced capability.
    ///
    /// Returns `true` if the registry is empty (vacuously true).
    #[must_use]
    pub fn is_ready() -> bool {
        Self::snapshot_checks()
            .iter()
            .all(|check| check() != HealthStatus::Unhealthy)
    }

    /// Get per-component health status.
    ///
    /// Returns a snapshot of all registered components and their current
    /// status. Useful for detailed health endpoints.
    #[must_use]
    pub fn components() -> Vec<(String, HealthStatus)> {
        // Snapshot (name, check) pairs under the lock; run checks after
        // releasing it. See `snapshot_checks` for the reentrancy rationale.
        let snapshot: Vec<(String, HealthCheck)> = {
            let registry = Self::global();
            let Ok(components) = registry.components.lock() else {
                return Vec::new();
            };
            components
                .iter()
                .map(|c| (c.name.clone(), Arc::clone(&c.check)))
                .collect()
        };
        snapshot
            .into_iter()
            .map(|(name, check)| (name, check()))
            .collect()
    }

    /// Get a JSON representation of the health state.
    ///
    /// Suitable for a `/health/detailed` endpoint response.
    #[cfg(feature = "serde_json")]
    #[must_use]
    pub fn to_json() -> serde_json::Value {
        let components = Self::components();
        let overall = if Self::is_healthy() {
            "healthy"
        } else if Self::is_ready() {
            "degraded"
        } else {
            "unhealthy"
        };

        serde_json::json!({
            "status": overall,
            "components": components.iter().map(|(name, status)| {
                serde_json::json!({
                    "name": name,
                    "status": status.as_str(),
                })
            }).collect::<Vec<_>>()
        })
    }

    /// Clear all registered components (for testing only).
    #[cfg(test)]
    pub(crate) fn reset() {
        let registry = Self::global();
        if let Ok(mut components) = registry.components.lock() {
            components.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};

    use super::*;

    /// Tests share global statics -- serialise them.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Acquire the shared test lock and reset global registry state.
    /// Returned guard holds the lock for the caller's test body.
    fn serial_test_guard() -> std::sync::MutexGuard<'static, ()> {
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        HealthRegistry::reset();
        guard
    }

    #[test]
    fn empty_registry_is_healthy() {
        let _guard = serial_test_guard();

        assert!(HealthRegistry::is_healthy());
        assert!(HealthRegistry::is_ready());
        assert_eq!(
            HealthRegistry::components(),
            [] as [(std::string::String, crate::health::registry::HealthStatus); 0]
        );
    }

    #[test]
    fn register_and_check_healthy() {
        let _guard = serial_test_guard();

        HealthRegistry::register("transport", || HealthStatus::Healthy);
        HealthRegistry::register("database", || HealthStatus::Healthy);

        assert!(HealthRegistry::is_healthy());
        assert!(HealthRegistry::is_ready());

        let components = HealthRegistry::components();
        assert_eq!(components.len(), 2);
        assert_eq!(components[0].0, "transport");
        assert_eq!(components[0].1, HealthStatus::Healthy);
        assert_eq!(components[1].0, "database");
        assert_eq!(components[1].1, HealthStatus::Healthy);
    }

    #[test]
    fn unhealthy_component_fails_check() {
        let _guard = serial_test_guard();

        HealthRegistry::register("transport", || HealthStatus::Healthy);
        HealthRegistry::register("database", || HealthStatus::Unhealthy);

        assert!(!HealthRegistry::is_healthy());
        assert!(!HealthRegistry::is_ready());
    }

    #[test]
    fn degraded_is_ready_but_not_healthy() {
        let _guard = serial_test_guard();

        HealthRegistry::register("transport", || HealthStatus::Healthy);
        HealthRegistry::register("circuit_breaker", || HealthStatus::Degraded);

        assert!(!HealthRegistry::is_healthy());
        assert!(HealthRegistry::is_ready());
    }

    #[test]
    fn dynamic_health_check_reflects_state_changes() {
        let _guard = serial_test_guard();

        // Simulate a component whose health changes at runtime
        let state = Arc::new(AtomicU8::new(0)); // 0=healthy, 1=degraded, 2=unhealthy
        let state_clone = state.clone();

        HealthRegistry::register("dynamic", move || {
            match state_clone.load(Ordering::Relaxed) {
                0 => HealthStatus::Healthy,
                1 => HealthStatus::Degraded,
                _ => HealthStatus::Unhealthy,
            }
        });

        // Initially healthy
        assert!(HealthRegistry::is_healthy());
        assert!(HealthRegistry::is_ready());

        // Transition to degraded
        state.store(1, Ordering::Relaxed);
        assert!(!HealthRegistry::is_healthy());
        assert!(HealthRegistry::is_ready());

        // Transition to unhealthy
        state.store(2, Ordering::Relaxed);
        assert!(!HealthRegistry::is_healthy());
        assert!(!HealthRegistry::is_ready());

        // Recovery back to healthy
        state.store(0, Ordering::Relaxed);
        assert!(HealthRegistry::is_healthy());
        assert!(HealthRegistry::is_ready());
    }

    /// A scoped registration counts while its handle lives and is gone once
    /// the handle drops, leaving the components registered beside it.
    #[test]
    fn a_scoped_registration_ends_with_its_handle() {
        let _guard = serial_test_guard();

        HealthRegistry::register("permanent", || HealthStatus::Healthy);
        let scoped = HealthRegistry::register_scoped("retired", || HealthStatus::Unhealthy);
        let id = scoped.id();
        assert!(HealthRegistry::is_registered(id));
        assert!(
            !HealthRegistry::is_ready(),
            "the unhealthy component counts"
        );

        drop(scoped);
        assert!(!HealthRegistry::is_registered(id));
        assert!(
            HealthRegistry::is_ready(),
            "the dropped component no longer counts"
        );
        let names: Vec<String> = HealthRegistry::components()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, ["permanent"]);
    }

    #[test]
    fn health_status_as_str() {
        assert_eq!(HealthStatus::Healthy.as_str(), "healthy");
        assert_eq!(HealthStatus::Degraded.as_str(), "degraded");
        assert_eq!(HealthStatus::Unhealthy.as_str(), "unhealthy");
    }

    #[test]
    #[cfg(feature = "serde_json")]
    fn to_json_includes_all_components() {
        let _guard = serial_test_guard();

        HealthRegistry::register("kafka", || HealthStatus::Healthy);
        HealthRegistry::register("clickhouse", || HealthStatus::Degraded);

        let json = HealthRegistry::to_json();

        assert_eq!(json["status"], "degraded");

        let components = json["components"].as_array().unwrap();
        assert_eq!(components.len(), 2);

        assert_eq!(components[0]["name"], "kafka");
        assert_eq!(components[0]["status"], "healthy");

        assert_eq!(components[1]["name"], "clickhouse");
        assert_eq!(components[1]["status"], "degraded");
    }

    #[test]
    #[cfg(feature = "serde_json")]
    fn to_json_empty_registry() {
        let _guard = serial_test_guard();

        let json = HealthRegistry::to_json();
        assert_eq!(json["status"], "healthy");
        assert_eq!(
            json["components"].as_array().unwrap().as_slice(),
            [] as [serde_json::Value; 0]
        );
    }

    #[test]
    #[cfg(feature = "serde_json")]
    fn to_json_unhealthy_status() {
        let _guard = serial_test_guard();

        HealthRegistry::register("broken", || HealthStatus::Unhealthy);

        let json = HealthRegistry::to_json();
        assert_eq!(json["status"], "unhealthy");
    }
}
