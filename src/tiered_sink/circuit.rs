// Project:   scalo
// File:      src/tiered_sink/circuit.rs
// Purpose:   Circuit breaker for sink health tracking
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Circuit breaker for sink health tracking.
//!
//! ## One mutable cell, no split state
//!
//! All mutable breaker state (phase, consecutive-failure count, last-failure
//! time, half-open probe permit) lives behind a single [`std::sync::Mutex`].
//! An earlier design split the phase (`RwLock<CircuitState>`) from the failure
//! counter (`AtomicU32`): the count and the phase could be observed/updated
//! out of step, so two racing failures could each drive their own transition
//! (double-transition), and the count could disagree with the phase. Folding
//! everything into one lock makes every transition a single atomic
//! check-and-set. The critical sections are tiny and hold NO `.await`, so the
//! lock never crosses a suspension point (`await_holding_lock` is denied
//! crate-wide).
//!
//! A separate lock-free [`AtomicU8`] mirror is published (under the lock) for
//! the health-check closure, which must read the phase without taking the lock.
//!
//! ## Single half-open probe
//!
//! When the reset timeout elapses the breaker admits exactly ONE probe via
//! [`allow_request`](CircuitBreaker::allow_request): the first caller takes the
//! probe permit (Open -> HalfOpen) and proceeds; concurrent callers are
//! refused until the probe resolves (success -> Closed, failure -> Open). This
//! stops a recovery thundering-herd from hammering a still-fragile downstream.
//! [`state`](CircuitBreaker::state) / [`is_open`](CircuitBreaker::is_open) are
//! side-effect-free observers (they report the *effective* phase for gauges and
//! tests) and never take the probe permit -- only `allow_request` does.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - requests flow through normally.
    Closed,
    /// Circuit is open - requests are rejected, sink is known unhealthy.
    Open,
    /// Circuit is half-open - one probe request allowed to test recovery.
    HalfOpen,
}

impl CircuitState {
    /// Health-mirror code: 0 = Closed, 1 = Open, 2 = HalfOpen.
    fn code(self) -> u8 {
        match self {
            Self::Closed => 0,
            Self::Open => 1,
            Self::HalfOpen => 2,
        }
    }
}

/// All mutable breaker state, guarded as one unit.
#[derive(Debug)]
struct Inner {
    state: CircuitState,
    consecutive_failures: u32,
    last_failure_ms: u64,
    /// HalfOpen only: `true` while the single recovery-probe permit is still
    /// available to be claimed. Cleared the instant a probe is admitted, and
    /// whenever we (re)enter Open or Closed.
    probe_available: bool,
}

/// Circuit breaker for protecting against unhealthy sinks.
///
/// Tracks consecutive failures and opens when a threshold is reached. After
/// the reset timeout it admits a single probe to test recovery.
pub struct CircuitBreaker {
    inner: Mutex<Inner>,
    failure_threshold: u32,
    reset_timeout: Duration,
    /// Lock-free mirror of `inner.state` for the sync health-check closure.
    health_state: Arc<AtomicU8>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    ///
    /// - `failure_threshold`: Number of consecutive failures before opening
    /// - `reset_timeout`: Time to wait before admitting a recovery probe
    #[must_use]
    pub fn new(failure_threshold: u32, reset_timeout: Duration) -> Self {
        let health_state = Arc::new(AtomicU8::new(CircuitState::Closed.code()));

        #[cfg(feature = "health")]
        {
            let hs = Arc::clone(&health_state);
            crate::health::HealthRegistry::register("circuit_breaker", move || {
                match hs.load(Ordering::Acquire) {
                    0 => crate::health::HealthStatus::Healthy,   // Closed
                    2 => crate::health::HealthStatus::Degraded,  // HalfOpen
                    _ => crate::health::HealthStatus::Unhealthy, // Open
                }
            });
        }

        Self {
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                last_failure_ms: 0,
                probe_available: false,
            }),
            failure_threshold,
            reset_timeout,
            health_state,
        }
    }

    /// Lock the inner cell, tolerating poison (breaker state is just counters;
    /// a panic elsewhere must not wedge the data plane).
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Set the phase and publish it to the lock-free health mirror. Must be
    /// called while holding the lock so the mirror never races ahead of the
    /// authoritative phase.
    fn set_state(&self, inner: &mut Inner, new: CircuitState) {
        inner.state = new;
        self.health_state.store(new.code(), Ordering::Release);
    }

    /// Whether the Open reset timeout has elapsed relative to `last_failure_ms`.
    fn reset_elapsed(&self, inner: &Inner) -> bool {
        let now = current_epoch_millis();
        Duration::from_millis(now.saturating_sub(inner.last_failure_ms)) >= self.reset_timeout
    }

    /// Gate a request through the breaker, claiming the half-open probe permit
    /// when appropriate. This is the ONLY method that transitions Open ->
    /// HalfOpen, and it admits exactly one probe.
    ///
    /// Returns `true` if the caller may proceed to the sink.
    pub async fn allow_request(&self) -> bool {
        let mut inner = self.lock();
        match inner.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if self.reset_elapsed(&inner) {
                    // First caller past the timeout claims the sole probe.
                    self.set_state(&mut inner, CircuitState::HalfOpen);
                    inner.probe_available = false;
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                if inner.probe_available {
                    inner.probe_available = false;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Get the current *effective* circuit state (side-effect-free).
    ///
    /// Reports `HalfOpen` once an Open breaker's reset timeout has elapsed, for
    /// gauges and tests, but does NOT perform the transition or take the probe
    /// permit -- only [`allow_request`](Self::allow_request) does.
    pub async fn state(&self) -> CircuitState {
        let inner = self.lock();
        if inner.state == CircuitState::Open && self.reset_elapsed(&inner) {
            CircuitState::HalfOpen
        } else {
            inner.state
        }
    }

    /// Check if requests should be allowed through (effective state is Closed).
    pub async fn is_closed(&self) -> bool {
        self.state().await == CircuitState::Closed
    }

    /// Check if circuit is open (effective state is Open -- timeout not elapsed).
    pub async fn is_open(&self) -> bool {
        self.state().await == CircuitState::Open
    }

    /// Record a successful request: clears failures and closes the circuit.
    pub async fn record_success(&self) {
        let mut inner = self.lock();
        inner.consecutive_failures = 0;
        inner.probe_available = false;
        self.set_state(&mut inner, CircuitState::Closed);
    }

    /// Record a failed request. Opens the circuit once consecutive failures
    /// reach the threshold; a failure while half-open re-opens immediately
    /// (the count is not reset until a success, so it is still >= threshold).
    pub async fn record_failure(&self) {
        let mut inner = self.lock();
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        inner.last_failure_ms = current_epoch_millis();

        if inner.consecutive_failures >= self.failure_threshold {
            inner.probe_available = false;
            self.set_state(&mut inner, CircuitState::Open);
        }
    }

    /// Get the number of consecutive failures.
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.lock().consecutive_failures
    }

    /// Reset the circuit breaker to closed state.
    pub async fn reset(&self) {
        let mut inner = self.lock();
        inner.consecutive_failures = 0;
        inner.last_failure_ms = 0;
        inner.probe_available = false;
        self.set_state(&mut inner, CircuitState::Closed);
    }
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("failure_threshold", &self.failure_threshold)
            .field("reset_timeout", &self.reset_timeout)
            .field("consecutive_failures", &self.consecutive_failures())
            .finish_non_exhaustive()
    }
}

fn current_epoch_millis() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_initial_state_is_closed() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.is_closed().await);
    }

    #[tokio::test]
    async fn test_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure().await;
        assert!(cb.is_closed().await);

        cb.record_failure().await;
        assert!(cb.is_closed().await);

        cb.record_failure().await;
        assert!(cb.is_open().await);
        assert_eq!(cb.consecutive_failures(), 3);
    }

    #[tokio::test]
    async fn test_success_resets_failures() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.consecutive_failures(), 2);

        cb.record_success().await;
        assert_eq!(cb.consecutive_failures(), 0);
        assert!(cb.is_closed().await);
    }

    #[tokio::test]
    async fn test_half_open_after_timeout() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(50));

        cb.record_failure().await;
        assert!(cb.is_open().await);

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cb.state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_half_open_success_closes() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(10));

        cb.record_failure().await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        cb.record_success().await;
        assert!(cb.is_closed().await);
    }

    #[tokio::test]
    async fn test_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(10));

        cb.record_failure().await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        cb.record_failure().await;
        assert!(cb.is_open().await);
    }

    #[tokio::test]
    async fn test_reset() {
        let cb = CircuitBreaker::new(1, Duration::from_secs(30));

        cb.record_failure().await;
        assert!(cb.is_open().await);

        cb.reset().await;
        assert!(cb.is_closed().await);
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[tokio::test]
    async fn allow_request_admits_exactly_one_half_open_probe() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(10));

        // Closed: always admitted.
        assert!(cb.allow_request().await);

        // Trip it open; while open the timeout has not elapsed -> refused.
        cb.record_failure().await;
        assert!(!cb.allow_request().await);

        // After the timeout, exactly ONE probe is admitted; the next is refused
        // until the probe resolves.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cb.allow_request().await, "first caller takes the probe");
        assert!(
            !cb.allow_request().await,
            "second concurrent caller must be refused -- one probe only"
        );

        // Probe succeeds -> closed -> admits again.
        cb.record_success().await;
        assert!(cb.allow_request().await);
    }

    // Concurrency race test: hammer record_failure + allow_request from many
    // tasks at once. The unified lock must keep the failure count and phase
    // consistent (no double-transition / torn state) and never admit more than
    // one probe per half-open window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_failures_and_probes_stay_consistent() {
        let cb = Arc::new(CircuitBreaker::new(5, Duration::from_millis(10)));

        // 50 tasks each record a failure concurrently.
        let mut handles = Vec::new();
        for _ in 0..50 {
            let cb = Arc::clone(&cb);
            handles.push(tokio::spawn(async move {
                cb.record_failure().await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Exactly 50 failures counted (no lost/torn increments), circuit open.
        assert_eq!(cb.consecutive_failures(), 50);
        assert!(cb.is_open().await);

        // After the timeout, race many allow_request calls: AT MOST one may win
        // the probe permit.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut probes = Vec::new();
        for _ in 0..50 {
            let cb = Arc::clone(&cb);
            probes.push(tokio::spawn(async move { cb.allow_request().await }));
        }
        let mut admitted = 0;
        for p in probes {
            if p.await.unwrap() {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 1, "exactly one probe admitted, got {admitted}");
    }
}
