// Project:   scalo
// File:      src/governor/rate_limit.rs
// Purpose:   Token-bucket rate limiter -- a hard, contractual outbound cap
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Token-bucket / GCRA rate limiter.
//!
//! A HARD, contractual rate cap for outbound work -- the kind a downstream
//! states explicitly ("this API allows 100 req/s"). Distinct from the adaptive
//! sink concurrency controller (planned): that one *discovers* the downstream's
//! capacity from RTT/errors; this one *enforces* a fixed ceiling the operator
//! already knows.
//!
//! ## Built on the `governor` crate (no reinvented wheel)
//!
//! Backed by the battle-proven `governor` crate (GCRA -- generic cell rate
//! algorithm; 64-bit lock-free state, CAS-updated). We import it as `gcra` to
//! avoid a name clash with scalo's own `governor` module. This type is a thin
//! facade so callers depend on a stable scalo API, not the crate directly, and
//! so a disabled limiter (`rps == 0`) is a true zero-cost no-op.
//!
//! ## Thundering-herd safety
//!
//! [`acquire`](RateLimiter::acquire) uses governor's `until_ready_with_jitter`,
//! NOT a bare sleep-the-exact-deficit: when N tasks unblock at the same refill
//! instant a fixed wait would stampede the downstream. Jitter (up to one cell
//! period) spreads the wakeups. (Safety practice cribbed from governor's own
//! guidance + the v2.10 reference review.)
//!
//! **Delivery guarantee:** the limiter only DELAYS work. It never drops,
//! reorders past a commit, or acks before the sink confirms -- at-least-once is
//! preserved (purely an admission delay in front of `send`).

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

// The `governor` crate, renamed to `gcra` in Cargo.toml to avoid clashing with
// this very module's name.
use gcra::{DefaultDirectRateLimiter, Jitter, Quota};

/// Configuration for a [`RateLimiter`].
///
/// `rps == 0` (the default) means **disabled** -- the limiter is a no-op (the
/// derived `Default` is `rps = 0, burst = 0`). A non-zero `rps` enforces that
/// steady rate; `burst` is the bucket depth (the largest momentary spike
/// permitted), defaulting to one second of `rps`.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Steady-state requests per second. `0` disables the limiter.
    #[serde(default)]
    pub rps: u32,

    /// Bucket depth (max momentary burst). `0` -> default to `rps` (one
    /// second's worth), so a config that sets only `rps` still bursts sanely.
    #[serde(default)]
    pub burst: u32,
}

impl RateLimitConfig {
    /// Build an enabled config at `rps` with a default burst of `rps`.
    #[must_use]
    pub fn per_second(rps: u32) -> Self {
        Self { rps, burst: rps }
    }

    /// Set an explicit burst depth.
    #[must_use]
    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst = burst;
        self
    }

    /// Whether this config enables throttling (`rps > 0`).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.rps > 0
    }

    /// Effective burst capacity (tokens): `burst`, or `rps` when `burst` is 0,
    /// floored at 1 so an enabled limiter can always hold at least one token.
    #[must_use]
    fn capacity(self) -> u32 {
        let cap = if self.burst == 0 { self.rps } else { self.burst };
        cap.max(1)
    }
}

/// A token-bucket rate limiter. Cheap to [`clone`](Clone) (shares one limiter).
#[derive(Clone)]
pub struct RateLimiter {
    inner: Option<Arc<Inner>>,
    /// A short label for metrics (e.g. the route or sink name). Bounded
    /// cardinality -- never per-message.
    label: &'static str,
}

struct Inner {
    limiter: DefaultDirectRateLimiter,
    /// Jitter cap for `until_ready_with_jitter` (~one cell period).
    jitter: Jitter,
}

impl RateLimiter {
    /// Build a limiter from config with a static `label` for metrics.
    ///
    /// A disabled config (`rps == 0`) yields a transparent no-op limiter.
    #[must_use]
    pub fn new(config: RateLimitConfig, label: &'static str) -> Self {
        if !config.is_enabled() {
            return Self { inner: None, label };
        }
        // rps > 0 here, so these NonZero conversions cannot fail.
        let rps = NonZeroU32::new(config.rps).unwrap_or(NonZeroU32::MIN);
        let burst = NonZeroU32::new(config.capacity()).unwrap_or(NonZeroU32::MIN);
        let quota = Quota::per_second(rps).allow_burst(burst);
        let limiter = DefaultDirectRateLimiter::direct(quota);
        // Jitter up to one cell period (1/rps): spreads simultaneous wakeups so
        // a refill instant doesn't release a thundering herd at once.
        let period = Duration::from_secs_f64(1.0 / f64::from(config.rps));
        let jitter = Jitter::up_to(period);
        Self {
            inner: Some(Arc::new(Inner { limiter, jitter })),
            label,
        }
    }

    /// Whether this limiter actually throttles (`rps > 0`).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Try to take one token without waiting. Returns `true` if admitted.
    ///
    /// A disabled limiter always admits.
    #[must_use]
    pub fn try_acquire(&self) -> bool {
        match &self.inner {
            None => true,
            Some(inner) => inner.limiter.check().is_ok(),
        }
    }

    /// Acquire one token, awaiting (without spinning) until one is available,
    /// with jitter to avoid a thundering herd.
    ///
    /// A disabled limiter returns immediately. Returns the time spent waiting
    /// (near-zero when a token was already available).
    pub async fn acquire(&self) -> Duration {
        let Some(inner) = &self.inner else {
            return Duration::ZERO;
        };
        let started = Instant::now();
        inner.limiter.until_ready_with_jitter(inner.jitter).await;
        let waited = started.elapsed();
        self.record(waited);
        waited
    }

    /// Emit throttle metrics (no-op without the `metrics` feature).
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn record(&self, waited: Duration) {
        #[cfg(feature = "metrics")]
        if waited > Duration::from_millis(1) {
            ::metrics::counter!("rate_limited_total", "limiter" => self.label).increment(1);
            ::metrics::histogram!("rate_limit_wait_seconds", "limiter" => self.label)
                .record(waited.as_secs_f64());
        }
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("label", &self.label)
            .field("enabled", &self.is_enabled())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_limiter_is_transparent() {
        let rl = RateLimiter::new(RateLimitConfig::default(), "test");
        assert!(!rl.is_enabled());
        // Always admits, no bucket accounting.
        for _ in 0..1000 {
            assert!(rl.try_acquire());
        }
    }

    #[test]
    fn try_acquire_drains_then_refuses_within_burst() {
        // burst=3 -> three immediate tokens, fourth refused (no time passed).
        let rl = RateLimiter::new(RateLimitConfig::per_second(1).with_burst(3), "test");
        assert!(rl.try_acquire());
        assert!(rl.try_acquire());
        assert!(rl.try_acquire());
        assert!(
            !rl.try_acquire(),
            "bucket exhausted -- fourth try must fail without waiting"
        );
    }

    #[test]
    fn config_capacity_defaults_burst_to_rps() {
        let c = RateLimitConfig::per_second(50);
        assert_eq!(c.burst, 50);
        assert!(c.is_enabled());
        // burst=0 with rps=10 still yields a capacity of 10, not 0.
        let c2 = RateLimitConfig { rps: 10, burst: 0 };
        assert_eq!(c2.capacity(), 10);
    }

    // Real-time timing test (NOT the tokio paused clock): governor's limiter
    // uses its own monotonic clock the virtual clock would not drive. Asserts
    // only a LOWER bound -- until_ready never returns before the GCRA delay, and
    // jitter only ADDS, so a slow runner can only make it pass by more.
    #[tokio::test]
    async fn acquire_waits_when_empty_then_admits_after_refill() {
        // rps=100 -> one token / 10ms, burst=1.
        let rl = RateLimiter::new(RateLimitConfig::per_second(100).with_burst(1), "test");
        let first = rl.acquire().await;
        assert!(
            first < Duration::from_millis(20),
            "cold bucket admits ~immediately (jitter aside), got {first:?}"
        );

        // Drain any remaining initial token, then the next acquire must wait for
        // a refill (at least the ~10ms GCRA delay; jitter may add more).
        let _ = rl.try_acquire();
        let waited = rl.acquire().await;
        assert!(
            waited >= Duration::from_millis(8),
            "empty bucket must wait ~10ms+ for a refill, waited {waited:?}"
        );
    }

    #[tokio::test]
    async fn enabled_limiter_paces_to_rate() {
        // burst=1, rps=100 -> after draining, acquires pace at >= ~10ms each.
        let rl = RateLimiter::new(RateLimitConfig::per_second(100).with_burst(1), "test");
        let _ = rl.acquire().await; // drain initial token
        let start = std::time::Instant::now();
        for _ in 0..5 {
            rl.acquire().await;
        }
        // 5 tokens at 100rps ~= 50ms minimum (jitter only adds); floor at 40ms.
        assert!(
            start.elapsed() >= Duration::from_millis(40),
            "5 acquires at 100rps must take ~50ms+, took {:?}",
            start.elapsed()
        );
    }
}
