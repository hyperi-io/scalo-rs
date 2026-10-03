// Project:   scalo
// File:      src/sink_stack/adaptive.rs
// Purpose:   Adaptive request concurrency (AIMD) limiter + tower layer
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Adaptive request concurrency (ARC) for the sink stack.
//!
//! A loss-based AIMD concurrency limiter: the in-flight limit grows additively
//! while the downstream is healthy and the limit is well used, and shrinks
//! multiplicatively on an overload signal (a sink backpressure or a per-attempt
//! timeout). It discovers the downstream's safe concurrency instead of needing a
//! hand-tuned static cap.
//!
//! ## Why hand-rolled
//!
//! It is built directly on a tokio [`Semaphore`], which already gives correct,
//! cancellation-safe, NON-spinning FIFO queueing of waiters (acquire parks on
//! the semaphore; a dropped/timed-out acquire removes itself). The only thing a
//! plain semaphore lacks is a correct DYNAMIC limit, so this adds:
//!
//! - **grow**: [`Semaphore::add_permits`].
//! - **shrink without over-admit**: shrink the available permits immediately via
//!   [`Semaphore::forget_permits`]; any remainder that is still in-flight is
//!   recorded as a `debt` and absorbed as those permits are returned -- a
//!   returned permit under debt is `forget`-ten (its capacity destroyed) rather
//!   than handed to the next waiter. This "drain, don't revoke" rule means the
//!   total never temporarily exceeds the new, lower limit.
//!
//! `min_limit` floors at 1 so a failing downstream can never drive the limit to
//! zero (which would deadlock the sink).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Outcome of a guarded operation, as seen by the AIMD controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Succeeded (or failed in a way unrelated to overload) -- may grow the limit.
    Success,
    /// Failed due to overload (backpressure / timeout) -- shrinks the limit.
    Overload,
}

/// Loss-based AIMD adaptive concurrency limiter. Cheap to clone via `Arc`.
#[derive(Debug)]
pub struct AdaptiveLimiter {
    sem: Arc<Semaphore>,
    /// Current concurrency limit (read lock-free; written only under `resize`).
    limit: AtomicUsize,
    /// Permits whose capacity must be destroyed on return to complete a shrink
    /// the in-flight permits had not yet allowed (drain-don't-revoke).
    debt: AtomicUsize,
    /// Operations currently holding a permit.
    in_flight: AtomicUsize,
    /// Serialises limit transitions so concurrent outcomes can't double-apply.
    resize: Mutex<()>,
    min_limit: usize,
    max_limit: usize,
    increase_by: usize,
    decrease_factor: f64,
}

/// Minimum utilisation (in-flight / limit) before a success grows the limit, so
/// an idle limiter does not ratchet the limit up without real demand.
const GROW_UTILISATION_PERCENT: usize = 80;

impl AdaptiveLimiter {
    /// Build a limiter. `min_limit` is floored at 1; `initial`/`max` are clamped
    /// into a sane range; `decrease_factor` into `[0.5, 1.0)`.
    #[must_use]
    pub fn new(
        initial: usize,
        min_limit: usize,
        max_limit: usize,
        increase_by: usize,
        decrease_factor: f64,
    ) -> Arc<Self> {
        let min = min_limit.max(1);
        let max = max_limit.max(min);
        let initial = initial.clamp(min, max);
        let factor = decrease_factor.clamp(0.5, 0.999);
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(initial)),
            limit: AtomicUsize::new(initial),
            debt: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            resize: Mutex::new(()),
            min_limit: min,
            max_limit: max,
            increase_by: increase_by.max(1),
            decrease_factor: factor,
        })
    }

    /// Current concurrency limit.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Acquire)
    }

    /// Operations currently in flight.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Acquire a concurrency slot, parking (without spinning) until one frees.
    pub async fn acquire(self: &Arc<Self>) -> Permit {
        // `acquire_owned` parks on the semaphore queue; cancellation (drop) is
        // handled by tokio. The semaphore is never closed, so this cannot error.
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .expect("adaptive limiter semaphore is never closed");
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        Permit {
            inner: Some(permit),
            limiter: Arc::clone(self),
        }
    }

    /// Acquire a slot, giving up after `timeout`. `None` means the wait elapsed
    /// (the caller sheds as transient backpressure -- never a drop).
    pub async fn acquire_timeout(self: &Arc<Self>, timeout: Duration) -> Option<Permit> {
        tokio::time::timeout(timeout, self.acquire()).await.ok()
    }

    /// Try to take a slot without waiting.
    #[must_use]
    pub fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let permit = Arc::clone(&self.sem).try_acquire_owned().ok()?;
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        Some(Permit {
            inner: Some(permit),
            limiter: Arc::clone(self),
        })
    }

    /// Feed an outcome to the AIMD controller, adjusting the limit.
    pub fn record(&self, outcome: Outcome) {
        let _guard = self
            .resize
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.limit.load(Ordering::Acquire);
        let new = match outcome {
            Outcome::Overload => {
                // Multiplicative decrease, floored at min.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let shrunk = (current as f64 * self.decrease_factor) as usize;
                shrunk.max(self.min_limit)
            }
            Outcome::Success => {
                // Additive increase, but only when the limit is well used.
                let in_flight = self.in_flight.load(Ordering::Acquire);
                if in_flight * 100 >= current * GROW_UTILISATION_PERCENT {
                    (current + self.increase_by).min(self.max_limit)
                } else {
                    current
                }
            }
        };
        if new != current {
            self.apply_limit(current, new);
        }
    }

    /// Apply a new limit (caller holds `resize`). Grows via add_permits (cancel
    /// any pending debt first); shrinks by forgetting available permits now and
    /// deferring the in-flight remainder to `debt`.
    fn apply_limit(&self, current: usize, new: usize) {
        if new > current {
            let mut grow = new - current;
            // Cancel outstanding debt before adding fresh permits.
            let debt = self.debt.load(Ordering::Acquire);
            let cancel = grow.min(debt);
            if cancel > 0 {
                self.debt.fetch_sub(cancel, Ordering::AcqRel);
                grow -= cancel;
            }
            if grow > 0 {
                self.sem.add_permits(grow);
            }
        } else {
            let shrink = current - new;
            // Remove what's available immediately; defer the rest to debt, to be
            // absorbed as in-flight permits are returned (no over-admit).
            let forgot = self.sem.forget_permits(shrink);
            if shrink > forgot {
                self.debt.fetch_add(shrink - forgot, Ordering::AcqRel);
            }
        }
        self.limit.store(new, Ordering::Release);
    }
}

/// A held concurrency slot. Returns its capacity to the limiter on drop, unless
/// a pending shrink (`debt`) requires the capacity be destroyed instead.
#[derive(Debug)]
pub struct Permit {
    inner: Option<OwnedSemaphorePermit>,
    limiter: Arc<AdaptiveLimiter>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.limiter.in_flight.fetch_sub(1, Ordering::AcqRel);
        let permit = self.inner.take().expect("permit held until drop");
        // Absorb a pending shrink: under debt, destroy this permit's capacity
        // (forget) instead of returning it to the pool. Otherwise return it
        // normally, which wakes the next waiter.
        let took_debt = self.debt_take().is_some();
        if took_debt {
            permit.forget();
        } else {
            drop(permit);
        }
    }
}

impl Permit {
    /// Atomically claim one unit of debt if any remains.
    fn debt_take(&self) -> Option<()> {
        self.limiter
            .debt
            .try_update(Ordering::AcqRel, Ordering::Acquire, |d| {
                if d > 0 { Some(d - 1) } else { None }
            })
            .ok()
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gates_to_the_limit() {
        let lim = AdaptiveLimiter::new(2, 1, 10, 1, 0.5);
        let _p1 = lim.try_acquire().expect("1st");
        let _p2 = lim.try_acquire().expect("2nd");
        assert!(lim.try_acquire().is_none(), "3rd blocked at limit 2");
        assert_eq!(lim.in_flight(), 2);
    }

    #[tokio::test]
    async fn min_limit_floors_at_one_no_deadlock() {
        let lim = AdaptiveLimiter::new(8, 1, 8, 1, 0.5);
        // Hammer overload: the limit must never reach 0 (that would deadlock).
        for _ in 0..50 {
            lim.record(Outcome::Overload);
            assert!(lim.limit() >= 1, "limit floored at 1");
        }
        assert_eq!(lim.limit(), 1);
        // Still acquirable.
        assert!(lim.try_acquire().is_some());
    }

    #[tokio::test]
    async fn grows_only_when_well_utilised() {
        let lim = AdaptiveLimiter::new(4, 1, 100, 1, 0.5);
        // Idle success: no growth (utilisation 0).
        lim.record(Outcome::Success);
        assert_eq!(lim.limit(), 4, "idle success does not grow the limit");

        // Saturate (4 in-flight at limit 4) then a success grows it.
        let permits: Vec<_> = (0..4).map(|_| lim.try_acquire().unwrap()).collect();
        lim.record(Outcome::Success);
        assert_eq!(lim.limit(), 5, "well-utilised success grows the limit");
        drop(permits);
    }

    #[tokio::test]
    async fn shrink_never_over_admits() {
        // The crux: shrink while permits are in-flight must NOT let total exceed
        // the new limit once they return (drain-don't-revoke via debt).
        let lim = AdaptiveLimiter::new(10, 1, 10, 1, 0.5);
        // Hold all 10.
        let permits: Vec<_> = (0..10).map(|_| lim.try_acquire().unwrap()).collect();
        assert!(lim.try_acquire().is_none());

        // Shrink to 5 while all 10 are in-flight: available=0, so 5 become debt.
        lim.record(Outcome::Overload); // 10 * 0.5 = 5
        assert_eq!(lim.limit(), 5);

        // Return all 10. Five of them must be absorbed by the debt (forgotten),
        // leaving exactly 5 acquirable -- never 10.
        drop(permits);
        let mut reacquired = Vec::new();
        while let Some(p) = lim.try_acquire() {
            reacquired.push(p);
        }
        assert_eq!(
            reacquired.len(),
            5,
            "total capacity must equal the new limit, not over-admit"
        );
    }

    #[tokio::test]
    async fn grow_wakes_a_waiter_without_spin() {
        let lim = AdaptiveLimiter::new(1, 1, 10, 1, 0.5);
        let held = lim.try_acquire().unwrap();

        // A waiter parks on the semaphore (no spin).
        let lim2 = Arc::clone(&lim);
        let waiter = tokio::spawn(async move { lim2.acquire().await });
        tokio::task::yield_now().await;

        // Growing the limit adds a permit -> wakes the waiter.
        lim.apply_limit(1, 2);
        let _woken = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must wake promptly, not spin")
            .unwrap();
        drop(held);
    }

    #[tokio::test]
    async fn acquire_timeout_sheds_when_saturated() {
        let lim = AdaptiveLimiter::new(1, 1, 10, 1, 0.5);
        let _held = lim.try_acquire().unwrap();
        let shed = lim.acquire_timeout(Duration::from_millis(20)).await;
        assert!(shed.is_none(), "saturated acquire sheds after the timeout");
    }
}
