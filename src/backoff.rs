// Project:   scalo
// File:      src/backoff.rs
// Purpose:   Exponential backoff with jitter shared by every retry loop
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Exponential backoff with jitter, shared by every hand-rolled retry loop in
//! the crate: the OTLP export gate, the Kafka poll and commit retries, and the
//! batch engine's recv and sink retries.
//!
//! The wait doubles with each consecutive failure up to a ceiling and is spread
//! by +/-20% so a fleet that lost the same dependency does not retry in
//! lockstep. Callers keep their own failure count and reset it on success.

use std::time::Duration;

/// Jitter applied either side of the computed wait, as a percentage.
const JITTER_PCT: u64 = 20;

/// Consecutive failures after which the wait stops doubling; bounds the shift.
const MAX_DOUBLINGS: u32 = 16;

/// A doubling wait schedule between `base` and `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Backoff {
    base: Duration,
    max: Duration,
}

impl Backoff {
    /// The schedule for a dependency that is expected back within seconds: a
    /// broker, a coordinator or a sink. 100 ms doubling to 2 s.
    #[cfg(any(
        feature = "transport-kafka",
        all(feature = "worker-batch", feature = "transport")
    ))]
    pub(crate) const TRANSIENT: Self =
        Self::new(Duration::from_millis(100), Duration::from_secs(2));

    /// A schedule whose first wait is `base` and whose waits never exceed `max`.
    pub(crate) const fn new(base: Duration, max: Duration) -> Self {
        Self { base, max }
    }

    /// The jittered wait before retrying after the `failures`-th consecutive
    /// failure (1-based), never above the ceiling.
    pub(crate) fn delay(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(MAX_DOUBLINGS);
        let wait = self
            .base
            .saturating_mul(2_u32.saturating_pow(doublings))
            .min(self.max);
        // Clamped again: jitter widens either side, so a wait at the ceiling could pass it.
        jitter(wait).min(self.max)
    }
}

/// Spread a wait by +/-[`JITTER_PCT`].
///
/// Seeded from the clock rather than an RNG: two processes only need different
/// offsets, which does not justify a dependency.
fn jitter(base: Duration) -> Duration {
    let base_millis = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let span = base_millis / 100 * JITTER_PCT;
    if span == 0 {
        return base;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    let offset = nanos % (span * 2);
    Duration::from_millis(base_millis.saturating_add(offset).saturating_sub(span))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_doubles_within_the_jitter_band() {
        let schedule = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
        let first = schedule.delay(1);
        let second = schedule.delay(2);
        assert!(
            first >= Duration::from_millis(800) && first <= Duration::from_millis(1_200),
            "first wait {first:?} outside the band around 1s"
        );
        assert!(
            second >= Duration::from_millis(1_600) && second <= Duration::from_millis(2_400),
            "second wait {second:?} outside the band around 2s"
        );
    }

    #[test]
    fn delay_never_passes_the_ceiling_and_never_overflows() {
        let schedule = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        for failures in [0_u32, 6, 50, 1_000, u32::MAX] {
            let wait = schedule.delay(failures);
            assert!(
                wait <= Duration::from_secs(2),
                "wait {wait:?} after {failures} failures exceeded the ceiling"
            );
        }
        assert!(
            schedule.delay(u32::MAX) >= Duration::from_millis(1_600),
            "a long run of failures sits in the ceiling band"
        );
    }

    #[test]
    fn a_zero_failure_count_waits_the_base() {
        let schedule = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        let wait = schedule.delay(0);
        assert!(wait >= Duration::from_millis(80) && wait <= Duration::from_millis(120));
    }
}
