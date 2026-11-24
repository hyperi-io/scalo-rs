// Project:   scalo
// File:      src/backoff.rs
// Purpose:   Exponential backoff with jitter and Retry-After hints
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Exponential backoff with jitter, and a destination's `Retry-After` hint as
//! the next wait.
//!
//! The wait doubles with each consecutive failure up to a ceiling and is spread
//! by +/-20% so a fleet that lost the same dependency does not retry in
//! lockstep. Callers keep their own failure count and reset it on success.
//!
//! A destination that names its own wait (`SendResult::RetryAfter`, an HTTP
//! `Retry-After` header) gets that wait, capped at the schedule's ceiling: a
//! throttled downstream can legally ask for a day.
//!
//! [`Backoff::delays`] is the same schedule as an iterator, the shape a retry
//! loop such as `backon`'s takes.
//!
//! ```
//! use std::time::Duration;
//! use scalo::backoff::Backoff;
//!
//! let schedule = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
//! let third = schedule.delay(3);
//! assert!(third >= Duration::from_millis(320) && third <= Duration::from_millis(480));
//!
//! // The destination's hint wins, capped at the ceiling.
//! assert_eq!(schedule.next_delay(3, Some(Duration::from_secs(2))), Duration::from_secs(2));
//! assert_eq!(schedule.next_delay(3, Some(Duration::from_secs(60))), Duration::from_secs(5));
//!
//! // Three retries' waits.
//! assert_eq!(schedule.delays().take(3).count(), 3);
//! ```

use std::time::Duration;

/// Jitter applied either side of the computed wait, as a percentage.
const JITTER_PCT: u64 = 20;

/// Consecutive failures after which the wait stops doubling. Bounds the shift.
const MAX_DOUBLINGS: u32 = 16;

/// A doubling wait schedule between `base` and `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    base: Duration,
    max: Duration,
}

impl Backoff {
    /// The schedule for a dependency that is expected back within seconds: a
    /// broker, a coordinator or a sink. 100 ms doubling to 2 s.
    pub const TRANSIENT: Self = Self::new(Duration::from_millis(100), Duration::from_secs(2));

    /// A schedule whose first wait is `base` and whose waits never exceed `max`.
    #[must_use]
    pub const fn new(base: Duration, max: Duration) -> Self {
        Self { base, max }
    }

    /// The first wait.
    #[must_use]
    pub const fn base(&self) -> Duration {
        self.base
    }

    /// The ceiling no wait passes, a destination's hint included.
    #[must_use]
    pub const fn max(&self) -> Duration {
        self.max
    }

    /// The jittered wait before retrying after the `failures`-th consecutive
    /// failure (1-based), never above the ceiling.
    #[must_use]
    pub fn delay(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(MAX_DOUBLINGS);
        let wait = self
            .base
            .saturating_mul(2_u32.saturating_pow(doublings))
            .min(self.max);
        // Clamped again: jitter widens either side, so a wait at the ceiling could pass it.
        jitter(wait).min(self.max)
    }

    /// The wait after the `failures`-th consecutive failure: the destination's
    /// `retry_after` hint when it gave one, else [`delay`](Self::delay). The
    /// hint is taken whole, unjittered, up to the ceiling.
    #[must_use]
    pub fn next_delay(&self, failures: u32, retry_after: Option<Duration>) -> Duration {
        match retry_after {
            Some(hint) => hint.min(self.max),
            None => self.delay(failures),
        }
    }

    /// The waits after the first, second, third and later consecutive
    /// failures, without end. `take(n)` bounds it to `n` retries.
    #[must_use]
    pub const fn delays(self) -> Delays {
        Delays {
            schedule: self,
            failures: 0,
        }
    }
}

/// The endless iterator of a [`Backoff`]'s waits, from [`Backoff::delays`].
#[derive(Debug, Clone)]
pub struct Delays {
    schedule: Backoff,
    failures: u32,
}

impl Iterator for Delays {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        self.failures = self.failures.saturating_add(1);
        Some(self.schedule.delay(self.failures))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (usize::MAX, None)
    }
}

/// The wait for a retry loop that already has a candidate wait, such as one
/// from [`Backoff::delays`]: the destination's `advertised` wait, capped at
/// `max`, when it gave one, else `candidate`.
///
/// `None` for `candidate` means the retry budget is spent, and that stands
/// whatever the destination advertised: a hint paces an attempt the schedule
/// already granted and never grants another.
#[must_use]
pub fn hinted_delay(
    advertised: Option<Duration>,
    candidate: Option<Duration>,
    max: Duration,
) -> Option<Duration> {
    match (advertised, candidate) {
        (Some(delay), Some(_)) => Some(delay.min(max)),
        (_, candidate) => candidate,
    }
}

/// The wait an HTTP `Retry-After` header value asks for, in its delta-seconds
/// form. The HTTP-date form, and anything unparseable, is `None`, which leaves
/// the caller's own schedule in charge.
#[must_use]
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

/// Sub-second nanoseconds of the system clock: enough to give two processes
/// different offsets without a random-number dependency.
pub(crate) fn clock_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()))
}

/// Spread a wait by +/-[`JITTER_PCT`].
fn jitter(base: Duration) -> Duration {
    let base_millis = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let span = base_millis / 100 * JITTER_PCT;
    if span == 0 {
        return base;
    }
    let offset = clock_seed() % (span * 2);
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

    #[test]
    fn delays_walk_the_schedule_without_end_until_taken() {
        let schedule = Backoff::new(Duration::from_millis(100), Duration::from_millis(500));
        let ms = Duration::from_millis;
        // (failures, least, most): 100 ms doubling to the 500 ms ceiling, +/-20%.
        let bands = [
            (1, ms(80), ms(120)),
            (2, ms(160), ms(240)),
            (3, ms(320), ms(480)),
            (4, ms(400), ms(500)),
            (1_000, ms(400), ms(500)),
        ];
        for (failures, least, most) in bands {
            let wait = schedule.delays().nth(failures - 1).unwrap_or_default();
            assert!(
                wait >= least && wait <= most,
                "wait {failures} is {wait:?}, outside {least:?}..={most:?}"
            );
        }
        for retries in [0_usize, 1, 3, 1_000] {
            assert_eq!(schedule.delays().take(retries).count(), retries);
        }
        assert_eq!(schedule.delays().size_hint(), (usize::MAX, None));
    }

    #[test]
    fn a_hint_replaces_the_schedule_up_to_the_ceiling() {
        let schedule = Backoff::new(Duration::from_millis(100), Duration::from_secs(30));
        let secs = Duration::from_secs;
        // (case, hint, wait)
        let cases = [
            ("under the ceiling", secs(5), secs(5)),
            ("at the ceiling", secs(30), secs(30)),
            ("over the ceiling", secs(86_400), secs(30)),
            ("zero", Duration::ZERO, Duration::ZERO),
        ];
        for (case, hint, wait) in cases {
            assert_eq!(schedule.next_delay(7, Some(hint)), wait, "{case}");
        }
        let unhinted = schedule.next_delay(1, None);
        assert!(unhinted >= Duration::from_millis(80) && unhinted <= Duration::from_millis(120));
    }

    #[test]
    fn a_hinted_delay_paces_a_granted_attempt_and_never_grants_one() {
        let secs = Duration::from_secs;
        let max = secs(30);
        // (case, advertised, candidate, wait)
        let cases = [
            (
                "hint under the cap",
                Some(secs(5)),
                Some(secs(1)),
                Some(secs(5)),
            ),
            (
                "hint over the cap",
                Some(secs(86_400)),
                Some(secs(1)),
                Some(max),
            ),
            ("no hint", None, Some(secs(2)), Some(secs(2))),
            ("budget spent, hinted", Some(secs(5)), None, None),
            ("budget spent", None, None, None),
        ];
        for (case, advertised, candidate, wait) in cases {
            assert_eq!(hinted_delay(advertised, candidate, max), wait, "{case}");
        }
    }

    #[test]
    fn retry_after_parses_delta_seconds_only() {
        // (case, header, wait)
        let cases = [
            ("seconds", "120", Some(Duration::from_secs(120))),
            ("padded", " 3 ", Some(Duration::from_secs(3))),
            ("zero", "0", Some(Duration::ZERO)),
            ("http date", "Wed, 21 Oct 2015 07:28:00 GMT", None),
            ("negative", "-5", None),
            ("fraction", "1.5", None),
            ("empty", "", None),
        ];
        for (case, header, wait) in cases {
            assert_eq!(parse_retry_after(header), wait, "{case}");
        }
    }
}
