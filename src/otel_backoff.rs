// Project:   scalo
// File:      src/otel_backoff.rs
// Purpose:   Backoff gate wrapping the OTLP exporters
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Exponential backoff with jitter for the OTLP exporters.
//!
//! Both SDK exporters are called on a fixed schedule and neither retries: a
//! collector that is down is dialled again at the next tick, forever, at the
//! same rate. That is a connect attempt and a log line per tick from every
//! service in the fleet, for as long as the outage lasts.
//!
//! The gate wraps an exporter and suppresses attempts while backing off,
//! doubling the wait after each consecutive failure up to a ceiling and
//! spreading it with jitter so a fleet that lost its collector together does
//! not come back in lockstep. One success resets it.
//!
//! Suppressed exports report success to the SDK. For metrics that costs
//! nothing -- OTLP is cumulative by default, so the next export that lands
//! carries the full value. For spans it drops the batch, which is the same
//! outcome as the bounded queue overflowing, only cheaper.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Ceiling on the wait between attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(900);

/// Jitter applied either side of the computed wait, as a percentage.
const JITTER_PCT: u64 = 20;

/// Consecutive failures after which the wait stops doubling.
///
/// Bounds the shift so the doubling cannot overflow.
const MAX_DOUBLINGS: u32 = 16;

/// Tracks consecutive export failures and how long to stay quiet.
#[derive(Debug)]
pub(crate) struct BackoffGate {
    base: Duration,
    state: Mutex<GateState>,
}

#[derive(Debug)]
struct GateState {
    consecutive_failures: u32,
    blocked_until: Option<Instant>,
    /// Failures suppressed since the last log line, so the recovery message
    /// can say what the outage actually cost.
    suppressed: u64,
}

impl BackoffGate {
    /// A gate whose first wait after a failure is `base`.
    pub(crate) fn new(base: Duration) -> Self {
        Self {
            base: base.max(Duration::from_millis(100)),
            state: Mutex::new(GateState {
                consecutive_failures: 0,
                blocked_until: None,
                suppressed: 0,
            }),
        }
    }

    /// Whether this attempt should be skipped.
    pub(crate) fn should_skip(&self) -> bool {
        let mut state = self.lock();
        match state.blocked_until {
            Some(until) if Instant::now() < until => {
                state.suppressed += 1;
                true
            }
            _ => false,
        }
    }

    /// Clear the backoff after a successful export.
    pub(crate) fn record_success(&self, what: &str) {
        let mut state = self.lock();
        if state.consecutive_failures > 0 {
            tracing::info!(
                target: "scalo::otel",
                failures = state.consecutive_failures,
                suppressed_attempts = state.suppressed,
                "OTLP {what} export recovered"
            );
        }
        state.consecutive_failures = 0;
        state.blocked_until = None;
        state.suppressed = 0;
    }

    /// Record a failed export and start (or extend) the backoff.
    pub(crate) fn record_failure(&self, what: &str, error: &str) {
        let mut state = self.lock();
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        let wait = self.wait_for(state.consecutive_failures);
        state.blocked_until = Some(Instant::now() + wait);

        // Only the first failure of an outage is logged at warn: the rest are
        // the same fact repeated, and a fleet-wide outage would otherwise
        // flood every service's logs for its duration.
        if state.consecutive_failures == 1 {
            tracing::warn!(
                target: "scalo::otel",
                error,
                retry_in_secs = wait.as_secs(),
                "OTLP {what} export failed, backing off"
            );
        } else {
            tracing::debug!(
                target: "scalo::otel",
                error,
                failures = state.consecutive_failures,
                retry_in_secs = wait.as_secs(),
                "OTLP {what} export still failing"
            );
        }
    }

    /// Exponential wait for the given failure count, with jitter.
    fn wait_for(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(MAX_DOUBLINGS);
        let scaled = self
            .base
            .saturating_mul(2_u32.saturating_pow(doublings))
            .min(MAX_BACKOFF);
        // Clamped again after jitter: jitter widens either side, so applying
        // it to a value already at the ceiling would push past it.
        jitter(scaled).min(MAX_BACKOFF)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        // A poisoned lock only means a previous holder panicked while
        // adjusting counters; the numbers are advisory, so carry on with them.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Spread a wait by +/-[`JITTER_PCT`] so a fleet does not retry in lockstep.
///
/// Seeded from the wall clock rather than an RNG: the only requirement is
/// that two processes do not pick the same offset, and no dependency should
/// be added for that.
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
    let millis = base_millis.saturating_add(offset).saturating_sub(span);
    Duration::from_millis(millis)
}

/// Wraps the OTLP metric exporter so failures back off.
#[cfg(feature = "otel-metrics")]
#[derive(Debug)]
pub(crate) struct GatedMetricExporter<E> {
    inner: E,
    gate: BackoffGate,
}

#[cfg(feature = "otel-metrics")]
impl<E> GatedMetricExporter<E> {
    pub(crate) fn new(inner: E, base: Duration) -> Self {
        Self {
            inner,
            gate: BackoffGate::new(base),
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl<E> opentelemetry_sdk::metrics::exporter::PushMetricExporter for GatedMetricExporter<E>
where
    E: opentelemetry_sdk::metrics::exporter::PushMetricExporter,
{
    async fn export(
        &self,
        metrics: &opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        if self.gate.should_skip() {
            return Ok(());
        }
        match self.inner.export(metrics).await {
            Ok(()) => {
                self.gate.record_success("metric");
                Ok(())
            }
            Err(e) => {
                self.gate.record_failure("metric", &e.to_string());
                // Reported as handled: the SDK logs every error it is given,
                // and the gate has already said what happened, once.
                Ok(())
            }
        }
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        self.inner.temporality()
    }
}

/// Wraps the OTLP span exporter so failures back off.
#[cfg(feature = "otel-tracing")]
#[derive(Debug)]
pub(crate) struct GatedSpanExporter<E> {
    inner: E,
    gate: BackoffGate,
}

#[cfg(feature = "otel-tracing")]
impl<E> GatedSpanExporter<E> {
    pub(crate) fn new(inner: E, base: Duration) -> Self {
        Self {
            inner,
            gate: BackoffGate::new(base),
        }
    }
}

#[cfg(feature = "otel-tracing")]
impl<E> opentelemetry_sdk::trace::SpanExporter for GatedSpanExporter<E>
where
    E: opentelemetry_sdk::trace::SpanExporter,
{
    async fn export(
        &self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        if self.gate.should_skip() {
            return Ok(());
        }
        match self.inner.export(batch).await {
            Ok(()) => {
                self.gate.record_success("span");
                Ok(())
            }
            Err(e) => {
                self.gate.record_failure("span", &e.to_string());
                Ok(())
            }
        }
    }

    fn shutdown_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&mut self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_blocks_the_next_attempt() {
        let gate = BackoffGate::new(Duration::from_secs(60));
        assert!(!gate.should_skip(), "a fresh gate must not block");
        gate.record_failure("metric", "connection refused");
        assert!(gate.should_skip(), "the attempt after a failure is skipped");
    }

    #[test]
    fn success_clears_the_backoff() {
        let gate = BackoffGate::new(Duration::from_secs(60));
        gate.record_failure("metric", "connection refused");
        assert!(gate.should_skip());
        gate.record_success("metric");
        assert!(!gate.should_skip(), "a success must reopen the gate");
    }

    #[test]
    fn wait_doubles_and_stops_at_the_ceiling() {
        let gate = BackoffGate::new(Duration::from_secs(1));
        // Jitter is +/-20%, so compare against the band rather than a point.
        let first = gate.wait_for(1);
        let second = gate.wait_for(2);
        assert!(
            first <= Duration::from_millis(1_200) && first >= Duration::from_millis(800),
            "first wait {first:?} outside the jitter band around 1s"
        );
        assert!(
            second <= Duration::from_millis(2_400) && second >= Duration::from_millis(1_600),
            "second wait {second:?} outside the jitter band around 2s"
        );
        let far = gate.wait_for(1_000);
        assert!(
            far <= MAX_BACKOFF,
            "wait {far:?} must never exceed the {MAX_BACKOFF:?} ceiling"
        );
    }

    #[test]
    fn wait_never_overflows_on_a_long_outage() {
        let gate = BackoffGate::new(Duration::from_secs(60));
        for failures in [50_u32, 1_000, u32::MAX] {
            let wait = gate.wait_for(failures);
            assert!(wait <= MAX_BACKOFF, "wait {wait:?} exceeded the ceiling");
        }
    }

    #[test]
    fn suppressed_attempts_are_counted() {
        let gate = BackoffGate::new(Duration::from_secs(60));
        gate.record_failure("span", "connection refused");
        assert!(gate.should_skip());
        assert!(gate.should_skip());
        let state = gate.lock();
        assert_eq!(state.suppressed, 2, "each skipped attempt must be counted");
    }
}
