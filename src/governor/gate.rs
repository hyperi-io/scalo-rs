// Project:   scalo
// File:      src/governor/gate.rs
// Purpose:   Inbound gate: edge-detecting pause/resume over the pressure latch
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Inbound gate: drives an actuator on each pause/resume transition.
//!
//! The [`UnifiedPressure`] latch tells us *whether* to hold; the
//! [`InboundGate`] turns that latched boolean into EDGE events. Each
//! [`evaluate`](InboundGate::evaluate) samples the latch and, on a
//! transition only, calls the [`GateActuator`] exactly once -- `pause()`
//! on the false->true (rising) edge, `resume()` on the true->false
//! (falling) edge. While the latch stays held, repeated `evaluate()`
//! calls return [`Admit::Hold`] but do NOT re-call `pause()`; likewise a
//! released latch returns [`Admit::Yes`] without re-calling `resume()`.
//!
//! INBOUND side only: the actuator pauses a source's recv/ingest (stops
//! pulling new work) so the in-flight buffer drains under pressure. NEVER
//! wired to the outbound drain (sink) -- gating the drain would deadlock the
//! pipeline. `send` is never involved here.
//!
//! Gated behind the `governor` feature; wired into the receive transport by
//! [`SelfRegulationGovernor`](super::SelfRegulationGovernor) (default-on).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::source::UnifiedPressure;

/// Longest a gate stays open for an expired hold when no receive returns a record.
const RELEASE_WINDOW: Duration = Duration::from_secs(2);

/// Drives the inbound source on pause/resume edges.
///
/// Implementors translate a gate edge into a concrete action on the
/// ingest side -- e.g. stop polling a Kafka consumer, stop accepting on an
/// HTTP listener. The gate guarantees each method fires EXACTLY ONCE per
/// transition, so an implementation is free to be non-idempotent (toggle
/// a flag, pause/resume a stream) without double-pausing.
pub trait GateActuator: Send + Sync {
    /// Pause the inbound source. Called once on the rising edge.
    fn pause(&self);
    /// Resume the inbound source. Called once on the falling edge.
    fn resume(&self);
}

/// An observability decorator over a [`GateActuator`].
///
/// Wrap the real actuator (Kafka pause/resume, a [`NoopActuator`], etc.) so
/// each pause/resume EDGE emits a metric + brake-reason log, then forwards to
/// the inner actuator. Because the [`InboundGate`] fires each edge EXACTLY
/// ONCE, the `inbound_paused` gauge and `self_regulation_inbound_pauses_total`
/// counter track real transitions, not per-evaluate noise -- a `paused`/
/// `resumed` log pair and a gauge dashboards can graph.
pub struct ObservingActuator {
    inner: Box<dyn GateActuator>,
    /// Stable source label for the log line (e.g. `"kafka"`, `"http"`).
    source: &'static str,
}

impl ObservingActuator {
    /// Wrap `inner` so pause/resume edges emit metrics + logs under `source`.
    #[must_use]
    pub fn new(source: &'static str, inner: Box<dyn GateActuator>) -> Self {
        Self { inner, source }
    }
}

impl GateActuator for ObservingActuator {
    fn pause(&self) {
        // `self_regulation_` domain prefix + a `source` label: a bare
        // `inbound_paused` collides with nothing today but reads ambiguously
        // next to `MemoryGuard`/`ScalingPressure` gauges, and the label lets two
        // governed receivers (e.g. Kafka + HTTP) be told apart -- without it the
        // single global series shows "unpaused" while one source is still held.
        #[cfg(feature = "metrics")]
        {
            ::metrics::gauge!("self_regulation_inbound_paused", "source" => self.source).set(1.0);
            ::metrics::counter!("self_regulation_inbound_pauses_total", "source" => self.source)
                .increment(1);
        }
        tracing::warn!(
            source = self.source,
            "self-regulation: inbound PAUSED under pressure (memory/back-pressure brake)"
        );
        self.inner.pause();
    }

    fn resume(&self) {
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("self_regulation_inbound_paused", "source" => self.source).set(0.0);
        // A hold that reached max_hold resumes too, with the pressure not cleared.
        tracing::info!(source = self.source, "self-regulation: inbound RESUMED");
        self.inner.resume();
    }
}

/// A no-op actuator for tests and send-only pipelines.
///
/// Useful when a stage wants the gate's [`Admit`] decision (to stop
/// pulling work in its own loop) but has nothing external to pause --
/// the gate's held/released state is still observable via
/// [`InboundGate::is_held`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopActuator;

impl GateActuator for NoopActuator {
    fn pause(&self) {}
    fn resume(&self) {}
}

/// The gate's admission decision for the next unit of inbound work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// Admit the next unit -- the gate is open.
    Yes,
    /// Hold (do not admit) -- the gate is closed under pressure.
    Hold,
}

/// Edge-detecting inbound gate over a [`UnifiedPressure`] latch.
///
/// Wraps the recv/ingest side of a stage. Each
/// [`evaluate`](Self::evaluate) consults the latch and drives the
/// [`GateActuator`] once per transition. See the
/// [module docs](crate::governor) for the full contract.
pub struct InboundGate {
    pressure: Arc<UnifiedPressure>,
    actuator: Box<dyn GateActuator>,
    /// Last edge state we drove the actuator to. Tracked separately from
    /// the pressure latch so the actuator fires EXACTLY ONCE per
    /// transition even though `should_hold()` returns `true` repeatedly
    /// while latched.
    paused_edge: AtomicBool,
    /// The latch's expired-hold count this gate has already opened for.
    expired_seen: AtomicU64,
    /// `0` while no release window is open; while one is, the latch clock
    /// reading it opened at, plus one.
    release_window: AtomicU64,
}

impl InboundGate {
    /// Build a gate over a shared pressure latch and an actuator.
    ///
    /// The gate starts in the released (open) state; the first
    /// [`evaluate`](Self::evaluate) under pressure will fire `pause()`.
    #[must_use]
    pub fn new(pressure: Arc<UnifiedPressure>, actuator: Box<dyn GateActuator>) -> Self {
        let expired_seen = AtomicU64::new(pressure.expired_holds());
        Self {
            pressure,
            actuator,
            paused_edge: AtomicBool::new(false),
            expired_seen,
            release_window: AtomicU64::new(0),
        }
    }

    /// Sample the latch and drive the actuator on a transition.
    ///
    /// Computes [`should_hold`](UnifiedPressure::should_hold) from the
    /// pressure, then uses a `compare_exchange` on the edge flag so:
    ///
    /// - false->true (rising edge): `compare_exchange(false, true)`
    ///   succeeds exactly once -> call `pause()`.
    /// - true->false (falling edge): `compare_exchange(true, false)`
    ///   succeeds exactly once -> call `resume()`.
    /// - no change: `compare_exchange` fails -> no actuator call.
    ///
    /// A hold that reached the latch's
    /// [`max_hold`](UnifiedPressure::with_max_hold) opens a release window on
    /// every gate on the latch, whichever caller the latch itself admitted.
    /// The window resumes the source once and returns [`Admit::Yes`] until
    /// [`note_received`](Self::note_received) reports a receive that returned
    /// a record, or 2 s pass. A resumed source has to fetch before it returns
    /// anything, so the first receive after the resume is often empty. Once
    /// the window closes, the next evaluation pauses again if the latch
    /// re-armed.
    ///
    /// Returns [`Admit::Hold`] when held, [`Admit::Yes`] otherwise. Never
    /// touches the outbound side.
    ///
    /// # Single-evaluator contract
    ///
    /// `evaluate()` MUST be called from a SINGLE task (the one recv loop that
    /// owns this source). The edge `compare_exchange` and the actuator call are
    /// not one atomic step: two tasks racing across a pressure transition could
    /// interleave so the actuator (an async pause/resume on the consumer) runs
    /// out of order, stranding the source paused while the flag reads open. The
    /// in-tree wiring satisfies this -- each transport's recv loop calls
    /// `evaluate()` once per `recv`. Do NOT share one `InboundGate` across
    /// concurrent evaluators; give each source its own gate.
    pub fn evaluate(&self) -> Admit {
        let hold = self.pressure.should_hold();
        let expired = self.pressure.expired_holds();
        if self.expired_seen.swap(expired, Ordering::AcqRel) != expired {
            let opened = self.pressure.now_nanos().saturating_add(1);
            self.release_window.store(opened, Ordering::Release);
        } else if !hold {
            // The level released the latch, so a later hold must not inherit the window.
            self.release_window.store(0, Ordering::Release);
        }
        if hold && !self.release_window_open() {
            // Rising edge: flip false -> true exactly once.
            if self
                .paused_edge
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.actuator.pause();
            }
            Admit::Hold
        } else {
            // Falling edge: flip true -> false exactly once.
            if self
                .paused_edge
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.actuator.resume();
            }
            Admit::Yes
        }
    }

    /// Report how many records the receive after the last
    /// [`evaluate`](Self::evaluate) returned.
    ///
    /// A receive that returned a record closes the release window an expired
    /// hold opened, so the next evaluation pauses again if the latch still
    /// holds. Call it from the task that calls `evaluate()`, once per receive;
    /// a caller that never reports keeps each window open for 2 s.
    pub fn note_received(&self, records: usize) {
        if records > 0 {
            self.release_window.store(0, Ordering::Release);
        }
    }

    /// Whether a release window is open, closing one that has run its 2 s.
    fn release_window_open(&self) -> bool {
        let opened = self.release_window.load(Ordering::Acquire);
        if opened == 0 {
            return false;
        }
        let open_for = self.pressure.now_nanos().saturating_sub(opened - 1);
        if Duration::from_nanos(open_for) < RELEASE_WINDOW {
            return true;
        }
        self.release_window.store(0, Ordering::Release);
        false
    }

    /// Whether the gate last drove the actuator to the held state.
    ///
    /// Reflects the edge flag, not a fresh pressure sample -- it is the
    /// state the actuator has been driven to by the most recent
    /// [`evaluate`](Self::evaluate).
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.paused_edge.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governor::source::{Hysteresis, Pressure, PressureSource};
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    /// Scriptable pressure source: stores the
    /// reading as a bit-pattern `u64` so it stays `Sync` without `unsafe`
    /// or a lock.
    struct MockSource {
        value: AtomicU64,
        hard: bool,
    }

    impl MockSource {
        fn new(value: f64, hard: bool) -> Self {
            Self {
                value: AtomicU64::new(value.to_bits()),
                hard,
            }
        }
        fn set(&self, value: f64) {
            self.value.store(value.to_bits(), Ordering::Relaxed);
        }
    }

    impl PressureSource for MockSource {
        fn name(&self) -> &'static str {
            "mock"
        }
        fn sample(&self) -> Pressure {
            Pressure::new(f64::from_bits(self.value.load(Ordering::Relaxed)))
        }
        fn is_hard(&self) -> bool {
            self.hard
        }
    }

    /// Counting actuator: records exactly how many times each edge fired.
    struct CountingActuator {
        pause_calls: AtomicUsize,
        resume_calls: AtomicUsize,
    }

    impl CountingActuator {
        fn new() -> Self {
            Self {
                pause_calls: AtomicUsize::new(0),
                resume_calls: AtomicUsize::new(0),
            }
        }
        fn pauses(&self) -> usize {
            self.pause_calls.load(Ordering::Relaxed)
        }
        fn resumes(&self) -> usize {
            self.resume_calls.load(Ordering::Relaxed)
        }
    }

    impl GateActuator for CountingActuator {
        fn pause(&self) {
            self.pause_calls.fetch_add(1, Ordering::Relaxed);
        }
        fn resume(&self) {
            self.resume_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A `GateActuator` that forwards to a shared `Arc<CountingActuator>`
    /// so the test can both hand the gate an actuator AND inspect the
    /// counts afterwards (the gate takes a `Box`, consuming ownership).
    struct SharedActuator(Arc<CountingActuator>);

    impl GateActuator for SharedActuator {
        fn pause(&self) {
            self.0.pause();
        }
        fn resume(&self) {
            self.0.resume();
        }
    }

    fn governor_with(source: Arc<MockSource>) -> Arc<UnifiedPressure> {
        let hyst = Hysteresis::new(0.80, 0.65).expect("valid band");
        Arc::new(UnifiedPressure::new(
            vec![source as Arc<dyn PressureSource>],
            hyst,
        ))
    }

    /// THE adversarial proving test for the gate.
    ///
    /// Drives one gate through low->high->high->low->low->high and proves:
    ///   1. `pause()` fires EXACTLY ONCE per rising edge (not once per
    ///      `evaluate()` while latched);
    ///   2. `resume()` fires EXACTLY ONCE per falling edge;
    ///   3. `evaluate()` returns `Hold` while latched, `Yes` otherwise;
    ///   4. the latch re-arms cleanly (the second rising edge fires
    ///      `pause()` again -- no sticky state).
    #[test]
    fn gate_drives_actuator_exactly_once_per_edge() {
        let mem = Arc::new(MockSource::new(0.10, true));
        let pressure = governor_with(Arc::clone(&mem));
        let counter = Arc::new(CountingActuator::new());
        let gate = InboundGate::new(
            Arc::clone(&pressure),
            Box::new(SharedActuator(Arc::clone(&counter))),
        );

        // LOW: open, no actuator calls yet.
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert!(!gate.is_held());
        assert_eq!(counter.pauses(), 0);
        assert_eq!(counter.resumes(), 0);

        // RISING edge: 0.10 -> 0.90 (>= pause_above) -> pause() ONCE.
        mem.set(0.90);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert!(gate.is_held());
        assert_eq!(counter.pauses(), 1, "pause once on rising edge");
        assert_eq!(counter.resumes(), 0);

        // STILL HIGH: latched. Many evaluate()s, still Hold, NO extra
        // pause() -- this is the edge-dedup invariant.
        for _ in 0..5 {
            assert_eq!(gate.evaluate(), Admit::Hold);
        }
        assert_eq!(counter.pauses(), 1, "no re-pause while latched");
        assert_eq!(counter.resumes(), 0);

        // Inside the band (0.70 > resume_below 0.65): latch HOLDS, still
        // no extra calls.
        mem.set(0.70);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(counter.pauses(), 1, "band holds, no re-pause");
        assert_eq!(counter.resumes(), 0);

        // FALLING edge: 0.70 -> 0.50 (<= resume_below) -> resume() ONCE.
        mem.set(0.50);
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert!(!gate.is_held());
        assert_eq!(counter.pauses(), 1);
        assert_eq!(counter.resumes(), 1, "resume once on falling edge");

        // STILL LOW: open. Many evaluate()s, still Yes, NO extra resume().
        for _ in 0..5 {
            assert_eq!(gate.evaluate(), Admit::Yes);
        }
        assert_eq!(counter.pauses(), 1);
        assert_eq!(counter.resumes(), 1, "no re-resume while released");

        // SECOND RISING edge: re-arms cleanly -> pause() AGAIN (count 2).
        mem.set(0.95);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert!(gate.is_held());
        assert_eq!(counter.pauses(), 2, "latch re-arms, pause fires again");
        assert_eq!(counter.resumes(), 1);
    }

    const SECOND: u64 = 1_000_000_000;

    /// A latch with the default 30 s bound over one HARD source at `level`,
    /// timed on a clock the test moves.
    fn timed_governor(level: f64) -> (Arc<MockSource>, Arc<AtomicU64>, Arc<UnifiedPressure>) {
        let mem = Arc::new(MockSource::new(level, true));
        let now = Arc::new(AtomicU64::new(0));
        let pressure = UnifiedPressure::new(
            vec![Arc::clone(&mem) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("valid band"),
        )
        .with_manual_clock(Arc::clone(&now));
        (mem, now, Arc::new(pressure))
    }

    fn counting_gate(pressure: &Arc<UnifiedPressure>) -> (InboundGate, Arc<CountingActuator>) {
        let counter = Arc::new(CountingActuator::new());
        let gate = InboundGate::new(
            Arc::clone(pressure),
            Box::new(SharedActuator(Arc::clone(&counter))),
        );
        (gate, counter)
    }

    /// A paused source whose level never clears resumes once at max_hold and
    /// pauses again once a receive returns records; a level that settled in
    /// the band stays resumed.
    #[test]
    fn a_hold_past_max_hold_resumes_the_source_once() {
        let (mem, now, pressure) = timed_governor(0.85);
        let (gate, counter) = counting_gate(&pressure);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(counter.pauses(), 1);

        now.store(30 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes, "one window at max_hold");
        assert_eq!(counter.resumes(), 1);
        gate.note_received(10);
        assert_eq!(gate.evaluate(), Admit::Hold, "0.85 re-arms");
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(counter.pauses(), 2, "one pause per re-arm");
        assert_eq!(counter.resumes(), 1);

        mem.set(0.70);
        now.store(60 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert_eq!(gate.evaluate(), Admit::Yes, "0.70 does not re-arm");
        assert_eq!(counter.resumes(), 2);
        assert_eq!(counter.pauses(), 2);
    }

    /// Two sources on one latch: the latch admits one caller when a hold
    /// expires, and each gate still resumes once for it.
    #[test]
    fn every_gate_on_the_latch_opens_once_per_expired_hold() {
        let (_mem, now, pressure) = timed_governor(0.85);
        let (first, first_count) = counting_gate(&pressure);
        let (second, second_count) = counting_gate(&pressure);
        assert_eq!(first.evaluate(), Admit::Hold);
        assert_eq!(second.evaluate(), Admit::Hold);

        now.store(30 * SECOND, Ordering::Relaxed);
        assert_eq!(first.evaluate(), Admit::Yes, "takes the latch's window");
        assert_eq!(second.evaluate(), Admit::Yes, "re-armed latch, same expiry");
        assert_eq!(
            first.evaluate(),
            Admit::Yes,
            "open until a receive returns records"
        );
        assert_eq!(first_count.resumes(), 1);
        assert_eq!(second_count.resumes(), 1);

        first.note_received(1);
        second.note_received(1);
        assert_eq!(first.evaluate(), Admit::Hold);
        assert_eq!(second.evaluate(), Admit::Hold);
        assert_eq!(first_count.pauses(), 2);
        assert_eq!(second_count.pauses(), 2);
        assert_eq!(first_count.resumes(), 1, "one resume per expired hold");
        assert_eq!(second_count.resumes(), 1, "one resume per expired hold");
    }

    /// A pull source behind the gate whose first poll after a resume returns
    /// nothing, as a consumer that has to fetch again does.
    struct RefetchingSource {
        paused: AtomicBool,
        refetching: AtomicBool,
        batch: usize,
    }

    impl RefetchingSource {
        fn poll(&self) -> usize {
            if self.paused.load(Ordering::Relaxed) || self.refetching.swap(false, Ordering::Relaxed)
            {
                return 0;
            }
            self.batch
        }
    }

    struct SourceActuator(Arc<RefetchingSource>);

    impl GateActuator for SourceActuator {
        fn pause(&self) {
            self.0.paused.store(true, Ordering::Relaxed);
        }
        fn resume(&self) {
            self.0.paused.store(false, Ordering::Relaxed);
            self.0.refetching.store(true, Ordering::Relaxed);
        }
    }

    /// A gate over a source that refetches after a resume, and one receive in
    /// the order a transport's recv runs it: evaluate, poll, report.
    fn refetching_gate(pressure: &Arc<UnifiedPressure>) -> impl Fn() -> usize {
        let source = Arc::new(RefetchingSource {
            paused: AtomicBool::new(false),
            refetching: AtomicBool::new(false),
            batch: 500,
        });
        let gate = InboundGate::new(
            Arc::clone(pressure),
            Box::new(SourceActuator(Arc::clone(&source))),
        );
        move || {
            let _ = gate.evaluate();
            let records = source.poll();
            gate.note_received(records);
            records
        }
    }

    /// An expired hold admits the batch after the resume even though the first
    /// poll after it returns nothing. With a window of one evaluation the
    /// second receive paused the source again and the hold admitted nothing.
    #[test]
    fn an_expired_hold_admits_a_batch_through_an_empty_first_poll() {
        let (_mem, now, pressure) = timed_governor(0.85);
        let recv = refetching_gate(&pressure);
        assert_eq!(recv(), 0, "paused");

        now.store(30 * SECOND, Ordering::Relaxed);
        let admitted: usize = (0..5).map(|_| recv()).sum();
        assert_eq!(
            admitted, 500,
            "one batch through the window, then paused again"
        );

        now.store(59 * SECOND, Ordering::Relaxed);
        assert_eq!(recv(), 0, "held until the next max_hold");
        now.store(60 * SECOND, Ordering::Relaxed);
        let admitted: usize = (0..5).map(|_| recv()).sum();
        assert_eq!(admitted, 500, "one batch per expired hold");
    }

    /// A window no receive fills closes after 2 s, and the source pauses again.
    #[test]
    fn a_release_window_closes_after_2_s_without_records() {
        let (_mem, now, pressure) = timed_governor(0.85);
        let (gate, counter) = counting_gate(&pressure);
        assert_eq!(gate.evaluate(), Admit::Hold);

        now.store(30 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes);
        gate.note_received(0);
        now.store(31 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes, "1 s into the window");
        now.store(32 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Hold, "2 s bound");
        assert_eq!(counter.pauses(), 2);
        assert_eq!(counter.resumes(), 1);
    }

    /// A window ends when the level releases the latch: a hold that arms after
    /// it pauses at once.
    #[test]
    fn a_hold_armed_after_the_level_released_gets_no_window() {
        let (mem, now, pressure) = timed_governor(0.85);
        let (gate, counter) = counting_gate(&pressure);
        assert_eq!(gate.evaluate(), Admit::Hold);

        now.store(30 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes);
        mem.set(0.50);
        assert_eq!(gate.evaluate(), Admit::Yes, "released by the level");
        mem.set(0.90);
        now.store(31 * SECOND, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Hold, "a new hold, no window");
        assert_eq!(counter.pauses(), 2);
        assert_eq!(counter.resumes(), 1);
    }

    #[test]
    fn noop_actuator_gate_still_tracks_held_state() {
        let mem = Arc::new(MockSource::new(0.10, true));
        let pressure = governor_with(Arc::clone(&mem));
        let gate = InboundGate::new(Arc::clone(&pressure), Box::new(NoopActuator));

        assert_eq!(gate.evaluate(), Admit::Yes);
        assert!(!gate.is_held());

        mem.set(0.90);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert!(gate.is_held());

        mem.set(0.10);
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert!(!gate.is_held());
    }
}
