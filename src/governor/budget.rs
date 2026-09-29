// Project:   scalo
// File:      src/governor/budget.rs
// Purpose:   Byte-budget controller: AIMD lever with memory HARD override
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Byte-budget controller: the self-regulation lever.
//!
//! Sizes the inbound byte budget: the payload bytes one receive retains and
//! one sub-block holds. Memory pressure is the only thing that shrinks it.
//! Each block folded in moves it one step:
//!
//! - **memory pressure** (the pressure latch holds): multiplicative-decrease
//!   by `md_factor` (`< 1`), toward the floor.
//! - **otherwise**: additive-increase by `ai_step`, capped at `max_bytes`.
//! - **floor**: the budget never drops below `min_bytes` (derived from
//!   `floor_records`) and never reaches `0`. The [`record_cap`] poll
//!   safety cap stays `>= 1`.
//!
//! [`record_cap`]: ByteBudgetController::record_cap
//!
//! How busy the stage is does not move the budget. Under a backlog a stage
//! spends about as long processing a block as it waits between receives, at
//! any block size, so a utilisation-driven decrease would halve the budget
//! every block down to the floor, where the fixed cost of each sink call caps
//! throughput. Utilisation and CPU saturation are the autoscaler's signal.
//!
//! Starts BIG (`start_bytes`) so a cold pipeline is not artificially
//! throttled, and grows from there until memory pressure says otherwise.
//!
//! Gated behind the `governor` feature; folded per block by
//! [`run_governed`](crate::worker::BatchEngine::run_governed) (default-on).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::source::UnifiedPressure;

/// Default [`ByteBudgetConfig::ema_alpha`], a field with no effect on the budget.
const DEFAULT_EMA_ALPHA: f64 = 0.3;

/// Default [`ByteBudgetConfig::target_rho`], a field with no effect on the budget.
const DEFAULT_TARGET_RHO: f64 = 0.7;

/// Configuration for the [`ByteBudgetController`].
///
/// All byte values are in bytes. The controller starts at `start_bytes`
/// and moves between `min_bytes` (derived from `floor_records`) and
/// `max_bytes`.
#[derive(Debug, Clone, Copy)]
pub struct ByteBudgetConfig {
    /// Initial budget. Starts BIG so a cold pipeline is not throttled.
    pub start_bytes: u64,
    /// Hard ceiling on the budget (additive-increase saturates here).
    pub max_bytes: u64,
    /// Floor in records: the budget never drops below
    /// `floor_records * nominal_record_bytes` (and never below 1 byte).
    pub floor_records: u64,
    /// Nominal per-record size used to derive the byte floor from
    /// `floor_records`. Record sizes vary at runtime; this is only the
    /// floor estimate, not a live measurement.
    pub nominal_record_bytes: u64,
    /// Has no effect on the budget, which moves on memory pressure alone.
    /// Accepted so a config that sets it still builds. Default `0.7`.
    pub target_rho: f64,
    /// Additive-increase step: bytes added per block folded in while the
    /// pressure latch is clear.
    pub ai_step: u64,
    /// Multiplicative-decrease factor in `(0, 1)`, applied per block folded in
    /// while the pressure latch holds. Default `0.5`.
    pub md_factor: f64,
    /// Has no effect on the budget, which reads no timing signal. Accepted so
    /// a config that sets it still builds. Default `0.3`.
    pub ema_alpha: f64,
    /// Poll-safety cap on record count, independent of the byte budget.
    /// A tiny-record flood cannot blow the count even within budget.
    pub record_cap: usize,
}

impl Default for ByteBudgetConfig {
    fn default() -> Self {
        Self {
            // 8 MiB start, 64 MiB ceiling -- generous defaults; a real
            // deployment tunes these via config in a later phase.
            start_bytes: 8 * 1024 * 1024,
            max_bytes: 64 * 1024 * 1024,
            floor_records: 1,
            nominal_record_bytes: 1024,
            target_rho: DEFAULT_TARGET_RHO,
            ai_step: 256 * 1024,
            md_factor: 0.5,
            ema_alpha: DEFAULT_EMA_ALPHA,
            record_cap: 2000,
        }
    }
}

impl ByteBudgetConfig {
    /// The derived absolute byte floor: `floor_records * nominal_record_bytes`,
    /// clamped to at least `1` (the budget is never `0`).
    #[must_use]
    fn min_bytes(&self) -> u64 {
        self.floor_records
            .saturating_mul(self.nominal_record_bytes)
            .max(1)
    }

    /// Sanitise the config so the budget cannot misbehave: force `md_factor`
    /// into `(0, 1)`, and ensure `record_cap >= 1`, `max_bytes >= min_bytes`,
    /// and `start_bytes` inside `[min, max]`.
    fn sanitised(mut self) -> Self {
        if !self.md_factor.is_finite() || self.md_factor <= 0.0 || self.md_factor >= 1.0 {
            self.md_factor = 0.5;
        }
        self.record_cap = self.record_cap.max(1);
        let min = self.min_bytes();
        self.max_bytes = self.max_bytes.max(min);
        self.start_bytes = self.start_bytes.clamp(min, self.max_bytes);
        self
    }
}

/// Write the budget to `self_regulation_byte_budget`, on every change.
fn publish_budget(bytes: u64) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("self_regulation_byte_budget").set(bytes as f64);
    #[cfg(not(feature = "metrics"))]
    let _ = bytes;
}

/// Byte-budget lever: shrinks while the memory pressure latch holds, grows
/// otherwise.
///
/// [`observe`](Self::observe) is the control step; `byte_budget()` and
/// `record_cap()` are the cheap reads the recv loop consults. All state is
/// interior-mutable and `Sync`.
pub struct ByteBudgetController {
    cfg: ByteBudgetConfig,
    pressure: Arc<UnifiedPressure>,
    /// Current byte budget.
    budget: AtomicU64,
}

impl ByteBudgetController {
    /// Build a controller from config and a shared pressure latch.
    ///
    /// The config is sanitised (ranges clamped, floors enforced); the
    /// budget starts at the sanitised `start_bytes`.
    #[must_use]
    pub fn new(cfg: ByteBudgetConfig, pressure: Arc<UnifiedPressure>) -> Self {
        let cfg = cfg.sanitised();
        publish_budget(cfg.start_bytes);
        Self {
            budget: AtomicU64::new(cfg.start_bytes),
            cfg,
            pressure,
        }
    }

    /// Fold one block into the budget.
    ///
    /// While the pressure latch holds, the budget shrinks by `md_factor`
    /// toward the floor; otherwise it grows by `ai_step` toward `max_bytes`.
    /// The arguments describe the block (its payload bytes, how long it took
    /// to process, and the gap since the previous receive) and do not move
    /// the budget.
    pub fn observe(&self, batch_bytes: u64, process_time: Duration, ingest_interval: Duration) {
        let _ = (batch_bytes, process_time, ingest_interval);

        // The budget admits nothing, so it leaves an expired hold's window to the gates.
        if self.pressure.should_hold_without_admitting() {
            self.multiplicative_decrease();
        } else {
            self.additive_increase();
        }
    }

    /// Additive-increase: budget += ai_step, saturating at `max_bytes`.
    fn additive_increase(&self) {
        let cur = self.budget.load(Ordering::Relaxed);
        let next = cur.saturating_add(self.cfg.ai_step).min(self.cfg.max_bytes);
        self.budget.store(next, Ordering::Relaxed);
        publish_budget(next);
    }

    /// Multiplicative-decrease: budget *= md_factor, clamped to the floor
    /// and never `0`.
    fn multiplicative_decrease(&self) {
        let cur = self.budget.load(Ordering::Relaxed);
        // f64 math then back to u64; budgets are well under 2^52 so this is
        // lossless in the operating range. `floor()` then clamp.
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        let scaled = (cur as f64 * self.cfg.md_factor).floor() as u64;
        let next = scaled.max(self.cfg.min_bytes());
        self.budget.store(next, Ordering::Relaxed);
        publish_budget(next);
    }

    /// Current byte budget. Always `>= min_bytes`, never `0`.
    #[must_use]
    pub fn byte_budget(&self) -> u64 {
        self.budget.load(Ordering::Relaxed)
    }

    /// Poll-safety record cap (recv max count), independent of the byte
    /// budget. Always `>= 1` so a tiny-record flood cannot blow the count
    /// even when many records fit inside the byte budget.
    #[must_use]
    pub fn record_cap(&self) -> usize {
        self.cfg.record_cap
    }

    /// The shared pressure governor this controller drives off, so a caller
    /// can read the combined [`level`](UnifiedPressure::level) without holding
    /// a second `Arc`.
    #[must_use]
    pub fn pressure(&self) -> &Arc<UnifiedPressure> {
        &self.pressure
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governor::source::{Hysteresis, Pressure, PressureSource};
    use std::sync::atomic::AtomicU64 as StdAtomicU64;

    /// Scriptable HARD source so the test can force `should_hold()`.
    struct MockSource {
        value: StdAtomicU64,
    }
    impl MockSource {
        fn new(value: f64) -> Self {
            Self {
                value: StdAtomicU64::new(value.to_bits()),
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
            true
        }
    }

    fn controller(
        cfg: ByteBudgetConfig,
        src: &Arc<MockSource>,
    ) -> (ByteBudgetController, Arc<UnifiedPressure>) {
        let hyst = Hysteresis::new(0.80, 0.65).expect("valid band");
        let pressure = Arc::new(UnifiedPressure::new(
            vec![Arc::clone(src) as Arc<dyn PressureSource>],
            hyst,
        ));
        (
            ByteBudgetController::new(cfg, Arc::clone(&pressure)),
            pressure,
        )
    }

    fn test_cfg() -> ByteBudgetConfig {
        ByteBudgetConfig {
            start_bytes: 10_000,
            max_bytes: 100_000,
            floor_records: 1,
            nominal_record_bytes: 1000, // min_bytes = 1000
            target_rho: 0.7,
            ai_step: 5_000,
            md_factor: 0.5,
            ema_alpha: 1.0, // alpha=1 -> EMA == latest sample (deterministic)
            record_cap: 2000,
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn starts_big_at_start_bytes() {
        let src = Arc::new(MockSource::new(0.0));
        let (ctl, _p) = controller(test_cfg(), &src);
        assert_eq!(ctl.byte_budget(), 10_000);
        assert!(ctl.record_cap() >= 1);
        assert_eq!(ctl.record_cap(), 2000);
    }

    /// Block timings from an idle stage to a fully busy one, as
    /// `(process_time, ingest_interval)`. A backlog reads as fully busy at any
    /// block size.
    fn utilisations() -> [(Duration, Duration); 4] {
        [
            (ms(10), ms(100)),
            (ms(90), ms(100)),
            (ms(100), ms(100)),
            (ms(20), ms(19)),
        ]
    }

    /// Without memory pressure the budget grows one `ai_step` a block to the
    /// cap, however busy the stage is.
    #[test]
    fn without_memory_pressure_the_budget_grows_to_the_cap_at_any_utilisation() {
        for (process, ingest) in utilisations() {
            let src = Arc::new(MockSource::new(0.0));
            let (ctl, _p) = controller(test_cfg(), &src);

            ctl.observe(500, process, ingest);
            assert_eq!(
                ctl.byte_budget(),
                15_000,
                "one ai_step up (10_000 + 5_000) at process {process:?}, ingest {ingest:?}"
            );
            let mut last = ctl.byte_budget();
            for _ in 0..50 {
                ctl.observe(500, process, ingest);
                let now = ctl.byte_budget();
                assert!(
                    now >= last,
                    "monotone up at process {process:?}, ingest {ingest:?}"
                );
                last = now;
            }
            assert_eq!(ctl.byte_budget(), 100_000, "capped at max_bytes");
        }
    }

    /// A zero ingest interval (the first block, or back-to-back receives)
    /// grows the budget like any other block without memory pressure.
    #[test]
    fn a_zero_ingest_interval_grows_the_budget_without_memory_pressure() {
        let src = Arc::new(MockSource::new(0.0));
        let (ctl, _p) = controller(test_cfg(), &src);

        ctl.observe(500, ms(5), Duration::ZERO);
        assert_eq!(
            ctl.byte_budget(),
            15_000,
            "the first block does not shrink the budget"
        );
        ctl.observe(0, Duration::ZERO, Duration::ZERO);
        assert_eq!(ctl.byte_budget(), 20_000, "an empty block grows it too");
    }

    /// The memory latch halves the budget every block, whatever the timings,
    /// down to the floor and never below it.
    #[test]
    fn the_memory_latch_shrinks_the_budget_toward_the_floor() {
        for (process, ingest) in utilisations() {
            let src = Arc::new(MockSource::new(0.0));
            let (ctl, _p) = controller(test_cfg(), &src);
            ctl.observe(500, process, ingest);
            ctl.observe(500, process, ingest);
            assert_eq!(ctl.byte_budget(), 20_000);

            src.set(0.95);
            ctl.observe(500, process, ingest);
            assert_eq!(
                ctl.byte_budget(),
                10_000,
                "20_000 * 0.5 at process {process:?}, ingest {ingest:?}"
            );
            for _ in 0..20 {
                ctl.observe(500, process, ingest);
            }
            assert_eq!(ctl.byte_budget(), 1_000, "clamped to min_bytes");
            assert!(ctl.record_cap() >= 1);
        }
    }

    /// The floor still admits a record, and once the memory latch releases the
    /// budget climbs back one `ai_step` a block to the cap, with the stage fully
    /// busy throughout.
    #[test]
    fn the_budget_recovers_from_the_floor_when_the_memory_latch_releases() {
        let src = Arc::new(MockSource::new(0.95));
        let (ctl, _p) = controller(test_cfg(), &src);
        let busy = (ms(100), ms(100));

        for _ in 0..30 {
            ctl.observe(500, busy.0, busy.1);
        }
        assert_eq!(ctl.byte_budget(), 1_000, "the held latch pins the floor");
        assert!(ctl.byte_budget() >= test_cfg().nominal_record_bytes);

        // Below resume_below (0.65), so the latch releases.
        src.set(0.10);
        ctl.observe(500, busy.0, busy.1);
        assert_eq!(
            ctl.byte_budget(),
            6_000,
            "floor + one ai_step (1000 + 5000)"
        );
        let mut last = ctl.byte_budget();
        for _ in 0..50 {
            ctl.observe(500, busy.0, busy.1);
            let now = ctl.byte_budget();
            assert!(now >= last, "monotone up while recovering");
            last = now;
        }
        assert_eq!(ctl.byte_budget(), 100_000, "recovers to the cap");
    }

    /// A driver folds a block between two receives. When the hold expires
    /// there, the budget still shrinks as held and the next receive's gate
    /// gets the window, so the paused source resumes.
    #[test]
    fn the_budget_leaves_an_expired_hold_to_the_gate() {
        use crate::governor::{Admit, InboundGate, NoopActuator};

        let src = Arc::new(MockSource::new(0.85));
        let now = Arc::new(StdAtomicU64::new(0));
        let pressure = Arc::new(
            UnifiedPressure::new(
                vec![Arc::clone(&src) as Arc<dyn PressureSource>],
                Hysteresis::new(0.80, 0.65).expect("valid band"),
            )
            .with_manual_clock(Arc::clone(&now)),
        );
        let ctl = ByteBudgetController::new(test_cfg(), Arc::clone(&pressure));
        let gate = InboundGate::new(Arc::clone(&pressure), Box::new(NoopActuator));
        assert_eq!(gate.evaluate(), Admit::Hold);

        now.store(30_000_000_000, Ordering::Relaxed);
        ctl.observe(0, Duration::ZERO, ms(100));
        assert_eq!(ctl.byte_budget(), 5_000, "held: 10_000 halved");
        assert_eq!(gate.evaluate(), Admit::Yes, "the gate gets the window");
        gate.note_received(1);
        assert_eq!(gate.evaluate(), Admit::Hold);
    }

    /// Every change to the budget reaches the gauge, the start value included.
    #[cfg(feature = "metrics")]
    #[test]
    fn the_budget_gauge_follows_every_change() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = ::metrics::set_default_local_recorder(&recorder);

        let budget = || {
            handle.render().lines().find_map(|line| {
                line.strip_prefix("self_regulation_byte_budget ")?
                    .parse::<f64>()
                    .ok()
            })
        };

        let src = Arc::new(MockSource::new(0.0));
        let (ctl, _p) = controller(test_cfg(), &src);
        assert_eq!(budget(), Some(10_000.0), "the start budget");
        ctl.observe(500, ms(10), ms(100));
        assert_eq!(budget(), Some(15_000.0), "one additive step");
    }

    /// Config sanitisation: garbage ranges fall back to safe defaults and
    /// the budget never starts at or reaches zero.
    #[test]
    fn config_is_sanitised() {
        let src = Arc::new(MockSource::new(0.0));
        let bad = ByteBudgetConfig {
            start_bytes: 0, // below floor
            max_bytes: 0,   // below floor
            floor_records: 2,
            nominal_record_bytes: 500, // min_bytes = 1000
            target_rho: 5.0,           // out of range -> default 0.7
            ai_step: 1_000,
            md_factor: 2.0, // out of range -> default 0.5
            ema_alpha: 0.0, // out of range -> default
            record_cap: 0,  // -> clamped to 1
        };
        let (ctl, _p) = controller(bad, &src);
        assert_eq!(ctl.byte_budget(), 1_000, "start clamped up to min_bytes");
        assert!(ctl.record_cap() >= 1);
    }
}
