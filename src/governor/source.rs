// Project:   scalo
// File:      src/governor/source.rs
// Purpose:   Pressure seam + memory source for the self-regulation governor
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Pressure seam: normalised readings, sources, and the unified latch.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::memory::MemoryGuard;

/// Default bound, in seconds, on a hold whose level stays above `resume_below`.
pub(crate) const DEFAULT_MAX_HOLD_SECS: u64 = 30;

/// Least time between two warnings that a hold reached its bound.
#[cfg(feature = "logger")]
const EXPIRED_HOLD_WARN_INTERVAL_MS: u64 = 60_000;

/// Change in the level that republishes `self_regulation_pressure_ratio`.
#[cfg(feature = "metrics")]
const LEVEL_PUBLISH_STEP: f64 = 0.001;

/// Nanoseconds in `d`, saturating at `u64::MAX`.
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// The monotonic time a hold is measured on.
enum HoldClock {
    /// Nanoseconds since the latch was built.
    Monotonic(Instant),
    /// Nanoseconds a test sets.
    #[cfg(test)]
    Manual(Arc<AtomicU64>),
}

impl HoldClock {
    fn now_nanos(&self) -> u64 {
        match self {
            Self::Monotonic(epoch) => nanos(epoch.elapsed()),
            #[cfg(test)]
            Self::Manual(now) => now.load(Ordering::Relaxed),
        }
    }
}

/// A normalised pressure reading, clamped to `[0.0, 1.0]` on construction.
///
/// `NaN` collapses to `0.0` (treat an unreadable source as no pressure,
/// never as max pressure -- a `NaN` masquerading as `1.0` would wedge the
/// governor into a permanent hold).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Pressure(f64);

impl Pressure {
    /// Construct a reading, clamping to `[0.0, 1.0]`. `NaN` becomes `0.0`.
    #[must_use]
    pub fn new(value: f64) -> Self {
        // `clamp` panics on NaN, and `f64::max(NaN, 0.0)` returns 0.0 only
        // for the left-NaN form -- be explicit so the intent is obvious.
        let v = if value.is_nan() {
            0.0
        } else {
            value.clamp(0.0, 1.0)
        };
        Self(v)
    }

    /// The clamped reading in `[0.0, 1.0]`.
    #[must_use]
    pub fn get(&self) -> f64 {
        self.0
    }
}

/// A source of normalised pressure feeding the unified governor.
///
/// Implementors are wrappers over the real signal (memory guard, and
/// later CPU, queue depth, etc.). Keeping the trait here -- not in the
/// signal's own module -- keeps `memory` a leaf with no governor
/// dependency.
pub trait PressureSource: Send + Sync {
    /// Stable identifier for diagnostics (e.g. `"memory"`).
    fn name(&self) -> &'static str;

    /// Sample the current pressure.
    fn sample(&self) -> Pressure;

    /// Sensitivity weight applied to SOFT signals in the combine. HARD
    /// signals ignore this (they are never down-weighted). Default `1.0`.
    fn weight(&self) -> f64 {
        1.0
    }

    /// HARD signals are never masked or down-weighted -- their raw reading
    /// always competes for the combined level. Default `false`.
    fn is_hard(&self) -> bool {
        false
    }
}

/// HARD pressure source backed by the [`MemoryGuard`].
///
/// A thin wrapper so `memory` stays a leaf module: the trait
/// implementation lives here, in the governor, not in `guard.rs`.
pub struct MemoryPressureSource(Arc<MemoryGuard>);

impl MemoryPressureSource {
    /// Wrap a shared memory guard as a pressure source.
    #[must_use]
    pub fn new(guard: Arc<MemoryGuard>) -> Self {
        Self(guard)
    }
}

impl PressureSource for MemoryPressureSource {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn sample(&self) -> Pressure {
        Pressure::new(self.0.pressure_ratio())
    }

    fn weight(&self) -> f64 {
        1.0
    }

    fn is_hard(&self) -> bool {
        true
    }
}

/// HARD pressure source over the bytes a push source holds unanswered, read
/// as a fraction of its held-byte ceiling.
///
/// On the shared [`UnifiedPressure`] it arms the same latch that pauses Kafka
/// partitions and refuses pushes, so held responses and memory brake through
/// one gate. A gRPC receive server built with a governor attaches one itself.
pub struct AckHeldSource {
    held: Arc<AtomicU64>,
    ceiling: u64,
}

impl AckHeldSource {
    /// Read `held` bytes against `ceiling`. A zero ceiling reads full as soon
    /// as anything is held.
    #[must_use]
    pub fn new(held: Arc<AtomicU64>, ceiling: u64) -> Self {
        Self {
            held,
            ceiling: ceiling.max(1),
        }
    }
}

impl PressureSource for AckHeldSource {
    fn name(&self) -> &'static str {
        "ack_held"
    }

    fn sample(&self) -> Pressure {
        Pressure::new(self.held.load(Ordering::Relaxed) as f64 / self.ceiling as f64)
    }

    fn is_hard(&self) -> bool {
        true
    }
}

/// Hysteresis band for the pause/resume latch.
///
/// `pause_above` must be strictly greater than `resume_below`, otherwise
/// there is no band to hold the latch and it degenerates to a single
/// threshold (flapping). [`Self::new`] validates this.
#[derive(Debug, Clone, Copy)]
pub struct Hysteresis {
    /// Arm the latch (start holding) when the level reaches this.
    pub pause_above: f64,
    /// Release the latch (stop holding) when the level drops to this.
    pub resume_below: f64,
}

impl Hysteresis {
    /// Construct a band, validating `pause_above > resume_below` and that
    /// both bounds sit inside `[0.0, 1.0]`.
    ///
    /// The range check matters because [`Pressure`] samples are clamped to
    /// `[0.0, 1.0]`: a `resume_below < 0.0` could never release the latch
    /// (permanent stuck-pause) and a `pause_above > 1.0` could never arm it
    /// (brake silently disabled).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the bounds are non-finite, outside `[0.0, 1.0]`, or
    /// `pause_above` is not strictly greater than `resume_below`.
    pub fn new(pause_above: f64, resume_below: f64) -> Result<Self, String> {
        if !pause_above.is_finite() || !resume_below.is_finite() {
            return Err(format!(
                "hysteresis bounds must be finite, got pause_above={pause_above}, \
                 resume_below={resume_below}"
            ));
        }
        if !(0.0..=1.0).contains(&pause_above) || !(0.0..=1.0).contains(&resume_below) {
            return Err(format!(
                "hysteresis bounds must be within [0.0, 1.0] (pressure levels are \
                 clamped to that range), got pause_above={pause_above}, \
                 resume_below={resume_below}"
            ));
        }
        if pause_above <= resume_below {
            return Err(format!(
                "hysteresis requires pause_above > resume_below, got \
                 pause_above={pause_above}, resume_below={resume_below}"
            ));
        }
        Ok(Self {
            pause_above,
            resume_below,
        })
    }
}

/// A per-source diagnostic line in a [`UnifiedPressureSnapshot`].
#[derive(Debug, Clone)]
pub struct SourceReading {
    /// Source identifier.
    pub name: &'static str,
    /// Raw clamped sample.
    pub raw: f64,
    /// Weight the source declares.
    pub weight: f64,
    /// Whether the source is HARD (raw, never masked).
    pub is_hard: bool,
    /// The value that competed for the combined level (raw for HARD,
    /// `raw * weight` for SOFT).
    pub effective: f64,
}

/// Point-in-time breakdown of the governor for diagnostics / metrics.
#[derive(Debug, Clone)]
pub struct UnifiedPressureSnapshot {
    /// Per-source readings.
    pub sources: Vec<SourceReading>,
    /// Max raw reading across HARD sources (`0.0` if none).
    pub hard_max: f64,
    /// Max weighted reading across SOFT sources (`0.0` if none).
    pub soft_max: f64,
    /// Combined level (`hard_max.max(soft_max)`).
    pub level: f64,
    /// Latched hold state at snapshot time.
    pub paused: bool,
}

/// Combines pressure sources into one level under a hysteretic latch.
///
/// See the [module docs](crate::governor) for the design invariants. The
/// latch state is one atomic word so [`should_hold`](Self::should_hold) is a
/// cheap, `Sync` hot-path check.
pub struct UnifiedPressure {
    /// Behind a lock so a source can join a latch already shared by the
    /// transports it gates.
    sources: parking_lot::RwLock<Vec<Arc<dyn PressureSource>>>,
    hyst: Hysteresis,
    /// `0` while released; while held, the clock reading the hold armed at,
    /// plus one, so one compare-exchange ends exactly the hold that was read.
    hold: AtomicU64,
    /// Longest a hold lasts with the level above `resume_below`, in clock
    /// nanoseconds. `0` is no bound.
    max_hold_nanos: u64,
    /// Holds that reached `max_hold_nanos` and were ended by it.
    expired_holds: AtomicU64,
    clock: HoldClock,
    /// When the last expired-hold warning went out, in Unix epoch ms.
    #[cfg(feature = "logger")]
    last_expiry_warn_ms: AtomicU64,
    /// The level last written to `self_regulation_pressure_ratio`, as bits.
    #[cfg(feature = "metrics")]
    published_level: AtomicU64,
}

impl UnifiedPressure {
    /// Build a governor over the given sources and hysteresis band, with a
    /// hold bounded at 30 s (see [`with_max_hold`](Self::with_max_hold)).
    #[must_use]
    pub fn new(sources: Vec<Arc<dyn PressureSource>>, hyst: Hysteresis) -> Self {
        Self {
            sources: parking_lot::RwLock::new(sources),
            hyst,
            hold: AtomicU64::new(0),
            max_hold_nanos: nanos(Duration::from_secs(DEFAULT_MAX_HOLD_SECS)),
            expired_holds: AtomicU64::new(0),
            clock: HoldClock::Monotonic(Instant::now()),
            #[cfg(feature = "logger")]
            last_expiry_warn_ms: AtomicU64::new(0),
            #[cfg(feature = "metrics")]
            published_level: AtomicU64::new(f64::NAN.to_bits()),
        }
    }

    /// Bound how long the latch holds while the level stays above
    /// `resume_below`. `Duration::ZERO` removes the bound.
    ///
    /// Memory the process already holds -- allocator arenas, pages retained
    /// after a free -- can keep the level above `resume_below` with no intake
    /// at all, and an unbounded latch then holds intake paused for good. Once
    /// a hold has lasted `max_hold`, the next caller of
    /// [`should_hold`](Self::should_hold) is admitted, a warning names the
    /// level and the time held, and `self_regulation_max_hold_releases_total`
    /// counts it. The evaluation after that re-arms if the level is still at
    /// `pause_above`, so the latch still brakes a level that keeps rising.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use scalo::governor::{Hysteresis, UnifiedPressure};
    ///
    /// // Admit one window per minute of a hold the level never clears.
    /// let band = Hysteresis::new(0.80, 0.65)?;
    /// let latch = UnifiedPressure::new(Vec::new(), band).with_max_hold(Duration::from_secs(60));
    /// assert!(!latch.should_hold());
    /// # Ok::<(), String>(())
    /// ```
    #[must_use]
    pub fn with_max_hold(mut self, max_hold: Duration) -> Self {
        self.max_hold_nanos = nanos(max_hold);
        self
    }

    /// The same latch, timed on a clock the test sets in nanoseconds.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_manual_clock(mut self, now: Arc<AtomicU64>) -> Self {
        self.clock = HoldClock::Manual(now);
        self
    }

    /// The bound in force, `Duration::ZERO` when there is none.
    #[cfg(test)]
    pub(crate) fn max_hold(&self) -> Duration {
        Duration::from_nanos(self.max_hold_nanos)
    }

    /// Add a source after construction.
    ///
    /// Proves the seam accepts a new signal kind (e.g. a future CPU
    /// source) with zero change to the gate API -- existing callers of
    /// [`level`](Self::level) / [`should_hold`](Self::should_hold) are
    /// untouched.
    pub fn add_source(&mut self, source: Arc<dyn PressureSource>) {
        self.sources.get_mut().push(source);
    }

    /// Add a source to a governor already shared behind an `Arc`.
    ///
    /// It counts from the next [`level`](Self::level) on, for every holder of
    /// the governor.
    pub fn attach_source(&self, source: Arc<dyn PressureSource>) {
        self.sources.write().push(source);
    }

    /// Combined pressure level in `[0.0, 1.0]`.
    ///
    /// `hard_max` = max raw reading over HARD sources (never weighted,
    /// never masked). `soft_max` = max of `sample * weight` over SOFT
    /// sources. `level = hard_max.max(soft_max)`.
    ///
    /// Soft `weight()` is clamped to `[0.0, 1.0]` (non-finite -> `1.0`) at the
    /// combine: `add_source` is the advertised extension seam, so a future
    /// soft source returning a stray weight cannot push `level()` outside its
    /// documented range or silently neutralise itself with a negative/NaN
    /// weight. One branch per soft source per `evaluate()` (not per record);
    /// with no soft source wired today the cost is nil.
    #[must_use]
    pub fn level(&self) -> f64 {
        let mut hard_max = 0.0_f64;
        let mut soft_max = 0.0_f64;
        for src in self.sources.read().iter() {
            let raw = src.sample().get();
            if src.is_hard() {
                hard_max = hard_max.max(raw);
            } else {
                let w = src.weight();
                let w = if w.is_finite() {
                    w.clamp(0.0, 1.0)
                } else {
                    1.0
                };
                soft_max = soft_max.max(raw * w);
            }
        }
        hard_max.max(soft_max)
    }

    /// Hysteretic hold latch over [`level`](Self::level), bounded in time.
    /// Call it where work is admitted.
    ///
    /// - Held and `level <= resume_below` -> release, return `false`.
    /// - Not held and `level >= pause_above` -> arm, return `true`.
    /// - Held for [`max_hold`](Self::with_max_hold) with the level still above
    ///   `resume_below` -> release for this one caller, return `false`. The
    ///   next evaluation re-arms if the level is still at `pause_above`.
    /// - Otherwise -> return the current latch state (the band holds it).
    ///
    /// Beyond [`level`](Self::level) it loads the latch word and, with
    /// `metrics`, the last published level; a held latch with a bound also
    /// reads the monotonic clock.
    #[must_use]
    pub fn should_hold(&self) -> bool {
        self.evaluate(true)
    }

    /// The latch for a caller that admits no work, such as a sizing lever.
    ///
    /// Arms and releases on the level as [`should_hold`](Self::should_hold)
    /// does, but never takes the admission a hold past `max_hold` lets
    /// through, so that admission reaches a caller that admits work.
    #[must_use]
    pub(crate) fn should_hold_without_admitting(&self) -> bool {
        self.evaluate(false)
    }

    /// Holds that `max_hold` has ended since the latch was built.
    pub(crate) fn expired_holds(&self) -> u64 {
        self.expired_holds.load(Ordering::Acquire)
    }

    /// The clock holds are measured on, in nanoseconds.
    pub(crate) fn now_nanos(&self) -> u64 {
        self.clock.now_nanos()
    }

    fn evaluate(&self, admitting: bool) -> bool {
        let level = self.level();
        #[cfg(feature = "metrics")]
        self.publish_level(level);
        let hold = self.hold.load(Ordering::Acquire);
        if hold == 0 {
            if level >= self.hyst.pause_above {
                let armed = self.clock.now_nanos().saturating_add(1);
                self.hold.store(armed, Ordering::Release);
                return true;
            }
            return false;
        }
        if level <= self.hyst.resume_below {
            self.hold.store(0, Ordering::Release);
            return false;
        }
        !(admitting && self.end_expired_hold(hold, level))
    }

    /// End the hold read as `hold` once it has lasted `max_hold`, for exactly
    /// one caller.
    fn end_expired_hold(&self, hold: u64, level: f64) -> bool {
        if self.max_hold_nanos == 0 {
            return false;
        }
        let held_for = self.clock.now_nanos().saturating_sub(hold - 1);
        if held_for < self.max_hold_nanos {
            return false;
        }
        // Fails when a racing caller ended this hold, or released and re-armed it.
        if self
            .hold
            .compare_exchange(hold, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.expired_holds.fetch_add(1, Ordering::AcqRel);
        self.note_expired_hold(level, Duration::from_nanos(held_for));
        true
    }

    #[cold]
    fn note_expired_hold(&self, level: f64, held_for: Duration) {
        let signal = self.dominant_source();
        #[cfg(feature = "metrics")]
        ::metrics::counter!("self_regulation_max_hold_releases_total", "signal" => signal)
            .increment(1);
        #[cfg(feature = "logger")]
        let due =
            crate::logger::log_debounced(&self.last_expiry_warn_ms, EXPIRED_HOLD_WARN_INTERVAL_MS);
        // Without the logger helpers the warning is already bounded to one per max_hold.
        #[cfg(not(feature = "logger"))]
        let due = true;
        if due {
            tracing::warn!(
                pressure = level,
                resume_below = self.hyst.resume_below,
                held_secs = held_for.as_secs_f64(),
                signal,
                "self-regulation: hold reached max_hold with pressure still above resume_below; \
                 admitting one window"
            );
        }
    }

    /// The source whose reading sets the level, to label an expired hold.
    fn dominant_source(&self) -> &'static str {
        self.snapshot()
            .sources
            .into_iter()
            .max_by(|a, b| a.effective.total_cmp(&b.effective))
            .map_or("none", |reading| reading.name)
    }

    /// Write the level to `self_regulation_pressure_ratio` when it has moved by
    /// `LEVEL_PUBLISH_STEP`, keeping the recorder off most evaluations.
    #[cfg(feature = "metrics")]
    fn publish_level(&self, level: f64) {
        let last = f64::from_bits(self.published_level.load(Ordering::Relaxed));
        if last.is_nan() || (level - last).abs() >= LEVEL_PUBLISH_STEP {
            self.published_level
                .store(level.to_bits(), Ordering::Relaxed);
            ::metrics::gauge!("self_regulation_pressure_ratio").set(level);
        }
    }

    /// Per-source breakdown plus the combined level and latch state.
    #[must_use]
    pub fn snapshot(&self) -> UnifiedPressureSnapshot {
        let sources = self.sources.read();
        let mut readings = Vec::with_capacity(sources.len());
        let mut hard_max = 0.0_f64;
        let mut soft_max = 0.0_f64;
        for src in sources.iter() {
            let raw = src.sample().get();
            let weight = src.weight();
            let is_hard = src.is_hard();
            let effective = if is_hard { raw } else { raw * weight };
            if is_hard {
                hard_max = hard_max.max(raw);
            } else {
                soft_max = soft_max.max(effective);
            }
            readings.push(SourceReading {
                name: src.name(),
                raw,
                weight,
                is_hard,
                effective,
            });
        }
        UnifiedPressureSnapshot {
            sources: readings,
            hard_max,
            soft_max,
            level: hard_max.max(soft_max),
            paused: self.hold.load(Ordering::Acquire) != 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    /// Scriptable test double: a source whose reading can be set at
    /// runtime so a single `UnifiedPressure` can be driven through a
    /// rising/falling sequence. Stores the reading as bit-pattern `u64`
    /// so it stays `Sync` without a lock (crate forbids `unsafe`, so a
    /// `Cell` would not be `Sync`).
    struct MockSource {
        name: &'static str,
        value: AtomicU64,
        weight: f64,
        hard: bool,
    }

    impl MockSource {
        fn new(name: &'static str, value: f64, weight: f64, hard: bool) -> Self {
            Self {
                name,
                value: AtomicU64::new(value.to_bits()),
                weight,
                hard,
            }
        }

        fn set(&self, value: f64) {
            self.value.store(value.to_bits(), Ordering::Relaxed);
        }
    }

    impl PressureSource for MockSource {
        fn name(&self) -> &'static str {
            self.name
        }
        fn sample(&self) -> Pressure {
            Pressure::new(f64::from_bits(self.value.load(Ordering::Relaxed)))
        }
        fn weight(&self) -> f64 {
            self.weight
        }
        fn is_hard(&self) -> bool {
            self.hard
        }
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn pressure_clamps_and_handles_nan() {
        assert!(approx(Pressure::new(-1.0).get(), 0.0));
        assert!(approx(Pressure::new(2.0).get(), 1.0));
        assert!(approx(Pressure::new(0.5).get(), 0.5));
        // NaN must collapse to 0.0, NOT to 1.0 -- a NaN reading must never
        // wedge the governor into a permanent hold.
        assert!(approx(Pressure::new(f64::NAN).get(), 0.0));
        assert!(approx(Pressure::new(f64::INFINITY).get(), 1.0));
        assert!(approx(Pressure::new(f64::NEG_INFINITY).get(), 0.0));
    }

    #[test]
    fn hysteresis_rejects_inverted_band() {
        assert!(Hysteresis::new(0.80, 0.65).is_ok());
        assert!(Hysteresis::new(0.65, 0.80).is_err());
        assert!(Hysteresis::new(0.80, 0.80).is_err());
        assert!(Hysteresis::new(f64::NAN, 0.5).is_err());
    }

    /// Out-of-`[0,1]` bands must be rejected: `Pressure` clamps every sample to
    /// `[0,1]`, so a band outside that range is unreachable in one direction --
    /// `resume_below < 0.0` can never release (permanent stuck-pause) and
    /// `pause_above > 1.0` can never arm (brake silently disabled).
    #[test]
    fn hysteresis_rejects_out_of_range_band() {
        // resume_below below the clamp floor -> latch could never release.
        assert!(Hysteresis::new(0.5, -0.1).is_err());
        // pause_above above the clamp ceiling -> latch could never arm.
        assert!(Hysteresis::new(1.5, 0.65).is_err());
        // Both ends in range, valid ordering -> still ok (no regression).
        assert!(Hysteresis::new(1.0, 0.0).is_ok());
        assert!(Hysteresis::new(0.80, 0.65).is_ok());
    }

    /// A future SOFT source returning a stray weight must not push `level()`
    /// outside `[0,1]` or silently neutralise itself. `add_source` is the
    /// advertised extension seam, so the combine clamps each soft weight to
    /// `[0,1]` (non-finite -> `1.0`).
    #[test]
    fn soft_weight_is_clamped_at_combine() {
        // weight > 1 at full pressure -> clamped to 1.0, level == 1.0 (not 5.0).
        let over = Arc::new(MockSource::new("over", 1.0, 5.0, false));
        let p = UnifiedPressure::new(
            vec![Arc::clone(&over) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("band"),
        );
        assert!(
            approx(p.level(), 1.0),
            "level must stay <= 1.0, got {}",
            p.level()
        );

        // NaN weight -> 1.0 multiplier (not dropped by f64::max(NaN)).
        let nan_w = Arc::new(MockSource::new("nan", 0.5, f64::NAN, false));
        let p2 = UnifiedPressure::new(
            vec![Arc::clone(&nan_w) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("band"),
        );
        assert!(
            approx(p2.level(), 0.5),
            "NaN weight -> 1.0, got {}",
            p2.level()
        );

        // Negative weight -> clamped to 0.0, contributes nothing.
        let neg = Arc::new(MockSource::new("neg", 1.0, -2.0, false));
        let p3 = UnifiedPressure::new(
            vec![Arc::clone(&neg) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("band"),
        );
        assert!(
            approx(p3.level(), 0.0),
            "negative weight -> 0, got {}",
            p3.level()
        );
    }

    /// The adversarial proving test.
    ///
    /// Drives one `UnifiedPressure` through the full pause/resume cycle and
    /// proves the two riskiest invariants:
    ///   1. a saturated SOFT signal at weight 0.5 cannot force a hold the
    ///      HARD signal would not (no soft-masks-hard, no spurious hold);
    ///   2. the hysteresis latch arms on the rising edge, holds inside the
    ///      band, releases on the falling edge, and re-arms cleanly (no
    ///      sticky state).
    ///
    /// Step 6 proves a third soft source plugs in via `add_source` with
    /// zero change to the gate API.
    #[test]
    fn adversarial_combine_and_hysteresis() {
        let hyst = Hysteresis::new(0.80, 0.65).expect("valid band");

        // HARD memory source + a SOFT "cpu" source at weight 0.5.
        let mem = Arc::new(MockSource::new("memory", 0.50, 1.0, true));
        let cpu = Arc::new(MockSource::new("cpu", 1.0, 0.5, false));

        let governor = UnifiedPressure::new(
            vec![
                Arc::clone(&mem) as Arc<dyn PressureSource>,
                Arc::clone(&cpu) as Arc<dyn PressureSource>,
            ],
            hyst,
        );

        // Step 1: memory=0.50, cpu=1.0 (saturated SOFT).
        // soft = 1.0 * 0.5 = 0.50; hard = 0.50; level = max(0.50, 0.50) = 0.50.
        // A saturated SOFT signal at weight 0.5 CANNOT force a hold the HARD
        // signal would not. level < pause_above -> no hold.
        assert!(
            approx(governor.level(), 0.50),
            "level should be 0.50, got {}",
            governor.level()
        );
        assert!(
            !governor.should_hold(),
            "saturated soft signal must not mask/force a hold"
        );

        // Step 2: memory rises to 0.85 -> rising edge latches.
        mem.set(0.85);
        assert!(approx(governor.level(), 0.85), "hard 0.85 dominates");
        assert!(
            governor.should_hold(),
            "rising edge above pause_above latches"
        );

        // Step 3: memory falls to 0.70 -> inside band (> resume_below) -> holds.
        mem.set(0.70);
        assert!(approx(governor.level(), 0.70));
        assert!(
            governor.should_hold(),
            "0.70 is inside the hysteresis band -> latch stays held"
        );

        // Step 4: memory falls to 0.60 -> below resume_below -> releases.
        mem.set(0.60);
        assert!(approx(governor.level(), 0.60));
        assert!(
            !governor.should_hold(),
            "falling edge below resume_below releases the latch"
        );

        // Step 5: memory back to 0.85 -> latch re-arms (no sticky state).
        mem.set(0.85);
        assert!(
            governor.should_hold(),
            "latch must re-arm cleanly with no sticky state"
        );

        // Step 6: add a THIRD soft source via add_source -- proves the seam
        // accepts a new signal kind with zero gate-API change. Release first
        // so we can observe the new source's effect cleanly.
        mem.set(0.10);
        let mut governor = governor;
        let queue = Arc::new(MockSource::new("queue_depth", 0.0, 0.5, false));
        governor.add_source(Arc::clone(&queue) as Arc<dyn PressureSource>);

        // Drop out of the band first (everything low) so the latch releases.
        cpu.set(0.0);
        assert!(!governor.should_hold(), "all sources low -> released");

        // Now saturate the new SOFT source: 1.0 * 0.5 = 0.50, still under
        // pause_above. Same gate API, same behaviour -- a weighted soft
        // source cannot force a hold on its own.
        queue.set(1.0);
        assert!(
            approx(governor.level(), 0.50),
            "new soft source weighted in"
        );
        assert!(
            !governor.should_hold(),
            "weighted third soft source still cannot force a hold"
        );

        // And the HARD signal still gets through unmasked over the new source.
        mem.set(0.90);
        assert!(approx(governor.level(), 0.90), "hard signal unmasked");
        assert!(
            governor.should_hold(),
            "hard signal re-arms over soft sources"
        );
    }

    #[test]
    fn snapshot_reports_per_source_breakdown() {
        let hyst = Hysteresis::new(0.80, 0.65).expect("valid band");
        let mem = Arc::new(MockSource::new("memory", 0.70, 1.0, true));
        let cpu = Arc::new(MockSource::new("cpu", 0.40, 0.5, false));
        let governor = UnifiedPressure::new(
            vec![
                mem as Arc<dyn PressureSource>,
                cpu as Arc<dyn PressureSource>,
            ],
            hyst,
        );

        let snap = governor.snapshot();
        assert_eq!(snap.sources.len(), 2);
        assert!(approx(snap.hard_max, 0.70));
        assert!(approx(snap.soft_max, 0.20)); // 0.40 * 0.5
        assert!(approx(snap.level, 0.70));
        assert!(!snap.paused);

        let cpu_reading = snap
            .sources
            .iter()
            .find(|r| r.name == "cpu")
            .expect("cpu present");
        assert!(!cpu_reading.is_hard);
        assert!(approx(cpu_reading.effective, 0.20));
    }

    /// Held bytes at the ceiling arm the latch the inbound gate reads, and the
    /// gate pauses once per edge, not once per evaluation.
    #[test]
    fn ack_held_source_trips_the_inbound_gate() {
        use crate::governor::{Admit, GateActuator, InboundGate};
        use std::sync::atomic::AtomicUsize;

        struct Counting {
            pauses: Arc<AtomicUsize>,
            resumes: Arc<AtomicUsize>,
        }
        impl GateActuator for Counting {
            fn pause(&self) {
                self.pauses.fetch_add(1, Ordering::SeqCst);
            }
            fn resume(&self) {
                self.resumes.fetch_add(1, Ordering::SeqCst);
            }
        }

        let held = Arc::new(AtomicU64::new(0));
        // Attached to a latch already shared, as a receive server does.
        let pressure = Arc::new(UnifiedPressure::new(
            Vec::new(),
            Hysteresis::new(0.80, 0.65).expect("band"),
        ));
        let source = AckHeldSource::new(Arc::clone(&held), 1000);
        assert!(source.is_hard(), "held bytes are never down-weighted");
        pressure.attach_source(Arc::new(source));
        let (pauses, resumes) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let gate = InboundGate::new(
            Arc::clone(&pressure),
            Box::new(Counting {
                pauses: Arc::clone(&pauses),
                resumes: Arc::clone(&resumes),
            }),
        );

        assert_eq!(gate.evaluate(), Admit::Yes, "nothing held");
        held.store(1000, Ordering::Relaxed);
        assert!(pressure.should_hold(), "held bytes at the ceiling hold");
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(gate.evaluate(), Admit::Hold);
        assert_eq!(pauses.load(Ordering::SeqCst), 1, "one pause per edge");

        held.store(700, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Hold, "0.70 is inside the band");
        held.store(500, Ordering::Relaxed);
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert_eq!(gate.evaluate(), Admit::Yes);
        assert_eq!(resumes.load(Ordering::SeqCst), 1, "one resume per edge");

        held.store(1, Ordering::Relaxed);
        assert!(
            approx(AckHeldSource::new(held, 0).sample().get(), 1.0),
            "a zero ceiling reads full once anything is held"
        );
    }

    #[test]
    fn memory_pressure_source_wraps_guard_as_hard() {
        use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

        // Pinned to the reservation counter so the ratio is the 700/1000 this
        // test sets, not the host's own memory usage.
        let guard = Arc::new(MemoryGuard::with_usage_source(
            MemoryGuardConfig {
                limit_bytes: 1000,
                pressure_threshold: 0.80,
                ..Default::default()
            },
            UsageSource::Reservations,
        ));
        guard.add_bytes(700); // 70%
        let src = MemoryPressureSource::new(Arc::clone(&guard));

        assert_eq!(src.name(), "memory");
        assert!(src.is_hard());
        assert!(approx(src.weight(), 1.0));
        assert!(
            approx(src.sample().get(), 0.70),
            "sample should mirror guard.pressure_ratio(), got {}",
            src.sample().get()
        );
    }

    const SECOND: u64 = 1_000_000_000;

    /// A latch with the default bound over one HARD `memory` source at
    /// `level`, timed on a clock the test moves.
    fn timed_latch(level: f64) -> (UnifiedPressure, Arc<MockSource>, Arc<AtomicU64>) {
        let mem = Arc::new(MockSource::new("memory", level, 1.0, true));
        let now = Arc::new(AtomicU64::new(0));
        let latch = UnifiedPressure::new(
            vec![Arc::clone(&mem) as Arc<dyn PressureSource>],
            Hysteresis::new(0.80, 0.65).expect("band"),
        )
        .with_manual_clock(Arc::clone(&now));
        (latch, mem, now)
    }

    /// The wedge: a burst arms the latch, the level settles in the band on
    /// memory the paused intake cannot free, and the hold ends at 30 s. The
    /// band then keeps the latch released.
    #[test]
    fn a_hold_the_level_never_clears_releases_at_max_hold() {
        let (latch, mem, now) = timed_latch(0.85);
        assert_eq!(
            latch.max_hold(),
            Duration::from_secs(30),
            "the default bound"
        );
        assert!(latch.should_hold(), "arms at 0.85");

        mem.set(0.70);
        now.store(30 * SECOND - 1, Ordering::Relaxed);
        assert!(latch.should_hold(), "in the band, 1 ns short of max_hold");

        now.store(30 * SECOND, Ordering::Relaxed);
        assert!(!latch.should_hold(), "30 s held at 0.70 admits");
        assert_eq!(latch.expired_holds(), 1);
        assert!(
            !latch.should_hold(),
            "0.70 is under pause_above, so the band keeps the latch released"
        );
        assert_eq!(latch.expired_holds(), 1, "one expiry per hold");
    }

    /// A level still at `pause_above` is admitted once per max_hold and held
    /// again on the evaluation after.
    #[test]
    fn a_level_still_high_rearms_on_the_next_evaluation() {
        let (latch, _mem, now) = timed_latch(0.85);
        assert!(latch.should_hold());

        now.store(30 * SECOND, Ordering::Relaxed);
        assert!(!latch.should_hold(), "one admission at max_hold");
        assert!(
            latch.should_hold(),
            "re-armed: 0.85 is still at pause_above"
        );

        now.store(60 * SECOND - 1, Ordering::Relaxed);
        assert!(latch.should_hold(), "the new hold started at 30 s");
        now.store(60 * SECOND, Ordering::Relaxed);
        assert!(!latch.should_hold(), "and ends 30 s later");
        assert_eq!(latch.expired_holds(), 2);
    }

    /// Falling to `resume_below` releases at once, as it always did, and is
    /// not counted as an expired hold.
    #[test]
    fn a_level_at_resume_below_releases_as_before() {
        let (latch, mem, now) = timed_latch(0.85);
        assert!(latch.should_hold());

        now.store(SECOND, Ordering::Relaxed);
        mem.set(0.65);
        assert!(!latch.should_hold(), "resume_below releases at 1 s");
        assert_eq!(latch.expired_holds(), 0);

        now.store(40 * SECOND, Ordering::Relaxed);
        mem.set(0.70);
        assert!(!latch.should_hold(), "released, and 0.70 does not re-arm");
    }

    /// `Duration::ZERO` is the unbounded latch.
    #[test]
    fn a_zero_max_hold_never_releases_by_time() {
        let (latch, mem, now) = timed_latch(0.85);
        let latch = latch.with_max_hold(Duration::ZERO);
        assert!(latch.should_hold());

        mem.set(0.70);
        now.store(10 * 24 * 3600 * SECOND, Ordering::Relaxed);
        assert!(latch.should_hold(), "ten days in the band, still held");
        assert_eq!(latch.expired_holds(), 0);
    }

    /// A caller that admits nothing sees an expired hold as held, and leaves
    /// the admission to the caller that admits work.
    #[test]
    fn a_non_admitting_evaluation_leaves_the_window_to_an_admitting_one() {
        let (latch, _mem, now) = timed_latch(0.85);
        assert!(latch.should_hold());

        now.store(30 * SECOND, Ordering::Relaxed);
        assert!(latch.should_hold_without_admitting());
        assert!(latch.should_hold_without_admitting());
        assert_eq!(latch.expired_holds(), 0);
        assert!(
            !latch.should_hold(),
            "the admitting caller still gets the window"
        );
    }

    /// Callers racing into one expired hold: exactly one is admitted.
    #[test]
    fn exactly_one_racing_caller_takes_an_expired_hold() {
        const CALLERS: usize = 8;
        for round in 0..100 {
            let (latch, _mem, now) = timed_latch(0.85);
            assert!(latch.should_hold());
            now.store(30 * SECOND, Ordering::Relaxed);

            let start = std::sync::Barrier::new(CALLERS);
            let admitted = std::sync::atomic::AtomicUsize::new(0);
            std::thread::scope(|s| {
                for _ in 0..CALLERS {
                    s.spawn(|| {
                        start.wait();
                        if !latch.should_hold() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            });
            assert_eq!(admitted.load(Ordering::Relaxed), 1, "round {round}");
            assert_eq!(latch.expired_holds(), 1, "round {round}");
        }
    }

    /// The latch writes its level while it holds, and counts an expired hold
    /// under the source that set the level.
    #[cfg(feature = "metrics")]
    #[test]
    fn the_latch_publishes_its_level_and_counts_expired_holds() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = ::metrics::set_default_local_recorder(&recorder);
        let series = |prefix: &str| {
            handle
                .render()
                .lines()
                .find_map(|line| line.strip_prefix(prefix)?.parse::<f64>().ok())
        };

        let (latch, mem, now) = timed_latch(0.85);
        assert!(latch.should_hold());
        mem.set(0.70);
        assert!(latch.should_hold());
        assert_eq!(
            series("self_regulation_pressure_ratio "),
            Some(0.70),
            "the level while held"
        );

        now.store(30 * SECOND, Ordering::Relaxed);
        assert!(!latch.should_hold());
        assert_eq!(
            series("self_regulation_max_hold_releases_total{signal=\"memory\"} "),
            Some(1.0)
        );
    }
}
