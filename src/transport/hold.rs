// Project:   scalo
// File:      src/transport/hold.rs
// Purpose:   How long a hop holds its sender's answer, and the deadline it gives the next hop
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! How long a hop holds its sender's answer, and the deadline it gives the
//! next hop.
//!
//! A hop that answers its sender only once the next hop has taken the records
//! must answer before that sender gives up. So each hop holds for less time
//! than the hop in front of it waits, and waits on the hop behind it for less
//! time than it holds. [`HoldLadder`] derives both numbers for every depth in a
//! chain of such hops from one top hold and one step, so no hop copies its
//! neighbour's number.

use std::time::Duration;

/// The least headroom kept below a sender's deadline.
const MIN_MARGIN: Duration = Duration::from_secs(1);

/// The longest a response is held: `max_hold`, or the sender's deadline less a
/// margin of a tenth of it, at least 1 s and at most half the deadline.
///
/// The budget an armed gRPC server gives each push, for an app's own listener
/// to give the same: read the deadline with `grpc::sender_deadline`, and answer
/// before the budget runs out, since tonic cuts a handler at the sender's
/// deadline with `Cancelled`, which a sender cannot tell from a crash.
///
/// ```
/// use std::time::Duration;
/// use scalo::transport::hold::hold_budget;
///
/// let max_hold = Duration::from_secs(25);
/// assert_eq!(hold_budget(max_hold, None), max_hold);
/// // A 10 s deadline keeps a 1 s margin.
/// assert_eq!(hold_budget(max_hold, Some(Duration::from_secs(10))), Duration::from_secs(9));
/// ```
#[must_use]
pub fn hold_budget(max_hold: Duration, sender_deadline: Option<Duration>) -> Duration {
    let Some(deadline) = sender_deadline else {
        return max_hold;
    };
    let margin = (deadline / 10).max(MIN_MARGIN).min(deadline / 2);
    max_hold.min(deadline.saturating_sub(margin))
}

/// Hold times and next-hop deadlines for a chain of hops that each hold their
/// sender's answer until the next hop has the records.
///
/// The hop at depth 0 holds for `top` and gives the next hop `top - step`. The
/// hop at depth `k` gives the next hop `top - step * (k + 1)`, and holds for
/// the [`hold_budget`] of the deadline the hop in front of it gives. Every hop
/// therefore holds for less time than the hop in front of it waits, and waits
/// for less time than it holds.
///
/// A hop serving flows of different lengths takes the depth of its role in the
/// longest of them and uses it for all: a hold shorter than the sender's
/// deadline is always safe.
///
/// ```
/// use std::time::Duration;
/// use scalo::transport::hold::HoldLadder;
///
/// let ingress = HoldLadder::DEFAULT.rung(0).expect("depth 0");
/// assert_eq!(ingress.hold, Duration::from_secs(25));
/// assert_eq!(ingress.next_hop_deadline, Duration::from_secs(20));
///
/// let middle = HoldLadder::DEFAULT.rung(1).expect("depth 1");
/// assert_eq!(middle.hold, Duration::from_secs(18));
/// assert_eq!(middle.next_hop_deadline, Duration::from_secs(15));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HoldLadder {
    top: Duration,
    step: Duration,
}

/// One hop's place on a [`HoldLadder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Rung {
    /// The longest this hop holds its sender's answer.
    pub hold: Duration,
    /// The deadline this hop gives the next hop: a send timeout, or a
    /// producer's delivery timeout.
    pub next_hop_deadline: Duration,
}

/// Why [`HoldLadder::new`] refused a ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HoldLadderError {
    /// A step no longer than the margin a hold keeps below its sender's
    /// deadline would let a hop hold past its own next-hop deadline.
    #[error("hold ladder step {step:?} must exceed {min:?} for a {top:?} top hold")]
    StepTooShort {
        /// The top hold asked for.
        top: Duration,
        /// The step asked for.
        step: Duration,
        /// The step must be longer than this.
        min: Duration,
    },
    /// A top hold no longer than the step leaves no hop a next-hop deadline.
    #[error("hold ladder top {top:?} must exceed its step {step:?}")]
    NoRung {
        /// The top hold asked for.
        top: Duration,
        /// The step asked for.
        step: Duration,
    },
}

impl HoldLadder {
    /// A 25 s top hold, each depth's next-hop deadline 5 s shorter than the
    /// last: four depths.
    pub const DEFAULT: Self = Self {
        top: Duration::from_secs(25),
        step: Duration::from_secs(5),
    };

    /// A ladder whose depth-0 hop holds for `top`, each depth's next-hop
    /// deadline `step` shorter than the one before.
    ///
    /// `step` must be longer than a tenth of `top` and longer than 1 s, the
    /// most margin [`hold_budget`] keeps below any deadline up to `top`, and
    /// `top` must be longer than `step`.
    pub fn new(top: Duration, step: Duration) -> Result<Self, HoldLadderError> {
        let min = (top / 10).max(MIN_MARGIN);
        if step <= min {
            return Err(HoldLadderError::StepTooShort { top, step, min });
        }
        if top <= step {
            return Err(HoldLadderError::NoRung { top, step });
        }
        Ok(Self { top, step })
    }

    /// The hold of the depth-0 hop.
    #[must_use]
    pub const fn top(&self) -> Duration {
        self.top
    }

    /// How much shorter each depth's next-hop deadline is than the one before.
    #[must_use]
    pub const fn step(&self) -> Duration {
        self.step
    }

    /// The hold and next-hop deadline of the hop at `depth`, or `None` past
    /// the last depth whose next-hop deadline is above zero.
    #[must_use]
    pub fn rung(&self, depth: usize) -> Option<Rung> {
        let next_hop_deadline = self.next_hop_deadline(depth)?;
        let hold = match depth.checked_sub(1) {
            None => self.top,
            Some(front) => hold_budget(self.top, Some(self.next_hop_deadline(front)?)),
        };
        Some(Rung {
            hold,
            next_hop_deadline,
        })
    }

    /// Every rung, from depth 0 to the last.
    pub fn rungs(&self) -> impl Iterator<Item = Rung> + use<> {
        let ladder = *self;
        (0..).map_while(move |depth| ladder.rung(depth))
    }

    /// The deadline the hop at `depth` gives the next hop, while above zero.
    fn next_hop_deadline(&self, depth: usize) -> Option<Duration> {
        let steps = u32::try_from(depth.checked_add(1)?).ok()?;
        let deadline = self.top.checked_sub(self.step.checked_mul(steps)?)?;
        (!deadline.is_zero()).then_some(deadline)
    }
}

impl Default for HoldLadder {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// Every hop holds for less time than the hop in front of it waits, waits
    /// for less time than it holds, and keeps its whole hold under the budget
    /// the front hop's deadline gives it.
    #[test]
    fn every_rung_holds_inside_the_deadline_in_front_and_past_its_own() {
        for (top, step, depths) in [
            (25_000, 5_000, 4),
            (60_000, 7_000, 8),
            (120_000, 13_000, 9),
            (10_000, 2_000, 4),
            (5_000, 1_500, 3),
        ] {
            let ladder = HoldLadder::new(ms(top), ms(step)).unwrap();
            let rungs: Vec<Rung> = ladder.rungs().collect();
            assert_eq!(rungs.len(), depths, "{ladder:?}");
            assert_eq!(rungs[0].hold, ladder.top(), "{ladder:?}");
            for (depth, rung) in rungs.iter().enumerate() {
                assert!(
                    rung.hold > rung.next_hop_deadline,
                    "{ladder:?} depth {depth}: {rung:?}"
                );
                assert!(
                    !rung.next_hop_deadline.is_zero(),
                    "{ladder:?} depth {depth}"
                );
            }
            for (depth, pair) in rungs.windows(2).enumerate() {
                let (front, behind) = (pair[0], pair[1]);
                assert!(
                    behind.hold < front.next_hop_deadline,
                    "{ladder:?} depth {}: {behind:?} under {front:?}",
                    depth + 1
                );
                assert_eq!(
                    hold_budget(behind.hold, Some(front.next_hop_deadline)),
                    behind.hold,
                    "{ladder:?} depth {}: the sender's deadline cuts the hold short",
                    depth + 1
                );
                assert_eq!(
                    front
                        .next_hop_deadline
                        .checked_sub(behind.next_hop_deadline),
                    Some(ladder.step()),
                    "{ladder:?} depth {}",
                    depth + 1
                );
            }
        }
    }

    /// The default ladder's first three depths hold 25 s with a 20 s next-hop
    /// deadline, 18 s with 15 s, then 13.5 s.
    #[test]
    fn the_default_ladder_keeps_the_three_depth_values() {
        assert_eq!(HoldLadder::default(), HoldLadder::DEFAULT);
        let ladder = HoldLadder::DEFAULT;

        let ingress = ladder.rung(0).unwrap();
        assert_eq!(ingress.hold, ms(25_000));
        assert_eq!(ingress.next_hop_deadline, ms(20_000));

        let middle = ladder.rung(1).unwrap();
        assert_eq!(middle.hold, ms(18_000));
        assert_eq!(middle.next_hop_deadline, ms(15_000));

        let sink = ladder.rung(2).unwrap();
        assert_eq!(sink.hold, ms(13_500));
    }

    /// A hop that keeps its depth in a flow with a stage missing in front of it
    /// holds for less than the deadline its nearer sender gives.
    #[test]
    fn a_flow_with_a_stage_missing_keeps_every_hold_inside_its_sender() {
        let ladder = HoldLadder::DEFAULT;
        let ingress = ladder.rung(0).unwrap();
        let sink = ladder.rung(2).unwrap();
        assert!(sink.hold < ingress.next_hop_deadline);
        assert_eq!(
            hold_budget(sink.hold, Some(ingress.next_hop_deadline)),
            sink.hold,
            "the shorter sink hold is kept whole"
        );
    }

    #[test]
    fn a_step_inside_the_hold_margin_is_refused() {
        for (top, step, min) in [
            (25_000, 2_500, 2_500),
            (25_000, 1_000, 2_500),
            (5_000, 1_000, 1_000),
            (5_000, 0, 1_000),
            (120_000, 12_000, 12_000),
        ] {
            assert_eq!(
                HoldLadder::new(ms(top), ms(step)),
                Err(HoldLadderError::StepTooShort {
                    top: ms(top),
                    step: ms(step),
                    min: ms(min),
                }),
                "top {top} ms, step {step} ms"
            );
        }
    }

    #[test]
    fn a_top_no_longer_than_its_step_is_refused() {
        for (top, step) in [(5_000, 5_000), (5_000, 6_000), (0, 2_000)] {
            assert_eq!(
                HoldLadder::new(ms(top), ms(step)),
                Err(HoldLadderError::NoRung {
                    top: ms(top),
                    step: ms(step),
                }),
                "top {top} ms, step {step} ms"
            );
        }
        assert_eq!(
            HoldLadder::new(ms(25_000), ms(5_000)),
            Ok(HoldLadder::DEFAULT)
        );
    }

    #[test]
    fn a_depth_past_the_last_rung_has_none() {
        let ladder = HoldLadder::DEFAULT;
        assert!(ladder.rung(3).is_some());
        assert_eq!(ladder.rung(4), None, "a 0 s next-hop deadline");
        assert_eq!(ladder.rung(usize::MAX), None);
        assert_eq!(ladder.rungs().count(), 4);
    }
}
