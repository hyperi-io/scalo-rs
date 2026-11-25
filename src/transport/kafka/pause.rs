// Project:   scalo
// File:      src/transport/kafka/pause.rs
// Purpose:   Partitions a caller holds paused, composed with the inbound gate's hold
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Partitions a caller holds paused, and how that composes with the inbound
//! gate, which pauses the whole assignment.
//!
//! A partition is fetched only while neither holds it. The gate's resume
//! leaves the caller's partitions paused, and a caller's resume while the
//! gate holds leaves the partition to the gate's. librdkafka forgets a pause
//! across a rebalance, so a revoke drops the caller's hold and an assignment
//! starts unpaused.

use std::collections::HashSet;
use std::sync::Arc;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionList;

use super::metrics::{Rebalanced, StatsContext};

/// A partition, keyed as the tokens carry it.
pub(super) type PartitionKey = (Arc<str>, i32);

/// The call a [`PartitionControlError`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionOp {
    /// `KafkaTransport::pause_partitions`.
    Pause,
    /// `KafkaTransport::resume_partitions`.
    Resume,
    /// `KafkaTransport::seek_partition`.
    Seek,
}

impl PartitionOp {
    /// The `op` label this call is counted under.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Seek => "seek",
        }
    }
}

impl std::fmt::Display for PartitionOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a pause, resume or seek took no effect. Each is counted in
/// `transport_partition_control_errors_total{transport="kafka",op,reason}`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PartitionControlError {
    /// The transport was built to send only.
    #[error("kafka {op}: this transport only sends, so it has no consumer")]
    NoConsumer {
        /// The call refused.
        op: PartitionOp,
    },
    /// This consumer does not hold the partition: never assigned, or revoked.
    #[error("kafka {op}: this consumer does not hold {topic}/{partition}")]
    NotAssigned {
        /// The call refused.
        op: PartitionOp,
        /// The partition's topic.
        topic: String,
        /// The partition.
        partition: i32,
    },
    /// A seek to a negative offset, which librdkafka reads as a logical one.
    #[error("kafka seek: {offset} is not an offset of {topic}/{partition}")]
    InvalidOffset {
        /// The partition's topic.
        topic: String,
        /// The partition.
        partition: i32,
        /// The offset asked for.
        offset: i64,
    },
    /// librdkafka refused the call, or the seek did not finish in time.
    #[error("kafka {op}: librdkafka refused it: {detail}")]
    Refused {
        /// The call refused.
        op: PartitionOp,
        /// librdkafka's reason.
        detail: String,
    },
}

impl PartitionControlError {
    /// The call this error came from.
    #[must_use]
    pub const fn op(&self) -> PartitionOp {
        match self {
            Self::NoConsumer { op } | Self::NotAssigned { op, .. } | Self::Refused { op, .. } => {
                *op
            }
            Self::InvalidOffset { .. } => PartitionOp::Seek,
        }
    }

    /// The `reason` label this error is counted under.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::NoConsumer { .. } => "no_consumer",
            Self::NotAssigned { .. } => "not_assigned",
            Self::InvalidOffset { .. } => "invalid_offset",
            Self::Refused { .. } => "refused",
        }
    }

    /// librdkafka's refusal of `op`.
    pub(super) fn refused(op: PartitionOp, error: &KafkaError) -> Self {
        Self::Refused {
            op,
            detail: error.to_string(),
        }
    }
}

/// Count `error`, and hand it back for the caller to return.
pub(super) fn counted(error: PartitionControlError) -> PartitionControlError {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(
        "transport_partition_control_errors_total",
        "transport" => "kafka",
        "op" => error.op().as_str(),
        "reason" => error.reason()
    )
    .increment(1);
    error
}

/// The librdkafka calls pausing makes, so the composition is tested without a broker.
pub(super) trait PauseTarget {
    /// Whether the consumer holds the partition now.
    fn holds(&self, topic: &str, partition: i32) -> bool;
    /// Every partition the consumer holds now.
    #[cfg_attr(
        not(feature = "governor"),
        allow(
            dead_code,
            reason = "only the inbound gate pauses the whole assignment"
        )
    )]
    fn assignment(&self) -> Result<Vec<PartitionKey>, KafkaError>;
    fn pause(&self, partitions: &[PartitionKey]) -> Result<(), KafkaError>;
    fn resume(&self, partitions: &[PartitionKey]) -> Result<(), KafkaError>;
    /// Record whether a caller holds any partition paused, so lag keeps rising.
    fn set_held(&self, held: bool);
}

fn topic_partition_list(partitions: &[PartitionKey]) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for (topic, partition) in partitions {
        tpl.add_partition(topic, *partition);
    }
    tpl
}

impl PauseTarget for BaseConsumer<StatsContext> {
    fn holds(&self, topic: &str, partition: i32) -> bool {
        self.context().assignment(topic, partition).is_some()
    }

    fn assignment(&self) -> Result<Vec<PartitionKey>, KafkaError> {
        Ok(Consumer::assignment(self)?
            .elements()
            .iter()
            .map(|e| (Arc::<str>::from(e.topic()), e.partition()))
            .collect())
    }

    fn pause(&self, partitions: &[PartitionKey]) -> Result<(), KafkaError> {
        Consumer::pause(self, &topic_partition_list(partitions))
    }

    fn resume(&self, partitions: &[PartitionKey]) -> Result<(), KafkaError> {
        Consumer::resume(self, &topic_partition_list(partitions))
    }

    fn set_held(&self, held: bool) {
        self.context().set_partitions_held(held);
    }
}

/// Who holds what paused. Every change runs under the one lock, so a gate
/// edge and a caller's call never interleave.
#[derive(Debug, Default)]
struct Holds {
    /// The inbound gate holds the whole assignment.
    gate: bool,
    /// Partitions the caller holds.
    caller: HashSet<PartitionKey>,
}

/// The pause state a transport and its gate actuator share.
#[derive(Debug, Default)]
pub(super) struct PauseState {
    holds: parking_lot::Mutex<Holds>,
}

fn keys(partitions: &[(&str, i32)]) -> Vec<PartitionKey> {
    partitions
        .iter()
        .map(|&(topic, partition)| (Arc::<str>::from(topic), partition))
        .collect()
}

impl PauseState {
    /// Hold `partitions` paused for the caller. All or none: every one must
    /// be held by this consumer.
    pub(super) fn pause(
        &self,
        target: &impl PauseTarget,
        partitions: &[(&str, i32)],
    ) -> Result<(), PartitionControlError> {
        let mut holds = self.holds.lock();
        if let Some(&(topic, partition)) = partitions.iter().find(|(t, p)| !target.holds(t, *p)) {
            return Err(PartitionControlError::NotAssigned {
                op: PartitionOp::Pause,
                topic: topic.to_string(),
                partition,
            });
        }
        let wanted = keys(partitions);
        if wanted.is_empty() {
            return Ok(());
        }
        target
            .pause(&wanted)
            .map_err(|e| PartitionControlError::refused(PartitionOp::Pause, &e))?;
        holds.caller.extend(wanted);
        target.set_held(true);
        Ok(())
    }

    /// Release the caller's hold on `partitions`. One the gate holds stays
    /// paused until the gate's resume. A refused resume keeps the hold, so
    /// the call can be made again.
    pub(super) fn resume(
        &self,
        target: &impl PauseTarget,
        partitions: &[(&str, i32)],
    ) -> Result<(), PartitionControlError> {
        let mut holds = self.holds.lock();
        let released: Vec<PartitionKey> = keys(partitions)
            .into_iter()
            .filter(|key| holds.caller.contains(key))
            .collect();
        if released.is_empty() {
            return Ok(());
        }
        if !holds.gate {
            target
                .resume(&released)
                .map_err(|e| PartitionControlError::refused(PartitionOp::Resume, &e))?;
        }
        for key in &released {
            holds.caller.remove(key);
        }
        target.set_held(!holds.caller.is_empty());
        Ok(())
    }

    /// The gate's rising edge: pause the whole assignment.
    #[cfg_attr(
        not(feature = "governor"),
        allow(dead_code, reason = "the inbound gate is the governor's")
    )]
    pub(super) fn gate_pause(&self, target: &impl PauseTarget) -> Result<(), KafkaError> {
        let mut holds = self.holds.lock();
        holds.gate = true;
        let assigned = target.assignment()?;
        if assigned.is_empty() {
            return Ok(());
        }
        target.pause(&assigned)
    }

    /// The gate's falling edge: resume the assignment, less what the caller holds.
    #[cfg_attr(
        not(feature = "governor"),
        allow(dead_code, reason = "the inbound gate is the governor's")
    )]
    pub(super) fn gate_resume(&self, target: &impl PauseTarget) -> Result<(), KafkaError> {
        let mut holds = self.holds.lock();
        holds.gate = false;
        let free: Vec<PartitionKey> = target
            .assignment()?
            .into_iter()
            .filter(|key| !holds.caller.contains(key))
            .collect();
        if free.is_empty() {
            return Ok(());
        }
        target.resume(&free)
    }

    /// Drop the caller's hold on each partition a rebalance revoked.
    pub(super) fn rebalanced(&self, target: &impl PauseTarget, changes: &[(u64, Rebalanced)]) {
        if changes.is_empty() {
            return;
        }
        let mut holds = self.holds.lock();
        if holds.caller.is_empty() {
            return;
        }
        for (_, change) in changes {
            if let Rebalanced::Revoked(lost) = change {
                for (topic, partition) in lost {
                    holds
                        .caller
                        .remove(&(Arc::<str>::from(topic.as_str()), *partition));
                }
            }
        }
        target.set_held(!holds.caller.is_empty());
    }

    /// Forget every hold: a rebuilt consumer starts with nothing paused.
    pub(super) fn clear(&self) {
        self.holds.lock().caller.clear();
    }

    /// The partitions the caller holds, in topic then partition order.
    pub(super) fn held(&self) -> Vec<PartitionKey> {
        let mut held: Vec<PartitionKey> = self.holds.lock().caller.iter().cloned().collect();
        held.sort_unstable();
        held
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};

    /// A consumer that records what is paused, as librdkafka keeps it.
    #[derive(Default)]
    struct Fake {
        assigned: Vec<PartitionKey>,
        paused: parking_lot::Mutex<HashSet<PartitionKey>>,
        refuse: bool,
        held: AtomicBool,
    }

    impl Fake {
        fn assigned(partitions: &[i32]) -> Self {
            Self {
                assigned: partitions.iter().map(|&p| (Arc::from("t"), p)).collect(),
                ..Self::default()
            }
        }

        fn paused(&self) -> Vec<i32> {
            let mut paused: Vec<i32> = self.paused.lock().iter().map(|(_, p)| *p).collect();
            paused.sort_unstable();
            paused
        }

        fn refused() -> KafkaError {
            KafkaError::PauseResume("refused".into())
        }
    }

    impl PauseTarget for Fake {
        fn holds(&self, topic: &str, partition: i32) -> bool {
            self.assigned
                .iter()
                .any(|(t, p)| &**t == topic && *p == partition)
        }

        fn assignment(&self) -> Result<Vec<PartitionKey>, KafkaError> {
            Ok(self.assigned.clone())
        }

        fn pause(&self, partitions: &[PartitionKey]) -> Result<(), KafkaError> {
            if self.refuse {
                return Err(Self::refused());
            }
            self.paused.lock().extend(partitions.iter().cloned());
            Ok(())
        }

        fn resume(&self, partitions: &[PartitionKey]) -> Result<(), KafkaError> {
            if self.refuse {
                return Err(Self::refused());
            }
            let mut paused = self.paused.lock();
            for key in partitions {
                paused.remove(key);
            }
            Ok(())
        }

        fn set_held(&self, held: bool) {
            self.held.store(held, Ordering::Relaxed);
        }
    }

    fn revoked(partitions: &[i32]) -> (u64, Rebalanced) {
        let lost = partitions.iter().map(|&p| ("t".to_string(), p)).collect();
        (1, Rebalanced::Revoked(lost))
    }

    #[test]
    fn a_caller_pause_and_resume_move_only_its_partitions() {
        let consumer = Fake::assigned(&[0, 1, 2]);
        let state = PauseState::default();
        state.pause(&consumer, &[("t", 1)]).expect("pause");
        assert_eq!(consumer.paused(), vec![1]);
        assert_eq!(state.held(), vec![(Arc::<str>::from("t"), 1)]);
        assert!(consumer.held.load(Ordering::Relaxed));

        state.resume(&consumer, &[("t", 1)]).expect("resume");
        assert_eq!(consumer.paused(), Vec::<i32>::new());
        assert_eq!(state.held(), Vec::<PartitionKey>::new());
        assert!(!consumer.held.load(Ordering::Relaxed));
    }

    type Steps = fn(&PauseState, &Fake);

    #[test]
    fn the_gate_and_the_caller_never_resume_what_the_other_holds() {
        // (case, steps, partitions still paused)
        let cases: [(&str, Steps, Vec<i32>); 4] = [
            (
                "the gate's resume leaves the caller's partition paused",
                |s, c| {
                    s.gate_pause(c).expect("gate pause");
                    s.pause(c, &[("t", 1)]).expect("pause");
                    s.gate_resume(c).expect("gate resume");
                },
                vec![1],
            ),
            (
                "the caller's resume under the gate leaves it to the gate",
                |s, c| {
                    s.pause(c, &[("t", 1)]).expect("pause");
                    s.gate_pause(c).expect("gate pause");
                    s.resume(c, &[("t", 1)]).expect("resume");
                },
                vec![0, 1, 2],
            ),
            (
                "once both have resumed nothing is paused",
                |s, c| {
                    s.pause(c, &[("t", 1)]).expect("pause");
                    s.gate_pause(c).expect("gate pause");
                    s.resume(c, &[("t", 1)]).expect("resume");
                    s.gate_resume(c).expect("gate resume");
                },
                vec![],
            ),
            (
                "a gate pause and resume with no caller hold resumes everything",
                |s, c| {
                    s.gate_pause(c).expect("gate pause");
                    s.gate_resume(c).expect("gate resume");
                },
                vec![],
            ),
        ];
        for (case, steps, still_paused) in cases {
            let consumer = Fake::assigned(&[0, 1, 2]);
            let state = PauseState::default();
            steps(&state, &consumer);
            assert_eq!(consumer.paused(), still_paused, "{case}");
        }
    }

    #[test]
    fn a_partition_this_consumer_does_not_hold_is_refused_whole() {
        let consumer = Fake::assigned(&[0]);
        let state = PauseState::default();
        let refused = state
            .pause(&consumer, &[("t", 0), ("t", 7)])
            .expect_err("partition 7 is not assigned");
        assert_eq!(
            refused,
            PartitionControlError::NotAssigned {
                op: PartitionOp::Pause,
                topic: "t".into(),
                partition: 7
            }
        );
        assert!(consumer.paused().is_empty(), "nothing is paused");
        assert_eq!(state.held(), Vec::<PartitionKey>::new());
    }

    #[test]
    fn a_refused_call_changes_no_hold() {
        let state = PauseState::default();
        let refusing = Fake {
            refuse: true,
            ..Fake::assigned(&[0])
        };
        let paused = state.pause(&refusing, &[("t", 0)]);
        assert!(matches!(
            paused,
            Err(PartitionControlError::Refused {
                op: PartitionOp::Pause,
                ..
            })
        ));
        assert!(state.held().is_empty(), "a refused pause holds nothing");

        let consumer = Fake::assigned(&[0]);
        state.pause(&consumer, &[("t", 0)]).expect("pause");
        let resumed = state.resume(&refusing, &[("t", 0)]);
        assert!(matches!(
            resumed,
            Err(PartitionControlError::Refused {
                op: PartitionOp::Resume,
                ..
            })
        ));
        assert_eq!(
            state.held(),
            vec![(Arc::<str>::from("t"), 0)],
            "a refused resume keeps the hold, so the call can be made again"
        );
        state.resume(&consumer, &[("t", 0)]).expect("resume again");
        assert_eq!(state.held(), Vec::<PartitionKey>::new());
    }

    #[test]
    fn a_revoke_drops_the_hold_and_an_assignment_starts_unpaused() {
        let consumer = Fake::assigned(&[0, 1]);
        let state = PauseState::default();
        state
            .pause(&consumer, &[("t", 0), ("t", 1)])
            .expect("pause");
        state.rebalanced(
            &consumer,
            &[
                revoked(&[0]),
                (2, Rebalanced::Assigned(vec![("t".to_string(), 0)])),
            ],
        );
        assert_eq!(
            state.held(),
            vec![(Arc::<str>::from("t"), 1)],
            "the revoked partition's hold went, the assignment added none"
        );
        state.rebalanced(&consumer, &[revoked(&[1])]);
        assert_eq!(state.held(), Vec::<PartitionKey>::new());
        assert!(!consumer.held.load(Ordering::Relaxed));
    }

    #[test]
    fn a_resume_of_a_partition_never_paused_calls_nothing() {
        let state = PauseState::default();
        let refusing = Fake {
            refuse: true,
            ..Fake::assigned(&[0])
        };
        state
            .resume(&refusing, &[("t", 0)])
            .expect("nothing to resume, so librdkafka is not asked");
    }

    #[test]
    fn each_error_names_its_call() {
        let cases = [
            (
                PartitionControlError::NoConsumer {
                    op: PartitionOp::Resume,
                },
                PartitionOp::Resume,
                "no_consumer",
            ),
            (
                PartitionControlError::InvalidOffset {
                    topic: "t".into(),
                    partition: 0,
                    offset: -1,
                },
                PartitionOp::Seek,
                "invalid_offset",
            ),
            (
                PartitionControlError::refused(PartitionOp::Seek, &Fake::refused()),
                PartitionOp::Seek,
                "refused",
            ),
        ];
        for (error, op, reason) in cases {
            assert_eq!(error.op(), op, "{error}");
            assert_eq!(error.reason(), reason, "{error}");
        }
    }
}
