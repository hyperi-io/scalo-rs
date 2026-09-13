// Project:   scalo
// File:      src/transport/kafka/classify.rs
// Purpose:   Produce-failure policy shared by every Kafka send path
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Produce-failure policy: how a failed Kafka send is classified, and how it is
//! reported without flooding the log.
//!
//! Every send path -- the transport's `send`, the standalone
//! [`KafkaProducer`](super::KafkaProducer), and both delivery callbacks -- asks
//! the same question of a failure: retry the same message, retry later, or give
//! up. They answer it here so the paths cannot drift.
//!
//! The classification reads [`KafkaError::rdkafka_error_code`], never the
//! rendered message. `Cargo.toml` pins `rdkafka` with `dynamic-linking`, so the
//! error STRING comes from whichever system librdkafka the image ships and can
//! change under a base-image bump; the error CODE is a stable protocol constant.

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// What a caller should do about a failed Kafka produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendFailure {
    /// The local producer queue is full. Drain it and re-offer the message.
    QueueFull,
    /// The record is over the message-size ceiling. No retry and no ceiling the
    /// broker currently carries will take it, so dead-letter the record and
    /// keep the transport.
    TooLarge,
    /// A transient broker, leader or network condition. The message is still
    /// deliverable, so the caller retries rather than dropping it.
    Retryable,
    /// Retrying cannot help -- authorisation or a permanent topic error.
    Fatal,
}

/// Classify a produce-side failure by its librdkafka error code.
pub(crate) fn classify_send_failure(err: &KafkaError) -> SendFailure {
    match err.rdkafka_error_code() {
        Some(RDKafkaErrorCode::QueueFull) => SendFailure::QueueFull,
        Some(RDKafkaErrorCode::MessageSizeTooLarge) => SendFailure::TooLarge,
        Some(code) if is_retryable(code) => SendFailure::Retryable,
        _ => SendFailure::Fatal,
    }
}

/// Codes that clear without operator action, so the message is worth retrying.
///
/// Mirrors Kafka's own retriable-error set plus the librdkafka-local transport
/// and timeout codes. `UnknownTopic` / `UnknownTopicOrPartition` is deliberately
/// absent: treating a missing topic as retryable hides a misconfigured
/// destination behind a retry loop.
fn is_retryable(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        // librdkafka-local: transport, DNS and timeout conditions.
        RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::Resolve
            | RDKafkaErrorCode::MessageTimedOut
            | RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::ISRInsufficient
            // Broker-side: leadership, replication and coordinator churn.
            | RDKafkaErrorCode::LeaderNotAvailable
            | RDKafkaErrorCode::NotLeaderForPartition
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::BrokerNotAvailable
            | RDKafkaErrorCode::ReplicaNotAvailable
            | RDKafkaErrorCode::NetworkException
            | RDKafkaErrorCode::CoordinatorNotAvailable
            | RDKafkaErrorCode::NotCoordinator
            | RDKafkaErrorCode::NotEnoughReplicas
            | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
            | RDKafkaErrorCode::KafkaStorageError
            | RDKafkaErrorCode::FencedLeaderEpoch
            | RDKafkaErrorCode::UnknownLeaderEpoch
            | RDKafkaErrorCode::ThrottlingQuotaExceeded
    )
}

/// Edge latch for a sustained failure, so an outage logs once instead of once
/// per record.
///
/// Every send path here runs per message at PB/day rates, so the failure branch
/// reports on the transition and leaves the per-message count to the metric.
#[derive(Debug, Default)]
pub(crate) struct DegradedLatch(AtomicBool);

impl DegradedLatch {
    /// True only on the transition INTO the failing state.
    pub(crate) fn enter(&self) -> bool {
        !self.0.swap(true, Ordering::Relaxed)
    }

    /// True only on the transition OUT of the failing state. Loads before it
    /// stores: the success path is per-record and a load is the cheaper half.
    pub(crate) fn clear(&self) -> bool {
        self.0.load(Ordering::Relaxed) && self.0.swap(false, Ordering::Relaxed)
    }
}

/// Broker-side delivery outcomes, recorded from librdkafka's poll thread.
///
/// A produce call only reports that a message was QUEUED. Whether the broker
/// took it arrives asynchronously in the delivery callback, so a context that
/// discards that callback reports full throughput while losing records.
#[derive(Debug, Default)]
pub(crate) struct DeliveryState {
    failures: AtomicU64,
    degraded: DegradedLatch,
}

impl DeliveryState {
    /// Count every failed delivery; report the first of a run.
    pub(crate) fn record_failure(&self, err: &KafkaError, topic: &str, partition: i32) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "metrics")]
        ::metrics::counter!("transport_send_errors_total", "transport" => "kafka").increment(1);
        if self.degraded.enter() {
            tracing::warn!(
                topic,
                partition,
                class = ?classify_send_failure(err),
                error = %err,
                "kafka delivery failed"
            );
        }
    }

    /// Report the first success after a run of failures.
    pub(crate) fn record_success(&self) {
        if self.degraded.clear() {
            tracing::info!("kafka delivery recovered");
        }
    }

    /// Deliveries the broker refused or never acknowledged.
    pub(crate) fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Record a delivery report of either outcome.
    pub(crate) fn record(&self, result: &rdkafka::producer::DeliveryResult<'_>) {
        use rdkafka::message::Message as _;
        match result {
            Ok(_) => self.record_success(),
            Err((err, msg)) => self.record_failure(err, msg.topic(), msg.partition()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_failures_are_counted_not_discarded() {
        let state = DeliveryState::default();
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed);

        assert_eq!(state.failures(), 0);
        state.record_failure(&err, "events.land", 0);
        state.record_failure(&err, "events.land", 1);
        assert_eq!(
            state.failures(),
            2,
            "every failed delivery must be counted -- a discarded report is a \
             lost record reported as throughput"
        );
    }

    #[test]
    fn delivery_state_latches_the_outage_and_clears_on_recovery() {
        let state = DeliveryState::default();
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::BrokerTransportFailure);

        state.record_failure(&err, "events.land", 0);
        state.record_failure(&err, "events.land", 0);
        assert!(
            !state.degraded.enter(),
            "a run of failures stays latched after the first"
        );
        state.record_success();
        assert!(state.degraded.enter(), "recovery cleared the latch");
        assert_eq!(state.failures(), 2);
    }

    #[test]
    fn degraded_latch_fires_once_per_transition() {
        let latch = DegradedLatch::default();
        assert!(latch.enter(), "first failure is the transition");
        assert!(!latch.enter(), "a sustained outage must not re-log");
        assert!(!latch.enter());
        assert!(latch.clear(), "recovery is a transition too");
        assert!(!latch.clear(), "a healthy run must not log per record");
        assert!(latch.enter(), "the latch re-arms after recovery");
    }

    #[test]
    fn queue_full_is_its_own_class() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull);
        assert_eq!(classify_send_failure(&err), SendFailure::QueueFull);
    }

    #[test]
    fn an_oversize_record_is_its_own_class() {
        // Classing it fatal tears the transport down over one poison record;
        // classing it retryable loops on a record no retry can deliver.
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
        assert_eq!(classify_send_failure(&err), SendFailure::TooLarge);
    }

    #[test]
    fn transient_broker_conditions_are_retryable_not_fatal() {
        // Reporting any of these as fatal drops a deliverable message in an
        // at-least-once pipeline.
        for code in [
            RDKafkaErrorCode::MessageTimedOut,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::LeaderNotAvailable,
            RDKafkaErrorCode::NotLeaderForPartition,
            RDKafkaErrorCode::RequestTimedOut,
            RDKafkaErrorCode::NotEnoughReplicas,
            RDKafkaErrorCode::CoordinatorNotAvailable,
            RDKafkaErrorCode::ThrottlingQuotaExceeded,
        ] {
            let err = KafkaError::MessageProduction(code);
            assert_eq!(
                classify_send_failure(&err),
                SendFailure::Retryable,
                "{code:?} should be retryable"
            );
        }
    }

    #[test]
    fn permanent_conditions_stay_fatal() {
        for code in [
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::ClusterAuthorizationFailed,
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::InvalidArgument,
        ] {
            let err = KafkaError::MessageProduction(code);
            assert_eq!(
                classify_send_failure(&err),
                SendFailure::Fatal,
                "{code:?} should be fatal"
            );
        }
    }

    #[test]
    fn an_error_carrying_no_code_is_fatal() {
        // Classification reads the code, so a codeless error must not be
        // mistaken for a retryable one.
        assert_eq!(
            classify_send_failure(&KafkaError::Canceled),
            SendFailure::Fatal
        );
    }

    #[test]
    fn a_substring_match_on_the_rendered_text_cannot_reach_this_verdict() {
        // MessageTimedOut renders without any "queue full" text, so text
        // matching can only call it fatal; by code it is retryable. Pins the
        // classifier to the code and blocks a return to string matching.
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);
        let rendered = err.to_string();
        assert!(
            !rendered.contains("queue full") && !rendered.contains("Local: Queue full"),
            "unexpected rendering: {rendered}"
        );
        assert_eq!(classify_send_failure(&err), SendFailure::Retryable);
    }
}
