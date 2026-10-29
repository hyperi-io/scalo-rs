// Project:   scalo
// File:      src/transport/kafka/classify.rs
// Purpose:   Produce- and consume-failure policy shared by every Kafka path
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Kafka failure policy: how a failed send or poll is classified, and how it is
//! reported without flooding the log.
//!
//! Every send path -- the transport's `send`, the standalone
//! [`KafkaProducer`](super::KafkaProducer), and both delivery callbacks -- asks
//! the same question of a failure: retry the same message, retry later, or give
//! up. They answer it here so the paths cannot drift.
//!
//! The consume path asks the matching question of a failed poll: keep polling
//! or give up. librdkafka reconnects to brokers and rejoins the group by
//! itself, so an unavailable broker is never a reason to end the consumer; only
//! a failure no retry can clear is surfaced.
//!
//! An authentication or TLS refusal reaches a client only through librdkafka's
//! error callback. The message it leaves behind times out like any message sent
//! to an unreachable broker, so [`AuthState`] keeps the refusal and the send path
//! reads it back, rather than guessing from the timeout.
//!
//! The classification reads [`KafkaError::rdkafka_error_code`], never the
//! rendered message. `Cargo.toml` pins `rdkafka` with `dynamic-linking`, so the
//! error STRING comes from whichever system librdkafka the image ships and can
//! change under a base-image bump; the error CODE is a stable protocol constant.

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use crate::backoff::Backoff;

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
    /// The destination topic does not exist yet. The caller keeps the record
    /// and retries, as it does a retryable failure.
    MissingTopic,
    /// Retrying cannot help -- authorisation or a permanent topic error.
    Fatal,
}

/// Classify a produce-side failure by its librdkafka error code.
pub(crate) fn classify_send_failure(err: &KafkaError) -> SendFailure {
    match err.rdkafka_error_code() {
        Some(RDKafkaErrorCode::QueueFull) => SendFailure::QueueFull,
        Some(RDKafkaErrorCode::MessageSizeTooLarge) => SendFailure::TooLarge,
        Some(code) if is_retryable(code) => SendFailure::Retryable,
        Some(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic) => {
            SendFailure::MissingTopic
        }
        _ => SendFailure::Fatal,
    }
}

/// Codes that clear without operator action, so the message is worth retrying.
///
/// Mirrors Kafka's own retriable-error set plus the librdkafka-local transport
/// and timeout codes. A missing topic is retried too, as
/// [`SendFailure::MissingTopic`], so it fails readiness and counts on its own.
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

/// What a caller should do about a failed Kafka poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecvFailure {
    /// librdkafka reached the end of a partition. Informational, not a failure.
    EndOfPartition,
    /// A broker, network, coordinator or group-membership condition that
    /// librdkafka recovers from by itself. The caller keeps polling.
    Transient,
    /// A subscribed topic does not exist yet: one not deployed yet, a topic
    /// ensure that failed, a broker rebuilt. The consumer waits for it.
    MissingTopic,
    /// Retrying cannot help -- authentication on first contact, authorisation,
    /// invalid configuration, or an error flagged fatal.
    Permanent,
    /// A code this module does not classify. Treated as permanent, and logged
    /// by name so it can be classified.
    Unclassified,
}

/// What the consumer knows when it classifies a poll failure.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RecvContext {
    /// Whether this consumer has reached a broker with its credentials before.
    /// An authentication or TLS failure after that is most likely a restarting
    /// broker; before it, a misconfiguration that should fail fast.
    pub(crate) credentials_proven: bool,
}

/// Classify a consume-side failure returned by a poll.
pub(crate) fn classify_recv_failure(err: &KafkaError, ctx: RecvContext) -> RecvFailure {
    match err {
        KafkaError::PartitionEOF(_) => RecvFailure::EndOfPartition,
        // librdkafka raises the fatal flag when the client can no longer be used.
        KafkaError::MessageConsumptionFatal(_) => RecvFailure::Permanent,
        _ => match err.rdkafka_error_code() {
            Some(code) if is_retryable(code) || is_consumer_retryable(code) => {
                RecvFailure::Transient
            }
            Some(RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic) => {
                RecvFailure::MissingTopic
            }
            Some(code) if is_auth_failure(code) && ctx.credentials_proven => RecvFailure::Transient,
            Some(code) if is_auth_failure(code) || is_recv_permanent(code) => {
                RecvFailure::Permanent
            }
            _ => RecvFailure::Unclassified,
        },
    }
}

/// Consume-side codes that clear on librdkafka's own reconnect, rejoin or
/// refetch, on top of the transport set in [`is_retryable`].
fn is_consumer_retryable(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        // Coordinator discovery and loading.
        RDKafkaErrorCode::WaitingForCoordinator
            | RDKafkaErrorCode::CoordinatorLoadInProgress
            // Group membership churn: the member rejoins on the next poll.
            | RDKafkaErrorCode::RebalanceInProgress
            | RDKafkaErrorCode::IllegalGeneration
            | RDKafkaErrorCode::UnknownMemberId
            | RDKafkaErrorCode::FencedMemberEpoch
            | RDKafkaErrorCode::StaleMemberEpoch
            | RDKafkaErrorCode::AssignmentLost
            | RDKafkaErrorCode::PollExceeded
            // Local waits and broker-set changes.
            | RDKafkaErrorCode::TimedOutQueue
            | RDKafkaErrorCode::NodeUpdate
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::RebootstrapRequired
            | RDKafkaErrorCode::Retry
            // Leadership catching up.
            | RDKafkaErrorCode::OffsetNotAvailable
            | RDKafkaErrorCode::PreferredLeaderNotAvailable
    )
}

/// Authentication and TLS handshake failures: permanent on first contact,
/// transient once the same credentials have worked.
pub(crate) fn is_auth_failure(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::SSL
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::IllegalSASLState
            | RDKafkaErrorCode::UnsupportedSASLMechanism
    )
}

/// Consume-side codes that need operator action whatever the consumer's history.
fn is_recv_permanent(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        // Authorisation: an ACL has to change.
        RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            // A partition the topic does not have.
            | RDKafkaErrorCode::UnknownPartition
            // Configuration the broker or client refuses.
            | RDKafkaErrorCode::InvalidArgument
            | RDKafkaErrorCode::InvalidConfig
            | RDKafkaErrorCode::InvalidGroupId
            | RDKafkaErrorCode::InvalidSessionTimeout
            | RDKafkaErrorCode::InconsistentGroupProtocol
            | RDKafkaErrorCode::UnsupportedVersion
            | RDKafkaErrorCode::UnsupportedFeature
            | RDKafkaErrorCode::UnsupportedAssignor
            // The client is finished or replaced.
            | RDKafkaErrorCode::Fatal
            | RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::FencedInstanceId
            // Offsets the operator chose not to reset automatically.
            | RDKafkaErrorCode::AutoOffsetReset
            | RDKafkaErrorCode::OffsetOutOfRange
            | RDKafkaErrorCode::LogTruncation
    )
}

/// Whether a failed synchronous commit can succeed if it is retried.
///
/// The generation-fencing codes are excluded: they mean the offsets belong to
/// an assignment this member no longer holds, so no retry can land them.
pub(crate) fn commit_is_retryable(err: &KafkaError) -> bool {
    err.rdkafka_error_code().is_some_and(|code| {
        is_retryable(code)
            || matches!(
                code,
                RDKafkaErrorCode::WaitingForCoordinator
                    | RDKafkaErrorCode::CoordinatorLoadInProgress
                    | RDKafkaErrorCode::RebalanceInProgress
                    | RDKafkaErrorCode::TimedOutQueue
                    | RDKafkaErrorCode::Retry
            )
    })
}

/// Poll-failure state for one consumer: counts transient failures, spaces the
/// polls that follow them, and reports an outage once rather than per poll.
#[derive(Debug, Default)]
pub(crate) struct RecvState {
    /// Transient failures since the last record arrived; drives the backoff.
    consecutive: AtomicU32,
    degraded: DegradedLatch,
    /// Latches the missing-topic warn to the first of a run.
    missing: DegradedLatch,
    /// Set by the first record, which proves the credentials were accepted.
    received: AtomicBool,
}

impl RecvState {
    /// Count a transient poll failure; warn on the first of a run only.
    pub(crate) fn record_transient(&self, err: &KafkaError) {
        let failures = self
            .consecutive
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        count_recv_error("transient");
        if self.degraded.enter() {
            tracing::warn!(
                error = %err,
                "kafka consume failed on a transient condition; polling on with backoff \
                 while librdkafka reconnects"
            );
        } else {
            tracing::debug!(error = %err, failures, "kafka consume still failing");
        }
    }

    /// Count a poll that found a subscribed topic missing, in the backoff
    /// with the transient failures; warn on the first of a run only.
    pub(crate) fn record_missing_topic(&self, err: &KafkaError) {
        self.consecutive.fetch_add(1, Ordering::Relaxed);
        count_recv_error("missing_topic");
        if self.missing.enter() {
            tracing::warn!(
                error = %err,
                "kafka: a subscribed topic does not exist; waiting for it, consuming once \
                 librdkafka's next metadata refresh finds it"
            );
        } else {
            tracing::debug!(error = %err, "kafka: a subscribed topic is still missing");
        }
    }

    /// Note a received record: report the first after a run of transient
    /// failures or a missing topic, and remember that the credentials work.
    pub(crate) fn record_success(&self) {
        // Load first: this runs once per poll and the flag is set only once.
        if !self.received.load(Ordering::Relaxed) {
            self.received.store(true, Ordering::Relaxed);
        }
        if self.missing.clear() {
            tracing::info!("kafka: the missing topic is being consumed");
        }
        if self.degraded.clear() {
            let failures = self.consecutive.swap(0, Ordering::Relaxed);
            tracing::info!(failures, "kafka consume resumed");
        } else {
            self.consecutive.store(0, Ordering::Relaxed);
        }
    }

    /// Whether a record has ever arrived on this consumer.
    pub(crate) fn has_received(&self) -> bool {
        self.received.load(Ordering::Relaxed)
    }

    /// How long to wait before the next poll after the current run of failures.
    pub(crate) fn backoff(&self) -> Duration {
        Backoff::TRANSIENT.delay(self.consecutive.load(Ordering::Relaxed))
    }

    /// Transient failures in the current run.
    #[cfg(test)]
    pub(crate) fn failures(&self) -> u32 {
        self.consecutive.load(Ordering::Relaxed)
    }
}

/// Count a poll failure that ends the consumer, and name a code this module
/// does not classify so it can be added.
pub(crate) fn record_permanent_recv_failure(err: &KafkaError, class: RecvFailure) {
    count_recv_error("permanent");
    if class == RecvFailure::Unclassified {
        tracing::error!(
            code = ?err.rdkafka_error_code(),
            error = %err,
            "kafka consume failed with a librdkafka code the transport does not classify; \
             treating it as permanent"
        );
    }
}

/// Count one poll failure by class.
fn count_recv_error(class: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(
        "transport_recv_errors_total",
        "transport" => "kafka",
        "class" => class
    )
    .increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = class;
}

/// Edge latch for a sustained failure, so an outage logs once instead of once
/// per record.
///
/// Every send and poll path here runs per message or per poll at PB/day rates,
/// so the failure branch reports on the transition and leaves the count to the
/// metric.
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

/// Whether a client's credentials and TLS setup are being refused, kept from
/// librdkafka's error callback so the send path and readiness can say why.
///
/// A refusal counts only until a broker first accepts the client. After that an
/// authentication or TLS failure is most likely a broker restarting, which
/// librdkafka rides out, so it is logged once and changes nothing else. One
/// state serves a transport's producer and every consumer it builds, because
/// they present the same credentials: a broker accepting either proves both.
#[derive(Debug, Default)]
pub(crate) struct AuthState {
    /// A broker has accepted this client, by reaching `UP` or confirming a record.
    proven: AtomicBool,
    /// The client is refused and no broker has accepted it yet.
    refused: AtomicBool,
    /// Latches the log line to the first refusal of a run.
    latch: DegradedLatch,
    /// The last refusal, for the message a caller sees.
    reason: parking_lot::Mutex<Option<String>>,
}

impl AuthState {
    /// Take an error from librdkafka's error callback. Returns whether it was an
    /// authentication or TLS failure, which this state has then handled.
    pub(crate) fn on_error(&self, err: &KafkaError, detail: &str) -> bool {
        let Some(code) = err.rdkafka_error_code().filter(|c| is_auth_failure(*c)) else {
            return false;
        };
        if self.proven.load(Ordering::Relaxed) {
            if self.latch.enter() {
                tracing::warn!(
                    code = ?code,
                    detail,
                    "kafka authentication or TLS failed after the credentials had worked; \
                     treating it as a restarting broker"
                );
            }
            return true;
        }
        *self.reason.lock() = Some(format!("{code:?}: {detail}"));
        self.refused.store(true, Ordering::Relaxed);
        if self.latch.enter() {
            tracing::error!(
                code = ?code,
                detail,
                "kafka refused this client's credentials or TLS setup. Sends fail and \
                 readiness reports not-ready until a broker accepts it. Check the broker \
                 CA, the client certificate and the SASL user and password"
            );
        }
        true
    }

    /// Record that a broker accepted this client: ends a refusal, and re-arms the
    /// log line for a later one.
    pub(crate) fn mark_proven(&self) {
        if self.proven.load(Ordering::Relaxed) {
            self.latch.clear();
            return;
        }
        self.proven.store(true, Ordering::Relaxed);
        self.refused.store(false, Ordering::Relaxed);
        *self.reason.lock() = None;
        if self.latch.clear() {
            tracing::info!("kafka: a broker accepted this client's credentials");
        }
    }

    /// Whether a broker has accepted this client.
    pub(crate) fn is_proven(&self) -> bool {
        self.proven.load(Ordering::Relaxed)
    }

    /// Whether the client is being refused and has never been accepted.
    pub(crate) fn is_refused(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// The last refusal, while the client is refused.
    pub(crate) fn refusal(&self) -> Option<String> {
        if self.is_refused() {
            self.reason.lock().clone()
        } else {
            None
        }
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
    /// Delivery reports handled, either outcome.
    reports: AtomicU64,
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

    /// Delivery reports handled so far, acked and failed alike.
    pub(crate) fn reports(&self) -> u64 {
        self.reports.load(Ordering::Relaxed)
    }

    /// Record a delivery report of either outcome.
    pub(crate) fn record(&self, result: &rdkafka::producer::DeliveryResult<'_>) {
        use rdkafka::message::Message as _;
        self.reports.fetch_add(1, Ordering::Relaxed);
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
    fn auth_state_takes_only_authentication_and_tls_errors() {
        for (code, handled) in [
            (RDKafkaErrorCode::Authentication, true),
            (RDKafkaErrorCode::SSL, true),
            (RDKafkaErrorCode::SaslAuthenticationFailed, true),
            (RDKafkaErrorCode::AllBrokersDown, false),
            (RDKafkaErrorCode::BrokerTransportFailure, false),
            (RDKafkaErrorCode::MessageTimedOut, false),
            (RDKafkaErrorCode::TopicAuthorizationFailed, false),
        ] {
            let auth = AuthState::default();
            let taken = auth.on_error(&KafkaError::Global(code), "ssl://broker/bootstrap: refused");
            assert_eq!(taken, handled, "{code:?} handled");
            assert_eq!(auth.is_refused(), handled, "{code:?} refused");
            assert_eq!(auth.refusal().is_some(), handled, "{code:?} reason kept");
        }
    }

    #[test]
    fn a_refusal_ends_once_a_broker_accepts_the_client_and_does_not_return() {
        let auth = AuthState::default();
        let refusal = KafkaError::Global(RDKafkaErrorCode::SaslAuthenticationFailed);
        assert!(auth.on_error(&refusal, "bad password"));
        assert!(auth.refusal().is_some_and(|r| r.contains("bad password")));

        auth.mark_proven();
        assert!(auth.is_proven() && !auth.is_refused() && auth.refusal().is_none());

        // The same code after the credentials worked is a restarting broker.
        assert!(auth.on_error(&refusal, "broker restarting"));
        assert!(!auth.is_refused(), "a proven client is never refused again");
    }

    #[cfg(feature = "logger")]
    #[test]
    fn a_refusal_is_logged_once_at_error_and_a_proven_blip_once_at_warn() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        struct Sink(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&buf);
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || Sink(Arc::clone(&writer))),
        );
        let refusal = KafkaError::Global(RDKafkaErrorCode::SSL);
        let auth = AuthState::default();
        tracing::subscriber::with_default(subscriber, || {
            // librdkafka repeats the error on every reconnect.
            for _ in 0..1000 {
                auth.on_error(&refusal, "certificate verify failed");
            }
            auth.mark_proven();
            for _ in 0..1000 {
                auth.on_error(&refusal, "broker restarting");
            }
        });

        let out = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        let count = |needle: &str| out.matches(needle).count();
        assert_eq!(count("ERROR"), 1, "{out}");
        assert_eq!(count("refused this client's credentials"), 1, "{out}");
        assert_eq!(count("accepted this client's credentials"), 1, "{out}");
        assert_eq!(count("WARN"), 1, "{out}");
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

    /// A send to a topic that does not exist yet is retried, never fatal: a
    /// topic ensure that failed or a rebuilt broker clears on its own.
    #[test]
    fn a_missing_destination_topic_is_retried() {
        for code in [
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::UnknownTopic,
        ] {
            assert_eq!(
                classify_send_failure(&KafkaError::MessageProduction(code)),
                SendFailure::MissingTopic,
                "{code:?}"
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

    // ---- consume side ------------------------------------------------------

    /// A consumer that has not yet reached a broker with its credentials.
    const FIRST_CONTACT: RecvContext = RecvContext {
        credentials_proven: false,
    };

    /// A consumer whose credentials a broker has already accepted.
    const PROVEN: RecvContext = RecvContext {
        credentials_proven: true,
    };

    #[test]
    fn an_unavailable_broker_never_ends_the_consumer() {
        // Each of these ended a running pipeline when every poll error was
        // fatal; librdkafka reconnects and rejoins on its own.
        for code in [
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::Resolve,
            RDKafkaErrorCode::OperationTimedOut,
            RDKafkaErrorCode::RequestTimedOut,
            RDKafkaErrorCode::NetworkException,
            RDKafkaErrorCode::BrokerNotAvailable,
            RDKafkaErrorCode::LeaderNotAvailable,
            RDKafkaErrorCode::NotLeaderForPartition,
            RDKafkaErrorCode::CoordinatorNotAvailable,
            RDKafkaErrorCode::NotCoordinator,
            RDKafkaErrorCode::WaitingForCoordinator,
            RDKafkaErrorCode::CoordinatorLoadInProgress,
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::IllegalGeneration,
            RDKafkaErrorCode::UnknownMemberId,
            RDKafkaErrorCode::FencedMemberEpoch,
            RDKafkaErrorCode::StaleMemberEpoch,
            RDKafkaErrorCode::AssignmentLost,
            RDKafkaErrorCode::PollExceeded,
            RDKafkaErrorCode::TimedOutQueue,
            RDKafkaErrorCode::DestroyBroker,
            RDKafkaErrorCode::RebootstrapRequired,
        ] {
            let err = KafkaError::MessageConsumption(code);
            assert_eq!(
                classify_recv_failure(&err, FIRST_CONTACT),
                RecvFailure::Transient,
                "{code:?} should keep the consumer polling"
            );
        }
    }

    #[test]
    fn a_failure_no_retry_can_clear_still_surfaces() {
        for code in [
            RDKafkaErrorCode::Authentication,
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::TopicAuthorizationFailed,
            RDKafkaErrorCode::GroupAuthorizationFailed,
            RDKafkaErrorCode::ClusterAuthorizationFailed,
            RDKafkaErrorCode::UnknownPartition,
            RDKafkaErrorCode::InvalidArgument,
            RDKafkaErrorCode::InvalidConfig,
            RDKafkaErrorCode::SSL,
            RDKafkaErrorCode::Fatal,
            RDKafkaErrorCode::AutoOffsetReset,
        ] {
            let err = KafkaError::MessageConsumption(code);
            assert_eq!(
                classify_recv_failure(&err, FIRST_CONTACT),
                RecvFailure::Permanent,
                "{code:?} should end the consumer"
            );
        }
    }

    #[test]
    fn an_auth_failure_fails_fast_on_first_contact() {
        // Nothing has accepted these credentials yet: a misconfiguration.
        for code in [
            RDKafkaErrorCode::Authentication,
            RDKafkaErrorCode::SSL,
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::IllegalSASLState,
            RDKafkaErrorCode::UnsupportedSASLMechanism,
        ] {
            let err = KafkaError::MessageConsumption(code);
            assert_eq!(
                classify_recv_failure(&err, FIRST_CONTACT),
                RecvFailure::Permanent,
                "{code:?} on first contact should end the consumer"
            );
        }
    }

    #[test]
    fn an_auth_failure_after_the_credentials_worked_is_a_restarting_broker() {
        for code in [
            RDKafkaErrorCode::Authentication,
            RDKafkaErrorCode::SSL,
            RDKafkaErrorCode::SaslAuthenticationFailed,
            RDKafkaErrorCode::IllegalSASLState,
            RDKafkaErrorCode::UnsupportedSASLMechanism,
        ] {
            let err = KafkaError::MessageConsumption(code);
            assert_eq!(
                classify_recv_failure(&err, PROVEN),
                RecvFailure::Transient,
                "{code:?} after the credentials worked should keep the consumer polling"
            );
        }
        // Authorisation is an ACL decision, not a restart, so it stays permanent.
        let denied = KafkaError::MessageConsumption(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert_eq!(
            classify_recv_failure(&denied, PROVEN),
            RecvFailure::Permanent
        );
    }

    #[test]
    fn the_fatal_flag_wins_over_a_transient_code() {
        // librdkafka can flag any code fatal; the client is unusable after it.
        let err = KafkaError::MessageConsumptionFatal(RDKafkaErrorCode::BrokerTransportFailure);
        assert_eq!(
            classify_recv_failure(&err, FIRST_CONTACT),
            RecvFailure::Permanent
        );
        assert_eq!(classify_recv_failure(&err, PROVEN), RecvFailure::Permanent);
    }

    /// A missing topic is waited for, whether or not the consumer may create
    /// it and whatever its history: nothing about it ends the consumer.
    #[test]
    fn a_missing_topic_is_waited_for() {
        for code in [
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::UnknownTopic,
        ] {
            for ctx in [FIRST_CONTACT, PROVEN] {
                let err = KafkaError::MessageConsumption(code);
                assert_eq!(
                    classify_recv_failure(&err, ctx),
                    RecvFailure::MissingTopic,
                    "{code:?}, {ctx:?}"
                );
            }
        }
        // An authorisation failure on a topic is still the operator's to fix.
        let denied = KafkaError::MessageConsumption(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert_eq!(
            classify_recv_failure(&denied, PROVEN),
            RecvFailure::Permanent
        );
    }

    #[test]
    fn a_missing_topic_backs_off_and_a_record_resets_the_run() {
        let state = RecvState::default();
        let err = KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition);
        state.record_missing_topic(&err);
        state.record_missing_topic(&err);
        assert_eq!(state.failures(), 2, "each wait counts toward the backoff");
        state.record_success();
        assert_eq!(state.failures(), 0);
    }

    #[test]
    fn end_of_partition_is_not_a_failure() {
        assert_eq!(
            classify_recv_failure(&KafkaError::PartitionEOF(3), FIRST_CONTACT),
            RecvFailure::EndOfPartition
        );
    }

    #[test]
    fn an_unlisted_code_is_unclassified_and_ends_the_consumer_by_name() {
        // Unclassified is handled as permanent, with its own log line.
        for err in [
            KafkaError::MessageConsumption(RDKafkaErrorCode::BadMessage),
            KafkaError::MessageConsumption(RDKafkaErrorCode::InvalidRecord),
            KafkaError::Canceled,
        ] {
            assert_eq!(
                classify_recv_failure(&err, PROVEN),
                RecvFailure::Unclassified,
                "{err:?}"
            );
        }
    }

    #[test]
    fn a_commit_is_retried_only_while_the_offsets_can_still_land() {
        for code in [
            RDKafkaErrorCode::WaitingForCoordinator,
            RDKafkaErrorCode::CoordinatorNotAvailable,
            RDKafkaErrorCode::NotCoordinator,
            RDKafkaErrorCode::CoordinatorLoadInProgress,
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::RequestTimedOut,
            RDKafkaErrorCode::BrokerTransportFailure,
            RDKafkaErrorCode::AllBrokersDown,
        ] {
            assert!(
                commit_is_retryable(&KafkaError::ConsumerCommit(code)),
                "{code:?} should be retried"
            );
        }
        // A new generation owns the partitions: retrying cannot land these offsets.
        for code in [
            RDKafkaErrorCode::IllegalGeneration,
            RDKafkaErrorCode::UnknownMemberId,
            RDKafkaErrorCode::FencedMemberEpoch,
            RDKafkaErrorCode::StaleMemberEpoch,
            RDKafkaErrorCode::GroupAuthorizationFailed,
        ] {
            assert!(
                !commit_is_retryable(&KafkaError::ConsumerCommit(code)),
                "{code:?} should not be retried"
            );
        }
    }

    #[test]
    fn recv_state_backs_off_through_an_outage_and_resets_on_recovery() {
        let ceiling = Duration::from_secs(2);
        let state = RecvState::default();
        let err = KafkaError::MessageConsumption(RDKafkaErrorCode::AllBrokersDown);
        for _ in 0..10 {
            state.record_transient(&err);
        }
        assert!(
            state.backoff() >= ceiling * 4 / 5 && state.backoff() <= ceiling,
            "a sustained outage backs off to the ceiling, got {:?}",
            state.backoff()
        );
        assert!(!state.degraded.enter(), "the outage stays latched");
        assert!(!state.has_received());
        state.record_success();
        assert!(state.has_received(), "a record proves the credentials");
        assert_eq!(state.consecutive.load(Ordering::Relaxed), 0);
        assert!(
            state.backoff() <= Duration::from_millis(120),
            "a record resets the backoff"
        );
        assert!(state.degraded.enter(), "recovery cleared the latch");
    }
}
