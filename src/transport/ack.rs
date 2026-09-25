// Project:   scalo
// File:      src/transport/ack.rs
// Purpose:   At-least-once source acknowledgements: capability, config, release API
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! At-least-once source acknowledgements.
//!
//! A **source ack** is what a source is told once its records are safe: a Kafka
//! offset commit, a gRPC or HTTP response. A **piece** is one downstream
//! delivery covering part of a block (a sub-block, a destination, a DLQ write).
//! The source ack is **released** once every piece reported, with the merged
//! [`DeliveryStatus`]: `Delivered`, `Dropped` and `Rejected` release it,
//! `Errored` withholds it so the record is delivered again.
//!
//! The pieces:
//!
//! - [`AcknowledgementsConfig`] -- the `acknowledgements.enabled` key, default
//!   on, present only on ack-capable transports.
//! - [`AckControl`] -- the run-time view a caller queries through
//!   [`TransportReceiver::ack_control`]:
//!   enabled, arm, held.
//! - [`AcknowledgingReceiver`] -- the type-level capability. Kafka and gRPC
//!   implement it, pipe and memory do not.
//! - [`SourceAck`] -- the release API for a hand-rolled receive loop.
//! - [`Tickets`] -- admission and outcome for an app's own listeners.
//! - [`SinkConfirmation`] and [`EffectiveGuarantee`] -- what a sink confirms,
//!   and the guarantee a pipeline actually gives, exported as
//!   `pipeline_delivery_guarantee`.
//!
//! The `BatchEngine` pipeline builder drives all of this for an engine-run app;
//! see `docs/pipeline/acknowledgements.md`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::error::TransportResult;
use super::finalizer::{BatchFinalizer, DeliveryStatus, PieceFinalizer};
use super::traits::{FromCascade, TransportReceiver};

/// Time left before a hold deadline at which a waiting release gives up, so
/// the source still answers its sender before the sender's own deadline.
pub const HOLD_RELEASE_MARGIN: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// The `acknowledgements` section of an ack-capable transport.
///
/// With `enabled: true` (the default) a source releases its acknowledgement --
/// a Kafka offset commit, a gRPC or HTTP response -- only once every piece of
/// work built from the record has been delivered, dead-lettered with the DLQ's
/// confirmation, or dropped by policy. A failed delivery withholds it, so the
/// record is delivered again: duplicates are possible, loss is not.
///
/// With `enabled: false` the source acknowledges at receipt, before the record
/// is processed: a crash or a failed delivery loses what was acknowledged.
///
/// ```yaml
/// kafka:
///   acknowledgements:
///     enabled: true
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(default)]
#[non_exhaustive]
pub struct AcknowledgementsConfig {
    /// Hold the source acknowledgement until every piece is delivered.
    pub enabled: bool,
}

impl Default for AcknowledgementsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl AcknowledgementsConfig {
    /// A config with `enabled` set as given.
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl FromCascade for AcknowledgementsConfig {}

// ---------------------------------------------------------------------------
// Capability
// ---------------------------------------------------------------------------

/// How a source acknowledges its records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AckKind {
    /// The source re-delivers what was not released (Kafka, file): holding the
    /// ack needs nothing from the sender.
    Pull,
    /// The source answers a sender (gRPC, HTTP): it holds the answer only once
    /// armed, and answers at enqueue otherwise.
    Push,
}

/// What a source holds unreleased right now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HeldAcks {
    /// Records held.
    pub count: u64,
    /// Payload bytes held, where the source counts them.
    pub bytes: u64,
    /// How long the oldest held record has waited.
    pub oldest_age: Option<Duration>,
    /// The earliest instant by which a held record must be released.
    pub earliest_deadline: Option<Instant>,
}

impl HeldAcks {
    /// A snapshot with every field given.
    #[must_use]
    pub const fn new(
        count: u64,
        bytes: u64,
        oldest_age: Option<Duration>,
        earliest_deadline: Option<Instant>,
    ) -> Self {
        Self {
            count,
            bytes,
            oldest_age,
            earliest_deadline,
        }
    }
}

/// The run-time acknowledgement controls of a receiver.
///
/// Object-safe, so a caller generic over any
/// [`TransportReceiver`] reaches it through
/// [`ack_control`](super::TransportReceiver::ack_control) without naming the
/// transport.
pub trait AckControl: Send + Sync {
    /// Whether `acknowledgements.enabled` is on for this source.
    fn enabled(&self) -> bool;

    /// Promise that the caller releases every token this source hands out from
    /// now on, through [`release`](super::TransportReceiver::release).
    ///
    /// A push source holds its answers only once armed, so a caller that never
    /// arms keeps the answer-at-enqueue behaviour. Arm before the first `recv`.
    /// Idempotent.
    fn arm(&self);

    /// Whether [`arm`](Self::arm) has been called.
    fn is_armed(&self) -> bool;

    /// How this source acknowledges.
    fn kind(&self) -> AckKind;

    /// What the source holds unreleased.
    fn held(&self) -> HeldAcks;
}

/// A receive transport that can hold its source acknowledgement until every
/// piece built from a record is delivered.
///
/// Kafka and gRPC implement it. Pipe and memory do not: they have no
/// acknowledgement to hold.
pub trait AcknowledgingReceiver: TransportReceiver {
    /// The `acknowledgements` config this source was built with.
    fn acknowledgements(&self) -> AcknowledgementsConfig;
}

/// What a sink's `Ok` proves about delivery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SinkConfirmation {
    /// Nothing beyond the call returning: the write counts as delivered, and
    /// the pipeline reports best effort.
    #[default]
    None,
    /// Written durably on this node.
    Local,
    /// Confirmed by the next hop (a broker ack, a server's answer).
    Remote,
}

/// Why a sender would dead-letter a record instead of sending it, or why no
/// DLQ backend can ever hold an entry.
///
/// Returned by
/// [`TransportSender::dead_letter_reason`](super::TransportSender::dead_letter_reason),
/// and by `Dlq::refusal` with the `dlq` feature.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DeadLetterReason {
    /// The record is over the sender's size ceiling, or the entry over every
    /// DLQ backend's.
    #[error("record of {bytes} bytes is over the sender's {limit}-byte ceiling")]
    TooLarge {
        /// The record's payload bytes, or the entry's serialised bytes.
        bytes: usize,
        /// The ceiling it is measured against.
        limit: usize,
    },
    /// An outbound `dlq` filter matched it.
    #[error("an outbound filter routed it to the DLQ")]
    OutboundFilter,
}

/// The `reason` label of a record over a sender's size ceiling.
pub(crate) const TOO_LARGE: &str = "too_large";

impl DeadLetterReason {
    /// The `reason` label value.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::TooLarge { .. } => TOO_LARGE,
            Self::OutboundFilter => "outbound_filter",
        }
    }
}

// ---------------------------------------------------------------------------
// Effective guarantee
// ---------------------------------------------------------------------------

/// The delivery guarantee a pipeline gives end to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryGuarantee {
    /// Every record is confirmed downstream before its source is released.
    AtLeastOnce,
    /// As `AtLeastOnce`, but the confirmation is a durable local write.
    AtLeastOnceLocal,
    /// A crash or failed delivery can lose acknowledged records.
    BestEffort,
}

impl DeliveryGuarantee {
    /// The `guarantee` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AtLeastOnce => "at_least_once",
            Self::AtLeastOnceLocal => "at_least_once_local",
            Self::BestEffort => "best_effort",
        }
    }
}

/// Why a pipeline gives the guarantee it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GuaranteeReason {
    /// Source holds its ack and the sink confirms remotely.
    Confirmed,
    /// The sink confirms with a durable local write only.
    SinkConfirmsLocally,
    /// The source has `acknowledgements.enabled: false`.
    AcksDisabled,
    /// The source has no acknowledgement to hold (pipe, memory).
    SourceCannotAck,
    /// The sink's `Ok` proves nothing beyond the call returning.
    SinkCannotConfirm,
    /// A push source with acknowledgements on that no caller armed, so it
    /// still answers at enqueue.
    Unarmed,
}

impl GuaranteeReason {
    /// The `reason` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::SinkConfirmsLocally => "sink_confirms_locally",
            Self::AcksDisabled => "acks_disabled",
            Self::SourceCannotAck => "source_cannot_ack",
            Self::SinkCannotConfirm => "sink_cannot_confirm",
            Self::Unarmed => "unarmed",
        }
    }
}

/// The guarantee a source and sink pair gives, with the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EffectiveGuarantee {
    /// The guarantee.
    pub guarantee: DeliveryGuarantee,
    /// Why.
    pub reason: GuaranteeReason,
}

impl EffectiveGuarantee {
    /// The guarantee of a pipeline reading `source` and writing to a sink that
    /// confirms as `sink`.
    #[must_use]
    pub fn of(source: Option<&dyn AckControl>, sink: SinkConfirmation) -> Self {
        let best_effort = |reason| Self {
            guarantee: DeliveryGuarantee::BestEffort,
            reason,
        };
        let Some(source) = source else {
            return best_effort(GuaranteeReason::SourceCannotAck);
        };
        if !source.enabled() {
            return best_effort(GuaranteeReason::AcksDisabled);
        }
        if source.kind() == AckKind::Push && !source.is_armed() {
            return best_effort(GuaranteeReason::Unarmed);
        }
        match sink {
            SinkConfirmation::Remote => Self {
                guarantee: DeliveryGuarantee::AtLeastOnce,
                reason: GuaranteeReason::Confirmed,
            },
            SinkConfirmation::Local => Self {
                guarantee: DeliveryGuarantee::AtLeastOnceLocal,
                reason: GuaranteeReason::SinkConfirmsLocally,
            },
            SinkConfirmation::None => best_effort(GuaranteeReason::SinkCannotConfirm),
        }
    }

    /// Set `pipeline_delivery_guarantee{guarantee, reason}` to 1 for this pair.
    pub fn publish(self) {
        #[cfg(feature = "metrics")]
        metrics::gauge!(
            "pipeline_delivery_guarantee",
            "guarantee" => self.guarantee.as_str(),
            "reason" => self.reason.as_str()
        )
        .set(1.0);
        tracing::info!(
            guarantee = self.guarantee.as_str(),
            reason = self.reason.as_str(),
            "pipeline delivery guarantee"
        );
    }

    /// As [`publish`](Self::publish), with a `listener` label, for an app
    /// that runs one source and sink pair per listener.
    pub fn publish_for(self, listener: &str) {
        #[cfg(feature = "metrics")]
        metrics::gauge!(
            "pipeline_delivery_guarantee",
            "guarantee" => self.guarantee.as_str(),
            "reason" => self.reason.as_str(),
            "listener" => listener.to_owned()
        )
        .set(1.0);
        tracing::info!(
            guarantee = self.guarantee.as_str(),
            reason = self.reason.as_str(),
            listener,
            "pipeline delivery guarantee"
        );
    }
}

// ---------------------------------------------------------------------------
// SourceAck -- the hand-rolled loop API
// ---------------------------------------------------------------------------

/// A merged status delivered once every piece of a unit has reported.
pub(crate) fn merged_channel() -> (
    BatchFinalizer,
    tokio::sync::oneshot::Receiver<DeliveryStatus>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let finalizer = BatchFinalizer::new(move |status| {
        let _ = tx.send(status);
    });
    (finalizer, rx)
}

/// When a wait on a unit with hold deadline `deadline` gives up:
/// [`HOLD_RELEASE_MARGIN`] before it.
pub(crate) fn give_up_at(deadline: Instant) -> tokio::time::Instant {
    tokio::time::Instant::from_std(
        deadline
            .checked_sub(HOLD_RELEASE_MARGIN)
            .unwrap_or(deadline),
    )
}

/// Release `tokens` `Errored` from a `Drop`, for a unit abandoned before its
/// own release: a panic, or a future dropped mid-way.
///
/// Drop cannot await, so the release is polled once. That is enough for a
/// push source, which answers its held senders synchronously inside
/// `release`, and a pull source withholds an unreleased token anyway.
pub(crate) fn release_abandoned<R: TransportReceiver>(receiver: &R, tokens: &[R::Token]) {
    let release = std::pin::pin!(receiver.release(tokens, DeliveryStatus::Errored));
    let polled = release.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
    // A subscriber that panics while this thread unwinds aborts the process.
    if std::thread::panicking() {
        return;
    }
    match polled {
        std::task::Poll::Ready(Ok(())) => {}
        std::task::Poll::Ready(Err(e)) => tracing::warn!(
            error = %e,
            transport = receiver.name(),
            "Releasing an abandoned block failed"
        ),
        std::task::Poll::Pending => tracing::warn!(
            transport = receiver.name(),
            "An abandoned block's release did not finish in one poll, so it stays unreleased: \
             TransportReceiver::release must do an Errored release before its first await"
        ),
    }
}

/// Await the merged status, giving up as `Errored` at `deadline` minus
/// [`HOLD_RELEASE_MARGIN`].
pub(crate) async fn await_merged(
    merged: tokio::sync::oneshot::Receiver<DeliveryStatus>,
    deadline: Option<Instant>,
) -> DeliveryStatus {
    let status = match deadline {
        Some(deadline) => tokio::time::timeout_at(give_up_at(deadline), merged)
            .await
            .unwrap_or(Ok(DeliveryStatus::Errored)),
        None => merged.await,
    };
    status.unwrap_or(DeliveryStatus::Errored)
}

/// Holds one received block's source acknowledgement until every piece built
/// from it reports, then releases it.
///
/// The one documented way for a hand-rolled receive loop:
///
/// ```rust,ignore
/// use scalo::transport::ack::SourceAck;
/// use scalo::transport::DeliveryStatus;
///
/// if let Some(control) = receiver.ack_control() {
///     control.arm(); // once, before the first recv
/// }
/// let batch = receiver.recv(1_000).await?;
/// let ack = SourceAck::new(&receiver, batch.commit_tokens);
/// let piece = ack.piece(); // one per table, file or destination
/// piece.report(DeliveryStatus::Delivered);
/// ack.release().await?; // seals, awaits the pieces, releases the source
/// ```
///
/// A piece dropped without a report counts as `Errored`. For a push source the
/// wait ends at its hold deadline, and the block is released `Errored`. A
/// `SourceAck` dropped before its release completes -- a panic, or a loop
/// future dropped mid-block -- releases the block `Errored` as it goes, so a
/// push source answers its senders at once.
#[must_use = "a SourceAck that is never released holds its source acknowledgement"]
pub struct SourceAck<'r, R: TransportReceiver> {
    receiver: &'r R,
    tokens: Vec<R::Token>,
    /// Present until `release` takes it.
    finalizer: Option<BatchFinalizer>,
    merged: Option<tokio::sync::oneshot::Receiver<DeliveryStatus>>,
    /// Set once `release` has released the tokens.
    released: bool,
}

impl<'r, R: TransportReceiver> SourceAck<'r, R> {
    /// Hold `tokens` -- a block's `commit_tokens` -- from `receiver`.
    pub fn new(receiver: &'r R, tokens: Vec<R::Token>) -> Self {
        let (finalizer, merged) = merged_channel();
        Self {
            receiver,
            tokens,
            finalizer: Some(finalizer),
            merged: Some(merged),
            released: false,
        }
    }

    /// A piece for one downstream delivery covering part of the block.
    ///
    /// # Panics
    ///
    /// Never: the finalizer is taken only by [`release`](Self::release),
    /// which consumes the ack.
    #[must_use]
    pub fn piece(&self) -> PieceFinalizer {
        self.finalizer
            .as_ref()
            .map(BatchFinalizer::piece)
            .expect("finalizer present until release consumes the ack")
    }

    /// The tokens this ack releases.
    #[must_use]
    pub fn tokens(&self) -> &[R::Token] {
        &self.tokens
    }

    /// Seal, wait for every piece, then release the source with the merged
    /// status, which is returned.
    ///
    /// # Errors
    ///
    /// The receiver's release error. The merged status is not returned then:
    /// a failed Kafka commit is covered by the next one, a failed push answer
    /// leaves the sender to retry.
    pub async fn release(mut self) -> TransportResult<DeliveryStatus> {
        let deadline = self.receiver.hold_deadline(&self.tokens);
        if let Some(finalizer) = self.finalizer.take() {
            finalizer.seal();
        }
        let status = match self.merged.take() {
            Some(merged) => await_merged(merged, deadline).await,
            None => DeliveryStatus::Errored,
        };
        let released = self.receiver.release(&self.tokens, status).await;
        self.released = true;
        released?;
        Ok(status)
    }
}

impl<R: TransportReceiver> Drop for SourceAck<'_, R> {
    fn drop(&mut self) {
        if !self.released {
            release_abandoned(self.receiver, &self.tokens);
        }
    }
}

impl<R: TransportReceiver> std::fmt::Debug for SourceAck<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceAck")
            .field("tokens", &self.tokens.len())
            .field("finalizer", &self.finalizer)
            .field("released", &self.released)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Tickets -- an app's own listeners
// ---------------------------------------------------------------------------

/// Why [`Tickets::admit`] refused a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Refused {
    /// Admitting it would take the held bytes past the ceiling.
    #[error("held bytes would pass the ceiling")]
    Ceiling,
    /// The listener is shutting down.
    #[error("closed to new requests")]
    Closed,
}

impl Refused {
    /// The `reason` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ceiling => "ceiling",
            Self::Closed => "closed",
        }
    }
}

/// How a held request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TicketOutcome {
    /// Every piece was delivered.
    Delivered,
    /// Dropped by policy; not a loss.
    Dropped,
    /// Dead-lettered, and the DLQ confirmed the write.
    Rejected,
    /// A piece failed: the sender must retry.
    Errored,
    /// The deadline passed before every piece reported: the sender must
    /// retry, and may deliver a duplicate.
    Expired,
}

impl TicketOutcome {
    /// Whether the sender may be answered with success.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Delivered | Self::Dropped | Self::Rejected)
    }

    /// The `outcome` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Dropped => "dropped",
            Self::Rejected => "rejected",
            Self::Errored => "errored",
            Self::Expired => "expired",
        }
    }

    fn from_status(status: DeliveryStatus) -> Self {
        match status {
            DeliveryStatus::Delivered => Self::Delivered,
            DeliveryStatus::Dropped => Self::Dropped,
            DeliveryStatus::Rejected => Self::Rejected,
            DeliveryStatus::Errored => Self::Errored,
        }
    }
}

struct TicketsInner {
    transport: &'static str,
    max_held_bytes: u64,
    held_bytes: AtomicU64,
    closed: AtomicBool,
    next_id: AtomicU64,
    /// Admission instant and deadline of every live ticket, for [`Tickets::held`].
    live: parking_lot::Mutex<HashMap<u64, (Instant, Instant)>>,
    #[cfg(feature = "memory")]
    memory_guard: Option<Arc<crate::memory::MemoryGuard>>,
}

impl TicketsInner {
    fn publish_held(&self, count: usize) {
        #[cfg(feature = "metrics")]
        {
            metrics::gauge!("transport_ack_held", "transport" => self.transport).set(count as f64);
            metrics::gauge!("transport_ack_held_bytes", "transport" => self.transport)
                .set(self.held_bytes.load(Ordering::Relaxed) as f64);
        }
        #[cfg(not(feature = "metrics"))]
        let _ = count;
    }
}

/// Admission and outcome for requests an app's own listener holds until their
/// records are delivered.
///
/// Each request takes a [`Ticket`] before it is queued, hands a
/// [`piece`](Ticket::piece) to each destination send, and answers its sender
/// from [`outcome`](Ticket::outcome). Held bytes are bounded: a request that
/// would pass the ceiling is refused, except when nothing is held, so an
/// oversized request is never refused forever.
///
/// Clone is cheap; clones share the ceiling.
#[derive(Clone)]
pub struct Tickets {
    inner: Arc<TicketsInner>,
}

impl Tickets {
    /// Tickets for the listener named `transport` (the metric label), holding
    /// at most `max_held_bytes` of requests at once.
    #[must_use]
    pub fn new(transport: &'static str, max_held_bytes: u64) -> Self {
        Self {
            inner: Arc::new(TicketsInner {
                transport,
                max_held_bytes,
                held_bytes: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                next_id: AtomicU64::new(0),
                live: parking_lot::Mutex::new(HashMap::new()),
                #[cfg(feature = "memory")]
                memory_guard: None,
            }),
        }
    }

    /// Lease held bytes on `guard` from admission to outcome, so held requests
    /// count toward memory pressure. Call before cloning or admitting.
    #[cfg(feature = "memory")]
    #[must_use]
    pub fn with_memory_guard(mut self, guard: Arc<crate::memory::MemoryGuard>) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.memory_guard = Some(guard);
        } else {
            tracing::warn!(
                transport = self.inner.transport,
                "Tickets already shared: the memory guard was not attached"
            );
        }
        self
    }

    /// Admit a request of `bytes` that must be answered by `deadline`.
    ///
    /// # Errors
    ///
    /// [`Refused::Ceiling`] when it would take the held bytes past the ceiling
    /// while something is held, [`Refused::Closed`] after [`close`](Self::close).
    pub fn admit(&self, bytes: u64, deadline: Instant) -> Result<Ticket, Refused> {
        let inner = &self.inner;
        let refused = if inner.closed.load(Ordering::Acquire) {
            Some(Refused::Closed)
        } else {
            let max = inner.max_held_bytes;
            inner
                .held_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                    (held == 0 || held.saturating_add(bytes) <= max)
                        .then(|| held.saturating_add(bytes))
                })
                .err()
                .map(|_| Refused::Ceiling)
        };
        if let Some(reason) = refused {
            #[cfg(feature = "metrics")]
            metrics::counter!(
                "transport_ack_refused_total",
                "transport" => inner.transport,
                "reason" => reason.as_str()
            )
            .increment(1);
            return Err(reason);
        }

        #[cfg(feature = "memory")]
        if let Some(guard) = &inner.memory_guard {
            guard.add_bytes(bytes);
        }
        let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let admitted = Instant::now();
        let count = {
            let mut live = inner.live.lock();
            live.insert(id, (admitted, deadline));
            live.len()
        };
        inner.publish_held(count);

        let (finalizer, merged) = merged_channel();
        Ok(Ticket {
            inner: Arc::clone(inner),
            id,
            bytes,
            admitted,
            deadline,
            finalizer: Some(finalizer),
            merged: Some(merged),
        })
    }

    /// Refuse every later request with [`Refused::Closed`]. Tickets already
    /// admitted keep running.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    /// What is held right now.
    #[must_use]
    pub fn held(&self) -> HeldAcks {
        let now = Instant::now();
        let live = self.inner.live.lock();
        let oldest = live.values().map(|(admitted, _)| *admitted).min();
        let earliest = live.values().map(|(_, deadline)| *deadline).min();
        HeldAcks {
            count: live.len() as u64,
            bytes: self.inner.held_bytes.load(Ordering::Relaxed),
            oldest_age: oldest.map(|t| now.saturating_duration_since(t)),
            earliest_deadline: earliest,
        }
    }
}

impl std::fmt::Debug for Tickets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tickets")
            .field("transport", &self.inner.transport)
            .field("max_held_bytes", &self.inner.max_held_bytes)
            .field("held", &self.held())
            .finish_non_exhaustive()
    }
}

/// One admitted request. Hand a [`piece`](Self::piece) to each destination
/// send, then answer the sender from [`outcome`](Self::outcome).
///
/// Dropping a ticket frees its held bytes.
#[must_use = "a ticket frees its held bytes only when its outcome is taken or it is dropped"]
pub struct Ticket {
    inner: Arc<TicketsInner>,
    id: u64,
    bytes: u64,
    admitted: Instant,
    deadline: Instant,
    finalizer: Option<BatchFinalizer>,
    merged: Option<tokio::sync::oneshot::Receiver<DeliveryStatus>>,
}

impl Ticket {
    /// A piece for one destination send of this request.
    ///
    /// # Panics
    ///
    /// Never: the finalizer is taken only by [`outcome`](Self::outcome), which
    /// consumes the ticket.
    #[must_use]
    pub fn piece(&self) -> PieceFinalizer {
        self.finalizer
            .as_ref()
            .map(BatchFinalizer::piece)
            .expect("finalizer present until outcome consumes the ticket")
    }

    /// The instant by which the sender must be answered.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Wait until every piece reported, or the deadline passed.
    pub async fn outcome(mut self) -> TicketOutcome {
        if let Some(finalizer) = self.finalizer.take() {
            finalizer.seal();
        }
        let Some(merged) = self.merged.take() else {
            return TicketOutcome::Errored;
        };
        let deadline = tokio::time::Instant::from_std(self.deadline);
        let outcome = match tokio::time::timeout_at(deadline, merged).await {
            Ok(Ok(status)) => TicketOutcome::from_status(status),
            Ok(Err(_)) => TicketOutcome::Errored,
            Err(_elapsed) => TicketOutcome::Expired,
        };
        #[cfg(feature = "metrics")]
        {
            metrics::counter!(
                "transport_ack_released_total",
                "transport" => self.inner.transport,
                "outcome" => outcome.as_str()
            )
            .increment(1);
            metrics::histogram!(
                "transport_ack_latency_seconds",
                "transport" => self.inner.transport,
                "outcome" => outcome.as_str()
            )
            .record(self.admitted.elapsed().as_secs_f64());
        }
        outcome
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let inner = &self.inner;
        inner.held_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        #[cfg(feature = "memory")]
        if let Some(guard) = &inner.memory_guard {
            guard.release(self.bytes);
        }
        let count = {
            let mut live = inner.live.lock();
            live.remove(&self.id);
            live.len()
        };
        inner.publish_held(count);
    }
}

impl std::fmt::Debug for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ticket")
            .field("bytes", &self.bytes)
            .field("admitted", &self.admitted)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acknowledgements_default_on_and_section_parses() {
        assert!(AcknowledgementsConfig::default().enabled);
        let off: AcknowledgementsConfig =
            serde_json::from_str(r#"{"enabled": false}"#).expect("parses");
        assert!(!off.enabled);
        let empty: AcknowledgementsConfig = serde_json::from_str("{}").expect("parses");
        assert!(empty.enabled, "an empty section keeps the default");
    }

    struct Control {
        enabled: bool,
        armed: AtomicBool,
        kind: AckKind,
    }

    impl AckControl for Control {
        fn enabled(&self) -> bool {
            self.enabled
        }
        fn arm(&self) {
            self.armed.store(true, Ordering::Release);
        }
        fn is_armed(&self) -> bool {
            self.armed.load(Ordering::Acquire)
        }
        fn kind(&self) -> AckKind {
            self.kind
        }
        fn held(&self) -> HeldAcks {
            HeldAcks::default()
        }
    }

    fn control(enabled: bool, armed: bool, kind: AckKind) -> Control {
        Control {
            enabled,
            armed: AtomicBool::new(armed),
            kind,
        }
    }

    #[test]
    fn effective_guarantee_names_the_weakest_link() {
        use DeliveryGuarantee as G;
        use GuaranteeReason as R;
        let of = |c: Option<&Control>, sink| {
            let g = EffectiveGuarantee::of(c.map(|c| c as &dyn AckControl), sink);
            (g.guarantee, g.reason)
        };
        assert_eq!(
            of(None, SinkConfirmation::Remote),
            (G::BestEffort, R::SourceCannotAck)
        );
        let off = control(false, true, AckKind::Pull);
        assert_eq!(
            of(Some(&off), SinkConfirmation::Remote),
            (G::BestEffort, R::AcksDisabled)
        );
        let unarmed_push = control(true, false, AckKind::Push);
        assert_eq!(
            of(Some(&unarmed_push), SinkConfirmation::Remote),
            (G::BestEffort, R::Unarmed)
        );
        let unarmed_pull = control(true, false, AckKind::Pull);
        assert_eq!(
            of(Some(&unarmed_pull), SinkConfirmation::Remote),
            (G::AtLeastOnce, R::Confirmed),
            "a pull source holds by not committing, armed or not"
        );
        assert_eq!(
            of(Some(&unarmed_pull), SinkConfirmation::Local),
            (G::AtLeastOnceLocal, R::SinkConfirmsLocally)
        );
        assert_eq!(
            of(Some(&unarmed_pull), SinkConfirmation::None),
            (G::BestEffort, R::SinkCannotConfirm)
        );
    }

    #[tokio::test]
    async fn ticket_answers_after_every_piece() {
        let tickets = Tickets::new("test", 1024);
        let ticket = tickets
            .admit(10, Instant::now() + Duration::from_secs(5))
            .expect("admitted");
        let first = ticket.piece();
        let second = ticket.piece();
        first.report(DeliveryStatus::Delivered);
        let late = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            second.report(DeliveryStatus::Rejected);
        });
        assert_eq!(ticket.outcome().await, TicketOutcome::Rejected);
        late.await.expect("joined");
        assert_eq!(tickets.held().count, 0, "outcome frees the ticket");
        assert_eq!(tickets.held().bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn ticket_expires_at_its_deadline() {
        let tickets = Tickets::new("test", 1024);
        let ticket = tickets
            .admit(10, Instant::now() + Duration::from_millis(200))
            .expect("admitted");
        let _never = ticket.piece();
        assert_eq!(ticket.outcome().await, TicketOutcome::Expired);
    }

    #[test]
    fn admission_refuses_past_the_ceiling_but_never_when_empty() {
        let tickets = Tickets::new("test", 100);
        let deadline = Instant::now() + Duration::from_secs(5);
        let oversized = tickets.admit(500, deadline).expect("admitted when empty");
        assert_eq!(tickets.admit(1, deadline).err(), Some(Refused::Ceiling));
        drop(oversized);
        let first = tickets.admit(60, deadline).expect("room");
        assert_eq!(tickets.admit(60, deadline).err(), Some(Refused::Ceiling));
        let second = tickets.admit(40, deadline).expect("exactly at the ceiling");
        assert_eq!(tickets.held().bytes, 100);
        drop((first, second));
        tickets.close();
        assert_eq!(tickets.admit(1, deadline).err(), Some(Refused::Closed));
    }
}
