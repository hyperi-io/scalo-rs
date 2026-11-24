// Project:   scalo
// File:      src/sink_stack/mod.rs
// Purpose:   Outbound sink-control stack (tower + backon + governor)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Outbound sink-control stack.
//!
//! Composes the outbound delivery controls around any [`TransportSender`] so an
//! app gets timeout, concurrency limiting, load-shedding, rate-limiting and
//! retry/backoff for free, without each sink re-deriving them.
//!
//! ## Layering
//!
//! The in-attempt controls are composed with tower's [`ServiceBuilder`]
//! (outer -> inner). With a static concurrency cap:
//!
//! ```text
//! [load-shed] -> rate-limit -> timeout -> concurrency-limit -> sender
//! ```
//!
//! With adaptive request concurrency (ARC, opt-in via
//! [`config::AdaptiveConfig`]) the AIMD limiter replaces the
//! static gate and wraps the TIMEOUT, so a timed-out or errored attempt feeds
//! its decrease and a fast success feeds its increase:
//!
//! ```text
//! [load-shed] -> rate-limit -> adaptive(AIMD) -> timeout -> sender
//! ```
//!
//! ARC discovers the downstream's safe concurrency from RTT/error feedback
//! (`min_limit` floors at 1 so a failing sink can never deadlock at zero). It is
//! the OUTBOUND sink limiter only -- kept distinct from the inbound worker-AIMD
//! so the two never double-regulate. At the limit an attempt parks on the
//! limiter's semaphore queue (no spin) for up to the attempt timeout, then fails
//! as transient backpressure that the retry loop re-sends -- saturation delays
//! delivery, never drops it.
//!
//! Retry/backoff wraps the whole composed service as the OUTERMOST control,
//! driven by `backon` (a closure-shaped retry loop -- the natural fit, and the
//! same retry-outermost shape proven elsewhere: each attempt is independently
//! timeout-bounded). Circuit-breaking is deliberately NOT a layer here: the
//! `TieredSink` already wraps its sink in its own breaker, and stacking a second
//! one would double-regulate.
//!
//! ## Delivery guarantee (at-least-once preserved)
//!
//! - Rate-limit and concurrency-limit only DELAY admission. They never drop.
//! - Retry re-sends the WHOLE batch on a transient failure (records are held
//!   behind an `Arc`, so a retry is a refcount bump, not a copy).
//! - A fatal error stops immediately (no point retrying a permanent failure).
//! - A `RetryAfter` is retried after the wait the destination named, capped at
//!   `max_backoff_ms`, in place of the schedule's own wait. It never adds an
//!   attempt beyond `max_retries`.
//! - A `Reject` is never retried. A rejected single record is dead-lettered,
//!   and a rejected batch is sent again one record at a time to find the ones
//!   refused, as a `FilteredDlq` batch is.
//! - Records the sender would or did dead-letter come back in
//!   [`SinkOutcome::dead_letters`], never counted as sent. The caller writes
//!   them to its DLQ and fires its commit tokens only on [`SendResult::Ok`]. Retries
//!   may re-deliver records a failed attempt already sent -- a prefix for the
//!   per-record default, any subset for Kafka's pipelined override
//!   (at-least-once: duplicates, never loss) -- identical to the transport's
//!   own `send_batch` contract.

mod adaptive;
mod config;

pub use adaptive::{AdaptiveLimiter, Outcome, Permit};
pub use config::{AdaptiveConfig, SinkStackConfig};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use tower::util::BoxCloneService;
use tower::{BoxError, Layer, Service, ServiceBuilder, ServiceExt};

use crate::governor::RateLimiter;
use crate::transport::{
    CommitToken, DeadLetter, DeadLetterReason, Record, Screened, SendResult, TransportError,
    TransportSender, WorkBatch,
};

/// The sender's screen: what it does with a record.
type Screen = Arc<dyn Fn(&Record) -> Screened + Send + Sync>;

/// A batch of records to deliver, shared cheaply across retry attempts.
///
/// Cloning is an `Arc` refcount bump, so a whole-batch retry never copies the
/// payload bytes.
#[derive(Debug, Clone)]
pub struct SinkBatch(Arc<Vec<Record>>);

impl SinkBatch {
    /// Wrap a batch of records for delivery through the stack.
    #[must_use]
    pub fn new(records: Vec<Record>) -> Self {
        Self(Arc::new(records))
    }

    /// Number of records in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Error surfaced by a single send attempt through the stack.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// Transient backpressure -- the attempt should be retried.
    #[error("sink transiently unavailable (backpressured)")]
    Transient,
    /// Permanent failure -- retrying will not help.
    #[error("sink fatal error: {0}")]
    Fatal(TransportError),
    /// The sender answered `FilteredDlq`: it dead-lettered a record of the
    /// batch. Retrying refuses the same record again.
    #[error("sink dead-lettered a record of the batch")]
    DeadLetter,
    /// The destination is busy and named the wait before the next attempt.
    #[error("sink asked for a retry after {0:?}")]
    RetryAfter(Duration),
    /// The destination refused the batch for good. Retrying refuses it again.
    #[error("sink rejected the batch: {0}")]
    Rejected(TransportError),
}

/// What a send through the stack did with a block.
#[derive(Debug)]
#[must_use = "route the dead letters to a DLQ before committing"]
pub struct SinkOutcome {
    /// The result for the records sent: `Ok`, `Backpressured`, `RetryAfter`
    /// or `Fatal`.
    pub result: SendResult,
    /// Records the sender would or did dead-letter, never sent. On `Ok` the
    /// caller writes them to its DLQ before it commits. On any other result
    /// it retries the block, which finds them again.
    pub dead_letters: Vec<DeadLetter>,
}

impl SinkOutcome {
    fn ok(dead_letters: Vec<DeadLetter>) -> Self {
        Self {
            result: SendResult::Ok,
            dead_letters,
        }
    }
}

// ---------------------------------------------------------------------------
// SinkService -- the one bridging shim: an enum-dispatch / trait sender,
// exposed as a tower Service so the standard tower layers can wrap it.
// ---------------------------------------------------------------------------

/// Tower [`Service`] adaptor over any [`TransportSender`].
///
/// `Request = SinkBatch`, `Response = ()`, `Error = SinkError`. This is the
/// single bridge between scalo's `send_batch` sender API and the tower layer
/// ecosystem -- everything else in the stack is a standard tower layer.
pub struct SinkService<S> {
    sender: Arc<S>,
    /// Whether every batch comes from the sender's screen, so it goes through
    /// `send_screened` rather than `send_batch`.
    screened: bool,
}

impl<S> SinkService<S> {
    /// Wrap a shared sender as a tower service.
    #[must_use]
    pub fn new(sender: Arc<S>) -> Self {
        Self {
            sender,
            screened: false,
        }
    }

    /// As [`new`](Self::new), for batches the sender's
    /// [`screen`](TransportSender::screen) kept: each goes through
    /// [`send_screened`](TransportSender::send_screened).
    fn screened(sender: Arc<S>) -> Self {
        Self {
            sender,
            screened: true,
        }
    }
}

// Hand-written Clone/Debug: deriving them would add a spurious `S: Clone` /
// `S: Debug` bound, but the sender is shared behind an `Arc` (never cloned by
// value), so the service is Clone regardless of `S`.
impl<S> Clone for SinkService<S> {
    fn clone(&self) -> Self {
        Self {
            sender: Arc::clone(&self.sender),
            screened: self.screened,
        }
    }
}

impl<S> std::fmt::Debug for SinkService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkService").finish_non_exhaustive()
    }
}

impl<S> Service<SinkBatch> for SinkService<S>
where
    S: TransportSender + 'static,
{
    type Response = ();
    type Error = SinkError;
    type Future = Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The sender applies its own internal backpressure inside send_batch.
        // Readiness here is unconditional.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: SinkBatch) -> Self::Future {
        let sender = Arc::clone(&self.sender);
        let screened = self.screened;
        Box::pin(async move {
            let sent = if screened {
                sender.send_screened(&req.0, None).await
            } else {
                sender.send_batch(&req.0).await
            };
            match sent {
                SendResult::Ok => Ok(()),
                SendResult::FilteredDlq => Err(SinkError::DeadLetter),
                SendResult::Backpressured => Err(SinkError::Transient),
                SendResult::RetryAfter(wait) => Err(SinkError::RetryAfter(wait)),
                SendResult::Reject(e) => Err(SinkError::Rejected(e)),
                SendResult::Fatal(e) => Err(SinkError::Fatal(e)),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Rate-limit layer -- a thin tower wrapper over the governor RateLimiter so
// the GCRA admission pacing composes into the ServiceBuilder like any layer.
// ---------------------------------------------------------------------------

/// Tower layer that paces admission through a [`RateLimiter`] (GCRA token
/// bucket). A disabled limiter (`rps == 0`) is a transparent no-op.
#[derive(Debug, Clone)]
pub struct RateLimitLayer {
    limiter: RateLimiter,
}

impl RateLimitLayer {
    /// Build the layer from a configured limiter.
    #[must_use]
    pub fn new(limiter: RateLimiter) -> Self {
        Self { limiter }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: self.limiter.clone(),
        }
    }
}

/// Service produced by [`RateLimitLayer`].
#[derive(Debug, Clone)]
pub struct RateLimitService<S> {
    inner: S,
    limiter: RateLimiter,
}

impl<S, Req> Service<Req> for RateLimitService<S>
where
    S: Service<Req> + Clone + Send + 'static,
    S::Future: Send,
    Req: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let limiter = self.limiter.clone();
        // Clone-and-swap so the readied `inner` moves into the future while a
        // fresh clone stays behind for the next poll_ready (standard tower idiom).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            limiter.acquire().await;
            inner.call(req).await
        })
    }
}

// ---------------------------------------------------------------------------
// SinkStack -- the assembled, retried delivery pipeline.
// ---------------------------------------------------------------------------

/// 0 means "no concurrency limit". Mapped to this large permit count so a
/// single boxed service type covers both configured and unlimited cases.
const UNLIMITED_CONCURRENCY: usize = 1 << 20;

/// Composed outbound sink-control pipeline. Clone is cheap (shared service).
#[derive(Clone)]
pub struct SinkStack {
    svc: BoxCloneService<SinkBatch, (), BoxError>,
    screen: Screen,
    backoff: ExponentialBuilder,
    max_backoff: Duration,
    max_retries: usize,
}

impl SinkStack {
    /// Build the stack around a shared sender per [`SinkStackConfig`].
    #[must_use]
    pub fn new<S>(sender: Arc<S>, cfg: &SinkStackConfig) -> Self
    where
        S: TransportSender + 'static,
    {
        let limiter = RateLimiter::new(cfg.rate_limit, "sink");
        let screened = Arc::clone(&sender);
        let screen: Screen = Arc::new(move |record| screened.screen(record));

        // In-attempt control core (Error = BoxError), one of two shapes:
        //
        // - ADAPTIVE (ARC): rate-limit -> adaptive -> timeout -> sender. The
        //   adaptive layer wraps the TIMEOUT so a timed-out (or errored) attempt
        //   feeds the AIMD decrease. A well-utilised success feeds the increase.
        //   Both layers keep Error = BoxError, so no error mapping is needed.
        // - FIXED: rate-limit -> timeout -> static concurrency gate -> sender.
        let core: BoxCloneService<SinkBatch, (), BoxError> = if let Some(ac) = cfg.adaptive {
            ServiceBuilder::new()
                .layer(RateLimitLayer::new(limiter))
                .layer(AdaptiveConcurrencyLayer::new(
                    ac.build_limiter(),
                    cfg.attempt_timeout(),
                ))
                .timeout(cfg.attempt_timeout())
                .service(SinkService::screened(sender))
                .boxed_clone()
        } else {
            let concurrency = if cfg.max_concurrency == 0 {
                UNLIMITED_CONCURRENCY
            } else {
                cfg.max_concurrency
            };
            ServiceBuilder::new()
                .layer(RateLimitLayer::new(limiter))
                .timeout(cfg.attempt_timeout())
                .concurrency_limit(concurrency)
                .service(SinkService::screened(sender))
                .boxed_clone()
        };

        // load-shed (optional, outermost in-attempt control): sheds when the
        // static concurrency gate is full. The adaptive gate always reports
        // ready and waits in `call`, so it never trips a shed. Both arms box to
        // the same service type.
        let svc = if cfg.load_shed {
            ServiceBuilder::new()
                .load_shed()
                .service(core)
                .boxed_clone()
        } else {
            core
        };

        Self {
            svc,
            screen,
            backoff: cfg.backoff(),
            max_backoff: Duration::from_millis(cfg.max_backoff_ms),
            max_retries: cfg.max_retries,
        }
    }

    /// Deliver a batch through the full stack, retrying transient failures with
    /// backoff.
    ///
    /// Records the sender's [`screen`](TransportSender::screen) would
    /// dead-letter are taken out before the send and returned in
    /// [`SinkOutcome::dead_letters`], and the ones an outbound `drop` filter
    /// takes are left out. The rest go through
    /// [`send_screened`](TransportSender::send_screened), so no outbound filter
    /// runs twice. A send the
    /// sender answers `FilteredDlq` names no record, so its records are sent
    /// again one at a time, and each it refuses joins the dead letters as
    /// [`DeadLetterReason::Rejected`]. Fire commit tokens only on
    /// [`SendResult::Ok`], once the dead letters are in the DLQ.
    pub async fn send_batch(&self, records: Vec<Record>) -> SinkOutcome {
        let mut dead_letters = Vec::new();
        let mut kept = Vec::with_capacity(records.len());
        for record in records {
            match (self.screen)(&record) {
                Screened::Send => kept.push(record),
                Screened::Dropped => {}
                Screened::DeadLetter(reason) => dead_letters.push(DeadLetter { record, reason }),
            }
        }
        if kept.is_empty() {
            return SinkOutcome::ok(dead_letters);
        }
        let batch = SinkBatch::new(kept);
        let result = self.deliver(batch.clone()).await;
        let refused = match result {
            SendResult::FilteredDlq => None,
            SendResult::Reject(e) => Some(e),
            result => {
                return SinkOutcome {
                    result,
                    dead_letters,
                };
            }
        };
        // A refused single record is already found: it is never sent again.
        if let (Some(e), [record]) = (&refused, batch.0.as_slice()) {
            reject(record.clone(), e, &mut dead_letters);
            return SinkOutcome::ok(dead_letters);
        }
        // Resending repeats the records already sent: duplicates, never loss.
        for record in batch.0.iter().cloned() {
            match self.deliver(SinkBatch::new(vec![record.clone()])).await {
                SendResult::Ok => {}
                SendResult::FilteredDlq => dead_letters.push(DeadLetter {
                    record,
                    reason: DeadLetterReason::Rejected,
                }),
                SendResult::Reject(e) => reject(record, &e, &mut dead_letters),
                failed => {
                    return SinkOutcome {
                        result: failed,
                        dead_letters,
                    };
                }
            }
        }
        SinkOutcome::ok(dead_letters)
    }

    /// One batch through the stack and its retries.
    async fn deliver(&self, batch: SinkBatch) -> SendResult {
        let svc = self.svc.clone();

        let attempt = || {
            // Clone the service per attempt so each retry has an owned, readied
            // handle (BoxCloneService clone is a cheap Arc bump).
            let mut svc = svc.clone();
            let batch = batch.clone();
            async move { svc.ready().await?.call(batch).await }
        };

        let max_wait = self.max_backoff;
        let result = attempt
            .retry(self.backoff.with_max_times(self.max_retries))
            .when(is_transient)
            .adjust(move |e: &BoxError, candidate| {
                crate::backoff::hinted_delay(asked_wait(e), candidate, max_wait)
            })
            .sleep(tokio::time::sleep)
            .notify(|e: &BoxError, wait: Duration| {
                tracing::debug!(error = %e, wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX), "sink stack retrying a batch");
                record_retry();
            })
            .await;

        match result {
            Ok(()) => SendResult::Ok,
            Err(e) => classify_final(e),
        }
    }

    /// Deliver a [`WorkBatch`]'s records through the stack -- the driver-path
    /// entry point.
    ///
    /// As the sink of a `BatchEngine` pipeline, hand the dead letters back
    /// through the block's `Refusals`, so the loop writes them to its DLQ
    /// before it releases the source:
    ///
    /// ```rust,ignore
    /// let stack = SinkStack::new(sender, &cfg);
    /// engine.pipeline(&receiver).run_with_pieces(process, |batch, pieces| {
    ///     let stack = stack.clone();
    ///     let refusals = pieces.refusals();
    ///     let records = batch.records.clone();
    ///     async move {
    ///         let sent = stack.send_batch(records).await;
    ///         for dead in &sent.dead_letters {
    ///             refusals.dead_letter(&dead.record, dead.reason.to_string());
    ///         }
    ///         match sent.result {
    ///             SendResult::Ok => Ok(()),
    ///             _ => Err(EngineError::Sink("sink stack could not deliver".into())),
    ///         }
    ///     }
    /// }).await?;
    /// ```
    ///
    /// `commit_tokens` are NOT sent -- they are the source's local ack concern.
    /// The records are cloned once into the retry-shared `Arc` (a `Bytes`
    /// refcount bump per record, not a payload copy).
    pub async fn send_workbatch<T: CommitToken>(&self, batch: &WorkBatch<T>) -> SinkOutcome {
        self.send_batch(batch.records.clone()).await
    }
}

impl std::fmt::Debug for SinkStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkStack")
            .field("max_retries", &self.max_retries)
            .finish_non_exhaustive()
    }
}

/// Tower layer applying adaptive request concurrency (ARC) via the AIMD
/// [`AdaptiveLimiter`]. Gating happens in `call` via the limiter's async acquire
/// (which parks on the semaphore queue -- no busy poll). A slot-wait that
/// exhausts `acquire_timeout` sheds as transient backpressure (retryable, no
/// loss). Keeps `Error = BoxError`, so it composes with the rest of the stack
/// without error mapping.
#[derive(Clone)]
struct AdaptiveConcurrencyLayer {
    limiter: Arc<AdaptiveLimiter>,
    acquire_timeout: Duration,
}

impl AdaptiveConcurrencyLayer {
    fn new(limiter: Arc<AdaptiveLimiter>, acquire_timeout: Duration) -> Self {
        Self {
            limiter,
            acquire_timeout,
        }
    }
}

impl<S> Layer<S> for AdaptiveConcurrencyLayer {
    type Service = AdaptiveConcurrencyService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        AdaptiveConcurrencyService {
            inner,
            limiter: Arc::clone(&self.limiter),
            acquire_timeout: self.acquire_timeout,
        }
    }
}

#[derive(Clone)]
struct AdaptiveConcurrencyService<S> {
    inner: S,
    limiter: Arc<AdaptiveLimiter>,
    acquire_timeout: Duration,
}

impl<S, Req> Service<Req> for AdaptiveConcurrencyService<S>
where
    S: Service<Req, Error = BoxError> + Clone + Send + 'static,
    S::Response: Send + 'static,
    S::Future: Send,
    Req: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Gating happens in `call` via the limiter's async acquire (parks on a
        // queue, no spin). Readiness here is unconditional.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let limiter = Arc::clone(&self.limiter);
        let acquire_timeout = self.acquire_timeout;
        // Clone-and-swap so the readied inner moves into the future.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            // Acquire a slot (parks without spinning). A slot-wait that exhausts
            // the timeout sheds as transient backpressure -- retryable, no drop.
            let Some(permit) = limiter.acquire_timeout(acquire_timeout).await else {
                return Err(Box::new(SinkError::Transient) as BoxError);
            };
            let result = inner.call(req).await;
            // Feed the loss-based AIMD: a timeout / backpressure is overload (the
            // limit should shrink). A permanent Fatal or a dead letter is NOT
            // overload, so it must not shrink the limit.
            let outcome = match &result {
                Ok(_) => Outcome::Success,
                Err(e) => {
                    if is_transient(e) {
                        Outcome::Overload
                    } else {
                        Outcome::Success
                    }
                }
            };
            limiter.record(outcome);
            drop(permit);
            result
        })
    }
}

/// Whether a boxed stack error is transient (retryable). `SinkError::Fatal`,
/// `SinkError::DeadLetter` and `SinkError::Rejected` are not. Timeout
/// (`Elapsed`), load-shed (`Overloaded`) and `RetryAfter` are transient and
/// worth another attempt.
fn is_transient(e: &BoxError) -> bool {
    !matches!(
        e.downcast_ref::<SinkError>(),
        Some(SinkError::Fatal(_) | SinkError::DeadLetter | SinkError::Rejected(_))
    )
}

/// The wait the destination asked for, when the attempt ended on one.
fn asked_wait(e: &BoxError) -> Option<Duration> {
    match e.downcast_ref::<SinkError>() {
        Some(SinkError::RetryAfter(wait)) => Some(*wait),
        _ => None,
    }
}

/// Dead-letter `record`, which the destination refused for good.
fn reject(record: Record, e: &TransportError, dead_letters: &mut Vec<DeadLetter>) {
    tracing::warn!(error = %e, bytes = record.payload.len(), "sink stack: the destination rejected a record; it goes to the DLQ");
    dead_letters.push(DeadLetter {
        record,
        reason: DeadLetterReason::Rejected,
    });
}

/// Map a terminal boxed error back to a [`SendResult`] for the caller's commit
/// decision. A fatal transport error is `Fatal` and a dead letter
/// `FilteredDlq`. An exhausted transient (timeout / overload / backpressure) is
/// `Backpressured` so the caller's outer loop can retry the whole block later
/// without losing data.
fn classify_final(e: BoxError) -> SendResult {
    match e.downcast::<SinkError>() {
        Ok(boxed) => match *boxed {
            SinkError::Fatal(te) => SendResult::Fatal(te),
            SinkError::DeadLetter => SendResult::FilteredDlq,
            SinkError::Transient => SendResult::Backpressured,
            SinkError::RetryAfter(wait) => SendResult::RetryAfter(wait),
            SinkError::Rejected(te) => SendResult::Reject(te),
        },
        Err(_) => SendResult::Backpressured,
    }
}

/// Emit the stack retry counter (no-op without the `metrics` feature).
fn record_retry() {
    #[cfg(feature = "metrics")]
    ::metrics::counter!("sink_stack_retries_total").increment(1);
}

#[cfg(test)]
mod tests;
