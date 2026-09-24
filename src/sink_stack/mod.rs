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
//! so the two never double-regulate. NOTE: the limiter backpressures (never
//! drops) when saturated, but its readiness check currently busy-polls while at
//! the limit; pair it with `load_shed` (or a non-zero `min_limit` headroom) for
//! sustained-overload deployments.
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
//! - Rate-limit and concurrency-limit only DELAY admission; they never drop.
//! - Retry re-sends the WHOLE batch on a transient failure (records are held
//!   behind an `Arc`, so a retry is a refcount bump, not a copy).
//! - A fatal error stops immediately (no point retrying a permanent failure).
//! - The method returns a [`SendResult`]; the caller fires its commit tokens
//!   only on [`SendResult::Ok`], exactly as for a bare `send_batch`. Retries
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
    CommitToken, Record, SendResult, TransportError, TransportSender, WorkBatch,
};

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
}

impl<S> SinkService<S> {
    /// Wrap a shared sender as a tower service.
    #[must_use]
    pub fn new(sender: Arc<S>) -> Self {
        Self { sender }
    }
}

// Hand-written Clone/Debug: deriving them would add a spurious `S: Clone` /
// `S: Debug` bound, but the sender is shared behind an `Arc` (never cloned by
// value), so the service is Clone regardless of `S`.
impl<S> Clone for SinkService<S> {
    fn clone(&self) -> Self {
        Self {
            sender: Arc::clone(&self.sender),
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
        // The sender applies its own internal backpressure inside send_batch;
        // readiness here is unconditional.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: SinkBatch) -> Self::Future {
        let sender = Arc::clone(&self.sender);
        Box::pin(async move {
            match sender.send_batch(&req.0).await {
                // Sent, or handled by an outbound dlq/drop filter -- both Ok.
                SendResult::Ok | SendResult::FilteredDlq => Ok(()),
                SendResult::Backpressured => Err(SinkError::Transient),
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

/// 0 means "no concurrency limit"; mapped to this large permit count so a
/// single boxed service type covers both configured and unlimited cases.
const UNLIMITED_CONCURRENCY: usize = 1 << 20;

/// Composed outbound sink-control pipeline. Clone is cheap (shared service).
#[derive(Clone)]
pub struct SinkStack {
    svc: BoxCloneService<SinkBatch, (), BoxError>,
    backoff: ExponentialBuilder,
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

        // In-attempt control core (Error = BoxError), one of two shapes:
        //
        // - ADAPTIVE (ARC): rate-limit -> adaptive -> timeout -> sender. The
        //   adaptive layer wraps the TIMEOUT so a timed-out (or errored) attempt
        //   feeds the AIMD decrease; a well-utilised success feeds the increase.
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
                .service(SinkService::new(sender))
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
                .service(SinkService::new(sender))
                .boxed_clone()
        };

        // load-shed (optional, outermost in-attempt control): when the
        // concurrency gate (static or adaptive) is full, shed immediately
        // instead of queueing. Both arms box to the same service type.
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
            backoff: cfg.backoff(),
            max_retries: cfg.max_retries,
        }
    }

    /// Deliver a batch through the full stack, retrying transient failures with
    /// backoff. Returns a [`SendResult`] with the same commit contract as a
    /// bare `send_batch`: fire commit tokens only on [`SendResult::Ok`].
    pub async fn send_batch(&self, records: Vec<Record>) -> SendResult {
        if records.is_empty() {
            return SendResult::Ok;
        }
        let batch = SinkBatch::new(records);
        let svc = self.svc.clone();

        let attempt = || {
            // Clone the service per attempt so each retry has an owned, readied
            // handle (BoxCloneService clone is a cheap Arc bump).
            let mut svc = svc.clone();
            let batch = batch.clone();
            async move { svc.ready().await?.call(batch).await }
        };

        let result = attempt
            .retry(self.backoff.with_max_times(self.max_retries))
            .when(is_transient)
            .sleep(tokio::time::sleep)
            .notify(|_e: &BoxError, _d: Duration| record_retry())
            .await;

        match result {
            Ok(()) => SendResult::Ok,
            Err(e) => classify_final(e),
        }
    }

    /// Deliver a [`WorkBatch`]'s records through the stack -- the driver-path
    /// entry point.
    ///
    /// The `worker` driver runs `get -> process -> sink -> commit` over a
    /// caller-supplied sink closure of shape
    /// `FnMut(&WorkBatch<T>) -> Future<Output = Result<(), EngineError>>`, and
    /// commits the source acks only when the sink returns `Ok`. Wrap a stack as
    /// that closure so every app gets retry/ARC/timeout/rate-limit on the
    /// outbound send while at-least-once is preserved (a non-`Ok` here skips the
    /// commit, so the block is redelivered):
    ///
    /// ```rust,ignore
    /// let stack = SinkStack::new(sender, &cfg);
    /// engine.run_workbatch(&receiver, shutdown, process, |batch| {
    ///     let stack = stack.clone();
    ///     let batch_records = batch; // borrowed for the call
    ///     async move {
    ///         match stack.send_workbatch(batch_records).await {
    ///             SendResult::Ok | SendResult::FilteredDlq => Ok(()),
    ///             _ => Err(EngineError::sink("sink stack could not deliver")),
    ///         }
    ///     }
    /// }, CommitMode::Auto, None).await?;
    /// ```
    ///
    /// `commit_tokens` are NOT sent -- they are the source's local ack concern,
    /// fired by the driver after this returns `Ok` (mirrors the bare
    /// `send_batch` contract). The records are cloned once into the retry-shared
    /// `Arc` (a `Bytes` refcount bump per record, not a payload copy).
    pub async fn send_workbatch<T: CommitToken>(&self, batch: &WorkBatch<T>) -> SendResult {
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
/// (which parks on the semaphore queue -- no busy poll); a slot-wait that
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
        // queue, no spin); readiness here is unconditional.
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
            // limit should shrink); a permanent Fatal is NOT overload, so it must
            // not shrink the limit for a non-congestion failure.
            let outcome = match &result {
                Ok(_) => Outcome::Success,
                Err(e) => {
                    if matches!(e.downcast_ref::<SinkError>(), Some(SinkError::Fatal(_))) {
                        Outcome::Success
                    } else {
                        Outcome::Overload
                    }
                }
            };
            limiter.record(outcome);
            drop(permit);
            result
        })
    }
}

/// Whether a boxed stack error is transient (retryable). A `SinkError::Fatal`
/// is the only non-retryable case; timeout (`Elapsed`) and load-shed
/// (`Overloaded`) are transient and worth another attempt.
fn is_transient(e: &BoxError) -> bool {
    !matches!(e.downcast_ref::<SinkError>(), Some(SinkError::Fatal(_)))
}

/// Map a terminal boxed error back to a [`SendResult`] for the caller's commit
/// decision. Only a fatal transport error is reported as `Fatal`; an exhausted
/// transient (timeout / overload / backpressure) is `Backpressured` so the
/// caller's outer loop can retry the whole block later without losing data.
fn classify_final(e: BoxError) -> SendResult {
    match e.downcast::<SinkError>() {
        Ok(boxed) => match *boxed {
            SinkError::Fatal(te) => SendResult::Fatal(te),
            SinkError::Transient => SendResult::Backpressured,
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
