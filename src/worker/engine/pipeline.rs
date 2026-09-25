// Project:   scalo
// File:      src/worker/engine/pipeline.rs
// Purpose:   Pipeline builder: the run loop that holds source acknowledgements
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The pipeline builder: `BatchEngine::pipeline(&receiver) ... .run(process, sink)`.
//!
//! The loop behind it holds each block's source acknowledgement until every
//! piece built from the block has reported, then releases it once through
//! [`TransportReceiver::release`] with the merged status. A piece is one sink
//! call (a whole block, or a sub-block when the governor streams), a dead-letter
//! write, or a piece the sink takes itself through [`BlockPieces`].
//!
//! - `acknowledgements.enabled` on (the default): the source is armed, and a
//!   push source answers its sender only on release.
//! - `acknowledgements.enabled: false`: the source is released at receipt,
//!   before the block is processed.
//! - A source with no acknowledgement (pipe, memory): released after the pieces,
//!   as the other run loops commit.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::driver::{
    CommitMode, DRAIN_IDLE_LIMIT, Delivery, LoopTicker, RecvCap, RetryWindow, SHUTDOWN_RETRY_LIMIT,
    SubBlockDrain, close_source, note_recovered, note_transient, recv_capped, settle_recv,
    stop_after_abandoned,
};
use super::{BatchEngine, EngineError, FilterDlqPolicy};
use crate::backoff::Backoff;
use crate::transport::ack::{
    AckControl, AckKind, DeadLetterReason, EffectiveGuarantee, SinkConfirmation, await_merged,
    give_up_at, merged_channel, release_abandoned,
};
use crate::transport::filter::FilteredDlqEntry;
use crate::transport::{
    BatchFinalizer, DeliveryStatus, PieceFinalizer, Record, TransportError, TransportReceiver,
    TransportSender, WorkBatch,
};

/// The ticker type of a pipeline with no periodic callback.
pub type NoTicker = fn() -> std::future::Ready<Result<(), EngineError>>;

/// A screen naming records the sink would dead-letter instead of sending.
type Screen<'a> = Box<dyn Fn(&Record) -> Option<DeadLetterReason> + Send + Sync + 'a>;

/// The `service` field of the dead letters the pipeline writes.
#[cfg(feature = "dlq")]
const DLQ_SERVICE: &str = "pipeline";

/// Builder for the run loop that holds source acknowledgements until delivery.
///
/// Built with [`BatchEngine::pipeline`]; see `docs/pipeline/acknowledgements.md`.
#[must_use = "a pipeline does nothing until run"]
pub struct Pipeline<'a, R, T = NoTicker> {
    engine: &'a BatchEngine,
    receiver: &'a R,
    shutdown: CancellationToken,
    commit: CommitMode,
    /// What the sink's `Ok` proves: set by `sender`, or declared with
    /// `sink_confirms` for a sink that is not a transport.
    confirms: Option<SinkConfirmation>,
    screen: Option<Screen<'a>>,
    ticker: Option<(Duration, T)>,
}

/// Hands a sink more pieces of the block it is writing: one per destination it
/// fans out to, or one for a write it confirms later. The block's source is
/// released only once each has reported; one dropped without a report counts
/// as `Errored`.
pub struct BlockPieces<'f> {
    finalizer: &'f BatchFinalizer,
}

impl BlockPieces<'_> {
    /// A piece of the block the sink is writing.
    #[must_use]
    pub fn piece(&self) -> PieceFinalizer {
        self.finalizer.piece()
    }
}

impl std::fmt::Debug for BlockPieces<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockPieces").finish_non_exhaustive()
    }
}

impl BatchEngine {
    /// Start a pipeline reading `receiver`.
    ///
    /// ```rust,ignore
    /// engine
    ///     .pipeline(&receiver)
    ///     .shutdown(shutdown)
    ///     .sender(&sender)
    ///     .run(|batch| Ok(batch), |out| send(out))
    ///     .await?;
    /// ```
    pub fn pipeline<'a, R: TransportReceiver>(&'a self, receiver: &'a R) -> Pipeline<'a, R> {
        Pipeline {
            engine: self,
            receiver,
            shutdown: CancellationToken::new(),
            commit: CommitMode::Auto,
            confirms: None,
            screen: None,
            ticker: None,
        }
    }
}

impl<'a, R: TransportReceiver, T> Pipeline<'a, R, T> {
    /// The token that stops the loop; it then drains the source as the other
    /// run loops do.
    pub fn shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Who releases the source: the engine (`Auto`, the default), or the sink,
    /// which then calls [`TransportReceiver::release`] itself for every block.
    pub fn commit(mut self, commit: CommitMode) -> Self {
        self.commit = commit;
        self
    }

    /// What the `Ok` of a sink that is not a transport proves, for
    /// `pipeline_delivery_guarantee`: a database writer that refuses a record
    /// itself, say. A transport sink uses [`sender`](Self::sender) instead,
    /// which sets this and screens the blocks too.
    pub fn sink_confirms(mut self, confirms: SinkConfirmation) -> Self {
        self.confirms = Some(confirms);
        self
    }

    /// The transport the sink writes to: its delivery confirmation, and the
    /// records it would dead-letter instead of sending, which the loop routes
    /// to the DLQ itself.
    pub fn sender<S: TransportSender>(mut self, sender: &'a S) -> Self {
        self.confirms = Some(sender.confirms_delivery());
        self.screen = Some(Box::new(move |record| sender.dead_letter_reason(record)));
        self
    }

    /// Run `tick` every `every` inside the loop (flush timers, maintenance).
    pub fn ticker<F, Fut>(self, every: Duration, tick: F) -> Pipeline<'a, R, F>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<(), EngineError>>,
    {
        Pipeline {
            engine: self.engine,
            receiver: self.receiver,
            shutdown: self.shutdown,
            commit: self.commit,
            confirms: self.confirms,
            screen: self.screen,
            ticker: Some((every, tick)),
        }
    }
}

impl<R, T, TFut> Pipeline<'_, R, T>
where
    R: TransportReceiver,
    T: FnMut() -> TFut,
    TFut: std::future::Future<Output = Result<(), EngineError>>,
{
    /// Run the loop until shutdown or a permanent error.
    ///
    /// `process` and `sink` take the shapes `run_governed` takes, and each
    /// sink call is one piece of its block.
    ///
    /// # Errors
    ///
    /// As [`run_with_pieces`](Self::run_with_pieces).
    pub async fn run<P, Sink, SinkFut>(self, process: P, mut sink: Sink) -> Result<(), EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        self.run_with_pieces(
            process,
            move |batch: &WorkBatch<R::Token>, _pieces: &BlockPieces<'_>| sink(batch),
        )
        .await
    }

    /// Run the loop with a sink that may take more pieces of each block through
    /// [`BlockPieces`], for a fan-out or a write it confirms later.
    ///
    /// # Errors
    ///
    /// As [`run_workbatch`](BatchEngine::run_workbatch), plus
    /// [`EngineError::Sink`] when a pull source's block ends `Errored` after
    /// its sink returned: a piece failed, the source is not released, and the
    /// loop stops so the block is read again. A push source's block ending
    /// `Errored` is released `Errored`, its sender retries, and the loop goes
    /// on. [`EngineError::SinkManagedUnsupported`] for
    /// [`CommitMode::SinkManaged`] while the governor streams sub-blocks.
    pub async fn run_with_pieces<P, Sink, SinkFut>(
        self,
        process: P,
        mut sink: Sink,
    ) -> Result<(), EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>, &BlockPieces<'_>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        let Pipeline {
            engine,
            receiver,
            shutdown,
            commit,
            confirms,
            screen,
            ticker,
        } = self;

        let control = receiver.ack_control();
        let mode = AckMode::of(control);
        if let (AckMode::Hold, Some(control)) = (mode, control) {
            control.arm();
        }
        EffectiveGuarantee::of(control, confirms.unwrap_or_default()).publish();
        if confirms.is_none() {
            tracing::warn!(
                transport = receiver.name(),
                "BatchEngine (pipeline) has neither a sender nor a declared sink confirmation, so \
                 nothing screens its blocks: a record a Kafka or gRPC sink would dead-letter (over \
                 its size ceiling, or matched by an outbound dlq filter) is dropped by send_batch \
                 and its block released as delivered. Pass .sender(&sender) for a transport sink, \
                 or .sink_confirms(..) for one that is not"
            );
        }

        #[cfg(feature = "governor")]
        let budget = engine.byte_budget.clone();
        #[cfg(feature = "governor")]
        if budget.is_some() && matches!(commit, CommitMode::SinkManaged) {
            return Err(EngineError::SinkManagedUnsupported);
        }

        tracing::info!(
            transport = receiver.name(),
            commit = ?commit,
            acknowledgements = mode.as_str(),
            "BatchEngine (pipeline) starting"
        );

        let retry = RetryWindow::new(&shutdown);
        let run = BlockRun {
            engine,
            receiver,
            mode,
            push: control.is_some_and(|c| c.kind() == AckKind::Push),
            commit,
            screen: screen.as_deref(),
            retry: &retry,
        };
        let mut ticker = LoopTicker::new(ticker);
        let mut recv_failures = 0_u32;
        #[cfg(feature = "governor")]
        let mut last_recv: Option<std::time::Instant> = None;

        loop {
            #[cfg(feature = "governor")]
            let (cap, sub_block_bytes) = match &budget {
                Some(budget) => (
                    RecvCap::Limits(crate::transport::RecvLimits {
                        max_records: engine.config.max_chunk_size.min(budget.record_cap()),
                        max_bytes: budget.byte_budget(),
                    }),
                    Some(budget.byte_budget()),
                ),
                None => (RecvCap::Records(engine.config.max_chunk_size), None),
            };
            #[cfg(not(feature = "governor"))]
            let (cap, sub_block_bytes) = (RecvCap::Records(engine.config.max_chunk_size), None);

            tokio::select! {
                biased;

                () = shutdown.cancelled() => {
                    tracing::info!("BatchEngine (pipeline) shutting down");
                    return run.drain(cap, sub_block_bytes, &process, &mut sink).await;
                }

                () = ticker.wait() => ticker.fire("pipeline").await,

                recv_result = recv_capped(receiver, cap) => {
                    #[cfg(feature = "governor")]
                    let ingest_interval = {
                        let now = std::time::Instant::now();
                        let interval = last_recv
                            .map(|prev| now.saturating_duration_since(prev))
                            .unwrap_or_default();
                        last_recv = Some(now);
                        interval
                    };
                    let Some(batch) =
                        settle_recv(recv_result, &mut recv_failures, &shutdown).await?
                    else {
                        continue;
                    };
                    #[cfg(feature = "governor")]
                    let block_bytes = batch.total_payload_bytes() as u64;
                    #[cfg(feature = "governor")]
                    let started = std::time::Instant::now();

                    if !is_empty_block(&batch) {
                        let Delivery::Sunk = run
                            .drive(batch, sub_block_bytes, &process, &mut sink)
                            .await?
                        else {
                            return stop_after_abandoned(receiver).await;
                        };
                    }

                    #[cfg(feature = "governor")]
                    if let Some(budget) = &budget {
                        budget.observe(block_bytes, started.elapsed(), ingest_interval);
                    }
                }
            }
        }
    }
}

/// A block with nothing to process, nothing to dead-letter and nothing to
/// release.
fn is_empty_block<T: crate::transport::CommitToken>(batch: &WorkBatch<T>) -> bool {
    batch.records.is_empty() && batch.commit_tokens.is_empty() && batch.dlq_entries.is_empty()
}

/// How a pipeline treats its source's acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckMode {
    /// Held until every piece reported (`acknowledgements.enabled`).
    Hold,
    /// Released at receipt (`acknowledgements.enabled: false`).
    AtReceipt,
    /// The source has none; released after the pieces, as a commit.
    Unheld,
}

impl AckMode {
    fn of(control: Option<&dyn AckControl>) -> Self {
        match control {
            Some(control) if control.enabled() => Self::Hold,
            Some(_) => Self::AtReceipt,
            None => Self::Unheld,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Hold => "held",
            Self::AtReceipt => "released_at_receipt",
            Self::Unheld => "none",
        }
    }
}

/// How one sink call ended.
enum SinkEnd {
    /// The sink took it.
    Sunk,
    /// The source's hold deadline came first.
    Expired,
    /// The sink still refused it when the retry window after shutdown closed.
    Abandoned,
}

/// Sleep until `at`, or forever when there is none.
async fn until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// One dead letter and, for a record the sink would refuse, why.
struct DeadLetter {
    entry: FilteredDlqEntry,
    screened: Option<&'static str>,
}

/// The `reason` of a dropped dead letter an inbound filter or `process`
/// produced, rather than one the sink would refuse.
const PRODUCED_DEAD_LETTER: &str = "dead_letter";

/// Count one dead letter dropped with nowhere to go.
fn count_dropped_dead_letter(reason: &'static str) {
    #[cfg(feature = "metrics")]
    metrics::counter!("pipeline_dead_letters_dropped_total", "reason" => reason).increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = reason;
}

/// Release `tokens` with `status`. A failure is logged and counted, since the
/// block is already delivered or already abandoned.
async fn release_source<R: TransportReceiver>(
    receiver: &R,
    tokens: &[R::Token],
    status: DeliveryStatus,
) {
    if let Err(e) = receiver.release(tokens, status).await {
        #[cfg(feature = "metrics")]
        metrics::counter!("transport_commit_errors_total", "transport" => receiver.name())
            .increment(1);
        tracing::warn!(
            error = %e,
            transport = receiver.name(),
            "Releasing the source failed; carrying on"
        );
    }
}

/// A block's source tokens, from before `process` runs until the block is
/// released.
///
/// A held block dropped unreleased -- a panic in `process` or the sink, or a
/// run future dropped mid-block -- is released `Errored`, so a push source
/// answers its sender and frees what the request held instead of keeping it
/// until the transport goes.
struct HeldBlock<'r, R: TransportReceiver> {
    receiver: &'r R,
    tokens: Vec<R::Token>,
    /// Released `Errored` on drop, until the block is released or let go.
    held: bool,
}

impl<R: TransportReceiver> HeldBlock<'_, R> {
    /// Release the block with `status`. It stays held until the release
    /// completes.
    async fn release(mut self, status: DeliveryStatus) {
        release_source(self.receiver, &self.tokens, status).await;
        self.held = false;
    }

    /// Leave the release to the sink (`SinkManaged`) or to the receipt
    /// release that already happened.
    fn let_go(mut self) {
        self.held = false;
    }
}

impl<R: TransportReceiver> Drop for HeldBlock<'_, R> {
    fn drop(&mut self) {
        if self.held {
            release_abandoned(self.receiver, &self.tokens);
        }
    }
}

/// Everything a block needs from the run it belongs to.
struct BlockRun<'r, R> {
    engine: &'r BatchEngine,
    receiver: &'r R,
    mode: AckMode,
    push: bool,
    commit: CommitMode,
    screen: Option<&'r (dyn Fn(&Record) -> Option<DeadLetterReason> + Send + Sync)>,
    retry: &'r RetryWindow<'r>,
}

impl<R: TransportReceiver> BlockRun<'_, R> {
    /// Drive one block through process, dead letters and sink, then release its
    /// source with the merged status of every piece.
    async fn drive<P, Sink, SinkFut>(
        &self,
        mut batch: WorkBatch<R::Token>,
        sub_block_bytes: Option<u64>,
        process: &P,
        sink: &mut Sink,
    ) -> Result<Delivery, EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>, &BlockPieces<'_>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        if self.mode == AckMode::AtReceipt {
            self.release(&batch.commit_tokens, DeliveryStatus::Delivered)
                .await;
        }
        let hold_deadline = match self.mode {
            AckMode::Hold => self.receiver.hold_deadline(&batch.commit_tokens),
            AckMode::AtReceipt | AckMode::Unheld => None,
        };
        let give_up = hold_deadline.map(give_up_at);

        // The whole-block sink sees the tokens, which `SinkManaged` releases from, and sub-blocks carry none.
        let tokens = match sub_block_bytes {
            None => batch.commit_tokens.clone(),
            Some(_) => std::mem::take(&mut batch.commit_tokens),
        };
        let block = HeldBlock {
            receiver: self.receiver,
            tokens,
            held: self.mode == AckMode::Hold,
        };

        let (finalizer, merged) = merged_channel();
        let pieces = BlockPieces {
            finalizer: &finalizer,
        };
        let inbound: Vec<DeadLetter> = std::mem::take(&mut batch.dlq_entries)
            .into_iter()
            .map(|entry| DeadLetter {
                entry,
                screened: None,
            })
            .collect();

        let driven = match sub_block_bytes {
            None => {
                self.drive_whole(batch, inbound, process, sink, &pieces, give_up)
                    .await
            }
            Some(bytes) => {
                self.drive_sub_blocks(batch, inbound, bytes, process, sink, &pieces, give_up)
                    .await
            }
        };
        let end = match driven {
            Ok(end) => end,
            Err(e) => {
                self.release_errored(block).await;
                return Err(e);
            }
        };
        if let SinkEnd::Abandoned = end {
            self.release_errored(block).await;
            return Ok(Delivery::Abandoned);
        }

        finalizer.seal();
        let status = tokio::select! {
            biased;
            status = await_merged(merged, hold_deadline) => Some(status),
            () = self.retry.closed() => None,
        };
        let Some(status) = status else {
            self.release_errored(block).await;
            return Ok(Delivery::Abandoned);
        };

        if matches!(self.commit, CommitMode::Auto) && self.mode != AckMode::AtReceipt {
            block.release(status).await;
        } else {
            block.let_go();
        }
        if status == DeliveryStatus::Errored && !self.push && self.mode != AckMode::AtReceipt {
            return Err(EngineError::Sink(
                "a piece of the block was not delivered; its source is not released, so it is \
                 read again after a restart"
                    .into(),
            ));
        }
        Ok(Delivery::Sunk)
    }

    /// The whole block in one sink call.
    async fn drive_whole<P, Sink, SinkFut>(
        &self,
        batch: WorkBatch<R::Token>,
        mut dead: Vec<DeadLetter>,
        process: &P,
        sink: &mut Sink,
        pieces: &BlockPieces<'_>,
        give_up: Option<tokio::time::Instant>,
    ) -> Result<SinkEnd, EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>, &BlockPieces<'_>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        #[cfg(feature = "memory")]
        let _lease = self.engine.lease_ingress_batch(&batch);
        let mut out = process(batch)?;
        dead.extend(
            std::mem::take(&mut out.dlq_entries)
                .into_iter()
                .map(|entry| DeadLetter {
                    entry,
                    screened: None,
                }),
        );
        self.screen_out(&mut out.records, &mut dead);
        match self.dead_letter(dead, pieces, give_up).await? {
            SinkEnd::Sunk => {}
            end => return Ok(end),
        }
        if out.records.is_empty() {
            return Ok(SinkEnd::Sunk);
        }
        let piece = pieces.piece();
        match self.sink_held(sink, &out, pieces, give_up).await {
            Ok(SinkEnd::Sunk) => {
                piece.report(DeliveryStatus::Delivered);
                Ok(SinkEnd::Sunk)
            }
            Ok(SinkEnd::Expired) => {
                piece.report(DeliveryStatus::Errored);
                Ok(SinkEnd::Expired)
            }
            Ok(SinkEnd::Abandoned) => Ok(SinkEnd::Abandoned),
            Err(e) => {
                tracing::error!(error = %e, "Sink failed (pipeline) -- terminal, stopping the run loop");
                Err(e)
            }
        }
    }

    /// The block in sub-blocks of about `sub_block_bytes`, one piece each.
    #[allow(clippy::too_many_arguments)]
    async fn drive_sub_blocks<P, Sink, SinkFut>(
        &self,
        batch: WorkBatch<R::Token>,
        dead: Vec<DeadLetter>,
        sub_block_bytes: u64,
        process: &P,
        sink: &mut Sink,
        pieces: &BlockPieces<'_>,
        give_up: Option<tokio::time::Instant>,
    ) -> Result<SinkEnd, EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>, &BlockPieces<'_>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        let WorkBatch { records, .. } = batch;
        match self.dead_letter(dead, pieces, give_up).await? {
            SinkEnd::Sunk => {}
            end => return Ok(end),
        }
        let mut sub_blocks = SubBlockDrain::new(records, sub_block_bytes);
        while let Some(sub_records) = sub_blocks.next_sub_block() {
            let sub_block: WorkBatch<R::Token> = WorkBatch::from_records(sub_records);
            #[cfg(feature = "memory")]
            let _lease = self.engine.lease_ingress_batch(&sub_block);
            let mut out = process(sub_block)?;
            let mut dead: Vec<DeadLetter> = std::mem::take(&mut out.dlq_entries)
                .into_iter()
                .map(|entry| DeadLetter {
                    entry,
                    screened: None,
                })
                .collect();
            self.screen_out(&mut out.records, &mut dead);
            match self.dead_letter(dead, pieces, give_up).await? {
                SinkEnd::Sunk => {}
                end => return Ok(end),
            }
            if out.records.is_empty() {
                continue;
            }
            let piece = pieces.piece();
            match self.sink_held(sink, &out, pieces, give_up).await {
                Ok(SinkEnd::Sunk) => piece.report(DeliveryStatus::Delivered),
                Ok(SinkEnd::Expired) => {
                    piece.report(DeliveryStatus::Errored);
                    return Ok(SinkEnd::Expired);
                }
                Ok(SinkEnd::Abandoned) => return Ok(SinkEnd::Abandoned),
                Err(e) => {
                    tracing::error!(error = %e, "Sink failed (pipeline streaming) -- terminal, stopping the run loop");
                    return Err(e);
                }
            }
        }
        Ok(SinkEnd::Sunk)
    }

    /// Move out of `records` every record the sink would dead-letter.
    fn screen_out(&self, records: &mut Vec<Record>, dead: &mut Vec<DeadLetter>) {
        let Some(screen) = self.screen else {
            return;
        };
        let mut kept = Vec::with_capacity(records.len());
        for record in std::mem::take(records) {
            match screen(&record) {
                None => kept.push(record),
                Some(reason) => dead.push(DeadLetter {
                    entry: FilteredDlqEntry {
                        payload: record.payload.to_vec(),
                        key: record.key.clone(),
                        reason: reason.to_string(),
                    },
                    screened: Some(reason.as_str()),
                }),
            }
        }
        *records = kept;
    }

    /// Write `dead` as one piece of the block: `Rejected` once the DLQ holds it,
    /// `Dropped` for an entry no DLQ backend can ever hold.
    ///
    /// A DLQ write that fails is retried with backoff, holding the block, until
    /// it lands, the hold deadline comes (`Expired`, the piece `Errored`), or
    /// the retry window after shutdown closes (`Abandoned`). A DLQ whose drain
    /// has exited answers `Errored` at once.
    ///
    /// Without a DLQ, entries from filters and `process` go through the
    /// [`FilterDlqPolicy`] as the other run loops route them, and records the
    /// sink would refuse are dropped and counted in
    /// `pipeline_dead_letters_dropped_total`. A disabled DLQ drops them all and
    /// counts each there.
    async fn dead_letter(
        &self,
        dead: Vec<DeadLetter>,
        pieces: &BlockPieces<'_>,
        give_up: Option<tokio::time::Instant>,
    ) -> Result<SinkEnd, EngineError> {
        if dead.is_empty() {
            return Ok(SinkEnd::Sunk);
        }
        let piece = pieces.piece();

        #[cfg(feature = "dlq")]
        if let Some(dlq) = &self.engine.dlq {
            let (status, end) = self.dead_letter_to(dlq, dead, give_up).await;
            if let Some(status) = status {
                piece.report(status);
            }
            return Ok(end);
        }
        #[cfg(not(feature = "dlq"))]
        let _ = give_up;

        let (screened, routed): (Vec<DeadLetter>, Vec<DeadLetter>) =
            dead.into_iter().partition(|d| d.screened.is_some());
        let route_policy = matches!(self.engine.filter_dlq_policy, FilterDlqPolicy::Route(_));
        let mut status = DeliveryStatus::Delivered;
        if !routed.is_empty() {
            self.engine
                .route_dlq_entries(routed.into_iter().map(|d| d.entry).collect())?;
            status = status.max(if route_policy {
                DeliveryStatus::Rejected
            } else {
                DeliveryStatus::Dropped
            });
        }
        if !screened.is_empty() {
            if route_policy {
                self.engine
                    .route_dlq_entries(screened.into_iter().map(|d| d.entry).collect())?;
                status = status.max(DeliveryStatus::Rejected);
            } else {
                for dropped in &screened {
                    count_dropped_dead_letter(dropped.screened.unwrap_or(PRODUCED_DEAD_LETTER));
                    tracing::warn!(
                        reason = %dropped.entry.reason,
                        "No DLQ is configured: a record the sink would refuse is dropped"
                    );
                }
                status = status.max(DeliveryStatus::Dropped);
            }
        }
        piece.report(status);
        Ok(SinkEnd::Sunk)
    }

    /// The DLQ half of [`dead_letter`](Self::dead_letter): the piece's status,
    /// `None` when the block is abandoned, and how the write ended.
    #[cfg(feature = "dlq")]
    async fn dead_letter_to(
        &self,
        dlq: &crate::dlq::Dlq,
        dead: Vec<DeadLetter>,
        give_up: Option<tokio::time::Instant>,
    ) -> (Option<DeliveryStatus>, SinkEnd) {
        let enabled = dlq.is_enabled();
        let mut status = DeliveryStatus::Delivered;
        let mut disabled_reasons: Vec<&'static str> = Vec::new();
        let mut entries: Vec<crate::dlq::DlqEntry> = Vec::with_capacity(dead.len());
        for d in dead {
            let entry = crate::dlq::DlqEntry::new(DLQ_SERVICE, d.entry.reason, d.entry.payload);
            let entry = match d.entry.key {
                Some(key) => entry.with_destination(key.as_ref()),
                None => entry,
            };
            if !enabled {
                disabled_reasons.push(d.screened.unwrap_or(PRODUCED_DEAD_LETTER));
            } else if let Some(refused) = dlq.refusal(&entry) {
                count_dropped_dead_letter(refused.as_str());
                tracing::warn!(
                    reason = refused.as_str(),
                    "No DLQ backend can hold this dead letter; it is dropped"
                );
                status = status.max(DeliveryStatus::Dropped);
                continue;
            }
            entries.push(entry);
        }
        if entries.is_empty() {
            return (Some(status), SinkEnd::Sunk);
        }

        let mut failures = 0_u32;
        loop {
            let attempt = dlq.write_confirmed(entries.clone());
            let written = match give_up {
                Some(at) => match tokio::time::timeout_at(at, attempt).await {
                    Ok(written) => written,
                    Err(_elapsed) => return (Some(DeliveryStatus::Errored), SinkEnd::Expired),
                },
                None => attempt.await,
            };
            match written {
                Ok(()) if enabled => {
                    note_recovered("dlq", failures);
                    return (Some(status.max(DeliveryStatus::Rejected)), SinkEnd::Sunk);
                }
                Ok(()) => {
                    for reason in disabled_reasons {
                        count_dropped_dead_letter(reason);
                    }
                    return (Some(status.max(DeliveryStatus::Dropped)), SinkEnd::Sunk);
                }
                Err(e @ crate::dlq::DlqError::Closed) => {
                    tracing::warn!(error = %e, "The DLQ has shut down; the block is not released");
                    return (Some(DeliveryStatus::Errored), SinkEnd::Sunk);
                }
                Err(e) => {
                    failures = failures.saturating_add(1);
                    note_transient("dlq", &e, failures);
                    tokio::select! {
                        biased;
                        () = self.retry.closed() => return (None, SinkEnd::Abandoned),
                        () = until(give_up) => return (Some(DeliveryStatus::Errored), SinkEnd::Expired),
                        () = tokio::time::sleep(Backoff::TRANSIENT.delay(failures)) => {}
                    }
                }
            }
        }
    }

    /// Sink `batch`, retrying a transient failure, until it is taken, the hold
    /// deadline comes, or the retry window after shutdown closes.
    async fn sink_held<Sink, SinkFut>(
        &self,
        sink: &mut Sink,
        batch: &WorkBatch<R::Token>,
        pieces: &BlockPieces<'_>,
        give_up: Option<tokio::time::Instant>,
    ) -> Result<SinkEnd, EngineError>
    where
        Sink: FnMut(&WorkBatch<R::Token>, &BlockPieces<'_>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        let mut failures = 0_u32;
        loop {
            let attempt = sink(batch, pieces);
            let result = match give_up {
                Some(at) => match tokio::time::timeout_at(at, attempt).await {
                    Ok(result) => result,
                    Err(_elapsed) => return Ok(SinkEnd::Expired),
                },
                None => attempt.await,
            };
            match result {
                Ok(()) => {
                    note_recovered("sink", failures);
                    return Ok(SinkEnd::Sunk);
                }
                Err(e) if e.is_transient() => {
                    failures = failures.saturating_add(1);
                    note_transient("sink", &e, failures);
                    tokio::select! {
                        biased;
                        () = self.retry.closed() => return Ok(SinkEnd::Abandoned),
                        () = until(give_up) => return Ok(SinkEnd::Expired),
                        () = tokio::time::sleep(Backoff::TRANSIENT.delay(failures)) => {}
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Release `tokens` with `status`, as [`release_source`] does.
    async fn release(&self, tokens: &[R::Token], status: DeliveryStatus) {
        release_source(self.receiver, tokens, status).await;
    }

    /// Release a held block `Errored`, so a push source answers its sender at
    /// once rather than at the deadline.
    async fn release_errored(&self, block: HeldBlock<'_, R>) {
        if self.mode == AckMode::Hold {
            block.release(DeliveryStatus::Errored).await;
        } else {
            block.let_go();
        }
    }

    /// Close the source, then drive every block it still returns until `recv`
    /// reports `Closed`: the pipeline half of the run loops' shutdown drain.
    async fn drain<P, Sink, SinkFut>(
        &self,
        cap: RecvCap,
        sub_block_bytes: Option<u64>,
        process: &P,
        sink: &mut Sink,
    ) -> Result<(), EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>, &BlockPieces<'_>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        self.retry.mark_shutdown_seen();
        close_source(self.receiver).await;

        let mut drained = 0_usize;
        let mut idle_since = tokio::time::Instant::now();
        let mut idle_polls = 0_u32;
        loop {
            let remaining = DRAIN_IDLE_LIMIT.saturating_sub(idle_since.elapsed());
            let batch = match tokio::time::timeout(remaining, recv_capped(self.receiver, cap)).await
            {
                Ok(Err(TransportError::Closed)) => break,
                Ok(Err(e)) if e.is_recoverable() => None,
                Ok(Err(e)) => return Err(EngineError::Transport(e)),
                Ok(Ok(batch)) => (!is_empty_block(&batch)).then_some(batch),
                Err(_elapsed) => None,
            };

            if let Some(batch) = batch {
                let records = batch.records.len();
                if let Delivery::Abandoned =
                    self.drive(batch, sub_block_bytes, process, sink).await?
                {
                    tracing::warn!(
                        transport = self.receiver.name(),
                        drained,
                        retry_limit = ?SHUTDOWN_RETRY_LIMIT,
                        "Shutdown drain stopped at a block the sink still refused when the \
                         retry window closed; it is not released"
                    );
                    return Ok(());
                }
                drained = drained.saturating_add(records);
                idle_since = tokio::time::Instant::now();
                idle_polls = 0;
                continue;
            }

            let remaining = DRAIN_IDLE_LIMIT.saturating_sub(idle_since.elapsed());
            if remaining.is_zero() {
                tracing::warn!(
                    transport = self.receiver.name(),
                    drained,
                    idle_limit = ?DRAIN_IDLE_LIMIT,
                    "Shutdown drain gave up: the source returned nothing and never reported Closed"
                );
                return Ok(());
            }
            idle_polls = idle_polls.saturating_add(1);
            tokio::time::sleep(Backoff::TRANSIENT.delay(idle_polls).min(remaining)).await;
        }

        tracing::info!(
            transport = self.receiver.name(),
            drained,
            "Shutdown drain complete"
        );
        Ok(())
    }
}
