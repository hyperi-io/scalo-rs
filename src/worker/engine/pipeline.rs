// Project:   scalo
// File:      src/worker/engine/pipeline.rs
// Purpose:   Pipeline builder: the run loop that holds source acknowledgements
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The pipeline builder: `BatchEngine::pipeline(&receiver) ... .run(process, sink)`.
//!
//! It runs the governed loop and holds each block's source acknowledgement
//! until every piece built from the block is delivered.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::driver::CommitMode;
use super::{BatchEngine, EngineError};
use crate::transport::ack::{DeadLetterReason, SinkConfirmation};
use crate::transport::{Record, TransportReceiver, TransportSender, WorkBatch};

/// The ticker type of a pipeline with no periodic callback.
pub type NoTicker = fn() -> std::future::Ready<Result<(), EngineError>>;

/// A screen naming records the sink would dead-letter instead of sending.
type Screen<'a> = Box<dyn Fn(&Record) -> Option<DeadLetterReason> + Send + Sync + 'a>;

/// Builder for the run loop that holds source acknowledgements until delivery.
///
/// Built with [`BatchEngine::pipeline`]; see `docs/pipeline/batch-engine.md`.
#[must_use = "a pipeline does nothing until run"]
pub struct Pipeline<'a, R, T = NoTicker> {
    engine: &'a BatchEngine,
    receiver: &'a R,
    shutdown: CancellationToken,
    commit: CommitMode,
    confirms: SinkConfirmation,
    screen: Option<Screen<'a>>,
    ticker: Option<(Duration, T)>,
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
            confirms: SinkConfirmation::None,
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

    /// Who releases the source: the engine (`Auto`, the default) or the sink.
    pub fn commit(mut self, commit: CommitMode) -> Self {
        self.commit = commit;
        self
    }

    /// What the sink's `Ok` proves, for `pipeline_delivery_guarantee`. Set by
    /// [`sender`](Self::sender) for a transport sink.
    pub fn sink_confirms(mut self, confirms: SinkConfirmation) -> Self {
        self.confirms = confirms;
        self
    }

    /// The transport the sink writes to: its delivery confirmation, and the
    /// records it would dead-letter instead of sending, which the loop routes
    /// to the DLQ itself.
    pub fn sender<S: TransportSender>(mut self, sender: &'a S) -> Self {
        self.confirms = sender.confirms_delivery();
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
    /// `process` and `sink` are those of
    /// [`run_governed`](BatchEngine::run_governed).
    ///
    /// # Errors
    ///
    /// As [`run_workbatch`](BatchEngine::run_workbatch).
    pub async fn run<P, Sink, SinkFut>(self, process: P, sink: Sink) -> Result<(), EngineError>
    where
        P: Fn(WorkBatch<R::Token>) -> Result<WorkBatch<R::Token>, EngineError>,
        Sink: FnMut(&WorkBatch<R::Token>) -> SinkFut,
        SinkFut: std::future::Future<Output = Result<(), EngineError>>,
    {
        let Pipeline {
            engine,
            receiver,
            shutdown,
            commit,
            confirms,
            screen: _screen,
            ticker,
        } = self;
        crate::transport::ack::EffectiveGuarantee::of(receiver.ack_control(), confirms).publish();
        #[cfg(feature = "governor")]
        {
            engine
                .run_governed(receiver, shutdown, process, sink, commit, ticker)
                .await
        }
        #[cfg(not(feature = "governor"))]
        {
            engine
                .run_workbatch(receiver, shutdown, process, sink, commit, ticker)
                .await
        }
    }
}
