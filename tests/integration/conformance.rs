// Project:   scalo
// File:      tests/integration/conformance.rs
// Purpose:   Acked-records-arrive conformance of the engine and SourceAck loops
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Every record whose source ack is released arrives, under each fault the
//! harness injects: the engine pipeline builder over a pull-shaped and a
//! push-shaped source, and a hand-rolled `SourceAck` loop over the push one.

use std::sync::Arc;
use std::time::Duration;

use scalo::deployment::test_support::conformance::{Case, Fault, FaultSink, Ledger, Verdict};
use scalo::transport::{
    DeliveryStatus, Record, SendResult, SourceAck, TransportError, TransportReceiver,
    TransportSender, WorkBatch,
};
use scalo::worker::{BatchEngine, BatchProcessingConfig, EngineError};
use tokio_util::sync::CancellationToken;

/// Hand a block to the fault sink, mapping its answer onto the engine's retry rules.
async fn deliver(sink: &FaultSink, records: &[Record]) -> Result<(), EngineError> {
    match sink.send_batch(records).await {
        SendResult::Ok | SendResult::FilteredDlq => Ok(()),
        SendResult::Backpressured => Err(TransportError::Backpressure.into()),
        SendResult::Fatal(e) => Err(EngineError::Sink(e.to_string())),
    }
}

/// The engine pipeline builder, an identity process, the fault sink.
async fn engine_pipeline<R>(
    source: Arc<R>,
    sink: Arc<FaultSink>,
    _ledger: Ledger,
    shutdown: CancellationToken,
) where
    R: TransportReceiver + 'static,
{
    let engine = BatchEngine::new(BatchProcessingConfig::default());
    let _ = engine
        .pipeline(&*source)
        .shutdown(shutdown)
        .sender(&*sink)
        .run(Ok, |out: &WorkBatch<R::Token>| {
            let sink = Arc::clone(&sink);
            let records = out.records.clone();
            async move { deliver(&sink, &records).await }
        })
        .await;
}

/// Send one block until the sink takes it or refuses it for good; `None` on shutdown.
async fn send_block(
    sink: &FaultSink,
    records: &[Record],
    shutdown: &CancellationToken,
) -> Option<DeliveryStatus> {
    loop {
        match sink.send_batch(records).await {
            SendResult::Ok => return Some(DeliveryStatus::Delivered),
            SendResult::FilteredDlq => return Some(DeliveryStatus::Rejected),
            SendResult::Fatal(_) => return Some(DeliveryStatus::Errored),
            SendResult::Backpressured => {
                if shutdown.is_cancelled() {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

/// A hand-rolled receive loop: arm the source, send each block, release it
/// through `SourceAck`, stop on a record the sink refuses for good, and drain
/// the source at shutdown.
async fn source_ack_loop<R>(
    source: Arc<R>,
    sink: Arc<FaultSink>,
    _ledger: Ledger,
    shutdown: CancellationToken,
) where
    R: TransportReceiver + 'static,
{
    if let Some(control) = source.ack_control() {
        control.arm();
    }
    let mut draining = false;
    loop {
        if !draining && shutdown.is_cancelled() {
            draining = true;
            let _ = source.close().await;
        }
        let Ok(batch) = source.recv(100).await else {
            return;
        };
        if batch.is_empty() {
            continue;
        }
        let ack = SourceAck::new(&*source, batch.commit_tokens);
        let piece = ack.piece();
        let status = send_block(&sink, &batch.records, &shutdown).await;
        piece.report(status.unwrap_or(DeliveryStatus::Errored));
        let released = ack.release().await;
        if !matches!(released, Ok(s) if s.should_commit()) {
            return;
        }
    }
}

/// A broken loop that releases each block at receipt, before its send: the
/// shape the harness exists to catch.
async fn release_at_receipt<R>(
    source: Arc<R>,
    sink: Arc<FaultSink>,
    _ledger: Ledger,
    shutdown: CancellationToken,
) where
    R: TransportReceiver + 'static,
{
    while !shutdown.is_cancelled() {
        let Ok(batch) = source.recv(100).await else {
            return;
        };
        if batch.is_empty() {
            continue;
        }
        let _ = source
            .release(&batch.commit_tokens, DeliveryStatus::Delivered)
            .await;
        if send_block(&sink, &batch.records, &shutdown).await != Some(DeliveryStatus::Delivered) {
            return;
        }
    }
}

async fn pull(fault: Fault) -> Verdict {
    Case::new(fault)
        .run_pull(|source, sink, ledger, shutdown| engine_pipeline(source, sink, ledger, shutdown))
        .await
}

async fn push(fault: Fault) -> Verdict {
    Case::new(fault)
        .run_push(|source, sink, ledger, shutdown| source_ack_loop(source, sink, ledger, shutdown))
        .await
}

async fn push_via_builder(fault: Fault) -> Verdict {
    Case::new(fault)
        .run_push(|source, sink, ledger, shutdown| engine_pipeline(source, sink, ledger, shutdown))
        .await
}

/// Nothing acknowledged is lost, and after the restart every record was acknowledged.
#[track_caller]
fn assert_conforms(verdict: &Verdict) {
    verdict.assert_lossless();
    assert_eq!(
        verdict.unacknowledged, 0,
        "the restart drains every record: {verdict:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_pull_graceful_stop_under_traffic() {
    assert_conforms(&pull(Fault::GracefulStop).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_pull_kill_mid_batch() {
    assert_conforms(&pull(Fault::KillMidBatch).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_pull_downstream_refusing() {
    assert_conforms(&pull(Fault::DownstreamRefusing).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_pull_record_rejected_mid_stream() {
    assert_conforms(&pull(Fault::RejectMidStream).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_pull_two_instances_over_one_source() {
    assert_conforms(&pull(Fault::TwoInstances).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_ack_push_graceful_stop_under_traffic() {
    assert_conforms(&push(Fault::GracefulStop).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_ack_push_kill_mid_batch() {
    assert_conforms(&push(Fault::KillMidBatch).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_ack_push_downstream_refusing() {
    assert_conforms(&push(Fault::DownstreamRefusing).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_ack_push_record_rejected_mid_stream() {
    assert_conforms(&push(Fault::RejectMidStream).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_ack_push_two_instances_over_one_source() {
    assert_conforms(&push(Fault::TwoInstances).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_push_graceful_stop_under_traffic() {
    assert_conforms(&push_via_builder(Fault::GracefulStop).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_push_kill_mid_batch() {
    assert_conforms(&push_via_builder(Fault::KillMidBatch).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_push_downstream_refusing() {
    assert_conforms(&push_via_builder(Fault::DownstreamRefusing).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_push_record_rejected_mid_stream() {
    assert_conforms(&push_via_builder(Fault::RejectMidStream).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_push_two_instances_over_one_source() {
    assert_conforms(&push_via_builder(Fault::TwoInstances).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_harness_catches_a_pull_ack_released_before_delivery() {
    let verdict = Case::new(Fault::KillMidBatch)
        .run_pull(|source, sink, ledger, shutdown| {
            release_at_receipt(source, sink, ledger, shutdown)
        })
        .await;
    assert!(
        !verdict.is_lossless(),
        "the kill must show as loss: {verdict:?}"
    );
    assert!(
        verdict.lost.contains(&100),
        "the killed send carried marker 100"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_harness_catches_a_push_source_answered_at_enqueue() {
    // Never armed, so the push source answers each request as it is queued.
    let verdict = Case::new(Fault::KillMidBatch)
        .run_push(|source, sink, ledger, shutdown| {
            release_at_receipt(source, sink, ledger, shutdown)
        })
        .await;
    assert!(
        !verdict.is_lossless(),
        "the kill must show as loss: {verdict:?}"
    );
}
