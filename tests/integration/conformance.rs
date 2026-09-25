// Project:   scalo
// File:      tests/integration/conformance.rs
// Purpose:   Acked-records-arrive conformance of the engine and SourceAck loops
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Every record whose source ack is released arrives, under each fault the
//! harness injects: the engine pipeline builder over a pull-shaped source, a
//! push-shaped source built armed, and the real gRPC server built armed, and a
//! hand-rolled `SourceAck` loop over the push source.

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
    eprintln!("{verdict:?}");
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
    // Built unarmed and never armed, so the push source answers each request as it is queued.
    let verdict = Case::new(Fault::KillMidBatch)
        .push_armed(false)
        .run_push(|source, sink, ledger, shutdown| {
            release_at_receipt(source, sink, ledger, shutdown)
        })
        .await;
    assert!(
        !verdict.is_lossless(),
        "the kill must show as loss: {verdict:?}"
    );
}

/// The real gRPC receive server under the engine pipeline.
#[cfg(feature = "transport-grpc")]
mod grpc {
    use super::*;
    use scalo::deployment::test_support::conformance::{marked_payloads, marker_of};
    use scalo::transport::{GrpcConfig, GrpcTransport, PayloadFormat, RecordMeta};
    use std::sync::Mutex;

    type Current = Arc<Mutex<Option<Arc<GrpcTransport>>>>;

    /// A receive server, built armed or with the unarmed default.
    async fn server(armed: bool) -> Arc<GrpcTransport> {
        Arc::new(
            GrpcTransport::builder(&GrpcConfig::server("127.0.0.1:0"))
                .armed(armed)
                .start()
                .await
                .expect("server"),
        )
    }

    /// A client of `server` that gives up on a send after 5 s.
    async fn client_of(server: &GrpcTransport) -> Arc<GrpcTransport> {
        let uri = format!("http://{}", server.local_addr().expect("bound"));
        let mut config = GrpcConfig::client(&uri);
        config.send_timeout_ms = 5_000;
        Arc::new(GrpcTransport::new(&config).await.expect("client"))
    }

    fn records(payloads: &[bytes::Bytes]) -> Vec<Record> {
        payloads
            .iter()
            .map(|p| Record {
                payload: p.clone(),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            })
            .collect()
    }

    /// Send each request of 10 records to whichever server is up, resending it
    /// until it is answered OK, and mark its records acknowledged then.
    async fn push_until_answered(current: Current, ledger: Ledger, n: u64) {
        for request in marked_payloads(n).chunks(10) {
            let block = records(request);
            loop {
                let client = current.lock().expect("current").clone();
                if let Some(client) = client
                    && matches!(client.send_batch(&block).await, SendResult::Ok)
                {
                    for m in request.iter().filter_map(|p| marker_of(p)) {
                        ledger.acked(m);
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    async fn poll_until(within: Duration, mut ready: impl FnMut() -> bool) -> bool {
        let until = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < until {
            if ready() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        ready()
    }

    /// A gRPC server built armed takes pushes before its pipeline runs, and is
    /// killed while the engine pipeline holds the send carrying the middle
    /// marker: every acknowledged record arrives after a new server takes over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_grpc_armed_kill_mid_batch() {
        let ledger = Ledger::with_records(200);
        let sink = FaultSink::new(ledger.clone());
        sink.hold_at(100);

        let first = server(true).await;
        let current: Current = Arc::new(Mutex::new(Some(client_of(&first).await)));
        let pushing = tokio::spawn(push_until_answered(
            Arc::clone(&current),
            ledger.clone(),
            200,
        ));
        // The listener is up before the pipeline runs, as in an app that builds its transport first.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let running = tokio::spawn(engine_pipeline(
            Arc::clone(&first),
            Arc::new(sink.instance(0)),
            ledger.clone(),
            CancellationToken::new(),
        ));

        assert!(
            poll_until(Duration::from_secs(30), || sink.held_by() == Some(0)).await,
            "the send carrying marker 100 never reached the sink"
        );
        // Kill: no new sends reach it, the pipeline task dies mid-send, the server goes.
        *current.lock().expect("current") = None;
        running.abort();
        let _ = running.await;
        drop(first);
        sink.unhold();

        let second = server(true).await;
        *current.lock().expect("current") = Some(client_of(&second).await);
        let shutdown = CancellationToken::new();
        let restarted = tokio::spawn(engine_pipeline(
            Arc::clone(&second),
            Arc::new(sink.instance(1)),
            ledger.clone(),
            shutdown.clone(),
        ));
        assert!(
            poll_until(Duration::from_secs(60), || pushing.is_finished()).await,
            "every request answered OK after the restart: {:?}",
            ledger.verdict()
        );
        shutdown.cancel();
        let _ = restarted.await;

        assert_conforms(&ledger.verdict());
    }

    /// The opt-out an app gets by not arming: the default server answers a
    /// push OK as soon as it is queued, before anything has received it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_unarmed_grpc_default_answers_at_enqueue() {
        let server = server(false).await;
        let client = client_of(&server).await;
        let block = records(&marked_payloads(3));

        let answered = client.send_batch(&block).await;
        assert!(
            matches!(answered, SendResult::Ok),
            "answered at enqueue: {answered:?}"
        );
        let control = server.ack_control().expect("ack-capable");
        assert!(!control.is_armed(), "built with the unarmed default");
        assert_eq!(control.held().count, 0, "nothing is held");

        // The answered records are still queued: nothing had delivered them.
        let mut received = 0;
        for _ in 0..100 {
            received += server.recv(10).await.expect("recv").records.len();
            if received == 3 {
                break;
            }
        }
        assert_eq!(received, 3, "all three queued after their OK");
    }
}
