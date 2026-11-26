// Project:   scalo
// File:      tests/integration/conformance.rs
// Purpose:   Acked-records-arrive conformance of the engine and SourceAck loops
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Every record whose source ack is released arrives, under each fault the
//! harness injects: the engine pipeline builder over a pull-shaped source, a
//! push-shaped source built armed, the real gRPC and HTTP servers built
//! armed and the real file source, and a hand-rolled `SourceAck` loop over
//! the push source. The same engine pipeline
//! sends through `AnySender::Custom` too, the path an app's own sender takes.

use std::sync::Arc;
use std::time::Duration;

use scalo::deployment::test_support::conformance::{Case, Fault, FaultSink, Ledger, Verdict};
use scalo::transport::{
    AnySender, DeliveryStatus, Record, SendResult, SourceAck, TransportBase, TransportError,
    TransportReceiver, TransportSender, WorkBatch,
};
use scalo::worker::{BatchEngine, BatchProcessingConfig, EngineError};
use tokio_util::sync::CancellationToken;

/// Hand a block to the sink, mapping its answer onto the engine's retry rules.
async fn deliver<S: TransportSender>(sink: &S, records: &[Record]) -> Result<(), EngineError> {
    match sink.send_batch(records).await {
        SendResult::Ok => Ok(()),
        // Names no record, so the block is never released as delivered.
        SendResult::FilteredDlq => Err(EngineError::Sink("the sink dead-lettered a record".into())),
        SendResult::Backpressured | SendResult::RetryAfter(_) => {
            Err(TransportError::Backpressure.into())
        }
        SendResult::Fatal(e) | SendResult::Reject(e) => Err(EngineError::Sink(e.to_string())),
    }
}

/// The engine pipeline builder, an identity process, the fault sink.
async fn engine_pipeline<R>(
    source: Arc<R>,
    sink: Arc<FaultSink>,
    ledger: Ledger,
    shutdown: CancellationToken,
) where
    R: TransportReceiver + 'static,
{
    engine_pipeline_via(source, sink, ledger, shutdown).await;
}

/// The engine pipeline sending through `AnySender::Custom` wrapping the fault sink.
async fn custom_pipeline<R>(
    source: Arc<R>,
    sink: Arc<FaultSink>,
    ledger: Ledger,
    shutdown: CancellationToken,
) where
    R: TransportReceiver + 'static,
{
    let custom = Arc::new(AnySender::Custom(sink));
    assert_eq!(custom.name(), "conformance-sink", "the custom arm forwards");
    engine_pipeline_via(source, custom, ledger, shutdown).await;
}

/// The engine pipeline builder, an identity process, any sender.
async fn engine_pipeline_via<R, S>(
    source: Arc<R>,
    sink: Arc<S>,
    _ledger: Ledger,
    shutdown: CancellationToken,
) where
    R: TransportReceiver + 'static,
    S: TransportSender + 'static,
{
    let engine = BatchEngine::new(BatchProcessingConfig::default());
    let _ = engine
        .pipeline(&*source)
        .shutdown(shutdown)
        .sender(&*sink)
        .run(Ok, |out: &WorkBatch<R::Token>| {
            let sink = Arc::clone(&sink);
            let records = out.records.clone();
            async move { deliver(&*sink, &records).await }
        })
        .await;
}

/// Send one block until the sink takes it or refuses it for good. `None` on shutdown.
async fn send_block(
    sink: &FaultSink,
    records: &[Record],
    shutdown: &CancellationToken,
) -> Option<DeliveryStatus> {
    loop {
        match sink.send_batch(records).await {
            SendResult::Ok => return Some(DeliveryStatus::Delivered),
            SendResult::FilteredDlq => return Some(DeliveryStatus::Rejected),
            SendResult::Fatal(_) | SendResult::Reject(_) => return Some(DeliveryStatus::Errored),
            SendResult::Backpressured | SendResult::RetryAfter(_) => {
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

async fn pull_custom(fault: Fault) -> Verdict {
    Case::new(fault)
        .run_pull(|source, sink, ledger, shutdown| custom_pipeline(source, sink, ledger, shutdown))
        .await
}

async fn push_custom(fault: Fault) -> Verdict {
    Case::new(fault)
        .run_push(|source, sink, ledger, shutdown| custom_pipeline(source, sink, ledger, shutdown))
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

/// Every fault over a pull source, with the sink behind `AnySender::Custom`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_pull_custom_sender_conforms_under_every_fault() {
    for fault in Fault::ALL {
        eprintln!("{fault:?}");
        assert_conforms(&pull_custom(fault).await);
    }
}

/// Every fault over a push source, with the sink behind `AnySender::Custom`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_push_custom_sender_conforms_under_every_fault() {
    for fault in Fault::ALL {
        eprintln!("{fault:?}");
        assert_conforms(&push_custom(fault).await);
    }
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

/// Poll `ready` every 5 ms until it holds or `within` passes. Whether it held.
#[cfg(any(
    feature = "transport-grpc",
    feature = "transport-file",
    all(feature = "transport-http", feature = "http-server")
))]
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

/// A real push server killed mid-batch under the engine pipeline: the steps
/// the gRPC and HTTP servers share.
#[cfg(any(
    feature = "transport-grpc",
    all(feature = "transport-http", feature = "http-server")
))]
mod push_server {
    use super::*;
    use scalo::deployment::test_support::conformance::{marked_payloads, marker_of};
    use scalo::transport::{PayloadFormat, RecordMeta};
    use std::future::Future;
    use std::sync::Mutex;

    /// The client the pushing task sends through, `None` while no server is up.
    type Current<C> = Arc<Mutex<Option<Arc<C>>>>;

    pub(super) fn records(payloads: &[bytes::Bytes]) -> Vec<Record> {
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
    async fn push_until_answered<C: TransportSender>(current: Current<C>, ledger: Ledger, n: u64) {
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

    /// A server built armed by `server` takes pushes before its pipeline runs,
    /// and is killed while the engine pipeline holds the send carrying the
    /// middle marker: every acknowledged record arrives after a new server
    /// takes over.
    pub(super) async fn armed_kill_mid_batch<R, C, SF, CF>(
        server: impl Fn() -> SF,
        client_of: impl Fn(Arc<R>) -> CF,
    ) where
        R: TransportReceiver + 'static,
        C: TransportSender + 'static,
        SF: Future<Output = Arc<R>>,
        CF: Future<Output = Arc<C>>,
    {
        let ledger = Ledger::with_records(200);
        let sink = FaultSink::new(ledger.clone());
        sink.hold_at(100);

        let first = server().await;
        let current: Current<C> = Arc::new(Mutex::new(Some(client_of(Arc::clone(&first)).await)));
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

        let second = server().await;
        *current.lock().expect("current") = Some(client_of(Arc::clone(&second)).await);
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

    /// The opt-out an app gets by not arming: `server`, built unarmed, answers
    /// a push OK as soon as it is queued, before anything has received it.
    pub(super) async fn unarmed_answers_at_enqueue<R, C>(server: &R, client: &C)
    where
        R: TransportReceiver,
        C: TransportSender,
    {
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

/// The real gRPC receive server under the engine pipeline.
#[cfg(feature = "transport-grpc")]
mod grpc {
    use super::push_server::{armed_kill_mid_batch, unarmed_answers_at_enqueue};
    use scalo::transport::{GrpcConfig, GrpcTransport};
    use std::sync::Arc;

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
    async fn client_of(server: Arc<GrpcTransport>) -> Arc<GrpcTransport> {
        let uri = format!("http://{}", server.local_addr().expect("bound"));
        let mut config = GrpcConfig::client(&uri);
        config.send_timeout_ms = 5_000;
        Arc::new(GrpcTransport::new(&config).await.expect("client"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_grpc_armed_kill_mid_batch() {
        armed_kill_mid_batch(|| server(true), client_of).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_unarmed_grpc_default_answers_at_enqueue() {
        let server = server(false).await;
        let client = client_of(Arc::clone(&server)).await;
        unarmed_answers_at_enqueue(&*server, &*client).await;
    }
}

/// The real HTTP receive server under the engine pipeline, each request one
/// NDJSON body of 10 records.
#[cfg(all(feature = "transport-http", feature = "http-server"))]
mod http {
    use super::push_server::{armed_kill_mid_batch, unarmed_answers_at_enqueue};
    use scalo::transport::AcknowledgementsConfig;
    use scalo::transport::http::{HttpTransport, HttpTransportConfig};
    use std::sync::Arc;

    fn ndjson(mut config: HttpTransportConfig) -> HttpTransportConfig {
        config.framing.framing_type = "ndjson".into();
        config
    }

    /// A receive server with acknowledgements on, built armed or with the
    /// unarmed default.
    async fn server(armed: bool) -> Arc<HttpTransport> {
        Arc::new(
            HttpTransport::builder(&ndjson(HttpTransportConfig::receiver("127.0.0.1:0")))
                .acknowledgements(AcknowledgementsConfig::new(true))
                .armed(armed)
                .start()
                .await
                .expect("server"),
        )
    }

    /// A client of `server` that gives up on a send after 5 s.
    async fn client_of(server: Arc<HttpTransport>) -> Arc<HttpTransport> {
        let url = format!("http://{}/ingest", server.local_addr().expect("bound"));
        let mut config = ndjson(HttpTransportConfig::sender(&url));
        config.send_timeout_ms = 5_000;
        Arc::new(HttpTransport::new(&config).await.expect("client"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_http_armed_kill_mid_batch() {
        armed_kill_mid_batch(|| server(true), client_of).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_unarmed_http_default_answers_at_enqueue() {
        let server = server(false).await;
        let client = client_of(Arc::clone(&server)).await;
        unarmed_answers_at_enqueue(&*server, &*client).await;
    }
}

/// The real file source under the engine pipeline, its acknowledgements on:
/// killed mid-batch with no close or flush, then a new reader over the same
/// file and sidecar.
#[cfg(feature = "transport-file")]
mod file {
    use super::*;
    use scalo::deployment::test_support::conformance::{marked_payloads, marker_of};
    use scalo::transport::AcknowledgementsConfig;
    use scalo::transport::file::{FileTransport, FileTransportConfig};
    use std::path::Path;

    /// The markers 0 to 199, as NDJSON lines.
    fn lines() -> Vec<Vec<u8>> {
        marked_payloads(200)
            .iter()
            .map(|p| [p.as_ref(), b"\n"].concat())
            .collect()
    }

    async fn reader(path: &Path) -> Arc<FileTransport> {
        let config = FileTransportConfig {
            path: path.display().to_string(),
            ..FileTransportConfig::default()
        };
        let reader = FileTransport::new(&config).await.expect("reader");
        Arc::new(reader.with_acknowledgements(AcknowledgementsConfig::new(true)))
    }

    /// The offset the sidecar of `path` holds.
    fn saved(path: &Path) -> u64 {
        let pos = path.with_extension("ndjson.pos");
        std::fs::read_to_string(pos)
            .ok()
            .and_then(|s| s.split_whitespace().next()?.parse().ok())
            .unwrap_or(0)
    }

    /// Kill the pipeline while the sink holds marker 100, with the file
    /// rotated under the reader first when `rotate` is set: every line the
    /// saved position had passed arrived, and the restart delivers the rest,
    /// the rotated file's tail included.
    async fn kill_mid_batch(rotate: bool) {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("in.ndjson");
        let lines = lines();
        let split = if rotate { 150 } else { 200 };
        std::fs::write(&live, lines[..split].concat()).expect("write");

        let ledger = Ledger::with_records(200);
        let sink = FaultSink::new(ledger.clone());
        sink.hold_at(100);
        let first = reader(&live).await;
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
        if rotate {
            std::fs::rename(&live, dir.path().join("in.ndjson.1")).expect("rotate");
            std::fs::write(&live, lines[split..].concat()).expect("write");
        }
        // Kill: the pipeline dies mid-send and the reader goes, closing nothing.
        running.abort();
        let _ = running.await;
        drop(first);
        sink.unhold();

        // What the saved position passed in the first file is acknowledged.
        let at_kill = saved(&live);
        let mut end = 0;
        for line in &lines[..split] {
            end += line.len() as u64;
            if end <= at_kill {
                ledger.acked(marker_of(line.trim_ascii_end()).expect("marked"));
            }
        }
        assert!(
            at_kill < end,
            "the held send kept the position short of the end"
        );

        let restarted = reader(&live).await;
        let shutdown = CancellationToken::new();
        let draining = tokio::spawn(engine_pipeline(
            Arc::clone(&restarted),
            Arc::new(sink.instance(1)),
            ledger.clone(),
            shutdown.clone(),
        ));
        let live_len = std::fs::metadata(&live).expect("live").len();
        assert!(
            poll_until(Duration::from_secs(60), || ledger.verdict().arrived == 200
                && saved(&live) == live_len)
            .await,
            "the restart delivers and saves every line: {:?}",
            ledger.verdict()
        );
        for m in 0..200 {
            ledger.acked(m);
        }
        shutdown.cancel();
        let _ = draining.await;
        assert_conforms(&ledger.verdict());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_file_kill_mid_batch() {
        kill_mid_batch(false).await;
    }

    /// A rotation before the held lines are released loses none of the
    /// renamed file's tail.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn engine_file_rotated_then_killed_mid_batch() {
        kill_mid_batch(true).await;
    }
}
