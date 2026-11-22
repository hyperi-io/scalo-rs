// Project:   scalo
// File:      src/transport/grpc/coalesce.rs
// Purpose:   Joins small send_batch blocks for one endpoint into one RouteBatch call
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The stage in front of one endpoint that joins small `send_batch` blocks into
//! one `RouteBatch` call.
//!
//! One task owns the stage. When one of `in_flight` call slots is free, it takes
//! the next queued block and every block queued behind it, up to
//! `max_batch_bytes` of payload and `max_message_size` encoded, and sends them as
//! one call. It sets no timer, so a block that finds a slot free goes out at once.
//!
//! A caller is answered once the call carrying its records returns, with that
//! call's result: a failed call fails every caller in it, and each sends again.
//! A caller still unanswered at `send_timeout_ms` is answered `Backpressured`.
//! Queued payload is capped at `ROOM_TARGETS` times `max_batch_bytes`, so a
//! caller that finds no room waits for it, and nothing is dropped. A block whose
//! caller has gone is dropped if the stage has not taken it yet.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::config::CoalesceConfig;
use super::{Framed, Wire, bodies_within};
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::types::SendResult;
use crate::transport::work_batch::Record;

/// Payload the stage holds queued, in multiples of `max_batch_bytes`.
const ROOM_TARGETS: u64 = 4;

/// Blocks the stage holds queued at once, whatever their bytes.
const QUEUED_BLOCKS: usize = 4096;

/// What a stage joins blocks up to.
#[derive(Debug, Clone, Copy)]
struct Limits {
    max_batch_bytes: u64,
    in_flight: usize,
    max_message_size: usize,
}

/// One caller's block, queued.
struct Item {
    records: Vec<Framed>,
    bytes: u64,
    /// The records' encoded length as fields of a `Batch` body.
    framed: usize,
    reply: oneshot::Sender<SendResult>,
    /// Room in the queue, given back when the stage takes the block.
    room: Option<OwnedSemaphorePermit>,
    /// The caller's trace context, which a call its block opens is sent under.
    #[cfg(feature = "transport-trace")]
    context: opentelemetry::Context,
}

/// The handle a `GrpcTransport` holds on the stage for its endpoint.
pub(super) struct Coalescer {
    queue: mpsc::Sender<Item>,
    room: Arc<Semaphore>,
    /// Permits `room` was made with, one per byte.
    room_bytes: u64,
    queued: Arc<AtomicU64>,
    limits: Limits,
    /// Sends a block itself once the stage task has gone.
    wire: Wire,
    task: JoinHandle<()>,
}

impl Coalescer {
    /// Start the stage for `wire`'s endpoint.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` when `in_flight` or `max_batch_bytes` is 0.
    pub(super) fn spawn(
        wire: Wire,
        config: &CoalesceConfig,
        max_message_size: usize,
    ) -> TransportResult<Self> {
        if config.in_flight == 0 || config.max_batch_bytes == 0 {
            return Err(TransportError::Config(format!(
                "grpc coalesce: in_flight ({}) and max_batch_bytes ({}) must each be at least 1, \
                 or set coalesce.enabled to false",
                config.in_flight, config.max_batch_bytes
            )));
        }
        let limits = Limits {
            max_batch_bytes: config.max_batch_bytes,
            in_flight: config.in_flight.min(Semaphore::MAX_PERMITS),
            max_message_size,
        };
        // A permit per byte, and a block takes its permits as a u32.
        let room_bytes = config
            .max_batch_bytes
            .saturating_mul(ROOM_TARGETS)
            .min(u64::from(u32::MAX))
            .min(Semaphore::MAX_PERMITS as u64);
        let (queue, blocks) = mpsc::channel(QUEUED_BLOCKS);
        let queued = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(run(wire.clone(), blocks, Arc::clone(&queued), limits));
        Ok(Self {
            queue,
            room: Arc::new(Semaphore::new(
                usize::try_from(room_bytes).unwrap_or(Semaphore::MAX_PERMITS),
            )),
            room_bytes,
            queued,
            limits,
            wire,
            task,
        })
    }

    /// Whether the stage joins `records`: a block under `max_batch_bytes` of
    /// payload.
    pub(super) fn joins(&self, records: &[Record]) -> bool {
        let bytes: u64 = records.iter().map(|r| r.payload.len() as u64).sum();
        bytes < self.limits.max_batch_bytes
    }

    /// Send `records` in the next call a slot frees for, and return that call's
    /// result once it returns, or `Backpressured` at `send_timeout_ms`.
    pub(super) async fn send(&self, records: Vec<Framed>) -> SendResult {
        if records.is_empty() {
            return SendResult::Ok;
        }
        let submitted = self.submit(records);
        let limit_ms = self.wire.send_timeout_ms;
        let outcome = if limit_ms == 0 {
            submitted.await
        } else if let Ok(outcome) =
            tokio::time::timeout(Duration::from_millis(limit_ms), submitted).await
        {
            outcome
        } else {
            // Answered as a call past its deadline is. A block already in a call
            // is still sent, so the caller's resend can duplicate it.
            return super::failed_rpc_result(&tonic::Status::deadline_exceeded(format!(
                "no answer within send_timeout_ms ({limit_ms} ms), the wait for a call slot \
                 included"
            )));
        };
        match outcome {
            Ok(result) => result,
            // The stage task has gone, so the block goes out on its own.
            Err(records) => {
                self.wire
                    .send_bodies(bodies_within(records, self.limits.max_message_size))
                    .await
            }
        }
    }

    /// Queue `records` and wait for the result of the call that carries them.
    /// `Err` hands them back when the stage task has gone.
    async fn submit(&self, records: Vec<Framed>) -> Result<SendResult, Vec<Framed>> {
        let bytes: u64 = records.iter().map(|r| r.record.payload.len() as u64).sum();
        let framed: usize = records.iter().map(|r| r.len).sum();
        // Capped at the whole room, so a block always fits once the room drains.
        let permits = u32::try_from(bytes.min(self.room_bytes)).unwrap_or(u32::MAX);
        let Ok(room) = Arc::clone(&self.room).acquire_many_owned(permits).await else {
            return Err(records);
        };
        let Ok(slot) = self.queue.reserve().await else {
            return Err(records);
        };
        let (reply, answer) = oneshot::channel();
        self.queued.fetch_add(bytes, Ordering::AcqRel);
        slot.send(Item {
            records,
            bytes,
            framed,
            reply,
            room: Some(room),
            #[cfg(feature = "transport-trace")]
            context: opentelemetry::Context::current(),
        });
        // Unanswered only when the call's task was dropped mid-call, and the
        // records may have been sent, so the caller sends them again.
        Ok(answer.await.unwrap_or(SendResult::Backpressured))
    }
}

impl Drop for Coalescer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Take queued blocks into calls, one call per free slot. Ends when the handle
/// has dropped and the queue is empty.
async fn run(wire: Wire, mut blocks: mpsc::Receiver<Item>, queued: Arc<AtomicU64>, limits: Limits) {
    let slots = Arc::new(Semaphore::new(limits.in_flight));
    // A block that did not fit the last call, still holding its room.
    let mut carried: Option<Item> = None;
    loop {
        let Ok(slot) = Arc::clone(&slots).acquire_owned().await else {
            return;
        };
        let Some(first) = next_live(&mut carried, &mut blocks, &queued).await else {
            return;
        };
        let mut bytes = first.bytes;
        let mut framed = first.framed;
        let mut taken = vec![first];
        while let Ok(item) = blocks.try_recv() {
            // Within max_message_size too, so the call is one request.
            let fits = bytes + item.bytes <= limits.max_batch_bytes
                && framed + item.framed <= limits.max_message_size;
            if !item.reply.is_closed() && !fits {
                carried = Some(item);
                break;
            }
            if let Some(item) = leave_queue(item, &queued) {
                bytes += item.bytes;
                framed += item.framed;
                taken.push(item);
            }
        }
        tokio::spawn(send_taken(
            wire.clone(),
            slot,
            taken,
            limits.max_message_size,
        ));
    }
}

/// The next block whose caller still waits: the one carried over from the last
/// call, else the next queued.
async fn next_live(
    carried: &mut Option<Item>,
    blocks: &mut mpsc::Receiver<Item>,
    queued: &AtomicU64,
) -> Option<Item> {
    if let Some(item) = carried.take().and_then(|item| leave_queue(item, queued)) {
        return Some(item);
    }
    loop {
        if let Some(item) = leave_queue(blocks.recv().await?, queued) {
            return Some(item);
        }
    }
}

/// Count `item` out of the queue and give its room back. `None`, the block
/// unsent, when its caller has gone.
fn leave_queue(mut item: Item, queued: &AtomicU64) -> Option<Item> {
    queued.fetch_sub(item.bytes, Ordering::AcqRel);
    item.room = None;
    (!item.reply.is_closed()).then_some(item)
}

/// Send the taken blocks as one call, give its slot back, and answer every
/// caller in it with the call's result.
async fn send_taken(
    wire: Wire,
    slot: OwnedSemaphorePermit,
    taken: Vec<Item>,
    max_message_size: usize,
) {
    #[cfg(feature = "metrics")]
    count_call(&taken);
    #[cfg(feature = "transport-trace")]
    let context = taken
        .first()
        .map(|item| item.context.clone())
        .unwrap_or_default();
    let mut replies = Vec::with_capacity(taken.len());
    let mut blocks = Vec::with_capacity(taken.len());
    for item in taken {
        replies.push(item.reply);
        blocks.push(item.records);
    }
    let call = wire.send_bodies(bodies_within(
        blocks.into_iter().flatten(),
        max_message_size,
    ));
    #[cfg(feature = "transport-trace")]
    let call = opentelemetry::context::FutureExt::with_context(call, context);
    let result = call.await;
    drop(slot);
    answer(replies, result);
}

/// Answer every caller in a call with its result. A caller that has gone needs
/// no answer.
fn answer(replies: Vec<oneshot::Sender<SendResult>>, result: SendResult) {
    let mut replies = replies.into_iter();
    let last = replies.next_back();
    for reply in replies {
        let _ = reply.send(copy_of(&result));
    }
    if let Some(reply) = last {
        let _ = reply.send(result);
    }
}

/// `result` again, for another caller in the same call.
fn copy_of(result: &SendResult) -> SendResult {
    match result {
        SendResult::Ok => SendResult::Ok,
        SendResult::Backpressured => SendResult::Backpressured,
        SendResult::FilteredDlq => SendResult::FilteredDlq,
        SendResult::Fatal(TransportError::Send(message)) => {
            SendResult::Fatal(TransportError::Send(message.clone()))
        }
        // A RouteBatch fails Fatal only with Send, so any other keeps its text.
        SendResult::Fatal(other) => SendResult::Fatal(TransportError::Send(other.to_string())),
    }
}

/// Count one joined call and the blocks and records it carries.
#[cfg(feature = "metrics")]
fn count_call(taken: &[Item]) {
    let records: usize = taken.iter().map(|item| item.records.len()).sum();
    metrics::counter!("transport_coalesced_calls_total", "transport" => "grpc").increment(1);
    metrics::counter!("transport_coalesced_blocks_total", "transport" => "grpc")
        .increment(taken.len() as u64);
    metrics::counter!("transport_coalesced_records_total", "transport" => "grpc")
        .increment(records as u64);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::{Semaphore, watch};
    use tokio::task::JoinHandle;
    use tonic::{Request, Response, Status};

    use super::super::batch::records_to_proto;
    use super::super::proto;
    use super::super::{CoalesceConfig, GrpcConfig, GrpcTransport};
    use crate::transport::error::TransportError;
    use crate::transport::filter::{FilterAction, FilterRule};
    use crate::transport::traits::TransportSender;
    use crate::transport::types::{PayloadFormat, SendResult};
    use crate::transport::work_batch::{Record, RecordMeta};

    /// The longest any wait here takes before the test fails.
    const WAIT: Duration = Duration::from_secs(10);

    /// How the recording server answers a `RouteBatch`.
    #[derive(Debug, Clone, Copy)]
    enum Reply {
        Accept,
        Unavailable,
        Refuse,
    }

    /// The calls a recording receive server took, and the gate its answers
    /// wait on.
    struct Calls {
        batches: parking_lot::Mutex<Vec<proto::Batch>>,
        arrived: watch::Sender<usize>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        /// One permit answers one call.
        gate: Semaphore,
        reply: parking_lot::Mutex<Reply>,
    }

    impl Calls {
        /// Answer the next `n` calls.
        fn answer(&self, n: usize) {
            self.gate.add_permits(n);
        }

        /// Wait until `n` calls have arrived.
        async fn arrived(&self, n: usize) {
            let mut arrived = self.arrived.subscribe();
            tokio::time::timeout(WAIT, arrived.wait_for(|got| *got >= n))
                .await
                .unwrap_or_else(|_| panic!("{n} calls did not arrive"))
                .expect("the server holds the sender");
        }

        /// Each call's payloads, in arrival order.
        fn payloads(&self) -> Vec<Vec<String>> {
            self.batches
                .lock()
                .iter()
                .map(|batch| {
                    batch
                        .records
                        .iter()
                        .map(|r| String::from_utf8_lossy(&r.payload).into_owned())
                        .collect()
                })
                .collect()
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }
    }

    /// A receive service that records every `RouteBatch` and answers each one
    /// when the gate lets it.
    struct Recording(Arc<Calls>);

    #[tonic::async_trait]
    impl proto::transport_server::Transport for Recording {
        async fn push(
            &self,
            _request: Request<proto::PushRequest>,
        ) -> Result<Response<proto::PushResponse>, Status> {
            Ok(Response::new(proto::PushResponse { accepted: 1 }))
        }

        async fn route_batch(
            &self,
            request: Request<proto::Batch>,
        ) -> Result<Response<proto::BatchAck>, Status> {
            let calls = &self.0;
            let batch = request.into_inner();
            let accepted = batch.records.len() as u64;
            let now = calls.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            calls.peak.fetch_max(now, Ordering::SeqCst);
            calls.batches.lock().push(batch);
            calls.arrived.send_modify(|n| *n += 1);
            calls
                .gate
                .acquire()
                .await
                .expect("the gate is never closed")
                .forget();
            calls.in_flight.fetch_sub(1, Ordering::SeqCst);
            let reply = *calls.reply.lock();
            match reply {
                Reply::Accept => Ok(Response::new(proto::BatchAck { accepted })),
                Reply::Unavailable => Err(Status::unavailable("receiver busy")),
                Reply::Refuse => Err(Status::invalid_argument("block refused")),
            }
        }

        async fn health_check(
            &self,
            _request: Request<proto::HealthCheckRequest>,
        ) -> Result<Response<proto::HealthCheckResponse>, Status> {
            Ok(Response::new(proto::HealthCheckResponse {
                status: proto::ServingStatus::Serving.into(),
            }))
        }
    }

    /// A recording receive server on a loopback port, answering `permits`
    /// calls before it holds the rest, and its URI.
    async fn recording(permits: usize) -> (Arc<Calls>, String) {
        // tonic's own default decoding limit.
        recording_within(permits, 4 * 1024 * 1024).await
    }

    /// As `recording`, refusing a request that decodes past `limit` bytes.
    async fn recording_within(permits: usize, limit: usize) -> (Arc<Calls>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(Calls {
            batches: parking_lot::Mutex::new(Vec::new()),
            arrived: watch::channel(0).0,
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            gate: Semaphore::new(permits),
            reply: parking_lot::Mutex::new(Reply::Accept),
        });
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    proto::transport_server::TransportServer::new(Recording(Arc::clone(&calls)))
                        .max_decoding_message_size(limit),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        (calls, format!("http://{addr}"))
    }

    fn joining(max_batch_bytes: u64, in_flight: usize) -> CoalesceConfig {
        CoalesceConfig {
            enabled: true,
            max_batch_bytes,
            in_flight,
        }
    }

    fn client(uri: &str, coalesce: CoalesceConfig) -> GrpcConfig {
        GrpcConfig {
            coalesce,
            ..GrpcConfig::client(uri)
        }
    }

    async fn sender(config: GrpcConfig) -> Arc<GrpcTransport> {
        Arc::new(GrpcTransport::new(&config).await.unwrap())
    }

    fn record(payload: &str) -> Record {
        Record {
            payload: bytes::Bytes::from(payload.to_owned()),
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    fn block(payloads: &[&str]) -> Vec<Record> {
        payloads.iter().map(|payload| record(payload)).collect()
    }

    /// Send `payloads` as one block, from a task of its own.
    fn offer(sender: &Arc<GrpcTransport>, payloads: &[&str]) -> JoinHandle<SendResult> {
        let sender = Arc::clone(sender);
        let records = block(payloads);
        tokio::spawn(async move { sender.send_batch(&records).await })
    }

    /// Payload bytes queued in `sender`'s stage.
    fn queued(sender: &GrpcTransport) -> u64 {
        sender
            .coalesce
            .as_ref()
            .expect("the sender joins blocks")
            .queued
            .load(Ordering::Acquire)
    }

    /// Offer `payloads`, and wait until the stage holds them queued.
    async fn queue(sender: &Arc<GrpcTransport>, payloads: &[&str]) -> JoinHandle<SendResult> {
        let before = queued(sender);
        let caller = offer(sender, payloads);
        until("the block queued", || queued(sender) > before).await;
        caller
    }

    /// Wait until `ready` holds.
    async fn until(what: &str, mut ready: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while !ready() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what}: not within {WAIT:?}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// A caller's result.
    async fn result(caller: JoinHandle<SendResult>) -> SendResult {
        tokio::time::timeout(WAIT, caller)
            .await
            .expect("the caller is answered")
            .expect("the caller's task")
    }

    /// Blocks queued while the only slot is busy go out as one call once it
    /// frees, each caller is answered only when that call returns, and the
    /// joined request is the one `records_to_proto` builds for its records.
    #[tokio::test]
    async fn blocks_queued_behind_a_busy_slot_go_out_as_one_call_answered_when_it_returns() {
        #[cfg(feature = "metrics")]
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        // Current-thread runtime: the stage's tasks run on this thread and see it.
        #[cfg(feature = "metrics")]
        let _local = metrics::set_default_local_recorder(&recorder);

        let (calls, uri) = recording(0).await;
        let sender = sender(client(&uri, joining(1 << 16, 1))).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let a = queue(&sender, &["a1", "a2"]).await;
        let b = queue(&sender, &["b1"]).await;

        calls.answer(1);
        calls.arrived(2).await;
        assert!(result(first).await.is_ok());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !a.is_finished() && !b.is_finished(),
            "answered before the call carrying the blocks returned"
        );

        calls.answer(1);
        assert!(result(a).await.is_ok());
        assert!(result(b).await.is_ok());
        assert_eq!(calls.payloads(), [vec!["x"], vec!["a1", "a2", "b1"]]);
        assert_eq!(
            calls.batches.lock()[1],
            records_to_proto(block(&["a1", "a2", "b1"]))
        );
        assert_eq!(queued(&sender), 0);

        #[cfg(feature = "metrics")]
        {
            let rendered = recorder.handle().render();
            for series in [
                r#"transport_coalesced_calls_total{transport="grpc"} 2"#,
                r#"transport_coalesced_blocks_total{transport="grpc"} 3"#,
                r#"transport_coalesced_records_total{transport="grpc"} 4"#,
            ] {
                assert!(
                    rendered.lines().any(|line| line == series),
                    "{series} missing:\n{rendered}"
                );
            }
        }
    }

    /// A call the receiver fails answers every block in it with that failure,
    /// so each caller sends again.
    #[tokio::test]
    async fn a_failed_call_answers_every_block_in_it_with_the_failure() {
        for reply in [Reply::Unavailable, Reply::Refuse] {
            let (calls, uri) = recording(0).await;
            *calls.reply.lock() = reply;
            let sender = sender(client(&uri, joining(1 << 16, 1))).await;
            let first = offer(&sender, &["x"]);
            calls.arrived(1).await;
            let a = queue(&sender, &["a"]).await;
            let b = queue(&sender, &["b1", "b2"]).await;

            calls.answer(2);
            for caller in [first, a, b] {
                let got = result(caller).await;
                let expected = match reply {
                    Reply::Unavailable => got.is_backpressured(),
                    _ => matches!(
                        &got,
                        SendResult::Fatal(TransportError::Send(message)) if message == "block refused"
                    ),
                };
                assert!(expected, "{reply:?}: got {got:?}");
            }
            assert_eq!(
                calls.payloads(),
                [vec!["x"], vec!["a", "b1", "b2"]],
                "{reply:?}"
            );
        }
    }

    /// At most `in_flight` calls run, each carries at most `max_batch_bytes`,
    /// and a caller that finds the room full waits for it: the queue never
    /// holds more than its room, and every block is sent once.
    #[tokio::test]
    async fn no_more_than_in_flight_calls_run_and_a_caller_with_no_room_waits() {
        let (calls, uri) = recording(0).await;
        // 4 byte blocks and an 8 byte cap: two blocks a call, and 32 bytes of room.
        let sender = sender(client(&uri, joining(8, 2))).await;
        let names: Vec<String> = (0..12).map(|i| format!("r{i:03}")).collect();
        let callers: Vec<_> = names
            .iter()
            .map(|name| offer(&sender, &[name.as_str()]))
            .collect();
        calls.arrived(2).await;
        until("the room filled", || queued(&sender) == 32).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(queued(&sender), 32, "the queue held more than its room");
        assert_eq!(*calls.arrived.borrow(), 2, "a call past in_flight started");
        assert!(
            callers.iter().all(|caller| !caller.is_finished()),
            "a caller was answered before its call returned"
        );

        calls.answer(1_000);
        for caller in callers {
            assert!(result(caller).await.is_ok());
        }
        let payloads = calls.payloads();
        let mut sent: Vec<&str> = payloads.iter().flatten().map(String::as_str).collect();
        sent.sort_unstable();
        assert_eq!(sent, names, "a block was dropped or sent twice");
        for call in &payloads {
            let bytes: usize = call.iter().map(String::len).sum();
            assert!(bytes <= 8, "a call of {bytes} bytes: {call:?}");
        }
        assert_eq!(calls.peak(), 2);
        assert_eq!(queued(&sender), 0);
    }

    /// A block whose caller has gone before the stage takes it is not sent.
    #[tokio::test]
    async fn a_block_whose_caller_has_gone_is_dropped_if_not_yet_taken() {
        let (calls, uri) = recording(0).await;
        let sender = sender(client(&uri, joining(1 << 16, 1))).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let gone = queue(&sender, &["gone"]).await;
        gone.abort();
        assert!(gone.await.is_err_and(|e| e.is_cancelled()));

        calls.answer(1);
        assert!(result(first).await.is_ok());
        let stays = offer(&sender, &["stays"]);
        calls.arrived(2).await;
        calls.answer(1);
        assert!(result(stays).await.is_ok());
        assert_eq!(calls.payloads(), [["x"], ["stays"]]);
        assert_eq!(queued(&sender), 0);
    }

    /// A caller that goes once its block is in a call leaves that call to run,
    /// and the other callers in it are answered.
    #[tokio::test]
    async fn a_call_answers_the_callers_left_in_it_when_one_has_gone() {
        let (calls, uri) = recording(0).await;
        let sender = sender(client(&uri, joining(1 << 16, 1))).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let gone = queue(&sender, &["gone"]).await;
        let stays = queue(&sender, &["stays"]).await;

        calls.answer(1);
        calls.arrived(2).await;
        gone.abort();
        assert!(gone.await.is_err_and(|e| e.is_cancelled()));
        calls.answer(1);
        assert!(result(first).await.is_ok());
        assert!(result(stays).await.is_ok());
        assert_eq!(calls.payloads(), [vec!["x"], vec!["gone", "stays"]]);
    }

    /// A record a filter dead-letters, or one over `max_message_size` on its
    /// own, comes out of its own caller's block: that caller is answered
    /// `FilteredDlq`, the others in the same call `Ok`, and the call carries
    /// every other record.
    #[tokio::test]
    async fn a_dead_letter_answers_its_own_caller_only() {
        let (calls, uri) = recording(0).await;
        let mut config = client(&uri, joining(1 << 16, 1)).with_max_message_size(256);
        config.filters_out = vec![FilterRule {
            expression: r#"status == "bad""#.into(),
            action: FilterAction::Dlq,
        }];
        let sender = sender(config).await;
        let oversize = format!(r#"{{"pad":"{}"}}"#, "x".repeat(300));

        let first = offer(&sender, &[r#"{"n":"x"}"#]);
        calls.arrived(1).await;
        let filtered = queue(
            &sender,
            &[r#"{"n":"a0"}"#, r#"{"status":"bad"}"#, r#"{"n":"a2"}"#],
        )
        .await;
        let too_large = queue(&sender, &[r#"{"n":"b0"}"#, oversize.as_str()]).await;
        let clean = queue(&sender, &[r#"{"n":"c0"}"#]).await;

        calls.answer(2);
        assert!(result(first).await.is_ok());
        assert!(result(filtered).await.is_filtered_dlq());
        assert!(result(too_large).await.is_filtered_dlq());
        assert!(result(clean).await.is_ok());
        assert_eq!(
            calls.payloads(),
            [
                vec![r#"{"n":"x"}"#],
                vec![
                    r#"{"n":"a0"}"#,
                    r#"{"n":"a2"}"#,
                    r#"{"n":"b0"}"#,
                    r#"{"n":"c0"}"#
                ],
            ]
        );
    }

    /// A block of `max_batch_bytes` or more goes straight out, past a busy slot
    /// and without queueing.
    #[tokio::test]
    async fn a_block_at_max_batch_bytes_goes_straight_out_past_a_busy_slot() {
        let (calls, uri) = recording(0).await;
        let sender = sender(client(&uri, joining(8, 1))).await;
        let small = offer(&sender, &["1234"]);
        calls.arrived(1).await;
        let large = offer(&sender, &["12345678"]);
        calls.arrived(2).await;
        assert_eq!(queued(&sender), 0, "the large block was queued");
        assert_eq!(calls.peak(), 2, "the large block waited for the slot");

        calls.answer(2);
        assert!(result(small).await.is_ok());
        assert!(result(large).await.is_ok());
        assert_eq!(calls.payloads(), [["1234"], ["12345678"]]);
    }

    /// A joined call stays within `max_message_size` encoded, so it is one
    /// request a receiver with the same limit takes, and its callers are
    /// answered when that request returns.
    #[tokio::test]
    async fn a_joined_call_stays_within_max_message_size() {
        const LIMIT: usize = 1024;
        let (calls, uri) = recording_within(0, LIMIT).await;
        let sender = sender(client(&uri, joining(1 << 16, 1)).with_max_message_size(LIMIT)).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        // 40 blocks of 60 bytes: about 15 fit a call at this limit, 2,400 bytes all told.
        let names: Vec<String> = (0..40)
            .map(|i| format!("{i:03}{}", "y".repeat(57)))
            .collect();
        let mut callers = Vec::new();
        for name in &names {
            callers.push(queue(&sender, &[name.as_str()]).await);
        }

        calls.answer(2);
        calls.arrived(3).await;
        assert!(result(first).await.is_ok());
        let joined = calls.payloads()[1].len();
        assert!(
            joined > 1 && joined < names.len(),
            "the first joined call carried {joined} blocks"
        );
        until("the first joined call's callers answered", || {
            callers.iter().filter(|caller| caller.is_finished()).count() == joined
        })
        .await;

        calls.answer(1_000);
        for caller in callers {
            assert!(result(caller).await.is_ok(), "a call the receiver refused");
        }
        let batches = calls.batches.lock();
        let sent: usize = batches.iter().map(|batch| batch.records.len()).sum();
        assert_eq!(sent, names.len() + 1, "a block was dropped or sent twice");
        for batch in batches.iter() {
            let len = prost::Message::encoded_len(batch);
            assert!(len <= LIMIT, "a request of {len} bytes past {LIMIT}");
        }
    }

    /// `send_timeout_ms` bounds a joined `send_batch` whole, the wait for a slot
    /// included. Against a receiver that never answers, every queued caller is
    /// answered `Backpressured` within about one limit, and a block no call took
    /// is not sent.
    #[tokio::test]
    async fn a_queued_block_is_answered_at_send_timeout_ms_by_a_receiver_that_hangs() {
        const LIMIT_MS: u64 = 500;
        let (calls, uri) = recording(0).await;
        // 4 byte blocks and a 5 byte cap: one block a call, so each waits its turn.
        let mut config = client(&uri, joining(5, 1));
        config.send_timeout_ms = LIMIT_MS;
        let sender = sender(config).await;
        let first = offer(&sender, &["xxxx"]);
        calls.arrived(1).await;
        let mut waiting = Vec::new();
        for name in ["aaaa", "bbbb", "cccc"] {
            waiting.push(queue(&sender, &[name]).await);
        }

        let started = tokio::time::Instant::now();
        for caller in waiting {
            let got = result(caller).await;
            assert!(got.is_backpressured(), "a queued caller got {got:?}");
        }
        let took = started.elapsed();
        // One call after another would answer the last at four limits.
        assert!(
            took < Duration::from_millis(LIMIT_MS * 9 / 5),
            "queued callers answered after {took:?} at a {LIMIT_MS} ms limit"
        );
        assert!(result(first).await.is_backpressured());

        // Past the calls a slot could still have started for the blocks.
        tokio::time::sleep(Duration::from_millis(LIMIT_MS * 2)).await;
        let payloads = calls.payloads();
        assert!(
            payloads
                .iter()
                .flatten()
                .all(|p| p != "bbbb" && p != "cccc"),
            "a block no call had taken was sent: {payloads:?}"
        );
        assert_eq!(queued(&sender), 0);
    }

    /// A block that finds a slot free goes out at once as its own call.
    #[tokio::test]
    async fn a_block_that_finds_a_slot_free_goes_out_on_its_own() {
        let (calls, uri) = recording(1_000).await;
        let sender = sender(client(&uri, CoalesceConfig::default())).await;
        for name in ["a", "b", "c"] {
            let sent = sender.send_batch(&[record(name)]).await;
            assert!(sent.is_ok(), "{name}: {sent:?}");
        }
        assert_eq!(calls.payloads(), [["a"], ["b"], ["c"]]);
    }

    /// Disabled, every `send_batch` is its own call at once, and each request
    /// is the one `records_to_proto` builds from its block.
    #[tokio::test]
    async fn disabled_sends_every_block_as_its_own_call() {
        let (calls, uri) = recording(0).await;
        let disabled = CoalesceConfig {
            enabled: false,
            ..CoalesceConfig::default()
        };
        let sender = sender(client(&uri, disabled)).await;
        assert!(sender.coalesce.is_none(), "a stage was started");
        let blocks: [&[&str]; 3] = [&["a1", "a2"], &["b1"], &["c1"]];
        let callers: Vec<_> = blocks.iter().map(|b| offer(&sender, b)).collect();
        calls.arrived(3).await;
        assert_eq!(calls.peak(), 3, "a block waited for another");

        calls.answer(3);
        for caller in callers {
            assert!(result(caller).await.is_ok());
        }
        let mut got: Vec<Vec<u8>> = calls
            .batches
            .lock()
            .iter()
            .map(prost::Message::encode_to_vec)
            .collect();
        let mut want: Vec<Vec<u8>> = blocks
            .iter()
            .map(|b| prost::Message::encode_to_vec(&records_to_proto(block(b))))
            .collect();
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }

    /// The `coalesce` keys default when left out, and a zero limit is refused
    /// while joining is on.
    #[tokio::test]
    async fn coalesce_keys_default_and_zero_limits_are_refused() {
        let config: GrpcConfig =
            serde_json::from_str(r#"{"endpoint":"http://127.0.0.1:1","coalesce":{"in_flight":8}}"#)
                .unwrap();
        assert_eq!(config.coalesce, joining(65_536, 8));
        for (coalesce, builds) in [
            (joining(1, 1), true),
            (joining(0, 4), false),
            (joining(65_536, 0), false),
            (
                CoalesceConfig {
                    enabled: false,
                    max_batch_bytes: 0,
                    in_flight: 0,
                },
                true,
            ),
        ] {
            let built = GrpcTransport::new(&client("http://127.0.0.1:1", coalesce)).await;
            assert_eq!(built.is_ok(), builds, "{coalesce:?}");
        }
    }
}
