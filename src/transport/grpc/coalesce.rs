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
//! One task owns the stage. When it holds a queued block and one of `in_flight`
//! call slots is free, it sends that block with every block queued behind it,
//! up to `max_batch_bytes` and `max_message_size` encoded, as one call. It sets
//! no timer, so a block that finds a slot free goes out at once.
//!
//! A caller is answered once the call carrying its records returns, with that
//! call's result: a call that fails for now fails every caller in it, and each
//! sends again. When the receiver refuses a call of several blocks for good, or
//! `ResourceExhausted`, each block goes again on its own through the same
//! slots, so one block's refusal stays its own, and a receiver whose limit is
//! below the sender's still takes each block.
//!
//! A block no call has taken by its caller's deadline is withdrawn unsent, and
//! the caller answered `Backpressured`. A block in a call waits for that call,
//! which `send_timeout_ms` bounds per request. Once the transport closes, every
//! caller still waiting is answered `Backpressured`.
//!
//! Queued blocks are capped at `ROOM_TARGETS` times `max_batch_bytes`, counted
//! encoded, so a caller that finds no room waits for it, and nothing is
//! dropped. A block whose caller has gone is dropped if the stage has not taken
//! it yet.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use super::config::CoalesceConfig;
use super::{Framed, Wire, bodies_within};
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::types::SendResult;

/// Room the stage holds for queued blocks, in multiples of `max_batch_bytes`.
const ROOM_TARGETS: u64 = 4;

/// Blocks the stage holds queued at once, whatever their bytes.
const QUEUED_BLOCKS: usize = 4096;

/// The least room a queued block takes, for the queue entry it holds whatever
/// its records weigh.
const MIN_BLOCK_BYTES: u64 = 64;

/// When a joined `send_batch` stops waiting for a call to take its block:
/// `send_timeout_ms` after it starts, or never when that is 0.
#[derive(Debug, Clone, Copy)]
pub(super) struct Deadline(Option<Instant>);

impl Deadline {
    pub(super) fn after(send_timeout_ms: u64) -> Self {
        Self((send_timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(send_timeout_ms)))
    }

    /// `work`'s output, or `None` once the deadline has passed.
    pub(super) async fn bound<T>(self, work: impl Future<Output = T>) -> Option<T> {
        match self.0 {
            None => Some(work.await),
            Some(at) => tokio::time::timeout_at(at, work).await.ok(),
        }
    }
}

/// The answer to a caller whose deadline passed with its block unsent:
/// `Backpressured`, so it sends again, counted as a wait that ran out rather
/// than as a failed call.
fn timed_out() -> SendResult {
    #[cfg(feature = "metrics")]
    metrics::counter!("transport_coalesced_timeouts_total", "transport" => "grpc").increment(1);
    SendResult::Backpressured
}

/// What the stage tells a caller about the call that carried its block.
enum Joined {
    /// The call's result.
    Answered(SendResult),
    /// The receiver refused a call that carried other blocks too, for good or
    /// `ResourceExhausted`, so the caller sends its block again on its own.
    Alone,
}

/// Why a submitted block was not sent.
enum Unsent {
    /// Its caller's deadline passed before a call took it.
    Expired,
    /// The stage task has gone, and the records are handed back.
    Gone(Vec<Framed>),
}

/// What a stage joins blocks up to.
#[derive(Debug, Clone, Copy)]
struct Limits {
    /// Most bytes, encoded, one joined call carries: the least of
    /// `max_batch_bytes`, the room and `max_message_size`.
    cap: usize,
    max_message_size: usize,
}

/// One caller's block, queued.
struct Item {
    records: Vec<Framed>,
    /// The records' encoded length as fields of a `Batch` body.
    framed: usize,
    /// Room in the queue the block holds: its encoded length, at least
    /// `MIN_BLOCK_BYTES`.
    charge: u64,
    reply: oneshot::Sender<Joined>,
    /// Set once, by the stage taking the block or by its caller withdrawing it
    /// at the deadline, so exactly one of the two happens.
    settled: Arc<AtomicBool>,
    /// Room in the queue, given back when the stage takes the block.
    room: Option<OwnedSemaphorePermit>,
    /// The caller's trace context, which a call its block opens is sent under.
    #[cfg(feature = "transport-trace")]
    context: opentelemetry::Context,
}

impl Item {
    /// Whether its caller still waits for a call to take it.
    fn live(&self) -> bool {
        !self.reply.is_closed() && !self.settled.load(Ordering::Acquire)
    }
}

/// The handle a `GrpcTransport` holds on the stage for its endpoint.
pub(super) struct Coalescer {
    queue: mpsc::Sender<Item>,
    room: Arc<Semaphore>,
    /// Permits `room` was made with, one per byte.
    room_bytes: u64,
    /// Room the queued blocks hold now.
    queued: Arc<AtomicU64>,
    /// The `in_flight` call slots, for joined calls and for blocks sent again
    /// on their own.
    slots: Arc<Semaphore>,
    /// Set once the transport closes.
    closing: watch::Sender<bool>,
    limits: Limits,
    /// Sends a block on its own.
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
        // A permit per byte, and a block takes its permits as a u32.
        let room_bytes = config
            .max_batch_bytes
            .saturating_mul(ROOM_TARGETS)
            .min(u64::from(u32::MAX))
            .min(Semaphore::MAX_PERMITS as u64);
        let limits = Limits {
            cap: usize::try_from(config.max_batch_bytes.min(room_bytes))
                .unwrap_or(usize::MAX)
                .min(max_message_size),
            max_message_size,
        };
        let slots = Arc::new(Semaphore::new(config.in_flight.min(Semaphore::MAX_PERMITS)));
        let (queue, blocks) = mpsc::channel(QUEUED_BLOCKS);
        let queued = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(run(
            wire.clone(),
            blocks,
            Arc::clone(&queued),
            Arc::clone(&slots),
            limits,
        ));
        Ok(Self {
            queue,
            room: Arc::new(Semaphore::new(
                usize::try_from(room_bytes).unwrap_or(Semaphore::MAX_PERMITS),
            )),
            room_bytes,
            queued,
            slots,
            closing: watch::Sender::new(false),
            limits,
            wire,
            task,
        })
    }

    /// Whether the stage joins `records`: a block under the joining cap,
    /// encoded. A larger one goes out on its own, split within
    /// `max_message_size`.
    pub(super) fn joins(&self, records: &[Framed]) -> bool {
        records.iter().map(|r| r.len).sum::<usize>() < self.limits.cap
    }

    /// Send `records` in the next call a slot frees for, and return that call's
    /// result once it returns. `again` frames the block once more for a resend
    /// on its own. Until a call takes the block, `deadline` withdraws it unsent
    /// and the answer is `Backpressured`. A block in a call gets that call's
    /// answer, which `send_timeout_ms` bounds per request. Once the transport
    /// closes, the answer is `Backpressured`.
    pub(super) async fn send(
        &self,
        records: Vec<Framed>,
        deadline: Deadline,
        again: impl FnOnce() -> Vec<Framed>,
    ) -> SendResult {
        if records.is_empty() {
            return SendResult::Ok;
        }
        let mut closing = self.closing.subscribe();
        let sent = async move {
            let held = match self.submit(records, deadline).await {
                Ok(Joined::Answered(result)) => return result,
                // Refused beside other blocks, and the stage keeps no copy.
                Ok(Joined::Alone) => None,
                Err(Unsent::Expired) => return timed_out(),
                Err(Unsent::Gone(records)) => Some(records),
            };
            // A slot first, within the deadline, so no more than in_flight
            // resends hold records at once. A closed transport has none.
            let _slot = match deadline.bound(self.slots.acquire()).await {
                None => return timed_out(),
                Some(Err(_)) => return SendResult::Backpressured,
                Some(Ok(slot)) => slot,
            };
            let records = held.unwrap_or_else(again);
            // Boxed, as it runs only here, so every send_batch future stays small.
            Box::pin(
                self.wire
                    .send_bodies(bodies_within(records, self.limits.max_message_size)),
            )
            .await
        };
        tokio::select! {
            biased;
            _ = closing.wait_for(|closed| *closed) => SendResult::Backpressured,
            result = sent => result,
        }
    }

    /// Stop admitting blocks, stop the stage and the calls it has in flight,
    /// and answer every caller still waiting `Backpressured`.
    pub(super) fn close(&self) {
        self.closing.send_replace(true);
        self.room.close();
        self.slots.close();
        self.task.abort();
    }

    /// Queue `records`, within `deadline` until a call takes them, and wait for
    /// the answer of that call.
    async fn submit(&self, records: Vec<Framed>, deadline: Deadline) -> Result<Joined, Unsent> {
        let framed: usize = records.iter().map(|r| r.len).sum();
        // Capped at the whole room, so a block always fits once the room drains.
        let charge = (framed as u64).max(MIN_BLOCK_BYTES).min(self.room_bytes);
        let permits = u32::try_from(charge).unwrap_or(u32::MAX);
        let room = match deadline
            .bound(Arc::clone(&self.room).acquire_many_owned(permits))
            .await
        {
            None => return Err(Unsent::Expired),
            Some(Err(_)) => return Err(Unsent::Gone(records)),
            Some(Ok(room)) => room,
        };
        let slot = match deadline.bound(self.queue.reserve()).await {
            None => return Err(Unsent::Expired),
            Some(Err(_)) => return Err(Unsent::Gone(records)),
            Some(Ok(slot)) => slot,
        };
        let (reply, mut answer) = oneshot::channel();
        let settled = Arc::new(AtomicBool::new(false));
        self.queued.fetch_add(charge, Ordering::AcqRel);
        slot.send(Item {
            records,
            framed,
            charge,
            reply,
            settled: Arc::clone(&settled),
            room: Some(room),
            #[cfg(feature = "transport-trace")]
            context: opentelemetry::Context::current(),
        });
        let answered = match deadline.bound(&mut answer).await {
            Some(answered) => answered,
            // Withdrawn before any call took it, so it is never sent.
            None if settle(&settled) => return Err(Unsent::Expired),
            // Taken: the call's own answer, which send_timeout_ms bounds.
            None => answer.await,
        };
        // Unanswered only when the call's task was dropped mid-call, and the
        // records may have been sent, so the caller sends them again.
        Ok(answered.unwrap_or(Joined::Answered(SendResult::Backpressured)))
    }
}

impl Drop for Coalescer {
    /// Stops the stage and, with it, the calls it has in flight.
    fn drop(&mut self) {
        self.close();
    }
}

/// Take queued blocks into calls, one call per free slot. Ends when the handle
/// has dropped and the queue is empty.
async fn run(
    wire: Wire,
    mut blocks: mpsc::Receiver<Item>,
    queued: Arc<AtomicU64>,
    slots: Arc<Semaphore>,
    limits: Limits,
) {
    // Owned here, so aborting this task aborts the calls in flight too.
    let mut calls: JoinSet<()> = JoinSet::new();
    // A block that did not fit the last call, still holding its room.
    let mut carried: Option<Item> = None;
    loop {
        while calls.try_join_next().is_some() {}
        // A block first, then a slot, so the stage holds no slot while idle.
        let Some(first) = next_live(&mut carried, &mut blocks, &queued).await else {
            return;
        };
        let Ok(slot) = Arc::clone(&slots).acquire_owned().await else {
            return;
        };
        let Some(first) = leave_queue(first, &queued) else {
            continue;
        };
        let mut framed = first.framed;
        let mut taken = vec![first];
        while let Ok(item) = blocks.try_recv() {
            if item.live() && framed + item.framed > limits.cap {
                carried = Some(item);
                break;
            }
            if let Some(item) = leave_queue(item, &queued) {
                framed += item.framed;
                taken.push(item);
            }
        }
        calls.spawn(send_taken(
            wire.clone(),
            slot,
            taken,
            limits.max_message_size,
        ));
    }
}

/// The next block whose caller still waits, still holding its room: the one
/// carried over from the last call, else the next queued.
async fn next_live(
    carried: &mut Option<Item>,
    blocks: &mut mpsc::Receiver<Item>,
    queued: &AtomicU64,
) -> Option<Item> {
    let mut next = carried.take();
    loop {
        let item = match next.take() {
            Some(item) => item,
            None => blocks.recv().await?,
        };
        if item.live() {
            return Some(item);
        }
        queued.fetch_sub(item.charge, Ordering::AcqRel);
    }
}

/// Count `item` out of the queue, give its room back and take it for a call.
/// `None`, the block unsent, when its caller has gone or withdrawn it.
fn leave_queue(mut item: Item, queued: &AtomicU64) -> Option<Item> {
    queued.fetch_sub(item.charge, Ordering::AcqRel);
    item.room = None;
    (!item.reply.is_closed() && settle(&item.settled)).then_some(item)
}

/// Settle a block for the stage taking it or its caller withdrawing it:
/// `true` for whichever of the two gets here first.
fn settle(settled: &AtomicBool) -> bool {
    settled
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

/// Send the taken blocks as one call, give its slot back, and answer every
/// caller in it.
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
    let several = blocks.len() > 1;
    let call = wire.answer_to(bodies_within(
        blocks.into_iter().flatten(),
        max_message_size,
    ));
    #[cfg(feature = "transport-trace")]
    let call = opentelemetry::context::FutureExt::with_context(call, context);
    let sent = call.await;
    drop(slot);
    // A refusal, for good or of these records, may be one block's alone, and
    // `ResourceExhausted` the joined request past a receiver's decode limit once decompressed.
    let alone = sent.result.is_fatal()
        || matches!(sent.result, SendResult::Reject(_))
        || sent.code == Some(tonic::Code::ResourceExhausted);
    if several && alone {
        for reply in replies {
            let _ = reply.send(Joined::Alone);
        }
    } else {
        answer(replies, sent.result);
    }
}

/// Answer every caller in a call with its result. A caller that has gone needs
/// no answer.
fn answer(replies: Vec<oneshot::Sender<Joined>>, result: SendResult) {
    let mut replies = replies.into_iter();
    let last = replies.next_back();
    for reply in replies {
        let _ = reply.send(Joined::Answered(copy_of(&result)));
    }
    if let Some(reply) = last {
        let _ = reply.send(Joined::Answered(result));
    }
}

/// `result` again, for another caller in the same call.
fn copy_of(result: &SendResult) -> SendResult {
    match result {
        SendResult::Ok => SendResult::Ok,
        SendResult::Backpressured => SendResult::Backpressured,
        SendResult::RetryAfter(wait) => SendResult::RetryAfter(*wait),
        SendResult::Reject(other) => SendResult::Reject(TransportError::Send(other.to_string())),
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

    use super::super::batch::{record_to_proto, records_to_proto};
    use super::super::proto;
    use super::super::{CoalesceConfig, GrpcConfig, GrpcTransport, as_field};
    use crate::compression::{Codec, Compression};
    use crate::transport::error::TransportError;
    use crate::transport::filter::{FilterAction, FilterRule};
    use crate::transport::traits::{TransportBase, TransportSender};
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
        /// Refuse a request carrying a record whose payload has a `!`.
        RefuseMarked,
    }

    /// The calls a recording receive server took, and the gate its answers
    /// wait on.
    struct Calls {
        batches: parking_lot::Mutex<Vec<proto::Batch>>,
        arrived: watch::Sender<usize>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        /// Calls the client dropped before they were answered.
        dropped: AtomicUsize,
        /// One permit answers one call.
        gate: Semaphore,
        reply: parking_lot::Mutex<Reply>,
    }

    /// Counts a call in `Calls::dropped` unless it is answered.
    struct Unanswered<'a>(Option<&'a AtomicUsize>);

    impl Drop for Unanswered<'_> {
        fn drop(&mut self) {
            if let Some(dropped) = self.0 {
                dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
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
            let marked = batch.records.iter().any(|r| r.payload.contains(&b'!'));
            let now = calls.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            calls.peak.fetch_max(now, Ordering::SeqCst);
            calls.batches.lock().push(batch);
            calls.arrived.send_modify(|n| *n += 1);
            let mut unanswered = Unanswered(Some(&calls.dropped));
            calls
                .gate
                .acquire()
                .await
                .expect("the gate is never closed")
                .forget();
            unanswered.0 = None;
            calls.in_flight.fetch_sub(1, Ordering::SeqCst);
            let reply = *calls.reply.lock();
            match reply {
                Reply::Accept => Ok(Response::new(proto::BatchAck { accepted })),
                Reply::RefuseMarked if !marked => Ok(Response::new(proto::BatchAck { accepted })),
                Reply::Unavailable => Err(Status::unavailable("receiver busy")),
                Reply::Refuse | Reply::RefuseMarked => {
                    Err(Status::invalid_argument("block refused"))
                }
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

    /// As `recording`, refusing a request that decodes past `limit` bytes:
    /// `OutOfRange`, or `ResourceExhausted` once decompressed.
    async fn recording_within(permits: usize, limit: usize) -> (Arc<Calls>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(Calls {
            batches: parking_lot::Mutex::new(Vec::new()),
            arrived: watch::channel(0).0,
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            gate: Semaphore::new(permits),
            reply: parking_lot::Mutex::new(Reply::Accept),
        });
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    proto::transport_server::TransportServer::new(Recording(Arc::clone(&calls)))
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
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

    /// Send a block of `name` to `sender`'s stage, counting each time it is
    /// framed again for a resend in `prepared`.
    fn resendable(
        sender: &Arc<GrpcTransport>,
        name: &str,
        prepared: &Arc<AtomicUsize>,
    ) -> JoinHandle<SendResult> {
        let sender = Arc::clone(sender);
        let prepared = Arc::clone(prepared);
        let name = name.to_owned();
        tokio::spawn(async move {
            let stage = sender.coalesce.as_ref().expect("the sender joins blocks");
            let again = || {
                prepared.fetch_add(1, Ordering::SeqCst);
                vec![as_field(record(&name))]
            };
            let block = vec![as_field(record(&name))];
            stage.send(block, super::Deadline::after(0), again).await
        })
    }

    /// Room the blocks queued in `sender`'s stage hold.
    fn queued(sender: &GrpcTransport) -> u64 {
        sender
            .coalesce
            .as_ref()
            .expect("the sender joins blocks")
            .queued
            .load(Ordering::Acquire)
    }

    /// Blocks queued in `sender`'s stage, counted by the queue itself.
    fn queued_blocks(sender: &GrpcTransport) -> usize {
        let stage = sender.coalesce.as_ref().expect("the sender joins blocks");
        stage.queue.max_capacity() - stage.queue.capacity()
    }

    /// The room a block of `record` alone takes.
    fn charge(record: Record) -> u64 {
        let framed = prost::encoding::message::encoded_len(1, &record_to_proto(record));
        (framed as u64).max(super::MIN_BLOCK_BYTES)
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

    /// A call that fails for now answers every block in it with that failure,
    /// so each caller sends again. One refused for good sends each block again
    /// on its own, and each caller gets its own block's answer.
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

            calls.answer(1_000);
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
            let mut payloads = calls.payloads();
            assert_eq!(
                payloads[..2],
                [vec!["x"], vec!["a", "b1", "b2"]],
                "{reply:?}"
            );
            let mut resent = payloads.split_off(2);
            resent.sort();
            match reply {
                Reply::Unavailable => assert!(resent.is_empty(), "resent {resent:?}"),
                _ => assert_eq!(resent, [vec!["a"], vec!["b1", "b2"]], "{reply:?}"),
            }
        }
    }

    /// When the receiver refuses a joined call for one caller's record, the
    /// blocks go again on their own, through the outbound filters and the size
    /// check once more: that caller is refused, and the other gets the answer
    /// its own block earns, with its dead letters left out again and counted
    /// once.
    #[tokio::test]
    async fn a_refusal_of_a_joined_call_fails_only_the_block_it_was_for() {
        #[cfg(feature = "metrics")]
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        // Current-thread runtime: the stage's tasks run on this thread and see it.
        #[cfg(feature = "metrics")]
        let _local = metrics::set_default_local_recorder(&recorder);

        let (calls, uri) = recording(0).await;
        *calls.reply.lock() = Reply::RefuseMarked;
        let mut config = client(&uri, joining(1 << 16, 1)).with_max_message_size(256);
        config.filters_out = vec![FilterRule {
            expression: r#"status == "bad""#.into(),
            action: FilterAction::Dlq,
        }];
        let sender = sender(config).await;
        let oversize = format!(r#"{{"pad":"{}"}}"#, "x".repeat(300));
        let names = ["x", "a0", "!a1", "b0"].map(|n| format!(r#"{{"n":"{n}"}}"#));
        let [x, a0, a1, b0] = names.each_ref().map(String::as_str);

        let first = offer(&sender, &[x]);
        calls.arrived(1).await;
        let bad = queue(&sender, &[a0, a1]).await;
        let other = queue(&sender, &[b0, r#"{"status":"bad"}"#, oversize.as_str()]).await;

        calls.answer(1_000);
        assert!(result(first).await.is_ok());
        let refused = result(bad).await;
        assert!(
            refused.is_fatal(),
            "the caller with the bad record got {refused:?}"
        );
        let got = result(other).await;
        assert!(got.is_filtered_dlq(), "the other caller got {got:?}");
        let mut payloads = calls.payloads();
        assert_eq!(payloads[..2], [vec![x], vec![a0, a1, b0]]);
        let mut resent = payloads.split_off(2);
        resent.sort();
        assert_eq!(resent, [vec![a0, a1], vec![b0]]);

        #[cfg(feature = "metrics")]
        {
            let rendered = recorder.handle().render();
            let too_large = series(
                &rendered,
                "transport_message_too_large_total",
                &[r#"transport="grpc""#],
            );
            assert_eq!(too_large, 1.0, "counted on the resend too:\n{rendered}");
        }
    }

    /// At most `in_flight` calls run, each carries at most `max_batch_bytes`,
    /// and a caller that finds the room full waits for it: the queue never
    /// holds more than its room, and every block is sent once.
    #[tokio::test]
    async fn no_more_than_in_flight_calls_run_and_a_caller_with_no_room_waits() {
        let (calls, uri) = recording(0).await;
        // 100 byte blocks and a 250 byte cap: two blocks a call, and 1,000 bytes of room.
        let sender = sender(client(&uri, joining(250, 2))).await;
        let names: Vec<String> = (0..16)
            .map(|i| format!("{i:03}{}", "r".repeat(97)))
            .collect();
        let each = charge(record(&names[0]));
        let full = (1_000 / each) * each;
        let callers: Vec<_> = names
            .iter()
            .map(|name| offer(&sender, &[name.as_str()]))
            .collect();
        calls.arrived(2).await;
        until("the room filled", || queued(&sender) == full).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(queued(&sender), full, "the queue held more than its room");
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
            assert!(bytes <= 250, "a call of {bytes} bytes");
        }
        assert_eq!(calls.peak(), 2);
        assert_eq!(queued(&sender), 0);
    }

    /// The room counts a block's encoded length, so blocks of headers and no
    /// payload stop at it as payload does.
    #[tokio::test]
    async fn blocks_of_headers_and_no_payload_stop_at_the_room() {
        let (calls, uri) = recording(0).await;
        let sender = sender(client(&uri, joining(1_000, 1))).await;
        let heavy = || {
            let mut record = record("");
            record.headers = (0..8).map(|i| (format!("h{i}"), vec![b'v'; 64])).collect();
            record
        };
        let each = charge(heavy());
        let fits = usize::try_from(4_000 / each).unwrap();
        assert!(fits < 20, "{fits} blocks of {each} bytes fit the room");

        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let callers: Vec<_> = (0..20)
            .map(|_| {
                let sender = Arc::clone(&sender);
                let records = vec![heavy()];
                tokio::spawn(async move { sender.send_batch(&records).await })
            })
            .collect();
        let full = each * fits as u64;
        until("the room filled", || queued(&sender) == full).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(queued(&sender), full, "blocks queued past the room");
        // The stage holds the first of them out of the queue, waiting for the slot.
        assert_eq!(
            queued_blocks(&sender) + 1,
            fits,
            "blocks queued past the room"
        );

        calls.answer(1_000);
        assert!(result(first).await.is_ok());
        for caller in callers {
            assert!(result(caller).await.is_ok());
        }
        let sent: usize = calls.batches.lock().iter().map(|b| b.records.len()).sum();
        assert_eq!(sent, 21, "a block was dropped or sent twice");
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

    /// Joining is decided on a block's encoded length. A block at the cap goes
    /// straight out past a busy slot without queueing, and so does a block of
    /// empty payloads whose headers take it past `max_message_size`, split
    /// within the limit. No request passes the limit.
    #[tokio::test]
    async fn a_block_at_the_cap_goes_straight_out_past_a_busy_slot() {
        const LIMIT: usize = 1024;
        let (calls, uri) = recording_within(0, LIMIT).await;
        let sender = sender(client(&uri, joining(200, 1)).with_max_message_size(LIMIT)).await;
        let small = offer(&sender, &["s".repeat(50).as_str()]);
        calls.arrived(1).await;
        let large = offer(&sender, &["l".repeat(200).as_str()]);
        calls.arrived(2).await;
        // No payload, and about 1,170 bytes of headers.
        let heavy: Vec<Record> = (0..4)
            .map(|_| {
                let mut record = record("");
                record.headers = (0..4).map(|i| (format!("h{i}"), vec![b'v'; 64])).collect();
                record
            })
            .collect();
        let alone = {
            let sender = Arc::clone(&sender);
            tokio::spawn(async move { sender.send_batch(&heavy).await })
        };
        calls.arrived(3).await;
        assert_eq!(queued(&sender), 0, "a block at the cap was queued");
        assert_eq!(calls.peak(), 3, "a block at the cap waited for the slot");

        calls.answer(1_000);
        for caller in [small, large, alone] {
            let got = result(caller).await;
            assert!(got.is_ok(), "got {got:?}");
        }
        let batches = calls.batches.lock();
        let sent: usize = batches.iter().map(|batch| batch.records.len()).sum();
        assert_eq!(sent, 6, "a record was dropped or sent twice");
        for batch in batches.iter() {
            let len = prost::Message::encoded_len(batch);
            assert!(len <= LIMIT, "a request of {len} bytes past {LIMIT}");
        }
    }

    /// The blocks of a joined call the receiver refuses for good go again on
    /// their own through the same `in_flight` slots, each framed again only
    /// once it holds one, so the refusal never opens, or holds records for,
    /// more than `in_flight` requests at once.
    #[tokio::test]
    async fn resends_after_a_refused_joined_call_hold_to_in_flight() {
        let (calls, uri) = recording(0).await;
        *calls.reply.lock() = Reply::RefuseMarked;
        let sender = sender(client(&uri, joining(1 << 16, 2))).await;
        let x = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let y = offer(&sender, &["y"]);
        calls.arrived(2).await;
        let names: Vec<String> = (0..10)
            .map(|i| {
                if i == 5 {
                    format!("!b{i}")
                } else {
                    format!("b{i}")
                }
            })
            .collect();
        let prepared = Arc::new(AtomicUsize::new(0));
        let mut callers = Vec::new();
        for name in &names {
            let before = queued(&sender);
            callers.push(resendable(&sender, name, &prepared));
            until("the block queued", || queued(&sender) > before).await;
        }

        // x and y return, then the ten blocks go as one call, which is refused.
        calls.answer(2);
        calls.arrived(3).await;
        calls.answer(1);
        // Two resends take the two slots and are held.
        calls.arrived(5).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            *calls.arrived.borrow(),
            5,
            "resends past the in_flight slots"
        );
        assert_eq!(
            prepared.load(Ordering::SeqCst),
            2,
            "resends framed before they held a slot"
        );
        assert_eq!(calls.peak(), 2);

        calls.answer(1_000);
        assert!(result(x).await.is_ok());
        assert!(result(y).await.is_ok());
        for (name, caller) in names.iter().zip(callers) {
            let got = result(caller).await;
            assert_eq!(got.is_fatal(), name.starts_with('!'), "{name} got {got:?}");
        }
        assert_eq!(prepared.load(Ordering::SeqCst), names.len());
        assert_eq!(*calls.arrived.borrow(), 13);
        assert_eq!(calls.peak(), 2);
    }

    /// `close` answers every caller still waiting `Backpressured` and stops the
    /// call in flight, counted out of the RPCs in flight, with every slot held
    /// by a receiver that never answers and no `send_timeout_ms`.
    #[tokio::test]
    async fn close_answers_every_waiting_caller_backpressured() {
        let (calls, uri) = recording(0).await;
        let mut config = client(&uri, joining(1 << 16, 1));
        config.send_timeout_ms = 0;
        let sender = sender(config).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let a = queue(&sender, &["a"]).await;
        let b = queue(&sender, &["b"]).await;
        #[cfg(feature = "metrics")]
        assert_eq!(sender.inflight.load(Ordering::SeqCst), 1);

        sender.close().await.unwrap();
        for caller in [first, a, b] {
            let got = result(caller).await;
            assert!(
                got.is_backpressured(),
                "after close a waiting caller got {got:?}"
            );
        }
        until("the call in flight stopped", || {
            calls.dropped.load(Ordering::SeqCst) == 1
        })
        .await;
        assert_eq!(calls.payloads(), [["x"]], "a block was sent after close");
        #[cfg(feature = "metrics")]
        until("the stopped call counted out", || {
            sender.inflight.load(Ordering::SeqCst) == 0
        })
        .await;
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

    /// A receiver whose decode limit is below the sender's takes each block
    /// alone, but refuses the joined call `ResourceExhausted` once it
    /// decompresses it. Each block then goes again on its own, and every one
    /// lands.
    #[tokio::test]
    async fn a_joined_call_past_the_receivers_limit_goes_again_block_by_block() {
        const LIMIT: usize = 1024;
        let (calls, uri) = recording_within(0, LIMIT).await;
        let sender = sender(
            client(&uri, joining(1 << 16, 1)).with_compression(Compression::new(Codec::Gzip)),
        )
        .await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        // Four blocks of 600 bytes: each fits the receiver, and no two do.
        let names: Vec<String> = (0..4).map(|i| format!("{i}{}", "z".repeat(599))).collect();
        let mut callers = Vec::new();
        for name in &names {
            callers.push(queue(&sender, &[name.as_str()]).await);
        }

        calls.answer(1_000);
        assert!(result(first).await.is_ok());
        for caller in callers {
            let got = result(caller).await;
            assert!(got.is_ok(), "a block the receiver takes alone got {got:?}");
        }
        // The refused joined call never reached the service.
        let mut want = vec![vec!["x".to_owned()]];
        want.extend(names.iter().map(|name| vec![name.clone()]));
        let mut got = calls.payloads();
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }

    /// `send_timeout_ms` bounds the wait for a call to take a block. Against a
    /// receiver that never answers, a block no call takes is answered
    /// `Backpressured` at its caller's deadline and never sent, and a block a
    /// call took is answered when that request reaches `send_timeout_ms`.
    #[tokio::test]
    async fn a_queued_block_is_answered_at_send_timeout_ms_by_a_receiver_that_hangs() {
        const LIMIT_MS: u64 = 500;
        #[cfg(feature = "metrics")]
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        // Current-thread runtime: the stage's tasks run on this thread and see it.
        #[cfg(feature = "metrics")]
        let _local = metrics::set_default_local_recorder(&recorder);

        let (calls, uri) = recording(0).await;
        // 100 byte blocks and a 150 byte cap: one block a call, so each waits its turn.
        let mut config = client(&uri, joining(150, 1));
        config.send_timeout_ms = LIMIT_MS;
        let sender = sender(config).await;
        let [x, a, b, c] = ['x', 'a', 'b', 'c'].map(|letter| letter.to_string().repeat(100));
        let first = offer(&sender, &[x.as_str()]);
        calls.arrived(1).await;
        // Queued well after the first call started, so a is still waiting when
        // that call ends and is taken next, and b and c reach their deadline
        // while the second call holds the slot.
        tokio::time::sleep(Duration::from_millis(LIMIT_MS / 5)).await;
        let started = tokio::time::Instant::now();
        let taken = queue(&sender, &[a.as_str()]).await;
        let mut withdrawn = Vec::new();
        for name in [&b, &c] {
            withdrawn.push(queue(&sender, &[name.as_str()]).await);
        }

        for caller in withdrawn {
            let got = result(caller).await;
            assert!(got.is_backpressured(), "a block no call took got {got:?}");
        }
        let took = started.elapsed();
        // Waiting for the second call to end would answer at 1.6 limits.
        assert!(
            took < Duration::from_millis(LIMIT_MS * 13 / 10),
            "blocks no call took answered after {took:?} at a {LIMIT_MS} ms limit"
        );
        let got = result(taken).await;
        assert!(got.is_backpressured(), "a block in a hung call got {got:?}");
        assert!(result(first).await.is_backpressured());

        // Past the calls a slot could still have started for the blocks.
        tokio::time::sleep(Duration::from_millis(LIMIT_MS * 2)).await;
        let payloads = calls.payloads();
        assert!(
            payloads.iter().flatten().all(|p| *p != b && *p != c),
            "a block no call had taken was sent"
        );
        assert_eq!(queued(&sender), 0);

        // Each block withdrawn counts as a wait that ran out, and each call
        // past its own deadline counts once, whatever it carried.
        #[cfg(feature = "metrics")]
        {
            let rendered = recorder.handle().render();
            let timeouts = series(
                &rendered,
                "transport_coalesced_timeouts_total",
                &[r#"transport="grpc""#],
            );
            assert_eq!(timeouts, 2.0, "wait timeouts:\n{rendered}");
            let redelivered = series(
                &rendered,
                "transport_redelivered_total",
                &[r#"transport="grpc""#, r#"reason="deadline""#],
            );
            assert_eq!(
                redelivered,
                payloads.len() as f64,
                "one count per call past its deadline:\n{rendered}"
            );
        }
    }

    /// After a refused joined call, a resend's wait for a slot keeps its
    /// caller's deadline from the start of `send_batch`: past it, the block is
    /// answered `Backpressured` and not sent. A resend that holds a slot gets
    /// its own request's answer, past that deadline.
    #[tokio::test]
    async fn a_resend_after_a_refused_joined_call_keeps_the_callers_deadline() {
        const LIMIT_MS: u64 = 1_000;
        let (calls, uri) = recording(0).await;
        *calls.reply.lock() = Reply::RefuseMarked;
        let mut config = client(&uri, joining(1 << 16, 1));
        config.send_timeout_ms = LIMIT_MS;
        let sender = sender(config).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let started = tokio::time::Instant::now();
        let bad = queue(&sender, &["!a"]).await;
        let clean = queue(&sender, &["b"]).await;

        // The joined call is refused past half the deadline. One resend takes
        // the only slot and is held, and the other waits for the slot.
        tokio::time::sleep(Duration::from_millis(LIMIT_MS * 6 / 10)).await;
        calls.answer(2);
        calls.arrived(3).await;
        until("the resend with no slot answered", || {
            bad.is_finished() || clean.is_finished()
        })
        .await;
        let took = started.elapsed();
        // A deadline restarted for the resend would answer at 1.6 limits.
        assert!(
            took < Duration::from_millis(LIMIT_MS * 13 / 10),
            "answered after {took:?} at a {LIMIT_MS} ms limit"
        );
        let (waited, held, held_is_bad) = if bad.is_finished() {
            (bad, clean, false)
        } else {
            (clean, bad, true)
        };
        let got = result(waited).await;
        assert!(got.is_backpressured(), "a resend with no slot got {got:?}");

        calls.answer(1);
        let got = result(held).await;
        if held_is_bad {
            assert!(got.is_fatal(), "the refused resend got {got:?}");
        } else {
            assert!(got.is_ok(), "the resend in a request got {got:?}");
        }
        assert!(result(first).await.is_ok());
        assert_eq!(*calls.arrived.borrow(), 3, "a resend with no slot was sent");
        #[cfg(feature = "metrics")]
        until("the resends counted out", || {
            sender.inflight.load(Ordering::SeqCst) == 0
        })
        .await;
    }

    /// The deadline bounds the wait for a call to take a block, not the call:
    /// a block taken just before its caller's deadline gets that call's own
    /// answer from a receiver that answers within `send_timeout_ms`, in one
    /// request.
    #[tokio::test]
    async fn a_block_taken_near_its_deadline_gets_its_calls_answer() {
        const LIMIT_MS: u64 = 2_000;
        let (calls, uri) = recording(0).await;
        let mut config = client(&uri, joining(1 << 16, 1));
        config.send_timeout_ms = LIMIT_MS;
        let sender = sender(config).await;
        let first = offer(&sender, &["x"]);
        calls.arrived(1).await;
        let a = queue(&sender, &["a"]).await;

        // The slot frees at 0.8 of a's deadline. The check lands past that
        // deadline and well before a's call times out, at 1.8 of the limit.
        tokio::time::sleep(Duration::from_millis(LIMIT_MS * 8 / 10)).await;
        calls.answer(1);
        calls.arrived(2).await;
        tokio::time::sleep(Duration::from_millis(LIMIT_MS * 3 / 10)).await;
        if a.is_finished() {
            let early = result(a).await;
            panic!("answered before its call returned: {early:?}");
        }
        calls.answer(1);
        assert!(result(first).await.is_ok());
        let got = result(a).await;
        assert!(got.is_ok(), "a block in a call got {got:?}");
        assert_eq!(calls.payloads(), [["x"], ["a"]]);
    }

    /// The stage takes a block only if its caller has not withdrawn it, and a
    /// block it has taken can no longer be withdrawn.
    #[test]
    fn a_withdrawn_block_is_never_taken() {
        let item = |withdrawn: bool| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            let item = super::Item {
                records: Vec::new(),
                framed: 0,
                charge: super::MIN_BLOCK_BYTES,
                reply,
                settled: Arc::new(std::sync::atomic::AtomicBool::new(withdrawn)),
                room: None,
                #[cfg(feature = "transport-trace")]
                context: opentelemetry::Context::current(),
            };
            (item, answer)
        };
        let queued = std::sync::atomic::AtomicU64::new(2 * super::MIN_BLOCK_BYTES);
        let (withdrawn, _waiting) = item(true);
        assert!(
            super::leave_queue(withdrawn, &queued).is_none(),
            "a withdrawn block was taken"
        );
        let (live, _waiting) = item(false);
        let settled = Arc::clone(&live.settled);
        assert!(super::leave_queue(live, &queued).is_some());
        assert!(!super::settle(&settled), "a taken block was withdrawn");
        assert_eq!(queued.load(Ordering::SeqCst), 0);
    }

    /// Dropping the transport stops the calls its stage has in flight.
    #[tokio::test]
    async fn dropping_the_transport_stops_its_calls_in_flight() {
        let (calls, uri) = recording(0).await;
        let sender = GrpcTransport::new(&client(&uri, joining(1 << 16, 1)))
            .await
            .unwrap();
        let records = block(&["x"]);
        tokio::select! {
            sent = sender.send_batch(&records) => {
                panic!("answered by a receiver that holds every call: {sent:?}");
            }
            () = calls.arrived(1) => {}
        }
        // The caller has gone, and its block is in a call.
        drop(sender);
        until("the call in flight stopped", || {
            calls.dropped.load(Ordering::SeqCst) == 1
        })
        .await;
    }

    /// The value of the series `name` with every one of `labels`, else 0.
    #[cfg(feature = "metrics")]
    fn series(rendered: &str, name: &str, labels: &[&str]) -> f64 {
        rendered
            .lines()
            .filter(|line| line.starts_with(&format!("{name}{{")))
            .filter(|line| labels.iter().all(|label| line.contains(label)))
            .find_map(|line| line.rsplit(' ').next()?.parse().ok())
            .unwrap_or(0.0)
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
