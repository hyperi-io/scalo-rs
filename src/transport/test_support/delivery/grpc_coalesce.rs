// Project:   scalo
// File:      src/transport/test_support/delivery/grpc_coalesce.rs
// Purpose:   Approach: many callers' small blocks for one gRPC destination sent as one call
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`GrpcCoalesce`]: a coalescing stage in front of every
//! [`Shape::Batch`] destination. A caller's block for such a destination goes to
//! a task that owns the stage. The task sends what it holds as one
//! [`send_batch`](TransportSender::send_batch) when the held payload reaches
//! [`target_bytes`](GrpcCoalesce::target_bytes), or
//! [`linger`](GrpcCoalesce::linger) after the first block arrived, whichever
//! comes first. Up to [`in_flight`](GrpcCoalesce::in_flight) such sends run at
//! once. A block of `target_bytes` or more skips the stage and is sent by its
//! caller, as [`InlineSequential`](super::InlineSequential) sends it, and does
//! not count against `in_flight`. [`Shape::PerRecord`] destinations are sent
//! directly, since a producer batches for itself.
//!
//! A caller is answered when the send carrying its records returns, never when
//! they are queued. Records the sender would dead-letter are taken out by the
//! caller first and answered dead-lettered, so a refusal answers its own caller
//! only. A send that fails answers every caller in it with that failure, so each
//! sends again: copies, never loss. A `FilteredDlq` answer names no record, so
//! the records are sent one by one after the call's in-flight slot is given back.
//!
//! Waiting payload is bounded by [`ROOM_TARGETS`] targets. A caller that finds
//! no room waits for it, and nothing is dropped. The bytes are the callers' own,
//! leased by each caller until it is answered. A caller that is cancelled
//! releases its lease, and its block is dropped if the stage has not taken it.
//! tokio's timer fires on whole milliseconds, so a linger under 1 ms waits at
//! most 1 ms.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::future::join_all;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use super::{
    CallerBatch, DeliveryApproach, DestinationId, Destinations, Outcome, Shape, answer, block_for,
    merge_results, send_block,
};
use crate::transport::{Record, SendResult, TransportSender};

/// Blocks a stage may have queued at once, whatever their bytes.
const QUEUED_BLOCKS: usize = 4096;

/// Payload a stage holds waiting, in multiples of the byte target.
pub const ROOM_TARGETS: u64 = 4;

/// Many callers' blocks for a batch-shaped destination, sent together.
#[derive(Clone)]
pub struct GrpcCoalesce {
    /// Payload bytes that send what the stage holds at once.
    pub target_bytes: u64,
    /// The longest the first block waits for others before it is sent.
    pub linger: Duration,
    /// Coalesced sends in flight to one destination at once.
    pub in_flight: usize,
    stages: Arc<OnceLock<Vec<Option<Stage>>>>,
    peak_queued: Arc<AtomicU64>,
}

impl std::fmt::Debug for GrpcCoalesce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcCoalesce")
            .field("target_bytes", &self.target_bytes)
            .field("linger", &self.linger)
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

impl GrpcCoalesce {
    /// Registry name.
    pub const NAME: &'static str = "grpc_coalesce";

    /// A 128 KiB target, a 2 ms linger and 4 sends in flight.
    #[must_use]
    pub fn new() -> Self {
        Self {
            target_bytes: 128 * 1024,
            linger: Duration::from_millis(2),
            in_flight: 4,
            stages: Arc::new(OnceLock::new()),
            peak_queued: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Most payload bytes waiting in a stage at once.
    #[must_use]
    pub fn peak_queued_bytes(&self) -> u64 {
        self.peak_queued.load(Ordering::Relaxed)
    }

    fn stages<S: TransportSender + 'static>(
        &self,
        destinations: &Destinations<S>,
    ) -> &[Option<Stage>] {
        self.stages.get_or_init(|| {
            destinations
                .iter()
                .map(|(id, _, _)| {
                    (destinations.shape(id) == Shape::Batch).then(|| {
                        Stage::spawn(
                            destinations.sender_arc(id),
                            Flush {
                                target_bytes: self.target_bytes,
                                linger: self.linger,
                                in_flight: self.in_flight.max(1),
                            },
                            Arc::clone(&self.peak_queued),
                        )
                    })
                })
                .collect()
        })
    }

    /// Send `block` to destination `id`: through its stage, or by this caller
    /// when the destination has none or the block is at the target. Returns
    /// `places` with the outcomes.
    async fn send_to<S: TransportSender + 'static>(
        &self,
        stages: &[Option<Stage>],
        id: DestinationId,
        sender: &S,
        block: Vec<Record>,
        places: Vec<usize>,
    ) -> (Vec<usize>, Vec<Outcome>) {
        let Some(stage) = stages.get(id.0).and_then(Option::as_ref) else {
            return (places, send_block(sender, &block).await);
        };
        let bytes: u64 = block.iter().map(|r| r.payload.len() as u64).sum();
        if bytes >= self.target_bytes {
            return (places, send_block(sender, &block).await);
        }
        (places, stage.submit(block, bytes).await)
    }
}

impl Default for GrpcCoalesce {
    fn default() -> Self {
        Self::new()
    }
}

impl DeliveryApproach for GrpcCoalesce {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn deliver<'a, S: TransportSender + 'static>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        let _lease = destinations.ledger().lease(batch.payload_bytes());
        let stages = self.stages(destinations);
        let mut outcomes = vec![Outcome::Sent; batch.records.len()];
        // The one block seen so far, held back until a second shows there is
        // something to send at the same time.
        let mut only: Option<(DestinationId, &S, Vec<Record>, Vec<usize>)> = None;
        let mut sends = Vec::new();
        for (id, _, sender) in destinations.iter() {
            let (block, places) = block_for(&batch, id, sender, &mut outcomes);
            if block.is_empty() {
                continue;
            }
            if sends.is_empty() && only.is_none() {
                only = Some((id, sender, block, places));
                continue;
            }
            if let Some((first_id, first, first_block, first_places)) = only.take() {
                sends.push(Box::pin(self.send_to(
                    stages,
                    first_id,
                    first,
                    first_block,
                    first_places,
                )));
            }
            sends.push(Box::pin(self.send_to(stages, id, sender, block, places)));
        }

        if let Some((id, sender, block, places)) = only {
            let sent = self.send_to(stages, id, sender, block, places).await;
            merge_results(&mut outcomes, [sent]);
        } else {
            merge_results(&mut outcomes, join_all(sends).await);
        }
        answer(outcomes, batch.answer)
    }
}

/// One caller's queued block and where its outcomes go.
struct Item {
    records: Vec<Record>,
    bytes: u64,
    queued_at: Instant,
    reply: oneshot::Sender<Vec<Outcome>>,
    /// Room in the stage, given back when the stage takes the block.
    room: Option<OwnedSemaphorePermit>,
}

/// When the stage sends what it holds.
#[derive(Clone, Copy)]
struct Flush {
    target_bytes: u64,
    linger: Duration,
    in_flight: usize,
}

/// The coalescing stage in front of one destination.
struct Stage {
    queue: mpsc::Sender<Item>,
    room: Arc<Semaphore>,
    queued: Arc<AtomicU64>,
    peak_queued: Arc<AtomicU64>,
}

impl Stage {
    fn spawn<S: TransportSender + 'static>(
        sender: Arc<S>,
        flush: Flush,
        peak_queued: Arc<AtomicU64>,
    ) -> Self {
        let (queue, blocks) = mpsc::channel(QUEUED_BLOCKS);
        let queued = Arc::new(AtomicU64::new(0));
        let room = flush
            .target_bytes
            .saturating_mul(ROOM_TARGETS)
            .clamp(1, u64::from(u32::MAX));
        tokio::spawn(run_stage(sender, blocks, Arc::clone(&queued), flush));
        Self {
            queue,
            room: Arc::new(Semaphore::new(usize::try_from(room).unwrap_or(usize::MAX))),
            queued,
            peak_queued,
        }
    }

    /// Queue `records` and wait for their outcomes.
    async fn submit(&self, records: Vec<Record>, bytes: u64) -> Vec<Outcome> {
        let count = records.len();
        let permits = u32::try_from(bytes).unwrap_or(u32::MAX);
        let Ok(room) = Arc::clone(&self.room).acquire_many_owned(permits).await else {
            return vec![Outcome::Failed; count];
        };
        let Ok(slot) = self.queue.reserve().await else {
            return vec![Outcome::Failed; count];
        };
        let (reply, answered) = oneshot::channel();
        let queued = self.queued.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.peak_queued.fetch_max(queued, Ordering::Relaxed);
        slot.send(Item {
            records,
            bytes,
            queued_at: Instant::now(),
            reply,
            room: Some(room),
        });
        answered
            .await
            .unwrap_or_else(|_| vec![Outcome::Failed; count])
    }
}

/// Take blocks, cut them into sends and start each when a slot is free. Ends
/// when every approach clone is gone and the queue is empty.
async fn run_stage<S: TransportSender + 'static>(
    sender: Arc<S>,
    mut blocks: mpsc::Receiver<Item>,
    queued: Arc<AtomicU64>,
    flush: Flush,
) {
    let slots = Arc::new(Semaphore::new(flush.in_flight));
    loop {
        let Ok(slot) = Arc::clone(&slots).acquire_owned().await else {
            return;
        };
        let Some(first) = next_live(&mut blocks, &queued).await else {
            return;
        };
        let deadline = first.queued_at + flush.linger;
        let mut bytes = first.bytes;
        let mut taken = vec![first];
        while bytes < flush.target_bytes
            && let Ok(mut item) = blocks.try_recv()
        {
            if leave_queue(&mut item, &queued) {
                bytes += item.bytes;
                taken.push(item);
            }
        }
        if bytes < flush.target_bytes {
            let wait = tokio::time::sleep_until(deadline);
            tokio::pin!(wait);
            while bytes < flush.target_bytes {
                tokio::select! {
                    biased;
                    item = blocks.recv() => match item {
                        Some(mut item) => {
                            if leave_queue(&mut item, &queued) {
                                bytes += item.bytes;
                                taken.push(item);
                            }
                        }
                        None => break,
                    },
                    () = &mut wait => break,
                }
            }
        }
        // A caller can go while the stage waits for more blocks.
        taken.retain(|item| !item.reply.is_closed());
        if !taken.is_empty() {
            tokio::spawn(send_taken(Arc::clone(&sender), slot, taken));
        }
    }
}

/// The next queued block whose caller is still waiting.
async fn next_live(blocks: &mut mpsc::Receiver<Item>, queued: &AtomicU64) -> Option<Item> {
    loop {
        let mut item = blocks.recv().await?;
        if leave_queue(&mut item, queued) {
            return Some(item);
        }
    }
}

/// Count `item` as out of the queue and give its room back. False when its
/// caller has gone, and the block is not sent.
fn leave_queue(item: &mut Item, queued: &AtomicU64) -> bool {
    queued.fetch_sub(item.bytes, Ordering::AcqRel);
    item.room = None;
    !item.reply.is_closed()
}

/// Send the taken blocks as one call and answer each caller its own records.
async fn send_taken<S: TransportSender>(
    sender: Arc<S>,
    slot: OwnedSemaphorePermit,
    mut taken: Vec<Item>,
) {
    let total: usize = taken.iter().map(|i| i.records.len()).sum();
    let mut merged: Vec<Record> = Vec::with_capacity(total);
    let mut counts = Vec::with_capacity(taken.len());
    for item in &mut taken {
        counts.push(item.records.len());
        merged.append(&mut item.records);
    }
    let result = sender.send_batch(&merged).await;
    drop(slot);
    if matches!(result, SendResult::FilteredDlq) {
        // The answer names no record, so each is sent again on its own.
        let mut outcomes = Vec::with_capacity(total);
        for record in &merged {
            outcomes.push(Outcome::from_send(&sender.send_record(record).await));
        }
        let mut rest = outcomes.as_slice();
        for (item, count) in taken.into_iter().zip(counts) {
            let (mine, after) = rest.split_at(count);
            rest = after;
            let _ = item.reply.send(mine.to_vec());
        }
        return;
    }
    let outcome = Outcome::from_send(&result);
    for (item, count) in taken.into_iter().zip(counts) {
        let _ = item.reply.send(vec![outcome; count]);
    }
}
