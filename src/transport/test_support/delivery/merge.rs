// Project:   scalo
// File:      src/transport/test_support/delivery/merge.rs
// Purpose:   Approach: parallel fan-out with a merge stage per batch-shaped destination
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`Merge`]: [`ParallelFanout`](super::ParallelFanout) plus a merge stage in
//! front of every [`Shape::Batch`] destination. A caller's block for such a
//! destination is queued in the stage's buffer, and a waiting caller takes the
//! drain turn: it waits the merge window, takes every queued block, sends them
//! in one [`send_batch`](TransportSender::send_batch) and answers each caller
//! its own slice. [`Shape::PerRecord`] destinations are sent directly, since a
//! producer batches for itself.
//!
//! The buffer is a bounded channel drained with `recv_many`
//! ([`MergeStore::Channel`]) or a mutex-guarded vector swapped out whole
//! ([`MergeStore::Swap`]). It is bounded in payload bytes by a semaphore, and
//! a block larger than the whole buffer is sent directly. The
//! bytes are the callers' own, leased by each caller until it is answered, so
//! a longer wait in the buffer shows as held bytes and the buffer takes no
//! lease of its own. A buffer of 0 bytes sends directly, as the fan-out does.
//!
//! The drain turn passes on as soon as the blocks are taken, so several merged
//! calls can be in flight to one destination, up to the sender's own limit.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::future::join_all;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::{
    CallerBatch, DeliveryApproach, DestinationId, Destinations, Outcome, Shape, answer, block_for,
    send_block,
};
use crate::transport::{Record, TransportSender};

/// Blocks one drain takes at most.
const MAX_BLOCKS_PER_DRAIN: usize = 4096;

/// How a merge stage keeps its queued blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStore {
    /// A bounded channel, drained with `recv_many`.
    Channel,
    /// A vector behind a mutex, swapped out whole by the drainer.
    Swap,
}

/// Parallel fan-out with a merge stage per batch-shaped destination.
#[derive(Clone)]
pub struct Merge {
    /// Where queued blocks wait.
    pub store: MergeStore,
    /// How long the drainer waits for more blocks before it sends.
    pub window: Duration,
    /// Payload bytes a stage may hold queued. 0 sends directly.
    pub buffer_bytes: u64,
    stages: Arc<OnceLock<Vec<Option<Stage>>>>,
    peak_queued: Arc<AtomicU64>,
}

impl std::fmt::Debug for Merge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Merge")
            .field("store", &self.store)
            .field("window", &self.window)
            .field("buffer_bytes", &self.buffer_bytes)
            .finish_non_exhaustive()
    }
}

impl Merge {
    /// Registry name of the channel store.
    pub const CHANNEL: &'static str = "merge_channel";
    /// Registry name of the swapped-buffer store.
    pub const SWAP: &'static str = "merge_swap";

    /// A merge approach with `store`, a 1 ms window and a 1 MiB buffer.
    #[must_use]
    pub fn new(store: MergeStore) -> Self {
        Self {
            store,
            window: Duration::from_millis(1),
            buffer_bytes: 1024 * 1024,
            stages: Arc::new(OnceLock::new()),
            peak_queued: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Most payload bytes queued in one stage at once.
    #[must_use]
    pub fn peak_queued_bytes(&self) -> u64 {
        self.peak_queued.load(Ordering::Relaxed)
    }

    fn stages<S: TransportSender>(&self, destinations: &Destinations<S>) -> &[Option<Stage>] {
        self.stages.get_or_init(|| {
            destinations
                .iter()
                .map(|(id, _, _)| {
                    (self.buffer_bytes > 0 && destinations.shape(id) == Shape::Batch)
                        .then(|| Stage::new(self.store, self.buffer_bytes))
                })
                .collect()
        })
    }

    /// Queue `records` on `stage` and wait for their outcomes, taking the
    /// drain turn whenever it is free and blocks are waiting.
    async fn submit<S: TransportSender>(
        &self,
        stage: &Stage,
        sender: &S,
        records: Vec<Record>,
    ) -> Vec<Outcome> {
        let count = records.len();
        let bytes: u64 = records.iter().map(|r| r.payload.len() as u64).sum();
        if bytes > stage.cap {
            // A block larger than the whole buffer is never queued.
            return send_block(sender, &records).await;
        }
        let permits = u32::try_from(bytes).unwrap_or(u32::MAX);
        let Ok(permit) = Arc::clone(&stage.room).acquire_many_owned(permits).await else {
            return vec![Outcome::Failed; count];
        };
        let (reply, mut answered) = oneshot::channel();
        let queued = stage.queued.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.peak_queued.fetch_max(queued, Ordering::Relaxed);
        let item = Item {
            records,
            bytes,
            reply,
            _room: permit,
        };
        if !stage.push(item).await {
            return vec![Outcome::Failed; count];
        }
        stage.more.notify_waiters();
        loop {
            let more = stage.more.notified();
            tokio::pin!(more);
            more.as_mut().enable();
            // Taking blocks races the answer, but once taken they are sent and
            // answered to the end, so no other caller's block is dropped.
            let taken = tokio::select! {
                biased;
                outcome = &mut answered => return outcome.unwrap_or_else(|_| vec![Outcome::Failed; count]),
                taken = self.take(stage) => taken,
            };
            match taken {
                Some(items) => send_items(stage, sender, items).await,
                None => {
                    tokio::select! {
                        biased;
                        outcome = &mut answered => {
                            return outcome.unwrap_or_else(|_| vec![Outcome::Failed; count]);
                        }
                        () = more => {}
                    }
                }
            }
        }
    }

    /// Take the drain turn, wait the window and take every queued block, or
    /// `None` when nothing was queued.
    async fn take(&self, stage: &Stage) -> Option<Vec<Item>> {
        match &stage.store {
            Store::Channel(_, receiver) => {
                let mut receiver = receiver.lock().await;
                if receiver.is_empty() {
                    return None;
                }
                if !self.window.is_zero() {
                    tokio::time::sleep(self.window).await;
                }
                let mut items = Vec::new();
                receiver.recv_many(&mut items, MAX_BLOCKS_PER_DRAIN).await;
                Some(items)
            }
            Store::Swap(turn, queue) => {
                let _turn = turn.lock().await;
                if !queue.lock().is_ok_and(|q| !q.is_empty()) {
                    return None;
                }
                if !self.window.is_zero() {
                    tokio::time::sleep(self.window).await;
                }
                queue.lock().ok().map(|mut q| std::mem::take(&mut *q))
            }
        }
    }
}

/// Send the taken blocks as one block and answer each caller its slice.
async fn send_items<S: TransportSender>(stage: &Stage, sender: &S, items: Vec<Item>) {
    let taken: u64 = items.iter().map(|i| i.bytes).sum();
    stage.queued.fetch_sub(taken, Ordering::AcqRel);
    let merged: Vec<Record> = items
        .iter()
        .flat_map(|i| i.records.iter().cloned())
        .collect();
    let mut outcomes = send_block(sender, &merged).await.into_iter();
    for item in items {
        let slice: Vec<Outcome> = outcomes.by_ref().take(item.records.len()).collect();
        let _ = item.reply.send(slice);
    }
}

impl DeliveryApproach for Merge {
    fn name(&self) -> &'static str {
        match self.store {
            MergeStore::Channel => Self::CHANNEL,
            MergeStore::Swap => Self::SWAP,
        }
    }

    async fn deliver<'a, S: TransportSender>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        let _lease = destinations.ledger().lease(batch.payload_bytes());
        let stages = self.stages(destinations);
        let mut outcomes = vec![Outcome::Sent; batch.records.len()];
        let mut sends = Vec::new();
        for (id, _, sender) in destinations.iter() {
            let (block, places) = block_for(&batch, id, sender, &mut outcomes);
            if block.is_empty() {
                continue;
            }
            let stage = stage_of(stages, id);
            sends.push(async move {
                let results = match stage {
                    Some(stage) => self.submit(stage, sender, block).await,
                    None => send_block(sender, &block).await,
                };
                (places, results)
            });
        }
        for (places, results) in join_all(sends).await {
            for (i, outcome) in places.into_iter().zip(results) {
                outcomes[i] = outcomes[i].max(outcome);
            }
        }
        answer(outcomes, batch.answer)
    }
}

fn stage_of(stages: &[Option<Stage>], id: DestinationId) -> Option<&Stage> {
    stages.get(id.0).and_then(Option::as_ref)
}

/// One caller's queued block and where its outcomes go.
struct Item {
    records: Vec<Record>,
    bytes: u64,
    reply: oneshot::Sender<Vec<Outcome>>,
    _room: OwnedSemaphorePermit,
}

enum Store {
    Channel(mpsc::Sender<Item>, tokio::sync::Mutex<mpsc::Receiver<Item>>),
    Swap(tokio::sync::Mutex<()>, std::sync::Mutex<Vec<Item>>),
}

/// The merge stage in front of one destination.
struct Stage {
    store: Store,
    /// Payload bytes the buffer may hold, as semaphore permits.
    room: Arc<Semaphore>,
    cap: u64,
    queued: AtomicU64,
    /// Woken when a block is queued.
    more: Notify,
}

impl Stage {
    fn new(store: MergeStore, buffer_bytes: u64) -> Self {
        let cap = buffer_bytes.clamp(1, u64::from(u32::MAX));
        let store = match store {
            MergeStore::Channel => {
                let (sender, receiver) = mpsc::channel(MAX_BLOCKS_PER_DRAIN);
                Store::Channel(sender, tokio::sync::Mutex::new(receiver))
            }
            MergeStore::Swap => Store::Swap(
                tokio::sync::Mutex::new(()),
                std::sync::Mutex::new(Vec::new()),
            ),
        };
        Self {
            store,
            room: Arc::new(Semaphore::new(usize::try_from(cap).unwrap_or(usize::MAX))),
            cap,
            queued: AtomicU64::new(0),
            more: Notify::new(),
        }
    }

    /// Queue `item`. False when the stage is closed.
    async fn push(&self, item: Item) -> bool {
        match &self.store {
            Store::Channel(sender, _) => sender.send(item).await.is_ok(),
            Store::Swap(_, queue) => queue.lock().map(|mut q| q.push(item)).is_ok(),
        }
    }
}
