// Project:   scalo
// File:      src/transport/test_support/delivery/parallel_fanout.rs
// Purpose:   Approach: per caller batch, one send_batch per destination, all at once
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`ParallelFanout`]: as [`InlineSequential`](super::InlineSequential), the
//! batch is delivered inside the caller's task with one
//! [`send_batch`](TransportSender::send_batch) per destination, but every
//! destination is sent at the same time and the outcomes are merged, so no
//! destination waits for another.

use futures_util::future::join_all;

use super::{CallerBatch, DeliveryApproach, Destinations, Outcome, answer, block_for, send_block};
use crate::transport::TransportSender;

/// Per caller batch, inline, every destination at the same time.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelFanout;

impl ParallelFanout {
    /// Registry name.
    pub const NAME: &'static str = "parallel_fanout";
}

impl DeliveryApproach for ParallelFanout {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn deliver<'a, S: TransportSender>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        let _lease = destinations.ledger().lease(batch.payload_bytes());
        let mut outcomes = vec![Outcome::Sent; batch.records.len()];
        let mut sends = Vec::new();
        for (id, _, sender) in destinations.iter() {
            let (block, places) = block_for(&batch, id, sender, &mut outcomes);
            if !block.is_empty() {
                sends.push(async move { (places, send_block(sender, &block).await) });
            }
        }
        for (places, results) in join_all(sends).await {
            for (i, outcome) in places.into_iter().zip(results) {
                outcomes[i] = outcomes[i].max(outcome);
            }
        }
        answer(outcomes, batch.answer)
    }
}
