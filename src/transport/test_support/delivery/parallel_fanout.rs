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
//! destination waits for another. By default every block is built before the
//! first send. With [`build_in_send`](ParallelFanout::build_in_send) each
//! destination's future builds its own block, so the first send starts after
//! one block is built, as [`InlineSequential`](super::InlineSequential) does.

use futures_util::future::join_all;

use super::{
    CallerBatch, DeliveryApproach, Destinations, Outcome, answer, block_for, send_block, split_for,
};
use crate::transport::TransportSender;

/// Per caller batch, inline, every destination at the same time.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelFanout {
    /// Build each destination's block inside its send future.
    pub build_in_send: bool,
}

impl ParallelFanout {
    /// Registry name, building every block before the first send.
    pub const NAME: &'static str = "parallel_fanout";
    /// Registry name of the same approach building each block in its send.
    pub const IN_SEND: &'static str = "parallel_fanout_in_send";
}

impl DeliveryApproach for ParallelFanout {
    fn name(&self) -> &'static str {
        if self.build_in_send {
            Self::IN_SEND
        } else {
            Self::NAME
        }
    }

    async fn deliver<'a, S: TransportSender>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        let _lease = destinations.ledger().lease(batch.payload_bytes());
        let mut outcomes = vec![Outcome::Sent; batch.records.len()];
        if self.build_in_send {
            let sends = destinations.iter().map(|(id, _, sender)| {
                let batch = &batch;
                async move {
                    let split = split_for(batch, id, sender);
                    let results = send_block(sender, &split.block).await;
                    (split, results)
                }
            });
            for (split, results) in join_all(sends).await {
                for i in split.dead_lettered {
                    outcomes[i] = outcomes[i].max(Outcome::DeadLettered);
                }
                for (i, outcome) in split.places.into_iter().zip(results) {
                    outcomes[i] = outcomes[i].max(outcome);
                }
            }
            return answer(outcomes, batch.answer);
        }
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
