// Project:   scalo
// File:      src/transport/test_support/delivery/parallel_pinned.rs
// Purpose:   Approach: per caller batch, every destination at once, each send boxed where it is built
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`ParallelPinned`]: [`ParallelFanout`](super::ParallelFanout) with each send
//! future boxed as it is built, so `join_all` holds and moves a pointer to it
//! and never copies the future. The outcomes merge the same way.

use futures_util::future::join_all;

use super::{
    CallerBatch, DeliveryApproach, Destinations, Outcome, answer, block_for, destination_send,
    merge_results,
};
use crate::transport::TransportSender;

/// Per caller batch, inline, every destination at the same time, each send
/// pinned in a box.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelPinned;

impl ParallelPinned {
    /// Registry name.
    pub const NAME: &'static str = "parallel_pinned";
}

impl DeliveryApproach for ParallelPinned {
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
                sends.push(Box::pin(destination_send(sender, block, places)));
            }
        }
        merge_results(&mut outcomes, join_all(sends).await);
        answer(outcomes, batch.answer)
    }
}
