// Project:   scalo
// File:      src/transport/test_support/delivery/batch_per_destination.rs
// Purpose:   Approach: one send_batch per destination in turn, an outcome per record
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`BatchPerDestination`]: the fetcher's shape on the batch path. Each
//! destination gets one [`send_batch`](TransportSender::send_batch) of the
//! records routed to it, one destination after another, and every destination
//! is sent whatever the ones before it answered, so each record's outcome is
//! the worst of its own destinations only.

use super::{CallerBatch, DeliveryApproach, Destinations, Outcome, answer, block_for, send_block};
use crate::transport::TransportSender;

/// One send_batch per destination in turn, with no stop at the first failure.
#[derive(Debug, Clone, Copy, Default)]
pub struct BatchPerDestination;

impl BatchPerDestination {
    /// Registry name.
    pub const NAME: &'static str = "batch_per_destination";
}

impl DeliveryApproach for BatchPerDestination {
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
        for (id, _, sender) in destinations.iter() {
            let (block, places) = block_for(&batch, id, sender, &mut outcomes);
            let results = send_block(sender, &block).await;
            for (i, outcome) in places.into_iter().zip(results) {
                outcomes[i] = outcomes[i].max(outcome);
            }
        }
        answer(outcomes, batch.answer)
    }
}
