// Project:   scalo
// File:      src/transport/test_support/delivery/auto_fanout.rs
// Purpose:   Approach: one destination as inline_sequential sends it, two or more as parallel_pinned
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`AutoFanout`]: a batch that builds exactly one block is sent as
//! [`InlineSequential`](super::InlineSequential) sends it, with no join and no
//! boxed future. A batch that builds two or more is sent as
//! [`ParallelPinned`](super::ParallelPinned) sends it.

use futures_util::future::join_all;

use super::{
    CallerBatch, DeliveryApproach, Destinations, Outcome, answer, block_for, destination_send,
    merge_results,
};
use crate::transport::{Record, SendResult, TransportSender};

/// Per caller batch, inline: one block alone, several at the same time.
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoFanout;

impl AutoFanout {
    /// Registry name.
    pub const NAME: &'static str = "auto_fanout";
}

impl DeliveryApproach for AutoFanout {
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
        // The one block seen so far, held back until a second shows there is
        // something to send at the same time.
        let mut only: Option<(&S, Vec<Record>, Vec<usize>)> = None;
        let mut sends = Vec::new();
        for (id, _, sender) in destinations.iter() {
            let (block, places) = block_for(&batch, id, sender, &mut outcomes);
            if block.is_empty() {
                continue;
            }
            if sends.is_empty() && only.is_none() {
                only = Some((sender, block, places));
                continue;
            }
            if let Some((first, first_block, first_places)) = only.take() {
                sends.push(Box::pin(destination_send(first, first_block, first_places)));
            }
            sends.push(Box::pin(destination_send(sender, block, places)));
        }

        if let Some((sender, block, places)) = only {
            let result = sender.send_batch(&block).await;
            if matches!(result, SendResult::FilteredDlq) {
                // The block answer names no record, so each is sent again on its own.
                for (record, i) in block.iter().zip(&places) {
                    let one = Outcome::from_send(&sender.send_record(record).await);
                    outcomes[*i] = outcomes[*i].max(one);
                }
            } else {
                let outcome = Outcome::from_send(&result);
                for i in places {
                    outcomes[i] = outcomes[i].max(outcome);
                }
            }
        } else {
            merge_results(&mut outcomes, join_all(sends).await);
        }
        answer(outcomes, batch.answer)
    }
}
