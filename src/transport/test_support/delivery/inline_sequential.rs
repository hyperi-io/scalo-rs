// Project:   scalo
// File:      src/transport/test_support/delivery/inline_sequential.rs
// Purpose:   Baseline approach: per caller batch, one send_batch per destination in turn
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`InlineSequential`]: the batch is delivered inside the caller's task, one
//! destination after another in id order. Each destination gets ONE
//! [`send_batch`](TransportSender::send_batch) holding the records routed to
//! it: a Kafka-shaped sender enqueues each record and then awaits every
//! confirmation, a gRPC-shaped one sends them as one call.
//!
//! Records a destination names in
//! [`dead_letter_reason`](TransportSender::dead_letter_reason) are taken out
//! first and answered dead-lettered. The first destination that does not take
//! its block stops the rest, as `RoutedSender::send_batch_fanout` does, and
//! every record routed to a destination not reached gets that outcome too.
//! With [`stop_at_failure`](InlineSequential::stop_at_failure) off, every
//! destination is sent whatever the ones before it answered, the policy the
//! fan-out, merge and per-destination approaches follow.

use super::{CallerBatch, DeliveryApproach, Destinations, Outcome, answer};
use crate::transport::{Record, SendResult, TransportSender};

/// Per caller batch, inline, destinations one after another.
#[derive(Debug, Clone, Copy)]
pub struct InlineSequential {
    /// Stop at the first destination that does not take its block.
    pub stop_at_failure: bool,
}

impl Default for InlineSequential {
    fn default() -> Self {
        Self {
            stop_at_failure: true,
        }
    }
}

impl InlineSequential {
    /// Registry name, stopping at the first failure.
    pub const NAME: &'static str = "inline_sequential";
    /// Registry name of the same approach sending every destination.
    pub const CONTINUE: &'static str = "inline_sequential_continue";
}

impl DeliveryApproach for InlineSequential {
    fn name(&self) -> &'static str {
        if self.stop_at_failure {
            Self::NAME
        } else {
            Self::CONTINUE
        }
    }

    async fn deliver<'a, S: TransportSender>(
        &'a self,
        destinations: &'a Destinations<S>,
        batch: CallerBatch,
    ) -> Vec<Outcome> {
        let _lease = destinations.ledger().lease(batch.payload_bytes());
        let mut outcomes = vec![Outcome::Sent; batch.records.len()];
        let mut stopped: Option<Outcome> = None;

        for (id, _, sender) in destinations.iter() {
            let routed: Vec<usize> = batch
                .records
                .iter()
                .enumerate()
                .filter(|(_, r)| r.routes.contains(&id))
                .map(|(i, _)| i)
                .collect();
            if routed.is_empty() {
                continue;
            }
            if let Some(failed) = stopped {
                for i in routed {
                    outcomes[i] = outcomes[i].max(failed);
                }
                continue;
            }

            let mut block: Vec<Record> = Vec::with_capacity(routed.len());
            let mut in_block: Vec<usize> = Vec::with_capacity(routed.len());
            for i in routed {
                let record = &batch.records[i].record;
                if sender.dead_letter_reason(record).is_some() {
                    outcomes[i] = outcomes[i].max(Outcome::DeadLettered);
                } else {
                    block.push(record.clone());
                    in_block.push(i);
                }
            }
            if block.is_empty() {
                continue;
            }

            let result = sender.send_batch(&block).await;
            if matches!(result, SendResult::FilteredDlq) {
                // The block answer names no record, so each is sent again on its own.
                for (record, i) in block.iter().zip(&in_block) {
                    let one = Outcome::from_send(&sender.send_record(record).await);
                    outcomes[*i] = outcomes[*i].max(one);
                }
                continue;
            }
            let outcome = Outcome::from_send(&result);
            for i in in_block {
                outcomes[i] = outcomes[i].max(outcome);
            }
            if self.stop_at_failure && outcome.is_undelivered() {
                stopped = Some(outcome);
            }
        }
        answer(outcomes, batch.answer)
    }
}
