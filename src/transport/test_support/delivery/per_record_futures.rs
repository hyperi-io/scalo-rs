// Project:   scalo
// File:      src/transport/test_support/delivery/per_record_futures.rs
// Purpose:   Baseline approach: one bounded future per record, destinations in turn
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`PerRecordFutures`]: one future per record, at most
//! [`in_flight`](PerRecordFutures::in_flight) at once for each caller batch.
//! Each sends its record to every destination it is routed to in turn with
//! [`send`](TransportSender::send), awaiting each: a `Backpressured` or `Fatal`
//! stops that record, and a `FilteredDlq` marks it dead-lettered and goes on.
//!
//! The approach does not send a refused record again. Retry is the caller's
//! policy, applied the same way to every approach.

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;

use super::{CallerBatch, DeliveryApproach, Destinations, Outcome, RoutedRecord, answer};
use crate::transport::{SendResult, TransportSender};

/// One future per record, bounded per caller batch.
#[derive(Debug, Clone, Copy)]
pub struct PerRecordFutures {
    /// Record futures in flight at once for one caller batch.
    pub in_flight: usize,
}

impl PerRecordFutures {
    /// Registry name.
    pub const NAME: &'static str = "per_record_futures";
}

impl Default for PerRecordFutures {
    fn default() -> Self {
        Self { in_flight: 1000 }
    }
}

impl DeliveryApproach for PerRecordFutures {
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
        let mut running = FuturesUnordered::new();
        let mut next = batch.records.iter().enumerate();
        loop {
            while running.len() < self.in_flight.max(1)
                && let Some((i, routed)) = next.next()
            {
                running.push(async move { (i, fan_out(destinations, routed).await) });
            }
            let Some((i, outcome)) = running.next().await else {
                break;
            };
            outcomes[i] = outcome;
        }
        drop(running);
        answer(outcomes, batch.answer)
    }
}

/// Send one record to each of its routes in turn.
async fn fan_out<S: TransportSender>(
    destinations: &Destinations<S>,
    routed: &RoutedRecord,
) -> Outcome {
    let key = routed.record.key.as_deref().unwrap_or("");
    let mut outcome = Outcome::Sent;
    for id in routed.routes.iter() {
        let result = destinations
            .sender(*id)
            .send(key, routed.record.payload.clone())
            .await;
        match result {
            SendResult::Ok => {}
            SendResult::FilteredDlq => outcome = Outcome::DeadLettered,
            SendResult::Backpressured | SendResult::Fatal(_) => {
                return Outcome::from_send(&result);
            }
        }
    }
    outcome
}
