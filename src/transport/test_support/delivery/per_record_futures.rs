// Project:   scalo
// File:      src/transport/test_support/delivery/per_record_futures.rs
// Purpose:   Baseline approach: one bounded future per record, destinations in turn
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`PerRecordFutures`]: one future per record, at most
//! [`in_flight`](PerRecordFutures::in_flight) at once. Each sends its record to
//! every destination it is routed to in turn with
//! [`send`](TransportSender::send), awaiting each: a `Backpressured` or `Fatal`
//! stops that record, and a `FilteredDlq` marks it dead-lettered and goes on.
//!
//! The backpressured records are sent again, to every destination, after a
//! wait that starts at [`backoff`](PerRecordFutures::backoff) and doubles, up
//! to [`retries`](PerRecordFutures::retries) times. Records still refused after
//! that are answered backpressured.

use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;

use super::{CallerBatch, DeliveryApproach, Destinations, Outcome, RoutedRecord, answer};
use crate::transport::{SendResult, TransportSender};

/// One future per record, bounded, with a bounded resend of the backpressured.
#[derive(Debug, Clone, Copy)]
pub struct PerRecordFutures {
    /// Record futures in flight at once.
    pub in_flight: usize,
    /// Times the backpressured records are sent again.
    pub retries: u32,
    /// Wait before the first resend. Each later wait doubles.
    pub backoff: Duration,
}

impl PerRecordFutures {
    /// Registry name.
    pub const NAME: &'static str = "per_record_futures";
}

impl Default for PerRecordFutures {
    fn default() -> Self {
        Self {
            in_flight: 1000,
            retries: 5,
            backoff: Duration::from_millis(200),
        }
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
        let mut pending: Vec<usize> = (0..batch.records.len()).collect();
        let mut backoff = self.backoff;

        for attempt in 0..=self.retries {
            if pending.is_empty() {
                break;
            }
            if attempt > 0 {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            let mut held = Vec::new();
            let mut running = FuturesUnordered::new();
            let mut next = pending.into_iter();
            loop {
                while running.len() < self.in_flight.max(1)
                    && let Some(i) = next.next()
                {
                    let routed = &batch.records[i];
                    running.push(async move { (i, fan_out(destinations, routed).await) });
                }
                let Some((i, outcome)) = running.next().await else {
                    break;
                };
                if outcome == Outcome::Backpressured {
                    held.push(i);
                } else {
                    outcomes[i] = outcome;
                }
            }
            pending = held;
        }
        for i in pending {
            outcomes[i] = Outcome::Backpressured;
        }
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
