// Project:   scalo
// File:      src/tiered_sink/drainer.rs
// Purpose:   Background drain task for spooled messages
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Background drain task for spooled messages.

use crate::tiered_sink::{CircuitBreaker, DrainStrategy};
use crate::transport::{Record, SendResult, TransportSender};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use yaque::Receiver;

/// Drainer state and rate tracking.
pub struct Drainer {
    strategy: DrainStrategy,
    current_rate: f64, // messages per second
    success_count: u64,
    failure_count: u64,
    last_adjustment: Instant,
}

impl Drainer {
    /// Create a new drainer with the given strategy.
    pub fn new(strategy: DrainStrategy) -> Self {
        let initial_rate = match strategy {
            DrainStrategy::Adaptive { initial_rate, .. } => initial_rate as f64,
            DrainStrategy::RateLimited { msgs_per_sec } => msgs_per_sec as f64,
            DrainStrategy::Greedy => f64::MAX,
        };

        Self {
            strategy,
            current_rate: initial_rate,
            success_count: 0,
            failure_count: 0,
            last_adjustment: Instant::now(),
        }
    }

    /// Record a successful drain.
    pub fn record_success(&mut self) {
        self.success_count += 1;
        self.maybe_adjust_rate();
    }

    /// Record a failed drain attempt.
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.maybe_adjust_rate();
    }

    /// Get the current delay between drain operations.
    pub fn delay(&self) -> Duration {
        if self.current_rate >= f64::MAX || self.current_rate <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / self.current_rate)
        }
    }

    /// Adjust rate based on success/failure ratio.
    fn maybe_adjust_rate(&mut self) {
        let DrainStrategy::Adaptive {
            initial_rate,
            max_rate,
        } = self.strategy
        else {
            return; // Only adjust for adaptive strategy
        };

        // Adjust every 100 operations or 1 second, whichever comes first
        let total = self.success_count + self.failure_count;
        let elapsed = self.last_adjustment.elapsed();

        if total < 100 && elapsed < Duration::from_secs(1) {
            return;
        }

        let success_ratio = if total > 0 {
            self.success_count as f64 / total as f64
        } else {
            1.0
        };

        // Adjust rate based on success ratio
        self.current_rate = if success_ratio > 0.95 {
            // Very successful, increase rate
            (self.current_rate * 1.5).min(max_rate as f64)
        } else if success_ratio > 0.8 {
            // Mostly successful, small increase
            (self.current_rate * 1.1).min(max_rate as f64)
        } else if success_ratio > 0.5 {
            // Mixed results, maintain rate
            self.current_rate
        } else {
            // Failing, reduce rate
            (self.current_rate * 0.5).max(initial_rate as f64 / 10.0)
        };

        // Reset counters
        self.success_count = 0;
        self.failure_count = 0;
        self.last_adjustment = Instant::now();
    }

    /// Get current rate for metrics.
    pub fn current_rate(&self) -> f64 {
        self.current_rate
    }
}

/// Result of a drain attempt.
enum DrainResult {
    /// This many records sent and committed
    Success(usize),
    /// The sink refused a batch of this many entries as `Fatal` for a record
    /// it did not name. The batch is still spooled, and the next passes send
    /// its entries one at a time.
    Isolate(usize),
    /// Sink is unavailable / backpressured
    SinkUnavailable,
    /// The send outlived `send_timeout`. The entry is still on disk,
    /// uncommitted, and is offered again on the next pass.
    SendTimedOut,
    /// Fatal error sending
    Fatal(String),
    /// Decompression error
    DecompressError(String),
    /// The spooled bytes could not be decoded back into a record (corrupt /
    /// incompatible frame). Dropped (committed) so it cannot wedge the queue.
    DecodeError(String),
    /// Queue is empty
    Empty,
    /// I/O error
    IoError,
    /// `guard.commit()` failed after the sink/codec returned a result
    /// that would normally consume the entry. The entry is STILL on
    /// disk; counters were NOT decremented. StrictFifo must stay
    /// gated until the next drain iteration retries.
    CommitFailed(String),
}

/// Feed one drain outcome back into the rate controller and the breaker.
async fn settle(
    result: DrainResult,
    drainer: &mut Drainer,
    circuit: &CircuitBreaker,
    send_timeout: Duration,
) {
    match result {
        DrainResult::Success(records) => {
            for _ in 0..records {
                drainer.record_success();
            }
            circuit.record_success().await;
            #[cfg(feature = "tracing")]
            tracing::debug!(
                records,
                rate = drainer.current_rate(),
                "Drained records to sink"
            );
        }
        DrainResult::Isolate(entries) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                entries,
                "Sink refused a drained batch; sending its entries one at a time"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = entries;
        }
        DrainResult::SinkUnavailable => {
            drainer.record_failure();
            circuit.record_failure().await;
            #[cfg(feature = "tracing")]
            tracing::debug!("Sink unavailable during drain, circuit may open");
        }
        DrainResult::SendTimedOut => {
            // Counts toward the breaker as the hot path's timeout does.
            drainer.record_failure();
            circuit.record_failure().await;
            #[cfg(feature = "tracing")]
            tracing::warn!(
                send_timeout = ?send_timeout,
                "Sink send timed out during drain; record stays spooled, will retry"
            );
        }
        DrainResult::Fatal(e) => {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "Fatal error during drain, dropping message");
        }
        DrainResult::DecompressError(e) => {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "Failed to decompress spooled message, dropping");
        }
        DrainResult::DecodeError(e) => {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "Failed to decode spooled record, dropping");
        }
        DrainResult::Empty | DrainResult::IoError => {
            // Nothing to do, just continue
        }
        DrainResult::CommitFailed(e) => {
            // Counters intact; entry still on disk. Treat as
            // failure so StrictFifo stays gated until retry.
            drainer.record_failure();
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "yaque commit failed; counters preserved, will retry");
        }
    }
}

/// Take one entry of `bytes` off the spool counters, stopping at
/// zero: a count already short of what is on disk must not wrap to
/// `u64::MAX`, which keeps StrictFifo's hot path shut and refuses every spill.
pub(crate) fn release(count: &AtomicU64, total: &AtomicU64, bytes: u64) {
    release_many(count, total, 1, bytes);
}

/// [`release`] for `entries` committed together, holding `bytes` between them.
fn release_many(count: &AtomicU64, total: &AtomicU64, entries: u64, bytes: u64) {
    let _ = count.try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
        Some(n.saturating_sub(entries))
    });
    let _ = total.try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
        Some(n.saturating_sub(bytes))
    });
}

/// How many spooled entries one drain pass takes and sends together.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainBatch {
    /// Most entries a pass takes.
    pub(crate) max_records: usize,
    /// Most spooled (stored) bytes a pass takes. A first entry over it is
    /// still taken on its own.
    pub(crate) max_bytes: u64,
}

/// How many leading entries of `sizes` fit `max_bytes`, never fewer than one.
fn fitting(sizes: &[u64], max_bytes: u64) -> usize {
    let mut total = 0_u64;
    let fit = sizes
        .iter()
        .take_while(|&&size| {
            total = total.saturating_add(size);
            total <= max_bytes
        })
        .count();
    fit.max(1)
}

/// One spooled entry, decoded for the sink, or why it could not be.
enum Entry {
    Record(Record),
    Unreadable(DrainResult),
}

/// Unframe, decompress and decode one spooled entry.
fn decode_entry(raw: Vec<u8>, crc: bool, codec: crate::tiered_sink::CompressionCodec) -> Entry {
    // Verify+strip the CRC header (when enabled), then decompress. Either
    // failing means the spilled bytes are unreadable, so the entry is dropped,
    // never replayed downstream.
    let data = match crate::spool_codec::unframe(crc, raw)
        .map_err(|c| c.0)
        .and_then(|c| codec.decompress(&c).map_err(|e| e.to_string()))
    {
        Ok(data) => data,
        Err(e) => return Entry::Unreadable(DrainResult::DecompressError(e)),
    };
    match Record::decode(&data) {
        Ok(record) => Entry::Record(record),
        Err(e) => Entry::Unreadable(DrainResult::DecodeError(e.to_string())),
    }
}

/// Take up to `cap` entries within `batch.max_bytes`, send the readable ones in
/// one `send_batch` under `send_timeout`, and commit every entry only when the
/// sink takes them. Any other answer rolls the whole batch back.
#[allow(clippy::too_many_arguments)]
async fn drain_once<S: TransportSender>(
    sink: &S,
    receiver: &mut Receiver,
    cap: usize,
    batch: DrainBatch,
    codec: crate::tiered_sink::CompressionCodec,
    crc: bool,
    send_timeout: Duration,
    spool_count: &AtomicU64,
    spool_bytes: &AtomicU64,
) -> Vec<DrainResult> {
    let first = match receiver.try_recv_batch_up_to(cap) {
        Ok(guard) => guard,
        Err(yaque::TryRecvError::QueueEmpty) => return vec![DrainResult::Empty],
        Err(yaque::TryRecvError::Io(e)) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %e, "I/O error reading from spool");
            #[cfg(not(feature = "tracing"))]
            let _ = e;
            return vec![DrainResult::IoError];
        }
    };
    let sizes: Vec<u64> = first.iter().map(|e| e.len() as u64).collect();
    let take = fitting(&sizes, batch.max_bytes);
    let stored: u64 = sizes.iter().take(take).sum();
    // yaque commits a guard whole, so a batch over the byte cap is rolled
    // back (on drop) and only what fits is taken again.
    let guard = if take == first.len() {
        first
    } else {
        drop(first);
        match receiver.try_recv_batch_up_to(take) {
            Ok(guard) => guard,
            Err(yaque::TryRecvError::QueueEmpty) => return vec![DrainResult::Empty],
            Err(yaque::TryRecvError::Io(_)) => return vec![DrainResult::IoError],
        }
    };
    let taken = guard.len();
    let mut records = Vec::with_capacity(taken);
    let mut unreadable = Vec::new();
    for raw in guard.iter() {
        match decode_entry(raw.clone(), crc, codec) {
            Entry::Record(record) => records.push(record),
            Entry::Unreadable(why) => unreadable.push(why),
        }
    }

    let sent = if records.is_empty() {
        None
    } else {
        Some(timeout(send_timeout, sink.send_batch(&records)).await)
    };
    let delivered = match sent {
        // Only unreadable entries: nothing to send, drop them.
        None => 0,
        Some(Ok(SendResult::Ok | SendResult::FilteredDlq)) => records.len(),
        // Backpressure, a timeout (the batch may or may not have reached the
        // sink) and a closed or misconfigured sink keep the whole batch spooled
        // (at-least-once).
        Some(Ok(SendResult::Backpressured)) => return vec![DrainResult::SinkUnavailable],
        Some(Err(_elapsed)) => return vec![DrainResult::SendTimedOut],
        Some(Ok(SendResult::Fatal(e))) if e.is_fatal() => {
            return vec![DrainResult::SinkUnavailable];
        }
        // A refusal of one record in a batch cannot say which: keep them all
        // and send them one at a time until the refused one is past.
        Some(Ok(SendResult::Fatal(_))) if taken > 1 => {
            return vec![DrainResult::Isolate(taken)];
        }
        // On its own, the refused record is its own fault: drop it.
        Some(Ok(SendResult::Fatal(e))) => {
            unreadable.push(DrainResult::Fatal(e.to_string()));
            0
        }
    };
    match guard.commit() {
        Ok(()) => {
            release_many(spool_count, spool_bytes, taken as u64, stored);
            let mut results = unreadable;
            if delivered > 0 {
                results.push(DrainResult::Success(delivered));
            }
            results
        }
        Err(e) => vec![DrainResult::CommitFailed(e.to_string())],
    }
}

/// Drain loop. Runs until shutdown signalled.
///
/// Each pass takes up to `batch` entries, at most one second's worth at the
/// strategy's current rate, and sends them in one `send_batch`.
///
/// `send_timeout`: bounds each downstream send, the hot path's limit, so a hung
/// sink cannot pin the spool lock, the StrictFifo gate or `shutdown()`.
///
/// `fifo_gate`: `Some` in StrictFifo, acquired per delivery cycle
/// to serialise with senders. `None` in Interleaved (lock-free).
#[allow(clippy::too_many_arguments)]
pub async fn drain_loop<S: TransportSender + 'static>(
    sink: Arc<S>,
    spool_receiver: Arc<Mutex<Receiver>>,
    spool_count: Arc<AtomicU64>,
    spool_bytes: Arc<AtomicU64>,
    circuit: Arc<CircuitBreaker>,
    codec: crate::tiered_sink::CompressionCodec,
    crc: bool,
    strategy: DrainStrategy,
    interval: Duration,
    send_timeout: Duration,
    batch: DrainBatch,
    shutdown: CancellationToken,
    fifo_gate: Option<Arc<Mutex<()>>>,
) {
    let mut drainer = Drainer::new(strategy);
    // Entries still to send one at a time after a refused batch.
    let mut isolate = 0_usize;

    #[cfg(feature = "shutdown")]
    let global_shutdown = crate::shutdown::token();

    // `interval` paces an idle or failing spool. After a pass that took an
    // entry off the queue the next starts at once, so a backlog drains at the
    // strategy's rate rather than one record per interval.
    let mut pause = interval;

    loop {
        // Check for shutdown (the sink's own token or the global shutdown token)
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::info!("Drain task shutting down (sink shutdown)");
                return;
            }
            () = async {
                #[cfg(feature = "shutdown")]
                global_shutdown.cancelled().await;
                #[cfg(not(feature = "shutdown"))]
                std::future::pending::<()>().await;
            } => {
                #[cfg(feature = "tracing")]
                tracing::info!("Drain task shutting down (global shutdown)");
                return;
            }
            () = tokio::time::sleep(pause) => {}
        }
        pause = interval;

        // Gate the drain through the breaker. When half-open this claims the
        // single recovery-probe permit, so the drainer alone probes the
        // recovering sink rather than racing the hot path onto it.
        if !circuit.allow_request().await {
            continue;
        }

        // StrictFifo: hold the gate across dequeue + send + commit.
        let _gate = match &fifo_gate {
            Some(gate) => Some(gate.lock().await),
            None => None,
        };

        // A second's worth at the current rate, so a rate-limited drain stays smooth.
        let cap = if isolate > 0 {
            isolate -= 1;
            1
        } else {
            per_pass(batch.max_records, drainer.current_rate())
        };
        // RecvGuard borrows the Receiver, so take, send and commit under its lock.
        let results = {
            let mut receiver = spool_receiver.lock().await;
            drain_once(
                sink.as_ref(),
                &mut receiver,
                cap,
                batch,
                codec,
                crc,
                send_timeout,
                &spool_count,
                &spool_bytes,
            )
            .await
        };

        let mut took = 0_usize;
        for result in results {
            match result {
                DrainResult::Success(records) => {
                    took += records;
                    pause = Duration::ZERO;
                }
                DrainResult::Fatal(_)
                | DrainResult::DecompressError(_)
                | DrainResult::DecodeError(_) => pause = Duration::ZERO,
                DrainResult::Isolate(entries) => {
                    isolate = entries;
                    pause = Duration::ZERO;
                }
                _ => {}
            }
            settle(result, &mut drainer, &circuit, send_timeout).await;
        }

        // The strategy's delay is per record, so a batch waits for each one.
        pause += drainer
            .delay()
            .saturating_mul(u32::try_from(took.max(1)).unwrap_or(u32::MAX));
    }
}

/// Records one pass takes: `max_records`, held to a second's worth at `rate`
/// records per second, never fewer than one.
fn per_pass(max_records: usize, rate: f64) -> usize {
    if rate >= max_records as f64 {
        return max_records.max(1);
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let at_rate = rate.ceil() as usize;
    at_rate.clamp(1, max_records.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drainer_greedy_no_delay() {
        let drainer = Drainer::new(DrainStrategy::Greedy);
        assert_eq!(drainer.delay(), Duration::ZERO);
    }

    #[test]
    fn test_drainer_rate_limited() {
        let drainer = Drainer::new(DrainStrategy::RateLimited { msgs_per_sec: 100 });
        assert_eq!(drainer.delay(), Duration::from_millis(10));
    }

    #[test]
    fn test_drainer_adaptive_initial() {
        let drainer = Drainer::new(DrainStrategy::adaptive(100, 1000));
        assert_eq!(drainer.delay(), Duration::from_millis(10));
    }

    #[test]
    fn test_drainer_rate_adjustment() {
        let mut drainer = Drainer::new(DrainStrategy::adaptive(100, 10000));

        // Simulate 100 successes
        for _ in 0..100 {
            drainer.record_success();
        }

        // Rate should have increased
        assert!(drainer.current_rate() > 100.0);
    }
}
