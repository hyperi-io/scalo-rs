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
    /// Successfully sent and committed
    Success,
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
        DrainResult::Success => {
            drainer.record_success();
            circuit.record_success().await;
            #[cfg(feature = "tracing")]
            tracing::debug!(rate = drainer.current_rate(), "Drained message to sink");
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
    let _ = count.try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
        Some(n.saturating_sub(1))
    });
    let _ = total.try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
        Some(n.saturating_sub(bytes))
    });
}

/// Drain loop. Runs until shutdown signalled.
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
    shutdown: CancellationToken,
    fifo_gate: Option<Arc<Mutex<()>>>,
) {
    let mut drainer = Drainer::new(strategy);

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

        // Try to receive, decompress, send, and commit all within the lock
        // This is necessary because RecvGuard borrows the Receiver
        let result: DrainResult = {
            let mut receiver = spool_receiver.lock().await;

            let recv_result = receiver.try_recv();
            match recv_result {
                Ok(guard) => {
                    let raw = guard.to_vec();
                    let compressed_len = raw.len() as u64;

                    // Verify+strip the CRC header (when enabled), then decompress.
                    // Either failing means the spilled bytes are unreadable -- drop
                    // (commit) the record below, never replay garbage downstream.
                    let decompress_result: std::result::Result<Vec<u8>, String> =
                        crate::spool_codec::unframe(crc, raw)
                            .map_err(|c| c.0)
                            .and_then(|c| codec.decompress(&c).map_err(|e| e.to_string()));
                    match decompress_result {
                        Ok(data) => {
                            // Decode the spooled record and replay it to the
                            // downstream. A record that no longer decodes cannot
                            // be replayed -- drop it (commit) like a decode error,
                            // never block the queue forever on a poison entry.
                            match Record::decode(&data) {
                                Ok(record) => {
                                    match timeout(
                                        send_timeout,
                                        sink.send_batch(std::slice::from_ref(&record)),
                                    )
                                    .await
                                    {
                                        Ok(SendResult::Ok | SendResult::FilteredDlq) => {
                                            // Commit FIRST; only decrement on commit
                                            // success so counters never drift while
                                            // the entry still sits in the queue.
                                            match guard.commit() {
                                                Ok(()) => {
                                                    release(
                                                        &spool_count,
                                                        &spool_bytes,
                                                        compressed_len,
                                                    );
                                                    DrainResult::Success
                                                }
                                                Err(e) => DrainResult::CommitFailed(e.to_string()),
                                            }
                                        }
                                        Ok(SendResult::Backpressured) => {
                                            // Don't commit - guard rolls back, retry later.
                                            drop(guard);
                                            DrainResult::SinkUnavailable
                                        }
                                        Err(_elapsed) => {
                                            // The send future is dropped; the entry
                                            // may or may not have reached the sink, so
                                            // roll back and retry (at-least-once).
                                            drop(guard);
                                            DrainResult::SendTimedOut
                                        }
                                        Ok(SendResult::Fatal(e)) if e.is_fatal() => {
                                            // The sink is closed or misconfigured (a
                                            // refused credential, say), which says
                                            // nothing about this record. Keep it
                                            // spooled until the sink is fixed.
                                            drop(guard);
                                            DrainResult::SinkUnavailable
                                        }
                                        Ok(SendResult::Fatal(e)) => match guard.commit() {
                                            Ok(()) => {
                                                release(&spool_count, &spool_bytes, compressed_len);
                                                DrainResult::Fatal(e.to_string())
                                            }
                                            Err(commit_err) => {
                                                DrainResult::CommitFailed(commit_err.to_string())
                                            }
                                        },
                                    }
                                }
                                Err(decode_err) => match guard.commit() {
                                    Ok(()) => {
                                        release(&spool_count, &spool_bytes, compressed_len);
                                        DrainResult::DecodeError(decode_err.to_string())
                                    }
                                    Err(commit_err) => {
                                        DrainResult::CommitFailed(commit_err.to_string())
                                    }
                                },
                            }
                        }
                        Err(e) => match guard.commit() {
                            Ok(()) => {
                                release(&spool_count, &spool_bytes, compressed_len);
                                DrainResult::DecompressError(e)
                            }
                            Err(commit_err) => DrainResult::CommitFailed(commit_err.to_string()),
                        },
                    }
                }
                Err(yaque::TryRecvError::QueueEmpty) => DrainResult::Empty,
                Err(yaque::TryRecvError::Io(e)) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(error = %e, "I/O error reading from spool");
                    DrainResult::IoError
                }
            }
        };

        if matches!(
            result,
            DrainResult::Success
                | DrainResult::Fatal(_)
                | DrainResult::DecompressError(_)
                | DrainResult::DecodeError(_)
        ) {
            pause = Duration::ZERO;
        }

        // Handle the result outside the lock
        settle(result, &mut drainer, &circuit, send_timeout).await;

        // Rate limiting rides the shutdown-aware pause at the top of the loop.
        pause += drainer.delay();
    }
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
