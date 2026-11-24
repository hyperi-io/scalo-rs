// Project:   scalo
// File:      src/tiered_sink/drainer.rs
// Purpose:   Background drain task for spooled messages
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Background drain task for spooled messages.

use crate::compression::Compressor;
use crate::tiered_sink::{CircuitBreaker, DrainStrategy};
use crate::transport::{Record, Screened, SendResult, TransportSender};
use crate::yaque::{self, Receiver};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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
    /// Sink is unavailable, backpressured, or did not answer by `send_timeout`.
    /// The entry is still on disk, uncommitted, and is offered again on the
    /// next pass.
    SinkUnavailable,
    /// The DLQ did not confirm the batch's dead letters. The batch is still
    /// spooled, and is offered again on the next pass.
    DeadLetterUnwritten,
    /// This many records the sink would not take were written to the DLQ, or
    /// dropped and counted when there is none, and committed.
    DeadLettered(usize),
    /// An entry failed its CRC: its stored bytes are damaged. Dropped
    /// (committed) so it cannot block the queue.
    Corrupt(String),
    /// The entry at the head of the spool does not decompress or decode under
    /// the spill codec, as when it was written under another. Nothing is
    /// committed, and the drain holds until the codec that wrote it is back.
    Held(String),
    /// Queue is empty
    Empty,
    /// I/O error
    IoError,
    /// `guard.commit()` failed after the sink/codec returned a result
    /// that would normally consume the entry. The entry is STILL on
    /// disk. Counters were NOT decremented. StrictFifo must stay
    /// gated until the next drain iteration retries.
    CommitFailed(String),
}

/// Feed one drain outcome back into the rate controller and the breaker.
async fn settle(result: DrainResult, drainer: &mut Drainer, circuit: &CircuitBreaker) {
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
        DrainResult::DeadLettered(records) => {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                records,
                "Drained records the sink would not take as dead letters"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = records;
        }
        DrainResult::DeadLetterUnwritten => {
            drainer.record_failure();
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "The DLQ did not confirm a drained batch's dead letters; it stays spooled"
            );
        }
        DrainResult::Corrupt(e) => {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "Spooled entry failed its CRC, dropping");
            #[cfg(not(feature = "tracing"))]
            let _ = e;
        }
        DrainResult::Held(_) => {
            // The loop logs the hold once, and the entry stays spooled.
            drainer.record_failure();
        }
        DrainResult::Empty | DrainResult::IoError => {
            // Nothing to do, just continue
        }
        DrainResult::CommitFailed(e) => {
            // Counters intact. Entry still on disk. Treat as
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

/// The `service` field of the dead letters the tiered sink writes.
#[cfg(feature = "dlq")]
const DLQ_SERVICE: &str = "tiered_sink";

/// A record the sink will never take, and why.
pub(crate) struct Undeliverable {
    pub(crate) record: Record,
    /// The `reason` label it counts under.
    pub(crate) label: &'static str,
    /// The DLQ entry's reason.
    pub(crate) reason: String,
}

/// What [`DeadLetters::write`] does with an entry the DLQ answers
/// `DlqError::Refused` for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnRefusal {
    /// Drop and count it: the sink will never take it, and no DLQ backend can.
    Drop,
    /// Leave the write unconfirmed, so the caller keeps a record the sink can still take.
    #[cfg_attr(
        not(feature = "dlq"),
        allow(dead_code, reason = "only `when_full: dlq` keeps a refused record")
    )]
    Keep,
}

/// Where the tiered sink writes the records it cannot deliver: the DLQ set
/// with `TieredSink::with_dlq`, or nowhere, dropping and counting them.
#[derive(Clone, Default)]
pub(crate) struct DeadLetters {
    #[cfg(feature = "dlq")]
    dlq: Arc<parking_lot::RwLock<Option<Arc<crate::dlq::Dlq>>>>,
}

impl DeadLetters {
    #[cfg(feature = "dlq")]
    pub(crate) fn set(&self, dlq: Arc<crate::dlq::Dlq>) {
        *self.dlq.write() = Some(dlq);
    }

    #[cfg(feature = "dlq")]
    pub(crate) fn dlq(&self) -> Option<Arc<crate::dlq::Dlq>> {
        self.dlq.read().clone()
    }

    /// Whether writing `dead` would store it: a DLQ is set, enabled, and has a
    /// backend that can hold it. [`write`](Self::write) drops it otherwise.
    #[cfg(feature = "dlq")]
    pub(crate) fn holds(&self, dead: &Undeliverable) -> bool {
        self.dlq()
            .is_some_and(|dlq| dlq.is_enabled() && dlq.refusal(&dlq_entry(&dlq, dead)).is_none())
    }

    /// Write `dead` to the DLQ and wait up to `wait` for it to confirm, or
    /// drop and count them when there is no DLQ. `false` when the DLQ did not
    /// confirm, so the caller keeps the records. A `DlqError::Refused` answer
    /// confirms the write under [`OnRefusal::Drop`], dropping and counting the
    /// entries it names, and leaves it unconfirmed under [`OnRefusal::Keep`].
    pub(crate) async fn write(
        &self,
        dead: Vec<Undeliverable>,
        wait: Duration,
        on_refusal: OnRefusal,
    ) -> bool {
        #[cfg(feature = "dlq")]
        if let Some(dlq) = self.dlq() {
            return write_to(&dlq, dead, wait, on_refusal).await;
        }
        #[cfg(not(feature = "dlq"))]
        let _ = (wait, on_refusal);
        for Undeliverable {
            record,
            label,
            reason,
        } in dead
        {
            count_dead_letter(label, "dropped");
            #[cfg(feature = "tracing")]
            tracing::error!(
                destination = record.key.as_deref().unwrap_or(""),
                reason = %reason,
                "The tiered sink has no DLQ: a record its sink will not take is dropped"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = (record, reason);
        }
        true
    }
}

/// `dead` as `dlq` writes it, capped.
#[cfg(feature = "dlq")]
fn dlq_entry(dlq: &crate::dlq::Dlq, dead: &Undeliverable) -> crate::dlq::DlqEntry {
    let entry = crate::dlq::DlqEntry::new(
        DLQ_SERVICE,
        dead.reason.clone(),
        dead.record.payload.to_vec(),
    );
    dlq.cap(match &dead.record.key {
        Some(key) => entry.with_destination(key.as_ref()),
        None => entry,
    })
}

/// The DLQ half of [`DeadLetters::write`].
#[cfg(feature = "dlq")]
async fn write_to(
    dlq: &crate::dlq::Dlq,
    dead: Vec<Undeliverable>,
    wait: Duration,
    on_refusal: OnRefusal,
) -> bool {
    let mut labels = Vec::with_capacity(dead.len());
    let mut entries = Vec::with_capacity(dead.len());
    for d in dead {
        let entry = dlq_entry(dlq, &d);
        if let Some(refused) = dlq.refusal(&entry) {
            count_dead_letter(d.label, "dropped");
            #[cfg(feature = "tracing")]
            tracing::warn!(
                reason = refused.as_str(),
                "No DLQ backend can hold this dead letter; it is dropped"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = refused;
            continue;
        }
        labels.push(d.label);
        entries.push(entry);
    }
    if entries.is_empty() {
        return true;
    }
    // `Refused` names entries by the bytes written, so keep those to match against.
    let written = match on_refusal {
        OnRefusal::Drop => entries.clone(),
        OnRefusal::Keep => Vec::new(),
    };
    match tokio::time::timeout(wait, dlq.write_confirmed(entries)).await {
        Ok(Ok(())) => {
            for label in labels {
                count_dead_letter(label, "dlq");
            }
            true
        }
        Ok(Err(crate::dlq::DlqError::Refused(refused))) if on_refusal == OnRefusal::Drop => {
            let mut dropped = 0_usize;
            for (label, entry) in labels.into_iter().zip(&written) {
                if refused.contains(entry) {
                    count_dead_letter(label, "dropped");
                    dropped += 1;
                } else {
                    count_dead_letter(label, "dlq");
                }
            }
            #[cfg(feature = "tracing")]
            tracing::warn!(
                dropped,
                "The DLQ's broker refused a dead letter for its size; it is dropped"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = dropped;
            true
        }
        Ok(Err(e)) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %e, "The DLQ refused the tiered sink's dead letters");
            #[cfg(not(feature = "tracing"))]
            let _ = e;
            false
        }
        Err(_elapsed) => false,
    }
}

/// Count one dead letter by its reason and where it went.
fn count_dead_letter(reason: &'static str, outcome: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(
        "tiered_sink_dead_letters_total",
        "reason" => reason,
        "outcome" => outcome
    )
    .increment(1);
    #[cfg(not(feature = "metrics"))]
    let _ = (reason, outcome);
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

/// Why a spooled entry did not decode.
enum Unreadable {
    /// It failed its CRC, so it is dropped.
    Corrupt(String),
    /// It does not decompress or decode under the spill codec, so it is kept.
    WrongCodec(String),
}

/// Unframe, decompress into `scratch` and decode one spooled entry.
fn decode_entry(
    raw: &[u8],
    crc: bool,
    decoder: &mut Compressor,
    scratch: &mut Vec<u8>,
) -> Result<Record, Unreadable> {
    let body =
        crate::spool_codec::unframe(crc, raw.to_vec()).map_err(|c| Unreadable::Corrupt(c.0))?;
    // The spill keeps no codec per entry, so a failure past the CRC is read as
    // an entry another codec wrote, never dropped.
    decoder
        .decompress_into(&body, scratch, usize::MAX)
        .map_err(|e| Unreadable::WrongCodec(e.to_string()))?;
    Record::decode(scratch).map_err(|e| Unreadable::WrongCodec(e.to_string()))
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
    decoder: &mut Compressor,
    scratch: &mut Vec<u8>,
    crc: bool,
    send_timeout: Duration,
    spool_count: &AtomicU64,
    spool_bytes: &AtomicU64,
    dead_letters: &DeadLetters,
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
    let mut take = fitting(&sizes, batch.max_bytes);
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
    let mut read_back = Vec::with_capacity(guard.len());
    let mut unreadable = Vec::new();
    let mut wrong_codec = None;
    for (index, raw) in guard.iter().enumerate() {
        match decode_entry(raw, crc, decoder, scratch) {
            Ok(record) => read_back.push(record),
            Err(Unreadable::Corrupt(e)) => unreadable.push(DrainResult::Corrupt(e)),
            Err(Unreadable::WrongCodec(e)) => {
                wrong_codec = Some((index, e));
                break;
            }
        }
    }
    // Only the entries before one the codec cannot read are taken: dropping
    // the guard rolls the batch back, so the rest stay spooled in order.
    let guard = match wrong_codec {
        None => guard,
        Some((0, error)) => return vec![DrainResult::Held(error)],
        Some((index, _)) => {
            drop(guard);
            take = index;
            match receiver.try_recv_batch_up_to(take) {
                Ok(guard) => guard,
                Err(yaque::TryRecvError::QueueEmpty) => return vec![DrainResult::Empty],
                Err(yaque::TryRecvError::Io(_)) => return vec![DrainResult::IoError],
            }
        }
    };
    let taken = guard.len();
    let stored: u64 = sizes.iter().take(taken).sum();
    let mut records = Vec::with_capacity(read_back.len());
    let mut dead = Vec::new();
    // Records an outbound drop filter takes are settled, and count as delivered.
    let mut dropped = 0;
    for record in read_back {
        match sink.screen(&record) {
            Screened::Send => records.push(record),
            Screened::Dropped => dropped += 1,
            Screened::DeadLetter(reason) => dead.push(Undeliverable {
                label: reason.as_str(),
                reason: reason.to_string(),
                record,
            }),
        }
    }

    let sent = if records.is_empty() {
        None
    } else {
        let deadline = std::time::Instant::now() + send_timeout;
        Some(sink.send_screened(&records, Some(deadline)).await)
    };
    let delivered = match sent {
        // Only unreadable entries, dropped records and dead letters: nothing to send.
        None => dropped,
        Some(SendResult::Ok) => records.len() + dropped,
        // Backpressure, a named wait, an elapsed deadline (the batch may or
        // may not have reached the sink) and a closed or misconfigured sink
        // keep the whole batch spooled (at-least-once).
        Some(SendResult::Backpressured | SendResult::RetryAfter(_)) => {
            return vec![DrainResult::SinkUnavailable];
        }
        Some(SendResult::Fatal(e)) if e.is_fatal() => {
            return vec![DrainResult::SinkUnavailable];
        }
        // A refusal or dead letter in a batch cannot say which record: keep
        // them all and send them one at a time until that one is past.
        Some(SendResult::Fatal(_) | SendResult::Reject(_) | SendResult::FilteredDlq)
            if records.len() > 1 =>
        {
            return vec![DrainResult::Isolate(taken)];
        }
        // On its own, the refused record is its own fault: dead-letter it.
        Some(SendResult::Fatal(e) | SendResult::Reject(e)) => {
            dead.extend(records.drain(..).map(|record| Undeliverable {
                label: crate::transport::DEAD_LETTER_REJECTED,
                reason: e.to_string(),
                record,
            }));
            0
        }
        Some(SendResult::FilteredDlq) => {
            dead.extend(records.drain(..).map(|record| Undeliverable {
                label: crate::transport::DEAD_LETTER_REJECTED,
                reason: crate::transport::DeadLetterReason::Rejected.to_string(),
                record,
            }));
            0
        }
    };
    let dead_lettered = dead.len();
    if dead_lettered > 0
        && !dead_letters
            .write(dead, send_timeout, OnRefusal::Drop)
            .await
    {
        return vec![DrainResult::DeadLetterUnwritten];
    }
    match guard.commit() {
        Ok(()) => {
            release_many(spool_count, spool_bytes, taken as u64, stored);
            let mut results = unreadable;
            if dead_lettered > 0 {
                results.push(DrainResult::DeadLettered(dead_lettered));
            }
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
    mut decoder: Compressor,
    held: Arc<AtomicBool>,
    crc: bool,
    strategy: DrainStrategy,
    interval: Duration,
    send_timeout: Duration,
    batch: DrainBatch,
    shutdown: CancellationToken,
    fifo_gate: Option<Arc<Mutex<()>>>,
    dead_letters: DeadLetters,
) {
    let mut drainer = Drainer::new(strategy);
    // Entries still to send one at a time after a refused batch.
    let mut isolate = 0_usize;
    // Decompressed entries land here, reused across passes.
    let mut scratch = Vec::new();

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
                &mut decoder,
                &mut scratch,
                crc,
                send_timeout,
                &spool_count,
                &spool_bytes,
                &dead_letters,
            )
            .await
        };

        let mut took = 0_usize;
        for result in results {
            note_hold(&result, &held, decoder.compression());
            match result {
                DrainResult::Success(records) => {
                    took += records;
                    pause = Duration::ZERO;
                }
                DrainResult::DeadLettered(_) | DrainResult::Corrupt(_) => pause = Duration::ZERO,
                DrainResult::Isolate(entries) => {
                    isolate = entries;
                    pause = Duration::ZERO;
                }
                _ => {}
            }
            settle(result, &mut drainer, &circuit).await;
        }

        // The strategy's delay is per record, so a batch waits for each one.
        pause += drainer
            .delay()
            .saturating_mul(u32::try_from(took.max(1)).unwrap_or(u32::MAX));
    }
}

/// Set or clear `held` from a pass's result, logging and counting only the
/// change, so a hold is reported once however many passes it lasts.
fn note_hold(result: &DrainResult, held: &AtomicBool, codec: crate::compression::Compression) {
    let holding = match result {
        DrainResult::Held(_) => true,
        DrainResult::Empty | DrainResult::IoError | DrainResult::SinkUnavailable => return,
        _ => false,
    };
    if held.swap(holding, AtomicOrdering::Relaxed) == holding {
        return;
    }
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("tiered_sink_drain_held").set(if holding { 1.0 } else { 0.0 });
    #[cfg(feature = "tracing")]
    if let DrainResult::Held(error) = result {
        tracing::error!(
            codec = %codec,
            error = %error,
            "The spill's next entry does not read under its codec, so the drain holds and keeps it. Reopen the sink under the codec that wrote it"
        );
    } else {
        tracing::info!(codec = %codec, "The spill reads under its codec again, so the drain resumes");
    }
    #[cfg(not(feature = "tracing"))]
    let _ = codec;
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
