// Project:   scalo
// File:      src/tiered_sink/drainer.rs
// Purpose:   Background drain task for spooled messages
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Background drain task for spooled messages.

use crate::spool_codec::{Decoders, QueueCell, StoreMetrics, Unreadable, written_ms};
use crate::tiered_sink::acks::Acks;
use crate::tiered_sink::tiered::Spill;
use crate::tiered_sink::{CircuitBreaker, DrainStrategy};
use crate::transport::{DeliveryStatus, Record, SendResult, TransportSender};
use crate::yaque::{self, QueueState, Receiver, Taken};
use std::path::PathBuf;
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
    /// An entry failed its CRC: its stored bytes are damaged. Counted and
    /// dropped (committed) so it cannot block the queue.
    Corrupt,
    /// The entry at the head of the spool names a codec this build lacks, or
    /// does not decompress or decode under its own. Nothing is committed, and
    /// the drain holds until a build that reads it opens the spill.
    Held(String),
    /// A header failed its parity check, so no later entry can be found. The
    /// spill is quarantined and a fresh one opened.
    Damaged(String),
    /// Queue is empty
    Empty,
    /// The read failed. Nothing was taken, and the next pass tries again.
    IoError(String),
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
        DrainResult::Held(_) => {
            // The loop logs the hold once, and the entry stays spooled.
            drainer.record_failure();
        }
        DrainResult::Damaged(e) => {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "A spilled entry's header is damaged; quarantining the spill");
            #[cfg(not(feature = "tracing"))]
            let _ = e;
        }
        DrainResult::Corrupt | DrainResult::Empty | DrainResult::IoError(_) => {}
    }
}

/// Take `records` holding `bytes` off the spool counters, stopping at zero: a
/// count already short of what is on disk must not wrap to `u64::MAX`, which
/// keeps StrictFifo's hot path shut and refuses every spill.
pub(crate) fn release_many(count: &AtomicU64, total: &AtomicU64, records: u64, bytes: u64) {
    let _ = count.try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
        Some(n.saturating_sub(records))
    });
    let _ = total.try_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
        Some(n.saturating_sub(bytes))
    });
    refresh_gauges(count, total);
}

/// Sets the spool's record and byte gauges from its counters.
fn refresh_gauges(count: &AtomicU64, total: &AtomicU64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::gauge!("spool_messages").set(count.load(AtomicOrdering::Relaxed) as f64);
        ::metrics::gauge!("spool_bytes").set(total.load(AtomicOrdering::Relaxed) as f64);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (count, total);
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

    /// What a written dead letter reports for its source: `Rejected` when an
    /// enabled DLQ holds it, `Dropped` when there is none and it was dropped.
    pub(crate) fn status(&self) -> DeliveryStatus {
        #[cfg(feature = "dlq")]
        if self.dlq().is_some_and(|dlq| dlq.is_enabled()) {
            return DeliveryStatus::Rejected;
        }
        #[cfg(not(feature = "dlq"))]
        let _ = self;
        DeliveryStatus::Dropped
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

/// Unframe one spooled entry, decompress it into `scratch` under its own
/// codec, and decode its records: each framed by a 4-byte little-endian
/// length, or one bare record in an entry of the format before. Over
/// `previous_cap` decoded bytes, an entry of the format before is damaged.
fn decode_entry(
    raw: &[u8],
    decoders: &mut Decoders,
    scratch: &mut Vec<u8>,
    previous_cap: usize,
) -> Result<Vec<Record>, Unreadable> {
    let entry = crate::spool_codec::unframe(raw).map_err(Unreadable::Damaged)?;
    decoders.decode_into(&entry, scratch, previous_cap)?;
    let unreadable = |e: crate::transport::RecordCodecError| Unreadable::Codec(e.to_string());
    let Some(counted) = entry.counted else {
        return Ok(vec![Record::decode(scratch).map_err(unreadable)?]);
    };
    let mut records = Vec::with_capacity(usize::try_from(counted.records).unwrap_or(0).min(4096));
    let mut rest = scratch.as_slice();
    while let Some((len, after)) = rest.split_first_chunk::<4>() {
        let len = usize::try_from(u32::from_le_bytes(*len)).unwrap_or(usize::MAX);
        let Some((record, after)) = after.split_at_checked(len) else {
            return Err(Unreadable::Codec(format!(
                "a record of {len} bytes runs past its entry's end"
            )));
        };
        records.push(Record::decode(record).map_err(unreadable)?);
        rest = after;
    }
    if !rest.is_empty() || records.len() != usize::try_from(counted.records).unwrap_or(0) {
        return Err(Unreadable::Codec(format!(
            "the entry holds {} records and {} stray bytes, not the {} its prefix names",
            records.len(),
            rest.len(),
            counted.records
        )));
    }
    Ok(records)
}

/// Frames `encoded` records as one entry's body: each record's 4-byte
/// little-endian length, then the record.
pub(crate) fn frame(encoded: &[u8], body: &mut Vec<u8>) {
    body.extend_from_slice(
        &u32::try_from(encoded.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    body.extend_from_slice(encoded);
}

/// The spill the drain reads from, and what it touches when the queue changes.
pub(crate) struct DrainQueue {
    pub(crate) spill: Arc<Mutex<Spill>>,
    /// `None` while a quarantine swaps in a fresh queue, or after it failed.
    pub(crate) receiver: Arc<parking_lot::Mutex<Option<Receiver>>>,
    pub(crate) cell: QueueCell,
    pub(crate) path: PathBuf,
    pub(crate) options: yaque::Options,
    pub(crate) metrics: Arc<StoreMetrics>,
    #[cfg(feature = "memory")]
    pub(crate) memory: Arc<std::sync::OnceLock<Arc<crate::memory::MemoryGuard>>>,
    /// Pieces waiting on spilled entries: a commit reports those it passes.
    pub(crate) acks: Arc<Acks>,
    /// The most an entry of the format before may decode to.
    pub(crate) previous_cap: usize,
}

/// The spool's entry and byte counters, shared with the sink.
pub(crate) struct Counters {
    pub(crate) count: Arc<AtomicU64>,
    pub(crate) bytes: Arc<AtomicU64>,
    /// Wakes the sends a full spool blocks once a pass frees room.
    pub(crate) progress: Arc<tokio::sync::Notify>,
    /// When the entry at the spool's front was written, 0 for none.
    pub(crate) oldest_ms: Arc<AtomicU64>,
}

impl Counters {
    /// Sets the front's write time after a pass, 0 when the pass left none.
    /// A spill that lands while the pass looked counts from now.
    fn front(&self, written_ms: u64) {
        self.oldest_ms.store(written_ms, AtomicOrdering::Release);
        if written_ms == 0 && self.count.load(AtomicOrdering::Acquire) > 0 {
            let _ = self.oldest_ms.compare_exchange(
                0,
                crate::spool_codec::now_ms(),
                AtomicOrdering::AcqRel,
                AtomicOrdering::Relaxed,
            );
        }
    }
}

/// The drain's health, shared with the sink's health check.
pub(crate) struct Flags {
    /// Set while the drain holds at an entry its codec cannot read.
    pub(crate) held: Arc<AtomicBool>,
    /// Set while the spool counts records the drain does not find.
    pub(crate) stalled: Arc<AtomicBool>,
}

impl DrainQueue {
    fn guard(&self) -> Option<Arc<yaque::MemoryGuard>> {
        #[cfg(feature = "memory")]
        return self.memory.get().cloned();
        #[cfg(not(feature = "memory"))]
        {
            let _ = self;
            None
        }
    }

    /// Reads up to `cap` entries within `max_bytes` on the blocking pool.
    async fn read(&self, cap: usize, max_bytes: u64) -> std::io::Result<Taken> {
        let receiver = Arc::clone(&self.receiver);
        let guard = self.guard();
        let (taken, reserved) = tokio::task::spawn_blocking(move || {
            let mut receiver = receiver.lock();
            let Some(receiver) = receiver.as_mut() else {
                return Ok((Taken::default(), 0));
            };
            let taken = receiver.read(cap, max_bytes, guard.as_deref())?;
            Ok::<_, std::io::Error>((taken, receiver.reserved_bytes()))
        })
        .await
        .map_err(std::io::Error::other)??;
        self.metrics.reserved(reserved, taken.held_back);
        Ok(taken)
    }

    /// Commits the `settled` entries, the first of those read, and returns the
    /// rest, on the blocking pool: a commit can delete segments and save the
    /// position. Reports the pieces waiting on the settled entries with each
    /// entry's status. Returns when the entry left at the front was written,
    /// 0 for none.
    async fn commit(&self, settled: &[(QueueState, DeliveryStatus)]) -> u64 {
        let entries = settled.len();
        let receiver = Arc::clone(&self.receiver);
        let guard = self.guard();
        let committed = tokio::task::spawn_blocking(move || {
            receiver.lock().as_mut().map(|receiver| {
                let kept = receiver.taken().get(entries).map(|raw| written_ms(raw));
                receiver.commit(entries, guard.as_deref());
                let front = kept.unwrap_or_else(|| front_written_ms(receiver));
                (receiver.disk_bytes(), front)
            })
        })
        .await;
        self.metrics.reserved(0, false);
        match committed {
            Ok(Some((disk, front))) => {
                self.acks.report_drained(settled);
                self.metrics.disk(disk);
                front
            }
            _ => 0,
        }
    }

    /// Ends a drained segment so its file goes, once the spool is empty.
    async fn release_drained(&self) {
        let Some(position) = self.receiver.lock().as_ref().map(Receiver::position) else {
            return;
        };
        let spill = Arc::clone(&self.spill);
        let receiver = Arc::clone(&self.receiver);
        let released = tokio::task::spawn_blocking(move || {
            let released = match spill.blocking_lock().sender.as_mut() {
                Some(sender) => sender.release_drained(position)?,
                None => false,
            };
            match receiver.lock().as_mut() {
                Some(receiver) if released => {
                    receiver.sweep()?;
                    Ok(Some(receiver.disk_bytes()))
                }
                _ => Ok(None),
            }
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|released: std::io::Result<Option<u64>>| released);
        match released {
            Ok(Some(disk)) => self.metrics.disk(disk),
            Ok(None) => {}
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %e, "cannot release the spill's drained segment");
                let _ = e;
            }
        }
    }

    /// Moves a spill whose header is damaged aside and opens a fresh one in
    /// its place. Every record from the damaged header on is set aside with it.
    async fn quarantine(&self, reason: String, counters: &Counters) {
        let spill = Arc::clone(&self.spill);
        let receiver = Arc::clone(&self.receiver);
        let cell = Arc::clone(&self.cell);
        let path = self.path.clone();
        let options = self.options;
        let acks = Arc::clone(&self.acks);
        let reopened = tokio::task::spawn_blocking(move || {
            let mut spill = spill.blocking_lock();
            let mut receiver = receiver.lock();
            // The halves hold the queue's locks, so they close before the fresh queue opens.
            spill.sender = None;
            *receiver = None;
            *cell.lock() = None;
            let quarantined = crate::spool_codec::quarantine_damaged(&path, &reason, options);
            // Their entries went aside with the queue, so their sources deliver again.
            acks.fail_all();
            let (sender, fresh) = quarantined.map_err(|e| e.to_string())?;
            *cell.lock() = Some(sender.shared());
            spill.sender = Some(sender);
            *receiver = Some(fresh);
            Ok::<_, String>(())
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|reopened| reopened);
        counters.count.store(0, AtomicOrdering::Relaxed);
        counters.bytes.store(0, AtomicOrdering::Relaxed);
        counters.oldest_ms.store(0, AtomicOrdering::Release);
        refresh_gauges(&counters.count, &counters.bytes);
        if let Err(e) = reopened {
            #[cfg(feature = "tracing")]
            tracing::error!(error = %e, "cannot reopen the spill after quarantining it; spills fail until a restart");
            let _ = e;
        }
    }
}

/// Take up to `cap` records' worth of entries within `batch.max_bytes`, send
/// the readable records in one `send_batch` under `send_timeout`, and commit
/// every entry only when the sink takes them. Any other answer returns the
/// whole batch to the queue. With a `cursor`, take the head entry and send
/// only its next record, committing the entry after its last.
#[allow(clippy::too_many_arguments)]
async fn drain_once<S: TransportSender>(
    sink: &S,
    queue: &DrainQueue,
    cap: usize,
    batch: DrainBatch,
    decoders: &mut Decoders,
    scratch: &mut Vec<u8>,
    send_timeout: Duration,
    counters: &Counters,
    dead_letters: &DeadLetters,
    cursor: Option<&mut Cursor>,
) -> (Vec<DrainResult>, usize) {
    let taken = match queue.read(cap, batch.max_bytes).await {
        Ok(taken) if taken.entries == 0 => return (vec![DrainResult::Empty], 0),
        Ok(taken) => taken.entries,
        Err(e) if crate::spool_codec::is_damaged_header(&e) => {
            return (vec![DrainResult::Damaged(e.to_string())], 0);
        }
        Err(e) => return (vec![DrainResult::IoError(e.to_string())], 0),
    };
    let (outcome, settled) = deliver(
        sink,
        queue,
        taken,
        decoders,
        scratch,
        send_timeout,
        counters,
        dead_letters,
        cursor,
    )
    .await;
    counters.front(queue.commit(&settled).await);
    (outcome, settled.len())
}

/// Where the drain is in the head entry while it sends records one at a
/// time: the records already settled, and the worst outcome among them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Cursor {
    at: usize,
    worst: DeliveryStatus,
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            at: 0,
            worst: DeliveryStatus::Delivered,
        }
    }
}

/// Decode, send and dead-letter the `taken` entries the receiver holds.
/// Returns the outcome and the entries to commit, each with where it ends and
/// its status for the pieces waiting on it, the rest going back to the queue.
/// With a `cursor`, only the head entry's next record goes, and the entry
/// commits once the cursor passes its last.
#[allow(clippy::too_many_arguments)]
async fn deliver<S: TransportSender>(
    sink: &S,
    queue: &DrainQueue,
    taken: usize,
    decoders: &mut Decoders,
    scratch: &mut Vec<u8>,
    send_timeout: Duration,
    counters: &Counters,
    dead_letters: &DeadLetters,
    cursor: Option<&mut Cursor>,
) -> (Vec<DrainResult>, Vec<(QueueState, DeliveryStatus)>) {
    // Each record read, with the index of the entry it came from.
    let mut read_back: Vec<(Record, usize)> = Vec::with_capacity(taken);
    let mut unreadable = Vec::new();
    let mut wrong_codec = None;
    // Each entry's end and status, worst of its records.
    let mut settled: Vec<(QueueState, DeliveryStatus)> = Vec::with_capacity(taken);
    // What the entries take on disk, each 4-byte header included, and the
    // records they count for in the spool.
    let (mut stored, mut held) = (0_u64, 0_u64);
    let mut head_records = 0;
    {
        let receiver = queue.receiver.lock();
        let Some(receiver) = receiver.as_ref() else {
            return (vec![DrainResult::Empty], Vec::new());
        };
        for (index, (raw, end)) in receiver.taken().iter().zip(receiver.ends()).enumerate() {
            let status = match decode_entry(raw, decoders, scratch, queue.previous_cap) {
                Ok(records) => {
                    if index == 0 {
                        head_records = records.len();
                    }
                    read_back.extend(records.into_iter().map(|record| (record, index)));
                    DeliveryStatus::Delivered
                }
                // Dropped as damaged: its source delivers it again.
                Err(Unreadable::Damaged(e)) => {
                    queue.metrics.damaged(&e);
                    unreadable.push(DrainResult::Corrupt);
                    DeliveryStatus::Errored
                }
                Err(Unreadable::Codec(e)) => {
                    wrong_codec = Some((index, e));
                    break;
                }
            };
            settled.push((*end, status));
            stored += 4 + raw.len() as u64;
            held += crate::spool_codec::shape(raw).records;
        }
    }
    // Only the entries before one the codec cannot read are taken. The rest
    // stay spooled in order.
    let take = match wrong_codec {
        None => taken,
        Some((0, error)) => return (vec![DrainResult::Held(error)], Vec::new()),
        Some((index, _)) => index,
    };
    if let Some(cursor) = cursor.as_deref() {
        read_back = read_back.into_iter().skip(cursor.at).take(1).collect();
    }
    let mut records = Vec::with_capacity(read_back.len());
    let mut owners = Vec::with_capacity(read_back.len());
    let mut dead = Vec::new();
    let mut dead_owners = Vec::new();
    for (record, owner) in read_back {
        match sink.dead_letter_reason(&record) {
            None => {
                records.push(record);
                owners.push(owner);
            }
            Some(reason) => {
                dead.push(Undeliverable {
                    label: reason.as_str(),
                    reason: reason.to_string(),
                    record,
                });
                dead_owners.push(owner);
            }
        }
    }

    let sent = if records.is_empty() {
        None
    } else {
        let deadline = tokio::time::Instant::now() + send_timeout;
        Some(sink.send_batch_by(&records, deadline).await)
    };
    let delivered = match sent {
        // Only unreadable entries and dead letters: nothing to send.
        None => 0,
        Some(SendResult::Ok) => records.len(),
        // Backpressure, a named wait, an elapsed deadline (the batch may or
        // may not have reached the sink) and a closed or misconfigured sink
        // keep the whole batch spooled (at-least-once).
        Some(SendResult::Backpressured | SendResult::RetryAfter(_)) => {
            return (vec![DrainResult::SinkUnavailable], Vec::new());
        }
        Some(SendResult::Fatal(e)) if e.is_fatal() => {
            return (vec![DrainResult::SinkUnavailable], Vec::new());
        }
        // A refusal or dead letter in a batch cannot say which record: keep
        // them all and send them one at a time until that one is past.
        Some(SendResult::Fatal(_) | SendResult::Reject(_) | SendResult::FilteredDlq)
            if records.len() > 1 =>
        {
            return (vec![DrainResult::Isolate(take)], Vec::new());
        }
        // On its own, the refused record is its own fault: dead-letter it.
        Some(SendResult::Fatal(e) | SendResult::Reject(e)) => {
            dead.extend(records.drain(..).map(|record| Undeliverable {
                label: crate::transport::DEAD_LETTER_REJECTED,
                reason: e.to_string(),
                record,
            }));
            dead_owners.append(&mut owners);
            0
        }
        Some(SendResult::FilteredDlq) => {
            dead.extend(records.drain(..).map(|record| Undeliverable {
                label: crate::transport::DEAD_LETTER_REJECTED,
                reason: crate::transport::DeadLetterReason::Rejected.to_string(),
                record,
            }));
            dead_owners.append(&mut owners);
            0
        }
    };
    let dead_lettered = dead.len();
    if dead_lettered > 0
        && !dead_letters
            .write(dead, send_timeout, OnRefusal::Drop)
            .await
    {
        return (vec![DrainResult::DeadLetterUnwritten], Vec::new());
    }
    let dead_status = dead_letters.status();
    for owner in dead_owners {
        if let Some((_, status)) = settled.get_mut(owner) {
            *status = (*status).max(dead_status);
        }
    }
    let mut results = unreadable;
    if dead_lettered > 0 {
        results.push(DrainResult::DeadLettered(dead_lettered));
    }
    if delivered > 0 {
        results.push(DrainResult::Success(delivered));
    }
    settled.truncate(take);
    // A record of the head entry is settled. The entry commits after its
    // last, with the worst outcome among its records.
    if let Some(cursor) = cursor {
        if let Some((_, status)) = settled.first_mut() {
            cursor.worst = cursor.worst.max(*status);
            *status = cursor.worst;
        }
        cursor.at += 1;
        if cursor.at < head_records {
            return (results, Vec::new());
        }
        *cursor = Cursor::default();
    }
    release_many(&counters.count, &counters.bytes, held, stored);
    (results, settled)
}

/// Passes in a row that find nothing while the spool counts records, before
/// the drain reports itself stalled.
const STALLED_AFTER_PASSES: u32 = 50;

/// Drain loop. Runs until shutdown signalled.
///
/// Each pass takes up to `batch` entries, at most one second's worth at the
/// strategy's current rate, and sends them in one `send_batch`. Reads and
/// commits run on the blocking pool, and no lock is held across the send.
///
/// `send_timeout`: bounds each downstream send, the hot path's limit, so a hung
/// sink cannot pin the StrictFifo gate or `shutdown()`.
///
/// `fifo_gate`: `Some` in StrictFifo, acquired per delivery cycle
/// to serialise with senders. `None` in Interleaved (lock-free).
#[allow(clippy::too_many_arguments)]
pub async fn drain_loop<S: TransportSender + 'static>(
    queue: DrainQueue,
    sink: Arc<S>,
    counters: Counters,
    circuit: Arc<CircuitBreaker>,
    flags: Flags,
    strategy: DrainStrategy,
    interval: Duration,
    send_timeout: Duration,
    batch: DrainBatch,
    shutdown: CancellationToken,
    fifo_gate: Option<Arc<Mutex<()>>>,
    dead_letters: DeadLetters,
) {
    let mut drainer = Drainer::new(strategy);
    let mut decoders = Decoders::default();
    // Entries still to send a record at a time after a refused batch, and the
    // records of the head one already settled.
    let mut isolate = 0_usize;
    let mut cursor = Cursor::default();
    // Decompressed entries land here, reused across passes.
    let mut scratch = Vec::new();
    let mut read_failing = false;
    let mut empty_passes = 0_u32;

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
        queue
            .metrics
            .oldest(counters.oldest_ms.load(AtomicOrdering::Acquire));

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
        let isolating = isolate > 0;
        let cap = if isolating {
            1
        } else {
            per_pass(batch.max_records, drainer.current_rate())
        };
        let (results, committed) = drain_once(
            sink.as_ref(),
            &queue,
            cap,
            batch,
            &mut decoders,
            &mut scratch,
            send_timeout,
            &counters,
            &dead_letters,
            isolating.then_some(&mut cursor),
        )
        .await;
        if isolating && committed > 0 {
            isolate -= 1;
        }

        let mut took = 0_usize;
        let mut progressed = false;
        for result in results {
            note_hold(&result, &flags.held);
            note_read_error(&result, &mut read_failing);
            match &result {
                DrainResult::Success(records) => {
                    took += records;
                    pause = Duration::ZERO;
                    progressed = true;
                }
                DrainResult::DeadLettered(_) | DrainResult::Corrupt => {
                    pause = Duration::ZERO;
                    progressed = true;
                }
                DrainResult::Isolate(entries) => {
                    isolate = *entries;
                    cursor = Cursor::default();
                    pause = Duration::ZERO;
                }
                DrainResult::Damaged(reason) => {
                    queue.quarantine(reason.clone(), &counters).await;
                    isolate = 0;
                    cursor = Cursor::default();
                    progressed = true;
                }
                DrainResult::Empty => {
                    if counters.count.load(AtomicOrdering::Relaxed) > 0 {
                        empty_passes = empty_passes.saturating_add(1);
                    } else {
                        empty_passes = 0;
                    }
                }
                _ => {}
            }
            settle(result, &mut drainer, &circuit).await;
        }
        if progressed {
            empty_passes = 0;
            if counters.count.load(AtomicOrdering::Relaxed) == 0 {
                queue.release_drained().await;
            }
            counters.progress.notify_waiters();
        }
        note_stall(empty_passes >= STALLED_AFTER_PASSES, &flags.stalled);

        // The strategy's delay is per record, so a batch waits for each one.
        pause += drainer
            .delay()
            .saturating_mul(u32::try_from(took.max(1)).unwrap_or(u32::MAX));
    }
}

/// Count a pass's read error, logging only when reads start or stop failing.
fn note_read_error(result: &DrainResult, failing: &mut bool) {
    let now_failing = match result {
        DrainResult::IoError(_) => true,
        DrainResult::Empty | DrainResult::SinkUnavailable => return,
        _ => false,
    };
    if let DrainResult::IoError(_) = result {
        #[cfg(feature = "metrics")]
        ::metrics::counter!("tiered_sink_drain_read_errors_total").increment(1);
    }
    if *failing == now_failing {
        return;
    }
    *failing = now_failing;
    #[cfg(feature = "tracing")]
    if let DrainResult::IoError(error) = result {
        tracing::warn!(error = %error, "the drain cannot read the spill; retrying each pass");
    } else {
        tracing::info!("the drain reads the spill again");
    }
}

/// Set or clear `stalled`, logging and gauging only the change.
fn note_stall(stalled: bool, flag: &AtomicBool) {
    if flag.swap(stalled, AtomicOrdering::Relaxed) == stalled {
        return;
    }
    #[cfg(feature = "metrics")]
    ::metrics::gauge!("tiered_sink_drain_stalled").set(if stalled { 1.0 } else { 0.0 });
    #[cfg(feature = "tracing")]
    if stalled {
        tracing::warn!("the spool counts records the drain does not find; it may be stalled");
    } else {
        tracing::info!("the drain is taking records again");
    }
}

/// Set or clear `held` from a pass's result, logging and counting only the
/// change, so a hold is reported once however many passes it lasts.
fn note_hold(result: &DrainResult, held: &AtomicBool) {
    let holding = match result {
        DrainResult::Held(_) => true,
        DrainResult::Empty | DrainResult::IoError(_) | DrainResult::SinkUnavailable => return,
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
            error = %error,
            "The spill's next entry does not read, so the drain holds and keeps it. Open the spill with a build that reads its codec"
        );
    } else {
        tracing::info!("The spill's next entry reads again, so the drain resumes");
    }
}

/// When the next entry in `receiver` was written, read and returned to the
/// queue, 0 for none.
pub(crate) fn front_written_ms(receiver: &mut Receiver) -> u64 {
    let front = match receiver.read(1, u64::MAX, None) {
        Ok(taken) if taken.entries > 0 => receiver.taken().first().map_or(0, |raw| written_ms(raw)),
        _ => 0,
    };
    receiver.commit(0, None);
    front
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
