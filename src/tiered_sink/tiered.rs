// Project:   scalo
// File:      src/tiered_sink/tiered.rs
// Purpose:   TieredSink implementation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TieredSink implementation.

use crate::compression::CompressorPool;
use crate::concurrency::{PeriodicTask, PeriodicWorker, TickError};
use crate::spool_codec::{self, FullDisk, QueueCell, StoreMetrics, SyncTicker};
use crate::tiered_sink::{
    CircuitBreaker, CircuitState, OrderingMode, Result, TieredSinkConfig, TieredSinkError, drainer,
};
use crate::transport::{Record, SendResult, TransportSender};
use crate::yaque::{self, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The `store` label on the spill's metrics.
pub(crate) const STORE: &str = "tiered_sink";

/// A spill at or over this many bytes is written on the blocking pool, where
/// its copy into the page cache cannot hold a runtime worker. Under it, the
/// write takes a few microseconds.
const INLINE_WRITE_BYTES: usize = 64 * 1024;

/// The spill's sending half and its full-disk state, behind one lock.
pub(crate) struct Spill {
    /// `None` while a quarantine swaps in a fresh queue, or after it failed.
    pub(crate) sender: Option<Sender>,
    pub(crate) full: FullDisk,
}

impl Spill {
    /// Appends one entry, unless the disk is full and its retry is not due.
    fn write(&mut self, prefix: &[u8], body: &[u8]) -> std::io::Result<()> {
        let now = Instant::now();
        let written = 4 + prefix.len() + body.len();
        let sender = self.sender.as_mut().ok_or_else(|| {
            std::io::Error::other("the spill is closed after a failed quarantine")
        })?;
        if !self.full.admits(now, sender.freed()) {
            self.full.refused(written);
            return Err(spool_codec::full_error());
        }
        match sender.send(prefix, body) {
            Ok(()) => {
                self.full.wrote(now);
                Ok(())
            }
            Err(e) => {
                if spool_codec::is_full(&e) {
                    self.full.failed(now, written, &e);
                }
                Err(e)
            }
        }
    }
}

/// A tiered sink with automatic disk spillover.
///
/// Wraps any [`TransportSender`] and automatically spills records to disk when
/// the downstream is unavailable or backpressuring. A background task drains
/// spooled records back to the downstream when it recovers.
///
/// Records take the happy path straight to the sender (`send_batch`, no encode);
/// only on a spill is a record serialised ([`Record::encode`]) into the spool,
/// and on drain decoded back ([`Record::decode`]) -- so the full record (payload,
/// routing key, headers, dedup key) survives a replay, with zero serialisation
/// cost on the happy path.
pub struct TieredSink<S: TransportSender> {
    sink: Arc<S>,
    /// The sending half. Held only for a write, never across an await.
    spill: Arc<Mutex<Spill>>,
    /// The open queue's shared half, for syncs.
    cell: QueueCell,
    metrics: Arc<StoreMetrics>,
    /// Bytes of `max_spool_bytes` held back for the read records a segment
    /// keeps on disk until it goes.
    segment_reserve: u64,
    /// The memory guard the drain reserves against, once one is set.
    #[cfg(feature = "memory")]
    memory: Arc<std::sync::OnceLock<Arc<crate::memory::MemoryGuard>>>,
    spool_count: Arc<AtomicU64>,
    spool_bytes: Arc<AtomicU64>,
    /// Notified each time the drain frees room.
    progress: Arc<tokio::sync::Notify>,
    /// When the entry at the spool's front was written, 0 for none.
    oldest_ms: Arc<AtomicU64>,
    circuit: Arc<CircuitBreaker>,
    /// Spill codec contexts, one per `send` compressing at once.
    compressors: CompressorPool,
    /// Set while the drain holds at an entry the spill codec cannot read.
    drain_held: Arc<std::sync::atomic::AtomicBool>,
    /// Fails readiness while `drain_held` is set, and degrades it while the
    /// drain is stalled or the disk is full. Leaves the registry with the sink.
    #[cfg(feature = "health")]
    _drain_health: crate::health::HealthRegistration,
    config: TieredSinkConfig,
    /// Stops the drainer and the disk poller. Every waiter sees the cancel, even one that waits after it.
    shutdown: CancellationToken,
    drain_handle: Option<JoinHandle<()>>,
    disk_available: Arc<std::sync::atomic::AtomicBool>,
    disk_poller_handle: Option<PeriodicWorker>,
    /// Syncs the spill every `sync_interval_ms`, and once more at shutdown.
    sync_ticker: Option<PeriodicWorker>,
    /// Set once `shutdown` has joined both tasks, so its own drop of `self` is not reported as one.
    shut_down: bool,

    /// Serialises send + drain in `StrictFifo`. `None` otherwise.
    fifo_gate: Option<Arc<Mutex<()>>>,

    /// Where the drain and `when_full: dlq` write records the sink will not take.
    #[cfg(feature = "dlq")]
    dead_letters: drainer::DeadLetters,

    // Metrics
    hot_path_count: AtomicU64,
    cold_path_count: AtomicU64,
}

impl<S: TransportSender + 'static> TieredSink<S> {
    /// Create a new TieredSink wrapping the given sender.
    ///
    /// Lock files left by a process that was killed are cleared on open, so a
    /// restart after a hard kill replays the spill cache. A spill cache another
    /// live sink holds is refused: give each sink its own `spool_path`.
    ///
    /// # Errors
    ///
    /// Returns [`TieredSinkError::SpoolOpen`] if the spill cache is locked by a
    /// live owner, or cannot be opened and was not quarantined (see
    /// `TieredSinkConfig::on_corruption`), and [`TieredSinkError::Codec`] for a
    /// `compression` the spill does not take.
    pub async fn new(sink: S, config: TieredSinkConfig) -> Result<Self> {
        Self::with_volume_limits(
            sink,
            config,
            std::env::var(crate::tiered_sink::WRITABLE_PATH_LIMITS_ENV).ok(),
            crate::env::runtime_context().is_kubernetes(),
        )
        .await
    }

    /// [`new`](Self::new) with the volume limits and the Kubernetes check
    /// given rather than read from the process.
    async fn with_volume_limits(
        sink: S,
        mut config: TieredSinkConfig,
        volume_limits: Option<String>,
        kubernetes: bool,
    ) -> Result<Self> {
        let codec_error = |e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e);
        let compression = super::config::ACCEPTS
            .check(config.compression)
            .map_err(codec_error)?;
        let compressors = CompressorPool::new(compression).map_err(codec_error)?;
        if config.max_spool_bytes.is_none()
            && let Some(cap) = volume_limits
                .as_deref()
                .and_then(|limits| config.volume_spool_cap(limits))
        {
            config.max_spool_bytes = Some(cap);
            #[cfg(feature = "tracing")]
            tracing::info!(
                spool_path = %config.spool_path.display(),
                max_spool_bytes = cap,
                "spool capped below its volume's size limit"
            );
        }
        // statvfs on an emptyDir reads the node's disk, so disk_aware alone lets
        // the spool pass the volume's sizeLimit, and kubelet evicts the pod.
        let spool_open_err = |message: String| TieredSinkError::SpoolOpen {
            path: config.spool_path.display().to_string(),
            message,
        };
        if kubernetes && config.disk_aware.is_some() && config.max_spool_bytes.is_none() {
            return Err(spool_open_err(
                "disk_aware needs max_spool_bytes on Kubernetes, or the spool on a writable \
                 path with a size limit"
                    .to_string(),
            ));
        }

        let (sender, receiver, initial_count, initial_bytes, options, segment_reserve, front_ms) =
            open_spill(&config).await.map_err(spool_open_err)?;

        let sink = Arc::new(sink);
        let cell: QueueCell = Arc::new(parking_lot::Mutex::new(Some(sender.shared())));
        let metrics = Arc::new(StoreMetrics::new(STORE, compression.codec));
        metrics.disk(sender.disk_bytes());
        let full = FullDisk::new(STORE);
        let disk_full = full.flag();
        let spill = Arc::new(Mutex::new(Spill {
            sender: Some(sender),
            full,
        }));
        let receiver = Arc::new(parking_lot::Mutex::new(Some(receiver)));
        #[cfg(feature = "memory")]
        let memory = Arc::new(std::sync::OnceLock::new());
        let spool_count = Arc::new(AtomicU64::new(initial_count));
        let spool_bytes = Arc::new(AtomicU64::new(initial_bytes));
        let progress = Arc::new(tokio::sync::Notify::new());
        let oldest_ms = Arc::new(AtomicU64::new(front_ms));

        warn_on_short_reset(&config);
        let circuit = Arc::new(CircuitBreaker::new(
            config.circuit_failure_threshold,
            config.circuit_reset_timeout(),
        ));
        let shutdown = CancellationToken::new();
        let drain_held = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drain_stalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(feature = "health")]
        let drain_health = drain_health(&drain_held, &drain_stalled, disk_full);
        #[cfg(not(feature = "health"))]
        let _ = disk_full;
        let sync_ticker = spool_codec::start_sync_ticker(
            SyncTicker {
                path: config.spool_path.clone(),
                store: STORE,
                cell: Arc::clone(&cell),
            },
            Duration::from_millis(config.sync_interval_ms),
            shutdown.clone(),
        );
        let disk_available = Arc::new(std::sync::atomic::AtomicBool::new(true));

        // Shared by senders + drainer in StrictFifo to enforce
        // total ordering at the sink.
        let fifo_gate =
            matches!(config.ordering, OrderingMode::StrictFifo).then(|| Arc::new(Mutex::new(())));

        // Start disk-aware capacity poller if configured
        let disk_poller_handle = config.disk_aware.as_ref().map(|disk_cfg| {
            let poller = DiskCapacityPoller {
                spool_path: config.spool_path.clone(),
                disk_available: Arc::clone(&disk_available),
                max_usage_percent: disk_cfg.max_usage_percent,
            };
            // A zero interval would panic the timer, so it reads as the smallest whole second.
            let every = std::time::Duration::from_secs(disk_cfg.poll_interval_secs.max(1));
            PeriodicWorker::spawn(poller, every, shutdown.clone())
        });

        let dead_letters = drainer::DeadLetters::default();
        let drain_handle = tokio::spawn(drainer::drain_loop(
            drainer::DrainQueue {
                spill: Arc::clone(&spill),
                receiver,
                cell: Arc::clone(&cell),
                path: config.spool_path.clone(),
                options,
                metrics: Arc::clone(&metrics),
                #[cfg(feature = "memory")]
                memory: Arc::clone(&memory),
            },
            Arc::clone(&sink),
            drainer::Counters {
                count: Arc::clone(&spool_count),
                bytes: Arc::clone(&spool_bytes),
                progress: Arc::clone(&progress),
                oldest_ms: Arc::clone(&oldest_ms),
            },
            Arc::clone(&circuit),
            drainer::Flags {
                held: Arc::clone(&drain_held),
                stalled: drain_stalled,
            },
            config.drain_strategy,
            config.drain_interval(),
            config.send_timeout_duration(),
            drainer::DrainBatch {
                max_records: config.drain_batch_max_records,
                max_bytes: config.drain_batch_max_bytes,
            },
            shutdown.clone(),
            fifo_gate.as_ref().map(Arc::clone),
            dead_letters.clone(),
        ));

        Ok(Self {
            sink,
            spill,
            cell,
            metrics,
            segment_reserve,
            #[cfg(feature = "memory")]
            memory,
            spool_count,
            spool_bytes,
            progress,
            oldest_ms,
            circuit,
            compressors,
            drain_held,
            #[cfg(feature = "health")]
            _drain_health: drain_health,
            config,
            shutdown,
            drain_handle: Some(drain_handle),
            disk_available,
            disk_poller_handle,
            sync_ticker,
            shut_down: false,
            fifo_gate,
            #[cfg(feature = "dlq")]
            dead_letters,
            hot_path_count: AtomicU64::new(0),
            cold_path_count: AtomicU64::new(0),
        })
    }

    /// Send a message through the tiered sink.
    ///
    /// The message goes through the hot path (direct to sink) when the circuit
    /// breaker admits it (closed, or the single half-open probe) and, in
    /// StrictFifo, the spool is empty. A `Backpressured` or `RetryAfter` answer, or
    /// a send past `send_timeout_ms`, spools it instead, as does a refused hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The sink names the record in `dead_letter_reason`, or answers
    ///   `FilteredDlq` or `Reject` for it ([`TieredSinkError::DeadLetter`])
    /// - The sink answers `Fatal` ([`TieredSinkError::Sink`])
    /// - The spool is full and `when_full` is `block`, or `dlq` with no DLQ
    ///   set, and the drain frees no room for it within `when_full_wait_ms`
    ///   ([`TieredSinkError::SpoolFull`])
    /// - The `disk_aware` threshold is crossed ([`TieredSinkError::DiskUnavailable`])
    /// - Compression or the spool write fails
    /// - `sync_on_spill` is set and the sync fails, with the record already
    ///   spilled, so a retry can deliver it twice
    pub async fn send(&self, record: &Record) -> Result<()> {
        // Never spooled: the drain would only meet it again.
        if let Some(reason) = self.sink.dead_letter_reason(record) {
            return Err(TieredSinkError::DeadLetter(reason.to_string()));
        }
        let wait = Duration::from_millis(self.config.when_full_wait_ms);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // Registered before the attempt, so a pass that ends during it still wakes this send.
            let mut progressed = std::pin::pin!(self.progress.notified());
            progressed.as_mut().enable();
            match self.send_once(record).await {
                // Waits outside the StrictFifo gate, which the drain needs to make room.
                Err(TieredSinkError::SpoolFull(message)) => {
                    if wait.is_zero()
                        || tokio::time::timeout_at(deadline, progressed).await.is_err()
                    {
                        return Err(TieredSinkError::SpoolFull(message));
                    }
                }
                sent => return sent,
            }
        }
    }

    /// One attempt at [`send`](Self::send), its `SpoolFull` not yet waited on.
    async fn send_once(&self, record: &Record) -> Result<()> {
        // StrictFifo: serialise decision + send + enqueue against
        // other senders and the drainer. Interleaved: no gate.
        let _gate = match &self.fifo_gate {
            Some(gate) => Some(gate.lock().await),
            None => None,
        };

        let use_hot_path = self.should_use_hot_path().await;

        if use_hot_path {
            match self.try_hot_path(record).await {
                Ok(()) => {
                    self.hot_path_count.fetch_add(1, AtomicOrdering::Relaxed);
                    #[cfg(feature = "metrics")]
                    ::metrics::counter!("spool_hot_path_total").increment(1);
                    return Ok(());
                }
                Err(e @ (TieredSinkError::Sink(_) | TieredSinkError::DeadLetter(_))) => {
                    return Err(e);
                }
                Err(_) => {
                    // Retryable error, fall through to spool
                }
            }
        }

        // Cold path: spool to disk
        self.spool_message(record).await?;
        self.cold_path_count.fetch_add(1, AtomicOrdering::Relaxed);
        #[cfg(feature = "metrics")]
        ::metrics::counter!("spool_cold_path_total").increment(1);
        Ok(())
    }

    /// Sync every record spilled so far to disk.
    ///
    /// A spill is flushed to the operating system and survives the process being
    /// killed, and survives the host losing power once a sync has run. The
    /// timed sync runs every [`sync_interval_ms`](TieredSinkConfig::sync_interval_ms).
    /// One call covers any number of spills, so a caller that accepts records in
    /// batches syncs once per batch. [`sync_on_spill`](TieredSinkConfig::sync_on_spill)
    /// syncs inside every spilling `send` instead.
    ///
    /// The sync runs on tokio's blocking pool.
    ///
    /// # Errors
    ///
    /// Returns [`TieredSinkError::Spool`] when a segment or the directory cannot
    /// be synced.
    pub async fn sync(&self) -> Result<()> {
        spool_codec::sync_queue(
            self.config.spool_path.clone(),
            STORE,
            Arc::clone(&self.cell),
        )
        .await
        .map_err(|e| TieredSinkError::Spool(format!("spool sync failed: {e}")))
    }

    /// Reserve each spilled record's length with `guard` before the drain reads
    /// it, so a pressed process drains in smaller batches.
    #[cfg(feature = "memory")]
    #[must_use]
    pub fn with_memory_guard(self, guard: Arc<crate::memory::MemoryGuard>) -> Self {
        let _ = self.memory.set(guard);
        self
    }

    /// Write to `dlq` the records the drain cannot deliver, and with
    /// `when_full: dlq` the record a full spool cannot take. Without one the
    /// drain drops and counts them, and a full spool blocks.
    #[cfg(feature = "dlq")]
    #[must_use]
    pub fn with_dlq(self, dlq: Arc<crate::dlq::Dlq>) -> Self {
        self.dead_letters.set(dlq);
        self
    }

    /// Determine if we should attempt the hot path.
    async fn should_use_hot_path(&self) -> bool {
        // Observe the effective state for the gauge (side-effect-free).
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_circuit_state").set(match self.circuit.state().await {
            CircuitState::Closed => 0.0,
            CircuitState::HalfOpen => 1.0,
            CircuitState::Open => 2.0,
        });

        // Ordering gate first -- it has NO side effects, so a StrictFifo refusal
        // must not consume the breaker's single half-open probe permit.
        let ordering_ok = match self.config.ordering {
            OrderingMode::Interleaved => true,
            // Only use hot path if the spool is already drained.
            OrderingMode::StrictFifo => self.spool_count.load(AtomicOrdering::Relaxed) == 0,
        };
        if !ordering_ok {
            return false;
        }

        // Breaker gate LAST: this is the one call that may claim the half-open
        // probe, so we only take it when we are actually going to send.
        self.circuit.allow_request().await
    }

    /// Try to send via hot path (direct to the downstream sender, no encode).
    async fn try_hot_path(&self, record: &Record) -> Result<()> {
        let deadline = std::time::Instant::now() + self.config.send_timeout_duration();

        match self
            .sink
            .send_batch_by(std::slice::from_ref(record), deadline)
            .await
        {
            SendResult::Ok => {
                self.circuit.record_success().await;
                Ok(())
            }
            SendResult::FilteredDlq => {
                // The sink answered, so the breaker counts a success.
                self.circuit.record_success().await;
                Err(TieredSinkError::DeadLetter(
                    crate::transport::DeadLetterReason::Rejected.to_string(),
                ))
            }
            SendResult::Reject(e) => {
                // The sink answered, so the breaker counts a success.
                self.circuit.record_success().await;
                Err(TieredSinkError::DeadLetter(e.to_string()))
            }
            // Backpressured, a named wait, or the deadline elapsed: spool and
            // count toward the circuit so sustained failure trips it.
            SendResult::Backpressured | SendResult::RetryAfter(_) => {
                self.circuit.record_failure().await;
                #[cfg(feature = "metrics")]
                ::metrics::counter!("spool_circuit_trips_total").increment(1);
                Err(TieredSinkError::Spool("sink backpressured".into()))
            }
            SendResult::Fatal(e) => {
                // Fatal error - propagate, don't spool.
                Err(TieredSinkError::Sink(e.to_string()))
            }
        }
    }

    /// Apply the [`WhenFull`](crate::tiered_sink::WhenFull) policy to a record
    /// the full spool cannot take: `Some(Ok)` once it is shed or the DLQ holds
    /// it, `None` to block with `SpoolFull`, scalo's lossless default. `Dlq`
    /// with no DLQ set, or a DLQ write that is not confirmed, blocks too.
    async fn on_full(&self, record: &Record) -> Option<Result<()>> {
        use crate::tiered_sink::WhenFull;
        match self.config.when_full {
            WhenFull::DropNewest => {
                Self::record_overflow_drop();
                Some(Ok(()))
            }
            WhenFull::Block => None,
            WhenFull::Dlq => self.full_to_dlq(record).await,
        }
    }

    /// `when_full: dlq`: write the record to the DLQ, or block without one.
    ///
    /// A disabled DLQ, or one with no backend that can hold the record, blocks
    /// too, and so does a broker that refuses it for its size: the record only
    /// overflowed, so dropping it would lose a record the sink could still take.
    async fn full_to_dlq(&self, record: &Record) -> Option<Result<()>> {
        #[cfg(feature = "dlq")]
        if self.dead_letters.dlq().is_some() {
            let dead = drainer::Undeliverable {
                record: record.clone(),
                label: "spool_full",
                reason: "the tiered sink's spool is full".to_string(),
            };
            if !self.dead_letters.holds(&dead) {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    "spool full: when_full is dlq but the DLQ is disabled or cannot hold the \
                     record; applying block"
                );
                return None;
            }
            if !self
                .dead_letters
                .write(
                    vec![dead],
                    self.config.send_timeout_duration(),
                    drainer::OnRefusal::Keep,
                )
                .await
            {
                return None;
            }
            #[cfg(feature = "metrics")]
            ::metrics::counter!("tiered_sink_overflow_total", "policy" => "dlq").increment(1);
            return Some(Ok(()));
        }
        #[cfg(not(feature = "dlq"))]
        let _ = record;
        #[cfg(feature = "tracing")]
        tracing::warn!("spool full: when_full is dlq but no DLQ is set; applying block");
        None
    }

    /// Record a shed (dropped) record so overflow loss is never silent.
    fn record_overflow_drop() {
        #[cfg(feature = "metrics")]
        {
            ::metrics::counter!("tiered_sink_overflow_total", "policy" => "drop_newest")
                .increment(1);
            ::metrics::counter!("tiered_sink_dropped_total", "policy" => "drop_newest")
                .increment(1);
        }
    }

    /// Reserve capacity (`try_update`) before enqueue. A failure or a
    /// cancelled future gives it back. Atomic reservation prevents two
    /// concurrent callers from both passing the cap check and overshooting.
    async fn spool_message(&self, record: &Record) -> Result<()> {
        // Check disk availability first
        if !self.disk_available.load(AtomicOrdering::Relaxed) {
            return Err(TieredSinkError::DiskUnavailable);
        }

        // Serialise the WHOLE record (payload + key + headers + dedup) ONLY here,
        // on the cold path -- the happy path never pays this cost. The drainer
        // decodes it back so a replay keeps full routing/dedup fidelity. Then
        // frame with a CRC32C header (when enabled) so a torn write / bit-rot in
        // the spilled bytes is detectable on drain rather than replayed silently.
        let encoded = record.encode();
        let mut compressed = Vec::new();
        let started = Instant::now();
        self.compressors.compress_into(&encoded, &mut compressed)?;
        let compressing = started.elapsed();
        let written_ms = spool_codec::now_ms();
        let prefix = spool_codec::prefix(
            self.compressors.compression().codec,
            self.config.crc,
            written_ms,
            &compressed,
        );
        // What the entry takes on disk, its 4-byte header included.
        let compressed_len = (4 + prefix.len() + compressed.len()) as u64;

        // Reserve item slot.
        if let Some(max_items) = self.config.max_spool_items {
            let max_items_u64 = max_items as u64;
            if self
                .spool_count
                .try_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |cur| {
                    if cur < max_items_u64 {
                        Some(cur + 1)
                    } else {
                        None
                    }
                })
                .is_err()
            {
                // Spool full on item count -> apply the WhenFull policy.
                if let Some(handled) = self.on_full(record).await {
                    return handled;
                }
                return Err(TieredSinkError::SpoolFull(format!(
                    "max items {max_items} reached"
                )));
            }
        } else {
            self.spool_count.fetch_add(1, AtomicOrdering::AcqRel);
        }
        // From here every early return, and a cancel at either await below,
        // gives the reservation back.
        let mut reservation = SpoolReservation {
            count: &self.spool_count,
            bytes: &self.spool_bytes,
            len: 0,
            kept: false,
        };

        // Reserve byte budget, holding back one segment of read records.
        if let Some(max_bytes) = self.config.max_spool_bytes {
            let room = max_bytes.saturating_sub(self.segment_reserve);
            if let Err(current_bytes) = self.spool_bytes.try_update(
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
                |cur| cur.checked_add(compressed_len).filter(|new| *new <= room),
            ) {
                // Spool full on byte budget -> apply the WhenFull policy.
                if let Some(handled) = self.on_full(record).await {
                    return handled;
                }
                return Err(TieredSinkError::SpoolFull(format!(
                    "max spool bytes {max_bytes} reached (current: {current_bytes}, \
                     new message: {compressed_len})"
                )));
            }
        } else {
            self.spool_bytes
                .fetch_add(compressed_len, AtomicOrdering::AcqRel);
        }
        reservation.len = compressed_len;

        let mut spill = self.spill.lock().await;
        let written = if spill.sender.as_ref().is_some_and(Sender::rolls_next)
            || compressed.len() >= INLINE_WRITE_BYTES
        {
            // Starting a segment, or a large copy: on the blocking pool.
            drop(spill);
            let spill = Arc::clone(&self.spill);
            tokio::task::spawn_blocking(move || spill.blocking_lock().write(&prefix, &compressed))
                .await
                .map_err(|e| TieredSinkError::Spool(format!("spill task failed: {e}")))?
        } else {
            let written = spill.write(&prefix, &compressed);
            drop(spill);
            written
        };
        match written {
            Ok(()) => {}
            Err(e) if spool_codec::is_full(&e) => {
                // A full disk backpressures exactly as a full spool does.
                drop(reservation);
                if let Some(handled) = self.on_full(record).await {
                    return handled;
                }
                return Err(TieredSinkError::SpoolFull(format!("disk full: {e}")));
            }
            Err(e) => return Err(TieredSinkError::Spool(e.to_string())),
        }
        reservation.kept = true;
        // The first record into an empty spool is its oldest.
        let _ = self.oldest_ms.compare_exchange(
            0,
            written_ms,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Relaxed,
        );
        self.metrics.wrote(
            encoded.len(),
            usize::try_from(compressed_len).unwrap_or(usize::MAX),
            compressing,
        );
        if let Some(shared) = self.cell.lock().as_ref() {
            self.metrics.disk(shared.disk_bytes());
        }
        if self.config.sync_on_spill {
            self.sync().await?;
        }

        #[cfg(feature = "metrics")]
        {
            ::metrics::gauge!("spool_messages")
                .set(self.spool_count.load(AtomicOrdering::Relaxed) as f64);
            ::metrics::gauge!("spool_bytes")
                .set(self.spool_bytes.load(AtomicOrdering::Relaxed) as f64);
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(
            spool_items = self.spool_count.load(AtomicOrdering::Relaxed),
            spool_bytes = self.spool_bytes.load(AtomicOrdering::Relaxed),
            "Message spooled to disk"
        );

        Ok(())
    }

    /// Get the number of messages currently in the spool.
    #[allow(clippy::cast_possible_truncation)]
    pub async fn spool_len(&self) -> usize {
        self.spool_count.load(AtomicOrdering::Relaxed) as usize
    }

    /// Check if the spool is empty.
    pub async fn spool_is_empty(&self) -> bool {
        self.spool_count.load(AtomicOrdering::Relaxed) == 0
    }

    /// Get the approximate number of bytes currently in the spool.
    #[must_use]
    pub fn spool_bytes(&self) -> u64 {
        self.spool_bytes.load(AtomicOrdering::Relaxed)
    }

    /// What the spool holds: its records, their bytes as stored, and how long
    /// ago the oldest was spilled, as of the drain's last pass. Reads counters
    /// only, so it never touches the disk.
    #[must_use]
    pub fn spool_state(&self) -> SpoolState {
        let entries = self.spool_count.load(AtomicOrdering::Relaxed);
        SpoolState {
            entries,
            bytes: self.spool_bytes.load(AtomicOrdering::Relaxed),
            oldest_age: if entries == 0 {
                None
            } else {
                spool_codec::age(self.oldest_ms.load(AtomicOrdering::Acquire))
            },
        }
    }

    /// Whether the drain is held at a spilled entry the spill codec cannot
    /// read, such as one written under another codec. Readiness fails while
    /// it is. The entry and everything after it stay on disk, and the drain
    /// resumes once the sink reopens under the codec that wrote them.
    #[must_use]
    pub fn is_drain_held(&self) -> bool {
        self.drain_held.load(AtomicOrdering::Relaxed)
    }

    /// Check if disk is available for spooling.
    #[must_use]
    pub fn is_disk_available(&self) -> bool {
        self.disk_available.load(AtomicOrdering::Relaxed)
    }

    /// Get the current circuit breaker state.
    pub async fn circuit_state(&self) -> CircuitState {
        self.circuit.state().await
    }

    /// Get hot path message count.
    #[must_use]
    pub fn hot_path_count(&self) -> u64 {
        self.hot_path_count.load(AtomicOrdering::Relaxed)
    }

    /// Get cold path (spooled) message count.
    #[must_use]
    pub fn cold_path_count(&self) -> u64 {
        self.cold_path_count.load(AtomicOrdering::Relaxed)
    }

    /// Get a reference to the underlying sink.
    pub fn inner(&self) -> &S {
        &self.sink
    }

    /// Manually reset the circuit breaker.
    pub async fn reset_circuit(&self) {
        self.circuit.reset().await;
    }

    /// Stop the drain task and the disk poller, and wait for both to exit.
    ///
    /// A drain already in flight finishes its send and commit first, or is cut
    /// off at `send_timeout_ms` if the downstream never answers. Records still
    /// in the spill cache stay on disk and replay on the next open.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(drain) = self.drain_handle.take() {
            let _ = drain.await;
        }
        if let Some(poller) = self.disk_poller_handle.take() {
            let _ = poller.join().await;
        }
        // Its shutdown runs one last sync, after the drain's last commit.
        if let Some(ticker) = self.sync_ticker.take() {
            let _ = ticker.join().await;
        }
        self.shut_down = true;
    }
}

/// What a tiered sink's spool holds, from [`TieredSink::spool_state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolState {
    /// Records spilled and not yet delivered.
    pub entries: u64,
    /// Their bytes as stored, compressed and framed, each with its 4-byte header.
    pub bytes: u64,
    /// How long ago the oldest was spilled. `None` when the spool is empty.
    pub oldest_age: Option<Duration>,
}

/// One spill's item slot and `len` bytes of spool capacity, given back on drop
/// unless `kept` is set once the record is on the spool.
struct SpoolReservation<'a> {
    count: &'a AtomicU64,
    bytes: &'a AtomicU64,
    len: u64,
    kept: bool,
}

impl Drop for SpoolReservation<'_> {
    fn drop(&mut self) {
        if !self.kept {
            drainer::release(self.count, self.bytes, self.len);
        }
    }
}

/// Warns when the breaker's reset timeout is below the send timeout: an
/// unanswered probe is lost after the reset timeout, so a shorter one lets a
/// second probe join a slow send.
fn warn_on_short_reset(config: &TieredSinkConfig) {
    #[cfg(feature = "tracing")]
    if config.circuit_reset_timeout_ms < config.send_timeout_ms {
        tracing::warn!(
            circuit_reset_timeout_ms = config.circuit_reset_timeout_ms,
            send_timeout_ms = config.send_timeout_ms,
            "circuit_reset_timeout_ms is below send_timeout_ms, so a half-open probe still sending can be joined by a second"
        );
    }
    #[cfg(not(feature = "tracing"))]
    let _ = config;
}

/// Fails readiness while the drain holds at an entry its codec cannot read,
/// and degrades it while the drain is stalled or the disk is full.
#[cfg(feature = "health")]
fn drain_health(
    held: &Arc<std::sync::atomic::AtomicBool>,
    stalled: &Arc<std::sync::atomic::AtomicBool>,
    disk_full: Arc<std::sync::atomic::AtomicBool>,
) -> crate::health::HealthRegistration {
    let (held, stalled) = (Arc::clone(held), Arc::clone(stalled));
    crate::health::HealthRegistry::register_scoped("tiered_sink_drain", move || {
        if held.load(AtomicOrdering::Relaxed) {
            crate::health::HealthStatus::Unhealthy
        } else if stalled.load(AtomicOrdering::Relaxed) || disk_full.load(AtomicOrdering::Relaxed) {
            crate::health::HealthStatus::Degraded
        } else {
            crate::health::HealthStatus::Healthy
        }
    })
}

/// What [`open_spill`] opened.
type OpenedSpill = (Sender, yaque::Receiver, u64, u64, yaque::Options, u64, u64);

/// Opens the spill's queue on the blocking pool, and counts what it holds:
/// its entries, the bytes they take on disk, the options it opened with, the
/// bytes of `max_spool_bytes` held back for one segment of read records, and
/// when its front entry was written.
async fn open_spill(config: &TieredSinkConfig) -> std::result::Result<OpenedSpill, String> {
    // A segment's read records stay on disk until it goes, so that much of
    // the cap is held back, and the segment shrinks to a quarter of the cap.
    let segment_bytes = match config.max_spool_bytes {
        Some(max) => config.segment_bytes.min(max / 4).max(1),
        None => config.segment_bytes,
    };
    let segment_reserve = config.max_spool_bytes.map_or(0, |_| segment_bytes);
    let options = yaque::Options {
        segment_bytes,
        read_buffer_bytes: config.read_buffer_bytes,
        recycle_bytes: usize::try_from(config.drain_batch_max_bytes).unwrap_or(usize::MAX),
        whole: Some(spool_codec::whole),
        damaged_tail: Some(spool_codec::keep_damaged_tail),
        store: STORE,
    };
    // Walks the segments' headers and may cut a torn tail, so it runs off the runtime.
    let path = config.spool_path.clone();
    let policy = config.on_corruption;
    tokio::task::spawn_blocking(move || {
        let (sender, mut receiver) =
            spool_codec::open_queue(&path, policy, options).map_err(|e| e.to_string())?;
        let (entries, bytes) = receiver.queued().unwrap_or((0, 0));
        let front_ms = drainer::front_written_ms(&mut receiver);
        // The cap counts each entry's 4-byte header, as the disk does.
        Ok((
            sender,
            receiver,
            entries,
            bytes + 4 * entries,
            options,
            segment_reserve,
            front_ms,
        ))
    })
    .await
    .map_err(|e| format!("spool open task failed: {e}"))?
}

/// Check available disk space using `statvfs`.
///
/// Returns `(total_bytes, available_bytes)` for the filesystem containing `path`.
///
/// # Safety
///
/// Calls `libc::statvfs` which is unsafe but well-defined when given a valid path.
#[cfg(unix)]
#[allow(unsafe_code)]
fn check_disk_space(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    let c_path = CString::new(path.to_string_lossy().as_bytes()).ok()?;

    // SAFETY: zeroed statvfs is a valid initialisation for the struct
    // before passing to libc::statvfs which fills all fields.
    // c_path is a valid null-terminated C string pointing to an existing
    // filesystem path, and stat is a properly-sized statvfs struct.
    #[allow(unsafe_code)]
    let stat = unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        let result = libc::statvfs(c_path.as_ptr(), &raw mut stat);
        if result != 0 {
            return None;
        }
        stat
    };

    // Portability: libc::statvfs field widths differ across platforms.
    // Linux: f_blocks/f_bavail/f_frsize are all u64. macOS (aarch64): f_frsize
    // is c_ulong (u64) but f_blocks/f_bavail are fsblkcnt_t (u32), so only those
    // two need the widening cast here. f_frsize is already u64. Fixes #39.
    #[cfg(target_os = "macos")]
    {
        let block_size: u64 = stat.f_frsize;
        let total: u64 = u64::from(stat.f_blocks) * block_size;
        let available: u64 = u64::from(stat.f_bavail) * block_size;
        Some((total, available))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let block_size = stat.f_frsize;
        let total = stat.f_blocks * block_size;
        let available = stat.f_bavail * block_size;
        Some((total, available))
    }
}

/// Total and available bytes of the volume holding `path`, as this process's
/// quota sees them.
#[cfg(windows)]
#[allow(unsafe_code)]
fn check_disk_space(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    use windows::core::PCWSTR;

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
    let (mut available, mut total) = (0_u64, 0_u64);
    // SAFETY: `wide` is a NUL-terminated path that outlives the call, and both
    // out-pointers are to live locals.
    unsafe {
        GetDiskFreeSpaceExW(
            PCWSTR(wide.as_ptr()),
            Some(&raw mut available),
            Some(&raw mut total),
            None,
        )
    }
    .ok()?;
    Some((total, available))
}

/// Background poller that checks disk usage and sets a flag.
struct DiskCapacityPoller {
    spool_path: std::path::PathBuf,
    disk_available: Arc<std::sync::atomic::AtomicBool>,
    max_usage_percent: f64,
}

impl PeriodicTask for DiskCapacityPoller {
    async fn tick(&mut self) -> std::result::Result<(), TickError> {
        let path = self.spool_path.clone();
        let disk_space = tokio::task::spawn_blocking(move || check_disk_space(&path))
            .await
            .ok()
            .flatten();

        #[cfg(feature = "metrics")]
        if let Some((total, avail)) = disk_space {
            ::metrics::gauge!("spool_disk_available_bytes").set(avail as f64);
            ::metrics::gauge!("spool_disk_total_bytes").set(total as f64);
        }

        let available = disk_space.is_none_or(|(total, avail)| {
            if total == 0 {
                return true;
            }
            let used_ratio = 1.0 - (avail as f64 / total as f64);
            let ok = used_ratio < self.max_usage_percent;
            #[cfg(feature = "tracing")]
            if !ok {
                tracing::warn!(
                    used_percent = format!("{:.1}%", used_ratio * 100.0),
                    threshold = format!("{:.1}%", self.max_usage_percent * 100.0),
                    "Disk usage exceeds threshold, pausing spool writes"
                );
            }
            ok
        });

        self.disk_available
            .store(available, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(&mut self) -> std::result::Result<(), TickError> {
        #[cfg(feature = "tracing")]
        tracing::debug!("Disk capacity poller shutting down");
        Ok(())
    }
}

impl<S: TransportSender> Drop for TieredSink<S> {
    fn drop(&mut self) {
        // Drop can only cancel the background tasks, not await them. With records
        // still spooled and no completed `shutdown().await`, warn and count so the
        // skipped shutdown shows up in dashboards.
        let pending = self.spool_count.load(AtomicOrdering::Relaxed);
        if pending > 0 && !self.shut_down {
            #[cfg(feature = "metrics")]
            ::metrics::counter!("spool_dropped_without_shutdown_total").increment(1);
            #[cfg(feature = "tracing")]
            tracing::warn!(
                pending,
                "TieredSink dropped with spooled work pending -- call shutdown().await for durability"
            );
        }
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered_sink::DiskAwareConfig;
    use crate::transport::{
        PayloadFormat, RecordMeta, TransportBase, TransportError, TransportResult,
    };
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    /// Build a Record carrying `payload` (no key/headers) for tests.
    fn rec(payload: &[u8]) -> Record {
        Record {
            payload: bytes::Bytes::copy_from_slice(payload),
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    /// Poll until the drainer has emptied the spool and released its byte count. A fixed sleep flakes on a loaded runner.
    async fn wait_for_drain<S: TransportSender + 'static>(tiered: &TieredSink<S>) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !(tiered.spool_is_empty().await && tiered.spool_bytes() == 0) {
            assert!(
                std::time::Instant::now() < deadline,
                "drainer left {} records, {} bytes in the spool",
                tiered.spool_len().await,
                tiered.spool_bytes()
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// A real `TransportSender` test double: records what it receives and can be
    /// toggled unavailable to drive the spill/circuit/drain paths.
    struct TestSink {
        available: AtomicBool,
        /// When set, every send answers `Fatal` with this error instead.
        fatal: std::sync::Mutex<Option<fn() -> TransportError>>,
        received: Mutex<Vec<Record>>,
        /// `send_batch` calls that delivered, and the most records one carried.
        delivering_calls: std::sync::atomic::AtomicUsize,
        largest_call: std::sync::atomic::AtomicUsize,
        /// A payload answered `Fatal(Send)`, and the whole call it is in.
        refused: std::sync::Mutex<Option<&'static [u8]>>,
        /// A payload answered `FilteredDlq`, and the whole call it is in.
        dead_lettered: std::sync::Mutex<Option<&'static [u8]>>,
        /// A payload named in `dead_letter_reason`.
        screened: std::sync::Mutex<Option<&'static [u8]>>,
    }

    impl TestSink {
        fn new() -> Self {
            Self {
                available: AtomicBool::new(true),
                fatal: std::sync::Mutex::new(None),
                received: Mutex::new(Vec::new()),
                delivering_calls: std::sync::atomic::AtomicUsize::new(0),
                largest_call: std::sync::atomic::AtomicUsize::new(0),
                refused: std::sync::Mutex::new(None),
                dead_lettered: std::sync::Mutex::new(None),
                screened: std::sync::Mutex::new(None),
            }
        }

        fn dead_letter_payload(&self, payload: Option<&'static [u8]>) {
            *self.dead_lettered.lock().unwrap() = payload;
        }

        fn screen_payload(&self, payload: Option<&'static [u8]>) {
            *self.screened.lock().unwrap() = payload;
        }

        fn refuse_payload(&self, payload: Option<&'static [u8]>) {
            *self.refused.lock().unwrap() = payload;
        }

        fn answer_fatal(&self, error: Option<fn() -> TransportError>) {
            *self.fatal.lock().unwrap() = error;
        }

        fn fatal_answer(&self) -> Option<SendResult> {
            self.fatal
                .lock()
                .unwrap()
                .map(|error| SendResult::Fatal(error()))
        }

        fn set_available(&self, available: bool) {
            self.available.store(available, AtomicOrdering::SeqCst);
        }

        async fn received_count(&self) -> usize {
            self.received.lock().await.len()
        }

        /// Payloads received, in order -- lets tests assert content + ordering.
        async fn received_payloads(&self) -> Vec<Vec<u8>> {
            self.received
                .lock()
                .await
                .iter()
                .map(|r| r.payload.to_vec())
                .collect()
        }
    }

    impl crate::transport::TransportBase for TestSink {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            self.available.load(AtomicOrdering::SeqCst)
        }
        fn name(&self) -> &'static str {
            "test-sink"
        }

        fn delivery_guarantee(&self) -> crate::transport::DeliveryGuarantee {
            crate::transport::DeliveryGuarantee::Acknowledged
        }
    }

    impl TransportSender for TestSink {
        fn dead_letter_reason(
            &self,
            record: &Record,
        ) -> Option<crate::transport::DeadLetterReason> {
            let screened = *self.screened.lock().unwrap();
            (screened == Some(record.payload.as_ref()))
                .then_some(crate::transport::DeadLetterReason::OutboundFilter)
        }

        async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
            if let Some(fatal) = self.fatal_answer() {
                return fatal;
            }
            if self.available.load(AtomicOrdering::SeqCst) {
                self.received.lock().await.push(rec(&payload));
                SendResult::Ok
            } else {
                SendResult::Backpressured
            }
        }

        async fn send_batch(&self, records: &[Record]) -> SendResult {
            if let Some(fatal) = self.fatal_answer() {
                return fatal;
            }
            let refused = *self.refused.lock().unwrap();
            if refused.is_some_and(|bad| records.iter().any(|r| r.payload.as_ref() == bad)) {
                return SendResult::Fatal(TransportError::Send("refused".into()));
            }
            let dead = *self.dead_lettered.lock().unwrap();
            if self.available.load(AtomicOrdering::SeqCst)
                && dead.is_some_and(|bad| records.iter().any(|r| r.payload.as_ref() == bad))
            {
                let mut r = self.received.lock().await;
                r.extend(
                    records
                        .iter()
                        .filter(|r| Some(r.payload.as_ref()) != dead)
                        .cloned(),
                );
                return SendResult::FilteredDlq;
            }
            if self.available.load(AtomicOrdering::SeqCst) {
                self.delivering_calls.fetch_add(1, AtomicOrdering::SeqCst);
                self.largest_call
                    .fetch_max(records.len(), AtomicOrdering::SeqCst);
                let mut r = self.received.lock().await;
                r.extend(records.iter().cloned());
                SendResult::Ok
            } else {
                SendResult::Backpressured
            }
        }
    }

    #[tokio::test]
    async fn test_hot_path_when_available() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-queue");

        let sink = TestSink::new();
        let config = TieredSinkConfig::new(&spool_path);

        let tiered = TieredSink::new(sink, config).await.unwrap();

        tiered.send(&rec(b"hello")).await.unwrap();

        assert_eq!(tiered.hot_path_count(), 1);
        assert_eq!(tiered.cold_path_count(), 0);
        assert!(tiered.spool_is_empty().await);
        assert_eq!(tiered.inner().received_count().await, 1);

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_cold_path_when_unavailable() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-queue");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1; // Open circuit quickly

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // First message triggers circuit open
        tiered.send(&rec(b"hello")).await.unwrap();

        // Should have spooled
        assert_eq!(tiered.cold_path_count(), 1);
        assert!(!tiered.spool_is_empty().await);
        assert_eq!(tiered.inner().received_count().await, 0);

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_circuit_opens_after_failures() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-queue");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 3;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // First two fail but circuit stays closed
        tiered.send(&rec(b"1")).await.unwrap();
        tiered.send(&rec(b"2")).await.unwrap();
        assert_eq!(tiered.circuit_state().await, CircuitState::Closed);

        // Third failure opens circuit
        tiered.send(&rec(b"3")).await.unwrap();
        assert_eq!(tiered.circuit_state().await, CircuitState::Open);

        tiered.shutdown().await;
    }

    /// `shutdown()` does not wait out the drain's rate-limit pause. Paused
    /// time makes the pause the only clock `shutdown` could be waiting on.
    #[tokio::test(start_paused = true)]
    async fn shutdown_cuts_the_drain_rate_limit_pause_short() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.path().join("q"));
        config.drain_strategy = crate::tiered_sink::DrainStrategy::RateLimited { msgs_per_sec: 1 };
        let tiered = TieredSink::new(sink, config).await.unwrap();
        tiered.send(&rec(b"paced")).await.unwrap();
        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;

        // The drain has just taken a record, so it is in its 1 s pause.
        let started = tokio::time::Instant::now();
        tiered.shutdown().await;
        let waited = started.elapsed();
        assert!(
            waited < std::time::Duration::from_millis(500),
            "shutdown waited {waited:?} of the drain's 1 s pause"
        );
    }

    /// A spill cancelled while it waits on the spool writer leaves no item or
    /// byte reserved: a phantom would keep StrictFifo off the hot path and,
    /// repeated, refuse every spill as full.
    #[tokio::test]
    async fn a_cancelled_spill_releases_its_reservation() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.path().join("q"));
        config.max_spool_items = Some(1);
        config.max_spool_bytes = Some(1 << 20);
        let tiered = TieredSink::new(sink, config).await.unwrap();

        let writer = tiered.spill.lock().await;
        for i in 0..3 {
            let cancelled = tokio::time::timeout(
                std::time::Duration::from_millis(20),
                tiered.send(&rec(b"cancelled")),
            )
            .await;
            assert!(
                cancelled.is_err(),
                "spill {i} returned {cancelled:?} instead of waiting on the writer"
            );
        }
        drop(writer);
        assert_eq!(
            (tiered.spool_len().await, tiered.spool_bytes()),
            (0, 0),
            "cancelled spills left a reservation behind"
        );
        tiered
            .send(&rec(b"next"))
            .await
            .expect("the cap of 1 item still has room after three cancelled spills");
        tiered.shutdown().await;
    }

    /// Counters already short of what is on disk stop at zero when the drain
    /// commits, rather than wrapping to `u64::MAX`, which would keep
    /// StrictFifo's hot path shut and refuse every spill.
    #[tokio::test]
    async fn spool_counters_never_wrap_below_zero() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let tiered = TieredSink::new(sink, spilling_config(&dir.path().join("q")))
            .await
            .unwrap();
        tiered.send(&rec(b"uncounted")).await.unwrap();
        tiered.spool_count.store(0, AtomicOrdering::Relaxed);
        tiered.spool_bytes.store(0, AtomicOrdering::Relaxed);

        tiered.inner().set_available(true);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while tiered.inner().received_count().await == 0 {
            assert!(std::time::Instant::now() < deadline, "never drained");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(tiered.spool_len().await, 0);
        assert_eq!(tiered.spool_bytes(), 0);
        tiered.shutdown().await;
    }

    /// A spool on a size-limited volume is capped below the limit, and on
    /// Kubernetes `disk_aware` with no cap at all is refused: statvfs there
    /// reads the node's disk, so kubelet would evict the pod first.
    #[tokio::test]
    async fn a_spool_on_a_size_limited_volume_is_capped_below_the_limit() {
        const GI: u64 = 1_073_741_824;
        // name, volume limits, disk_aware, on Kubernetes, explicit cap, and the
        // cap the sink runs with (None: refused).
        type Case = (&'static str, Option<String>, bool, bool, Option<u64>);
        let dir = tempdir().unwrap();
        let volume = dir.path().display().to_string();
        let on_volume = Some(format!("/elsewhere=1Gi,{volume}=2Gi"));
        let cases: [(Case, Option<Option<u64>>); 6] = [
            (
                (
                    "capped from the volume",
                    on_volume.clone(),
                    true,
                    true,
                    None,
                ),
                Some(Some(2 * GI / 10 * 9)),
            ),
            (
                ("an explicit cap wins", on_volume, true, true, Some(5)),
                Some(Some(5)),
            ),
            (
                (
                    "another volume's limit",
                    Some("/elsewhere=1Gi".into()),
                    false,
                    true,
                    None,
                ),
                Some(None),
            ),
            (("refused on Kubernetes", None, true, true, None), None),
            (
                ("disk_aware with a cap", None, true, true, Some(7)),
                Some(Some(7)),
            ),
            (
                ("bare metal reads its own disk", None, true, false, None),
                Some(None),
            ),
        ];
        for ((name, limits, disk_aware, kubernetes, cap), want) in cases {
            let mut config = TieredSinkConfig::new(dir.path().join(format!("q-{}", name.len())));
            config.disk_aware = disk_aware.then(DiskAwareConfig::default);
            config.max_spool_bytes = cap;
            let built =
                TieredSink::with_volume_limits(TestSink::new(), config, limits, kubernetes).await;
            match (built, want) {
                (Ok(tiered), Some(want)) => {
                    assert_eq!(tiered.config.max_spool_bytes, want, "{name}");
                    tiered.shutdown().await;
                }
                (Err(TieredSinkError::SpoolOpen { message, .. }), None) => {
                    assert!(message.contains("max_spool_bytes"), "{name}: {message}");
                }
                (Ok(_), None) => panic!("{name}: built"),
                (Err(e), _) => panic!("{name}: {e}"),
            }
        }
    }

    /// A backlog drains in order, byte for byte, and on the default drain
    /// interval: 200 records at one per interval would take 20 s, twice the
    /// `wait_for_drain` deadline.
    #[tokio::test]
    async fn test_drain_recovers_messages() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-queue");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        assert_eq!(config.drain_interval_ms, 100, "the default interval");

        let tiered = TieredSink::new(sink, config).await.unwrap();

        let payloads: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("recover me {i}").into_bytes())
            .collect();
        for payload in &payloads {
            tiered.send(&rec(payload)).await.unwrap();
        }
        assert_eq!(tiered.spool_len().await, payloads.len());

        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;

        // The EXACT payloads survive the encode -> spool -> drain -> decode
        // round-trip, in the order they were spilled.
        assert_eq!(
            tiered.inner().received_payloads().await,
            payloads,
            "drained records must match what was spilled"
        );

        tiered.shutdown().await;
    }

    /// A backlog drains in batches: 1000 spooled records reach the sink in
    /// two `send_batch` calls at the default 500-record batch, in order.
    #[tokio::test]
    async fn a_backlog_drains_in_batches() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.path().join("q"));
        config.drain_strategy = crate::tiered_sink::DrainStrategy::Greedy;
        let tiered = TieredSink::new(sink, config).await.unwrap();
        let payloads: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("b-{i:04}").into_bytes())
            .collect();
        for payload in &payloads {
            tiered.send(&rec(payload)).await.unwrap();
        }

        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;
        let calls = tiered.inner().delivering_calls.load(AtomicOrdering::SeqCst);
        assert_eq!(
            calls, 2,
            "1000 records took {calls} delivering send_batch calls"
        );
        assert_eq!(tiered.inner().received_payloads().await, payloads);
        tiered.shutdown().await;
    }

    /// The byte cap bounds a pass by the bytes stored: records whose stored
    /// size lets three fit go three to a `send_batch`, and every one arrives.
    #[tokio::test]
    async fn a_drain_pass_takes_no_more_than_the_byte_cap() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.path().join("q"));
        config.drain_strategy = crate::tiered_sink::DrainStrategy::Greedy;
        config.compression = crate::compression::Compression::NONE;
        let payloads: Vec<Vec<u8>> = (0..10).map(|i| vec![b'0' + i; 1000]).collect();
        let encoded = rec(&payloads[0]).encode();
        let stored = (4 + crate::spool_codec::PREFIX_LEN + encoded.len()) as u64;
        config.drain_batch_max_bytes = stored * 3 + stored / 2;
        let tiered = TieredSink::new(sink, config).await.unwrap();
        for payload in &payloads {
            tiered.send(&rec(payload)).await.unwrap();
        }

        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;
        let sink = tiered.inner();
        assert_eq!(sink.largest_call.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(
            sink.delivering_calls.load(AtomicOrdering::SeqCst),
            4,
            "3 + 3 + 3 + 1"
        );
        assert_eq!(sink.received_payloads().await, payloads);
        tiered.shutdown().await;
    }

    /// A batch the sink refuses as the fault of a record it does not name is
    /// kept, then sent one record at a time, so only the refused record is
    /// dropped and the rest arrive in order.
    #[tokio::test]
    async fn a_refused_batch_drops_only_the_refused_record() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.path().join("q"));
        config.drain_strategy = crate::tiered_sink::DrainStrategy::Greedy;
        let tiered = TieredSink::new(sink, config).await.unwrap();
        for payload in [&b"good-1"[..], b"poison", b"good-2"] {
            tiered.send(&rec(payload)).await.unwrap();
        }

        tiered.inner().refuse_payload(Some(b"poison"));
        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;
        assert_eq!(
            tiered.inner().received_payloads().await,
            vec![b"good-1".to_vec(), b"good-2".to_vec()]
        );
        tiered.shutdown().await;
    }

    /// A spooled record the sink refuses with `Fatal` is dropped when the error
    /// is about that record, and kept when the sink itself is closed or
    /// misconfigured: dropping then would lose the whole spool to a wrong
    /// password.
    #[tokio::test]
    async fn a_fatal_drain_answer_drops_only_what_is_the_records_fault() {
        let cases: [(&str, fn() -> TransportError, bool); 3] = [
            (
                "record specific",
                || TransportError::Send("poison".into()),
                false,
            ),
            (
                "sink misconfigured",
                || TransportError::Config("credentials refused".into()),
                true,
            ),
            ("sink closed", || TransportError::Closed, true),
        ];
        for (name, fatal, kept) in cases {
            let dir = tempdir().unwrap();
            let sink = TestSink::new();
            sink.set_available(false);
            let tiered = TieredSink::new(sink, spilling_config(&dir.path().join("q")))
                .await
                .unwrap();
            tiered.send(&rec(b"spooled")).await.unwrap();
            assert_eq!(tiered.spool_len().await, 1, "{name}: record spooled");

            tiered.inner().answer_fatal(Some(fatal));
            tiered.inner().set_available(true);
            // The drainer retries every few ms, so a record it keeps is offered
            // again and again. Give it several attempts before judging.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            assert_eq!(
                tiered.spool_len().await,
                usize::from(kept),
                "{name}: spool after the sink answered Fatal"
            );

            if kept {
                tiered.inner().answer_fatal(None);
                wait_for_drain(&tiered).await;
                assert_eq!(
                    tiered.inner().received_payloads().await,
                    vec![b"spooled".to_vec()],
                    "{name}: the kept record is delivered once the sink is fixed"
                );
            }
            tiered.shutdown().await;
        }
    }

    #[tokio::test]
    async fn test_drain_preserves_record_key_and_dedup() {
        // Spill a record WITH a routing key + dedup key, drain it back, and
        // assert both survive -- this is the whole point of Record-native spill
        // (the old byte spill would have lost them).
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("fidelity-queue");

        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 10;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        let record = Record {
            payload: bytes::Bytes::from_static(b"body"),
            key: Some(std::sync::Arc::from("orders")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: Some(123),
                format: PayloadFormat::Json,
            },
        }
        .with_dedup_key("idem-9");
        tiered.send(&record).await.unwrap();
        assert_eq!(tiered.spool_len().await, 1);

        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;

        let got = tiered.inner().received.lock().await;
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].key.as_deref(),
            Some("orders"),
            "routing key survives"
        );
        assert_eq!(
            got[0].dedup_key(),
            Some(b"idem-9".as_slice()),
            "dedup key survives"
        );
        assert_eq!(got[0].metadata.timestamp_ms, Some(123), "metadata survives");
        drop(got);

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_multi_record_failover_drains_all_in_order() {
        // Full operational cycle with MANY records: downstream down -> all spill
        // -> downstream recovers -> drainer replays EVERY record, in FIFO order,
        // with exact content. Proves no loss + ordering across the spill cache.
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("multi-queue");

        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 5;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // Spill 20 records while the downstream is down.
        let expected: Vec<Vec<u8>> = (0..20)
            .map(|i| format!("rec-{i:02}").into_bytes())
            .collect();
        for payload in &expected {
            tiered.send(&rec(payload)).await.unwrap();
        }
        assert_eq!(tiered.spool_len().await, 20, "all spilled, none lost");
        assert_eq!(
            tiered.inner().received_count().await,
            0,
            "nothing reached the down sink"
        );

        // Recover and let the drainer work through the backlog.
        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;

        assert!(tiered.spool_is_empty().await, "drainer cleared the backlog");
        assert_eq!(
            tiered.inner().received_payloads().await,
            expected,
            "every record delivered exactly once, in FIFO order, content intact"
        );

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_crc_spill_survives_drain() {
        // CRC enabled on the spill: records spilled during an outage must
        // round-trip intact through the CRC frame on drain (no corruption, no loss).
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("crc-spill-queue");

        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = TieredSinkConfig::new(&spool_path).crc(true);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 5;
        let tiered = TieredSink::new(sink, config).await.unwrap();

        let expected: Vec<Vec<u8>> = (0..6).map(|i| format!("crc-{i}").into_bytes()).collect();
        for p in &expected {
            tiered.send(&rec(p)).await.unwrap();
        }
        assert!(tiered.spool_len().await > 0, "records spilled");

        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;
        assert!(tiered.spool_is_empty().await);
        assert_eq!(
            tiered.inner().received_payloads().await,
            expected,
            "CRC-framed spill round-trips intact through the drain"
        );
        tiered.shutdown().await;
    }

    /// A spilled entry damaged on disk is dropped, counted once and logged, and
    /// the drain delivers the entries on either side of it, in order.
    #[tokio::test]
    async fn a_damaged_entry_is_dropped_counted_and_the_drain_goes_on() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("damaged");
        let mut config = spilling_config(&path);
        config.compression = crate::compression::Compression::NONE;
        let records: Vec<Vec<u8>> = (0..3).map(|i| format!("record-{i}").into_bytes()).collect();
        let sink = TestSink::new();
        sink.set_available(false);
        let tiered = TieredSink::new(sink, config.clone()).await.unwrap();
        for record in &records {
            tiered.send(&rec(record)).await.unwrap();
        }
        tiered.shutdown().await;

        // One body byte of the middle entry, past its 4-byte header and its prefix.
        let entry = 4 + crate::spool_codec::PREFIX_LEN + rec(&records[1]).encode().len();
        let segment = path.join("0.q");
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes[entry + 4 + crate::spool_codec::PREFIX_LEN + 2] ^= 0xFF;
        std::fs::write(&segment, &bytes).unwrap();

        #[cfg(feature = "metrics")]
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        #[cfg(feature = "metrics")]
        let _metrics = ::metrics::set_default_local_recorder(&recorder);
        let tiered = TieredSink::new(TestSink::new(), config).await.unwrap();
        wait_for_drain(&tiered).await;
        assert_eq!(
            tiered.inner().received_payloads().await,
            vec![records[0].clone(), records[2].clone()],
            "the damaged entry is never delivered"
        );
        #[cfg(feature = "metrics")]
        assert!(
            recorder
                .handle()
                .render()
                .contains("spool_damaged_entries_total{store=\"tiered_sink\"} 1"),
            "counted once"
        );
        tiered.shutdown().await;
    }

    /// Each spilled entry names its codec, so a spill reopened under another
    /// codec, or holding runs of several, drains every entry in order and
    /// never holds.
    #[tokio::test]
    async fn a_codec_change_drains_every_entry_under_its_own_codec() {
        use crate::compression::{Codec, Compression};

        let rows: [(&[Codec], Codec); 4] = [
            (&[Codec::Zstd], Codec::Lz4),
            (&[Codec::None], Codec::Zstd),
            (&[Codec::Lz4], Codec::None),
            (&[Codec::Zstd, Codec::Lz4, Codec::None], Codec::Zstd),
        ];
        for (written, read) in rows {
            let case = format!("{written:?} read under {read}");
            let dir = tempdir().unwrap();
            let path = dir.path().join("codec-change");
            let under = |codec| {
                let mut config = spilling_config(&path);
                config.compression = Compression::new(codec);
                config
            };
            let mut expected = Vec::new();
            for (run, codec) in written.iter().enumerate() {
                let sink = TestSink::new();
                sink.set_available(false);
                let tiered = TieredSink::new(sink, under(*codec)).await.unwrap();
                for i in 0..3 {
                    let payload = format!("{codec}-{run}-{i}").into_bytes();
                    tiered.send(&rec(&payload)).await.unwrap();
                    expected.push(payload);
                }
                tiered.shutdown().await;
            }

            let tiered = TieredSink::new(TestSink::new(), under(read)).await.unwrap();
            wait_for_drain(&tiered).await;
            assert!(!tiered.is_drain_held(), "{case}");
            assert_eq!(
                tiered.inner().received_payloads().await,
                expected,
                "{case}: every record, in order"
            );
            tiered.shutdown().await;
        }
    }

    /// An entry naming a codec this build does not know holds the drain:
    /// the entries before it arrive, and it and every later one stay spooled.
    #[tokio::test]
    async fn an_entry_in_an_unknown_codec_holds_the_drain() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unknown-codec");
        spill(&path, &payloads(2)).await;
        {
            let options = yaque::Options {
                segment_bytes: 1 << 20,
                read_buffer_bytes: 1024,
                recycle_bytes: 1024,
                whole: None,
                damaged_tail: None,
                store: STORE,
            };
            let (mut sender, _receiver) = yaque::channel(&path, options).unwrap();
            let body = rec(b"unknown").encode();
            let prefix = spool_codec::tagged(0x0F, true, spool_codec::now_ms(), &body);
            sender.send(&prefix, &body).unwrap();
            let body = rec(b"after").encode();
            let prefix = spool_codec::prefix(
                crate::compression::Codec::None,
                true,
                spool_codec::now_ms(),
                &body,
            );
            sender.send(&prefix, &body).unwrap();
        }

        let tiered = TieredSink::new(TestSink::new(), spilling_config(&path))
            .await
            .unwrap();
        assert!(held_within(&tiered).await, "held at the unknown codec");
        assert_eq!(tiered.inner().received_payloads().await, payloads(2));
        assert_eq!(
            tiered.spool_len().await,
            2,
            "it and the entry after it stay"
        );
        tiered.shutdown().await;
    }

    /// Whether the drain holds within 5 s.
    async fn held_within(tiered: &TieredSink<TestSink>) -> bool {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !tiered.is_drain_held() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }

    #[tokio::test]
    async fn test_strict_fifo_waits_for_drain() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-queue");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.ordering = OrderingMode::StrictFifo;
        config.circuit_failure_threshold = 1;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // First message gets spooled due to unavailable sink
        tiered.send(&rec(b"first message")).await.unwrap();
        assert_eq!(tiered.spool_len().await, 1);

        // Make sink available again
        tiered.inner().set_available(true);
        tiered.reset_circuit().await;

        // In StrictFifo mode, new messages should spool while spool is non-empty
        tiered.send(&rec(b"new message")).await.unwrap();

        // Should still be 2 messages in spool (strict FIFO queues new messages behind old)
        assert_eq!(tiered.spool_len().await, 2);
        assert_eq!(tiered.cold_path_count(), 2);

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_max_spool_bytes_enforced() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-bytes-limit");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        // Set a very small byte limit
        config.max_spool_bytes = Some(64);

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // First message should spool (compressed size fits)
        tiered.send(&rec(b"small")).await.unwrap();
        assert_eq!(tiered.cold_path_count(), 1);
        assert!(tiered.spool_bytes() > 0);

        // Keep sending until we hit the limit
        let mut hit_limit = false;
        for _ in 0..100 {
            match tiered.send(&rec(b"more data here")).await {
                Ok(()) => {}
                Err(TieredSinkError::SpoolFull(msg)) => {
                    assert!(msg.contains("max spool bytes"));
                    hit_limit = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(hit_limit, "should have hit spool byte limit");

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_when_full_drop_newest_sheds_instead_of_erroring() {
        use crate::tiered_sink::WhenFull;
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-drop-newest");

        let sink = TestSink::new();
        sink.set_available(false); // force everything to spool

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.max_spool_bytes = Some(50); // tiny cap
        config.when_full = WhenFull::DropNewest;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // Send well past the cap. With DropNewest, NONE of these may surface a
        // SpoolFull error -- the overflow is shed (Ok) and counted, not errored.
        for _ in 0..200 {
            match tiered.send(&rec(b"more data here")).await {
                Ok(()) => {}
                Err(e) => panic!("DropNewest must shed, never error: {e}"),
            }
        }

        // The spool never exceeds the byte cap (overflow was dropped, not stored).
        assert!(
            tiered.spool_bytes() <= 50,
            "spool must stay within cap under DropNewest, got {}",
            tiered.spool_bytes()
        );

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_when_full_block_is_default_and_errors() {
        // Default policy (Block) must still surface SpoolFull (lossless
        // backpressure) -- guards against the new field changing the default.
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-block-default");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.max_spool_bytes = Some(50);
        // when_full left at default (Block).

        let tiered = TieredSink::new(sink, config).await.unwrap();

        let mut hit_limit = false;
        for _ in 0..200 {
            if let Err(TieredSinkError::SpoolFull(_)) = tiered.send(&rec(b"more data here")).await {
                hit_limit = true;
                break;
            }
        }
        assert!(
            hit_limit,
            "default Block policy must still error on spool full"
        );

        tiered.shutdown().await;
    }

    /// A `send` the full spool blocks waits for the drain: `Ok` once a pass
    /// frees room, in either ordering, and `SpoolFull` once its wait runs out or
    /// at once with no wait. Every accepted record reaches the sink, in order.
    #[tokio::test]
    async fn a_blocked_send_waits_for_the_drain() {
        let cases = [
            (OrderingMode::Interleaved, 5_000, true, true),
            (OrderingMode::StrictFifo, 5_000, true, true),
            (OrderingMode::StrictFifo, 300, false, false),
            (OrderingMode::Interleaved, 0, true, false),
        ];
        for (ordering, wait_ms, recovers, delivered) in cases {
            let case = format!("{ordering:?} wait {wait_ms} ms, sink recovers {recovers}");
            let dir = tempdir().unwrap();
            let sink = TestSink::new();
            sink.set_available(false);
            let mut config = TieredSinkConfig::new(dir.path().join("spill"));
            config.circuit_failure_threshold = 1;
            config.circuit_reset_timeout_ms = 50;
            config.drain_interval_ms = 10;
            config.max_spool_items = Some(1);
            config.ordering = ordering;
            config.when_full_wait_ms = wait_ms;
            let tiered = Arc::new(TieredSink::new(sink, config).await.unwrap());
            tiered.send(&rec(b"first")).await.unwrap();

            let started = std::time::Instant::now();
            let blocked = {
                let tiered = Arc::clone(&tiered);
                tokio::spawn(async move { tiered.send(&rec(b"second")).await })
            };
            if recovers {
                tokio::time::sleep(Duration::from_millis(100)).await;
                tiered.inner().set_available(true);
            }
            let sent = blocked.await.unwrap();
            if delivered {
                sent.unwrap_or_else(|e| panic!("{case}: {e}"));
                wait_for_drain(&tiered).await;
                assert_eq!(
                    tiered.inner().received_payloads().await,
                    vec![b"first".to_vec(), b"second".to_vec()],
                    "{case}"
                );
            } else {
                assert!(
                    matches!(sent, Err(TieredSinkError::SpoolFull(_))),
                    "{case}: {sent:?}"
                );
                assert!(
                    started.elapsed() >= Duration::from_millis(wait_ms),
                    "{case}"
                );
                assert_eq!(
                    tiered.spool_len().await,
                    1,
                    "{case}: the spooled record stays"
                );
            }
            Arc::try_unwrap(tiered)
                .map_err(|_| "outstanding Arc refs")
                .unwrap()
                .shutdown()
                .await;
        }
    }

    /// A send the full disk refused goes through as soon as the drain deletes a
    /// segment, while the full-disk backoff still has seconds to run, and well
    /// inside `when_full_wait_ms`. StrictFifo keeps it off the hot path while
    /// the spool holds records.
    #[tokio::test]
    async fn a_send_the_full_disk_refused_goes_through_once_the_drain_frees_a_segment() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = TieredSinkConfig::new(dir.path().join("spill"));
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 10;
        config.ordering = OrderingMode::StrictFifo;
        config.drain_strategy = crate::tiered_sink::DrainStrategy::RateLimited { msgs_per_sec: 10 };
        config.segment_bytes = 64;
        config.when_full_wait_ms = 1500;
        let tiered = Arc::new(TieredSink::new(sink, config).await.unwrap());
        for i in 0..40 {
            tiered
                .send(&rec(format!("r{i:02}").as_bytes()))
                .await
                .unwrap();
        }
        // The disk filled: the backoff now refuses writes for seconds.
        {
            let mut spill = tiered.spill.lock().await;
            for _ in 0..8 {
                spill
                    .full
                    .failed(Instant::now(), 0, &spool_codec::full_error());
            }
        }

        let started = Instant::now();
        let blocked = {
            let tiered = Arc::clone(&tiered);
            tokio::spawn(async move { tiered.send(&rec(b"late")).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        tiered.inner().set_available(true);
        let sent = blocked.await.unwrap();
        let waited = started.elapsed();
        assert!(sent.is_ok(), "{sent:?} after {waited:?}");
        assert!(waited < Duration::from_millis(1000), "took {waited:?}");
        assert!(
            tiered.spool_len().await > 0,
            "sent while the spool still held records"
        );
        Arc::try_unwrap(tiered)
            .map_err(|_| "outstanding Arc refs")
            .unwrap()
            .shutdown()
            .await;
    }

    /// `spool_state` and the age gauge report the oldest spilled record from
    /// its write time, after the first spill and after a reopen, and `None`
    /// once the spool is drained.
    #[tokio::test]
    async fn spool_state_reports_the_oldest_spilled_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state");
        let tiered = spilled(&path, &payloads(2)).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        let state = tiered.spool_state();
        assert_eq!((state.entries, state.bytes), (2, tiered.spool_bytes()));
        let aged = |state: SpoolState| {
            state
                .oldest_age
                .is_some_and(|age| age >= Duration::from_millis(60))
        };
        assert!(aged(state), "after the spill: {state:?}");
        tiered.shutdown().await;

        #[cfg(feature = "metrics")]
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        #[cfg(feature = "metrics")]
        let _metrics = ::metrics::set_default_local_recorder(&recorder);
        let sink = TestSink::new();
        sink.set_available(false);
        let tiered = TieredSink::new(sink, spilling_config(&path)).await.unwrap();
        assert!(
            aged(tiered.spool_state()),
            "after the reopen: {:?}",
            tiered.spool_state()
        );
        #[cfg(feature = "metrics")]
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let rendered = recorder.handle().render();
            let gauge = rendered
                .lines()
                .find_map(|line| {
                    line.strip_prefix("spool_oldest_age_seconds{store=\"tiered_sink\"} ")
                })
                .and_then(|value| value.parse::<f64>().ok());
            assert!(gauge.is_some_and(|age| age >= 0.06), "{rendered}");
        }
        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;
        assert_eq!(
            tiered.spool_state(),
            SpoolState {
                entries: 0,
                bytes: 0,
                oldest_age: None,
            }
        );
        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_spool_bytes_decremented_on_drain() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-bytes-drain");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 10;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // Spool some messages
        tiered.send(&rec(b"drain me")).await.unwrap();
        tiered.send(&rec(b"drain me too")).await.unwrap();
        let bytes_after_spool = tiered.spool_bytes();
        assert!(bytes_after_spool > 0);

        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;

        // Bytes should be decremented
        assert_eq!(tiered.spool_bytes(), 0);
        assert!(tiered.spool_is_empty().await);

        tiered.shutdown().await;
    }

    #[tokio::test]
    async fn test_spool_count_initialised_from_existing_queue() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-init-count");

        // Phase 1: Create a TieredSink, spool messages, then drop
        {
            let sink = TestSink::new();
            sink.set_available(false);

            let mut config = TieredSinkConfig::new(&spool_path);
            config.circuit_failure_threshold = 1;

            let tiered = TieredSink::new(sink, config).await.unwrap();

            tiered.send(&rec(b"message 1")).await.unwrap();
            tiered.send(&rec(b"message 2")).await.unwrap();
            tiered.send(&rec(b"message 3")).await.unwrap();
            assert_eq!(tiered.spool_len().await, 3);

            tiered.shutdown().await;
        }

        // Phase 2: Re-open -- spool_count should reflect existing items
        {
            let sink = TestSink::new();
            let config = TieredSinkConfig::new(&spool_path);
            let tiered = TieredSink::new(sink, config).await.unwrap();

            assert_eq!(tiered.spool_len().await, 3);
            assert!(tiered.spool_bytes() > 0);

            tiered.shutdown().await;
        }
    }

    #[tokio::test]
    async fn test_disk_available_flag() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-disk-flag");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // By default, disk should be available
        assert!(tiered.is_disk_available());

        // Manually set flag to false to simulate full disk
        tiered.disk_available.store(false, AtomicOrdering::Relaxed);

        let result = tiered.send(&rec(b"should fail")).await;
        assert!(matches!(result, Err(TieredSinkError::DiskUnavailable)));

        tiered.shutdown().await;
    }

    /// C15 regression: N concurrent senders against a small cap.
    /// Pre-fix overshot. Atomic reservation keeps total exact.
    #[tokio::test]
    async fn test_max_spool_items_not_overshooting_under_concurrency() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-toctou-items");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.max_spool_items = Some(10);

        let tiered = Arc::new(TieredSink::new(sink, config).await.unwrap());

        // Fan out 100 concurrent senders against a cap of 10.
        let mut joins = Vec::new();
        for _ in 0..100 {
            let t = Arc::clone(&tiered);
            joins.push(tokio::spawn(
                async move { t.send(&rec(b"contention")).await },
            ));
        }

        let mut accepted = 0usize;
        let mut rejected = 0usize;
        for j in joins {
            match j.await.unwrap() {
                Ok(()) => accepted += 1,
                Err(TieredSinkError::SpoolFull(_)) => rejected += 1,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(accepted, 10, "cap must not overshoot (got {accepted})");
        assert_eq!(rejected, 90);
        assert_eq!(tiered.spool_len().await, 10);

        let tiered = Arc::try_unwrap(tiered)
            .map_err(|_| "outstanding Arc refs")
            .unwrap();
        tiered.shutdown().await;
    }

    /// A record the sink will never take comes back to the caller as
    /// `DeadLetter`, never spooled: one it screens out, and one it answers
    /// `FilteredDlq` for. A `Fatal` keeps its cause.
    #[tokio::test]
    async fn a_record_the_sink_will_not_take_comes_back_as_a_dead_letter() {
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.screen_payload(Some(b"screened"));
        sink.dead_letter_payload(Some(b"refused"));
        let tiered = TieredSink::new(sink, TieredSinkConfig::new(dir.path().join("q")))
            .await
            .unwrap();

        for payload in [&b"screened"[..], b"refused"] {
            let sent = tiered.send(&rec(payload)).await;
            assert!(
                matches!(sent, Err(TieredSinkError::DeadLetter(_))),
                "{payload:?}: {sent:?}"
            );
        }
        assert_eq!(
            tiered.spool_len().await,
            0,
            "a dead letter is never spooled"
        );
        assert_eq!(tiered.inner().received_count().await, 0);

        tiered
            .inner()
            .answer_fatal(Some(|| TransportError::Send("poison".into())));
        let fatal = tiered.send(&rec(b"other")).await;
        assert!(
            matches!(&fatal, Err(TieredSinkError::Sink(cause)) if cause.contains("poison")),
            "{fatal:?}"
        );
        tiered.shutdown().await;
    }

    /// A file DLQ under `dir`, and the token that stops it.
    #[cfg(feature = "dlq")]
    fn file_dlq(dir: &std::path::Path) -> (Arc<crate::dlq::Dlq>, CancellationToken) {
        let config = crate::dlq::DlqConfig {
            file: crate::dlq::FileDlqConfig {
                enabled: true,
                path: dir.to_path_buf(),
                rotation: crate::dlq::RotationPeriod::Daily,
                max_age_days: 1,
                compress_rotated: crate::compression::Compression::NONE,
                ..crate::dlq::FileDlqConfig::default()
            },
            mode: crate::dlq::DlqMode::FileOnly,
            flush_interval_ms: 20,
            ..crate::dlq::DlqConfig::default()
        };
        let shutdown = CancellationToken::new();
        let dlq = crate::dlq::Dlq::spawn(&config, "svc", None, shutdown.clone()).unwrap();
        (Arc::new(dlq), shutdown)
    }

    /// The `reason` of every entry the file DLQ under `dir` holds.
    #[cfg(feature = "dlq")]
    async fn dlq_reasons(dir: &std::path::Path) -> Vec<String> {
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            crate::dlq::test_files::written_lines(&dir, "svc")
                .iter()
                .map(|line| {
                    let entry: serde_json::Value = serde_json::from_str(line).unwrap();
                    entry["reason"].as_str().unwrap_or_default().to_string()
                })
                .collect()
        })
        .await
        .unwrap()
    }

    /// The drain writes a spooled record the sink answers `FilteredDlq` for,
    /// and one it refuses with `Fatal`, to the DLQ with the cause, and
    /// delivers the rest.
    #[cfg(feature = "dlq")]
    #[tokio::test]
    async fn the_drain_writes_records_the_sink_will_not_take_to_the_dlq() {
        let dir = tempdir().unwrap();
        let (dlq, dlq_shutdown) = file_dlq(&dir.path().join("dlq"));
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.path().join("q"));
        config.drain_strategy = crate::tiered_sink::DrainStrategy::Greedy;
        let tiered = TieredSink::new(sink, config).await.unwrap().with_dlq(dlq);
        for payload in [&b"good"[..], b"dead", b"poison"] {
            tiered.send(&rec(payload)).await.unwrap();
        }

        tiered.inner().dead_letter_payload(Some(b"dead"));
        tiered.inner().refuse_payload(Some(b"poison"));
        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;

        assert_eq!(
            tiered.inner().received_payloads().await,
            vec![b"good".to_vec()]
        );
        let mut reasons = dlq_reasons(&dir.path().join("dlq")).await;
        reasons.sort();
        assert_eq!(reasons.len(), 2, "{reasons:?}");
        assert!(reasons.iter().any(|r| r.contains("refused")), "{reasons:?}");
        assert!(
            reasons.contains(&crate::transport::DeadLetterReason::Rejected.to_string()),
            "{reasons:?}"
        );
        tiered.shutdown().await;
        dlq_shutdown.cancel();
    }

    /// A full spool under `when_full: dlq`, with the record that overflows it.
    #[cfg(feature = "dlq")]
    async fn overflow_into(
        dir: &std::path::Path,
        dlq: Option<Arc<crate::dlq::Dlq>>,
        overflow: &[u8],
    ) -> (TieredSink<TestSink>, Result<()>) {
        let sink = TestSink::new();
        sink.set_available(false);
        let mut config = spilling_config(&dir.join("q"));
        config.max_spool_items = Some(1);
        config.when_full = crate::tiered_sink::WhenFull::Dlq;
        let tiered = TieredSink::new(sink, config).await.unwrap();
        let tiered = match dlq {
            Some(dlq) => tiered.with_dlq(dlq),
            None => tiered,
        };
        tiered.send(&rec(b"spooled")).await.unwrap();
        let result = tiered.send(&rec(overflow)).await;
        (tiered, result)
    }

    /// `when_full: dlq` writes the record a full spool cannot take to the DLQ,
    /// and blocks with `SpoolFull` when no DLQ is set or the DLQ is disabled,
    /// since nothing would store it.
    #[cfg(feature = "dlq")]
    #[tokio::test]
    async fn a_full_spool_writes_to_the_dlq_under_when_full_dlq() {
        for case in ["file", "none", "disabled"] {
            let dir = tempdir().unwrap();
            let (file, dlq_shutdown) = file_dlq(&dir.path().join("dlq"));
            let dlq = match case {
                "file" => Some(file),
                "disabled" => Some(Arc::new(crate::dlq::Dlq::disabled())),
                _ => None,
            };
            let (tiered, overflow) = overflow_into(dir.path(), dlq, b"overflow").await;
            if case == "file" {
                assert!(overflow.is_ok(), "{case}: {overflow:?}");
                assert_eq!(dlq_reasons(&dir.path().join("dlq")).await.len(), 1);
            } else {
                assert!(
                    matches!(overflow, Err(TieredSinkError::SpoolFull(_))),
                    "{case}: {overflow:?}"
                );
            }
            assert_eq!(tiered.spool_len().await, 1, "{case}");
            tiered.shutdown().await;
            dlq_shutdown.cancel();
        }
    }

    /// A record over the ceiling of every DLQ backend blocks rather than being
    /// dropped. No broker is needed: the ceiling check runs before any write.
    #[cfg(feature = "dlq-kafka")]
    #[tokio::test]
    async fn a_full_spool_blocks_a_record_no_dlq_backend_can_hold() {
        let mut kafka = crate::dlq::unreachable_broker();
        kafka.sizing.producer.message_max_bytes = Some(4_096);
        let config = crate::dlq::DlqConfig {
            mode: crate::dlq::DlqMode::KafkaOnly,
            file: crate::dlq::FileDlqConfig {
                enabled: false,
                ..crate::dlq::FileDlqConfig::default()
            },
            kafka: crate::dlq::KafkaDlqConfig {
                enabled: true,
                common_topic: Some("dlq.unreachable".to_string()),
                ..crate::dlq::KafkaDlqConfig::default()
            },
            ..crate::dlq::DlqConfig::default()
        };
        let shutdown = CancellationToken::new();
        let dlq = Arc::new(
            crate::dlq::Dlq::spawn(&config, "svc", Some(&kafka), shutdown.clone()).unwrap(),
        );
        let dir = tempdir().unwrap();
        let (tiered, overflow) =
            overflow_into(dir.path(), Some(Arc::clone(&dlq)), &[b'x'; 8_192]).await;
        assert!(
            matches!(overflow, Err(TieredSinkError::SpoolFull(_))),
            "{overflow:?}"
        );
        assert_eq!(tiered.spool_len().await, 1);
        tiered.shutdown().await;
        shutdown.cancel();
    }

    /// The drain writes two spooled dead letters to a Kafka-only DLQ whose
    /// topic refuses one for its size, which the producer's own ceiling
    /// passes. The refused one is dropped and counted, the other lands once,
    /// and both spool entries are committed rather than written again.
    #[cfg(all(feature = "dlq-kafka", feature = "transport-kafka-test-support"))]
    #[tokio::test]
    #[ignore = "needs Docker for a Kafka broker -- run: cargo nextest run --run-ignored all --ignore-default-filter"]
    async fn the_drain_drops_a_dead_letter_the_dlq_broker_refuses_for_good() {
        use crate::deployment::test_support::shared_service::Suite;
        use crate::transport::kafka::test_support::{KAFKA_PORT, require_docker, shared_kafka};
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::consumer::{BaseConsumer, Consumer};

        const SUITE: Suite = Suite {
            repo: "scalo-rs",
            manifest_dir: env!("CARGO_MANIFEST_DIR"),
            tmp_dir: None,
        };
        /// How long the drain may take to empty the spool against the broker.
        const DRAINED_WITHIN: std::time::Duration = std::time::Duration::from_secs(60);

        if !require_docker() {
            return;
        }
        let bootstrap = shared_kafka(SUITE).await.addr(KAFKA_PORT);
        let topic = format!("tiered.dlq.refused-p{}", std::process::id());
        let admin: AdminClient<rdkafka::client::DefaultClientContext> =
            rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
                .expect("admin client");
        let spec =
            NewTopic::new(&topic, 1, TopicReplication::Fixed(1)).set("max.message.bytes", "262144");
        for created in admin
            .create_topics(&[spec], &AdminOptions::new())
            .await
            .expect("create topics")
        {
            created.expect("topic created");
        }
        let config = crate::dlq::DlqConfig {
            mode: crate::dlq::DlqMode::KafkaOnly,
            file: crate::dlq::FileDlqConfig {
                enabled: false,
                ..crate::dlq::FileDlqConfig::default()
            },
            kafka: crate::dlq::KafkaDlqConfig {
                enabled: true,
                routing: crate::dlq::DlqRouting::Common,
                common_topic: Some(topic.clone()),
                // A first produce to a new topic can outlast the default ack wait.
                send_timeout_ms: 30_000,
                ..crate::dlq::KafkaDlqConfig::default()
            },
            ..crate::dlq::DlqConfig::default()
        };
        let kafka = crate::transport::KafkaConfig {
            brokers: vec![bootstrap.clone()],
            group: String::new(),
            ..Default::default()
        };
        let shutdown = CancellationToken::new();
        let dlq = Arc::new(
            crate::dlq::Dlq::spawn(&config, "svc", Some(&kafka), shutdown.clone()).unwrap(),
        );
        // Incompressible, so zstd leaves the capped, base64'd entry over the topic's ceiling.
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let oversize: &'static [u8] = Box::leak(
            (0..600_000)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state.to_le_bytes()[0]
                })
                .collect::<Vec<u8>>()
                .into_boxed_slice(),
        );
        let dir = tempdir().unwrap();
        let sink = TestSink::new();
        sink.set_available(false);
        let mut spill = spilling_config(&dir.path().join("q"));
        spill.drain_strategy = crate::tiered_sink::DrainStrategy::Greedy;
        let tiered = TieredSink::new(sink, spill)
            .await
            .unwrap()
            .with_dlq(Arc::clone(&dlq));
        tiered.send(&rec(b"held")).await.unwrap();
        tiered.send(&rec(oversize)).await.unwrap();

        // One pass dead-letters both: the screen names one, the sink answers the other FilteredDlq.
        tiered.inner().screen_payload(Some(oversize));
        tiered.inner().dead_letter_payload(Some(b"held"));
        tiered.inner().set_available(true);
        let deadline = std::time::Instant::now() + DRAINED_WITHIN;
        while !(tiered.spool_is_empty().await && tiered.spool_bytes() == 0) {
            assert!(
                std::time::Instant::now() < deadline,
                "the spool still holds {} records {DRAINED_WITHIN:?} on",
                tiered.spool_len().await
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(
            tiered.inner().received_payloads().await,
            Vec::<Vec<u8>>::new(),
            "both records were dead letters"
        );
        assert_eq!(dlq.dropped(), 1, "only the refused entry is lost");
        let landed = tokio::task::spawn_blocking(move || {
            let consumer: BaseConsumer = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .set("group.id", "tiered-dlq-refused-watermarks")
                .create()
                .expect("watermark client");
            consumer
                .fetch_watermarks(&topic, 0, std::time::Duration::from_secs(10))
                .expect("watermarks")
                .1
        })
        .await
        .unwrap();
        assert_eq!(landed, 1, "the held entry landed once");
        tiered.shutdown().await;
        dlq.shutdown().await.expect("DLQ shutdown");
    }

    /// The yaque lock files a spool directory can hold.
    const LOCKS: [&str; 2] = ["send.lock", "recv.lock"];

    /// Config that spills every record: the sink is down and one failure opens the circuit.
    fn spilling_config(path: &std::path::Path) -> TieredSinkConfig {
        let mut config = TieredSinkConfig::new(path);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 5;
        config
    }

    /// A sink at `path` with its downstream down and every one of `payloads` in its spool.
    async fn spilled(path: &std::path::Path, payloads: &[Vec<u8>]) -> TieredSink<TestSink> {
        let sink = TestSink::new();
        sink.set_available(false);
        let tiered = TieredSink::new(sink, spilling_config(path)).await.unwrap();
        for p in payloads {
            tiered.send(&rec(p)).await.unwrap();
        }
        assert_eq!(tiered.spool_len().await, payloads.len());
        tiered
    }

    /// Spill `payloads` into a spool at `path`, then close it cleanly.
    async fn spill(path: &std::path::Path, payloads: &[Vec<u8>]) {
        spilled(path, payloads).await.shutdown().await;
    }

    /// Reopen the spool at `path` against a healthy sink and wait for the drain.
    async fn reopen_and_drain(path: &std::path::Path, expect: usize) -> Vec<Vec<u8>> {
        let tiered = TieredSink::new(TestSink::new(), spilling_config(path))
            .await
            .unwrap();
        assert_eq!(
            tiered.spool_len().await,
            expect,
            "spooled records recovered"
        );
        wait_for_drain(&tiered).await;
        let got = tiered.inner().received_payloads().await;
        tiered.shutdown().await;
        got
    }

    fn payloads(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("rec-{i:02}").into_bytes()).collect()
    }

    /// How long a shutdown may take before the test calls it hung.
    const SHUTDOWN_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

    /// Spilling config with the disk poller on. Its 5 s poll keeps it parked on the shutdown signal.
    /// The byte cap keeps it valid when the tests run inside a Kubernetes pod, as CI runners do.
    fn disk_aware_config(path: &std::path::Path) -> TieredSinkConfig {
        spilling_config(path)
            .max_spool_bytes(64 * 1024 * 1024)
            .disk_aware(crate::tiered_sink::DiskAwareConfig {
                max_usage_percent: 1.0,
                poll_interval_secs: 5,
            })
    }

    /// Lets both background tasks reach their shutdown wait, the poller first, as in a running sink.
    async fn let_background_tasks_park() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn shutdown_returns_with_the_disk_poller_running() {
        let dir = tempdir().unwrap();
        let tiered = TieredSink::new(
            TestSink::new(),
            disk_aware_config(&dir.path().join("spool")),
        )
        .await
        .unwrap();
        let_background_tasks_park().await;
        tokio::time::timeout(SHUTDOWN_BOUND, tiered.shutdown())
            .await
            .expect("shutdown must return while the disk poller is running");
    }

    #[tokio::test]
    async fn drop_stops_the_drainer_and_the_disk_poller() {
        let dir = tempdir().unwrap();
        let mut tiered = TieredSink::new(
            TestSink::new(),
            disk_aware_config(&dir.path().join("spool")),
        )
        .await
        .unwrap();
        let drainer = tiered.drain_handle.take().unwrap();
        let poller = tiered.disk_poller_handle.take().unwrap();
        let_background_tasks_park().await;
        drop(tiered);
        tokio::time::timeout(SHUTDOWN_BOUND, async {
            drainer.await.unwrap();
            poller.join().await.unwrap();
        })
        .await
        .expect("dropping the sink must stop both background tasks");
    }

    /// What `Drop` reported on this thread: the drop counter and the WARN-level log text.
    #[cfg(all(feature = "metrics", feature = "logger"))]
    struct DropReport {
        metrics: metrics_exporter_prometheus::PrometheusHandle,
        logs: Arc<std::sync::Mutex<Vec<u8>>>,
    }

    #[cfg(all(feature = "metrics", feature = "logger"))]
    impl DropReport {
        fn drops_counted(&self) -> u64 {
            self.metrics
                .render()
                .lines()
                .find_map(|line| line.strip_prefix("spool_dropped_without_shutdown_total "))
                .map_or(0, |value| value.trim().parse().unwrap())
        }

        fn warned(&self) -> bool {
            String::from_utf8_lossy(&self.logs.lock().unwrap())
                .contains("dropped with spooled work pending")
        }
    }

    /// Runs `close` on a sink holding three spooled records, with this thread's recorder and subscriber capturing `Drop`.
    #[cfg(all(feature = "metrics", feature = "logger"))]
    async fn report_drop<F, Fut>(close: F) -> DropReport
    where
        F: FnOnce(TieredSink<TestSink>) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        use tracing_subscriber::layer::SubscriberExt as _;

        struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Logs {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        let logs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = Arc::clone(&logs);
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::WARN)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || Logs(Arc::clone(&writer))),
            );

        let dir = tempdir().unwrap();
        {
            let _metrics = ::metrics::set_default_local_recorder(&recorder);
            let _logs = tracing::subscriber::set_default(subscriber);
            let tiered = spilled(&dir.path().join("spool"), &payloads(3)).await;
            close(tiered).await;
        }
        DropReport { metrics, logs }
    }

    /// What a sink built from `config` logs at WARN on this thread while it starts and shuts down.
    #[cfg(feature = "logger")]
    async fn startup_warnings(config: TieredSinkConfig) -> String {
        use tracing_subscriber::layer::SubscriberExt as _;

        struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Logs {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let logs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = Arc::clone(&logs);
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::WARN)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || Logs(Arc::clone(&writer))),
            );
        {
            let _logs = tracing::subscriber::set_default(subscriber);
            let tiered = TieredSink::new(TestSink::new(), config).await.unwrap();
            tiered.shutdown().await;
        }
        String::from_utf8_lossy(&logs.lock().unwrap()).into_owned()
    }

    #[cfg(feature = "logger")]
    #[tokio::test]
    async fn a_reset_timeout_below_the_send_timeout_warns_at_startup() {
        let dir = tempdir().unwrap();
        let mut config = TieredSinkConfig::new(dir.path().join("spool"));
        config.send_timeout_ms = 2000;
        config.circuit_reset_timeout_ms = 500;

        let logs = startup_warnings(config).await;

        assert!(logs.contains("circuit_reset_timeout_ms=500"), "{logs}");
        assert!(logs.contains("send_timeout_ms=2000"), "{logs}");
    }

    #[cfg(feature = "logger")]
    #[tokio::test]
    async fn the_default_timeouts_do_not_warn_at_startup() {
        let dir = tempdir().unwrap();

        let logs = startup_warnings(TieredSinkConfig::new(dir.path().join("spool"))).await;

        assert!(!logs.contains("circuit_reset_timeout_ms"), "{logs}");
    }

    #[cfg(all(feature = "metrics", feature = "logger"))]
    #[tokio::test]
    async fn shutdown_with_records_spooled_is_not_reported_as_a_drop() {
        let report = report_drop(TieredSink::shutdown).await;
        assert_eq!(
            (report.drops_counted(), report.warned()),
            (0, false),
            "(drops counted, warned) after shutdown"
        );
    }

    #[cfg(all(feature = "metrics", feature = "logger"))]
    #[tokio::test]
    async fn dropping_a_sink_with_records_spooled_is_reported() {
        let report = report_drop(|tiered| async move { drop(tiered) }).await;
        assert_eq!(
            (report.drops_counted(), report.warned()),
            (1, true),
            "(drops counted, warned) after a drop without shutdown"
        );
    }

    #[tokio::test]
    async fn records_spilled_before_shutdown_are_replayed_after_it() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        let sink = TestSink::new();
        sink.set_available(false);
        let tiered = TieredSink::new(sink, disk_aware_config(&spool))
            .await
            .unwrap();
        for p in payloads(5) {
            tiered.send(&rec(&p)).await.unwrap();
        }
        assert_eq!(tiered.spool_len().await, 5);
        let_background_tasks_park().await;
        tokio::time::timeout(SHUTDOWN_BOUND, tiered.shutdown())
            .await
            .expect("shutdown must return with records spilled");
        assert_eq!(
            reopen_and_drain(&spool, 5).await,
            payloads(5),
            "every record accepted before shutdown is replayed after it"
        );
    }

    /// A downstream that takes the batch and never answers, as a peer that accepts the connection and stalls.
    struct HangingSink {
        inner: TestSink,
        hang: AtomicBool,
        /// `send_batch` calls that started while `hang` was set.
        hung_sends: AtomicU64,
        /// The most records one hung call carried.
        largest_hung: std::sync::atomic::AtomicUsize,
    }

    impl HangingSink {
        fn new() -> Self {
            Self {
                inner: TestSink::new(),
                hang: AtomicBool::new(false),
                hung_sends: AtomicU64::new(0),
                largest_hung: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl TransportBase for HangingSink {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            self.inner.is_healthy()
        }
        fn name(&self) -> &'static str {
            "hanging-sink"
        }

        fn delivery_guarantee(&self) -> crate::transport::DeliveryGuarantee {
            crate::transport::DeliveryGuarantee::Acknowledged
        }
    }

    impl TransportSender for HangingSink {
        async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
            self.inner.send(destination, payload).await
        }

        async fn send_batch(&self, records: &[Record]) -> SendResult {
            if self.hang.load(AtomicOrdering::SeqCst) {
                self.largest_hung
                    .fetch_max(records.len(), AtomicOrdering::SeqCst);
                self.hung_sends.fetch_add(1, AtomicOrdering::SeqCst);
                return std::future::pending().await;
            }
            self.inner.send_batch(records).await
        }
    }

    /// Spilling config with a send timeout short enough to cut a hung send off inside a test.
    fn hung_downstream_config(path: &std::path::Path) -> TieredSinkConfig {
        let mut config = spilling_config(path);
        config.send_timeout_ms = 200;
        config
    }

    /// Spill `payloads(n)` through a down `HangingSink`, then make it hang.
    async fn spill_then_hang(path: &std::path::Path, n: usize) -> TieredSink<HangingSink> {
        let sink = HangingSink::new();
        sink.inner.set_available(false);
        let tiered = TieredSink::new(sink, hung_downstream_config(path))
            .await
            .unwrap();
        for p in payloads(n) {
            tiered.send(&rec(&p)).await.unwrap();
        }
        assert_eq!(tiered.spool_len().await, n);
        tiered.inner().hang.store(true, AtomicOrdering::SeqCst);
        tiered
    }

    /// Poll until the drainer has started `at_least` hung sends. It runs on its own task.
    async fn wait_for_hung_sends(tiered: &TieredSink<HangingSink>, at_least: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while tiered.inner().hung_sends.load(AtomicOrdering::SeqCst) < at_least {
            assert!(
                std::time::Instant::now() < deadline,
                "drainer started {} hung sends, wanted {at_least}",
                tiered.inner().hung_sends.load(AtomicOrdering::SeqCst)
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn shutdown_returns_while_the_drainer_send_hangs() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        let tiered = spill_then_hang(&spool, 3).await;
        wait_for_hung_sends(&tiered, 1).await;

        tokio::time::timeout(SHUTDOWN_BOUND, tiered.shutdown())
            .await
            .expect("shutdown must return while a drain send to the downstream hangs");
        assert_eq!(
            reopen_and_drain(&spool, 3).await,
            payloads(3),
            "a send that never completed leaves its record spooled for the next open"
        );
    }

    #[tokio::test]
    async fn drainer_retries_a_record_whose_send_timed_out() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        let tiered = spill_then_hang(&spool, 3).await;

        // The second hung send is the first record offered again after its send was cut off.
        wait_for_hung_sends(&tiered, 2).await;
        assert_eq!(
            tiered.inner().largest_hung.load(AtomicOrdering::SeqCst),
            3,
            "the hung send carried the whole batch"
        );
        assert_eq!(
            tiered.spool_len().await,
            3,
            "a timed-out batch commits none of its records"
        );

        tiered.inner().hang.store(false, AtomicOrdering::SeqCst);
        tiered.inner().inner.set_available(true);
        wait_for_drain(&tiered).await;
        assert_eq!(
            tiered.inner().inner.received_payloads().await,
            payloads(3),
            "every record is delivered once the downstream answers, in order"
        );
        tiered.shutdown().await;
    }

    /// Entries of `dir` whose name starts with `prefix`.
    fn entries_with_prefix(dir: &std::path::Path, prefix: &str) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
            .map(|e| e.path())
            .collect()
    }

    /// The pid of a process that has exited and been reaped.
    #[cfg(unix)]
    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    /// Kills the child process on drop so a failing assert never leaks it.
    struct KillOnDrop(std::process::Child);

    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Env var that turns the dead-owner test into the child that gets SIGKILLed.
    const KILL_CHILD_DIR: &str = "SCALO_TEST_SPOOL_KILL_CHILD_DIR";

    #[tokio::test]
    async fn a_lock_left_by_a_dead_pid_does_not_block_reopen() {
        // Child half: spill, signal the parent, then wait to be killed with the spool open.
        if let Ok(dir) = std::env::var(KILL_CHILD_DIR) {
            let dir = std::path::PathBuf::from(dir);
            let sink = TestSink::new();
            sink.set_available(false);
            let tiered = TieredSink::new(sink, spilling_config(&dir.join("spool")))
                .await
                .unwrap();
            for p in payloads(5) {
                tiered.send(&rec(&p)).await.unwrap();
            }
            std::fs::write(dir.join("ready"), b"1").unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            drop(tiered);
            return;
        }

        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        let test_name = format!(
            "{}::a_lock_left_by_a_dead_pid_does_not_block_reopen",
            module_path!().split_once("::").unwrap().1
        );
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([test_name.as_str(), "--exact", "--nocapture"])
            .env(KILL_CHILD_DIR, dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut child = KillOnDrop(child);
        let child_pid = child.0.id();
        for _ in 0..3000 {
            if dir.path().join("ready").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(dir.path().join("ready").exists(), "child never spilled");

        // While the child lives, the OS lock it holds refuses a second open.
        let refused = TieredSink::new(TestSink::new(), spilling_config(&spool)).await;
        assert!(
            matches!(refused, Err(TieredSinkError::SpoolOpen { .. })),
            "a spool a live process holds must refuse the open"
        );
        assert!(
            entries_with_prefix(&spool, "corrupt-").is_empty(),
            "nothing quarantined"
        );

        // SIGKILL: no destructor runs, so both lock files stay behind.
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        for lock in LOCKS {
            let contents = std::fs::read_to_string(spool.join(lock)).unwrap();
            assert!(
                contents.starts_with(&format!("pid={child_pid}\n")),
                "{lock} left by the killed child: {contents:?}"
            );
        }

        assert_eq!(
            reopen_and_drain(&spool, 5).await,
            payloads(5),
            "every record spilled before the kill is replayed after the restart"
        );
        assert!(
            entries_with_prefix(&spool, "corrupt-").is_empty(),
            "nothing quarantined"
        );
        assert!(
            entries_with_prefix(dir.path(), "spool.corrupt-").is_empty(),
            "nothing quarantined"
        );
    }

    #[tokio::test]
    async fn an_empty_lock_file_is_treated_as_stale() {
        // A lock file's text names its owner for a person, and no open reads it.
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        spill(&spool, &payloads(3)).await;
        for lock in LOCKS {
            std::fs::write(spool.join(lock), b"").unwrap();
        }
        assert_eq!(reopen_and_drain(&spool, 3).await, payloads(3));

        // Nor one whose text names no owner.
        spill(&spool, &payloads(2)).await;
        std::fs::write(spool.join("send.lock"), b"pid=\ntoken=x").unwrap();
        assert_eq!(reopen_and_drain(&spool, 2).await, payloads(2));
        assert!(
            entries_with_prefix(&spool, "corrupt-").is_empty(),
            "nothing quarantined"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_stale_version_lock_does_not_hang_open() {
        // A version/lock an older build left is not a lock any open waits on.
        fn runtime() -> tokio::runtime::Runtime {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
        }
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        runtime().block_on(spill(&spool, &payloads(2)));
        let lock = spool.join("version").join("lock");
        std::fs::write(&lock, format!("pid={}\ntoken=7\n", dead_pid())).unwrap();

        // The open runs on its own thread so a spinning open fails the test instead of hanging it.
        let (tx, rx) = std::sync::mpsc::channel();
        let opener_spool = spool.clone();
        std::thread::spawn(move || {
            let drained = runtime().block_on(reopen_and_drain(&opener_spool, 2));
            let _ = tx.send(drained);
        });
        let drained = rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("open must not spin on a lock left by a dead process");
        assert_eq!(drained, payloads(2));
    }

    #[tokio::test]
    async fn a_live_lock_held_by_another_open_is_not_stolen() {
        // Two sinks on one spool path in one process: the second must fail, never quarantine.
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        let sink = TestSink::new();
        sink.set_available(false);
        let first = TieredSink::new(sink, spilling_config(&spool))
            .await
            .unwrap();
        for p in payloads(4) {
            first.send(&rec(&p)).await.unwrap();
        }

        let second = TieredSink::new(TestSink::new(), spilling_config(&spool)).await;
        match second {
            Err(TieredSinkError::SpoolOpen { message, .. }) => {
                assert!(message.contains("locked"), "names the lock: {message}");
            }
            Err(e) => panic!("expected SpoolOpen, got {e}"),
            Ok(_) => panic!("a second open of a live spool must be refused"),
        }
        assert!(
            entries_with_prefix(&spool, "corrupt-").is_empty(),
            "nothing quarantined"
        );
        assert!(
            entries_with_prefix(dir.path(), "spool.corrupt-").is_empty(),
            "nothing quarantined"
        );

        // The first owner still has every record and still drains them.
        assert_eq!(first.spool_len().await, 4);
        first.inner().set_available(true);
        wait_for_drain(&first).await;
        assert_eq!(first.inner().received_payloads().await, payloads(4));
        first.shutdown().await;
    }

    /// A spill reaches the operating system through a file the spill holds open,
    /// so it succeeds after the spool directory is deleted, and only a sync notices.
    /// `sync_on_spill` makes that sync part of accepting the record, and `sync`
    /// reports it on request.
    #[tokio::test]
    async fn sync_on_spill_does_not_accept_a_spill_it_cannot_sync() {
        for (sync_on_spill, accepted) in [(false, true), (true, false)] {
            let dir = tempdir().unwrap();
            let spool = dir.path().join("spool");
            let sink = TestSink::new();
            sink.set_available(false);
            let config = spilling_config(&spool).sync_on_spill(sync_on_spill);
            let tiered = TieredSink::new(sink, config).await.unwrap();

            tiered.send(&rec(b"first")).await.unwrap();
            tiered.sync().await.expect("a spill that can be synced");
            std::fs::remove_dir_all(&spool).unwrap();

            let second = tiered.send(&rec(b"second")).await;
            assert_eq!(
                second.is_ok(),
                accepted,
                "sync_on_spill {sync_on_spill}: {second:?}"
            );
            assert!(
                matches!(tiered.sync().await, Err(TieredSinkError::Spool(_))),
                "sync_on_spill {sync_on_spill}: an explicit sync reports it"
            );
            tiered.shutdown().await;
        }
    }

    /// yaque panics on a version file that is not a version or names another
    /// release, and on a `.q` file that is not a segment number, which ends a
    /// service built with `panic = "abort"`. `TieredSink::new` refuses each
    /// under `fail` and sets the queue aside under `quarantine`. A spill in
    /// another spool format is refused under both, and nothing is moved.
    #[tokio::test]
    async fn a_spill_queue_yaque_would_panic_on_is_an_open_failure() {
        let version = std::path::Path::new("version").join("contents");
        for (name, corruption, other_format) in [
            ("not a version", Some("not a version"), false),
            ("the format before", Some("0.6.6"), true),
            ("an older format", Some("0.5.2"), true),
            ("only whitespace", Some("\n"), false),
            ("not a segment", None, false),
        ] {
            let dir = tempdir().unwrap();
            let spool = dir.path().join("spool");
            spill(&spool, &payloads(2)).await;
            match corruption {
                Some(text) => std::fs::write(spool.join(&version), text).unwrap(),
                None => std::fs::write(spool.join("foo.q"), b"x").unwrap(),
            }

            let mut failing = spilling_config(&spool);
            failing.on_corruption = crate::spool_codec::CorruptionPolicy::Fail;
            let refused = TieredSink::new(TestSink::new(), failing).await;
            assert!(
                matches!(refused, Err(TieredSinkError::SpoolOpen { .. })),
                "{name}: fail refuses"
            );

            let opened = TieredSink::new(TestSink::new(), spilling_config(&spool)).await;
            if other_format {
                let Err(TieredSinkError::SpoolOpen { message, .. }) = opened else {
                    panic!("{name}: another format is refused under quarantine too");
                };
                assert!(message.contains("spool format"), "{name}: {message}");
                assert!(entries_with_prefix(&spool, "corrupt-").is_empty(), "{name}");
                continue;
            }
            let tiered =
                opened.unwrap_or_else(|e| panic!("{name}: quarantine opens a fresh queue: {e}"));
            assert_eq!(tiered.spool_len().await, 0, "{name}: a fresh queue");
            assert_eq!(
                entries_with_prefix(&spool, "corrupt-").len(),
                1,
                "{name}: the corrupt queue is kept"
            );
            tiered.shutdown().await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn quarantine_works_when_the_spool_path_is_a_mount_point() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        // Restores the parent's permissions so the tempdir can be removed.
        struct Writable(std::path::PathBuf);
        impl Drop for Writable {
            fn drop(&mut self) {
                let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
            }
        }

        let dir = tempdir().unwrap();
        let parent = dir.path().join("mnt");
        let spool = parent.join("spool");
        spill(&spool, &payloads(2)).await;
        // A segment that is a directory makes the sender refuse to open: real corruption.
        std::fs::create_dir(spool.join("99.q")).unwrap();
        let inode = std::fs::metadata(&spool).unwrap().ino();

        // A read-only parent makes rename(2) of the spool path fail, as it does on a mount point.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();
        let _restore = Writable(parent.clone());

        let tiered = TieredSink::new(TestSink::new(), spilling_config(&spool))
            .await
            .expect("a corrupt spool on a mount point is quarantined in place");
        assert_eq!(
            std::fs::metadata(&spool).unwrap().ino(),
            inode,
            "the spool path itself is never renamed"
        );
        let quarantined = entries_with_prefix(&spool, "corrupt-");
        assert_eq!(quarantined.len(), 1, "contents moved into one subdirectory");
        assert!(
            quarantined[0].join("recv-metadata").exists(),
            "the corrupt queue is preserved for forensics"
        );
        assert!(quarantined[0].join("0.q").exists());

        // The fresh queue in the same path spills and drains normally.
        tiered.inner().set_available(false);
        tiered.send(&rec(b"after")).await.unwrap();
        tiered.inner().set_available(true);
        wait_for_drain(&tiered).await;
        assert_eq!(
            tiered.inner().received_payloads().await,
            vec![b"after".to_vec()]
        );
        tiered.shutdown().await;
    }
}
