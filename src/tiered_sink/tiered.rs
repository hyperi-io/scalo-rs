// Project:   scalo
// File:      src/tiered_sink/tiered.rs
// Purpose:   TieredSink implementation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! TieredSink implementation.

use crate::tiered_sink::{
    CircuitBreaker, CircuitState, CompressionCodec, OrderingMode, Result, TieredSinkConfig,
    TieredSinkError, drainer,
};
use crate::transport::{Record, SendResult, TransportSender};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use yaque::{Receiver, Sender};

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
    spool_sender: Arc<Mutex<Sender>>,
    /// Receiver is owned by TieredSink but accessed via Arc clone by drainer task
    #[allow(dead_code)]
    spool_receiver: Arc<Mutex<Receiver>>,
    spool_count: Arc<AtomicU64>,
    spool_bytes: Arc<AtomicU64>,
    circuit: Arc<CircuitBreaker>,
    codec: CompressionCodec,
    config: TieredSinkConfig,
    /// Stops the drainer and the disk poller; every waiter sees the cancel, even one that waits after it.
    shutdown: CancellationToken,
    drain_handle: Option<JoinHandle<()>>,
    disk_available: Arc<std::sync::atomic::AtomicBool>,
    disk_poller_handle: Option<JoinHandle<()>>,
    /// Set once `shutdown` has joined both tasks, so its own drop of `self` is not reported as one.
    shut_down: bool,

    /// Serialises send + drain in `StrictFifo`. `None` otherwise.
    fifo_gate: Option<Arc<Mutex<()>>>,

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
    /// `TieredSinkConfig::on_corruption`).
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

        // Reads the process table and may wait out a lock mid-write, so it runs off the runtime.
        let open_path = config.spool_path.clone();
        let policy = config.on_corruption;
        let (sender, receiver) =
            tokio::task::spawn_blocking(move || crate::spool_codec::open_queue(&open_path, policy))
                .await
                .map_err(|e| spool_open_err(format!("spool open task failed: {e}")))?
                .map_err(|e| spool_open_err(e.to_string()))?;

        let sink = Arc::new(sink);
        let spool_sender = Arc::new(Mutex::new(sender));
        let spool_receiver = Arc::new(Mutex::new(receiver));

        // Recover counters in spawn_blocking -- segment
        // files can be GB-sized after a crash; walking them on the
        // async runtime pins a tokio worker.
        let spool_path_for_scan = config.spool_path.clone();
        let (initial_count, initial_bytes) = tokio::task::spawn_blocking(move || {
            crate::spool_codec::queued_items_and_bytes(&spool_path_for_scan)
        })
        .await
        .ok()
        .and_then(std::result::Result::ok)
        .unwrap_or((0, 0));
        let spool_count = Arc::new(AtomicU64::new(initial_count));
        let spool_bytes = Arc::new(AtomicU64::new(initial_bytes));

        // An unanswered probe is lost after the reset timeout, so a shorter one lets a second probe join a slow send.
        #[cfg(feature = "tracing")]
        if config.circuit_reset_timeout_ms < config.send_timeout_ms {
            tracing::warn!(
                circuit_reset_timeout_ms = config.circuit_reset_timeout_ms,
                send_timeout_ms = config.send_timeout_ms,
                "circuit_reset_timeout_ms is below send_timeout_ms, so a half-open probe still sending can be joined by a second"
            );
        }
        let circuit = Arc::new(CircuitBreaker::new(
            config.circuit_failure_threshold,
            config.circuit_reset_timeout(),
        ));
        let shutdown = CancellationToken::new();
        let codec = config.compression;
        let disk_available = Arc::new(std::sync::atomic::AtomicBool::new(true));

        // Shared by senders + drainer in StrictFifo to enforce
        // total ordering at the sink.
        let fifo_gate =
            matches!(config.ordering, OrderingMode::StrictFifo).then(|| Arc::new(Mutex::new(())));

        // Start disk-aware capacity poller if configured
        let disk_poller_handle = config.disk_aware.as_ref().map(|disk_cfg| {
            let spool_path = config.spool_path.clone();
            let disk_flag = Arc::clone(&disk_available);
            let shutdown_clone = shutdown.clone();
            let poll_interval = std::time::Duration::from_secs(disk_cfg.poll_interval_secs);
            let max_usage = disk_cfg.max_usage_percent;

            tokio::spawn(disk_capacity_poller(
                spool_path,
                disk_flag,
                max_usage,
                poll_interval,
                shutdown_clone,
            ))
        });

        // Start drain task
        let drain_handle = tokio::spawn(drainer::drain_loop(
            Arc::clone(&sink),
            Arc::clone(&spool_receiver),
            Arc::clone(&spool_count),
            Arc::clone(&spool_bytes),
            Arc::clone(&circuit),
            codec,
            config.crc,
            config.drain_strategy,
            config.drain_interval(),
            config.send_timeout_duration(),
            drainer::DrainBatch {
                max_records: config.drain_batch_max_records,
                max_bytes: config.drain_batch_max_bytes,
            },
            shutdown.clone(),
            fifo_gate.as_ref().map(Arc::clone),
        ));

        Ok(Self {
            sink,
            spool_sender,
            spool_receiver,
            spool_count,
            spool_bytes,
            circuit,
            codec,
            config,
            shutdown,
            drain_handle: Some(drain_handle),
            disk_available,
            disk_poller_handle,
            shut_down: false,
            fifo_gate,
            hot_path_count: AtomicU64::new(0),
            cold_path_count: AtomicU64::new(0),
        })
    }

    /// Send a message through the tiered sink.
    ///
    /// The message goes through the hot path (direct to sink) when the circuit
    /// breaker admits it (closed, or the single half-open probe) and, in
    /// StrictFifo, the spool is empty. A `Backpressured` answer or a send past
    /// `send_timeout_ms` spools it instead, as does a refused hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The sink answers `Fatal` ([`TieredSinkError::Sink`])
    /// - The spool is full and `when_full` is `block`, `dlq` or `drop_oldest`
    ///   ([`TieredSinkError::SpoolFull`])
    /// - The `disk_aware` threshold is crossed ([`TieredSinkError::DiskUnavailable`])
    /// - Compression or the spool write fails
    pub async fn send(&self, record: &Record) -> Result<()> {
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
                Err(TieredSinkError::Sink(_)) => {
                    // Fatal error, don't spool
                    return Err(TieredSinkError::Sink("fatal sink error".into()));
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
        let send_timeout = self.config.send_timeout_duration();

        match timeout(
            send_timeout,
            self.sink.send_batch(std::slice::from_ref(record)),
        )
        .await
        {
            Ok(SendResult::Ok | SendResult::FilteredDlq) => {
                self.circuit.record_success().await;
                Ok(())
            }
            Ok(SendResult::Backpressured) => {
                // Downstream backpressured/unavailable: spool and count toward
                // the circuit so sustained failure trips it.
                self.circuit.record_failure().await;
                #[cfg(feature = "metrics")]
                ::metrics::counter!("spool_circuit_trips_total").increment(1);
                Err(TieredSinkError::Spool("sink backpressured".into()))
            }
            Ok(SendResult::Fatal(e)) => {
                // Fatal error - propagate, don't spool.
                Err(TieredSinkError::Sink(e.to_string()))
            }
            Err(_timeout) => {
                self.circuit.record_failure().await;
                #[cfg(feature = "metrics")]
                ::metrics::counter!("spool_circuit_trips_total").increment(1);
                Err(TieredSinkError::Spool("send timeout".into()))
            }
        }
    }

    /// Decide the action for a spool-full event per the configured
    /// [`WhenFull`](crate::tiered_sink::WhenFull) policy.
    ///
    /// Returns `true` to DROP the incoming record (shed and continue), `false`
    /// to BLOCK (return `SpoolFull` so the caller backpressures the inbound
    /// source -- scalo's lossless default).
    ///
    /// `Dlq` and `DropOldest` are not yet wired (they need a DLQ handle and
    /// oldest-eviction access respectively); until then they degrade to the
    /// SAFE, lossless `Block` with a warning -- never silent loss.
    fn drop_on_full(&self) -> bool {
        use crate::tiered_sink::WhenFull;
        match self.config.when_full {
            WhenFull::DropNewest => true,
            WhenFull::Block => false,
            WhenFull::Dlq | WhenFull::DropOldest => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    policy = ?self.config.when_full,
                    "spool full: policy not yet wired (needs DLQ handle / oldest-eviction); \
                     applying Block (lossless backpressure)"
                );
                false
            }
        }
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

    /// Reserve capacity (`try_update`) before enqueue; a failure or a
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
        let compressed = self.codec.compress(&encoded)?;
        let compressed = crate::spool_codec::frame(self.config.crc, compressed);
        let compressed_len = compressed.len() as u64;

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
                if self.drop_on_full() {
                    Self::record_overflow_drop();
                    return Ok(());
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

        // Reserve byte budget.
        if let Some(max_bytes) = self.config.max_spool_bytes {
            if let Err(current_bytes) = self.spool_bytes.try_update(
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
                |cur| {
                    cur.checked_add(compressed_len)
                        .filter(|new| *new <= max_bytes)
                },
            ) {
                // Spool full on byte budget -> apply the WhenFull policy.
                if self.drop_on_full() {
                    Self::record_overflow_drop();
                    return Ok(());
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

        let mut sender = self.spool_sender.lock().await;
        sender
            .send(compressed)
            .await
            .map_err(|e| TieredSinkError::Spool(e.to_string()))?;
        drop(sender);
        reservation.kept = true;

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
        let handles = [self.drain_handle.take(), self.disk_poller_handle.take()];
        for handle in handles.into_iter().flatten() {
            let _ = handle.await;
        }
        self.shut_down = true;
    }
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

/// Check available disk space using `statvfs`.
///
/// Returns `(total_bytes, available_bytes)` for the filesystem containing `path`.
///
/// # Safety
///
/// Calls `libc::statvfs` which is unsafe but well-defined when given a valid path.
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
    // two need the widening cast here; f_frsize is already u64. Fixes #39.
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

/// Background poller that checks disk usage and sets a flag.
async fn disk_capacity_poller(
    spool_path: std::path::PathBuf,
    disk_available: Arc<std::sync::atomic::AtomicBool>,
    max_usage_percent: f64,
    poll_interval: std::time::Duration,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                #[cfg(feature = "tracing")]
                tracing::debug!("Disk capacity poller shutting down");
                return;
            }
            () = tokio::time::sleep(poll_interval) => {}
        }

        let disk_space = check_disk_space(&spool_path);

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
            let ok = used_ratio < max_usage_percent;
            #[cfg(feature = "tracing")]
            if !ok {
                tracing::warn!(
                    used_percent = format!("{:.1}%", used_ratio * 100.0),
                    threshold = format!("{:.1}%", max_usage_percent * 100.0),
                    "Disk usage exceeds threshold, pausing spool writes"
                );
            }
            ok
        });

        disk_available.store(available, std::sync::atomic::Ordering::Relaxed);
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

    /// Poll until the drainer has emptied the spool and released its byte count; a fixed sleep flakes on a loaded runner.
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
            }
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
    }

    impl TransportSender for TestSink {
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

        let writer = tiered.spool_sender.lock().await;
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
        config.compression = CompressionCodec::None;
        let payloads: Vec<Vec<u8>> = (0..10).map(|i| vec![b'0' + i; 1000]).collect();
        let stored = crate::spool_codec::frame(config.crc, rec(&payloads[0]).encode()).len() as u64;
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

    #[tokio::test]
    async fn test_crc_drops_corrupt_spilled_record() {
        // CRC enabled: a corrupt spilled record must be DROPPED on drain (logged),
        // never replayed as garbage downstream.
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("crc-corrupt-queue");

        // Spill one record with the sink down + compression off (predictable
        // on-disk layout), then shut down to flush.
        {
            let sink = TestSink::new();
            sink.set_available(false);
            let mut config = TieredSinkConfig::new(&spool_path).crc(true);
            config.compression = CompressionCodec::None;
            config.circuit_failure_threshold = 1;
            let tiered = TieredSink::new(sink, config).await.unwrap();
            tiered.send(&rec(b"poison record bytes")).await.unwrap();
            assert_eq!(tiered.spool_len().await, 1);
            tiered.shutdown().await;
        }

        // Flip a byte in the CRC-covered region (past the 4-byte queue header +
        // 4-byte CRC header) so the checksum must reject it.
        let seg = spool_path.join("0.q");
        let mut bytes = std::fs::read(&seg).unwrap();
        bytes[8] ^= 0xFF;
        std::fs::write(&seg, &bytes).unwrap();

        // Reopen with the sink available; the corrupt record must be dropped, not
        // delivered, and the spool must end up empty.
        let sink = TestSink::new();
        let mut config = TieredSinkConfig::new(&spool_path).crc(true);
        config.compression = CompressionCodec::None;
        config.drain_interval_ms = 5;
        let tiered = TieredSink::new(sink, config).await.unwrap();
        wait_for_drain(&tiered).await;
        assert!(
            tiered.spool_is_empty().await,
            "corrupt record was drained (dropped)"
        );
        assert_eq!(
            tiered.inner().received_count().await,
            0,
            "a corrupt record must NEVER be delivered"
        );
        tiered.shutdown().await;
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
        config.max_spool_bytes = Some(50);

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
    /// Pre-fix overshot; atomic reservation keeps total exact.
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

    /// Spilling config with the disk poller on; its 5 s poll keeps it parked on the shutdown signal.
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
            poller.await.unwrap();
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

    /// Poll until the drainer has started `at_least` hung sends; it runs on its own task.
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
    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    /// Backdate a lock file past the window in which its owner could still be writing it.
    fn backdate(lock: &std::path::Path) {
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        std::fs::File::options()
            .write(true)
            .open(lock)
            .unwrap()
            .set_modified(old)
            .unwrap();
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
        // A kill between the lock's create and its write leaves an empty file.
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        spill(&spool, &payloads(3)).await;
        for lock in LOCKS {
            std::fs::write(spool.join(lock), b"").unwrap();
            backdate(&spool.join(lock));
        }
        assert_eq!(reopen_and_drain(&spool, 3).await, payloads(3));

        // A lock with no parseable owner is stale too.
        spill(&spool, &payloads(2)).await;
        std::fs::write(spool.join("send.lock"), b"pid=\ntoken=x").unwrap();
        backdate(&spool.join("send.lock"));
        assert_eq!(reopen_and_drain(&spool, 2).await, payloads(2));
        assert!(
            entries_with_prefix(&spool, "corrupt-").is_empty(),
            "nothing quarantined"
        );
    }

    #[test]
    fn a_stale_version_lock_does_not_hang_open() {
        // yaque spins on version/lock without a timeout, so a leftover one hangs every open.
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

    #[tokio::test]
    async fn a_lock_held_by_a_live_process_is_not_stolen() {
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        spill(&spool, &payloads(3)).await;
        let owner = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let owner = KillOnDrop(owner);
        std::fs::write(
            spool.join("recv.lock"),
            format!("pid={}\ntoken=42\n", owner.0.id()),
        )
        .unwrap();

        let reopened = TieredSink::new(TestSink::new(), spilling_config(&spool)).await;
        assert!(
            matches!(reopened, Err(TieredSinkError::SpoolOpen { .. })),
            "a lock whose owner is alive must refuse the open"
        );
        assert!(
            spool.join("recv.lock").exists(),
            "the live owner's lock stays"
        );
        assert!(
            entries_with_prefix(&spool, "corrupt-").is_empty(),
            "nothing quarantined"
        );

        // Once the owner is gone the same spool opens with every record intact.
        drop(owner);
        assert_eq!(reopen_and_drain(&spool, 3).await, payloads(3));
    }

    #[tokio::test]
    async fn a_lock_being_written_is_not_mistaken_for_stale() {
        // An empty lock younger than the write window is re-read before it is judged.
        let dir = tempdir().unwrap();
        let spool = dir.path().join("spool");
        spill(&spool, &payloads(1)).await;
        let owner = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let owner = KillOnDrop(owner);
        let lock = spool.join("send.lock");
        std::fs::write(&lock, b"").unwrap();
        let owner_pid = owner.0.id();
        let writer = {
            let lock = lock.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(100));
                std::fs::write(&lock, format!("pid={owner_pid}\ntoken=9\n")).unwrap();
            })
        };

        let reopened = TieredSink::new(TestSink::new(), spilling_config(&spool)).await;
        writer.join().unwrap();
        assert!(
            matches!(reopened, Err(TieredSinkError::SpoolOpen { .. })),
            "the owner finished writing its lock, so the open is refused"
        );
        assert!(lock.exists());
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
        // A truncated receiver position makes the queue refuse to open: real corruption.
        std::fs::write(spool.join("recv-metadata"), [0u8; 3]).unwrap();
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
