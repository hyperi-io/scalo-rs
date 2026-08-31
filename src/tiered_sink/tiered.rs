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
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;
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
    shutdown: Arc<Notify>,
    drain_handle: Option<JoinHandle<()>>,
    disk_available: Arc<std::sync::atomic::AtomicBool>,
    #[allow(dead_code)]
    disk_poller_handle: Option<JoinHandle<()>>,

    /// Serialises send + drain in `StrictFifo`. `None` otherwise.
    fifo_gate: Option<Arc<Mutex<()>>>,

    // Metrics
    hot_path_count: AtomicU64,
    cold_path_count: AtomicU64,
}

impl<S: TransportSender + 'static> TieredSink<S> {
    /// Create a new TieredSink wrapping the given sender.
    ///
    /// # Errors
    ///
    /// Returns an error if the spool file cannot be opened.
    pub async fn new(sink: S, config: TieredSinkConfig) -> Result<Self> {
        let (sender, receiver) = match yaque::channel(&config.spool_path) {
            Ok(channel) => channel,
            // The spill cache won't open (corrupt segments / metadata). Under the
            // default Quarantine policy, move it aside (forensics preserved) and
            // start fresh so a poisoned cache can never wedge startup.
            Err(e) if config.on_corruption == crate::spool_codec::CorruptionPolicy::Quarantine => {
                let moved =
                    crate::spool_codec::quarantine_dir(&config.spool_path).map_err(|qe| {
                        TieredSinkError::SpoolOpen {
                            path: config.spool_path.display().to_string(),
                            message: format!("quarantine failed: {qe}"),
                        }
                    })?;
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    path = %config.spool_path.display(),
                    quarantined = ?moved,
                    error = %e,
                    "spill cache could not be opened; quarantined and starting fresh"
                );
                let _ = moved;
                yaque::channel(&config.spool_path).map_err(|e2| TieredSinkError::SpoolOpen {
                    path: config.spool_path.display().to_string(),
                    message: e2.to_string(),
                })?
            }
            Err(e) => {
                return Err(TieredSinkError::SpoolOpen {
                    path: config.spool_path.display().to_string(),
                    message: e.to_string(),
                });
            }
        };

        let sink = Arc::new(sink);
        let spool_sender = Arc::new(Mutex::new(sender));
        let spool_receiver = Arc::new(Mutex::new(receiver));

        // Recover counters in spawn_blocking -- segment
        // files can be GB-sized after a crash; walking them on the
        // async runtime pins a tokio worker.
        let spool_path_for_scan = config.spool_path.clone();
        let (initial_count, initial_bytes) =
            tokio::task::spawn_blocking(move || spool_item_count_and_bytes(&spool_path_for_scan))
                .await
                .unwrap_or((0, 0));
        let spool_count = Arc::new(AtomicU64::new(initial_count));
        let spool_bytes = Arc::new(AtomicU64::new(initial_bytes));

        let circuit = Arc::new(CircuitBreaker::new(
            config.circuit_failure_threshold,
            config.circuit_reset_timeout(),
        ));
        let shutdown = Arc::new(Notify::new());
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
            let shutdown_clone = Arc::clone(&shutdown);
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
            Arc::clone(&shutdown),
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
            fifo_gate,
            hot_path_count: AtomicU64::new(0),
            cold_path_count: AtomicU64::new(0),
        })
    }

    /// Send a message through the tiered sink.
    ///
    /// The message goes through the hot path (direct to sink) if:
    /// - Circuit is closed AND ordering mode is Interleaved
    /// - OR circuit is closed AND ordering is StrictFifo AND spool is empty
    ///
    /// Otherwise, the message is spooled to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The sink returns a fatal error
    /// - The spool is full
    /// - Compression fails
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

    /// Reserve capacity (`fetch_update`) before enqueue; roll back
    /// on failure. Atomic reservation prevents two concurrent
    /// callers from both passing the cap check and overshooting.
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
                .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |cur| {
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

        // Reserve byte budget; roll back item slot on failure.
        if let Some(max_bytes) = self.config.max_spool_bytes {
            if let Err(current_bytes) = self.spool_bytes.fetch_update(
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
                |cur| {
                    cur.checked_add(compressed_len)
                        .filter(|new| *new <= max_bytes)
                },
            ) {
                // Roll back the item-slot reservation taken above.
                self.spool_count.fetch_sub(1, AtomicOrdering::AcqRel);
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

        // Enqueue; roll back both reservations on failure.
        let mut sender = self.spool_sender.lock().await;
        if let Err(e) = sender.send(compressed).await {
            drop(sender);
            self.spool_count.fetch_sub(1, AtomicOrdering::AcqRel);
            self.spool_bytes
                .fetch_sub(compressed_len, AtomicOrdering::AcqRel);
            return Err(TieredSinkError::Spool(e.to_string()));
        }
        drop(sender);

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

    /// Shutdown the drain task gracefully.
    pub async fn shutdown(mut self) {
        self.shutdown.notify_one();
        if let Some(handle) = self.drain_handle.take() {
            let _ = handle.await;
        }
    }
}

/// Count existing items and sum payload bytes in a yaque queue directory.
///
/// yaque stores messages as `[4-byte Hamming header][payload]` in segment files
/// named `<n>.q`. The receiver position is persisted in `recv-metadata`.
///
/// Returns `(item_count, payload_bytes)`.
fn spool_item_count_and_bytes(path: &std::path::Path) -> (u64, u64) {
    if !path.is_dir() {
        return (0, 0);
    }

    // Read receiver state from recv-metadata (two big-endian u64: segment, position)
    let recv_metadata_path = path.join("recv-metadata");
    let (recv_segment, recv_position) = if recv_metadata_path.exists() {
        std::fs::read(&recv_metadata_path)
            .ok()
            .and_then(|data| {
                if data.len() >= 16 {
                    let segment = u64::from_be_bytes(data[0..8].try_into().ok()?);
                    let position = u64::from_be_bytes(data[8..16].try_into().ok()?);
                    Some((segment, position))
                } else {
                    None
                }
            })
            .unwrap_or((0, 0))
    } else {
        (0, 0)
    };

    // Collect segment files at or after the receiver position
    let mut segments: Vec<u64> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let file_path = entry.path();
            if file_path.extension().and_then(|e| e.to_str()) == Some("q")
                && let Some(seg_num) = file_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                && seg_num >= recv_segment
            {
                segments.push(seg_num);
            }
        }
    }
    segments.sort_unstable();

    let header_eof: [u8; 4] = [255, 255, 255, 255];
    let mut count = 0u64;
    let mut bytes = 0u64;

    for &seg_num in &segments {
        let seg_path = path.join(format!("{seg_num}.q"));
        let Ok(file_data) = std::fs::read(&seg_path) else {
            continue;
        };

        #[allow(clippy::cast_possible_truncation)]
        let start = if seg_num == recv_segment {
            recv_position as usize
        } else {
            0
        };

        let mut pos = start;
        while pos + 4 <= file_data.len() {
            let header_bytes: [u8; 4] = file_data[pos..pos + 4].try_into().unwrap_or([0; 4]);
            if header_bytes == header_eof {
                break;
            }
            let encoded = u32::from_be_bytes(header_bytes);
            let payload_len = (encoded & 0x03_FF_FF_FF) as usize;
            pos += 4 + payload_len;
            if pos <= file_data.len() {
                count += 1;
                bytes += payload_len as u64;
            }
        }
    }

    (count, bytes)
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
    shutdown: Arc<Notify>,
) {
    loop {
        tokio::select! {
            () = shutdown.notified() => {
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
        // Durability requires explicit `shutdown().await`.
        // Drop can only notify the background drainer -- it can't
        // await it. If anything's still spooled at drop time, the
        // caller has skipped the explicit shutdown and risks losing
        // that data. Warn + count so this shows up in dashboards.
        let pending = self.spool_count.load(AtomicOrdering::Relaxed);
        if pending > 0 {
            #[cfg(feature = "metrics")]
            ::metrics::counter!("spool_dropped_without_shutdown_total").increment(1);
            #[cfg(feature = "tracing")]
            tracing::warn!(
                pending,
                "TieredSink dropped with spooled work pending -- call shutdown().await for durability"
            );
        }
        self.shutdown.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{PayloadFormat, RecordMeta, TransportResult};
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

    /// A real `TransportSender` test double: records what it receives and can be
    /// toggled unavailable to drive the spill/circuit/drain paths.
    struct TestSink {
        available: AtomicBool,
        received: Mutex<Vec<Record>>,
    }

    impl TestSink {
        fn new() -> Self {
            Self {
                available: AtomicBool::new(true),
                received: Mutex::new(Vec::new()),
            }
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
            if self.available.load(AtomicOrdering::SeqCst) {
                self.received.lock().await.push(rec(&payload));
                SendResult::Ok
            } else {
                SendResult::Backpressured
            }
        }

        async fn send_batch(&self, records: &[Record]) -> SendResult {
            if self.available.load(AtomicOrdering::SeqCst) {
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

    #[tokio::test]
    async fn test_drain_recovers_messages() {
        let dir = tempdir().unwrap();
        let spool_path = dir.path().join("test-queue");

        let sink = TestSink::new();
        sink.set_available(false);

        let mut config = TieredSinkConfig::new(&spool_path);
        config.circuit_failure_threshold = 1;
        config.circuit_reset_timeout_ms = 50;
        config.drain_interval_ms = 10;

        let tiered = TieredSink::new(sink, config).await.unwrap();

        // Spool a message
        tiered.send(&rec(b"recover me")).await.unwrap();
        assert_eq!(tiered.spool_len().await, 1);

        // Make sink available and wait for drain
        tiered.inner().set_available(true);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Should have drained -- and the EXACT payload must survive the
        // encode -> spool -> drain -> decode round-trip (full Record fidelity).
        assert!(tiered.spool_is_empty().await);
        assert_eq!(tiered.inner().received_count().await, 1);
        assert_eq!(
            tiered.inner().received_payloads().await,
            vec![b"recover me".to_vec()],
            "drained record content must match what was spilled"
        );

        tiered.shutdown().await;
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(tiered.spool_is_empty().await);

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
        for _ in 0..40 {
            if tiered.spool_is_empty().await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

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
        for _ in 0..40 {
            if tiered.spool_is_empty().await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
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
        // CRC enabled: a corrupt spilled record must be DROPPED on drain (logged +
        // counted), never replayed as garbage downstream.
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
        for _ in 0..40 {
            if tiered.spool_is_empty().await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
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

        // Make sink available and wait for drain
        tiered.inner().set_available(true);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

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
}
