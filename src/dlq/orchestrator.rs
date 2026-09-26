// Project:   scalo
// File:      src/dlq/orchestrator.rs
// Purpose:   Dlq orchestrator over BackgroundSink<DlqEntry>
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Dlq orchestrator.
//!
//! Wraps a [`BackgroundSink<DlqEntry>`] whose drain (`DlqDrain`)
//! dispatches batches across one or more [`super::DlqBackend`]
//! variants using the configured [`DlqMode`].
//!
//! ## Hot path
//!
//! `try_send` / `send` queue an entry onto the in-memory mpsc and
//! return. The drain task -- the only place that touches backends --
//! coalesces queued entries into batches and writes to backends. The
//! caller never blocks on disk, Kafka, or HTTP I/O.
//!
//! ## Modes
//!
//! - `Cascade` / `FileOnly` / `KafkaOnly` -- try backends in order;
//!   each entry goes to the first backend that takes it.
//! - `FanOut` -- send to all backends, succeed if any succeed.
//!
//! ## Shutdown
//!
//! On `CancellationToken::cancel()` the drain finishes its in-flight
//! batch, drains the queue, waits for the Kafka backend's acks as a
//! `flush` would, counts in [`Dlq::dropped`] what nothing confirmed, then
//! exits. Use [`Dlq::shutdown`] for graceful join. Dropping all `Dlq`
//! handles also triggers a clean exit (channel closes, drain drains,
//! then exits).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::concurrency::{
    BackgroundSink, BackgroundSinkConfig, BackgroundSinkHandle, DrainError, Overflow, SinkDrain,
    SinkError,
};

use super::backend::DlqBackend;
use super::config::{DlqConfig, DlqMode};
use super::entry::DlqEntry;
use super::error::DlqError;
use super::file::FileDlqInner;

/// Unified DLQ. Caller queues entries from any task; the orchestrator
/// drains them off-runtime via the configured backends.
///
/// Clone is cheap (`mpsc::Sender` clone). The single-owner shutdown
/// handle stays inside `Arc<AsyncMutex<Option<...>>>` so `Dlq` itself
/// is `Clone`.
#[derive(Clone)]
pub struct Dlq {
    sink: Option<BackgroundSink<DlqEntry>>,
    join: Arc<AsyncMutex<Option<BackgroundSinkHandle>>>,
    enabled: bool,
    mode: DlqMode,
    /// Child of the user-supplied shutdown token. The drain task runs
    /// on this child, so [`Dlq::shutdown`] can cancel only the DLQ
    /// without affecting the caller's broader shutdown plan. When the
    /// caller cancels their own token, the child fires too (normal
    /// child-token semantics), so the drain still exits on global
    /// shutdown.
    cancel: CancellationToken,
    /// Dead letters that had nowhere to go: sends into a disabled DLQ
    /// plus batches every backend refused. Shared with the drain and
    /// across clones so [`Dlq::dropped`] surfaces the full loss.
    lost: Arc<AtomicU64>,
    /// Debounce clock (epoch ms) for the dead-letter-drop ERROR log.
    lost_log_ms: Arc<AtomicU64>,
    /// The largest serialised entry any backend can hold, when every backend
    /// has a ceiling.
    #[cfg_attr(
        not(feature = "transport"),
        allow(dead_code, reason = "read by `refusal` only")
    )]
    ceiling: Option<usize>,
}

/// Counts the bytes written to it, to size an entry without keeping its body.
#[cfg(feature = "transport")]
#[derive(Default)]
struct ByteCount(usize);

#[cfg(feature = "transport")]
impl std::io::Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Minimum interval between dead-letter-drop ERROR logs; the counters
/// still move for every dropped entry.
const DROP_LOG_INTERVAL_MS: u64 = 5_000;

/// Log at most once per interval. Local copy of the atomic pattern in
/// `logger::log_debounced`, which sits behind the `logger` feature the
/// `dlq` feature does not require.
fn drop_log_due(last_epoch_ms: &AtomicU64, min_interval_ms: u64) -> bool {
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let last = last_epoch_ms.load(Ordering::Relaxed);
    // A racer that read the same `last` fails the exchange, so one caller claims the window.
    now.saturating_sub(last) >= min_interval_ms
        && last_epoch_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
}

impl std::fmt::Debug for Dlq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dlq")
            .field("enabled", &self.enabled)
            .field("mode", &self.mode)
            .field(
                "pending",
                &self.sink.as_ref().map_or(0, BackgroundSink::pending),
            )
            .field("dropped", &self.dropped())
            .finish_non_exhaustive()
    }
}

impl Dlq {
    /// Build a disabled DLQ. All `send` / `try_send` calls succeed as
    /// no-ops, but every entry that would have been routed is counted in
    /// [`Dlq::dropped`], emitted as `dlq_dropped_total{reason="disabled"}`,
    /// and shouted with a rate-limited ERROR -- a lost dead letter must
    /// never be silent.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            sink: None,
            join: Arc::new(AsyncMutex::new(None)),
            enabled: false,
            mode: DlqMode::default(),
            cancel: CancellationToken::new(),
            lost: Arc::new(AtomicU64::new(0)),
            lost_log_ms: Arc::new(AtomicU64::new(0)),
            ceiling: None,
        }
    }

    /// Spawn the DLQ with whatever backends the config enables.
    ///
    /// `kafka_config` is required if the config has `kafka.enabled =
    /// true` (or the mode demands Kafka). Pass `None` if the service
    /// has no Kafka transport -- Kafka mode/enabled flags are honoured
    /// where possible and a clear `Err(DlqError::NotConfigured)` is
    /// returned if Kafka is required but unavailable.
    ///
    /// # Errors
    ///
    /// Returns `Err` if any enabled backend fails to initialise.
    pub fn spawn(
        config: &DlqConfig,
        service_name: &str,
        #[cfg(feature = "dlq-kafka")] kafka_config: Option<&crate::transport::KafkaConfig>,
        #[cfg(not(feature = "dlq-kafka"))] _kafka_config: Option<&()>,
        shutdown: CancellationToken,
    ) -> Result<Self, DlqError> {
        if !config.enabled {
            return Ok(Self::disabled());
        }

        let backends = build_backends(
            config,
            service_name,
            #[cfg(feature = "dlq-kafka")]
            kafka_config,
        )?;

        if backends.is_empty() {
            warn!("DLQ enabled but no backends configured -- entries will be dropped");
            return Ok(Self::disabled());
        }

        let names: Vec<&'static str> = backends.iter().map(DlqBackend::name).collect();
        debug!(mode = ?config.mode, backends = ?names, "DLQ initialised");
        // An entry is refused for good only when every backend would refuse it.
        let ceiling = backends
            .iter()
            .map(DlqBackend::entry_ceiling)
            .collect::<Option<Vec<usize>>>()
            .and_then(|ceilings| ceilings.into_iter().max());

        let lost = Arc::new(AtomicU64::new(0));
        let lost_log_ms = Arc::new(AtomicU64::new(0));
        let drain = DlqDrain {
            mode: config.mode,
            backends,
            lost: Arc::clone(&lost),
            lost_log_ms: Arc::clone(&lost_log_ms),
        };

        let sink_config = BackgroundSinkConfig {
            queue_capacity: config.queue_capacity,
            batch_size: config.batch_size,
            flush_interval: std::time::Duration::from_millis(config.flush_interval_ms),
            overflow: Overflow::Drop,
            metric_prefix: Some("dlq"),
        };

        // Derive a child token so `Dlq::shutdown` can stop the drain
        // without forcing the caller to cancel their broader shutdown
        // plan. The child fires automatically when the parent fires,
        // so global shutdown still drains the DLQ.
        let cancel = shutdown.child_token();
        let (sink, handle) = BackgroundSink::spawn(drain, sink_config, cancel.clone());

        Ok(Self {
            sink: Some(sink),
            join: Arc::new(AsyncMutex::new(Some(handle))),
            enabled: true,
            mode: config.mode,
            cancel,
            lost,
            lost_log_ms,
            ceiling,
        })
    }

    /// Count and shout dead letters that had nowhere to go. Callers still
    /// return `Ok` -- this is visibility, not a new failure mode.
    fn note_dropped(&self, count: u64) {
        if count == 0 {
            return;
        }
        let total = self.lost.fetch_add(count, Ordering::Relaxed) + count;
        ::metrics::counter!("dlq_dropped_total", "reason" => "disabled").increment(count);
        if drop_log_due(&self.lost_log_ms, DROP_LOG_INTERVAL_MS) {
            error!(
                count,
                total, "DLQ is disabled or failed to start -- dead letters are being DROPPED"
            );
        }
    }

    /// Whether the DLQ is accepting entries.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Configured routing mode (informational).
    #[must_use]
    pub fn mode(&self) -> DlqMode {
        self.mode
    }

    /// Approximate queue depth (drain may be mid-recv).
    #[must_use]
    pub fn pending(&self) -> usize {
        self.sink.as_ref().map_or(0, BackgroundSink::pending)
    }

    /// Total entries dropped since spawn: queue overflow, sends into a
    /// disabled DLQ, batches every backend refused, and entries only Kafka
    /// held that a flush or the shutdown found lost or never acked.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.sink.as_ref().map_or(0, BackgroundSink::dropped) + self.lost.load(Ordering::Relaxed)
    }

    /// Sync-shaped queue submission. Returns immediately. On a full
    /// queue, returns `Err(DlqError::QueueFull)` and increments the
    /// drop counter -- caller decides whether to log, escalate, or
    /// proceed.
    ///
    /// # Errors
    ///
    /// `QueueFull` if the in-memory queue is full. `Closed` if the
    /// drain has exited.
    pub fn try_send(&self, entry: DlqEntry) -> Result<(), DlqError> {
        let Some(sink) = self.sink.as_ref() else {
            self.note_dropped(1);
            return Ok(());
        };
        sink.try_push(entry).map_err(map_sink_err)
    }

    /// Async submission that awaits queue space.
    ///
    /// Successful return means the entry is queued, NOT that it is
    /// durably written. Use [`Self::flush`] for that.
    ///
    /// # Errors
    ///
    /// `Closed` if the drain has exited.
    pub async fn send(&self, entry: DlqEntry) -> Result<(), DlqError> {
        let Some(sink) = self.sink.as_ref() else {
            self.note_dropped(1);
            return Ok(());
        };
        sink.push_blocking(entry).await.map_err(map_sink_err)
    }

    /// Async batch submission. Each entry is queued individually; the
    /// drain decides how to coalesce.
    ///
    /// # Errors
    ///
    /// `Closed` if the drain has exited mid-batch.
    pub async fn send_batch(&self, entries: Vec<DlqEntry>) -> Result<(), DlqError> {
        let Some(sink) = self.sink.as_ref() else {
            self.note_dropped(entries.len() as u64);
            return Ok(());
        };
        for entry in entries {
            sink.push_blocking(entry).await.map_err(map_sink_err)?;
        }
        Ok(())
    }

    /// Block until the drain has written every entry queued before this
    /// call, and report whether any of those writes was refused.
    ///
    /// `Ok` means every batch the drain wrote since the previous flush was
    /// accepted by a backend. For the Kafka backend accepted means acked by
    /// the broker: the flush waits up to `kafka.send_timeout_ms` for that,
    /// off the runtime, and entries only Kafka held that were refused or
    /// never acked count as refused. A refused batch is reported by the
    /// first flush after it and not again; it is also counted in
    /// [`Dlq::dropped`]. What "accepted" means per backend is in
    /// `docs/pipeline/dlq.md`.
    ///
    /// # Errors
    ///
    /// `File` if every backend refused a batch written since the previous
    /// flush, whether the write was size-, tick- or barrier-triggered.
    /// `Kafka` if the Kafka backend lost entries no other backend holds.
    /// `Closed` if the drain has exited before this barrier was processed.
    pub async fn flush(&self) -> Result<(), DlqError> {
        let Some(sink) = self.sink.as_ref() else {
            return Ok(());
        };
        sink.flush().await.map_err(map_sink_err)
    }

    /// Write `entries` and confirm them: `Ok` only once a backend holds every
    /// one, in the sense [`flush`](Self::flush) gives "accepted".
    ///
    /// The entries are written as one batch of their own, and the answer is
    /// about that batch, so a concurrent caller's refusal never reaches this
    /// call and this call's refusal never reaches another caller's `flush`.
    /// The durable flush after the write (the Kafka ack wait) covers every
    /// earlier write too, so a loss it finds is returned here and to the next
    /// `flush` as well. A disabled DLQ counts the entries in
    /// [`dropped`](Self::dropped) and returns `Ok`, as `send` does.
    ///
    /// # Errors
    ///
    /// `File` or `Kafka` when no backend holds the entries, the durable flush
    /// after them found a loss, or the Kafka backend purged entries it has
    /// not yet heard back about. `Closed` if the drain has exited.
    pub async fn write_confirmed(&self, entries: Vec<DlqEntry>) -> Result<(), DlqError> {
        let Some(sink) = self.sink.as_ref() else {
            self.note_dropped(entries.len() as u64);
            return Ok(());
        };
        sink.write_confirmed(entries).await.map_err(map_sink_err)
    }

    /// Why no backend of this DLQ can ever hold `entry`, or `None` when one
    /// can, or the DLQ is disabled.
    ///
    /// Only a Kafka backend has a ceiling: `message.max.bytes`, measured
    /// against the entry as it is written, base64 payload included. A file or
    /// HTTP backend beside it takes what Kafka refuses, so the DLQ refuses an
    /// entry only when every backend does. A caller holding a source
    /// acknowledgement leaves such an entry out of
    /// [`write_confirmed`](Self::write_confirmed) and releases it `Dropped`:
    /// no retry can land it. Every other `write_confirmed` error can clear, so
    /// that caller holds the block and writes again. A broker or topic ceiling
    /// below the producer's is not seen here, and fails the write instead.
    #[cfg(feature = "transport")]
    #[must_use]
    pub fn refusal(&self, entry: &DlqEntry) -> Option<crate::transport::DeadLetterReason> {
        let limit = self.ceiling?;
        let mut size = ByteCount::default();
        serde_json::to_writer(&mut size, entry).ok()?;
        (size.0 > limit).then_some(crate::transport::DeadLetterReason::TooLarge {
            bytes: size.0,
            limit,
        })
    }

    /// Cancel the internal child token (drain flushes its batch and
    /// exits), then await the drain. Cancelling here rather than only
    /// awaiting the join is what stops `shutdown` hanging when the
    /// caller has not separately cancelled the token passed to `spawn`.
    ///
    /// Before it exits the drain waits for the Kafka backend's acks, up to
    /// `kafka.send_timeout_ms` plus 5 s when it has to purge, and counts
    /// the entries only Kafka held that were never acked in
    /// [`Dlq::dropped`]. A caller that needs that loss as an `Err` calls
    /// [`Self::flush`] first.
    ///
    /// Idempotent across clones: the join happens once; later calls see
    /// an empty join slot and return Ok.
    ///
    /// # Errors
    ///
    /// Returns `Err(DlqError::Closed)` if the drain task panicked.
    pub async fn shutdown(&self) -> Result<(), DlqError> {
        self.cancel.cancel();
        let mut guard = self.join.lock().await;
        let Some(handle) = guard.take() else {
            return Ok(());
        };
        handle
            .join()
            .await
            .map_err(|e| DlqError::File(format!("DLQ drain join failed: {e}")))?;
        Ok(())
    }
}

fn map_sink_err(e: SinkError) -> DlqError {
    match e {
        SinkError::Overflow => DlqError::QueueFull,
        SinkError::Closed => DlqError::Closed,
        SinkError::Drain(d) => map_drain_err(d),
    }
}

/// A Kafka or HTTP backend's own loss keeps its variant; every other drain
/// failure reads as `File`, as it always has.
fn map_drain_err(d: DrainError) -> DlqError {
    let source = match d {
        DrainError::Backend(source) => source,
        io @ DrainError::Io(_) => return DlqError::File(io.to_string()),
    };
    match source.downcast::<DlqError>() {
        Ok(own) => match *own {
            #[cfg(feature = "dlq-kafka")]
            DlqError::Kafka(msg) => DlqError::Kafka(msg),
            DlqError::BackendError(msg) => DlqError::BackendError(msg),
            other @ (DlqError::Io(_)
            | DlqError::Serialization(_)
            | DlqError::File(_)
            | DlqError::AllBackendsFailed(_)
            | DlqError::NotConfigured
            | DlqError::QueueFull
            | DlqError::Closed) => DlqError::File(DrainError::Backend(Box::new(other)).to_string()),
        },
        Err(foreign) => DlqError::File(DrainError::Backend(foreign).to_string()),
    }
}

fn build_backends(
    config: &DlqConfig,
    service_name: &str,
    #[cfg(feature = "dlq-kafka")] kafka_config: Option<&crate::transport::KafkaConfig>,
) -> Result<Vec<DlqBackend>, DlqError> {
    let mut backends: Vec<DlqBackend> = Vec::new();
    let mode = config.mode;

    // Kafka first (primary in cascade) -- feature-gated.
    #[cfg(feature = "dlq-kafka")]
    {
        let want_kafka = matches!(
            mode,
            DlqMode::Cascade | DlqMode::FanOut | DlqMode::KafkaOnly
        );
        if want_kafka && config.kafka.enabled {
            let kc = kafka_config.ok_or_else(|| {
                DlqError::Kafka(
                    "DLQ Kafka backend enabled but no KafkaConfig provided to Dlq::spawn".into(),
                )
            })?;
            backends.push(DlqBackend::Kafka(super::kafka::KafkaDlqInner::new(
                kc,
                &config.kafka,
            )?));
        }
    }

    // File second (fallback in cascade) -- always available.
    let want_file = matches!(mode, DlqMode::Cascade | DlqMode::FanOut | DlqMode::FileOnly);
    if want_file && config.file.enabled {
        backends.push(DlqBackend::File(FileDlqInner::new(
            &config.file,
            service_name,
        )?));
    }

    // HTTP -- feature-gated, added when explicitly enabled.
    #[cfg(feature = "dlq-http")]
    {
        if config.http.enabled {
            backends.push(DlqBackend::Http(super::http::HttpDlqInner::new(
                &config.http,
            )?));
        }
    }

    Ok(backends)
}

/// Drain task -- owns the backends and implements cascade / fan-out
/// dispatch. Lives inside the actor task spawned by `BackgroundSink`.
struct DlqDrain {
    mode: DlqMode,
    backends: Vec<DlqBackend>,
    /// Shared with [`Dlq`] so backend-refused batches surface in `dropped()`.
    lost: Arc<AtomicU64>,
    /// Shared debounce clock for the dead-letter-drop ERROR log.
    lost_log_ms: Arc<AtomicU64>,
}

impl DlqDrain {
    /// Count and shout dead letters no backend holds: a batch every backend
    /// refused, or entries a barrier found only Kafka held and lost.
    fn note_lost(&self, count: u64) {
        if count == 0 {
            return;
        }
        let total = self.lost.fetch_add(count, Ordering::Relaxed) + count;
        ::metrics::counter!("dlq_dropped_total", "reason" => "backends_failed").increment(count);
        if drop_log_due(&self.lost_log_ms, DROP_LOG_INTERVAL_MS) {
            error!(
                count,
                total, "no DLQ backend holds these entries -- dead letters are being DROPPED"
            );
        }
    }

    /// Each entry goes to the first backend that takes it, so a backend that
    /// queued part of the batch before failing hands on only the rest.
    async fn write_cascade(&mut self, batch: &[DlqEntry]) -> Result<(), DrainError> {
        let mut rest = batch;
        let mut last_err: Option<DlqError> = None;
        for backend in &mut self.backends {
            match backend.send_batch(rest).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let taken = backend.queued_before_failure().min(rest.len());
                    rest = &rest[taken..];
                    warn!(
                        backend = backend.name(),
                        error = %e,
                        count = rest.len(),
                        "DLQ backend failed in cascade, trying next"
                    );
                    last_err = Some(e);
                }
            }
        }
        // The actor discards the batch on Err -- count the loss.
        self.note_lost(rest.len() as u64);
        let msg = last_err.map_or_else(|| "no backends configured".to_string(), |e| e.to_string());
        Err(DrainError::Backend(Box::new(DlqError::AllBackendsFailed(
            msg,
        ))))
    }

    /// Every backend gets the whole batch; an entry is safe while any holds it.
    async fn write_fan_out(&mut self, batch: &[DlqEntry]) -> Result<(), DrainError> {
        // Per backend: entries it holds, and whether it took the whole batch.
        let mut outcomes: Vec<(usize, bool)> = Vec::with_capacity(self.backends.len());
        let mut errs: Vec<String> = Vec::new();
        for backend in &mut self.backends {
            match backend.send_batch(batch).await {
                Ok(()) => outcomes.push((batch.len(), true)),
                Err(e) => {
                    warn!(
                        backend = backend.name(),
                        error = %e,
                        count = batch.len(),
                        "DLQ backend failed in fan-out"
                    );
                    errs.push(format!("{}:{}", backend.name(), e));
                    outcomes.push((backend.queued_before_failure(), false));
                }
            }
        }
        let whole = outcomes
            .iter()
            .filter(|(_, took_whole)| *took_whole)
            .count();
        if whole > 0 {
            for (backend, &(held, took_whole)) in self.backends.iter_mut().zip(&outcomes) {
                if whole > usize::from(took_whole) {
                    backend.share_custody(held);
                }
            }
            return Ok(());
        }
        // Entries a backend queued before failing stay in its custody.
        let kept = outcomes.iter().map(|(held, _)| *held).max().unwrap_or(0);
        // The actor discards the batch on Err -- count the loss.
        self.note_lost(batch.len().saturating_sub(kept) as u64);
        Err(DrainError::Backend(Box::new(DlqError::AllBackendsFailed(
            errs.join("; "),
        ))))
    }
}

impl SinkDrain<DlqEntry> for DlqDrain {
    async fn write_batch(&mut self, batch: Vec<DlqEntry>) -> Result<(), DrainError> {
        if batch.is_empty() {
            return Ok(());
        }
        match self.mode {
            DlqMode::Cascade | DlqMode::FileOnly | DlqMode::KafkaOnly => {
                self.write_cascade(&batch).await
            }
            DlqMode::FanOut => self.write_fan_out(&batch).await,
        }
    }

    /// Run every backend's durable flush and count what they found lost.
    async fn flush_durable(&mut self) -> Result<(), DrainError> {
        let mut first_err: Option<DlqError> = None;
        let mut lost = 0;
        for backend in &mut self.backends {
            let result = backend.flush_durable().await;
            lost += backend.take_durable_losses();
            if let Err(e) = result {
                warn!(backend = backend.name(), error = %e, "DLQ backend durable flush failed");
                first_err.get_or_insert(e);
            }
        }
        self.note_lost(lost);
        first_err.map_or(Ok(()), |e| Err(DrainError::Backend(Box::new(e))))
    }

    /// Settled once no backend holds entries whose fate it has not heard.
    fn settled(&self) -> bool {
        self.backends.iter().all(|b| b.unsettled() == 0)
    }

    /// Settle every backend before the drain drops it, and count what no
    /// backend confirmed: dropping the Kafka producer discards what it holds.
    async fn close(&mut self) -> Result<(), DrainError> {
        let settled = self.flush_durable().await;
        let unconfirmed: u64 = self
            .backends
            .iter_mut()
            .map(DlqBackend::take_unconfirmed)
            .sum();
        if unconfirmed > 0 {
            warn!(
                unconfirmed,
                "DLQ entries had no delivery report when the drain closed; counted as dropped"
            );
            self.note_lost(unconfirmed);
        }
        settled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dlq::config::{FileDlqConfig, RotationPeriod};
    use crate::dlq::entry::DlqSource;

    fn tmp_config(dir: &std::path::Path) -> DlqConfig {
        DlqConfig {
            file: FileDlqConfig {
                enabled: true,
                path: dir.to_path_buf(),
                rotation: RotationPeriod::Daily,
                max_age_days: 1,
                compress_rotated: false,
            },
            mode: DlqMode::FileOnly,
            queue_capacity: 1024,
            batch_size: 16,
            flush_interval_ms: 20,
            ..DlqConfig::default()
        }
    }

    fn test_entry(reason: &str) -> DlqEntry {
        DlqEntry::new("test", reason, b"payload".to_vec())
            .with_destination("acme.auth")
            .with_source(DlqSource::kafka("events", 1, 42))
    }

    fn spawn_dlq(cfg: &DlqConfig, shutdown: &CancellationToken) -> Dlq {
        Dlq::spawn(
            cfg,
            "svc",
            #[cfg(feature = "dlq-kafka")]
            None,
            #[cfg(not(feature = "dlq-kafka"))]
            None,
            shutdown.clone(),
        )
        .expect("spawn")
    }

    /// Replace the service directory with a regular file so every file-backend write fails.
    fn break_file_backend(dir: &std::path::Path) {
        std::fs::remove_dir_all(dir.join("svc")).expect("remove dlq dir");
        std::fs::write(dir.join("svc"), b"not a directory").expect("plant file");
    }

    async fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !done() {
            assert!(std::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    fn dlq_lines(dir: &std::path::Path) -> usize {
        std::fs::read_to_string(dir.join("svc/dlq.ndjson")).map_or(0, |body| body.lines().count())
    }

    #[tokio::test]
    async fn disabled_dlq_accepts_silently() {
        let dlq = Dlq::disabled();
        dlq.send(test_entry("err")).await.expect("noop");
        dlq.send_batch(vec![test_entry("err")]).await.expect("noop");
        dlq.flush().await.expect("noop flush");
        dlq.shutdown().await.expect("noop shutdown");
    }

    /// Issue #22: a disabled DLQ must surface every entry it drops --
    /// sends stay `Ok` but the drop counter moves.
    #[tokio::test]
    async fn disabled_dlq_counts_dropped_entries() {
        let dlq = Dlq::disabled();
        assert_eq!(dlq.dropped(), 0);
        dlq.send(test_entry("a")).await.expect("noop send");
        dlq.try_send(test_entry("b")).expect("noop try_send");
        dlq.send_batch(vec![test_entry("c"), test_entry("d")])
            .await
            .expect("noop batch");
        dlq.write_confirmed(vec![test_entry("e")])
            .await
            .expect("noop confirmed write");
        assert_eq!(dlq.dropped(), 5, "every routed entry counts as dropped");
        // Clones share the counter, matching the shared drain contract.
        assert_eq!(dlq.clone().dropped(), 5);
    }

    /// Put back the service directory [`break_file_backend`] replaced.
    fn mend_file_backend(dir: &std::path::Path) {
        std::fs::remove_file(dir.join("svc")).expect("remove planted file");
        std::fs::create_dir(dir.join("svc")).expect("recreate dlq dir");
    }

    #[tokio::test]
    async fn a_confirmed_write_no_backend_holds_is_refused_to_its_caller_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&tmp_config(dir.path()), &shutdown);
        break_file_backend(dir.path());

        let refused = dlq.write_confirmed(vec![test_entry("mine")]).await;
        assert!(
            matches!(refused, Err(DlqError::File(_))),
            "got: {refused:?}"
        );
        assert_eq!(dlq.dropped(), 1, "the refused entry is counted lost");
        dlq.flush()
            .await
            .expect("the refusal went to the caller that wrote it, not to a flusher");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn another_writers_refusal_never_reaches_a_confirmed_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&tmp_config(dir.path()), &shutdown);
        break_file_backend(dir.path());
        dlq.send(test_entry("queued")).await.expect("queued");
        // The 20 ms tick writes it, and the broken backend refuses it.
        wait_until("tick write refused", || dlq.dropped() >= 1).await;

        mend_file_backend(dir.path());
        // The file backend reopens on a write once its 250 ms backoff has run.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        dlq.write_confirmed(vec![test_entry("mine")])
            .await
            .expect("a backend holds this caller's entry");
        assert_eq!(dlq_lines(dir.path()), 1);
        let flush = dlq.flush().await;
        assert!(
            matches!(flush, Err(DlqError::File(_))),
            "the writer whose entry was refused still hears of it: {flush:?}"
        );
        shutdown.cancel();
    }

    /// Issue #22: a file backend whose writes fail (the read-only-rootfs
    /// shape) must surface the loss in the drop counter, not vanish
    /// behind a startup fallback.
    #[tokio::test]
    async fn failed_writer_surfaces_drop_counter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&tmp_config(dir.path()), &shutdown);
        break_file_backend(dir.path());

        dlq.send(test_entry("err")).await.expect("queued");
        let flush = dlq.flush().await;
        assert!(flush.is_err(), "flush must surface the drain failure");
        assert!(
            dlq.dropped() >= 1,
            "drop counter must surface the lost entry"
        );

        shutdown.cancel();
    }

    /// Issue #187: a batch the backend refused on a tick write must fail
    /// the flush that covers it, and only that flush.
    #[tokio::test]
    async fn flush_fails_when_a_tick_write_was_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = tmp_config(dir.path());
        cfg.batch_size = 1024;
        cfg.flush_interval_ms = 20;
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&cfg, &shutdown);
        break_file_backend(dir.path());

        dlq.send(test_entry("err")).await.expect("queued");
        // batch_size is out of reach, so only the tick can have written it.
        wait_until("tick write refused", || dlq.dropped() >= 1).await;

        let flush = dlq.flush().await;
        assert!(matches!(flush, Err(DlqError::File(_))), "got: {flush:?}");
        dlq.flush()
            .await
            .expect("a loss is reported by one flush only");
        shutdown.cancel();
    }

    /// Issue #187: a batch the backend refused on a size-triggered write
    /// must fail the flush that covers it.
    #[tokio::test]
    async fn flush_fails_when_a_size_write_was_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = tmp_config(dir.path());
        cfg.batch_size = 2;
        cfg.flush_interval_ms = 60_000;
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&cfg, &shutdown);
        break_file_backend(dir.path());

        dlq.send(test_entry("a")).await.expect("queued");
        dlq.send(test_entry("b")).await.expect("queued");
        // The tick is a minute away, so only the size trigger can have written it.
        wait_until("size write refused", || dlq.dropped() >= 2).await;

        let flush = dlq.flush().await;
        assert!(matches!(flush, Err(DlqError::File(_))), "got: {flush:?}");
        dlq.flush()
            .await
            .expect("a loss is reported by one flush only");
        shutdown.cancel();
    }

    /// Size and tick writes that land must not fail the flush that covers them.
    #[tokio::test]
    async fn flush_is_ok_when_tick_and_size_writes_landed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = tmp_config(dir.path());
        cfg.batch_size = 2;
        cfg.flush_interval_ms = 20;
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&cfg, &shutdown);

        for i in 0..3 {
            dlq.send(test_entry(&format!("err_{i}")))
                .await
                .expect("send");
        }
        // Two entries go on the size trigger, the third waits for the tick.
        wait_until("all three entries written", || dlq_lines(dir.path()) == 3).await;

        dlq.flush().await.expect("every write landed");
        assert_eq!(dlq.dropped(), 0);
        shutdown.cancel();
        dlq.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn file_only_writes_and_flushes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&tmp_config(dir.path()), &shutdown);

        for i in 0..5 {
            dlq.send(test_entry(&format!("err_{i}")))
                .await
                .expect("send");
        }
        dlq.flush().await.expect("flush");

        let path = dir.path().join("svc/dlq.ndjson");
        let body = std::fs::read_to_string(&path).expect("read");
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), 5);

        shutdown.cancel();
        dlq.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn try_send_returns_queue_full_when_saturated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = tmp_config(dir.path());
        cfg.queue_capacity = 2;
        cfg.batch_size = 1024;
        cfg.flush_interval_ms = 60_000; // drain rarely fires
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&cfg, &shutdown);

        let mut full_count = 0;
        for i in 0..50 {
            if let Err(DlqError::QueueFull) = dlq.try_send(test_entry(&format!("err_{i}"))) {
                full_count += 1;
            }
        }
        assert!(full_count > 0, "expected at least one QueueFull");
        shutdown.cancel();
    }

    /// `file-rotate` drops a write to a file it could not open and reports
    /// `Ok`; the loss must still reach `flush()` and `dropped()`.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_write_to_a_dlq_file_it_cannot_open_is_counted_lost() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("svc/dlq.ndjson");
        std::fs::create_dir(dir.path().join("svc")).expect("create dir");
        std::fs::write(&file, b"").expect("create file");
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o400)).expect("chmod");
        if std::fs::OpenOptions::new().append(true).open(&file).is_ok() {
            eprintln!("skipping: this process writes files regardless of their mode");
            return;
        }
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&tmp_config(dir.path()), &shutdown);

        dlq.send(test_entry("err")).await.expect("queued");
        let flush = dlq.flush().await;
        assert!(flush.is_err(), "the write reached no file: {flush:?}");
        assert_eq!(dlq.dropped(), 1);
        assert_eq!(dlq_lines(dir.path()), 0);
        shutdown.cancel();
    }

    /// The Kafka backend against a broker that never acks an entry.
    #[cfg(feature = "dlq-kafka")]
    mod no_broker {
        use super::*;
        use crate::dlq::config::{DlqRouting, KafkaDlqConfig};
        use crate::dlq::kafka::unreachable_broker;

        /// Kafka alone, with only the barrier and shutdown writing.
        fn kafka_only(send_timeout_ms: u64) -> DlqConfig {
            DlqConfig {
                mode: DlqMode::KafkaOnly,
                queue_capacity: 1024,
                batch_size: 1024,
                flush_interval_ms: 600_000,
                file: FileDlqConfig {
                    enabled: false,
                    ..FileDlqConfig::default()
                },
                kafka: KafkaDlqConfig {
                    enabled: true,
                    routing: DlqRouting::Common,
                    common_topic: "dlq.unreachable".to_string(),
                    send_timeout_ms,
                    ..KafkaDlqConfig::default()
                },
                ..DlqConfig::default()
            }
        }

        fn spawn(send_timeout_ms: u64) -> Dlq {
            Dlq::spawn(
                &kafka_only(send_timeout_ms),
                "svc",
                Some(&unreachable_broker()),
                CancellationToken::new(),
            )
            .expect("spawn")
        }

        /// Shutdown drops the producer, which discards what it holds; what no
        /// broker acked by then must be counted, not lost silently.
        #[tokio::test]
        async fn shutdown_counts_the_entries_no_broker_acked() {
            let dlq = spawn(300);
            for i in 0..3 {
                dlq.send(test_entry(&format!("err_{i}")))
                    .await
                    .expect("queued");
            }
            dlq.shutdown().await.expect("shutdown");
            assert_eq!(dlq.dropped(), 3, "every unacked entry counted");
        }

        /// Only an entry every backend would refuse is refused: Kafka alone
        /// refuses one over its ceiling, and a file backend beside it takes it.
        #[tokio::test]
        async fn an_entry_over_the_only_backends_ceiling_is_refused() {
            let mut kafka = unreachable_broker();
            kafka.sizing.producer.message_max_bytes = Some(2_000_000);
            // Within the ceiling as raw bytes, over it once base64 grows it by a third.
            let over = DlqEntry::new("svc", "err", vec![b'x'; 1_600_000]);
            let under = DlqEntry::new("svc", "err", vec![b'x'; 100]);

            let alone = Dlq::spawn(
                &kafka_only(300),
                "svc",
                Some(&kafka),
                CancellationToken::new(),
            )
            .expect("spawn");
            let refused = alone.refusal(&over);
            assert!(
                matches!(
                    refused,
                    Some(crate::transport::DeadLetterReason::TooLarge { bytes, limit })
                        if limit == 2_000_000 - 128 && bytes > limit
                ),
                "base64 grows the payload past the ceiling: {refused:?}"
            );
            assert_eq!(alone.refusal(&under), None);

            let dir = tempfile::tempdir().expect("tempdir");
            let mut both = kafka_only(300);
            both.mode = DlqMode::Cascade;
            both.file = tmp_config(dir.path()).file;
            let cascade =
                Dlq::spawn(&both, "svc", Some(&kafka), CancellationToken::new()).expect("spawn");
            assert_eq!(
                cascade.refusal(&over),
                None,
                "the file backend takes what Kafka refuses"
            );
            assert_eq!(Dlq::disabled().refusal(&over), None);
        }

        #[tokio::test]
        async fn a_kafka_loss_fails_the_flush_as_a_kafka_error() {
            let dlq = spawn(300);
            dlq.send(test_entry("err")).await.expect("queued");
            let flush = dlq.flush().await;
            assert!(matches!(flush, Err(DlqError::Kafka(_))), "got: {flush:?}");
            assert_eq!(dlq.dropped(), 1);
            dlq.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn dlq_clone_shares_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shutdown = CancellationToken::new();
        let dlq = spawn_dlq(&tmp_config(dir.path()), &shutdown);

        let dlq2 = dlq.clone();
        dlq.send(test_entry("a")).await.expect("send a");
        dlq2.send(test_entry("b")).await.expect("send b");
        dlq.flush().await.expect("flush");

        let path = dir.path().join("svc/dlq.ndjson");
        let body = std::fs::read_to_string(&path).expect("read");
        assert_eq!(body.trim().lines().count(), 2);

        shutdown.cancel();
        dlq.shutdown().await.expect("shutdown");
    }
}
