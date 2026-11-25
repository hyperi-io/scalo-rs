// Project:   scalo
// File:      src/spool/queue.rs
// Purpose:   Disk-backed async FIFO queue implementation
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Disk-backed async FIFO queue implementation.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::compression::{Codec, Compression, Compressor};
use crate::concurrency::PeriodicWorker;
use crate::spool::{CorruptionPolicy, Result, SpoolConfig, SpoolError};
use crate::spool_codec::{
    self, CorruptionError, Decoders, FullDisk, QueueCell, StoreMetrics, SyncTicker, Unreadable,
};
use crate::yaque::{self, MemoryGuard, Receiver, Sender};
use tokio_util::sync::CancellationToken;

/// The `store` label on the spool's metrics.
const STORE: &str = "spool";

/// Disk-backed async FIFO queue with optional compression.
///
/// Crash-safe writes, survives restarts. A push writes the record once, from
/// the caller's bytes or the compressor's reused buffer. A pop reads into a
/// reused buffer, and [`pop_front_into`](Self::pop_front_into) decodes into
/// the caller's.
pub struct Spool {
    /// `None` only after a failed recovery, which leaves the spool closed.
    queue: Option<Queue>,
    config: SpoolConfig,
    len: usize,
    /// Payload bytes of the records queued, as written. Counted at open and
    /// moved by each push and take, so `max_size_bytes` never walks the
    /// directory.
    bytes: u64,
    /// Compresses pushes, and its output buffer, both reused.
    compressor: Compressor,
    packed: Vec<u8>,
    /// Decompresses each pop under the codec its entry names.
    decoders: Decoders,
    full: FullDisk,
    metrics: StoreMetrics,
    /// The open queue's shared half, for the timed sync.
    cell: QueueCell,
    shutdown: CancellationToken,
    ticker: Option<PeriodicWorker>,
    #[cfg(feature = "memory")]
    memory: Option<Arc<MemoryGuard>>,
    /// Degraded while the disk is full. Leaves the registry with the spool.
    #[cfg(feature = "health")]
    _disk_health: crate::health::HealthRegistration,
}

/// The open queue. Each half holds an OS lock on its lock file.
struct Queue {
    sender: Sender,
    receiver: Receiver,
}

/// The open queue, or an error once a failed recovery has closed it.
fn live(queue: &mut Option<Queue>) -> Result<&mut Queue> {
    queue.as_mut().ok_or_else(|| {
        SpoolError::Queue("spool is closed: recovery from a corrupt cache failed".into())
    })
}

impl Spool {
    /// Open the queue at the configured path, creating it if absent.
    ///
    /// A queue another live process (or another open `Spool` in this one)
    /// holds is refused. A process that was killed holds nothing: the OS
    /// dropped its locks.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError::Open`] if the queue is locked by a live owner, or
    /// cannot be opened and was not quarantined (see
    /// [`CorruptionPolicy`]), and [`SpoolError::Compression`] for a
    /// `compression_level` zstd does not take.
    pub async fn open(config: SpoolConfig) -> Result<Self> {
        let codec = if config.compress {
            Compression::new(Codec::Zstd).with_level(config.compression_level)
        } else {
            Compression::NONE
        };
        let compressor =
            Compressor::new(codec).map_err(|e| SpoolError::Compression(e.to_string()))?;
        // Walks segment files and may cut a torn tail, so it runs off the runtime.
        let path = config.path.clone();
        let policy = config.on_corruption;
        let options = options(&config);
        let (sender, receiver, len, bytes) = tokio::task::spawn_blocking(move || {
            let (sender, receiver) = open_queue(&path, policy, options)?;
            let queued = receiver.queued().unwrap_or_default();
            Ok::<_, SpoolError>((sender, receiver, queued.entries, queued.bytes))
        })
        .await
        .map_err(|e| SpoolError::Open {
            path: config.path.display().to_string(),
            message: format!("spool open task failed: {e}"),
        })??;

        let cell: QueueCell = Arc::new(parking_lot::Mutex::new(Some(sender.shared())));
        let shutdown = CancellationToken::new();
        let ticker = spool_codec::start_sync_ticker(
            SyncTicker {
                path: config.path.clone(),
                store: STORE,
                cell: Arc::clone(&cell),
                waiters: None,
            },
            Duration::from_millis(config.sync_interval_ms),
            shutdown.clone(),
        );
        let full = FullDisk::new(STORE);
        #[cfg(feature = "health")]
        let disk_health = {
            let flag = full.flag();
            crate::health::HealthRegistry::register_scoped("spool_disk", move || {
                if flag.load(std::sync::atomic::Ordering::Relaxed) {
                    crate::health::HealthStatus::Degraded
                } else {
                    crate::health::HealthStatus::Healthy
                }
            })
        };
        let metrics = StoreMetrics::new(STORE, codec.codec);
        metrics.disk(sender.disk_bytes());
        Ok(Self {
            queue: Some(Queue { sender, receiver }),
            config,
            len: usize::try_from(len).unwrap_or(usize::MAX),
            bytes,
            compressor,
            packed: Vec::new(),
            decoders: Decoders::default(),
            full,
            metrics,
            cell,
            shutdown,
            ticker,
            #[cfg(feature = "memory")]
            memory: None,
            #[cfg(feature = "health")]
            _disk_health: disk_health,
        })
    }

    /// Reserve each record's length with `guard` before reading it, and
    /// release it once the record is handed off.
    #[cfg(feature = "memory")]
    #[must_use]
    pub fn with_memory_guard(mut self, guard: Arc<MemoryGuard>) -> Self {
        self.memory = Some(guard);
        self
    }

    /// Quarantine the current (corrupt) cache and reopen a fresh empty queue.
    ///
    /// Moves the queue's files into a timestamped `corrupt-*` subdirectory of
    /// the spool path (forensics preserved) and opens a fresh queue in place.
    /// Used by the read paths when a header is damaged, under either policy.
    fn recover(&mut self, trigger: spool_codec::QuarantineTrigger, reason: &str) -> Result<()> {
        // The halves hold the queue's locks, so they close before the fresh queue opens.
        self.queue = None;
        *self.cell.lock() = None;
        self.len = 0;
        self.bytes = 0;
        spool_codec::quarantine(&self.config.path, trigger, reason)?;
        let (sender, receiver) = open_queue(
            &self.config.path,
            CorruptionPolicy::Fail,
            options(&self.config),
        )?;
        *self.cell.lock() = Some(sender.shared());
        self.queue = Some(Queue { sender, receiver });
        Ok(())
    }

    /// Create a new spool at the given path with default settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue cannot be created.
    pub async fn create(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(SpoolConfig::new(path.as_ref())).await
    }

    /// Create a new spool with compression enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue cannot be created.
    pub async fn create_compressed(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(SpoolConfig::with_compression(path.as_ref())).await
    }

    /// Push data onto the queue, compressing first if enabled.
    ///
    /// A push that returns `Ok` is in the operating system's hands, so it
    /// survives the process being killed. It survives the host losing power
    /// once a sync has run: the timed sync every `sync_interval_ms`,
    /// [`sync`](Self::sync), or `sync_on_push`, which syncs on every push.
    ///
    /// # Errors
    ///
    /// Errors on size/item-count cap, compression failure, or I/O, and
    /// [`SpoolError::DiskFull`] while the disk is full: nothing is written, and
    /// a retry once the consumer frees space goes through. A failed
    /// `sync_on_push` sync returns the error with the record queued, so a retry
    /// can queue it twice.
    pub async fn push(&mut self, data: &[u8]) -> Result<()> {
        if let Some(max) = self.config.max_items
            && self.len >= max
        {
            return Err(SpoolError::MaxItemsReached { max });
        }
        let started = Instant::now();
        let body: &[u8] = if self.config.compress {
            self.compressor
                .compress_into(data, &mut self.packed)
                .map_err(|e| SpoolError::Compression(e.to_string()))?;
            &self.packed
        } else {
            data
        };
        let compressing = started.elapsed();
        let decoded = u32::try_from(data.len()).map_err(|_| {
            SpoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "a record of {} bytes is past what a spool entry names",
                    data.len()
                ),
            ))
        })?;
        let prefix = spool_codec::prefix(
            self.compressor.compression().codec,
            self.config.crc,
            spool_codec::now_ms(),
            spool_codec::Counted {
                records: 1,
                decoded,
            },
            body,
        );
        let written = prefix.len() + body.len();

        if let Some(max_bytes) = self.config.max_size_bytes
            && self.bytes.saturating_add(written as u64) > max_bytes
        {
            return Err(SpoolError::MaxSizeReached { max_bytes });
        }

        let now = Instant::now();
        let queue = live(&mut self.queue)?;
        if !self.full.admits(now, queue.sender.freed()) {
            self.full.refused(4 + written);
            return Err(SpoolError::DiskFull(spool_codec::full_error().to_string()));
        }
        match queue.sender.send(&prefix, body) {
            Ok(()) => self.full.wrote(now),
            Err(e) if spool_codec::is_full(&e) => {
                self.full.failed(now, 4 + written, &e);
                return Err(SpoolError::DiskFull(e.to_string()));
            }
            Err(e) => return Err(SpoolError::Io(e)),
        }
        self.metrics.wrote(data.len(), 4 + written, compressing);
        self.metrics.disk(queue.sender.disk_bytes());

        self.len += 1;
        self.bytes += written as u64;
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_queue_depth").set(self.len as f64);
        if self.config.sync_on_push {
            self.sync().await?;
        }
        Ok(())
    }

    /// Sync every record pushed so far to disk.
    ///
    /// A push is flushed to the operating system and survives the process
    /// being killed, but not the host losing power until a sync has run. One
    /// call covers any number of pushes: push a batch, then sync once. The
    /// timed sync does the same every `sync_interval_ms`.
    ///
    /// The sync runs on tokio's blocking pool.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError::Io`] when a segment or the directory cannot be
    /// synced, and [`SpoolError::Queue`] once a failed recovery has closed the
    /// spool.
    pub async fn sync(&mut self) -> Result<()> {
        live(&mut self.queue)?;
        spool_codec::sync_queue(
            self.config.path.clone(),
            STORE,
            Arc::clone(&self.cell),
            None,
        )
        .await
        .map(|_| ())
        .map_err(SpoolError::Io)
    }

    /// Take one record of `written` bytes off the counts.
    fn taken(&mut self, written: usize) {
        self.len = self.len.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(written as u64);
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_queue_depth").set(self.len as f64);
    }

    /// Once the spool is empty, ends a drained segment so its file goes.
    fn release_drained(&mut self) {
        if self.len > 0 {
            return;
        }
        let Some(queue) = self.queue.as_mut() else {
            return;
        };
        let released = queue
            .sender
            .release_drained(queue.receiver.position())
            .and_then(|released| {
                if released {
                    queue.receiver.sweep()
                } else {
                    Ok(())
                }
            });
        if let Err(e) = released {
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %e, "cannot release the spool's drained segment");
            let _ = e;
        }
        self.metrics.disk(queue.sender.disk_bytes());
    }

    /// Reads the front record into the open transaction. `Ok(None)` when the
    /// queue is empty, or after a damaged header quarantined it.
    fn read_front(&mut self) -> Result<Option<usize>> {
        #[cfg(feature = "memory")]
        let guard = self.memory.as_deref();
        #[cfg(not(feature = "memory"))]
        let guard: Option<&MemoryGuard> = None;
        let queue = live(&mut self.queue)?;
        let read = queue.receiver.read(1, u64::MAX, guard);
        self.metrics
            .reserved(queue.receiver.reserved_bytes(), false);
        match read {
            Ok(taken) if taken.entries == 0 => Ok(None),
            Ok(_) => Ok(Some(queue.receiver.taken()[0].len())),
            Err(e) if spool_codec::is_damaged_header(&e) => {
                self.recover(
                    spool_codec::QuarantineTrigger::DamagedHeader,
                    &e.to_string(),
                )?;
                Ok(None)
            }
            Err(e) => Err(SpoolError::Io(e)),
        }
    }

    /// Ends the open transaction: commits the record read, or returns it.
    fn finish(&mut self, commit: bool) {
        #[cfg(feature = "memory")]
        let guard = self.memory.as_deref();
        #[cfg(not(feature = "memory"))]
        let guard: Option<&MemoryGuard> = None;
        if let Some(queue) = self.queue.as_mut() {
            queue.receiver.commit(usize::from(commit), guard);
            self.metrics
                .reserved(queue.receiver.reserved_bytes(), false);
        }
    }

    /// Decodes the record the open transaction holds into `out`, under the
    /// codec its entry names.
    fn decode_front(&mut self, out: &mut Vec<u8>) -> std::result::Result<(), Unreadable> {
        let Some(queue) = self.queue.as_ref() else {
            return Err(Unreadable::Codec("spool is closed".into()));
        };
        let entry =
            spool_codec::unframe(&queue.receiver.taken()[0]).map_err(Unreadable::Damaged)?;
        self.decoders
            .decode_into(&entry, out, spool_codec::PREVIOUS_DECODE_CAP)
    }

    /// Drops the record that failed its CRC, counting it. Under `Fail` the
    /// caller then returns [`SpoolError::Corrupted`] naming it.
    fn drop_damaged(&mut self, written: usize, error: &CorruptionError) -> Result<()> {
        self.finish(true);
        self.taken(written);
        self.release_drained();
        self.metrics.damaged(error);
        if self.config.on_corruption == CorruptionPolicy::Fail {
            return Err(SpoolError::Corrupted(error.to_string()));
        }
        Ok(())
    }

    /// Peek at the first item without removing it.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs. A record
    /// that fails its CRC is dropped and counted, and under the `Fail` policy
    /// returns [`SpoolError::Corrupted`]. Under `Quarantine` the peek goes on
    /// to the next record.
    pub async fn peek(&mut self) -> Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        loop {
            let Some(written) = self.read_front()? else {
                return Ok(None);
            };
            match self.decode_front(&mut out) {
                Ok(()) => {
                    self.finish(false);
                    return Ok(Some(out));
                }
                Err(Unreadable::Damaged(error)) => self.drop_damaged(written, &error)?,
                Err(Unreadable::Codec(reason)) => {
                    self.finish(false);
                    return Err(SpoolError::Decompression(reason));
                }
            }
        }
    }

    /// Remove the first item from the queue.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub async fn pop(&mut self) -> Result<()> {
        if let Some(written) = self.read_front()? {
            self.finish(true);
            self.taken(written);
            self.release_drained();
        }
        Ok(())
    }

    /// Pop and return the first item, atomically receiving and removing it.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs.
    pub async fn pop_front(&mut self) -> Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        Ok(self.pop_front_into(&mut out).await?.then_some(out))
    }

    /// Pop the first item into `out`, replacing its contents. `Ok(false)` when
    /// the queue is empty. Reusing `out` across calls, a pop allocates nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs. A record
    /// that fails its CRC is dropped and counted, and under the `Fail` policy
    /// returns [`SpoolError::Corrupted`]. Under `Quarantine` the pop goes on to
    /// the next record.
    pub async fn pop_front_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        loop {
            let Some(written) = self.read_front()? else {
                return Ok(false);
            };
            match self.decode_front(out) {
                Ok(()) => {
                    self.finish(true);
                    self.taken(written);
                    self.release_drained();
                    return Ok(true);
                }
                Err(Unreadable::Damaged(error)) => self.drop_damaged(written, &error)?,
                Err(Unreadable::Codec(reason)) => {
                    self.finish(false);
                    return Err(SpoolError::Decompression(reason));
                }
            }
        }
    }

    /// Receive an item, awaiting if the queue is empty. Preferred consumer API.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs. A record
    /// that fails its CRC is dropped and counted, and under the `Fail` policy
    /// returns [`SpoolError::Corrupted`] once, naming it.
    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        loop {
            let mut out = Vec::new();
            if self.pop_front_into(&mut out).await? {
                return Ok(out);
            }
            let receiver = &mut live(&mut self.queue)?.receiver;
            std::future::poll_fn(|cx| receiver.poll_ready(cx)).await;
        }
    }

    /// Approximate item count, tracked internally.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Drain all items from the queue.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn clear(&mut self) -> Result<()> {
        let queue = live(&mut self.queue)?;
        loop {
            // Bounded reads, so a large backlog is never in memory at once.
            let taken = queue
                .receiver
                .read(1024, 4 << 20, None)
                .map_err(SpoolError::Io)?;
            if taken.entries == 0 {
                break;
            }
            queue.receiver.commit(taken.entries, None);
        }
        self.len = 0;
        self.bytes = 0;
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_queue_depth").set(0.0);
        self.release_drained();
        Ok(())
    }

    /// Get the configuration for this spool.
    #[must_use]
    pub fn config(&self) -> &SpoolConfig {
        &self.config
    }

    /// Payload bytes of the records queued, the count `max_size_bytes` caps.
    #[must_use]
    pub fn queued_bytes(&self) -> u64 {
        self.bytes
    }

    /// Get the approximate directory size in bytes, consumed records still in
    /// their segment included. Walks the directory with blocking calls.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be read.
    pub fn file_size(&self) -> Result<u64> {
        let mut total = 0u64;
        if self.config.path.is_dir() {
            for entry in std::fs::read_dir(&self.config.path)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    total += entry.metadata()?.len();
                }
            }
        }
        Ok(total)
    }

    /// Stop the timed sync, after one last sync of everything pushed.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError::Io`] when the last sync fails.
    pub async fn close(mut self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(ticker) = self.ticker.take() {
            ticker
                .join()
                .await
                .map_err(|e| SpoolError::Queue(format!("spool sync task failed: {e}")))?;
        } else {
            self.sync().await?;
        }
        Ok(())
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        // The timed sync's shutdown runs one last sync.
        self.shutdown.cancel();
    }
}

/// The queue options a spool opens with.
fn options(config: &SpoolConfig) -> yaque::Options {
    yaque::Options {
        segment_bytes: config.segment_bytes,
        read_buffer_bytes: config.read_buffer_bytes,
        recycle_bytes: config.read_buffer_bytes,
        whole: Some(spool_codec::whole),
        damaged_tail: Some(spool_codec::keep_damaged_tail),
        shape: Some(spool_codec::shape),
        store: STORE,
    }
}

/// Open the queue through the shared lock and quarantine path.
fn open_queue(
    path: &Path,
    policy: CorruptionPolicy,
    options: yaque::Options,
) -> Result<(Sender, Receiver)> {
    spool_codec::open_queue(path, policy, options).map_err(|e| SpoolError::Open {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

impl std::fmt::Debug for Spool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spool")
            .field("path", &self.config.path)
            .field("len", &self.len)
            .field("compress", &self.config.compress)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_create_and_push_pop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-queue");

        let mut spool = Spool::create(&path).await.unwrap();
        assert!(spool.is_empty());

        spool.push(b"hello").await.unwrap();
        spool.push(b"world").await.unwrap();

        assert_eq!(spool.len(), 2);
        assert!(!spool.is_empty());

        assert_eq!(spool.pop_front().await.unwrap(), Some(b"hello".to_vec()));
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"world".to_vec()));

        assert!(spool.is_empty());
    }

    /// `max_size_bytes` caps the bytes queued, counted as records come and
    /// go: draining a full spool makes room again, however much of its segment
    /// file the drained records still occupy. A reopen counts what is queued.
    #[tokio::test]
    async fn the_size_cap_counts_queued_bytes_not_the_directory() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("capped");
        let config = SpoolConfig::new(&path).max_size_bytes(1024);
        let record = [b'r'; 100];

        let mut spool = Spool::open(config.clone()).await.unwrap();
        let mut pushed = 0;
        while spool.push(&record).await.is_ok() {
            pushed += 1;
            assert!(pushed < 100, "the 1 KiB cap never refused a push");
        }
        assert!(pushed > 0, "the first push fits the cap");
        for _ in 0..pushed {
            assert!(spool.pop_front().await.unwrap().is_some());
        }
        assert!(spool.is_empty());
        spool
            .push(&record)
            .await
            .expect("an emptied spool has room under its cap");

        drop(spool);
        let mut reopened = Spool::open(config).await.unwrap();
        assert_eq!(reopened.len(), 1);
        for _ in 1..pushed {
            reopened.push(&record).await.expect("room for the rest");
        }
        assert!(
            matches!(
                reopened.push(&record).await,
                Err(SpoolError::MaxSizeReached { .. })
            ),
            "the reopened spool counts the record it already holds"
        );
    }

    #[tokio::test]
    async fn test_pop_front_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-queue");

        let mut spool = Spool::create(&path).await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-queue");

        let mut spool = Spool::create_compressed(&path).await.unwrap();

        let data = b"hello world ".repeat(100);
        spool.push(&data).await.unwrap();

        // Verify decompression works - data comes back correctly
        let retrieved = spool.pop_front().await.unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_max_items_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-queue");

        let config = SpoolConfig::new(&path).max_items(2);
        let mut spool = Spool::open(config).await.unwrap();

        spool.push(b"one").await.unwrap();
        spool.push(b"two").await.unwrap();

        let result = spool.push(b"three").await;
        assert!(matches!(
            result,
            Err(SpoolError::MaxItemsReached { max: 2 })
        ));
    }

    #[tokio::test]
    async fn test_clear() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-queue");

        let mut spool = Spool::create(&path).await.unwrap();
        spool.push(b"one").await.unwrap();
        spool.push(b"two").await.unwrap();

        assert_eq!(spool.len(), 2);
        spool.clear().unwrap();
        assert!(spool.is_empty());
    }

    #[tokio::test]
    async fn test_len_survives_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-reopen-queue");

        // Open, push items, then drop
        {
            let mut spool = Spool::create(&path).await.unwrap();
            spool.push(b"one").await.unwrap();
            spool.push(b"two").await.unwrap();
            spool.push(b"three").await.unwrap();
            assert_eq!(spool.len(), 3);
        }

        // Reopen -- len should reflect existing items
        {
            let spool = Spool::create(&path).await.unwrap();
            assert_eq!(spool.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_len_survives_partial_consume_and_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-partial-queue");

        // Open, push 5, consume 2
        {
            let mut spool = Spool::create(&path).await.unwrap();
            for i in 0..5 {
                spool.push(format!("item-{i}").as_bytes()).await.unwrap();
            }
            assert_eq!(spool.len(), 5);
            spool.pop_front().await.unwrap(); // consume 1
            spool.pop_front().await.unwrap(); // consume 2
            assert_eq!(spool.len(), 3);
        }

        // Reopen -- should show 3 remaining
        {
            let spool = Spool::create(&path).await.unwrap();
            assert_eq!(spool.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_data_survives_unclean_restart() {
        // Crash recovery: push distinct payloads, drop the spool WITHOUT a clean
        // drain (simulating a process kill), reopen, and read every payload back
        // in order with exact bytes. Proves the disk queue recovers DATA, not
        // just the count -- the durability contract behind the cold path.
        let dir = tempdir().unwrap();
        let path = dir.path().join("crash-queue");

        let payloads: Vec<Vec<u8>> = (0..8)
            .map(|i| format!("payload-{i}").into_bytes())
            .collect();
        {
            let mut spool = Spool::create(&path).await.unwrap();
            for p in &payloads {
                spool.push(p).await.unwrap();
            }
            // Drop here = unclean stop: no clear(), no graceful shutdown.
        }

        let mut spool = Spool::create(&path).await.unwrap();
        assert_eq!(
            spool.len(),
            payloads.len(),
            "all items recovered after restart"
        );
        for expected in &payloads {
            let got = spool.pop_front().await.unwrap();
            assert_eq!(
                got.as_ref(),
                Some(expected),
                "exact payload recovered in FIFO order"
            );
        }
        assert!(spool.is_empty());
    }

    #[tokio::test]
    async fn test_peek_rolls_back_and_survives_restart() {
        // Transactional read: peek() does an uncommitted try_recv and lets the
        // guard roll back on drop, so the item is NOT consumed. After a restart
        // the peeked item must still be present and re-readable -- this is the
        // at-least-once property at the spool layer (an in-flight item that was
        // never committed is redelivered, never silently lost).
        let dir = tempdir().unwrap();
        let path = dir.path().join("rollback-queue");

        {
            let mut spool = Spool::create(&path).await.unwrap();
            spool.push(b"alpha").await.unwrap();
            spool.push(b"beta").await.unwrap();

            // Peek does not consume.
            assert_eq!(spool.peek().await.unwrap(), Some(b"alpha".to_vec()));
            assert_eq!(spool.len(), 2, "peek must not decrement the queue");
        }

        // Reopen: the un-committed peek rolled back, so both items remain.
        let mut spool = Spool::create(&path).await.unwrap();
        assert_eq!(
            spool.len(),
            2,
            "rolled-back peek leaves both items after restart"
        );
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"alpha".to_vec()));
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"beta".to_vec()));
    }

    #[tokio::test]
    async fn test_compressed_data_survives_restart() {
        // Compression + crash recovery together: a zstd-compressed payload must
        // round-trip across a restart (the compressed bytes are what land on
        // disk, so this also exercises the on-disk-then-decompress path).
        let dir = tempdir().unwrap();
        let path = dir.path().join("compressed-crash-queue");

        let data = b"the quick brown fox ".repeat(64);
        {
            let mut spool = Spool::create_compressed(&path).await.unwrap();
            spool.push(&data).await.unwrap();
        }

        let mut spool = Spool::create_compressed(&path).await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(data));
    }

    #[tokio::test]
    async fn test_crc_roundtrip_and_survives_restart() {
        // CRC-enabled spool: push/pop round-trips, and the framed record (with
        // its checksum header) survives an unclean restart.
        let dir = tempdir().unwrap();
        let path = dir.path().join("crc-queue");
        let payload = b"integrity-protected payload".to_vec();
        {
            let mut spool = Spool::open(SpoolConfig::new(&path).crc(true))
                .await
                .unwrap();
            spool.push(&payload).await.unwrap();
        }
        let mut spool = Spool::open(SpoolConfig::new(&path).crc(true))
            .await
            .unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(payload));
    }

    /// A record damaged on disk is dropped and counted, and the records on
    /// either side of it still come back in order. `Fail` also returns
    /// `Corrupted` once for it. Nothing is quarantined.
    #[tokio::test]
    async fn a_damaged_record_is_dropped_counted_and_the_rest_read() {
        for policy in [CorruptionPolicy::Quarantine, CorruptionPolicy::Fail] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("damaged");
            let records: [&[u8]; 3] = [b"record-0", b"record-1", b"record-2"];
            let config = SpoolConfig::new(&path).on_corruption(policy);
            let mut spool = Spool::open(config.clone()).await.unwrap();
            for record in records {
                spool.push(record).await.unwrap();
            }
            drop(spool);
            // One body byte of the middle record, past its 4-byte header and its prefix.
            let entry = 4 + spool_codec::PREFIX_LEN + records[0].len();
            let segment = path.join("0.q");
            let mut bytes = std::fs::read(&segment).unwrap();
            bytes[entry + 4 + spool_codec::PREFIX_LEN + 1] ^= 0xFF;
            std::fs::write(&segment, &bytes).unwrap();

            #[cfg(feature = "metrics")]
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            #[cfg(feature = "metrics")]
            let _metrics = ::metrics::set_default_local_recorder(&recorder);
            let mut spool = Spool::open(config).await.unwrap();
            assert_eq!(spool.pop_front().await.unwrap(), Some(records[0].to_vec()));
            if policy == CorruptionPolicy::Fail {
                assert!(
                    matches!(spool.pop_front().await, Err(SpoolError::Corrupted(_))),
                    "Fail returns the damage once"
                );
            }
            assert_eq!(
                spool.pop_front().await.unwrap(),
                Some(records[2].to_vec()),
                "{policy:?}: the read goes on past it"
            );
            assert_eq!(spool.pop_front().await.unwrap(), None, "{policy:?}");
            #[cfg(feature = "metrics")]
            assert!(
                recorder
                    .handle()
                    .render()
                    .contains("spool_damaged_entries_total{store=\"spool\"} 1"),
                "{policy:?}: counted once"
            );
            let quarantined = std::fs::read_dir(&path)
                .unwrap()
                .flatten()
                .any(|e| e.file_name().to_string_lossy().starts_with("corrupt-"));
            assert!(!quarantined, "{policy:?}: nothing quarantined");
        }
    }

    /// A spool in the format before opens, restamped: its records read back,
    /// then the ones pushed after them, and one that decodes past the cap is
    /// dropped as damaged.
    #[tokio::test]
    async fn a_spool_of_the_format_before_reads_on() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("before");
        let oversized = vec![b'o'; spool_codec::PREVIOUS_DECODE_CAP + 1];
        {
            let options = yaque::Options {
                shape: None,
                ..options(&SpoolConfig::new(&path))
            };
            let (mut sender, _receiver) = yaque::channel(&path, options).unwrap();
            for (codec, record) in [
                (Codec::None, &b"before-0"[..]),
                (Codec::Zstd, &oversized[..]),
                (Codec::Zstd, b"before-1"),
            ] {
                let mut body = Vec::new();
                Compressor::new(Compression::new(codec))
                    .unwrap()
                    .compress_into(record, &mut body)
                    .unwrap();
                let prefix = spool_codec::previous_prefix(codec, true, 1, &body);
                sender.send(&prefix, &body).unwrap();
            }
        }
        let stamp = path.join("version").join("contents");
        std::fs::write(&stamp, "0.7.0\n").unwrap();

        let mut spool = Spool::open(SpoolConfig::with_compression(&path))
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&stamp).unwrap().trim(), "0.8.0");
        assert_eq!(spool.len(), 3);
        spool.push(b"after").await.unwrap();
        let mut read = Vec::new();
        while let Some(record) = spool.pop_front().await.unwrap() {
            read.push(record);
        }
        assert_eq!(read, [&b"before-0"[..], b"before-1", b"after"]);
    }

    /// A damaged header in the newest segment keeps every record before it.
    /// The open copies the segment from that header on into a `corrupt-*`
    /// directory, cuts it off, and takes new records after it.
    #[tokio::test]
    async fn a_damaged_header_in_the_newest_segment_is_kept_aside_at_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("damaged-header");
        let config = SpoolConfig::new(&path);
        let records: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("record-{i}").into_bytes())
            .collect();
        let mut spool = Spool::open(config.clone()).await.unwrap();
        for record in &records {
            spool.push(record).await.unwrap();
        }
        drop(spool);
        let entry = 4 + spool_codec::PREFIX_LEN + records[0].len();
        let segment = path.join("0.q");
        let mut bytes = std::fs::read(&segment).unwrap();
        // A parity bit of record 4's header.
        bytes[4 * entry] ^= 0x04;
        std::fs::write(&segment, &bytes).unwrap();

        let mut spool = Spool::open(config).await.unwrap();
        assert_eq!(spool.len(), 4);
        for record in &records[..4] {
            assert_eq!(spool.pop_front().await.unwrap().as_ref(), Some(record));
        }
        assert_eq!(spool.pop_front().await.unwrap(), None);
        let kept: Vec<_> = std::fs::read_dir(&path)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("corrupt-"))
            .collect();
        assert_eq!(kept.len(), 1, "one corrupt-* directory");
        assert_eq!(
            std::fs::read(kept[0].path().join("0.q")).unwrap(),
            bytes[4 * entry..],
            "the bytes from the damaged header on are kept"
        );
        spool.push(b"after").await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"after".to_vec()));
    }

    /// A header damaged on disk quarantines the queue on read. The old
    /// handles close before the fresh queue opens, so their drop can neither
    /// release the fresh locks nor overwrite its saved position.
    #[tokio::test]
    async fn test_recovered_queue_keeps_its_locks_and_position() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("spill-cache");
        // A segment per record, so the damaged one is not the newest, which the open would cut.
        let mut config = SpoolConfig::new(&path);
        config.segment_bytes = 16;
        {
            let mut spool = Spool::open(config.clone()).await.unwrap();
            for record in [&b"consumed first"[..], b"poisoned record", b"third"] {
                spool.push(record).await.unwrap();
            }
            assert_eq!(
                spool.pop_front().await.unwrap(),
                Some(b"consumed first".to_vec())
            );
        }
        let segment = path.join("1.q");
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes[0] ^= 0x04;
        std::fs::write(&segment, &bytes).unwrap();

        let mut spool = Spool::open(config.clone()).await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), None, "recovered to empty");
        assert!(path.join("send.lock").exists(), "fresh sender lock held");
        assert!(path.join("recv.lock").exists(), "fresh receiver lock held");
        assert!(
            matches!(
                Spool::open(config.clone()).await,
                Err(SpoolError::Open { .. })
            ),
            "a second open of the recovered queue is refused"
        );

        spool.push(b"after recovery").await.unwrap();
        drop(spool);
        let mut spool = Spool::open(config).await.unwrap();
        assert_eq!(
            spool.pop_front().await.unwrap(),
            Some(b"after recovery".to_vec()),
            "the fresh queue's position survives a restart"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_reopens_after_a_stale_lock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stale-lock-queue");
        {
            let mut spool = Spool::create(&path).await.unwrap();
            spool.push(b"kept").await.unwrap();
        }
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead = child.id();
        child.wait().unwrap();
        std::fs::write(path.join("send.lock"), format!("pid={dead}\ntoken=1\n")).unwrap();
        std::fs::write(path.join("recv.lock"), format!("pid={dead}\ntoken=1\n")).unwrap();

        let mut spool = Spool::create(&path).await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"kept".to_vec()));
    }

    #[tokio::test]
    async fn test_crc_with_compression() {
        // CRC frames the COMPRESSED bytes (what lands on disk). The two compose.
        let dir = tempdir().unwrap();
        let path = dir.path().join("crc-zstd-queue");
        let data = b"compressible ".repeat(50);
        let mut spool = Spool::open(SpoolConfig::new(&path).compress(true).crc(true))
            .await
            .unwrap();
        spool.push(&data).await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(data));
    }

    /// yaque saves the receive position by truncating `recv-metadata` and then
    /// writing 16 bytes, so a process killed between the two leaves it short,
    /// and a power loss can leave it pointing at a segment that is gone. Either
    /// way the queue refused to open, and the quarantine policy moved every
    /// spooled record aside. The queue now reopens with its records, replaying
    /// from its first segment: duplicates, never loss.
    #[tokio::test]
    async fn a_lost_receive_position_replays_the_queue_instead_of_quarantining() {
        let mut gone = 7_u64.to_be_bytes().to_vec();
        gone.extend_from_slice(&0_u64.to_be_bytes());
        let cases: [(&str, Vec<u8>); 3] = [
            ("empty", Vec::new()),
            ("half written", vec![0; 8]),
            ("a segment that is gone", gone),
        ];
        for (name, torn) in cases {
            let dir = tempdir().unwrap();
            let path = dir.path().join("torn");
            let mut spool = Spool::create(&path).await.unwrap();
            for record in [b"a", b"b", b"c"] {
                spool.push(record).await.unwrap();
            }
            assert_eq!(spool.pop_front().await.unwrap(), Some(b"a".to_vec()));
            drop(spool);
            std::fs::write(path.join("recv-metadata"), torn).unwrap();

            let mut reopened = Spool::create(&path)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let mut replayed = Vec::new();
            while let Some(record) = reopened.pop_front().await.unwrap() {
                replayed.push(record);
            }
            assert!(
                replayed.ends_with(&[b"b".to_vec(), b"c".to_vec()]),
                "{name}: replayed {replayed:?}"
            );
            let quarantined = std::fs::read_dir(&path)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(|entry| entry.file_name().to_string_lossy().starts_with("corrupt-"));
            assert!(!quarantined, "{name}: the queue was quarantined");
        }
    }

    /// yaque panics on a version file that is not a version or names another
    /// release, and on a `.q` file that is not a segment number, which ends a
    /// service built with `panic = "abort"`. Each is an open failure instead:
    /// `fail` returns an error naming the file, and `quarantine` sets the queue
    /// aside, opens a fresh one and keeps the corrupt files. A queue in another
    /// spool format is refused under both.
    #[tokio::test]
    async fn a_queue_yaque_would_panic_on_is_an_open_failure() {
        let version = std::path::Path::new("version").join("contents");
        let version_named = version.display().to_string();
        // (case, text for the version file or None for a stray `.q` file, what the error names)
        let cases: [(&str, Option<&str>, &str); 6] = [
            ("not a version", Some("not a version"), &version_named),
            ("the format before", Some("0.6.6"), &version_named),
            ("an older format", Some("0.5.2"), &version_named),
            ("only whitespace", Some("\n"), &version_named),
            ("not a segment", None, "foo.q"),
            (
                "not a segment named like a quarantine",
                None,
                "corrupt-old.q",
            ),
        ];
        for (name, version_text, named) in cases {
            let dir = tempdir().unwrap();
            let path = dir.path().join("queue");
            let mut spool = Spool::create(&path).await.unwrap();
            spool.push(b"kept").await.unwrap();
            drop(spool);
            match version_text {
                Some(text) => std::fs::write(path.join(&version), text).unwrap(),
                None => std::fs::write(path.join(named), b"x").unwrap(),
            }

            let failed = Spool::open(SpoolConfig::new(&path).on_corruption(CorruptionPolicy::Fail))
                .await
                .expect_err(name);
            assert!(
                matches!(&failed, SpoolError::Open { message, .. } if message.contains(named)),
                "{name}: {failed}"
            );

            let opened = Spool::open(SpoolConfig::new(&path)).await;
            if version_text.is_some_and(|text| text.starts_with("0.")) {
                // Another spool format is refused under quarantine too, and nothing moves.
                assert!(
                    matches!(&opened, Err(SpoolError::Open { message, .. }) if message.contains("spool format")),
                    "{name}: {opened:?}"
                );
                assert!(path.join("0.q").exists(), "{name}: records left in place");
                continue;
            }
            let mut fresh = opened.unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(fresh.is_empty(), "{name}: a fresh queue");
            fresh.push(b"new").await.unwrap();
            assert_eq!(fresh.pop_front().await.unwrap(), Some(b"new".to_vec()));
            let quarantined: Vec<_> = std::fs::read_dir(&path)
                .unwrap()
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with("corrupt-"))
                .collect();
            assert_eq!(quarantined.len(), 1, "{name}: one quarantine directory");
            assert!(
                quarantined[0].path().join("0.q").exists(),
                "{name}: records kept"
            );
        }
    }

    /// `sync` and `sync_on_push` leave the queue as it was: a sync of an empty
    /// or a part-consumed spool succeeds, and the records still come back in
    /// order.
    #[tokio::test]
    async fn sync_leaves_the_queue_readable() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open(SpoolConfig::new(dir.path().join("batched")))
            .await
            .unwrap();
        spool.sync().await.unwrap();
        spool.push(b"a").await.unwrap();
        spool.push(b"b").await.unwrap();
        spool.sync().await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"a".to_vec()));
        spool.sync().await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(b"b".to_vec()));

        let config = SpoolConfig::new(dir.path().join("eager")).sync_on_push(true);
        assert!(config.sync_on_push);
        let mut eager = Spool::open(config).await.unwrap();
        for record in [b"x", b"y", b"z"] {
            eager.push(record).await.unwrap();
        }
        for record in [b"x", b"y", b"z"] {
            assert_eq!(eager.pop_front().await.unwrap(), Some(record.to_vec()));
        }
    }

    #[tokio::test]
    async fn test_debug_format() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-queue");

        let spool = Spool::create(&path).await.unwrap();
        let debug = format!("{spool:?}");
        assert!(debug.contains("Spool"));
        assert!(debug.contains("test-queue"));
    }
}
