// Project:   scalo
// File:      src/spool/queue.rs
// Purpose:   Disk-backed async FIFO queue implementation using yaque
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Disk-backed async FIFO queue implementation.

use crate::spool::{CorruptionPolicy, Result, SpoolConfig, SpoolError};
use std::path::Path;
use yaque::{Receiver, Sender};

/// Disk-backed async FIFO queue with optional compression.
///
/// Crash-safe writes, survives restarts. Built on
/// [yaque](https://crates.io/crates/yaque) (transactional persistent queue).
pub struct Spool {
    sender: Sender,
    receiver: Receiver,
    config: SpoolConfig,
    len: usize,
}

impl Spool {
    /// Open the queue at the configured path, creating it if absent.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue cannot be opened or created.
    pub async fn open(config: SpoolConfig) -> Result<Self> {
        let (sender, receiver) = match yaque::channel(&config.path) {
            Ok(channel) => channel,
            // The cache won't open (corrupt segments / metadata). Under the
            // default Quarantine policy, move it aside and start fresh so a
            // poisoned spill cache can never wedge startup.
            Err(e) if config.on_corruption == CorruptionPolicy::Quarantine => {
                let moved = quarantine_dir(&config.path)?;
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    path = %config.path.display(),
                    quarantined = ?moved,
                    error = %e,
                    "spool cache could not be opened; quarantined and starting fresh"
                );
                // `moved` + `e` are read only by the tracing warn! above; reference
                // both so the no-tracing build (feature `spool` without `tracing`)
                // doesn't flag them as unused under -D warnings.
                let _ = (&moved, &e);
                yaque::channel(&config.path).map_err(|e2| SpoolError::Open {
                    path: config.path.display().to_string(),
                    message: e2.to_string(),
                })?
            }
            Err(e) => {
                return Err(SpoolError::Open {
                    path: config.path.display().to_string(),
                    message: e.to_string(),
                });
            }
        };

        // yaque exposes no count API -- parse segment files to count items
        // between the receiver position and the end.
        let len = count_existing_items(&config.path).unwrap_or(0);

        Ok(Self {
            sender,
            receiver,
            config,
            len,
        })
    }

    /// Quarantine the current (corrupt) cache and reopen a fresh empty queue.
    ///
    /// Renames the cache directory aside to `<path>.corrupt-YYYYMMDD-HHMMSS`
    /// (forensics preserved) and rebuilds the sender/receiver on a fresh queue.
    /// Used by the read paths when a CRC check fails under the Quarantine policy.
    /// Returns the quarantined path (if the dir existed).
    fn recover(&mut self, reason: &str) -> Result<Option<std::path::PathBuf>> {
        let moved = quarantine_dir(&self.config.path)?;
        let (sender, receiver) =
            yaque::channel(&self.config.path).map_err(|e| SpoolError::Open {
                path: self.config.path.display().to_string(),
                message: e.to_string(),
            })?;
        // Reassigning drops the old handles (closing the now-renamed dir's files).
        self.sender = sender;
        self.receiver = receiver;
        self.len = 0;
        #[cfg(feature = "tracing")]
        tracing::warn!(
            path = %self.config.path.display(),
            quarantined = ?moved,
            reason,
            "spool corruption detected; quarantined and started fresh"
        );
        let _ = reason;
        Ok(moved)
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
    /// # Errors
    ///
    /// Errors on size/item-count cap, compression failure, or I/O.
    pub async fn push(&mut self, data: &[u8]) -> Result<()> {
        if let Some(max) = self.config.max_items
            && self.len >= max
        {
            return Err(SpoolError::MaxItemsReached { max });
        }

        // Approximate -- checked before write.
        if let Some(max_bytes) = self.config.max_size_bytes
            && self.file_size()? >= max_bytes
        {
            return Err(SpoolError::MaxSizeReached { max_bytes });
        }

        let body = if self.config.compress {
            self.compress(data)?
        } else {
            data.to_vec()
        };
        let to_write = Self::frame(self.config.crc, body);

        self.sender
            .send(to_write)
            .await
            .map_err(|e| SpoolError::Queue(e.to_string()))?;

        self.len += 1;
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_queue_depth").set(self.len as f64);
        Ok(())
    }

    /// Apply the configured [`CorruptionPolicy`] to a read-time error.
    ///
    /// Under `Quarantine`, a [`SpoolError::Corrupted`] triggers
    /// [`recover`](Self::recover) (the corrupt cache is moved aside, a fresh
    /// queue takes its place) and returns `Ok(())` so the caller reports an empty
    /// queue. Any other error, or the `Fail` policy, propagates unchanged.
    fn recover_on_read(&mut self, e: SpoolError) -> Result<()> {
        if self.config.on_corruption == CorruptionPolicy::Quarantine
            && matches!(e, SpoolError::Corrupted(_))
        {
            self.recover("CRC mismatch on read")?;
            Ok(())
        } else {
            Err(e)
        }
    }

    /// Peek at the first item without removing it.
    ///
    /// yaque has no direct peek -- `try_recv` then let the guard roll back
    /// on drop to leave the item in the queue.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs. A CRC
    /// failure under `Fail` policy returns [`SpoolError::Corrupted`]; under
    /// `Quarantine` the cache is recovered and `Ok(None)` is returned.
    pub async fn peek(&mut self) -> Result<Option<Vec<u8>>> {
        // `step` is owned, so the receiver borrow (held by the guard + the match
        // scrutinee) is fully released before any `recover_on_read` call.
        let step: Option<Result<Vec<u8>>> = match self.receiver.try_recv() {
            Ok(guard) => {
                let outcome = Self::unframe(self.config.crc, guard.to_vec())
                    .and_then(|body| Self::decode(self.config.compress, body));
                // No commit -- guard rollback on drop keeps the item.
                drop(guard);
                Some(outcome)
            }
            Err(yaque::TryRecvError::Io(e)) => Some(Err(SpoolError::Io(e))),
            Err(yaque::TryRecvError::QueueEmpty) => None,
        };
        match step {
            Some(Ok(data)) => Ok(Some(data)),
            Some(Err(e)) => self.recover_on_read(e).map(|()| None),
            None => Ok(None),
        }
    }

    /// Remove the first item from the queue.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub async fn pop(&mut self) -> Result<()> {
        match self.receiver.try_recv() {
            Ok(guard) => {
                guard
                    .commit()
                    .map_err(|e| SpoolError::Queue(e.to_string()))?;
                self.len = self.len.saturating_sub(1);
                Ok(())
            }
            Err(yaque::TryRecvError::Io(e)) => Err(SpoolError::Io(e)),
            Err(yaque::TryRecvError::QueueEmpty) => Ok(()), // Nothing to pop
        }
    }

    /// Pop and return the first item, atomically receiving and removing it.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs.
    pub async fn pop_front(&mut self) -> Result<Option<Vec<u8>>> {
        let step: Option<Result<Vec<u8>>> = match self.receiver.try_recv() {
            Ok(guard) => {
                let decoded = Self::unframe(self.config.crc, guard.to_vec())
                    .and_then(|body| Self::decode(self.config.compress, body));
                match decoded {
                    Ok(data) => match guard.commit() {
                        Ok(()) => Some(Ok(data)),
                        Err(e) => Some(Err(SpoolError::Queue(e.to_string()))),
                    },
                    Err(e) => {
                        drop(guard); // roll back -- leave the item for recovery
                        Some(Err(e))
                    }
                }
            }
            Err(yaque::TryRecvError::Io(e)) => Some(Err(SpoolError::Io(e))),
            Err(yaque::TryRecvError::QueueEmpty) => None,
        };
        match step {
            Some(Ok(data)) => {
                self.len = self.len.saturating_sub(1);
                #[cfg(feature = "metrics")]
                ::metrics::gauge!("spool_queue_depth").set(self.len as f64);
                Ok(Some(data))
            }
            Some(Err(e)) => self.recover_on_read(e).map(|()| None),
            None => Ok(None),
        }
    }

    /// Receive an item, awaiting if the queue is empty. Preferred consumer API.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression fails or an I/O error occurs.
    pub async fn recv(&mut self) -> Result<Vec<u8>> {
        let guard = self
            .receiver
            .recv()
            .await
            .map_err(|e| SpoolError::Queue(e.to_string()))?;

        // `guard` is a let-binding (not a match temporary), so dropping it fully
        // releases the receiver borrow before any recover.
        let decoded = Self::unframe(self.config.crc, guard.to_vec())
            .and_then(|body| Self::decode(self.config.compress, body));
        let data = match decoded {
            Ok(data) => {
                guard
                    .commit()
                    .map_err(|e| SpoolError::Queue(e.to_string()))?;
                data
            }
            Err(e) => {
                drop(guard);
                // recv has no Option for "empty": under Quarantine we recover
                // (cache moved aside, fresh queue) then still surface the
                // corruption once so the caller knows the in-flight record was lost.
                self.recover_on_read(e)?;
                return Err(SpoolError::Corrupted(
                    "recovered from corrupt cache; the in-flight record was lost".into(),
                ));
            }
        };
        self.len = self.len.saturating_sub(1);
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_queue_depth").set(self.len as f64);
        Ok(data)
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
        // yaque has no built-in clear -- drain by committing every item.
        loop {
            match self.receiver.try_recv() {
                Ok(guard) => {
                    guard
                        .commit()
                        .map_err(|e| SpoolError::Queue(e.to_string()))?;
                }
                Err(yaque::TryRecvError::QueueEmpty) => break,
                Err(yaque::TryRecvError::Io(e)) => return Err(SpoolError::Io(e)),
            }
        }
        self.len = 0;
        #[cfg(feature = "metrics")]
        ::metrics::gauge!("spool_queue_depth").set(0.0);
        Ok(())
    }

    /// Get the configuration for this spool.
    #[must_use]
    pub fn config(&self) -> &SpoolConfig {
        &self.config
    }

    /// Get the approximate directory size in bytes.
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

    /// Compress data using zstd.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(data, self.config.compression_level)
            .map_err(|e| SpoolError::Compression(e.to_string()))
    }

    /// CRC32C framing -- delegates to the shared [`spool_codec`](crate::spool_codec).
    fn frame(crc: bool, body: Vec<u8>) -> Vec<u8> {
        crate::spool_codec::frame(crc, body)
    }

    /// Decompress the record body when compression is enabled.
    fn decode(compress: bool, body: Vec<u8>) -> Result<Vec<u8>> {
        if compress {
            zstd::decode_all(body.as_slice()).map_err(|e| SpoolError::Decompression(e.to_string()))
        } else {
            Ok(body)
        }
    }

    /// Verify+strip the CRC header (shared logic); a checksum failure becomes
    /// [`SpoolError::Corrupted`].
    fn unframe(crc: bool, raw: Vec<u8>) -> Result<Vec<u8>> {
        crate::spool_codec::unframe(crc, raw).map_err(|e| SpoolError::Corrupted(e.0))
    }
}

/// Rename a corrupt cache directory aside (shared logic), mapping any I/O error
/// into [`SpoolError`].
fn quarantine_dir(path: &Path) -> Result<Option<std::path::PathBuf>> {
    Ok(crate::spool_codec::quarantine_dir(path)?)
}

/// Count items in a yaque queue dir by walking segment files.
///
/// yaque stores messages as `[4-byte Hamming header][payload]` in `<n>.q`
/// segments; receiver position lives in `recv-metadata`. Count from the
/// receiver position to the end of the highest segment.
fn count_existing_items(path: &std::path::Path) -> std::io::Result<usize> {
    if !path.is_dir() {
        return Ok(0);
    }

    // Read receiver state from recv-metadata (two big-endian u64: segment, position)
    let recv_metadata_path = path.join("recv-metadata");
    let (recv_segment, recv_position) = if recv_metadata_path.exists() {
        let data = std::fs::read(&recv_metadata_path)?;
        if data.len() >= 16 {
            let segment = u64::from_be_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            let position = u64::from_be_bytes(data[8..16].try_into().unwrap_or([0; 8]));
            (segment, position)
        } else {
            (0, 0)
        }
    } else {
        (0, 0)
    };

    // Collect all segment numbers
    let mut segments: Vec<u64> = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        if file_path.extension().and_then(|e| e.to_str()) == Some("q")
            && let Some(stem) = file_path.file_stem().and_then(|s| s.to_str())
            && let Ok(seg_num) = stem.parse::<u64>()
            && seg_num >= recv_segment
        {
            segments.push(seg_num);
        }
    }
    segments.sort_unstable();

    let mut count = 0usize;
    // Header EOF marker in yaque
    let header_eof: [u8; 4] = [255, 255, 255, 255];

    for &seg_num in &segments {
        let seg_path = path.join(format!("{seg_num}.q"));
        let file_data = std::fs::read(&seg_path)?;

        // Start position: if this is the receiver's segment, skip to receiver position
        #[allow(clippy::cast_possible_truncation)]
        let start = if seg_num == recv_segment {
            recv_position as usize
        } else {
            0
        };

        let mut pos = start;
        while pos + 4 <= file_data.len() {
            let header_bytes: [u8; 4] = file_data[pos..pos + 4].try_into().unwrap_or([0; 4]);

            // Check for EOF marker
            if header_bytes == header_eof {
                break; // End of segment, move to next
            }

            // Decode length from Hamming-encoded header (lower 26 bits)
            let encoded = u32::from_be_bytes(header_bytes);
            let payload_len = (encoded & 0x03_FF_FF_FF) as usize;

            pos += 4 + payload_len;
            if pos <= file_data.len() {
                count += 1;
            }
        }
    }

    Ok(count)
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

    /// Corrupt one payload byte in a CRC-enabled spool's segment file.
    fn flip_payload_byte(path: &std::path::Path) {
        let seg = path.join("0.q");
        let mut bytes = std::fs::read(&seg).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&seg, &bytes).unwrap();
    }

    #[tokio::test]
    async fn test_crc_detects_payload_corruption_under_fail_policy() {
        // The crux: with CRC on (Fail policy), a flipped payload byte on disk
        // must surface as SpoolError::Corrupted, never as silently-wrong bytes.
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt-queue");
        {
            let cfg = SpoolConfig::new(&path)
                .crc(true)
                .on_corruption(CorruptionPolicy::Fail);
            let mut spool = Spool::open(cfg).await.unwrap();
            spool
                .push(b"the original bytes that must not silently change")
                .await
                .unwrap();
        }
        flip_payload_byte(&path);

        let cfg = SpoolConfig::new(&path)
            .crc(true)
            .on_corruption(CorruptionPolicy::Fail);
        let mut spool = Spool::open(cfg).await.unwrap();
        let result = spool.pop_front().await;
        assert!(
            matches!(result, Err(SpoolError::Corrupted(_))),
            "Fail policy must surface corruption, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_corruption_quarantines_and_starts_fresh() {
        // Default policy: a corrupt cache is renamed aside (timestamped, for
        // forensics) and a FRESH queue takes its place -- the service continues
        // rather than wedging or serving bad data.
        let dir = tempdir().unwrap();
        let path = dir.path().join("spill-cache");
        {
            // Default on_corruption == Quarantine.
            let mut spool = Spool::open(SpoolConfig::new(&path).crc(true))
                .await
                .unwrap();
            spool.push(b"poisoned record").await.unwrap();
        }
        flip_payload_byte(&path);

        let mut spool = Spool::open(SpoolConfig::new(&path).crc(true))
            .await
            .unwrap();
        // The corrupt record reads back as "empty" after the cache is recovered.
        assert_eq!(
            spool.pop_front().await.unwrap(),
            None,
            "Quarantine recovers to a fresh empty queue"
        );

        // The corrupt directory was preserved aside with a timestamped name.
        let quarantined = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .contains("spill-cache.corrupt-")
            });
        assert!(
            quarantined,
            "corrupt cache must be renamed aside, not deleted"
        );

        // The fresh queue is fully usable.
        spool.push(b"after recovery").await.unwrap();
        assert_eq!(
            spool.pop_front().await.unwrap(),
            Some(b"after recovery".to_vec())
        );
    }

    #[tokio::test]
    async fn test_crc_with_compression() {
        // CRC frames the COMPRESSED bytes (what lands on disk); the two compose.
        let dir = tempdir().unwrap();
        let path = dir.path().join("crc-zstd-queue");
        let data = b"compressible ".repeat(50);
        let mut spool = Spool::open(SpoolConfig::new(&path).compress(true).crc(true))
            .await
            .unwrap();
        spool.push(&data).await.unwrap();
        assert_eq!(spool.pop_front().await.unwrap(), Some(data));
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
