// Project:   scalo
// File:      src/worker/accumulator.rs
// Purpose:   Bounded batch accumulator with time/count/bytes drain thresholds
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Bounded batch accumulator for data-plane pipeline batching.
//!
//! Accumulates items from multiple producers (HTTP handlers, gRPC handlers, etc.)
//! and drains them as batches when any threshold is met:
//! - Item count reaches `max_items`
//! - Byte count reaches `max_bytes`
//! - Time since last drain reaches `max_wait`
//!
//! Bounded -- `push` waits for room when the channel is full (backpressure),
//! and `try_push` returns [`AccumulatorFull`] instead of waiting.
//! A batch never exceeds `max_bytes` unless one item alone is larger, and
//! that item goes out alone.
//! Shutdown-safe -- `drain_remaining()` flushes buffered items.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::worker::BatchAccumulator;
//! use std::time::Duration;
//!
//! let (acc, mut drainer) = BatchAccumulator::new(
//!     1000,                        // channel capacity (backpressure bound)
//!     100,                         // max items per batch
//!     1024 * 1024,                 // max bytes per batch (1MB)
//!     Duration::from_millis(10),   // max wait before flush
//! );
//!
//! // Producers push (from HTTP handlers, etc.)
//! acc.push(payload, payload.len()).await?;
//!
//! // Consumer drains batches (background task)
//! loop {
//!     let batch = drainer.next_batch().await;
//!     if batch.is_empty() { break; } // shutdown
//!     process_batch(&batch);
//! }
//! ```

use std::time::Duration;

use tokio::sync::mpsc;

/// Accumulator configuration.
#[derive(Debug, Clone)]
pub struct AccumulatorConfig {
    /// Channel capacity (bounded -- pushers get error when full).
    pub channel_capacity: usize,
    /// Maximum items per batch before auto-drain.
    pub max_items: usize,
    /// Maximum accumulated bytes per batch before auto-drain.
    pub max_bytes: usize,
    /// Maximum time since last drain before auto-flush.
    pub max_wait: Duration,
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 10_000,
            max_items: 100,
            max_bytes: 1024 * 1024, // 1MB
            max_wait: Duration::from_millis(10),
        }
    }
}

/// Push handle -- cloneable, used by producers to send items into the accumulator.
#[derive(Clone)]
pub struct BatchAccumulator<T> {
    tx: mpsc::Sender<(T, usize)>, // (item, byte_size)
}

/// Drain handle -- used by a single consumer to receive batches.
pub struct BatchDrainer<T> {
    rx: mpsc::Receiver<(T, usize)>,
    config: AccumulatorConfig,
    buffer: Vec<T>,
    buffer_bytes: usize,
    /// An item that did not fit in the batch being returned. It opens the next one.
    held: Option<(T, usize)>,
}

/// Error when the accumulator channel is full, from [`BatchAccumulator::try_push`].
#[derive(Debug, thiserror::Error)]
#[error("accumulator full -- backpressure active ({capacity} items buffered)")]
pub struct AccumulatorFull {
    pub capacity: usize,
}

/// Error when the drainer has been dropped, from [`BatchAccumulator::push`].
#[derive(Debug, thiserror::Error)]
#[error("accumulator closed -- the drainer was dropped")]
pub struct AccumulatorClosed;

impl<T: Send + 'static> BatchAccumulator<T> {
    /// Create a new accumulator + drainer pair.
    ///
    /// Returns `(push_handle, drain_handle)`. The push handle is `Clone` for
    /// sharing across HTTP/gRPC handlers. The drain handle is used by a single
    /// background task to receive batches.
    #[must_use]
    pub fn new(config: AccumulatorConfig) -> (Self, BatchDrainer<T>) {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let drainer = BatchDrainer {
            rx,
            buffer: Vec::with_capacity(config.max_items),
            buffer_bytes: 0,
            held: None,
            config: config.clone(),
        };
        (Self { tx }, drainer)
    }

    /// Push an item into the accumulator, waiting while the channel is full.
    ///
    /// `byte_size` is used for the bytes threshold. Pass `payload.len()`.
    ///
    /// # Errors
    ///
    /// Returns `AccumulatorClosed` if the drainer has been dropped.
    pub async fn push(&self, item: T, byte_size: usize) -> Result<(), AccumulatorClosed> {
        self.tx
            .send((item, byte_size))
            .await
            .map_err(|_| AccumulatorClosed)
    }

    /// Push an item without waiting.
    ///
    /// # Errors
    ///
    /// Returns `AccumulatorFull` if the channel is at capacity. A closed
    /// drainer also returns `AccumulatorFull`.
    pub fn try_push(&self, item: T, byte_size: usize) -> Result<(), AccumulatorFull> {
        self.tx
            .try_send((item, byte_size))
            .map_err(|_| AccumulatorFull {
                // `max_capacity()` is the configured channel size. `capacity()`
                // is *remaining* permits (~0 here, since we only build this
                // error when the channel is full), which rendered a misleading
                // "(0 items buffered)" diagnostic.
                capacity: self.tx.max_capacity(),
            })
    }

    /// Check if the accumulator has been closed (drainer dropped).
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// Drain accumulated [`Record`]s into a [`WorkBatch`] for push-ingest sources.
///
/// Push-ingest transports (HTTP, gRPC) accumulate [`Record`]s via a
/// `BatchAccumulator<Record>`, then bridge the drained block to the engine's
/// canonical [`WorkBatch`] currency. The caller supplies the `commit_tokens`,
/// the source's own sequence tokens (`HttpToken`, `GrpcToken`) for the records
/// drained -- the accumulator carries only the payload records. A push source
/// answered each sender when it queued the record, and its commit is a no-op,
/// so these tokens acknowledge nothing further.
///
/// [`Record`]: crate::transport::Record
/// [`WorkBatch`]: crate::transport::WorkBatch
#[cfg(feature = "transport")]
#[must_use]
pub fn records_into_work_batch<T: crate::transport::CommitToken>(
    records: Vec<crate::transport::Record>,
    commit_tokens: Vec<T>,
) -> crate::transport::WorkBatch<T> {
    crate::transport::WorkBatch::new(records, commit_tokens)
}

impl<T> BatchDrainer<T> {
    /// Wait for the next batch.
    ///
    /// Blocks until any threshold is met (items, bytes, or time). Returns
    /// an empty vec when the channel is closed (all producers dropped = shutdown).
    pub async fn next_batch(&mut self) -> Vec<T> {
        // The item held back from the previous batch opens this one.
        if let Some((item, size)) = self.held.take() {
            self.append(item, size);
        }
        // If buffer already meets a threshold, drain immediately
        if self.threshold_met() {
            return self.take_buffer();
        }

        // Wait for items against a FIXED deadline. The deadline is set once per
        // accumulation window (not recreated per arriving item), so trickle
        // traffic -- items arriving every < max_wait -- cannot defer the flush
        // indefinitely. The prior code created a fresh `sleep(max_wait)` on every
        // loop iteration, so each arrival reset the timer and the first buffered
        // item's latency was unbounded, breaking the documented "time since last
        // drain reaches max_wait" guarantee.
        let sleep = tokio::time::sleep(self.config.max_wait);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                biased;

                // Time threshold -- flush whatever we have
                () = &mut sleep => {
                    if self.buffer.is_empty() {
                        // No items at all -- re-arm a fresh window and keep
                        // waiting (don't return an empty batch).
                        sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + self.config.max_wait);
                        continue;
                    }
                    return self.take_buffer();
                }

                // New item arrived
                item = self.rx.recv() => {
                    match item {
                        Some((val, size)) => {
                            if let Some(batch) = self.offer(val, size) {
                                return batch;
                            }
                        }
                        None => {
                            // Channel closed -- drain remaining
                            return self.take_buffer();
                        }
                    }
                }
            }
        }
    }

    /// Drain any remaining buffered items (for graceful shutdown).
    ///
    /// Returns everything still held as one `Vec`, past `max_items` and
    /// `max_bytes`. [`next_batch`](Self::next_batch) keeps batches within
    /// those bounds.
    pub fn drain_remaining(&mut self) -> Vec<T> {
        if let Some((item, size)) = self.held.take() {
            self.append(item, size);
        }
        // Drain channel
        while let Ok((val, size)) = self.rx.try_recv() {
            self.append(val, size);
        }
        self.take_buffer()
    }

    /// Add an item to the buffer, returning the batch it completes, if any.
    ///
    /// An item that would push a non-empty buffer past `max_bytes` is held
    /// instead, and the buffer is returned. The held item opens the next batch.
    fn offer(&mut self, val: T, size: usize) -> Option<Vec<T>> {
        let overflows = self.buffer_bytes.saturating_add(size) > self.config.max_bytes;
        if !self.buffer.is_empty() && overflows {
            self.held = Some((val, size));
            return Some(self.take_buffer());
        }
        self.append(val, size);
        self.threshold_met().then(|| self.take_buffer())
    }

    fn append(&mut self, val: T, size: usize) {
        self.buffer_bytes += size;
        self.buffer.push(val);
    }

    fn threshold_met(&self) -> bool {
        self.buffer.len() >= self.config.max_items || self.buffer_bytes >= self.config.max_bytes
    }

    fn take_buffer(&mut self) -> Vec<T> {
        self.buffer_bytes = 0;
        std::mem::take(&mut self.buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_drain_on_item_count() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 5,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1), // won't trigger
        };
        let (acc, mut drainer) = BatchAccumulator::new(config);

        // Push 5 items -- should trigger drain
        for i in 0..5 {
            acc.push(i, 1).await.unwrap();
        }

        let batch = drainer.next_batch().await;
        assert_eq!(batch.len(), 5);
        assert_eq!(batch, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_drain_on_byte_threshold() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 1000, // won't trigger
            max_bytes: 10,   // a batch holds at most 10 bytes
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::new(config);

        // Four items of 3 bytes: three fit (9 bytes), the fourth would make 12.
        for i in 0..4 {
            acc.push(i, 3).await.unwrap();
        }

        let batch = drainer.next_batch().await;
        assert_eq!(batch, vec![0, 1, 2]);

        // The held fourth item opens the next batch once the channel closes.
        drop(acc);
        let batch = drainer.next_batch().await;
        assert_eq!(batch, vec![3]);
    }

    /// A batch never exceeds `max_bytes` unless one item alone is larger, and
    /// that item goes out alone. Each case is (item sizes, expected batches).
    #[tokio::test]
    async fn test_batches_never_exceed_max_bytes() {
        let cases: &[(&str, &[usize], &[&[usize]])] = &[
            ("exact fill", &[5, 5, 5, 5], &[&[5, 5], &[5, 5]]),
            ("fits before push", &[3, 3, 3, 3], &[&[3, 3, 3], &[3]]),
            (
                "oversized item goes alone",
                &[3, 20, 3],
                &[&[3], &[20], &[3]],
            ),
            ("oversized item first", &[20, 3], &[&[20], &[3]]),
            (
                "mixed sizes",
                &[4, 7, 2, 9, 1, 1, 8],
                &[&[4], &[7, 2], &[9, 1], &[1, 8]],
            ),
        ];

        for (name, sizes, want) in cases {
            let config = AccumulatorConfig {
                channel_capacity: 100,
                max_items: 1000,
                max_bytes: 10,
                max_wait: Duration::from_mins(1),
            };
            // Each item's byte size is its value.
            let (acc, mut drainer) = BatchAccumulator::<usize>::new(config);
            for &size in *sizes {
                acc.push(size, size).await.unwrap();
            }
            drop(acc);

            let mut got: Vec<Vec<usize>> = Vec::new();
            loop {
                let batch = drainer.next_batch().await;
                if batch.is_empty() {
                    break;
                }
                got.push(batch);
            }

            let want: Vec<Vec<usize>> = want.iter().map(|batch| batch.to_vec()).collect();
            assert_eq!(got, want, "case: {name}");
            for batch in &got {
                let total: usize = batch.iter().sum();
                assert!(
                    total <= 10 || batch.len() == 1,
                    "case {name}: batch {batch:?} holds {total} bytes"
                );
            }
        }
    }

    /// An item held back from a full batch is returned by `drain_remaining`,
    /// ahead of the items still in the channel.
    #[tokio::test]
    async fn test_drain_remaining_returns_a_held_item_first() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 1000,
            max_bytes: 10,
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::<i32>::new(config);
        for item in 1..=5 {
            acc.push(item, 3).await.unwrap();
        }

        // 1, 2 and 3 make 9 bytes. Item 4 is held, and 5 stays in the channel.
        let batch = drainer.next_batch().await;
        assert_eq!(batch, vec![1, 2, 3]);

        assert_eq!(drainer.drain_remaining(), vec![4, 5]);
    }

    /// `push` waits for room when the channel is full rather than failing.
    #[tokio::test(start_paused = true)]
    async fn test_push_waits_for_room_when_full() {
        let config = AccumulatorConfig {
            channel_capacity: 1,
            max_items: 2,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::<i32>::new(config);
        acc.push(1, 1).await.unwrap();

        let producer = acc.clone();
        let second = tokio::spawn(async move { producer.push(2, 1).await });

        // Nothing has drained, so the channel is still full and the push waits.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !second.is_finished(),
            "push returned while the channel was full"
        );

        // next_batch takes item 1, freeing the slot, so the second push completes.
        let batch = drainer.next_batch().await;
        assert_eq!(batch, vec![1, 2]);
        second.await.unwrap().unwrap();
    }

    /// `push` reports a closed drainer as `AccumulatorClosed`, not as full.
    #[tokio::test]
    async fn test_push_reports_a_dropped_drainer_as_closed() {
        let (acc, drainer) = BatchAccumulator::<i32>::new(AccumulatorConfig::default());
        drop(drainer);

        let err = acc.push(1, 1).await.expect_err("drainer dropped -> closed");
        assert_eq!(
            err.to_string(),
            "accumulator closed -- the drainer was dropped"
        );
    }

    #[tokio::test]
    async fn test_drain_on_time_threshold() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 1000,
            max_bytes: usize::MAX,
            max_wait: Duration::from_millis(50), // 50ms
        };
        let (acc, mut drainer) = BatchAccumulator::new(config);

        // Push 2 items (below count/byte threshold)
        acc.push(1, 1).await.unwrap();
        acc.push(2, 1).await.unwrap();

        // Drain should fire after 50ms timeout
        let batch = drainer.next_batch().await;
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn test_backpressure_when_full() {
        let config = AccumulatorConfig {
            channel_capacity: 3,
            max_items: 100,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, _drainer) = BatchAccumulator::<i32>::new(config);

        // Fill to capacity
        acc.push(1, 1).await.unwrap();
        acc.push(2, 1).await.unwrap();
        acc.push(3, 1).await.unwrap();

        // Next push should fail (backpressure)
        let result = acc.try_push(4, 1);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_backpressure_error_reports_configured_capacity() {
        let config = AccumulatorConfig {
            channel_capacity: 3,
            max_items: 100,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, _drainer) = BatchAccumulator::<i32>::new(config);

        acc.push(1, 1).await.unwrap();
        acc.push(2, 1).await.unwrap();
        acc.push(3, 1).await.unwrap();

        let err = acc.try_push(4, 1).expect_err("channel full -> error");
        // Reports the CONFIGURED capacity (3), not the remaining permits (0).
        assert_eq!(err.capacity, 3);
    }

    /// Items arriving steadily faster than `max_wait` must NOT defer the flush:
    /// the accumulation-window deadline is fixed per batch, so it fires even
    /// while items keep coming. Regression for the prior per-iteration
    /// `sleep(max_wait)` that was recreated on every arrival, resetting the
    /// timer indefinitely under trickle traffic. `start_paused` makes the clock
    /// deterministic.
    #[tokio::test(start_paused = true)]
    async fn test_trickle_traffic_flushes_on_fixed_deadline() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 1000,       // never trips on count
            max_bytes: usize::MAX, // never trips on bytes
            max_wait: Duration::from_millis(100),
        };
        let (acc, mut drainer) = BatchAccumulator::<i32>::new(config);

        // One item every 40ms -- well under the 100ms window.
        tokio::spawn(async move {
            for i in 0..6 {
                acc.push(i, 1).await.unwrap();
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        });

        let batch = drainer.next_batch().await;
        // The fixed 100ms deadline flushes the items buffered within the window
        // (those landing at 0/40/80ms), NOT all 6. Old reset-per-arrival
        // behaviour would only flush after the producer stopped.
        assert!(
            !batch.is_empty(),
            "should flush items buffered within the window"
        );
        assert!(
            batch.len() < 6,
            "expected a partial flush at the fixed deadline, got all {} items \
             (timer reset on each arrival?)",
            batch.len()
        );
    }

    #[tokio::test]
    async fn test_shutdown_drains_remaining() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 1000,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::new(config);

        acc.push(10, 1).await.unwrap();
        acc.push(20, 1).await.unwrap();

        // Drop the push handle (simulates shutdown)
        drop(acc);

        // next_batch should return remaining items
        let batch = drainer.next_batch().await;
        assert_eq!(batch, vec![10, 20]);

        // Subsequent call returns empty (channel closed)
        let batch = drainer.next_batch().await;
        assert_eq!(batch, [] as [i32; 0]);
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 3,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::new(config);

        // Push 7 items -- should produce 2 full batches + 1 partial
        for i in 0..7 {
            acc.push(i, 1).await.unwrap();
        }
        drop(acc); // signal shutdown to drain the last partial

        let b1 = drainer.next_batch().await;
        assert_eq!(b1.len(), 3);

        let b2 = drainer.next_batch().await;
        assert_eq!(b2.len(), 3);

        let b3 = drainer.next_batch().await;
        assert_eq!(b3.len(), 1); // remaining partial batch

        let b4 = drainer.next_batch().await;
        assert_eq!(b4, [] as [i32; 0]); // channel closed
    }

    #[tokio::test]
    async fn test_push_handle_is_clone() {
        let config = AccumulatorConfig::default();
        let (acc, mut drainer) = BatchAccumulator::new(config);

        let acc2 = acc.clone();

        acc.push(1, 1).await.unwrap();
        acc2.push(2, 1).await.unwrap();

        drop(acc);
        drop(acc2);

        let batch = drainer.next_batch().await;
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn test_drain_remaining_on_shutdown() {
        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 1000,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::new(config);

        acc.push(1, 1).await.unwrap();
        acc.push(2, 1).await.unwrap();
        acc.push(3, 1).await.unwrap();
        drop(acc);

        let remaining = drainer.drain_remaining();
        assert_eq!(remaining, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_empty_drain_returns_empty() {
        let config = AccumulatorConfig::default();
        let (_acc, mut drainer) = BatchAccumulator::<i32>::new(config);

        let remaining = drainer.drain_remaining();
        assert_eq!(remaining, [] as [i32; 0]);
    }

    /// Push-ingest helper: drained Records + supplied tokens become a WorkBatch.
    #[cfg(feature = "transport")]
    #[tokio::test]
    async fn test_records_drain_into_work_batch() {
        use crate::transport::{CommitToken, PayloadFormat, Record, RecordMeta};
        use bytes::Bytes;

        #[derive(Debug, Clone)]
        struct PushTok(u64);
        impl std::fmt::Display for PushTok {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "push-{}", self.0)
            }
        }
        impl CommitToken for PushTok {}

        let record = |payload: &'static [u8]| Record {
            payload: Bytes::from_static(payload),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        };

        let config = AccumulatorConfig {
            channel_capacity: 100,
            max_items: 3,
            max_bytes: usize::MAX,
            max_wait: Duration::from_mins(1),
        };
        let (acc, mut drainer) = BatchAccumulator::<Record>::new(config);
        acc.push(record(b"{\"a\":1}"), 7).await.unwrap();
        acc.push(record(b"{\"b\":2}"), 7).await.unwrap();
        acc.push(record(b"{\"c\":3}"), 7).await.unwrap();

        let block = drainer.next_batch().await;
        assert_eq!(block.len(), 3);

        // Two source acks for a three-record block (push sources ack per request,
        // not per record) -- the helper must NOT tie token count to record count.
        let tokens = vec![PushTok(1), PushTok(2)];
        let wb = records_into_work_batch(block, tokens);
        assert_eq!(wb.record_count(), 3);
        assert_eq!(wb.commit_tokens.len(), 2);
        assert!(wb.dlq_entries.is_empty());
    }
}
