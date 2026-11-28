// Project:   scalo
// File:      benches/delivery_path/observe.rs
// Purpose:   A sender wrapper counting the bytes a destination holds and its backpressure
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use scalo::transport::ack::{DeadLetterReason, SinkConfirmation};
use scalo::transport::{Record, SendResult, TransportBase, TransportResult, TransportSender};

/// What [`Observed`] counts, shared with the cell that reads it.
#[derive(Default)]
pub struct Counts {
    records: AtomicU64,
    in_flight: AtomicU64,
    peak: AtomicU64,
    backpressured: AtomicU64,
}

impl Counts {
    /// Most payload bytes inside calls to the destination at once: an upper
    /// bound on what its queue held.
    pub fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Records offered to the destination, a resend counted each time.
    pub fn attempted_records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }

    /// Calls the destination answered `Backpressured`.
    pub fn backpressured(&self) -> u64 {
        self.backpressured.load(Ordering::Relaxed)
    }
}

/// `S`, counting the payload bytes inside its calls and its `Backpressured`
/// answers.
pub struct Observed<S> {
    inner: S,
    counts: Arc<Counts>,
}

impl<S> Observed<S> {
    pub fn new(inner: S) -> (Self, Arc<Counts>) {
        let counts = Arc::new(Counts::default());
        (
            Self {
                inner,
                counts: Arc::clone(&counts),
            },
            counts,
        )
    }

    async fn counted(
        &self,
        records: u64,
        bytes: u64,
        call: impl Future<Output = SendResult>,
    ) -> SendResult {
        self.counts.records.fetch_add(records, Ordering::Relaxed);
        let now = self.counts.in_flight.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.counts.peak.fetch_max(now, Ordering::Relaxed);
        let result = call.await;
        self.counts.in_flight.fetch_sub(bytes, Ordering::Relaxed);
        if result.is_backpressured() {
            self.counts.backpressured.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

impl<S: TransportSender> TransportBase for Observed<S> {
    async fn close(&self) -> TransportResult<()> {
        self.inner.close().await
    }

    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn delivery_guarantee(&self) -> scalo::transport::DeliveryGuarantee {
        self.inner.delivery_guarantee()
    }
}

impl<S: TransportSender> TransportSender for Observed<S> {
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        let bytes = payload.len() as u64;
        self.counted(1, bytes, self.inner.send(destination, payload))
            .await
    }

    async fn send_record(&self, record: &Record) -> SendResult {
        self.counted(
            1,
            record.payload.len() as u64,
            self.inner.send_record(record),
        )
        .await
    }

    async fn send_batch(&self, records: &[Record]) -> SendResult {
        let bytes = records.iter().map(|r| r.payload.len() as u64).sum();
        self.counted(records.len() as u64, bytes, self.inner.send_batch(records))
            .await
    }

    fn confirms_delivery(&self) -> SinkConfirmation {
        self.inner.confirms_delivery()
    }

    fn dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        self.inner.dead_letter_reason(record)
    }
}
