// Project:   scalo
// File:      src/transport/test_support/shaped.rs
// Purpose:   A sender whose latency, batching, backpressure and faults are set by config
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! [`ShapedSender`]: a [`TransportSender`] whose behaviour is set by a
//! [`ShapedConfig`], for tests and benches that need a destination with a known
//! shape and no network.
//!
//! Two presets stand for the two shapes a data path meets:
//!
//! - [`kafka_like`]: enqueue is cheap, each record confirms when the linger
//!   window it joined closes plus a broker round trip, and a full queue answers
//!   [`SendResult::Backpressured`] after a bounded wait.
//! - [`grpc_like`]: one call is one batch, pays a round trip plus a cost per
//!   record and per KiB, and is accepted all or nothing.
//!
//! [`PRESETS_CALIBRATED`] says whether the preset numbers came from a run
//! against the real transports.
//!
//! Every wait is on tokio's clock, so a test under `start_paused` runs the
//! shape in virtual time. Jitter comes from a seeded generator, so a run
//! repeats.
//!
//! tokio's timer fires on whole milliseconds, so each sender sleeps whole
//! milliseconds and carries the rest, and the timer's overshoot, into its next
//! wait. A single call can be up to a millisecond off, while the mean across
//! calls holds. Below 1 ms the shaped senders model mean latency only, and a
//! result that turns on latency under 1 ms is for tier 2, the real transports,
//! to decide.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, Semaphore};
use tokio::time::Instant;

use crate::transport::ack::{DeadLetterReason, SinkConfirmation};
use crate::transport::{
    PayloadFormat, Record, RecordMeta, SendResult, TransportBase, TransportError, TransportResult,
    TransportSender,
};

/// Whether the timings in [`kafka_like`] and [`grpc_like`] come from the
/// `delivery_path` bench's `--tier 2 --calibrate` run against the real Kafka
/// and gRPC transports. The queue caps and the calls-in-flight cap are config
/// values, not measured.
pub const PRESETS_CALIBRATED: bool = true;

/// How a call's records are answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeMode {
    /// Each record is queued on its own and confirms on its own, as a Kafka
    /// producer does. A full queue stops the offers, and the records already
    /// queued are still awaited.
    PerRecord,
    /// The call's records are accepted together or not at all, as one gRPC
    /// `RouteBatch` is.
    AllOrNothing,
}

/// A predicate over a payload that the sender dead-letters.
pub type RefusePredicate = fn(&[u8]) -> bool;

/// A destination that does not answer: every call waits `after`, then answers
/// [`SendResult::Backpressured`], as an unreachable gRPC endpoint does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Down {
    /// How long a call waits before it is refused.
    pub after: Duration,
}

/// Confirmations stop for `duration` at the start of every `period`, so a
/// queue with a byte cap fills and backpressures in bursts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stall {
    /// Time between the starts of two stalls.
    pub period: Duration,
    /// How long each stall lasts.
    pub duration: Duration,
}

/// Faults injected into a [`ShapedSender`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    /// Added to every call's latency.
    pub extra_latency: Duration,
    /// The destination is down.
    pub down: Option<Down>,
    /// Records whose payload matches are dead-lettered, and named by
    /// [`dead_letter_reason`](TransportSender::dead_letter_reason).
    pub refuse: Option<RefusePredicate>,
    /// Confirmations stall in bursts.
    pub stall: Option<Stall>,
}

/// The shape of a [`ShapedSender`].
#[derive(Debug, Clone, Copy)]
pub struct ShapedConfig {
    /// Round trip paid once per call, or once per confirmation window.
    pub call_latency: Duration,
    /// Cost of each record in a call.
    pub record_cost: Duration,
    /// Cost of each KiB of payload in a call.
    pub kib_cost: Duration,
    /// When set, a record offered with no window open opens one of this
    /// length, as a producer's linger timer starts at the first record of a
    /// batch. Every record offered while it is open confirms when it closes,
    /// plus `call_latency`.
    pub confirm_window: Option<Duration>,
    /// How a call's records are answered.
    pub outcome: OutcomeMode,
    /// Bytes that may wait for confirmation at once. Zero is no cap. A call
    /// larger than a cap is let in when the queue is empty.
    pub queue_cap_bytes: u64,
    /// Records that may wait for confirmation at once. Zero is no cap.
    pub queue_cap_records: u64,
    /// How long an offer waits for room in a full queue before it answers
    /// [`SendResult::Backpressured`].
    pub queue_full_wait: Duration,
    /// Calls in flight at once. A call over the cap waits for a slot. Zero is
    /// no cap.
    pub max_calls_in_flight: usize,
    /// Upper bound of the uniform jitter added to each call's latency.
    pub latency_jitter: Duration,
    /// Seed for the jitter.
    pub seed: u64,
    /// Keep every payload taken into the queue, in order, for
    /// [`ShapedSender::queued_payloads`].
    pub log_order: bool,
    /// Faults.
    pub faults: Faults,
}

impl Default for ShapedConfig {
    fn default() -> Self {
        Self {
            call_latency: Duration::ZERO,
            record_cost: Duration::ZERO,
            kib_cost: Duration::ZERO,
            confirm_window: None,
            outcome: OutcomeMode::AllOrNothing,
            queue_cap_bytes: 0,
            queue_cap_records: 0,
            queue_full_wait: Duration::ZERO,
            max_calls_in_flight: 0,
            latency_jitter: Duration::ZERO,
            seed: 0,
            log_order: false,
            faults: Faults::default(),
        }
    }
}

/// A Kafka producer's shape: per-record confirmation after a linger window,
/// and a byte-capped queue that backpressures after a bounded wait.
#[must_use]
pub fn kafka_like() -> ShapedConfig {
    ShapedConfig {
        // A lone record's time to its delivery report with `linger.ms` 0.
        call_latency: Duration::from_micros(3_926),
        // Slope of a block's time to its last delivery report per record.
        record_cost: Duration::from_nanos(3_565),
        kib_cost: Duration::ZERO,
        // A lone record's time to its delivery report under the throughput
        // profile, less `call_latency`.
        confirm_window: Some(Duration::from_micros(21_184)),
        outcome: OutcomeMode::PerRecord,
        // `ProducerKnobs::buffer_memory_bytes` of the throughput profile.
        queue_cap_bytes: 64 * 1024 * 1024,
        // librdkafka's default `queue.buffering.max.messages`, which scalo leaves unset.
        queue_cap_records: 100_000,
        // Time from a `send` to `Backpressured` at a full producer queue.
        queue_full_wait: Duration::from_millis(5_028),
        max_calls_in_flight: 0,
        latency_jitter: Duration::ZERO,
        seed: 0,
        log_order: false,
        faults: Faults::default(),
    }
}

/// A gRPC `RouteBatch` call's shape: one round trip per call plus a cost per
/// record and per KiB, accepted all or nothing.
#[must_use]
pub fn grpc_like() -> ShapedConfig {
    ShapedConfig {
        // Intercept of a `RouteBatch` call's time fitted on records and KiB.
        call_latency: Duration::from_micros(416),
        // The same fit's cost per record.
        record_cost: Duration::from_nanos(1_094),
        // The same fit's cost per KiB.
        kib_cost: Duration::from_nanos(4_919),
        confirm_window: None,
        outcome: OutcomeMode::AllOrNothing,
        queue_cap_bytes: 0,
        // `GrpcConfig::recv_buffer_size`: the receive channel, in records.
        queue_cap_records: 10_000,
        queue_full_wait: Duration::ZERO,
        // hyper's HTTP/2 server default for `max_concurrent_streams`, which
        // scalo's gRPC server leaves unset.
        max_calls_in_flight: 200,
        latency_jitter: Duration::ZERO,
        seed: 0,
        log_order: false,
        faults: Faults::default(),
    }
}

/// Buckets in [`ShapedStats::batch_sizes`].
pub const BATCH_BUCKETS: usize = 18;

/// Counters a [`ShapedSender`] keeps, read with [`ShapedSender::stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShapedStats {
    /// Calls to `send`, `send_record` or `send_batch`.
    pub calls: u64,
    /// Records offered across every call.
    pub records_offered: u64,
    /// Records confirmed, counted when their call returns.
    pub records_confirmed: u64,
    /// Records dead-lettered by the refuse fault.
    pub records_dead_lettered: u64,
    /// Calls answered [`SendResult::Backpressured`].
    pub backpressured_calls: u64,
    /// Most records offered in one call.
    pub max_batch: u64,
    /// Calls by records offered, in power-of-two buckets: bucket `i` holds
    /// calls of `2^(i-1) + 1 ..= 2^i` records, bucket 0 calls of 0 or 1.
    pub batch_sizes: [u64; BATCH_BUCKETS],
    /// Most payload bytes waiting for confirmation at once.
    pub peak_bytes_in_flight: u64,
    /// Most calls in flight at once.
    pub peak_calls_in_flight: u64,
}

#[derive(Default)]
struct Counters {
    calls: AtomicU64,
    records_offered: AtomicU64,
    records_confirmed: AtomicU64,
    records_dead_lettered: AtomicU64,
    backpressured_calls: AtomicU64,
    max_batch: AtomicU64,
    batch_sizes: [AtomicU64; BATCH_BUCKETS],
    peak_bytes_in_flight: AtomicU64,
    calls_in_flight: AtomicU64,
    peak_calls_in_flight: AtomicU64,
}

/// A [`TransportSender`] with the shape its [`ShapedConfig`] sets.
///
/// Clones share one queue, one set of counters and one clock.
#[derive(Clone)]
pub struct ShapedSender {
    inner: Arc<Inner>,
}

struct Inner {
    name: &'static str,
    config: ShapedConfig,
    epoch: Instant,
    queue_bytes: AtomicU64,
    queue_records: AtomicU64,
    room: Notify,
    slots: Option<Semaphore>,
    rng: AtomicU64,
    /// Nanoseconds slept short of what was asked, carried into the next wait.
    debt_ns: AtomicI64,
    /// When the open confirmation window closes, in nanoseconds from `epoch`.
    window_end: AtomicU64,
    counters: Counters,
    closed: AtomicBool,
    order: std::sync::Mutex<Vec<bytes::Bytes>>,
}

impl std::fmt::Debug for ShapedSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShapedSender")
            .field("name", &self.inner.name)
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl ShapedSender {
    /// A sender named `name` with the shape `config`. Its clock starts now.
    #[must_use]
    pub fn new(name: &'static str, config: ShapedConfig) -> Self {
        let slots =
            (config.max_calls_in_flight > 0).then(|| Semaphore::new(config.max_calls_in_flight));
        Self {
            inner: Arc::new(Inner {
                name,
                config,
                epoch: Instant::now(),
                queue_bytes: AtomicU64::new(0),
                queue_records: AtomicU64::new(0),
                room: Notify::new(),
                slots,
                rng: AtomicU64::new(config.seed),
                debt_ns: AtomicI64::new(0),
                window_end: AtomicU64::new(0),
                counters: Counters::default(),
                closed: AtomicBool::new(false),
                order: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    /// The shape this sender was built with.
    #[must_use]
    pub fn config(&self) -> &ShapedConfig {
        &self.inner.config
    }

    /// A snapshot of the counters.
    #[must_use]
    pub fn stats(&self) -> ShapedStats {
        let c = &self.inner.counters;
        let mut batch_sizes = [0; BATCH_BUCKETS];
        for (out, bucket) in batch_sizes.iter_mut().zip(&c.batch_sizes) {
            *out = bucket.load(Ordering::Relaxed);
        }
        ShapedStats {
            calls: c.calls.load(Ordering::Relaxed),
            records_offered: c.records_offered.load(Ordering::Relaxed),
            records_confirmed: c.records_confirmed.load(Ordering::Relaxed),
            records_dead_lettered: c.records_dead_lettered.load(Ordering::Relaxed),
            backpressured_calls: c.backpressured_calls.load(Ordering::Relaxed),
            max_batch: c.max_batch.load(Ordering::Relaxed),
            batch_sizes,
            peak_bytes_in_flight: c.peak_bytes_in_flight.load(Ordering::Relaxed),
            peak_calls_in_flight: c.peak_calls_in_flight.load(Ordering::Relaxed),
        }
    }

    /// Payload bytes waiting for confirmation now.
    #[must_use]
    pub fn bytes_in_flight(&self) -> u64 {
        self.inner.queue_bytes.load(Ordering::Relaxed)
    }

    /// Every payload taken into the queue, in the order taken. Empty unless
    /// [`ShapedConfig::log_order`] is set.
    #[must_use]
    pub fn queued_payloads(&self) -> Vec<bytes::Bytes> {
        self.inner
            .order
            .lock()
            .map(|order| order.clone())
            .unwrap_or_default()
    }

    fn log<'r>(&self, records: impl IntoIterator<Item = &'r Record>) {
        if self.inner.config.log_order
            && let Ok(mut order) = self.inner.order.lock()
        {
            order.extend(records.into_iter().map(|r| r.payload.clone()));
        }
    }

    fn refuses(&self, payload: &[u8]) -> bool {
        self.inner.config.faults.refuse.is_some_and(|f| f(payload))
    }

    async fn call(&self, records: &[Record]) -> SendResult {
        let inner = &self.inner;
        if inner.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }
        let c = &inner.counters;
        c.calls.fetch_add(1, Ordering::Relaxed);
        let offered = u64::try_from(records.len()).unwrap_or(u64::MAX);
        c.records_offered.fetch_add(offered, Ordering::Relaxed);
        c.max_batch.fetch_max(offered, Ordering::Relaxed);
        c.batch_sizes[bucket(offered)].fetch_add(1, Ordering::Relaxed);

        if let Some(down) = inner.config.faults.down {
            self.wait_until(Instant::now() + down.after).await;
            c.backpressured_calls.fetch_add(1, Ordering::Relaxed);
            return SendResult::Backpressured;
        }

        // A slot is held until the call answers, as an HTTP/2 stream is.
        let _slot = match &inner.slots {
            Some(slots) => match slots.acquire().await {
                Ok(permit) => Some(permit),
                Err(_) => return SendResult::Fatal(TransportError::Closed),
            },
            None => None,
        };
        let _in_flight = CallInFlight::enter(inner);

        let result = match inner.config.outcome {
            OutcomeMode::PerRecord => self.per_record(records).await,
            OutcomeMode::AllOrNothing => self.all_or_nothing(records).await,
        };
        if result.is_backpressured() {
            c.backpressured_calls.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn per_record(&self, records: &[Record]) -> SendResult {
        let inner = &self.inner;
        let cfg = &inner.config;
        let start = Instant::now();
        let mut held = QueueHold::new(inner);
        let mut confirm_at = start;
        let mut queued: u64 = 0;
        let mut dead_lettered = false;
        let mut refused = false;
        for record in records {
            if self.refuses(&record.payload) {
                inner
                    .counters
                    .records_dead_lettered
                    .fetch_add(1, Ordering::Relaxed);
                dead_lettered = true;
                continue;
            }
            if !held
                .take(len_u64(&record.payload), 1, cfg.queue_full_wait)
                .await
            {
                refused = true;
                break;
            }
            self.log(std::iter::once(record));
            queued += 1;
            let offered_at = start + scale(cfg.record_cost, queued);
            let confirmed = self.confirm_time(offered_at) + cfg.call_latency + self.extra();
            confirm_at = confirm_at.max(self.after_stall(confirmed));
        }
        self.wait_until(confirm_at).await;
        inner
            .counters
            .records_confirmed
            .fetch_add(queued, Ordering::Relaxed);
        drop(held);
        if refused {
            SendResult::Backpressured
        } else if dead_lettered {
            SendResult::FilteredDlq
        } else {
            SendResult::Ok
        }
    }

    async fn all_or_nothing(&self, records: &[Record]) -> SendResult {
        let inner = &self.inner;
        let cfg = &inner.config;
        let start = Instant::now();
        let mut dead_lettered = false;
        let mut sent: u64 = 0;
        let mut bytes: u64 = 0;
        for record in records {
            if self.refuses(&record.payload) {
                inner
                    .counters
                    .records_dead_lettered
                    .fetch_add(1, Ordering::Relaxed);
                dead_lettered = true;
            } else {
                sent += 1;
                bytes += len_u64(&record.payload);
            }
        }
        if sent > 0 {
            let mut held = QueueHold::new(inner);
            if !held.take(bytes, sent, cfg.queue_full_wait).await {
                return SendResult::Backpressured;
            }
            self.log(records.iter().filter(|r| !self.refuses(&r.payload)));
            let cost = cfg.call_latency
                + scale(cfg.record_cost, sent)
                + scale(cfg.kib_cost, bytes.div_ceil(1024))
                + self.extra();
            let done = self.after_stall(self.confirm_time(start + cost));
            self.wait_until(done).await;
            inner
                .counters
                .records_confirmed
                .fetch_add(sent, Ordering::Relaxed);
        }
        if dead_lettered {
            SendResult::FilteredDlq
        } else {
            SendResult::Ok
        }
    }

    /// Sleep whole milliseconds toward `deadline`, carrying the part the timer
    /// cannot hit, and any overshoot, into the next wait.
    async fn wait_until(&self, deadline: Instant) {
        const TICK_NS: i64 = 1_000_000;
        let start = Instant::now();
        let asked = signed_nanos(deadline.saturating_duration_since(start));
        let owed = self.inner.debt_ns.swap(0, Ordering::AcqRel);
        let wanted = asked.saturating_add(owed);
        let whole = wanted.div_euclid(TICK_NS) * TICK_NS;
        if whole <= 0 {
            self.inner.debt_ns.fetch_add(wanted, Ordering::AcqRel);
            return;
        }
        tokio::time::sleep(Duration::from_nanos(whole.unsigned_abs())).await;
        let slept = signed_nanos(start.elapsed());
        self.inner
            .debt_ns
            .fetch_add(wanted - slept, Ordering::AcqRel);
    }

    /// When the window a record offered at `at` joins closes, opening one if
    /// none is open, or `at` with no window configured.
    fn confirm_time(&self, at: Instant) -> Instant {
        let Some(window) = self.inner.config.confirm_window else {
            return at;
        };
        let at_ns = as_u64_nanos(at.saturating_duration_since(self.inner.epoch));
        let window_ns = as_u64_nanos(window);
        let end = &self.inner.window_end;
        let mut current = end.load(Ordering::Acquire);
        loop {
            if at_ns < current {
                return self.inner.epoch + Duration::from_nanos(current);
            }
            let opened = at_ns.saturating_add(window_ns);
            match end.compare_exchange_weak(current, opened, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return self.inner.epoch + Duration::from_nanos(opened),
                Err(seen) => current = seen,
            }
        }
    }

    /// `at`, or the end of the stall it falls in.
    fn after_stall(&self, at: Instant) -> Instant {
        let Some(stall) = self.inner.config.faults.stall else {
            return at;
        };
        let period = stall.period.as_nanos().max(1);
        let phase = at.saturating_duration_since(self.inner.epoch).as_nanos() % period;
        let stalled = stall.duration.as_nanos();
        if phase < stalled {
            at + nanos(stalled - phase)
        } else {
            at
        }
    }

    /// Injected latency plus seeded jitter for one call.
    fn extra(&self) -> Duration {
        let cfg = &self.inner.config;
        let jitter_ns = cfg.latency_jitter.as_nanos();
        let jitter = if jitter_ns == 0 {
            Duration::ZERO
        } else {
            let state = self
                .inner
                .rng
                .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
            nanos(u128::from(splitmix(state)) % jitter_ns)
        };
        cfg.faults.extra_latency + jitter
    }
}

/// One call counted in flight until dropped.
struct CallInFlight<'a> {
    inner: &'a Inner,
}

impl<'a> CallInFlight<'a> {
    fn enter(inner: &'a Inner) -> Self {
        let c = &inner.counters;
        let now = c.calls_in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        c.peak_calls_in_flight.fetch_max(now, Ordering::Relaxed);
        Self { inner }
    }
}

impl Drop for CallInFlight<'_> {
    fn drop(&mut self) {
        self.inner
            .counters
            .calls_in_flight
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Queue bytes one call holds, given back when it is dropped, so a cancelled
/// call frees its room.
struct QueueHold<'a> {
    inner: &'a Inner,
    bytes: u64,
    records: u64,
}

impl<'a> QueueHold<'a> {
    fn new(inner: &'a Inner) -> Self {
        Self {
            inner,
            bytes: 0,
            records: 0,
        }
    }

    /// Take room for `records` records of `bytes` bytes, waiting up to `wait`
    /// for it. False when the room did not come.
    async fn take(&mut self, bytes: u64, records: u64, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        loop {
            let notified = self.inner.room.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_take(bytes, records) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::select! {
                () = notified => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    fn try_take(&mut self, bytes: u64, records: u64) -> bool {
        let Some(previous) = take_room(
            &self.inner.queue_bytes,
            bytes,
            self.inner.config.queue_cap_bytes,
        ) else {
            return false;
        };
        if take_room(
            &self.inner.queue_records,
            records,
            self.inner.config.queue_cap_records,
        )
        .is_none()
        {
            self.inner.queue_bytes.fetch_sub(bytes, Ordering::AcqRel);
            return false;
        }
        self.bytes += bytes;
        self.records += records;
        self.inner
            .counters
            .peak_bytes_in_flight
            .fetch_max(previous + bytes, Ordering::Relaxed);
        true
    }
}

/// Add `amount` to `level` unless that passes `cap`, letting anything into an
/// empty queue. Returns the level before, or `None` when it did not fit.
fn take_room(level: &AtomicU64, amount: u64, cap: u64) -> Option<u64> {
    level
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (cap == 0 || current == 0 || current + amount <= cap).then_some(current + amount)
        })
        .ok()
}

impl Drop for QueueHold<'_> {
    fn drop(&mut self) {
        if self.bytes > 0 || self.records > 0 {
            self.inner
                .queue_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
            self.inner
                .queue_records
                .fetch_sub(self.records, Ordering::AcqRel);
            self.inner.room.notify_waiters();
        }
    }
}

impl TransportBase for ShapedSender {
    async fn close(&self) -> TransportResult<()> {
        self.inner.closed.store(true, Ordering::Relaxed);
        if let Some(slots) = &self.inner.slots {
            slots.close();
        }
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.inner.closed.load(Ordering::Relaxed) && self.inner.config.faults.down.is_none()
    }

    fn name(&self) -> &'static str {
        self.inner.name
    }
}

impl TransportSender for ShapedSender {
    async fn send(&self, destination: &str, payload: bytes::Bytes) -> SendResult {
        let record = Record {
            payload,
            key: Some(Arc::from(destination)),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        };
        self.call(std::slice::from_ref(&record)).await
    }

    async fn send_record(&self, record: &Record) -> SendResult {
        self.call(std::slice::from_ref(record)).await
    }

    async fn send_batch(&self, records: &[Record]) -> SendResult {
        if records.is_empty() {
            return SendResult::Ok;
        }
        self.call(records).await
    }

    fn confirms_delivery(&self) -> SinkConfirmation {
        SinkConfirmation::Remote
    }

    fn dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        self.refuses(&record.payload)
            .then_some(DeadLetterReason::OutboundFilter)
    }
}

fn len_u64(payload: &[u8]) -> u64 {
    u64::try_from(payload.len()).unwrap_or(u64::MAX)
}

/// `cost * n`, saturating.
fn scale(cost: Duration, n: u64) -> Duration {
    nanos(cost.as_nanos().saturating_mul(u128::from(n)))
}

fn signed_nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

fn as_u64_nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn nanos(ns: u128) -> Duration {
    Duration::from_nanos(u64::try_from(ns).unwrap_or(u64::MAX))
}

fn bucket(records: u64) -> usize {
    let bits = u64::BITS - records.saturating_sub(1).leading_zeros();
    usize::try_from(bits).map_or(BATCH_BUCKETS - 1, |b| b.min(BATCH_BUCKETS - 1))
}

/// The splitmix64 output function.
fn splitmix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(payload: &'static [u8]) -> Record {
        Record {
            payload: bytes::Bytes::from_static(payload),
            key: Some(Arc::from("t")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_record_opens_a_window_and_confirms_when_it_closes_plus_the_round_trip() {
        let sender = ShapedSender::new(
            "k",
            ShapedConfig {
                call_latency: Duration::from_millis(2),
                confirm_window: Some(Duration::from_millis(20)),
                outcome: OutcomeMode::PerRecord,
                ..ShapedConfig::default()
            },
        );
        tokio::time::advance(Duration::from_millis(5)).await;
        let start = Instant::now();
        let first = tokio::spawn({
            let sender = sender.clone();
            async move { sender.send("t", bytes::Bytes::from_static(b"x")).await }
        });
        tokio::time::sleep(Duration::from_millis(15)).await;
        // Joins the window the first record opened 15 ms ago.
        let second = sender.send("t", bytes::Bytes::from_static(b"y")).await;
        assert!(second.is_ok());
        assert_eq!(start.elapsed(), Duration::from_millis(22));
        assert!(first.await.is_ok_and(|r| r.is_ok()));
        assert_eq!(sender.stats().records_confirmed, 2);
        assert_eq!(sender.bytes_in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_queue_answers_backpressured_after_its_wait_and_keeps_what_was_queued() {
        let sender = ShapedSender::new(
            "k",
            ShapedConfig {
                confirm_window: Some(Duration::from_secs(10)),
                outcome: OutcomeMode::PerRecord,
                queue_cap_bytes: 2,
                queue_full_wait: Duration::from_millis(50),
                ..ShapedConfig::default()
            },
        );
        let records = [record(b"a"), record(b"b"), record(b"c")];
        assert!(sender.send_batch(&records).await.is_backpressured());
        let stats = sender.stats();
        assert_eq!(stats.records_confirmed, 2, "the two queued are confirmed");
        assert_eq!(stats.peak_bytes_in_flight, 2);
        assert_eq!(sender.bytes_in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_all_or_nothing_call_pays_one_round_trip_plus_its_costs() {
        let sender = ShapedSender::new(
            "g",
            ShapedConfig {
                call_latency: Duration::from_millis(1),
                record_cost: Duration::from_millis(1),
                kib_cost: Duration::from_millis(10),
                ..ShapedConfig::default()
            },
        );
        let start = Instant::now();
        let records = [record(b"a"), record(b"b")];
        assert!(sender.send_batch(&records).await.is_ok());
        // 1 ms for the call, 1 ms for each record, 10 ms for the KiB it rounds up to.
        assert_eq!(start.elapsed(), Duration::from_millis(13));
        assert_eq!(sender.stats().calls, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn faults_refuse_a_record_and_take_a_destination_down() {
        let refusing = ShapedSender::new(
            "g",
            ShapedConfig {
                faults: Faults {
                    refuse: Some(|p| p.first() == Some(&b'!')),
                    ..Faults::default()
                },
                ..ShapedConfig::default()
            },
        );
        let records = [record(b"a"), record(b"!b")];
        assert!(refusing.dead_letter_reason(&records[1]).is_some());
        assert!(refusing.dead_letter_reason(&records[0]).is_none());
        assert!(refusing.send_batch(&records).await.is_filtered_dlq());
        assert_eq!(refusing.stats().records_confirmed, 1);

        let down = ShapedSender::new(
            "d",
            ShapedConfig {
                faults: Faults {
                    down: Some(Down {
                        after: Duration::from_millis(3),
                    }),
                    ..Faults::default()
                },
                ..ShapedConfig::default()
            },
        );
        assert!(down.send_batch(&records).await.is_backpressured());
        assert!(!down.is_healthy());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_holds_confirmations_until_it_ends() {
        let sender = ShapedSender::new(
            "k",
            ShapedConfig {
                faults: Faults {
                    stall: Some(Stall {
                        period: Duration::from_millis(100),
                        duration: Duration::from_millis(40),
                    }),
                    ..Faults::default()
                },
                ..ShapedConfig::default()
            },
        );
        let start = Instant::now();
        assert!(
            sender
                .send("t", bytes::Bytes::from_static(b"x"))
                .await
                .is_ok()
        );
        assert_eq!(start.elapsed(), Duration::from_millis(40));
    }

    #[tokio::test(start_paused = true)]
    async fn latency_under_a_millisecond_keeps_its_mean_across_calls() {
        let sender = ShapedSender::new(
            "g",
            ShapedConfig {
                call_latency: Duration::from_micros(400),
                ..ShapedConfig::default()
            },
        );
        let start = Instant::now();
        for _ in 0..10 {
            assert!(sender.send_batch(&[record(b"a")]).await.is_ok());
        }
        assert_eq!(start.elapsed(), Duration::from_millis(4));
    }

    #[tokio::test(start_paused = true)]
    async fn a_record_cap_refuses_a_call_that_does_not_fit() {
        let sender = ShapedSender::new(
            "g",
            ShapedConfig {
                call_latency: Duration::from_millis(10),
                queue_cap_records: 3,
                ..ShapedConfig::default()
            },
        );
        let two = [record(b"a"), record(b"b")];
        let first = tokio::spawn({
            let sender = sender.clone();
            let two = two.clone();
            async move { sender.send_batch(&two).await }
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(sender.send_batch(&two).await.is_backpressured());
        assert!(first.await.is_ok_and(|r| r.is_ok()));
        assert!(sender.send_batch(&two).await.is_ok());
    }

    #[test]
    fn batch_sizes_fall_in_power_of_two_buckets() {
        assert_eq!(bucket(0), 0);
        assert_eq!(bucket(1), 0);
        assert_eq!(bucket(2), 1);
        assert_eq!(bucket(3), 2);
        assert_eq!(bucket(1000), 10);
        assert_eq!(bucket(u64::MAX), BATCH_BUCKETS - 1);
    }

    #[test]
    fn jitter_repeats_for_a_seed() {
        let config = ShapedConfig {
            latency_jitter: Duration::from_millis(10),
            seed: 7,
            ..ShapedConfig::default()
        };
        let a = ShapedSender::new("a", config);
        let b = ShapedSender::new("b", config);
        let draws_a: Vec<Duration> = (0..5).map(|_| a.extra()).collect();
        let draws_b: Vec<Duration> = (0..5).map(|_| b.extra()).collect();
        assert_eq!(draws_a, draws_b);
        assert!(draws_a.iter().all(|d| *d < Duration::from_millis(10)));
    }
}
