// Project:   scalo
// File:      src/deployment/test_support/conformance.rs
// Purpose:   Acked-records-arrive conformance harness for pipeline tests
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Acked-records-arrive conformance harness.
//!
//! An app drives marked records through its own pipeline, injects a fault,
//! restarts, and asks one question: did every record whose source ack was
//! released either ARRIVE at the sink or get COUNTED as dead-lettered or
//! dropped with a reason? A plain arrival check cannot tell a lost record from
//! one that was never acknowledged, so the [`Ledger`] tracks all four and the
//! [`Verdict`] names every acknowledged marker that is unaccounted for.
//! Duplicates are counted and reported, never failed: at-least-once allows them.
//!
//! The pieces:
//!
//! - [`marked_payloads`] / [`marker_of`]: JSON records carrying a unique
//!   [`MARKER_FIELD`], which a transform that passes fields through keeps.
//! - [`PullLog`] / [`PullSource`]: a partitioned log with cumulative commits
//!   per partition, the shape of a Kafka source. A fresh [`PullSource`] starts
//!   from the committed offsets, which is what a restart sees.
//! - [`PushSource`]: the shape of a gRPC or HTTP source. A client pushes
//!   requests and resends each until it is answered with success; the source
//!   answers from [`release`](TransportReceiver::release) once armed, and at
//!   enqueue otherwise.
//! - [`FaultSink`]: a [`TransportSender`] that records arrivals and can refuse
//!   every send, hold one in flight, or permanently reject chosen markers.
//! - [`Case`]: runs a [`Fault`] against the app's pipeline over a pull or push
//!   source, restarts it, drains, and returns the [`Verdict`].
//!
//! ```rust,ignore
//! use scalo::deployment::test_support::conformance::{Case, Fault};
//!
//! for fault in Fault::ALL {
//!     let verdict = Case::new(fault)
//!         .run_pull(|source, sink, ledger, shutdown| async move {
//!             my_app::run_pipeline(&*source, &*sink, &ledger, shutdown).await;
//!         })
//!         .await;
//!     verdict.assert_lossless();
//! }
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::transport::{
    AckControl, AckKind, AcknowledgementsConfig, AcknowledgingReceiver, CommitToken,
    DeliveryStatus, HeldAcks, PayloadFormat, Record, RecordMeta, SendResult, TransportBase,
    TransportError, TransportReceiver, TransportResult, TransportSender, WorkBatch,
};

/// JSON field every conformance record carries its marker in.
pub const MARKER_FIELD: &str = "conformance_marker";

/// `n` JSON payloads, marker `0..n`, each `{"conformance_marker":<i>,"body":"record-<i>"}`.
#[must_use]
pub fn marked_payloads(n: u64) -> Vec<bytes::Bytes> {
    (0..n)
        .map(|i| bytes::Bytes::from(format!(r#"{{"{MARKER_FIELD}":{i},"body":"record-{i}"}}"#)))
        .collect()
}

/// The marker a JSON payload carries in [`MARKER_FIELD`], if any.
#[must_use]
pub fn marker_of(payload: &[u8]) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    value.get(MARKER_FIELD)?.as_u64()
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn json_record(payload: bytes::Bytes) -> Record {
    Record {
        payload,
        key: None,
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

// ---------------------------------------------------------------------------
// Ledger and verdict
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct LedgerState {
    records: u64,
    acked: BTreeSet<u64>,
    arrived: BTreeMap<u64, u32>,
    dead_lettered: BTreeSet<u64>,
    dropped: BTreeMap<u64, String>,
}

/// What one run acknowledged, delivered and accounted for, keyed by marker.
///
/// Cheap to clone: every clone shares one record. The source records acks, the
/// sink records arrivals, and the app's own DLQ or filter path records the rest.
#[derive(Debug, Clone, Default)]
pub struct Ledger(Arc<Mutex<LedgerState>>);

impl Ledger {
    /// A ledger for a run of `records` marked records, markers `0..records`.
    #[must_use]
    pub fn with_records(records: u64) -> Self {
        let ledger = Self::default();
        lock(&ledger.0).records = records;
        ledger
    }

    /// The source acknowledged `marker`: its upstream will never send it again.
    pub fn acked(&self, marker: u64) {
        lock(&self.0).acked.insert(marker);
    }

    /// `marker` reached the sink. A second arrival is a duplicate, not a failure.
    pub fn arrived(&self, marker: u64) {
        *lock(&self.0).arrived.entry(marker).or_default() += 1;
    }

    /// `marker` was written to a dead-letter queue that confirmed the write.
    pub fn dead_lettered(&self, marker: u64) {
        lock(&self.0).dead_lettered.insert(marker);
    }

    /// `marker` was dropped on purpose, for `reason`.
    pub fn dropped(&self, marker: u64, reason: impl Into<String>) {
        lock(&self.0).dropped.insert(marker, reason.into());
    }

    /// Judge the run so far.
    #[must_use]
    pub fn verdict(&self) -> Verdict {
        let s = lock(&self.0);
        let accounted = |m: &u64| {
            s.arrived.contains_key(m) || s.dead_lettered.contains(m) || s.dropped.contains_key(m)
        };
        Verdict {
            acked: s.acked.len(),
            unacknowledged: usize::try_from(s.records)
                .unwrap_or(usize::MAX)
                .saturating_sub(s.acked.len()),
            arrived: s.arrived.len(),
            dead_lettered: s.dead_lettered.len(),
            dropped: s.dropped.len(),
            duplicates: s
                .arrived
                .values()
                .map(|n| u64::from(n.saturating_sub(1)))
                .sum(),
            lost: s.acked.iter().filter(|m| !accounted(m)).copied().collect(),
        }
    }

    fn progress(&self) -> (usize, usize) {
        let s = lock(&self.0);
        (s.acked.len(), s.arrived.values().map(|&n| n as usize).sum())
    }
}

/// The outcome of a run: counts by marker, plus every acknowledged marker that
/// neither arrived nor was counted.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Verdict {
    /// Markers whose source ack was released.
    pub acked: usize,
    /// Records of the run (see [`Ledger::with_records`]) whose ack was never
    /// released. Not a loss: the upstream still holds them.
    pub unacknowledged: usize,
    /// Distinct markers that reached the sink.
    pub arrived: usize,
    /// Markers a confirmed dead-letter write accounts for.
    pub dead_lettered: usize,
    /// Markers dropped on purpose, with a reason.
    pub dropped: usize,
    /// Arrivals beyond the first per marker. Reported, never failed.
    pub duplicates: u64,
    /// Acknowledged markers that are unaccounted for: the loss.
    pub lost: Vec<u64>,
}

impl Verdict {
    /// Whether every acknowledged marker arrived or was counted.
    #[must_use]
    pub fn is_lossless(&self) -> bool {
        self.lost.is_empty()
    }

    /// Panic naming the lost markers unless the run was lossless.
    ///
    /// # Panics
    ///
    /// When any acknowledged marker neither arrived nor was counted.
    #[track_caller]
    pub fn assert_lossless(&self) {
        assert!(
            self.is_lossless(),
            "{} acknowledged record(s) neither arrived nor were counted: {:?} ({self:?})",
            self.lost.len(),
            self.lost
        );
    }
}

// ---------------------------------------------------------------------------
// Pull source: a partitioned log with cumulative commits
// ---------------------------------------------------------------------------

/// Position of one record in a [`PullLog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullToken {
    /// Partition index.
    pub partition: usize,
    /// Offset within the partition.
    pub offset: usize,
}

impl std::fmt::Display for PullToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.partition, self.offset)
    }
}

impl CommitToken for PullToken {}

#[derive(Debug)]
struct PullState {
    partitions: Vec<Vec<bytes::Bytes>>,
    /// Next offset a restart reads, per partition: everything below it is acknowledged.
    committed: Vec<usize>,
}

/// A partitioned log with cumulative per-partition commits, like a Kafka topic.
///
/// Committing offset `k` acknowledges every offset up to `k` in that partition,
/// which is how a commit that skips an unfinished record loses it.
#[derive(Debug, Clone)]
pub struct PullLog {
    state: Arc<Mutex<PullState>>,
    ledger: Ledger,
    acknowledgements: AcknowledgementsConfig,
}

impl PullLog {
    /// Spread `payloads` round-robin over `partitions` partitions.
    ///
    /// # Panics
    ///
    /// When `partitions` is zero.
    #[must_use]
    pub fn new(ledger: Ledger, partitions: usize, payloads: Vec<bytes::Bytes>) -> Self {
        assert!(partitions > 0, "a pull log needs at least one partition");
        let mut parts = vec![Vec::new(); partitions];
        for (i, p) in payloads.into_iter().enumerate() {
            parts[i % partitions].push(p);
        }
        Self {
            state: Arc::new(Mutex::new(PullState {
                committed: vec![0; partitions],
                partitions: parts,
            })),
            ledger,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }

    /// The `acknowledgements` config its sources report.
    #[must_use]
    pub fn acknowledgements(mut self, acknowledgements: AcknowledgementsConfig) -> Self {
        self.acknowledgements = acknowledgements;
        self
    }

    /// A consumer of `assigned` partitions, starting at their committed offsets.
    #[must_use]
    pub fn source(&self, assigned: &[usize]) -> PullSource {
        let committed = lock(&self.state).committed.clone();
        PullSource {
            log: self.clone(),
            positions: Mutex::new(assigned.iter().map(|&p| (p, committed[p])).collect()),
            armed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    /// Records not yet acknowledged, over every partition.
    #[must_use]
    pub fn uncommitted(&self) -> usize {
        let s = lock(&self.state);
        s.partitions
            .iter()
            .zip(&s.committed)
            .map(|(p, &c)| p.len().saturating_sub(c))
            .sum()
    }
}

/// One consumer instance over a [`PullLog`]; a new one is a restart.
#[derive(Debug)]
pub struct PullSource {
    log: PullLog,
    /// `(partition, next offset to read)` for each assigned partition.
    positions: Mutex<Vec<(usize, usize)>>,
    armed: AtomicBool,
    closed: AtomicBool,
}

impl TransportBase for PullSource {
    async fn close(&self) -> TransportResult<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    fn name(&self) -> &'static str {
        "conformance-pull"
    }
}

impl TransportReceiver for PullSource {
    type Token = PullToken;

    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<PullToken>> {
        // Like Kafka, a closed pull source stops at once: what it did not commit is read again.
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }
        let batch = {
            let state = lock(&self.log.state);
            let mut positions = lock(&self.positions);
            let mut records = Vec::new();
            let mut tokens = Vec::new();
            for (partition, next) in positions.iter_mut() {
                while records.len() < max
                    && let Some(payload) = state.partitions[*partition].get(*next)
                {
                    records.push(json_record(payload.clone()));
                    tokens.push(PullToken {
                        partition: *partition,
                        offset: *next,
                    });
                    *next += 1;
                }
            }
            WorkBatch::new(records, tokens)
        };
        if batch.is_empty() {
            // An empty log waits by awaiting, never by spinning the worker.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(batch)
    }

    async fn commit(&self, tokens: &[PullToken]) -> TransportResult<()> {
        let mut state = lock(&self.log.state);
        for t in tokens {
            let from = state.committed[t.partition];
            if t.offset < from {
                continue;
            }
            // Cumulative: committing k acknowledges every offset below it in the partition.
            for offset in from..=t.offset {
                if let Some(m) = marker_of(&state.partitions[t.partition][offset]) {
                    self.log.ledger.acked(m);
                }
            }
            state.committed[t.partition] = t.offset + 1;
        }
        Ok(())
    }

    fn ack_control(&self) -> Option<&dyn AckControl> {
        Some(self)
    }
}

impl AckControl for PullSource {
    fn enabled(&self) -> bool {
        self.log.acknowledgements.enabled
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    fn kind(&self) -> AckKind {
        AckKind::Pull
    }

    fn held(&self) -> HeldAcks {
        let committed = lock(&self.log.state).committed.clone();
        let held = lock(&self.positions)
            .iter()
            .map(|&(p, next)| next.saturating_sub(committed[p]) as u64)
            .sum();
        HeldAcks::new(held, 0, None, None)
    }
}

impl AcknowledgingReceiver for PullSource {
    fn acknowledgements(&self) -> AcknowledgementsConfig {
        self.log.acknowledgements
    }
}

// ---------------------------------------------------------------------------
// Push source: a sender that resends until answered with success
// ---------------------------------------------------------------------------

/// The request a pushed record belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushToken {
    /// Request id, unique per [`PushSource`].
    pub request: u64,
}

impl std::fmt::Display for PushToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "req-{}", self.request)
    }
}

impl CommitToken for PushToken {}

#[derive(Debug)]
struct HeldRequest {
    remaining: usize,
    worst: DeliveryStatus,
    deadline: Instant,
    answer: oneshot::Sender<bool>,
}

/// A push source (the shape of a gRPC or HTTP server) holding each request's
/// answer until every record in it is released.
///
/// Unarmed, it answers success at enqueue, which is how a push hop loses what
/// it acknowledged when it is killed. A dropped source drops its held answers,
/// and the client resends those requests.
#[derive(Debug)]
pub struct PushSource {
    queue: Mutex<VecDeque<(bytes::Bytes, PushToken)>>,
    held: Mutex<HashMap<u64, HeldRequest>>,
    next_request: AtomicU64,
    hold: Duration,
    acknowledgements: AcknowledgementsConfig,
    armed: AtomicBool,
    closed: AtomicBool,
    queued: Notify,
}

impl PushSource {
    /// A source that holds each answer for at most `hold`.
    #[must_use]
    pub fn new(hold: Duration) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            held: Mutex::new(HashMap::new()),
            next_request: AtomicU64::new(0),
            hold,
            acknowledgements: AcknowledgementsConfig::default(),
            armed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            queued: Notify::new(),
        }
    }

    /// The `acknowledgements` config it reports.
    #[must_use]
    pub fn with_acknowledgements(mut self, acknowledgements: AcknowledgementsConfig) -> Self {
        self.acknowledgements = acknowledgements;
        self
    }

    /// Queue one request; the receiver resolves to `true` for a success answer.
    pub fn push(&self, payloads: Vec<bytes::Bytes>) -> oneshot::Receiver<bool> {
        let (answer, answered) = oneshot::channel();
        if self.closed.load(Ordering::Acquire) {
            let _ = answer.send(false);
            return answered;
        }
        let request = self.next_request.fetch_add(1, Ordering::Relaxed);
        let holding = self.acknowledgements.enabled && self.armed.load(Ordering::Acquire);
        if holding {
            lock(&self.held).insert(
                request,
                HeldRequest {
                    remaining: payloads.len(),
                    worst: DeliveryStatus::Delivered,
                    deadline: Instant::now() + self.hold,
                    answer,
                },
            );
        } else {
            let _ = answer.send(true);
        }
        let mut queue = lock(&self.queue);
        for p in payloads {
            queue.push_back((p, PushToken { request }));
        }
        drop(queue);
        self.queued.notify_one();
        answered
    }

    /// Drop every held answer and refuse new requests, as a killed server's
    /// closed connections do: each sender sees no answer and resends.
    pub fn abandon(&self) {
        self.closed.store(true, Ordering::Release);
        lock(&self.held).clear();
        lock(&self.queue).clear();
        self.queued.notify_waiters();
    }

    fn settle(&self, tokens: &[PushToken], outcome: DeliveryStatus) {
        let mut held = lock(&self.held);
        for t in tokens {
            let Some(req) = held.get_mut(&t.request) else {
                continue;
            };
            req.worst = req.worst.max(outcome);
            req.remaining = req.remaining.saturating_sub(1);
            if req.remaining == 0
                && let Some(done) = held.remove(&t.request)
            {
                let _ = done.answer.send(done.worst.should_commit());
            }
        }
    }
}

impl TransportBase for PushSource {
    async fn close(&self) -> TransportResult<()> {
        self.closed.store(true, Ordering::Release);
        self.queued.notify_waiters();
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    fn name(&self) -> &'static str {
        "conformance-push"
    }
}

impl TransportReceiver for PushSource {
    type Token = PushToken;

    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<PushToken>> {
        let wake = self.queued.notified();
        let (records, tokens): (Vec<_>, Vec<_>) = {
            let mut queue = lock(&self.queue);
            let n = queue.len().min(max);
            queue.drain(..n).map(|(p, t)| (json_record(p), t)).unzip()
        };
        if records.is_empty() {
            // After close, what was queued is returned first, then Closed.
            if self.closed.load(Ordering::Acquire) {
                return Err(TransportError::Closed);
            }
            let _ = tokio::time::timeout(Duration::from_millis(20), wake).await;
        }
        Ok(WorkBatch::new(records, tokens))
    }

    async fn commit(&self, tokens: &[PushToken]) -> TransportResult<()> {
        self.settle(tokens, DeliveryStatus::Delivered);
        Ok(())
    }

    async fn release(&self, tokens: &[PushToken], outcome: DeliveryStatus) -> TransportResult<()> {
        self.settle(tokens, outcome);
        Ok(())
    }

    fn hold_deadline(&self, tokens: &[PushToken]) -> Option<Instant> {
        let held = lock(&self.held);
        tokens
            .iter()
            .filter_map(|t| held.get(&t.request).map(|r| r.deadline))
            .min()
    }

    fn ack_control(&self) -> Option<&dyn AckControl> {
        Some(self)
    }
}

impl AckControl for PushSource {
    fn enabled(&self) -> bool {
        self.acknowledgements.enabled
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    fn kind(&self) -> AckKind {
        AckKind::Push
    }

    fn held(&self) -> HeldAcks {
        let held = lock(&self.held);
        let deadline = held.values().map(|r| r.deadline).min();
        HeldAcks::new(held.len() as u64, 0, None, deadline)
    }
}

impl AcknowledgingReceiver for PushSource {
    fn acknowledgements(&self) -> AcknowledgementsConfig {
        self.acknowledgements
    }
}

/// The live push instances a client sends to; the case swaps them on restart.
#[derive(Debug, Clone, Default)]
struct Endpoints(Arc<Mutex<Vec<Arc<PushSource>>>>);

impl Endpoints {
    fn add(&self, source: Arc<PushSource>) {
        lock(&self.0).push(source);
    }

    fn remove(&self, source: &Arc<PushSource>) {
        lock(&self.0).retain(|s| !Arc::ptr_eq(s, source));
    }

    fn pick(&self, n: u64) -> Option<Arc<PushSource>> {
        let live = lock(&self.0);
        let len = live.len() as u64;
        (len > 0).then(|| Arc::clone(&live[usize::try_from(n % len).unwrap_or(0)]))
    }
}

/// Send every request, resending each until it is answered with success, and
/// mark its markers acknowledged then.
async fn push_all(
    endpoints: Endpoints,
    ledger: Ledger,
    payloads: Vec<bytes::Bytes>,
    request_size: usize,
) {
    let mut attempt = 0u64;
    for request in payloads.chunks(request_size.max(1)) {
        loop {
            attempt += 1;
            let Some(source) = endpoints.pick(attempt) else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let answered = source.push(request.to_vec());
            drop(source);
            if let Ok(Ok(true)) = tokio::time::timeout(Duration::from_secs(10), answered).await {
                for m in request.iter().filter_map(|p| marker_of(p)) {
                    ledger.acked(m);
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Fault-injecting sink
// ---------------------------------------------------------------------------

/// The fault plan every [`FaultSink`] of one run shares.
#[derive(Debug, Default)]
struct Faults {
    refusing: AtomicBool,
    /// `(marker, window)`: the first send carrying `marker` starts refusing for `window`.
    refuse_at: Mutex<Option<(u64, Duration)>>,
    refuse_until: Mutex<Option<Instant>>,
    refused: AtomicU64,
    rejected: Mutex<BTreeSet<u64>>,
    rejections: AtomicU64,
    hold_at: Mutex<Option<u64>>,
    /// The instance whose send is held, while one is.
    held_by: Mutex<Option<usize>>,
    unhold: Notify,
    calls: AtomicU64,
}

/// A sink that records every marker it accepts, and injects faults on demand.
///
/// Every fault is keyed on a marker, so it lands at the same point of the
/// stream however fast the pipeline runs. Sinks made with
/// [`instance`](Self::instance) share one fault plan and ledger, and report
/// which instance a held send belongs to.
#[derive(Debug, Default)]
pub struct FaultSink {
    ledger: Ledger,
    faults: Arc<Faults>,
    instance: usize,
}

impl FaultSink {
    /// A healthy sink recording into `ledger`.
    #[must_use]
    pub fn new(ledger: Ledger) -> Self {
        Self {
            ledger,
            ..Self::default()
        }
    }

    /// A sink for pipeline instance `instance`, sharing this one's ledger and faults.
    #[must_use]
    pub fn instance(&self, instance: usize) -> Self {
        Self {
            ledger: self.ledger.clone(),
            faults: Arc::clone(&self.faults),
            instance,
        }
    }

    /// While `true`, every send is refused with [`SendResult::Backpressured`].
    pub fn refuse(&self, refusing: bool) {
        self.faults.refusing.store(refusing, Ordering::Release);
    }

    /// Refuse the send carrying `marker`, and every send for `window` after it.
    pub fn refuse_at(&self, marker: u64, window: Duration) {
        *lock(&self.faults.refuse_at) = Some((marker, window));
    }

    /// Sends refused so far.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.faults.refused.load(Ordering::Acquire)
    }

    /// Permanently reject `marker`: a block holding it records the records
    /// before it and returns [`SendResult::Fatal`].
    pub fn reject(&self, marker: u64) {
        lock(&self.faults.rejected).insert(marker);
    }

    /// Sends failed by a rejected marker so far.
    #[must_use]
    pub fn rejections(&self) -> u64 {
        self.faults.rejections.load(Ordering::Acquire)
    }

    /// Stop rejecting every marker.
    pub fn accept_all(&self) {
        lock(&self.faults.rejected).clear();
    }

    /// Number of `send_batch` calls so far, refused ones included.
    #[must_use]
    pub fn calls(&self) -> u64 {
        self.faults.calls.load(Ordering::Acquire)
    }

    /// Hold the send carrying `marker` in flight, before it records anything,
    /// until [`unhold`](Self::unhold): the point a kill or stop lands mid-batch.
    pub fn hold_at(&self, marker: u64) {
        *lock(&self.faults.hold_at) = Some(marker);
    }

    /// The instance whose send is held, if one is.
    #[must_use]
    pub fn held_by(&self) -> Option<usize> {
        *lock(&self.faults.held_by)
    }

    /// Let a held send continue, or forget it if its pipeline was killed.
    pub fn unhold(&self) {
        *lock(&self.faults.hold_at) = None;
        *lock(&self.faults.held_by) = None;
        self.faults.unhold.notify_waiters();
    }

    /// Whether this send is refused, starting a `refuse_at` window when it
    /// carries the trigger marker.
    fn refusing(&self, markers: &[Option<u64>]) -> bool {
        let faults = &self.faults;
        let now = Instant::now();
        {
            let mut trigger = lock(&faults.refuse_at);
            if let Some((marker, window)) = *trigger
                && markers.contains(&Some(marker))
            {
                *lock(&faults.refuse_until) = Some(now + window);
                *trigger = None;
            }
        }
        faults.refusing.load(Ordering::Acquire)
            || lock(&faults.refuse_until).is_some_and(|until| now < until)
    }
}

impl TransportBase for FaultSink {
    async fn close(&self) -> TransportResult<()> {
        Ok(())
    }

    fn is_healthy(&self) -> bool {
        !self.faults.refusing.load(Ordering::Acquire)
    }

    fn name(&self) -> &'static str {
        "conformance-sink"
    }
}

impl TransportSender for FaultSink {
    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        self.send_batch(std::slice::from_ref(&json_record(payload)))
            .await
    }

    async fn send_batch(&self, records: &[Record]) -> SendResult {
        let faults = &self.faults;
        faults.calls.fetch_add(1, Ordering::AcqRel);
        let markers: Vec<Option<u64>> = records.iter().map(|r| marker_of(&r.payload)).collect();

        let hold = {
            let mut hold_at = lock(&faults.hold_at);
            let hit = hold_at.is_some_and(|m| markers.contains(&Some(m)));
            if hit {
                *hold_at = None;
            }
            hit
        };
        if hold {
            // Created before held_by is set, so an unhold that follows cannot be missed.
            let released = faults.unhold.notified();
            *lock(&faults.held_by) = Some(self.instance);
            released.await;
        }

        if self.refusing(&markers) {
            faults.refused.fetch_add(1, Ordering::AcqRel);
            return SendResult::Backpressured;
        }
        let rejected = lock(&faults.rejected).clone();
        for marker in markers {
            if let Some(m) = marker.filter(|m| rejected.contains(m)) {
                faults.rejections.fetch_add(1, Ordering::AcqRel);
                return SendResult::Fatal(TransportError::Send(format!(
                    "conformance sink rejects marker {m}"
                )));
            }
            if let Some(m) = marker {
                self.ledger.arrived(m);
            }
        }
        SendResult::Ok
    }
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/// A fault injected into a conformance run.
///
/// Each lands at the send carrying the middle marker, `records / 2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    /// The shutdown token fires while that send is in flight; it then completes.
    GracefulStop,
    /// The pipeline task is aborted while that send is in flight.
    KillMidBatch,
    /// The sink refuses that send and every send for 300 ms after it, while
    /// the source keeps delivering.
    DownstreamRefusing,
    /// The sink rejects that record permanently, after the records before it
    /// in the block landed, until the pipeline restarts.
    RejectMidStream,
    /// Two instances share one source; the one holding that send is killed
    /// and the source's other consumers take over its share.
    TwoInstances,
}

impl Fault {
    /// Every fault, in declaration order.
    pub const ALL: [Self; 5] = [
        Self::GracefulStop,
        Self::KillMidBatch,
        Self::DownstreamRefusing,
        Self::RejectMidStream,
        Self::TwoInstances,
    ];
}

/// How long a stopping pipeline gets before it is aborted.
const STOP_GRACE: Duration = Duration::from_secs(15);

/// One running pipeline instance.
struct Instance {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl Instance {
    fn spawn<Fut>(shutdown: CancellationToken, run: Fut) -> Self
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self {
            shutdown,
            task: tokio::spawn(run),
        }
    }

    /// Fire shutdown and wait for the pipeline to drain, aborting it past the grace.
    async fn stop(self) {
        self.shutdown.cancel();
        self.join().await;
    }

    /// Wait for the pipeline to return, aborting it past the grace.
    async fn join(self) {
        let abort = self.task.abort_handle();
        if tokio::time::timeout(STOP_GRACE, self.task).await.is_err() {
            abort.abort();
        }
    }

    /// Abort the pipeline where it stands, as a kill -9 does.
    async fn kill(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// Poll `ready` every 5 ms until it holds or `within` passes; whether it held.
async fn wait_for(within: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let until = tokio::time::Instant::now() + within;
    loop {
        if ready() {
            return true;
        }
        if tokio::time::Instant::now() >= until {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Wait until neither acknowledgements nor arrivals move for `quiet`, or `within` passes.
async fn wait_until_quiet(ledger: &Ledger, quiet: Duration, within: Duration) {
    let until = tokio::time::Instant::now() + within;
    let mut last = ledger.progress();
    let mut since = tokio::time::Instant::now();
    while tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = ledger.progress();
        if now == last {
            if since.elapsed() >= quiet {
                return;
            }
        } else {
            last = now;
            since = tokio::time::Instant::now();
        }
    }
}

/// One conformance run: a [`Fault`] against an app's pipeline, a restart, a
/// drain, and the [`Verdict`].
///
/// The pipeline closure builds and runs the app over the given source and sink
/// until the token fires, and is called once per instance: a restart is a new
/// call. The [`Ledger`] it receives is where the app's own dead-letter and drop
/// paths record the markers they account for.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Case {
    /// The fault injected.
    pub fault: Fault,
    /// Marked records driven through the pipeline.
    pub records: u64,
    /// Partitions of the pull log; at least two for [`Fault::TwoInstances`].
    pub partitions: usize,
    /// Records per pushed request.
    pub request_size: usize,
    /// How long a push source holds an answer.
    pub hold: Duration,
    /// How long the final drain may take before the run is judged as it stands.
    pub drain_within: Duration,
}

impl Case {
    /// A case with 200 records over 2 partitions, 10 records per request.
    #[must_use]
    pub fn new(fault: Fault) -> Self {
        Self {
            fault,
            records: 200,
            partitions: 2,
            request_size: 10,
            hold: Duration::from_secs(5),
            drain_within: Duration::from_secs(30),
        }
    }

    /// Drive `records` marked records.
    #[must_use]
    pub fn records(mut self, records: u64) -> Self {
        self.records = records;
        self
    }

    /// Spread the pull log over `partitions` partitions.
    #[must_use]
    pub fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions.max(1);
        self
    }

    /// The marker every fault is keyed on.
    fn fault_marker(&self) -> u64 {
        self.records / 2
    }

    /// Run the case over a [`PullSource`].
    ///
    /// # Panics
    ///
    /// When the fault never lands, because the pipeline never sent the marker
    /// it is keyed on: a run that injected nothing proves nothing.
    pub async fn run_pull<F, Fut>(&self, pipeline: F) -> Verdict
    where
        F: Fn(Arc<PullSource>, Arc<FaultSink>, Ledger, CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let partitions = if self.fault == Fault::TwoInstances {
            self.partitions.max(2)
        } else {
            self.partitions
        };
        let ledger = Ledger::with_records(self.records);
        let log = PullLog::new(ledger.clone(), partitions, marked_payloads(self.records));
        let sink = FaultSink::new(ledger.clone());
        self.plan(&sink);
        let all: Vec<usize> = (0..partitions).collect();
        let start = |id: usize, assigned: &[usize]| {
            let shutdown = CancellationToken::new();
            let source = Arc::new(log.source(assigned));
            Instance::spawn(
                shutdown.clone(),
                pipeline(
                    source,
                    Arc::new(sink.instance(id)),
                    ledger.clone(),
                    shutdown,
                ),
            )
        };

        let landed = if self.fault == Fault::TwoInstances {
            // Instance i reads partition i; the middle marker's partition decides the victim.
            let mut instances: Vec<Option<Instance>> =
                all.iter().map(|&p| Some(start(p, &all[p..=p]))).collect();
            let landed = wait_for(self.drain_within, || sink.held_by().is_some()).await;
            if let Some(victim) = sink.held_by()
                && let Some(instance) = instances[victim].take()
            {
                instance.kill().await;
                sink.unhold();
                // Another consumer takes over the killed instance's partition from its commit.
                instances[victim] = Some(start(partitions, &all[victim..=victim]));
            }
            wait_for(self.drain_within, || log.uncommitted() == 0).await;
            for instance in instances.into_iter().flatten() {
                instance.stop().await;
            }
            landed
        } else {
            let first = start(0, &all);
            self.inject(first, &sink, &ledger, || {}).await
        };
        assert!(landed, "{:?} never landed: {}", self.fault, NEVER_LANDED);

        let restart = start(partitions + 1, &all);
        wait_for(self.drain_within, || log.uncommitted() == 0).await;
        restart.stop().await;
        ledger.verdict()
    }

    /// Run the case over a [`PushSource`] fed by a client that resends until answered.
    pub async fn run_push<F, Fut>(&self, pipeline: F) -> Verdict
    where
        F: Fn(Arc<PushSource>, Arc<FaultSink>, Ledger, CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let ledger = Ledger::with_records(self.records);
        let sink = FaultSink::new(ledger.clone());
        self.plan(&sink);
        let endpoints = Endpoints::default();
        let start = |id: usize| {
            let shutdown = CancellationToken::new();
            let source = Arc::new(PushSource::new(self.hold));
            endpoints.add(Arc::clone(&source));
            let instance = Instance::spawn(
                shutdown.clone(),
                pipeline(
                    Arc::clone(&source),
                    Arc::new(sink.instance(id)),
                    ledger.clone(),
                    shutdown,
                ),
            );
            (source, instance)
        };
        // A departing instance leaves the endpoint list first, so no sender reaches it after.
        let take_down = |source: &Arc<PushSource>| endpoints.remove(source);

        let mut instances = vec![Some(start(0))];
        if self.fault == Fault::TwoInstances {
            instances.push(Some(start(1)));
        }
        let client = tokio::spawn(push_all(
            endpoints.clone(),
            ledger.clone(),
            marked_payloads(self.records),
            self.request_size,
        ));

        let landed = if self.fault == Fault::TwoInstances {
            let landed = wait_for(self.drain_within, || sink.held_by().is_some()).await;
            if let Some(victim) = sink.held_by()
                && let Some((source, instance)) = instances[victim].take()
            {
                take_down(&source);
                instance.kill().await;
                source.abandon();
                sink.unhold();
            }
            wait_for(self.drain_within, || client.is_finished()).await;
            landed
        } else if let Some((source, instance)) = instances[0].take() {
            let landed = self
                .inject(instance, &sink, &ledger, || take_down(&source))
                .await;
            source.abandon();
            landed
        } else {
            false
        };
        for (source, instance) in instances.into_iter().flatten() {
            take_down(&source);
            instance.stop().await;
            source.abandon();
        }
        assert!(landed, "{:?} never landed: {}", self.fault, NEVER_LANDED);

        let (restart_source, restart) = start(2);
        wait_for(self.drain_within, || client.is_finished()).await;
        take_down(&restart_source);
        restart.stop().await;
        client.abort();
        ledger.verdict()
    }

    /// Key this case's fault on the middle marker, before any instance starts.
    fn plan(&self, sink: &FaultSink) {
        let marker = self.fault_marker();
        match self.fault {
            Fault::GracefulStop | Fault::KillMidBatch | Fault::TwoInstances => {
                sink.hold_at(marker);
            }
            Fault::DownstreamRefusing => sink.refuse_at(marker, Duration::from_millis(300)),
            Fault::RejectMidStream => sink.reject(marker),
        }
    }

    /// Wait for a single-instance fault to land, then stop or kill `instance`,
    /// calling `going_down` just before. Returns whether the fault landed.
    async fn inject(
        &self,
        instance: Instance,
        sink: &FaultSink,
        ledger: &Ledger,
        going_down: impl FnOnce(),
    ) -> bool {
        let within = self.drain_within;
        let quiet = Duration::from_millis(500);
        match self.fault {
            Fault::GracefulStop => {
                let landed = wait_for(within, || sink.held_by().is_some()).await;
                going_down();
                instance.shutdown.cancel();
                sink.unhold();
                instance.join().await;
                landed
            }
            Fault::KillMidBatch => {
                let landed = wait_for(within, || sink.held_by().is_some()).await;
                going_down();
                instance.kill().await;
                sink.unhold();
                landed
            }
            Fault::DownstreamRefusing => {
                let landed = wait_for(within, || sink.refused() > 0).await;
                wait_until_quiet(ledger, quiet, within).await;
                going_down();
                instance.stop().await;
                landed
            }
            Fault::RejectMidStream => {
                let landed = wait_for(within, || sink.rejections() > 0).await;
                wait_until_quiet(ledger, quiet, within).await;
                going_down();
                instance.stop().await;
                sink.accept_all();
                landed
            }
            Fault::TwoInstances => {
                unreachable!("TwoInstances runs its own instances, never through inject")
            }
        }
    }
}

/// Why a fault that never landed fails the case.
const NEVER_LANDED: &str =
    "the pipeline never sent the marker the fault is keyed on, so the run injected nothing";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_round_trip() {
        let payloads = marked_payloads(3);
        let markers: Vec<_> = payloads.iter().map(|p| marker_of(p)).collect();
        assert_eq!(markers, vec![Some(0), Some(1), Some(2)]);
        assert_eq!(marker_of(b"{}"), None);
        assert_eq!(marker_of(b"not json"), None);
    }

    #[test]
    fn an_acked_record_that_neither_arrived_nor_was_counted_is_lost() {
        let ledger = Ledger::with_records(6);
        for m in 0..5 {
            ledger.acked(m);
        }
        ledger.arrived(0);
        ledger.arrived(0);
        ledger.arrived(1);
        ledger.dead_lettered(2);
        ledger.dropped(3, "filtered");
        // Arrived without an ack is at-least-once, not loss.
        ledger.arrived(5);

        let v = ledger.verdict();
        assert_eq!(v.lost, vec![4]);
        assert_eq!(v.duplicates, 1);
        assert_eq!(v.unacknowledged, 1);
        assert_eq!(
            (v.acked, v.arrived, v.dead_lettered, v.dropped),
            (5, 3, 1, 1)
        );
        assert!(!v.is_lossless());
    }

    #[tokio::test]
    async fn a_commit_acknowledges_every_lower_offset_in_its_partition() {
        let ledger = Ledger::with_records(6);
        let log = PullLog::new(ledger.clone(), 2, marked_payloads(6));
        let source = log.source(&[0, 1]);
        let batch = source.recv(10).await.unwrap();
        assert_eq!(batch.len(), 6);

        // Partition 0 holds markers 0, 2, 4. Committing its offset 2 acks all three.
        source
            .commit(&[PullToken {
                partition: 0,
                offset: 2,
            }])
            .await
            .unwrap();
        assert_eq!(ledger.verdict().acked, 3);
        assert_eq!(ledger.verdict().lost, vec![0, 2, 4]);
        assert_eq!(log.uncommitted(), 3);

        // A restarted consumer starts from the committed offsets.
        let restarted = log.source(&[0, 1]);
        let redelivered = restarted.recv(10).await.unwrap();
        let markers: Vec<_> = redelivered
            .records
            .iter()
            .filter_map(|r| marker_of(&r.payload))
            .collect();
        assert_eq!(markers, vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn an_armed_push_source_answers_on_release_and_an_unarmed_one_at_enqueue() {
        let source = PushSource::new(Duration::from_secs(5));
        let unarmed = source.push(marked_payloads(2));
        assert_eq!(unarmed.await, Ok(true), "unarmed answers at enqueue");

        source.arm();
        let mut held = source.push(marked_payloads(2));
        let batch = source.recv(10).await.unwrap();
        assert_eq!(batch.len(), 4);
        let second: Vec<PushToken> = batch.commit_tokens[2..].to_vec();
        assert!(source.hold_deadline(&second).is_some());
        source
            .release(&second[..1], DeliveryStatus::Delivered)
            .await
            .unwrap();
        assert!(
            held.try_recv().is_err(),
            "held until every record is released"
        );
        source
            .release(&second[1..], DeliveryStatus::Errored)
            .await
            .unwrap();
        assert_eq!(held.await, Ok(false), "an errored record fails the request");

        let dropped = PushSource::new(Duration::from_secs(5));
        dropped.arm();
        let orphan = dropped.push(marked_payloads(1));
        drop(dropped);
        assert!(
            orphan.await.is_err(),
            "a killed source leaves the sender unanswered"
        );
    }

    #[tokio::test]
    async fn the_fault_sink_refuses_holds_and_rejects_on_demand() {
        let ledger = Ledger::with_records(3);
        let sink = Arc::new(FaultSink::new(ledger.clone()));
        let records: Vec<Record> = marked_payloads(3).into_iter().map(json_record).collect();

        sink.refuse(true);
        assert!(sink.send_batch(&records).await.is_backpressured());
        assert_eq!(ledger.verdict().arrived, 0);

        sink.refuse(false);
        sink.reject(1);
        assert!(sink.send_batch(&records).await.is_fatal());
        assert_eq!(
            ledger.verdict().arrived,
            1,
            "records before the rejected one landed"
        );

        assert_eq!(sink.rejections(), 1);
        sink.accept_all();

        // The hold is keyed on a marker and reports the instance holding it.
        sink.hold_at(2);
        let second = Arc::new(sink.instance(7));
        assert!(
            second.send_batch(&records[..1]).await.is_ok(),
            "no marker 2: not held"
        );
        let held = {
            let second = Arc::clone(&second);
            let records = records.clone();
            tokio::spawn(async move { second.send_batch(&records).await })
        };
        assert!(wait_for(Duration::from_secs(5), || sink.held_by() == Some(7)).await);
        assert_eq!(ledger.verdict().arrived, 1, "a held send records nothing");
        sink.unhold();
        assert!(held.await.unwrap().is_ok());
        assert_eq!(ledger.verdict().arrived, 3);
        assert_eq!(
            ledger.verdict().duplicates,
            2,
            "marker 0 arrived three times"
        );

        // A refusal window opens at its marker and lasts its window.
        sink.refuse_at(1, Duration::from_millis(100));
        assert!(
            sink.send_batch(&records[..1]).await.is_ok(),
            "marker 0 is before it"
        );
        assert!(sink.send_batch(&records).await.is_backpressured());
        assert!(
            sink.send_batch(&records[..1]).await.is_backpressured(),
            "inside the window"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            sink.send_batch(&records[..1]).await.is_ok(),
            "after the window"
        );
        assert_eq!(sink.refused(), 3);
        assert_eq!(sink.calls(), 8);
    }
}
