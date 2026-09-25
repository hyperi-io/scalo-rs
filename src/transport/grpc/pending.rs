// Project:   scalo
// File:      src/transport/grpc/pending.rs
// Purpose:   Push responses held until the records they carried are released
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Push responses held until every record they carried is released.
//!
//! An armed server gives each request one contiguous range of sequence
//! numbers, queues its records with those numbers as their tokens, and answers
//! the request only once the consumer has released every one of them. A token
//! is found by its sequence number alone, so it carries nothing beyond the
//! number it always had.
//!
//! The registry owns the held-byte ceiling as well: a request is admitted only
//! while the bytes already held leave room for it, and always when nothing is
//! held, so one request larger than the ceiling still gets through.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::transport::ack::{AckControl, AckKind, HeldAcks};
use crate::transport::finalizer::DeliveryStatus;

/// The held-byte ceiling when no memory guard sizes it.
pub(crate) const DEFAULT_MAX_HELD_BYTES: u64 = 256 * 1024 * 1024;

/// The longest a response is held, and the whole budget for a sender that sets
/// no deadline of its own.
pub(crate) const DEFAULT_MAX_HOLD: Duration = Duration::from_secs(25);

/// The least headroom kept below a sender's deadline.
const MIN_MARGIN: Duration = Duration::from_secs(1);

/// How a held request ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Every record was released, with the worst status among them.
    Released(DeliveryStatus),
    /// The hold budget ran out before every record was released.
    Expired,
    /// The transport shut down before every record was released.
    Shutdown,
}

/// The longest a response is held: `max_hold`, or the sender's deadline less a
/// margin of a tenth of it, at least 1 s and at most half the deadline.
#[must_use]
pub(crate) fn hold_budget(max_hold: Duration, sender_deadline: Option<Duration>) -> Duration {
    let Some(deadline) = sender_deadline else {
        return max_hold;
    };
    let margin = (deadline / 10).max(MIN_MARGIN).min(deadline / 2);
    max_hold.min(deadline.saturating_sub(margin))
}

/// How a registry is set up.
pub(crate) struct HoldSettings {
    /// Whether the transport holds responses at all once armed.
    pub(crate) enabled: bool,
    /// Armed from the start, so the first request that can arrive is held.
    pub(crate) armed: bool,
    /// Held bytes past which a request is refused.
    pub(crate) max_held_bytes: u64,
    /// The longest a response is held.
    pub(crate) max_hold: Duration,
    /// The `transport` label on the registry's metrics.
    pub(crate) label: &'static str,
    /// Leased the held bytes from admission to answer.
    #[cfg(feature = "memory")]
    pub(crate) guard: Option<Arc<crate::memory::MemoryGuard>>,
}

/// One held request: the records it still waits on and the sender to answer.
struct Entry {
    len: u64,
    remaining: u64,
    /// One bit per record, set once it is released, so a second release of
    /// the same token counts once.
    released: Box<[u64]>,
    worst: DeliveryStatus,
    /// `None` once the request was answered by its own handler.
    responder: Option<oneshot::Sender<Outcome>>,
    admitted: Instant,
    deadline: Instant,
    bytes: u64,
}

impl Entry {
    /// Whether `index` is one of this entry's records.
    fn covers(&self, index: u64) -> bool {
        index < self.len
    }

    /// Mark record `index` released with `status`: `None` when it already
    /// was, else whether it was the last one outstanding.
    fn mark(&mut self, index: u64, status: DeliveryStatus) -> Option<bool> {
        let (Ok(word), bit) = (usize::try_from(index / 64), 1_u64 << (index % 64)) else {
            return None;
        };
        let slot = self.released.get_mut(word)?;
        if *slot & bit != 0 {
            return None;
        }
        *slot |= bit;
        self.remaining -= 1;
        self.worst = self.worst.max(status);
        Some(self.remaining == 0)
    }
}

/// Held push responses, keyed by the first sequence number of each request.
///
/// Unwind-safe by construction, which `VectorCompatService` asserts: every
/// change to the map, the counters, the held bytes and the senders completes
/// before a metrics call, the only call here that can panic, and the lock does
/// not poison.
pub(crate) struct PendingRegistry {
    entries: parking_lot::Mutex<BTreeMap<u64, Entry>>,
    /// Entries held, read without the lock so a release with nothing held
    /// returns at once.
    count: AtomicUsize,
    /// Records not yet released across every entry, changed under the lock.
    records: AtomicU64,
    /// Bytes held across every entry, shared with the pressure source.
    held_bytes: Arc<AtomicU64>,
    enabled: AtomicBool,
    armed: AtomicBool,
    max_held_bytes: u64,
    max_hold: Duration,
    label: &'static str,
    #[cfg(feature = "memory")]
    guard: Option<Arc<crate::memory::MemoryGuard>>,
}

impl PendingRegistry {
    pub(crate) fn new(settings: HoldSettings) -> Self {
        Self {
            entries: parking_lot::Mutex::new(BTreeMap::new()),
            count: AtomicUsize::new(0),
            records: AtomicU64::new(0),
            held_bytes: Arc::new(AtomicU64::new(0)),
            enabled: AtomicBool::new(settings.enabled),
            armed: AtomicBool::new(settings.armed),
            max_held_bytes: settings.max_held_bytes,
            max_hold: settings.max_hold,
            label: settings.label,
            #[cfg(feature = "memory")]
            guard: settings.guard,
        }
    }

    /// Hold responses once armed, or answer every request at enqueue.
    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }

    /// Whether a request arriving now is held until release.
    pub(crate) fn holding(&self) -> bool {
        self.enabled.load(Ordering::Acquire) && self.armed.load(Ordering::Acquire)
    }

    /// The held-byte counter, for a pressure source reading it.
    #[cfg(feature = "governor")]
    pub(crate) fn held_bytes(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.held_bytes)
    }

    /// Held bytes past which a request is refused.
    #[cfg(feature = "governor")]
    pub(crate) fn max_held_bytes(&self) -> u64 {
        self.max_held_bytes
    }

    /// The hold budget for a request whose sender set `sender_deadline`.
    pub(crate) fn budget(&self, sender_deadline: Option<Duration>) -> Duration {
        hold_budget(self.max_hold, sender_deadline)
    }

    /// Reserve `bytes` against the ceiling, or `None` when it has no room.
    ///
    /// Always admits while nothing is held, so a request over the ceiling on
    /// its own is not refused forever.
    pub(crate) fn reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        let ceiling = self.max_held_bytes;
        self.held_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held == 0 || held.saturating_add(bytes) <= ceiling)
                    .then(|| held.saturating_add(bytes))
            })
            .ok()?;
        #[cfg(feature = "memory")]
        if let Some(guard) = &self.guard {
            guard.add_bytes(bytes);
        }
        // Built before the gauges, so a panic there still returns the bytes.
        let reservation = Reservation {
            registry: Arc::clone(self),
            bytes,
        };
        self.publish();
        Some(reservation)
    }

    /// Release records by sequence number with `status`, answering every
    /// request whose last record this was. Idempotent per record.
    pub(crate) fn release<I>(&self, seqs: I, status: DeliveryStatus)
    where
        I: IntoIterator<Item = u64>,
    {
        if self.count.load(Ordering::Acquire) == 0 {
            return;
        }
        let finished = {
            let mut entries = self.entries.lock();
            let mut completed = Vec::new();
            let mut marked = 0_u64;
            for seq in seqs {
                let Some((&base, entry)) = entries.range_mut(..=seq).next_back() else {
                    continue;
                };
                let index = seq - base;
                if !entry.covers(index) {
                    continue;
                }
                if let Some(last) = entry.mark(index, status) {
                    marked += 1;
                    if last {
                        completed.push(base);
                    }
                }
            }
            let finished: Vec<Entry> = completed
                .into_iter()
                .filter_map(|base| entries.remove(&base))
                .collect();
            self.records.fetch_sub(marked, Ordering::Relaxed);
            self.count.store(entries.len(), Ordering::Release);
            finished
        };
        if !finished.is_empty() {
            self.settle(finished, |entry| Outcome::Released(entry.worst));
        }
    }

    /// The earliest answer deadline among the requests `seqs` belong to.
    pub(crate) fn deadline<I>(&self, seqs: I) -> Option<std::time::Instant>
    where
        I: IntoIterator<Item = u64>,
    {
        if self.count.load(Ordering::Acquire) == 0 {
            return None;
        }
        let entries = self.entries.lock();
        seqs.into_iter()
            .filter_map(|seq| {
                let (&base, entry) = entries.range(..=seq).next_back()?;
                entry.covers(seq - base).then_some(entry.deadline)
            })
            .min()
            .map(Instant::into_std)
    }

    /// What is held now: records not yet released, and the bytes, age and
    /// earliest answer deadline of the requests carrying them.
    pub(crate) fn snapshot(&self) -> HeldAcks {
        let entries = self.entries.lock();
        let now = Instant::now();
        HeldAcks::new(
            entries.values().map(|e| e.remaining).sum(),
            self.held_bytes.load(Ordering::Acquire),
            entries
                .values()
                .map(|e| e.admitted)
                .min()
                .map(|t| now.saturating_duration_since(t)),
            entries
                .values()
                .map(|e| e.deadline)
                .min()
                .map(Instant::into_std),
        )
    }

    /// Answer every held request `Shutdown` and free what it held.
    pub(crate) fn shutdown(&self) {
        let drained = {
            let mut entries = self.entries.lock();
            self.count.store(0, Ordering::Release);
            self.records.store(0, Ordering::Relaxed);
            std::mem::take(&mut *entries)
        };
        if !drained.is_empty() {
            self.settle(drained.into_values().collect(), |_| Outcome::Shutdown);
        }
    }

    /// Free the bytes of entries already out of the map, answer every sender
    /// still waiting, and only then record metrics.
    ///
    /// A metrics recorder is the one call here that can panic, so a panic
    /// caught around it finds the ceiling, the memory guard and every sender
    /// already settled.
    fn settle(&self, entries: Vec<Entry>, outcome: impl Fn(&Entry) -> Outcome) {
        self.return_bytes(entries.iter().map(|e| e.bytes).sum());
        let answered: Vec<(&'static str, Instant)> = entries
            .into_iter()
            .filter_map(|mut entry| {
                let answer = outcome(&entry);
                let responder = entry.responder.take()?;
                let label = if responder.send(answer).is_ok() {
                    outcome_label(answer)
                } else {
                    "orphaned"
                };
                Some((label, entry.admitted))
            })
            .collect();
        self.publish();
        for (label, admitted) in answered {
            self.count_answer(label, admitted);
        }
    }

    /// Take the responder of the request at `base` so its handler answers it
    /// as expired, or false when release answered it first.
    fn expire(&self, base: u64) -> bool {
        let admitted = {
            let mut entries = self.entries.lock();
            entries
                .get_mut(&base)
                .and_then(|entry| entry.responder.take().map(|_| entry.admitted))
        };
        match admitted {
            Some(admitted) => {
                self.count_answer("expired", admitted);
                true
            }
            None => false,
        }
    }

    /// Records `from..` of the request at `base` never reached the queue: they
    /// count as released `Errored`, and the handler answers the request itself.
    fn refuse_from(&self, base: u64, from: u64) {
        let done = {
            let mut entries = self.entries.lock();
            let Some(entry) = entries.get_mut(&base) else {
                return;
            };
            entry.responder = None;
            let marked = (from..entry.len)
                .filter(|&index| entry.mark(index, DeliveryStatus::Errored).is_some())
                .count();
            self.records.fetch_sub(marked as u64, Ordering::Relaxed);
            let done = if entry.remaining == 0 {
                entries.remove(&base)
            } else {
                None
            };
            self.count.store(entries.len(), Ordering::Release);
            done
        };
        if let Some(entry) = done {
            self.return_bytes(entry.bytes);
        }
        self.publish();
    }

    /// Return `bytes` to the ceiling and the memory guard: atomics only, so it
    /// cannot panic.
    fn return_bytes(&self, bytes: u64) {
        let _ = self
            .held_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                Some(held.saturating_sub(bytes))
            });
        #[cfg(feature = "memory")]
        if let Some(guard) = &self.guard {
            guard.release(bytes);
        }
    }

    /// Record the held gauges, never while unwinding: a recorder that panics
    /// then would abort the process.
    fn publish(&self) {
        #[cfg(feature = "metrics")]
        if !std::thread::panicking() {
            metrics::gauge!("transport_ack_held", "transport" => self.label)
                .set(self.records.load(Ordering::Relaxed) as f64);
            metrics::gauge!("transport_ack_held_bytes", "transport" => self.label)
                .set(self.held_bytes.load(Ordering::Acquire) as f64);
        }
    }

    /// Count one answered or abandoned request, never while unwinding.
    fn count_answer(&self, outcome: &'static str, admitted: Instant) {
        #[cfg(feature = "metrics")]
        if !std::thread::panicking() {
            metrics::counter!(
                "transport_ack_released_total",
                "transport" => self.label,
                "outcome" => outcome
            )
            .increment(1);
            metrics::histogram!(
                "transport_ack_latency_seconds",
                "transport" => self.label,
                "outcome" => outcome
            )
            .record(admitted.elapsed().as_secs_f64());
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (outcome, admitted, self.label);
    }

    /// Count a request refused before it was held.
    pub(crate) fn count_refused(&self, reason: &'static str) {
        #[cfg(feature = "metrics")]
        metrics::counter!(
            "transport_ack_refused_total",
            "transport" => self.label,
            "reason" => reason
        )
        .increment(1);
        #[cfg(not(feature = "metrics"))]
        let _ = reason;
    }
}

impl AckControl for PendingRegistry {
    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
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
        self.snapshot()
    }
}

/// The `outcome` label for an answered request.
fn outcome_label(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Released(DeliveryStatus::Delivered) => "delivered",
        Outcome::Released(DeliveryStatus::Dropped) => "dropped",
        Outcome::Released(DeliveryStatus::Rejected) => "rejected",
        Outcome::Released(DeliveryStatus::Errored) => "errored",
        Outcome::Expired => "expired",
        Outcome::Shutdown => "shutdown",
    }
}

/// Bytes reserved against the ceiling, returned on drop unless a request
/// takes them.
pub(crate) struct Reservation {
    registry: Arc<PendingRegistry>,
    bytes: u64,
}

impl Reservation {
    /// Hold the request whose `len` records carry sequence numbers from
    /// `base`, answering it within `budget`.
    ///
    /// `floor` is the status the request starts at: `Dropped` when some of its
    /// input was skipped rather than queued.
    pub(crate) fn hold(
        mut self,
        base: u64,
        len: u64,
        budget: Duration,
        floor: DeliveryStatus,
    ) -> Held {
        let (responder, answer) = oneshot::channel();
        let admitted = Instant::now();
        // A budget too large to add is a year, not a panic per request.
        let deadline = admitted
            .checked_add(budget)
            .unwrap_or_else(|| admitted + Duration::from_secs(365 * 24 * 3_600));
        let words = usize::try_from(len.div_ceil(64)).unwrap_or(usize::MAX);
        let entry = Entry {
            len,
            remaining: len,
            released: vec![0; words].into_boxed_slice(),
            worst: floor,
            responder: Some(responder),
            admitted,
            deadline,
            bytes: std::mem::take(&mut self.bytes),
        };
        let registry = Arc::clone(&self.registry);
        {
            let mut entries = registry.entries.lock();
            entries.insert(base, entry);
            registry.records.fetch_add(len, Ordering::Relaxed);
            registry.count.store(entries.len(), Ordering::Release);
        }
        // Built before the gauges, so a panic there still removes the entry.
        let held = Held {
            registry,
            base,
            deadline,
            answer,
            progress: 0,
            queued: false,
        };
        held.registry.publish();
        held
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.registry.return_bytes(self.bytes);
            self.registry.publish();
        }
    }
}

/// A request held in the registry, from before its records are queued until
/// it is answered.
///
/// Dropped before [`queued`](Self::queued), it releases the records that
/// never reached the queue as `Errored`, since nothing else will.
pub(crate) struct Held {
    registry: Arc<PendingRegistry>,
    base: u64,
    deadline: Instant,
    answer: oneshot::Receiver<Outcome>,
    /// Records queued so far, when they are queued one at a time.
    progress: u64,
    queued: bool,
}

impl Held {
    /// Every record is in the receive queue.
    pub(crate) fn queued(&mut self) {
        self.queued = true;
    }

    /// One more record, in sequence order, is in the receive queue.
    #[cfg(feature = "transport-grpc-vector-compat")]
    pub(crate) fn advance(&mut self) {
        self.progress += 1;
    }

    /// Records from `index` on never reached the queue, and the caller answers
    /// the request itself.
    #[cfg(any(test, feature = "transport-grpc-vector-compat"))]
    pub(crate) fn refuse_from(&mut self, index: u64) {
        self.registry.refuse_from(self.base, index);
        self.queued = true;
    }

    /// Wait for the request's outcome, at most until its hold budget runs out.
    pub(crate) async fn outcome(mut self) -> Outcome {
        match tokio::time::timeout_at(self.deadline, &mut self.answer).await {
            Ok(Ok(outcome)) => outcome,
            // The registry drops a responder unanswered only when it goes away.
            Ok(Err(_)) => Outcome::Shutdown,
            Err(_) if self.registry.expire(self.base) => Outcome::Expired,
            // Release answered between the budget running out and the take.
            Err(_) => self.answer.try_recv().unwrap_or(Outcome::Shutdown),
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if !self.queued {
            self.registry.refuse_from(self.base, self.progress);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(max_held_bytes: u64) -> Arc<PendingRegistry> {
        Arc::new(PendingRegistry::new(HoldSettings {
            enabled: true,
            armed: true,
            max_held_bytes,
            max_hold: DEFAULT_MAX_HOLD,
            label: "test",
            #[cfg(feature = "memory")]
            guard: None,
        }))
    }

    fn held(registry: &Arc<PendingRegistry>, base: u64, len: u64, bytes: u64) -> Held {
        let mut held = registry.reserve(bytes).expect("room").hold(
            base,
            len,
            Duration::from_secs(5),
            DeliveryStatus::Delivered,
        );
        held.queued();
        held
    }

    #[test]
    fn the_budget_keeps_a_margin_below_the_sender_deadline() {
        let max = DEFAULT_MAX_HOLD;
        assert_eq!(
            hold_budget(max, None),
            max,
            "no deadline: the whole maximum"
        );
        assert_eq!(
            hold_budget(max, Some(Duration::from_secs(30))),
            Duration::from_secs(25),
            "a 30 s deadline keeps 3 s, and the 25 s maximum is shorter"
        );
        assert_eq!(
            hold_budget(max, Some(Duration::from_secs(2))),
            Duration::from_secs(1),
            "a 2 s deadline keeps the 1 s floor"
        );
        assert_eq!(
            hold_budget(max, Some(Duration::from_millis(500))),
            Duration::from_millis(250),
            "a deadline under 2 s keeps half, never all of it"
        );
        assert_eq!(hold_budget(max, Some(Duration::ZERO)), Duration::ZERO);
    }

    #[tokio::test]
    async fn the_last_record_released_answers_with_the_worst_status() {
        let registry = registry(1024);
        let held = held(&registry, 10, 3, 30);

        registry.release([10, 11], DeliveryStatus::Delivered);
        registry.release([12], DeliveryStatus::Rejected);

        assert_eq!(
            held.outcome().await,
            Outcome::Released(DeliveryStatus::Rejected)
        );
        assert_eq!(registry.snapshot().count, 0);
        assert_eq!(registry.snapshot().bytes, 0, "the bytes go back on answer");
    }

    #[tokio::test]
    async fn a_record_released_twice_counts_once() {
        let registry = registry(1024);
        let held = held(&registry, 0, 2, 10);

        registry.release([0, 0, 0], DeliveryStatus::Delivered);
        assert_eq!(registry.snapshot().count, 1, "one record is still out");
        assert_eq!(
            registry.records.load(Ordering::Relaxed),
            1,
            "the gauge's count agrees"
        );
        registry.release([1], DeliveryStatus::Delivered);

        assert_eq!(
            held.outcome().await,
            Outcome::Released(DeliveryStatus::Delivered)
        );
    }

    #[tokio::test]
    async fn tokens_of_other_requests_and_unknown_tokens_are_ignored() {
        let registry = registry(1024);
        let first = held(&registry, 0, 2, 10);
        let second = held(&registry, 2, 2, 10);

        registry.release([1, 2, 99], DeliveryStatus::Errored);
        assert_eq!(registry.snapshot().count, 2);
        registry.release([0, 3], DeliveryStatus::Delivered);

        assert_eq!(
            first.outcome().await,
            Outcome::Released(DeliveryStatus::Errored)
        );
        assert_eq!(
            second.outcome().await,
            Outcome::Released(DeliveryStatus::Errored)
        );
    }

    /// With acknowledgements off, the engine releases at receipt tokens this
    /// registry never held: below, between and past what it holds, or with
    /// nothing held at all.
    #[tokio::test]
    async fn releasing_seqs_never_held_is_a_no_op() {
        let registry = registry(1024);
        registry.release([0, 1, 2], DeliveryStatus::Errored);
        let later = held(&registry, 20, 1, 5);
        let first = held(&registry, 10, 2, 10);

        registry.release([0, 9, 12, 15, 21, u64::MAX], DeliveryStatus::Errored);
        let snapshot = registry.snapshot();
        assert_eq!((snapshot.count, snapshot.bytes), (3, 15), "{snapshot:?}");

        registry.release([10, 11, 20], DeliveryStatus::Delivered);
        for held in [first, later] {
            assert_eq!(
                held.outcome().await,
                Outcome::Released(DeliveryStatus::Delivered),
                "the stray Errored releases touched nothing"
            );
        }
    }

    #[test]
    fn the_ceiling_refuses_past_it_and_always_admits_when_nothing_is_held() {
        let registry = registry(100);
        let big = registry.reserve(500).expect("nothing held: admitted");
        assert!(registry.reserve(1).is_none(), "over the ceiling");
        drop(big);
        let a = registry.reserve(60).expect("room");
        assert!(registry.reserve(41).is_none(), "60 + 41 > 100");
        let b = registry.reserve(40).expect("60 + 40 fits");
        assert_eq!(registry.snapshot().bytes, 100);
        drop((a, b));
        assert_eq!(registry.snapshot().bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreleased_request_expires_at_its_budget_and_frees_on_release() {
        let registry = registry(1024);
        let mut held = registry.reserve(8).expect("room").hold(
            0,
            1,
            Duration::from_secs(1),
            DeliveryStatus::Delivered,
        );
        held.queued();

        assert_eq!(held.outcome().await, Outcome::Expired);
        assert_eq!(
            registry.snapshot().bytes,
            8,
            "the records are still held after the answer"
        );
        registry.release([0], DeliveryStatus::Delivered);
        assert_eq!(registry.snapshot().bytes, 0);
        assert_eq!(registry.snapshot().count, 0);
    }

    #[test]
    fn a_request_dropped_before_queueing_is_removed() {
        let registry = registry(1024);
        let held = registry.reserve(8).expect("room").hold(
            0,
            4,
            Duration::from_secs(1),
            DeliveryStatus::Delivered,
        );
        assert_eq!(registry.snapshot().count, 4, "four records held");
        drop(held);
        assert_eq!(registry.snapshot().count, 0);
        assert_eq!(registry.snapshot().bytes, 0);
    }

    #[tokio::test]
    async fn records_never_queued_count_errored_and_the_rest_still_free_it() {
        let registry = registry(1024);
        let mut held = registry.reserve(8).expect("room").hold(
            0,
            4,
            Duration::from_secs(5),
            DeliveryStatus::Delivered,
        );
        held.refuse_from(2);
        drop(held);
        assert_eq!(registry.snapshot().count, 2, "records 0 and 1 are queued");
        registry.release([0, 1], DeliveryStatus::Delivered);
        assert_eq!(registry.snapshot().count, 0);
        assert_eq!(registry.snapshot().bytes, 0);
    }

    #[tokio::test]
    async fn shutdown_answers_every_held_request() {
        let registry = registry(1024);
        let a = held(&registry, 0, 1, 8);
        let b = held(&registry, 1, 1, 8);
        registry.shutdown();
        assert_eq!(a.outcome().await, Outcome::Shutdown);
        assert_eq!(b.outcome().await, Outcome::Shutdown);
        assert_eq!(registry.snapshot().bytes, 0);
        registry.release([0, 1], DeliveryStatus::Delivered);
    }

    #[tokio::test]
    async fn the_deadline_is_the_earliest_of_the_requests_named() {
        let registry = registry(1024);
        let _late = registry.reserve(1).expect("room").hold(
            0,
            2,
            Duration::from_secs(20),
            DeliveryStatus::Delivered,
        );
        let early = registry.reserve(1).expect("room").hold(
            2,
            2,
            Duration::from_secs(2),
            DeliveryStatus::Delivered,
        );
        let only_late = registry.deadline([0, 1]).expect("held");
        let both = registry.deadline([1, 3]).expect("held");
        assert!(both < only_late);
        assert_eq!(registry.deadline([9]), None);
        drop(early);
    }

    /// A recorder whose counters panic, as a caller's recorder might.
    #[cfg(feature = "metrics")]
    struct PanickingCounters;

    #[cfg(feature = "metrics")]
    impl metrics::Recorder for PanickingCounters {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            panic!("recorder refuses counters");
        }
        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    /// A panic caught around a release finds every sender answered and every
    /// byte returned: the state the registry relies on is settled before the
    /// one call that can panic, the metrics recorder.
    #[cfg(feature = "metrics")]
    #[tokio::test(flavor = "current_thread")]
    async fn a_panic_caught_around_a_release_leaves_the_registry_settled() {
        let registry = registry(1024);
        let first = held(&registry, 0, 1, 10);
        let second = held(&registry, 1, 1, 20);

        let caught = {
            let _local = metrics::set_default_local_recorder(&PanickingCounters);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.release([0, 1], DeliveryStatus::Delivered);
            }))
        };
        assert!(caught.is_err(), "the recorder panicked");

        assert_eq!(
            first.outcome().await,
            Outcome::Released(DeliveryStatus::Delivered)
        );
        assert_eq!(
            second.outcome().await,
            Outcome::Released(DeliveryStatus::Delivered)
        );
        let held = registry.snapshot();
        assert_eq!((held.count, held.bytes), (0, 0));
        assert_eq!(registry.count.load(Ordering::Acquire), 0);
        assert_eq!(registry.records.load(Ordering::Relaxed), 0);
        assert!(registry.reserve(1024).is_some(), "the ceiling is whole");
    }

    #[test]
    fn a_huge_budget_holds_for_a_year_rather_than_panicking() {
        let registry = registry(1024);
        let _held =
            registry
                .reserve(1)
                .expect("room")
                .hold(0, 1, Duration::MAX, DeliveryStatus::Delivered);
        assert!(registry.deadline([0]).is_some());
    }

    fn settings(enabled: bool, armed: bool) -> HoldSettings {
        HoldSettings {
            enabled,
            armed,
            max_held_bytes: 1,
            max_hold: DEFAULT_MAX_HOLD,
            label: "test",
            #[cfg(feature = "memory")]
            guard: None,
        }
    }

    #[test]
    fn a_disabled_or_unarmed_registry_holds_nothing() {
        let unarmed = PendingRegistry::new(settings(true, false));
        assert!(!unarmed.holding());
        assert!(!unarmed.is_armed());
        let disabled = PendingRegistry::new(settings(false, true));
        disabled.arm();
        assert!(!disabled.holding());
    }

    /// Built armed, a registry holds from its first request, and a later
    /// `arm`, as the pipeline makes, changes nothing.
    #[test]
    fn a_registry_built_armed_holds_from_the_start_and_arm_is_idempotent() {
        let armed = PendingRegistry::new(settings(true, true));
        assert!(armed.holding());
        assert!(armed.is_armed());
        armed.arm();
        armed.arm();
        assert!(armed.holding());
    }
}
