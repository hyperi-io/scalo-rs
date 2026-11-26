// Project:   scalo
// File:      src/transport/traits.rs
// Purpose:   Transport trait definitions (sender + receiver split)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use super::ack::{AckControl, DeadLetterReason, SinkConfirmation};
use super::delivery::DeliveryGuarantee;
use super::error::{TransportError, TransportResult};
use super::filter::FilteredDlqEntry;
use super::finalizer::DeliveryStatus;
use super::types::{BlockOutcome, Message, SendResult};
use super::work_batch::{Record, WorkBatch};
use std::fmt::{Debug, Display};
use std::future::Future;

/// Transport-specific token for commit/acknowledgment.
///
/// Each transport provides its own token type capturing what it needs to
/// acknowledge message processing.
pub trait CommitToken: Clone + Send + Sync + Debug + Display + 'static {
    /// Get a string representation for logging/debugging.
    fn as_str(&self) -> String {
        format!("{self}")
    }
}

/// Filtered receive output a transport assembles before handing it back.
///
/// Carries passing messages AND any filter-routed DLQ entries in one struct, so
/// a caller cannot lose dead-letters by forgetting a separate drain step.
/// [`TransportReceiver::recv`] returns a [`WorkBatch`], which this converts into
/// via `From`: each message becomes a record, `filtered_tokens` join
/// `commit_tokens`, and `dlq_entries` carry across for the caller to route.
#[derive(Debug)]
pub struct RecvBatch<T: CommitToken> {
    /// Messages that passed all inbound filters (or had no filter match).
    pub messages: Vec<Message<T>>,
    /// Entries matched by `action: dlq` inbound filters. Caller routes to DLQ.
    pub dlq_entries: Vec<FilteredDlqEntry>,
    /// Commit tokens of messages removed by inbound `drop`/`dlq` filters.
    ///
    /// Handled records that produced no passing message. Carried into
    /// `WorkBatch.commit_tokens` so the block commit advances the source past
    /// them -- otherwise an all-filtered stretch stalls the Kafka offset.
    pub filtered_tokens: Vec<T>,
}

impl<T: CommitToken> RecvBatch<T> {
    /// An empty batch (no messages, no DLQ entries).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            messages: Vec::new(),
            dlq_entries: Vec::new(),
            filtered_tokens: Vec::new(),
        }
    }

    /// A batch of messages with no DLQ entries (e.g. filters disabled).
    #[must_use]
    pub fn from_messages(messages: Vec<Message<T>>) -> Self {
        Self {
            messages,
            dlq_entries: Vec::new(),
            filtered_tokens: Vec::new(),
        }
    }

    /// Whether the batch has no messages AND no filtered-only acks to commit.
    /// (DLQ entries may still be present alongside passing messages.)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.filtered_tokens.is_empty()
    }

    /// Number of passing messages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.messages.len()
    }
}

/// Lifecycle and introspection methods shared by senders and receivers.
pub trait TransportBase: Send + Sync {
    /// Shutdown the transport gracefully.
    ///
    /// A receiver stops taking new records and keeps the ones it already
    /// acknowledged for [`TransportReceiver::recv`]. Idempotent.
    fn close(&self) -> impl Future<Output = TransportResult<()>> + Send;

    /// Check if the transport is healthy and connected.
    fn is_healthy(&self) -> bool;

    /// Get transport name for logging/metrics.
    fn name(&self) -> &'static str;

    /// What this transport's [`SendResult::Ok`] proves about delivery.
    ///
    /// Required, with no default, so a new backend states the guarantee it
    /// keeps rather than inheriting one. The factory refuses to build a sender
    /// below its config's `delivery` key (see [`DeliveryRequirement`](super::DeliveryRequirement)).
    /// A receive-only transport states the guarantee of the senders it pairs with.
    fn delivery_guarantee(&self) -> DeliveryGuarantee;

    /// Active boot-time health probe.
    ///
    /// Run by the factory at startup (see [`boot_healthcheck`]) so a
    /// misconfigured or unreachable downstream fails fast instead of being
    /// discovered on the first `send`. The DEFAULT delegates to
    /// [`is_healthy`](Self::is_healthy) (passes unless the transport already
    /// knows it is unhealthy). A transport SHOULD override with a real probe
    /// (Kafka metadata fetch, HTTP GET, file-path writability, ...).
    ///
    /// Shape: a per-component async `Result<()>` probe, run in parallel at boot
    /// by the factory (see [`boot_healthcheck`]).
    fn healthcheck(&self) -> impl Future<Output = TransportResult<()>> + Send {
        async move {
            if self.is_healthy() {
                Ok(())
            } else {
                Err(TransportError::Connection(format!(
                    "{} transport failed boot healthcheck (not healthy)",
                    self.name()
                )))
            }
        }
    }
}

/// Boot-time healthcheck policy. Fail-fast by default (`enabled = true`):
/// startup aborts if a transport's [`healthcheck`](TransportBase::healthcheck)
/// errors or exceeds `timeout`. Set `enabled = false` to skip probing.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct HealthcheckConfig {
    /// Run the boot probe and fail-fast on failure. Default `true`.
    #[serde(default = "default_healthcheck_enabled")]
    pub enabled: bool,
    /// Per-transport probe timeout in milliseconds. Default 5000.
    #[serde(default = "default_healthcheck_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_healthcheck_enabled() -> bool {
    true
}

fn default_healthcheck_timeout_ms() -> u64 {
    5000
}

impl Default for HealthcheckConfig {
    fn default() -> Self {
        Self {
            enabled: default_healthcheck_enabled(),
            timeout_ms: default_healthcheck_timeout_ms(),
        }
    }
}

/// Run a transport's boot healthcheck under the given policy (fail-fast).
///
/// - `cfg.enabled == false` -> skip (always `Ok`).
/// - otherwise probe [`TransportBase::healthcheck`] under a `cfg.timeout_ms`
///   deadline. A probe error OR a timeout returns `Err` so the caller can abort
///   startup before accepting traffic.
///
/// # Errors
/// Returns the probe's error, or a `Connection` timeout error, when enabled.
pub async fn boot_healthcheck<T: TransportBase>(
    transport: &T,
    cfg: HealthcheckConfig,
) -> TransportResult<()> {
    if !cfg.enabled {
        return Ok(());
    }
    let probe = transport.healthcheck();
    match tokio::time::timeout(std::time::Duration::from_millis(cfg.timeout_ms), probe).await {
        Ok(result) => result,
        Err(_elapsed) => Err(TransportError::Connection(format!(
            "{} transport boot healthcheck timed out after {}ms",
            transport.name(),
            cfg.timeout_ms
        ))),
    }
}

/// Send-side transport.
///
/// The factory returns `AnySender` (enum dispatch) for runtime selection. With
/// the `metrics` feature, the Kafka, gRPC, HTTP, file and pipe backends emit
/// `transport_*` metrics to the installed recorder, with a `{namespace}_`
/// prefix only when a metrics namespace is configured.
pub trait TransportSender: TransportBase {
    /// Send raw bytes to a destination.
    ///
    /// The `destination` semantics depend on the transport:
    /// - Kafka: topic name (NOT a message/partition key -- scalo does not
    ///   model per-record partition keys at this layer)
    /// - gRPC: metadata routing key
    /// - HTTP: URL path suffix or ignored
    /// - File: filename suffix or ignored
    /// - Pipe: ignored (single stdout)
    /// - OTLP: ignored (one collector)
    fn send(
        &self,
        destination: &str,
        payload: bytes::Bytes,
    ) -> impl Future<Output = SendResult> + Send;

    /// Send one whole [`Record`]: key, payload and headers.
    ///
    /// The default calls [`send`](Self::send) with the record's `key` as the
    /// destination (empty when `None`) and its payload, so the headers are
    /// dropped. A transport that can carry headers overrides this, and the
    /// default [`send_batch`](Self::send_batch) then carries them too.
    fn send_record(&self, record: &Record) -> impl Future<Output = SendResult> + Send {
        async move {
            let destination = record.key.as_deref().unwrap_or("");
            self.send(destination, record.payload.clone()).await
        }
    }

    /// Send a whole block of [`Record`]s in one shot.
    ///
    /// The default sends each record individually via
    /// [`send_record`](Self::send_record), awaiting each before the next.
    /// Transports that can do better override this: gRPC sends the block as one
    /// `RouteBatch` RPC, OTLP as one export request, Kafka queues the whole
    /// block before awaiting any delivery report, and HTTP posts each run of
    /// records with the same key as one body. Commit tokens and inline-DLQ
    /// entries are NOT sent -- they are the SENDER's local concern. Fire the commit tokens
    /// locally after this returns [`SendResult::Ok`].
    ///
    /// ## At-least-once caveat -- per-record fallback can partially send
    ///
    /// Not atomic. If record `k` of `n` returns a transient non-`Ok`
    /// (`Backpressured`/`Fatal`), records `0..k` are already on the wire and
    /// this returns without unsending them. The caller retries the whole block,
    /// re-delivering the sent prefix (at-least-once -- duplicates, never loss).
    /// A native batch override (gRPC) sends the whole block as one RPC, avoiding
    /// the partial-send window.
    ///
    /// ## A dead-lettered record does NOT abort the batch
    ///
    /// A per-record `FilteredDlq` is not a send failure: the rest of the block
    /// is still sent. Stopping there would make the caller retry the whole
    /// block forever, since the same bytes are refused every time. `Drop`
    /// records never reach the wire. Every other non-`Ok` result
    /// (`Backpressured`, `RetryAfter`, `Reject`, `Fatal`) ends the block and is
    /// returned as it came.
    ///
    /// The result is then `FilteredDlq`, never `Ok`: every other record was
    /// sent, and at least one was dead-lettered and written nowhere. The answer
    /// names no record. A caller that holds the records takes out the ones
    /// [`dead_letter_reason`](Self::dead_letter_reason) names before the call,
    /// and on a `FilteredDlq` sends the block again one record at a time to
    /// find the one refused, or holds the block. It never releases it as
    /// delivered.
    fn send_batch(&self, records: &[Record]) -> impl Future<Output = SendResult> + Send {
        async move {
            let mut outcome = BlockOutcome::default();
            for record in records {
                let result = self.send_record(record).await;
                tracing::trace!(
                    transport = self.name(),
                    bytes = record.payload.len(),
                    result = ?result,
                    "record send"
                );
                if let Some(ended) = outcome.add(result) {
                    return ended;
                }
            }
            outcome.finish()
        }
    }

    /// Send a block, answering by `deadline` whatever the destination does.
    ///
    /// The default runs [`send_batch`](Self::send_batch) under
    /// `tokio::time::timeout_at`, which polls the send once before it looks at
    /// the clock, so a deadline already past still takes a send that answers at
    /// once. An elapsed deadline is
    /// [`SendResult::Backpressured`]: the records may or may not have reached
    /// the destination, so the caller keeps them and sends them again
    /// (at-least-once, duplicates never loss). A transport with its own
    /// request timeout may override this to hand the deadline to its client,
    /// and must still answer by it.
    fn send_batch_by(
        &self,
        records: &[Record],
        deadline: std::time::Instant,
    ) -> impl Future<Output = SendResult> + Send {
        async move {
            let now = std::time::Instant::now();
            let until = tokio::time::Instant::from_std(deadline);
            match tokio::time::timeout_at(until, self.send_batch(records)).await {
                Ok(result) => {
                    tracing::debug!(
                        transport = self.name(),
                        records = records.len(),
                        result = ?result,
                        "batch sent by deadline"
                    );
                    result
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        transport = self.name(),
                        records = records.len(),
                        budget_ms =
                            u64::try_from(deadline.saturating_duration_since(now).as_millis())
                                .unwrap_or(u64::MAX),
                        "send deadline elapsed; the records may have arrived and are sent again"
                    );
                    SendResult::Backpressured
                }
            }
        }
    }

    /// What this sender's `Ok` proves about delivery.
    ///
    /// Drives the `pipeline_delivery_guarantee` metric only: a sink that
    /// cannot confirm still counts its `Ok` as delivered. The default is
    /// [`SinkConfirmation::None`]; Kafka and gRPC confirm
    /// [`Remote`](SinkConfirmation::Remote).
    fn confirms_delivery(&self) -> SinkConfirmation {
        SinkConfirmation::None
    }

    /// Why this sender would dead-letter `record` instead of sending it, or
    /// `None` when it would send it or drop it by policy.
    ///
    /// A sender answers such a record `FilteredDlq` without writing it
    /// anywhere, so a caller that holds a source acknowledgement screens the
    /// block first and routes these to its DLQ. Every in-tree sender names an
    /// outbound `dlq` filter match, and Kafka and gRPC a record over their
    /// size ceiling. The default names none.
    fn dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        let _ = record;
        None
    }
}

/// Limits for a single byte-aware [`TransportReceiver::recv_limited`] poll.
///
/// A bare [`recv`](TransportReceiver::recv) takes only a RECORD cap, so one poll
/// can build a [`WorkBatch`] arbitrarily larger than any memory budget.
/// `RecvLimits` adds the BYTE bound: the governed driver passes its
/// self-regulation byte budget here so a single recv never retains more than
/// `max_bytes` (plus the one-oversized-record floor) before the sub-block split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvLimits {
    /// Hard cap on records per poll (`>= 1`). Bounds a tiny-record flood that
    /// stays within the byte budget.
    pub max_records: usize,
    /// Soft cap on the SUM of `payload.len()` per poll. A transport that has
    /// accumulated at least one record stops draining once payload bytes reach
    /// this value (so it retains at most `max_bytes + one oversized record`).
    /// The default impl has no byte-aware drain and bounds by `max_records`
    /// only.
    pub max_bytes: u64,
}

/// Receive-side transport -- generic over commit token type.
///
/// Input stages (receiver, fetcher) use concrete implementations directly for
/// type-safe token handling.
pub trait TransportReceiver: TransportBase {
    /// The token type for this transport.
    type Token: CommitToken;

    /// Receive up to `max` records as one [`WorkBatch`].
    ///
    /// Returns immediately with available records (may be fewer than `max`).
    /// Returns an empty batch if no records are available. The source acks for
    /// the whole block live on [`WorkBatch::commit_tokens`] -- they are decoupled
    /// from `records.len()` so a downstream fan-out cannot disturb them.
    ///
    /// **Filter behaviour:** if the transport has inbound filters configured,
    /// `recv()` removes records matching `action: drop` filters and carries
    /// records matching `action: dlq` filters in [`WorkBatch`]`.dlq_entries`
    /// alongside the passing [`WorkBatch`]`.records`. Route the DLQ entries via
    /// your own DLQ handle -- they cannot be silently lost.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let batch = transport.recv(100).await?;
    /// for entry in batch.dlq_entries {
    ///     dlq.send(DlqEntry::new("filter", entry.reason, entry.payload)).await?;
    /// }
    /// for record in batch.records { /* process */ }
    /// ```
    ///
    /// # Cancel-safety (REQUIRED of implementors)
    ///
    /// The governed run loop polls `recv()` inside a `tokio::select!` and DROPS
    /// the future when shutdown or a ticker wins, so it must not leave records
    /// half-consumed at an `.await` -- either gather records synchronously (no
    /// `.await` between taking a record off the wire and returning it) or buffer
    /// internally. The in-tree Kafka (keeps an unfinished poll for the next
    /// call), memory (awaits only on an empty buffer), and pipe and file (keep
    /// a partial line and unreturned records for the next call) impls satisfy
    /// this. A custom impl that holds records across an `.await` will drop data
    /// on cancellation.
    ///
    /// # Yielding (REQUIRED of implementors)
    ///
    /// A `recv` with nothing to return waits by awaiting, never by blocking
    /// the thread. Callers loop on it, and a `recv` that never pends keeps its
    /// worker: timers stop and sockets on that runtime go unanswered.
    ///
    /// # After `close()` (REQUIRED of implementors)
    ///
    /// `recv` returns the records the source had already acknowledged to their
    /// senders, then [`TransportError::Closed`], without waiting for new ones.
    /// The `BatchEngine` run loops drain a source this way at shutdown, so a
    /// `recv` that reports `Closed` with acknowledged records still queued
    /// loses them, and one that keeps waiting holds shutdown 5 s past its last
    /// record. A source that re-delivers what was not committed (Kafka, file),
    /// or acknowledges nothing (pipe), may report `Closed` at once.
    fn recv(
        &self,
        max: usize,
    ) -> impl Future<Output = TransportResult<WorkBatch<Self::Token>>> + Send;

    /// Byte-aware receive: bound a single poll by BOTH a record cap and a
    /// payload-byte cap (see [`RecvLimits`]).
    ///
    /// The governed driver uses this so the self-regulation byte budget bounds
    /// RECEIVE memory, not just the post-recv sub-block lease: a poll retains at
    /// most `limits.max_bytes` of payload (plus one oversized record), so the
    /// inbound footprint is bounded BEFORE the sub-block split, never after.
    ///
    /// **Default impl:** falls back to [`recv`](Self::recv)`(limits.max_records)`
    /// -- record-bounded only, byte cap ignored. Only transports that buffer a
    /// whole poll's bytes in one allocation (Kafka's recv-arena) override it.
    /// Channel/stream transports (Memory, gRPC, ...) already retain only one
    /// record's bytes at a time, so the fallback is correct for them. The
    /// syslog receiver is the exception: each record is a slice of the block
    /// it was read into, up to 64 KiB on TCP and TLS and 1 MiB on UDP, and
    /// keeps that whole block alive, so its payload bytes understate what a
    /// poll holds. Its queue of `recv_buffer_size` records bounds it instead.
    ///
    /// Filter behaviour and the `commit_tokens` contract match
    /// [`recv`](Self::recv).
    fn recv_limited(
        &self,
        limits: RecvLimits,
    ) -> impl Future<Output = TransportResult<WorkBatch<Self::Token>>> + Send {
        self.recv(limits.max_records)
    }

    /// Commit/acknowledge processed messages.
    ///
    /// - Kafka: commits consumer offsets
    /// - gRPC: no-op (no persistence)
    /// - File: advances read position
    /// - Memory: advances internal sequence
    fn commit(&self, tokens: &[Self::Token]) -> impl Future<Output = TransportResult<()>> + Send;

    /// The source's acknowledgement controls, or `None` for a source that does
    /// not hold its acknowledgement: pipe and memory have none, and file does
    /// not hold its own yet.
    ///
    /// See [`super::ack`]. The default is `None`.
    fn ack_control(&self) -> Option<&dyn AckControl> {
        None
    }

    /// Release the source acknowledgement of `tokens` with the merged status of
    /// every piece built from them.
    ///
    /// `Delivered`, `Dropped` and `Rejected` release it. `Errored` withholds it
    /// so the records are delivered again. The default commits when
    /// [`DeliveryStatus::should_commit`] holds and does nothing otherwise. A
    /// push source answers its held senders here.
    ///
    /// # One poll from a drop (REQUIRED of implementors)
    ///
    /// A block abandoned by a panic or a dropped future is released `Errored`
    /// from a `Drop`, which cannot await, so that future is polled ONCE and
    /// then dropped. An `Errored` release must do its work before its first
    /// `.await`: the in-tree push sources answer their senders synchronously,
    /// and Kafka records the withheld offsets synchronously. A release that
    /// returns `Pending` on its first poll is not driven further from a drop:
    /// the block stays unreleased, logged at WARN, and a push sender waits out
    /// its hold budget.
    ///
    /// # Errors
    ///
    /// The commit's error.
    fn release(
        &self,
        tokens: &[Self::Token],
        outcome: DeliveryStatus,
    ) -> impl Future<Output = TransportResult<()>> + Send {
        async move {
            if outcome.should_commit() {
                self.commit(tokens).await
            } else {
                Ok(())
            }
        }
    }

    /// The earliest instant by which `tokens` must be released, or `None` when
    /// nothing waits on them (a pull source).
    ///
    /// A push source returns its held senders' deadline, so a caller abandons a
    /// block it cannot deliver in time and releases it `Errored`.
    fn hold_deadline(&self, tokens: &[Self::Token]) -> Option<std::time::Instant> {
        let _ = tokens;
        None
    }
}

/// Combined transport -- implements both send and receive.
///
/// Most concrete impls (Kafka, gRPC, Memory, File, Pipe) qualify;
/// auto-implemented via blanket impl.
pub trait Transport: TransportSender + TransportReceiver {}

/// Blanket impl: anything that implements both traits is a Transport.
impl<T: TransportSender + TransportReceiver> Transport for T {}

/// Load a config from the cascade under a key.
///
/// The shared body behind each transport config's `from_cascade()` and the
/// sink stack's. Implementors only name their key. Without the `config`
/// feature the default method returns `Default::default()`.
pub trait FromCascade: Default + serde::Serialize + serde::de::DeserializeOwned + 'static {
    /// Load `Self` from the config cascade under `key`, registering the section
    /// in the global registry. Defaults when the cascade is not set up or the
    /// key is absent.
    ///
    /// # Errors
    ///
    /// [`TransportError::Config`] naming `key` when the section is present
    /// but does not deserialise into `Self`.
    fn from_cascade_key(key: &str) -> TransportResult<Self> {
        crate::cascade::section(key).map_err(|e| TransportError::Config(e.to_string()))
    }
}

#[cfg(test)]
mod healthcheck_tests {
    use super::*;
    use std::time::Duration;

    /// Probe that does NOT override `healthcheck` -- exercises the default impl
    /// (delegates to `is_healthy`).
    struct DefaultProbe {
        healthy: bool,
    }
    impl TransportBase for DefaultProbe {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            self.healthy
        }
        fn name(&self) -> &'static str {
            "default-probe"
        }

        fn delivery_guarantee(&self) -> DeliveryGuarantee {
            DeliveryGuarantee::Acknowledged
        }
    }

    /// Probe that OVERRIDES `healthcheck` with a real (here: scripted) body,
    /// optionally hanging to exercise the boot timeout.
    struct ActiveProbe {
        ok: bool,
        hang: bool,
    }
    impl TransportBase for ActiveProbe {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "active-probe"
        }

        fn delivery_guarantee(&self) -> DeliveryGuarantee {
            DeliveryGuarantee::Acknowledged
        }
        async fn healthcheck(&self) -> TransportResult<()> {
            if self.hang {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            if self.ok {
                Ok(())
            } else {
                Err(TransportError::Connection("active probe rejected".into()))
            }
        }
    }

    #[tokio::test]
    async fn default_healthcheck_delegates_to_is_healthy() {
        assert!(
            boot_healthcheck(
                &DefaultProbe { healthy: true },
                HealthcheckConfig::default()
            )
            .await
            .is_ok()
        );
        assert!(
            boot_healthcheck(
                &DefaultProbe { healthy: false },
                HealthcheckConfig::default()
            )
            .await
            .is_err(),
            "default probe must fail-fast when not healthy"
        );
    }

    #[tokio::test]
    async fn disabled_skips_probe_even_if_unhealthy() {
        // Would hang/fail if probed, but disabled must short-circuit to Ok.
        let t = ActiveProbe {
            ok: false,
            hang: true,
        };
        let cfg = HealthcheckConfig {
            enabled: false,
            timeout_ms: 10,
        };
        assert!(
            boot_healthcheck(&t, cfg).await.is_ok(),
            "disabled must skip"
        );
    }

    #[tokio::test]
    async fn active_probe_failure_fails_fast() {
        let t = ActiveProbe {
            ok: false,
            hang: false,
        };
        assert!(
            boot_healthcheck(&t, HealthcheckConfig::default())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn hanging_probe_times_out() {
        let t = ActiveProbe {
            ok: true,
            hang: true,
        };
        let cfg = HealthcheckConfig {
            enabled: true,
            timeout_ms: 50,
        };
        assert!(
            boot_healthcheck(&t, cfg).await.is_err(),
            "a hanging probe must hit the boot timeout"
        );
    }
}

#[cfg(test)]
mod send_tests {
    use super::*;
    use crate::transport::{PayloadFormat, RecordMeta};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    /// Answers each record by its payload: `reject`, `wait`, `busy`, `dlq` or
    /// `ok`, and never answers `hang`. Counts every send.
    #[derive(Default)]
    struct Scripted {
        sends: AtomicUsize,
    }

    impl TransportBase for Scripted {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "scripted"
        }
        fn delivery_guarantee(&self) -> DeliveryGuarantee {
            DeliveryGuarantee::Acknowledged
        }
    }

    impl TransportSender for Scripted {
        async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
            self.sends.fetch_add(1, Ordering::SeqCst);
            match String::from_utf8_lossy(&payload).as_ref() {
                "reject" => SendResult::Reject(TransportError::Send("refused".into())),
                "wait" => SendResult::RetryAfter(Duration::from_secs(7)),
                "busy" => SendResult::Backpressured,
                "dlq" => SendResult::FilteredDlq,
                "hang" => std::future::pending().await,
                _ => SendResult::Ok,
            }
        }
    }

    fn block(payloads: &[&'static str]) -> Vec<Record> {
        payloads
            .iter()
            .map(|p| Record {
                payload: bytes::Bytes::from_static(p.as_bytes()),
                key: None,
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            })
            .collect()
    }

    #[tokio::test]
    async fn the_per_record_default_stops_at_a_reject_or_a_named_wait() {
        // (case, block, result as text, records sent)
        let cases = [
            ("all sent", vec!["ok", "ok"], "Ok", 2),
            ("dead letter goes on", vec!["dlq", "ok"], "FilteredDlq", 2),
            ("reject stops", vec!["ok", "reject", "ok"], "Reject", 2),
            ("wait stops", vec!["wait", "ok"], "RetryAfter(7s)", 1),
            (
                "busy stops",
                vec!["ok", "busy", "reject"],
                "Backpressured",
                2,
            ),
        ];
        for (case, payloads, want, sent) in cases {
            let sender = Scripted::default();
            let got = sender.send_batch(&block(&payloads)).await;
            assert!(format!("{got:?}").starts_with(want), "{case}: {got:?}");
            assert_eq!(sender.sends.load(Ordering::SeqCst), sent, "{case}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_answers_in_time_or_hands_the_records_back() {
        let sender = Scripted::default();
        let records = block(&["ok", "reject"]);

        let in_time = sender
            .send_batch_by(&records, Instant::now() + Duration::from_secs(5))
            .await;
        assert!(in_time.is_reject(), "the sender's own answer: {in_time:?}");

        let hung = Scripted::default();
        let elapsed = hung
            .send_batch_by(
                &block(&["ok", "hang"]),
                Instant::now() + Duration::from_millis(50),
            )
            .await;
        assert!(
            elapsed.is_backpressured(),
            "an elapsed deadline keeps the records for a resend: {elapsed:?}"
        );
        assert_eq!(hung.sends.load(Ordering::SeqCst), 2, "the send was started");

        // A deadline already past still gets the first poll, as a zero timeout does.
        let late = Scripted::default();
        let at_once = late.send_batch_by(&block(&["ok"]), Instant::now()).await;
        assert!(at_once.is_ok(), "{at_once:?}");
        let late_hang = late.send_batch_by(&block(&["hang"]), Instant::now()).await;
        assert!(late_hang.is_backpressured(), "{late_hang:?}");
    }
}

/// Every config read through `FromCascade` takes its defaults when its
/// section is absent, and refuses a present section that does not parse,
/// naming it, rather than replacing it with defaults.
#[cfg(all(
    test,
    feature = "config",
    feature = "transport-http",
    feature = "transport-file",
    feature = "transport-pipe",
    feature = "transport-grpc",
    feature = "sink-stack"
))]
mod from_cascade_tests {
    use super::FromCascade;
    use crate::transport::TransportError;

    /// `T` read from `present` (a section that does not parse) and from
    /// `absent`: whether the first was refused naming the key, and whether the
    /// second came back as the defaults.
    fn reads<T: FromCascade>(present: &str, absent: &str) -> (bool, bool) {
        let defaults = serde_json::to_value(T::default()).ok();
        let refused = matches!(
            T::from_cascade_key(present),
            Err(TransportError::Config(m)) if m.contains(present)
        );
        let defaulted = matches!(
            T::from_cascade_key(absent),
            Ok(v) if serde_json::to_value(&v).ok() == defaults
        );
        (refused, defaulted)
    }

    #[test]
    fn a_section_that_does_not_parse_is_refused_and_an_absent_one_defaults() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "bad:\n  http:\n    recv_buffer_size: lots\n  file:\n    append: maybe\n  pipe:\n    recv_timeout_ms: soon\n  grpc:\n    max_message_size: huge\n  acks:\n    enabled: perhaps\n  sink_stack:\n    attempt_timeout_ms: later\n",
        )
        .unwrap();
        crate::config::setup(crate::config::ConfigOptions {
            config_paths: vec![dir.path().to_path_buf()],
            ..crate::config::ConfigOptions::default()
        })
        .unwrap();

        let cases: [(&str, (bool, bool)); 6] = [
            (
                "http",
                reads::<crate::transport::http::HttpTransportConfig>("bad.http", "none.http"),
            ),
            (
                "file",
                reads::<crate::transport::file::FileTransportConfig>("bad.file", "none.file"),
            ),
            (
                "pipe",
                reads::<crate::transport::pipe::PipeTransportConfig>("bad.pipe", "none.pipe"),
            ),
            (
                "grpc",
                reads::<crate::transport::grpc::GrpcConfig>("bad.grpc", "none.grpc"),
            ),
            (
                "acknowledgements",
                reads::<crate::transport::AcknowledgementsConfig>("bad.acks", "none.acks"),
            ),
            (
                "sink_stack",
                reads::<crate::sink_stack::SinkStackConfig>("bad.sink_stack", "none.sink_stack"),
            ),
        ];
        for (name, got) in cases {
            assert_eq!(
                got,
                (true, true),
                "{name}: (refused bad section, defaulted absent one)"
            );
        }
    }
}
