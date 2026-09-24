// Project:   scalo
// File:      src/transport/traits.rs
// Purpose:   Transport trait definitions (sender + receiver split)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use super::error::{TransportError, TransportResult};
use super::filter::FilteredDlqEntry;
use super::types::{Message, SendResult};
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

/// Result of a [`TransportReceiver::recv`] call.
///
/// Carries passing messages AND any filter-routed DLQ entries in one struct, so
/// a caller cannot lose dead-letters by forgetting a separate drain step. The
/// caller routes `dlq_entries` onward via its own DLQ handle.
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
    /// them -- otherwise an all-filtered stretch stalls the Kafka offset /
    /// leaks the Redis PEL.
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
    fn close(&self) -> impl Future<Output = TransportResult<()>> + Send;

    /// Check if the transport is healthy and connected.
    fn is_healthy(&self) -> bool;

    /// Get transport name for logging/metrics.
    fn name(&self) -> &'static str;

    /// Active boot-time health probe.
    ///
    /// Run by the factory at startup (see [`boot_healthcheck`]) so a
    /// misconfigured or unreachable downstream fails fast instead of being
    /// discovered on the first `send`. The DEFAULT delegates to
    /// [`is_healthy`](Self::is_healthy) (passes unless the transport already
    /// knows it is unhealthy); a transport SHOULD override with a real probe
    /// (Kafka metadata fetch, HTTP GET, Redis PING, file-path writability, ...).
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
///   deadline; a probe error OR a timeout returns `Err` so the caller can abort
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
/// The factory returns `AnySender` (enum dispatch) for runtime selection. All
/// implementations auto-emit `dfe_transport_*` metrics when a `MetricsManager`
/// recorder is installed.
pub trait TransportSender: TransportBase {
    /// Send raw bytes to a destination.
    ///
    /// The `destination` semantics depend on the transport:
    /// - Kafka: topic name (NOT a message/partition key -- scalo does not
    ///   model per-record partition keys at this layer)
    /// - gRPC: metadata routing key
    /// - HTTP: URL path suffix or ignored
    /// - File: filename suffix or ignored
    /// - Redis: stream name
    /// - Pipe: ignored (single stdout)
    fn send(
        &self,
        destination: &str,
        payload: bytes::Bytes,
    ) -> impl Future<Output = SendResult> + Send;

    /// Send a whole block of [`Record`]s in one shot.
    ///
    /// The default sends each record individually via [`send`](Self::send),
    /// using the record's own `key` as the destination (empty when `None`) and
    /// payload (a refcount bump, not a copy), awaiting each before the next.
    /// Transports that can do better override this: gRPC sends the block as one
    /// `RouteBatch` RPC, and Kafka queues the whole block before awaiting any
    /// delivery report. Commit tokens and inline-DLQ entries are NOT
    /// sent -- they are the SENDER's local concern; fire the commit tokens
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
    /// ## Outbound-filter dispositions do NOT abort the batch
    ///
    /// A per-record `FilteredDlq` is the record being HANDLED, not a send
    /// failure: skip it and continue. Returning it would make the caller retry
    /// the whole block forever -- the deterministic filter re-matches the same
    /// record every time (a livelock that stalls the source). `Drop` records
    /// likewise never reach the wire. Only `Backpressured`/`Fatal`
    /// short-circuit.
    fn send_batch(&self, records: &[Record]) -> impl Future<Output = SendResult> + Send {
        async move {
            for record in records {
                let destination = record.key.as_deref().unwrap_or("");
                match self.send(destination, record.payload.clone()).await {
                    // Sent, dropped (Ok), or suppressed by an outbound dlq
                    // filter -- all handled; keep going, do NOT abort the block.
                    SendResult::Ok | SendResult::FilteredDlq => {}
                    // Transient/fatal transport failure: stop so the caller
                    // retries the unconfirmed remainder of the block.
                    other @ (SendResult::Backpressured | SendResult::Fatal(_)) => return other,
                }
            }
            SendResult::Ok
        }
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
    /// internally. The in-tree Kafka (synchronous poll) and memory (awaits only
    /// on an empty buffer) impls satisfy this; a custom impl that holds records
    /// across an `.await` will drop data on cancellation.
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
    /// record's bytes at a time, so the fallback is correct for them.
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
    /// - Redis: XACK
    /// - File: advances read position
    /// - Memory: advances internal sequence
    fn commit(&self, tokens: &[Self::Token]) -> impl Future<Output = TransportResult<()>> + Send;
}

/// Combined transport -- implements both send and receive.
///
/// Most concrete impls (Kafka, gRPC, Memory, Redis, File, Pipe) qualify;
/// auto-implemented via blanket impl.
pub trait Transport: TransportSender + TransportReceiver {}

/// Blanket impl: anything that implements both traits is a Transport.
impl<T: TransportSender + TransportReceiver> Transport for T {}

/// Load a transport config from the cascade under a fixed key.
///
/// Consolidates the byte-identical `from_cascade()` bodies each transport config
/// used to repeat. Implementors only name their key. Without the `config`
/// feature the default method returns `Default::default()`.
pub trait FromCascade: Default + serde::Serialize + serde::de::DeserializeOwned + 'static {
    /// Load `Self` from the config cascade under `key`, registering the section
    /// in the global registry; falls back to `Default` if the cascade is
    /// unavailable or the key is absent/invalid.
    #[must_use]
    fn from_cascade_key(key: &str) -> Self {
        #[cfg(feature = "config")]
        {
            if let Some(cfg) = crate::config::try_get()
                && let Ok(value) = cfg.unmarshal_key_registered::<Self>(key)
            {
                return value;
            }
        }
        // Without `config`, or on any cascade miss, use defaults.
        #[cfg(not(feature = "config"))]
        let _ = key;
        Self::default()
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
