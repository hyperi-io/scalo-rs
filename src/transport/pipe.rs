// Project:   scalo
// File:      src/transport/pipe.rs
// Purpose:   Unix pipe transport (stdin/stdout)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # Pipe Transport
//!
//! Reads from stdin and writes to stdout for Unix pipeline composition.
//! Newline-delimited: each line is one message.
//!
//! ## Example
//!
//! ```rust,ignore
//! use scalo::transport::{PipeTransport, PipeTransportConfig};
//!
//! let config = PipeTransportConfig::default();
//! let transport = PipeTransport::new(&config);
//!
//! // Send writes payload + newline to stdout
//! transport.send("ignored", bytes::Bytes::from_static(b"hello world")).await;
//!
//! // Recv reads lines from stdin
//! let records = transport.recv(10).await?.records;
//! ```

use super::error::{TransportError, TransportResult};
use super::traits::{CommitToken, RecvBatch, TransportBase, TransportReceiver, TransportSender};
use super::types::{Message, PayloadFormat, SendResult};
use super::work_batch::WorkBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};

/// Commit token for pipe transport.
///
/// Contains a monotonic sequence number. Commit is a no-op
/// because stdin is a forward-only stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipeToken {
    /// Message sequence number.
    pub seq: u64,
}

impl CommitToken for PipeToken {}

impl std::fmt::Display for PipeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipe:{}", self.seq)
    }
}

/// Configuration for pipe transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipeTransportConfig {
    /// Receive timeout in milliseconds (0 = block until data). Default: 100.
    #[serde(default = "default_recv_timeout_ms")]
    pub recv_timeout_ms: u64,

    /// Inbound message filters (applied on recv before caller sees messages).
    #[serde(default)]
    pub filters_in: Vec<super::filter::FilterRule>,

    /// Outbound message filters (applied on send before transport dispatches).
    #[serde(default)]
    pub filters_out: Vec<super::filter::FilterRule>,
}

fn default_recv_timeout_ms() -> u64 {
    100
}

impl Default for PipeTransportConfig {
    fn default() -> Self {
        Self {
            recv_timeout_ms: default_recv_timeout_ms(),
            filters_in: Vec::new(),
            filters_out: Vec::new(),
        }
    }
}

impl PipeTransportConfig {
    /// Load from the config cascade under the `transport.pipe` key.
    #[must_use]
    pub fn from_cascade() -> Self {
        <Self as super::traits::FromCascade>::from_cascade_key("transport.pipe")
    }
}

/// Read side, kept across `recv` calls so a dropped call loses nothing.
struct ReadState {
    reader: BufReader<Box<dyn AsyncRead + Send + Unpin>>,
    /// Bytes of a line still waiting for its newline.
    line: Vec<u8>,
    /// Records read but not yet returned.
    pending: Vec<Message<PipeToken>>,
}

/// Unix pipe transport (stdin/stdout).
///
/// Send writes newline-delimited payloads to stdout.
/// Receive reads lines from stdin, each becoming a message.
/// Commit is a no-op (stdin cannot be rewound).
pub struct PipeTransport {
    stdin: tokio::sync::Mutex<ReadState>,
    stdout: tokio::sync::Mutex<tokio::io::Stdout>,
    sequence: AtomicU64,
    closed: Arc<AtomicBool>,
    recv_timeout_ms: u64,
    /// `Err` holds the rule compile error, and send and recv refuse while it stands.
    filter_engine: Result<super::filter::TransportFilterEngine, String>,
}

impl PipeTransport {
    /// Create a new pipe transport.
    ///
    /// Filter rules compile against the `transport.filter_tiers` gates, as on
    /// every other backend. A rule that fails to compile leaves the transport
    /// unhealthy: `send` returns [`SendResult::Fatal`] and `recv` returns
    /// [`TransportError::Config`], both carrying the compile error, so a `drop`
    /// or `dlq` rule never silently stops applying. [`AnySender`](super::AnySender)
    /// and [`AnyReceiver`](super::AnyReceiver) refuse to build such a pipe.
    #[must_use]
    pub fn new(config: &PipeTransportConfig) -> Self {
        let filter_engine = Self::compile_filters(config).map_err(|e| match e {
            TransportError::Config(detail) => detail,
            other => other.to_string(),
        });
        if let Err(detail) = &filter_engine {
            tracing::error!(
                error = %detail,
                "Pipe transport filters failed to compile -- send and recv refuse until the rules are fixed"
            );
        }
        Self::with_filter_engine(config, filter_engine, Box::new(tokio::io::stdin()))
    }

    /// Create a pipe transport, failing when a filter rule does not compile.
    ///
    /// The factory path, so a bad rule fails construction as on every other
    /// backend.
    pub(crate) fn try_new(config: &PipeTransportConfig) -> TransportResult<Self> {
        let filter_engine = Self::compile_filters(config)?;
        Ok(Self::with_filter_engine(
            config,
            Ok(filter_engine),
            Box::new(tokio::io::stdin()),
        ))
    }

    /// A pipe transport reading `input` in place of stdin.
    #[cfg(all(test, unix))]
    fn with_input(
        config: &PipeTransportConfig,
        input: impl AsyncRead + Send + Unpin + 'static,
    ) -> TransportResult<Self> {
        let filter_engine = Self::compile_filters(config)?;
        Ok(Self::with_filter_engine(
            config,
            Ok(filter_engine),
            Box::new(input),
        ))
    }

    fn compile_filters(
        config: &PipeTransportConfig,
    ) -> TransportResult<super::filter::TransportFilterEngine> {
        super::filter::TransportFilterEngine::new(
            &config.filters_in,
            &config.filters_out,
            &crate::transport::filter::TransportFilterTierConfig::from_cascade(),
        )
    }

    fn with_filter_engine(
        config: &PipeTransportConfig,
        filter_engine: Result<super::filter::TransportFilterEngine, String>,
        input: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Self {
        #[cfg(feature = "logger")]
        tracing::info!(
            recv_timeout_ms = config.recv_timeout_ms,
            "Pipe transport opened"
        );

        let closed = Arc::new(AtomicBool::new(false));

        #[cfg(feature = "health")]
        {
            let h = Arc::clone(&closed);
            let filters_compiled = filter_engine.is_ok();
            crate::health::HealthRegistry::register("transport:pipe", move || {
                if filters_compiled && !h.load(Ordering::Relaxed) {
                    crate::health::HealthStatus::Healthy
                } else {
                    crate::health::HealthStatus::Unhealthy
                }
            });
        }

        Self {
            stdin: tokio::sync::Mutex::new(ReadState {
                reader: BufReader::new(input),
                line: Vec::new(),
                pending: Vec::new(),
            }),
            stdout: tokio::sync::Mutex::new(tokio::io::stdout()),
            sequence: AtomicU64::new(0),
            closed,
            recv_timeout_ms: config.recv_timeout_ms,
            filter_engine,
        }
    }

    /// The compiled filters, or the compile error to refuse traffic with.
    fn filters(&self) -> TransportResult<&super::filter::TransportFilterEngine> {
        self.filter_engine
            .as_ref()
            .map_err(|detail| TransportError::Config(detail.clone()))
    }
}

impl TransportBase for PipeTransport {
    async fn close(&self) -> TransportResult<()> {
        self.closed.store(true, Ordering::Relaxed);

        // Flush stdout before closing
        let mut stdout = self.stdout.lock().await;
        stdout
            .flush()
            .await
            .map_err(|e| TransportError::Internal(format!("stdout flush failed: {e}")))?;

        Ok(())
    }

    fn is_healthy(&self) -> bool {
        self.filter_engine.is_ok() && !self.closed.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "pipe"
    }
}

impl TransportSender for PipeTransport {
    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        if self.closed.load(Ordering::Relaxed) {
            return SendResult::Fatal(TransportError::Closed);
        }
        let filter_engine = match self.filters() {
            Ok(engine) => engine,
            Err(e) => return SendResult::Fatal(e),
        };

        // Outbound filter check
        if filter_engine.has_outbound_filters() {
            match filter_engine.apply_outbound(&payload) {
                super::filter::FilterDisposition::Pass => {}
                super::filter::FilterDisposition::Drop => return SendResult::Ok,
                super::filter::FilterDisposition::Dlq => return SendResult::FilteredDlq,
            }
        }

        let mut stdout = self.stdout.lock().await;

        // Write payload + newline
        if let Err(e) = stdout.write_all(&payload).await {
            return SendResult::Fatal(TransportError::Send(format!("stdout write failed: {e}")));
        }
        if let Err(e) = stdout.write_all(b"\n").await {
            return SendResult::Fatal(TransportError::Send(format!(
                "stdout newline write failed: {e}"
            )));
        }
        if let Err(e) = stdout.flush().await {
            return SendResult::Fatal(TransportError::Send(format!("stdout flush failed: {e}")));
        }

        #[cfg(feature = "logger")]
        tracing::debug!(
            bytes = payload.len(),
            "Pipe transport: message sent to stdout"
        );

        #[cfg(feature = "metrics")]
        {
            metrics::counter!("transport_sent_total", "transport" => "pipe").increment(1);
            metrics::counter!("transport_sent_bytes_total", "transport" => "pipe")
                .increment(payload.len() as u64);
        }

        SendResult::Ok
    }
}

impl TransportReceiver for PipeTransport {
    type Token = PipeToken;

    async fn recv(&self, max: usize) -> TransportResult<WorkBatch<Self::Token>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TransportError::Closed);
        }
        let filter_engine = self.filters()?;

        let mut stdin = self.stdin.lock().await;
        // `read_until` leaves a partial line in `line` when dropped, and
        // `pending` holds finished records, so a dropped call resumes here.
        let ReadState {
            reader,
            line,
            pending,
        } = &mut *stdin;

        for _ in pending.len()..max {
            let read_result = if self.recv_timeout_ms == 0 {
                // Block until data arrives
                reader.read_until(b'\n', line).await
            } else if pending.is_empty() {
                // First message: wait up to timeout
                match tokio::time::timeout(
                    std::time::Duration::from_millis(self.recv_timeout_ms),
                    reader.read_until(b'\n', line),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => break, // Timeout, return what we have (empty)
                }
            } else {
                // Subsequent messages: non-blocking attempt via short timeout
                match tokio::time::timeout(
                    std::time::Duration::from_millis(1),
                    reader.read_until(b'\n', line),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => break, // No more data ready
                }
            };

            match read_result {
                Ok(_) if line.is_empty() => {
                    // EOF on stdin
                    if pending.is_empty() {
                        return Err(TransportError::Closed);
                    }
                    break;
                }
                Ok(_) => {
                    // Strip the trailing newline and any carriage returns before it
                    let end = line
                        .iter()
                        .rposition(|&b| b != b'\n' && b != b'\r')
                        .map_or(0, |i| i + 1);
                    let payload_bytes = bytes::Bytes::copy_from_slice(&line[..end]);
                    line.clear();
                    if payload_bytes.is_empty() {
                        continue;
                    }

                    let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
                    let format = PayloadFormat::detect(&payload_bytes);
                    let timestamp_ms = chrono::Utc::now().timestamp_millis();

                    pending.push(Message {
                        key: None,
                        payload: payload_bytes,
                        token: PipeToken { seq },
                        timestamp_ms: Some(timestamp_ms),
                        format,
                    });
                }
                Err(e) => {
                    return Err(TransportError::Recv(format!("stdin read failed: {e}")));
                }
            }
        }
        // A call with a smaller `max` than the dropped one leaves the rest queued.
        let rest = pending.split_off(pending.len().min(max));
        let messages = std::mem::replace(pending, rest);
        drop(stdin);

        // Apply inbound filters via the shared partition helper; DLQ entries
        // are returned in the RecvBatch for the caller to route onward.
        let batch = filter_engine.partition_batch(
            messages,
            |m| m.payload.as_ref(),
            |m| m.key.clone(),
            |m| m.token,
        );
        let messages = batch.messages;
        let dlq_entries = batch.dlq_entries;
        let filtered_tokens = batch.filtered_tokens;

        #[cfg(feature = "logger")]
        if !messages.is_empty() {
            tracing::debug!(
                lines = messages.len(),
                "Pipe transport: batch received from stdin"
            );
        }

        // Transport-level ingress (post-filter, batch-at-a-time).
        #[cfg(feature = "metrics")]
        if !messages.is_empty() {
            let bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
            metrics::counter!("transport_received_bytes_total", "transport" => "pipe")
                .increment(bytes as u64);
            metrics::counter!("transport_received_events_total", "transport" => "pipe")
                .increment(messages.len() as u64);
        }

        Ok(RecvBatch {
            messages,
            dlq_entries,
            filtered_tokens,
        }
        .into())
    }

    async fn commit(&self, _tokens: &[Self::Token]) -> TransportResult<()> {
        // No-op: stdin is a forward-only stream, cannot rewind or acknowledge
        Ok(())
    }
}

impl super::traits::FromCascade for PipeTransportConfig {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_display() {
        let token = PipeToken { seq: 42 };
        assert_eq!(token.to_string(), "pipe:42");
    }

    #[test]
    fn token_as_str() {
        let token = PipeToken { seq: 7 };
        assert_eq!(token.as_str(), "pipe:7");
    }

    #[test]
    fn token_clone() {
        let token = PipeToken { seq: 99 };
        let cloned = token;
        assert_eq!(token, cloned);
    }

    #[test]
    fn config_defaults() {
        let config = PipeTransportConfig::default();
        assert_eq!(config.recv_timeout_ms, 100);
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = PipeTransportConfig {
            recv_timeout_ms: 500,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: PipeTransportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.recv_timeout_ms, 500);
    }

    #[test]
    fn config_serde_default_fields() {
        // Empty JSON should use defaults
        let parsed: PipeTransportConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed.recv_timeout_ms, 100);
    }

    #[tokio::test]
    async fn new_transport_is_healthy() {
        let config = PipeTransportConfig::default();
        let transport = PipeTransport::new(&config);
        assert!(transport.is_healthy());
        assert_eq!(transport.name(), "pipe");
    }

    #[tokio::test]
    async fn close_marks_unhealthy() {
        let config = PipeTransportConfig::default();
        let transport = PipeTransport::new(&config);

        transport.close().await.unwrap();
        assert!(!transport.is_healthy());
    }

    #[tokio::test]
    async fn send_after_close_returns_fatal() {
        let config = PipeTransportConfig::default();
        let transport = PipeTransport::new(&config);

        transport.close().await.unwrap();
        let result = transport
            .send("key", bytes::Bytes::from_static(b"data"))
            .await;
        assert!(result.is_fatal());
    }

    #[tokio::test]
    async fn recv_after_close_returns_error() {
        let config = PipeTransportConfig::default();
        let transport = PipeTransport::new(&config);

        transport.close().await.unwrap();
        let result = transport.recv(1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn commit_is_noop() {
        let config = PipeTransportConfig::default();
        let transport = PipeTransport::new(&config);

        let tokens = vec![PipeToken { seq: 0 }, PipeToken { seq: 1 }];
        let result = transport.commit(&tokens).await;
        assert!(result.is_ok());
    }

    /// A Tier 2 `dlq` rule: no cascade is installed in a unit test, so the
    /// default gates apply and reject it.
    fn config_with_uncompilable_rule() -> PipeTransportConfig {
        PipeTransportConfig {
            filters_in: vec![crate::transport::filter::FilterRule {
                expression: "severity > 3".into(),
                action: crate::transport::filter::FilterAction::Dlq,
            }],
            ..PipeTransportConfig::default()
        }
    }

    #[tokio::test]
    async fn uncompilable_filter_rule_refuses_traffic_instead_of_running_unfiltered() {
        let transport = PipeTransport::new(&config_with_uncompilable_rule());

        assert!(
            !transport.is_healthy(),
            "a pipe whose dlq rule did not compile must report unhealthy"
        );
        #[cfg(feature = "health")]
        assert!(
            crate::health::HealthRegistry::components()
                .iter()
                .any(|(name, status)| {
                    name == "transport:pipe" && *status == crate::health::HealthStatus::Unhealthy
                }),
            "the registered health probe must report the pipe unhealthy"
        );

        match transport.recv(1).await {
            Err(TransportError::Config(detail)) => assert!(
                detail.contains("filter_in[0]"),
                "recv must carry the compile error, got: {detail}"
            ),
            Err(other) => panic!("recv must refuse with the compile error, got: {other}"),
            Ok(batch) => panic!("recv must refuse, got {} records", batch.records.len()),
        }

        match transport
            .send("ignored", bytes::Bytes::from_static(br#"{"severity":5}"#))
            .await
        {
            SendResult::Fatal(TransportError::Config(detail)) => assert!(
                detail.contains("filter_in[0]"),
                "send must carry the compile error, got: {detail}"
            ),
            other => panic!("send must refuse with the compile error, got: {other:?}"),
        }
    }

    /// A transport reading the far end of a real OS pipe in place of stdin.
    #[cfg(unix)]
    fn piped(recv_timeout_ms: u64) -> (tokio::net::unix::pipe::Sender, PipeTransport) {
        let (tx, rx) = tokio::net::unix::pipe::pipe().unwrap();
        let config = PipeTransportConfig {
            recv_timeout_ms,
            ..PipeTransportConfig::default()
        };
        (tx, PipeTransport::with_input(&config, rx).unwrap())
    }

    #[cfg(unix)]
    fn payloads(batch: &WorkBatch<PipeToken>) -> Vec<&[u8]> {
        batch.records.iter().map(|r| r.payload.as_ref()).collect()
    }

    /// Drop `recv` after `ms` if it has not returned, as a losing `select!`
    /// arm does; `None` means it was dropped.
    #[cfg(unix)]
    async fn recv_within(
        transport: &PipeTransport,
        max: usize,
        ms: u64,
    ) -> Option<TransportResult<WorkBatch<PipeToken>>> {
        tokio::time::timeout(std::time::Duration::from_millis(ms), transport.recv(max))
            .await
            .ok()
    }

    /// A `recv` expected to return, bounded so a lost line fails the test
    /// rather than hanging it.
    #[cfg(unix)]
    async fn recv_now(
        transport: &PipeTransport,
        max: usize,
    ) -> TransportResult<WorkBatch<PipeToken>> {
        recv_within(transport, max, 5_000)
            .await
            .expect("recv returned within 5 s")
    }

    /// Dropping `recv` mid-line keeps both the line and the record read
    /// before it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_recv_dropped_mid_line_loses_nothing() {
        let (mut tx, transport) = piped(0);

        tx.write_all(b"{\"n\":1}\n{\"n\":").await.unwrap();
        assert!(
            recv_within(&transport, 10, 50).await.is_none(),
            "recv waits for the rest of the second line"
        );

        tx.write_all(b"2}\n").await.unwrap();
        let batch = recv_now(&transport, 2).await.unwrap();
        assert_eq!(payloads(&batch), [&b"{\"n\":1}"[..], b"{\"n\":2}"]);
        let seqs: Vec<u64> = batch.commit_tokens.iter().map(|t| t.seq).collect();
        assert_eq!(seqs, [0, 1]);

        drop(tx);
        assert!(
            matches!(recv_now(&transport, 10).await, Err(TransportError::Closed)),
            "nothing further arrives before EOF"
        );
    }

    /// The receive timeout drops the read the same way a caller does.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_receive_timeout_keeps_a_partial_line() {
        let (mut tx, transport) = piped(20);

        tx.write_all(b"{\"part\":").await.unwrap();
        let first = recv_now(&transport, 10).await.unwrap();
        assert!(first.records.is_empty(), "the line is not finished yet");

        tx.write_all(b"\"whole\"}\n").await.unwrap();
        let second = recv_now(&transport, 10).await.unwrap();
        assert_eq!(payloads(&second), [&b"{\"part\":\"whole\"}"[..]]);
    }

    /// Records kept from a dropped call come back no more than `max` at a time.
    #[cfg(unix)]
    #[tokio::test]
    async fn records_kept_from_a_dropped_recv_respect_max() {
        let (mut tx, transport) = piped(0);

        tx.write_all(b"a\nb\nc\nd").await.unwrap();
        assert!(
            recv_within(&transport, 10, 50).await.is_none(),
            "recv waits for the rest of the fourth line"
        );

        assert_eq!(
            payloads(&recv_now(&transport, 2).await.unwrap()),
            [b"a", b"b"]
        );
        assert_eq!(payloads(&recv_now(&transport, 1).await.unwrap()), [b"c"]);
        tx.write_all(b"\n").await.unwrap();
        assert_eq!(payloads(&recv_now(&transport, 1).await.unwrap()), [b"d"]);
    }

    #[tokio::test]
    async fn factory_refuses_a_pipe_whose_filter_rule_does_not_compile() {
        let config = crate::transport::TransportConfig {
            transport_type: crate::transport::TransportType::Pipe,
            pipe: Some(config_with_uncompilable_rule()),
            ..crate::transport::TransportConfig::default()
        };

        let sender = crate::transport::AnySender::from_transport_config(&config).await;
        assert!(
            matches!(sender, Err(TransportError::Config(_))),
            "AnySender must fail construction on a rule that does not compile"
        );

        let receiver = crate::transport::AnyReceiver::from_transport_config(&config).await;
        assert!(
            matches!(receiver, Err(TransportError::Config(_))),
            "AnyReceiver must fail construction on a rule that does not compile"
        );
    }
}
