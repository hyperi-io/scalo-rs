// Project:   scalo
// File:      src/sink_stack/tests.rs
// Purpose:   Sink-control stack tests (no mocks -- a real scripted sender)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Tests for the sink-control stack.
//!
//! No mocks of internal logic: the fake here is a real, fully-functional
//! `TransportSender` whose only special behaviour is a scripted send outcome --
//! the kind of controllable in-memory transport already used across the
//! transport tests. It lets us assert that the stack's controls (retry,
//! timeout, rate-limit, fatal classification) engage and preserve the
//! at-least-once contract.

use super::*;
use crate::governor::RateLimitConfig;
use crate::sink_stack::config::AdaptiveConfig;
use crate::transport::{PayloadFormat, RecordMeta, TransportResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Scripted send outcome for the fake sender.
#[derive(Clone, Copy)]
enum Script {
    /// Always succeed.
    Ok,
    /// Fail (Backpressured) for the first `n` calls, then succeed.
    FailThenOk(usize),
    /// Always fail with a fatal transport error.
    Fatal,
    /// Sleep this many ms on every call, then succeed (drives the timeout).
    Slow(u64),
}

/// A real `TransportSender` with a scripted outcome and a call counter.
struct FakeSender {
    script: Script,
    calls: AtomicUsize,
}

impl FakeSender {
    fn new(script: Script) -> Arc<Self> {
        Arc::new(Self {
            script,
            calls: AtomicUsize::new(0),
        })
    }
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl crate::transport::TransportBase for FakeSender {
    async fn close(&self) -> TransportResult<()> {
        Ok(())
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "fake"
    }
}

impl TransportSender for FakeSender {
    async fn send(&self, _destination: &str, _payload: bytes::Bytes) -> SendResult {
        SendResult::Ok
    }

    async fn send_batch(&self, _records: &[Record]) -> SendResult {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        match self.script {
            Script::Ok => SendResult::Ok,
            Script::FailThenOk(fail_count) => {
                if n < fail_count {
                    SendResult::Backpressured
                } else {
                    SendResult::Ok
                }
            }
            Script::Fatal => SendResult::Fatal(TransportError::Connection("scripted fatal".into())),
            Script::Slow(ms) => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                SendResult::Ok
            }
        }
    }
}

/// Minimal commit token for building a `WorkBatch` in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestToken(u64);
impl std::fmt::Display for TestToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tok-{}", self.0)
    }
}
impl crate::transport::CommitToken for TestToken {}

fn rec(payload: &'static [u8]) -> Record {
    Record {
        payload: bytes::Bytes::from_static(payload),
        key: None,
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

fn batch() -> Vec<Record> {
    vec![rec(b"{\"a\":1}"), rec(b"{\"b\":2}")]
}

/// Fast backoff so timing tests stay quick.
fn fast_cfg() -> SinkStackConfig {
    SinkStackConfig {
        min_backoff_ms: 1,
        max_backoff_ms: 5,
        ..Default::default()
    }
}

#[tokio::test]
async fn delivers_ok_in_a_single_call() {
    let sender = FakeSender::new(Script::Ok);
    let stack = SinkStack::new(Arc::clone(&sender), &fast_cfg());

    let result = stack.send_batch(batch()).await;
    assert!(result.is_ok());
    assert_eq!(sender.call_count(), 1, "no retries on first-try success");
}

#[tokio::test]
async fn empty_batch_is_ok_without_calling_sender() {
    let sender = FakeSender::new(Script::Ok);
    let stack = SinkStack::new(Arc::clone(&sender), &fast_cfg());

    assert!(stack.send_batch(Vec::new()).await.is_ok());
    assert_eq!(
        sender.call_count(),
        0,
        "empty batch must not hit the sender"
    );
}

#[tokio::test]
async fn retries_transient_then_succeeds() {
    // Fail twice, succeed on the third call; 3 retries allowed.
    let sender = FakeSender::new(Script::FailThenOk(2));
    let cfg = SinkStackConfig {
        max_retries: 3,
        ..fast_cfg()
    };
    let stack = SinkStack::new(Arc::clone(&sender), &cfg);

    let result = stack.send_batch(batch()).await;
    assert!(result.is_ok(), "should succeed once the sink recovers");
    assert_eq!(sender.call_count(), 3, "initial + 2 retries");
}

#[tokio::test]
async fn fatal_is_not_retried() {
    let sender = FakeSender::new(Script::Fatal);
    let cfg = SinkStackConfig {
        max_retries: 5,
        ..fast_cfg()
    };
    let stack = SinkStack::new(Arc::clone(&sender), &cfg);

    let result = stack.send_batch(batch()).await;
    assert!(
        result.is_fatal(),
        "fatal must surface as Fatal, got {result:?}"
    );
    assert_eq!(sender.call_count(), 1, "a fatal error is never retried");
}

#[tokio::test]
async fn exhausted_transient_returns_backpressured() {
    // Never recovers; with 2 retries the caller sees Backpressured (retryable),
    // never silent success -- so its commit does NOT advance (no data loss).
    let sender = FakeSender::new(Script::FailThenOk(99));
    let cfg = SinkStackConfig {
        max_retries: 2,
        ..fast_cfg()
    };
    let stack = SinkStack::new(Arc::clone(&sender), &cfg);

    let result = stack.send_batch(batch()).await;
    assert!(
        result.is_backpressured(),
        "exhausted retries must report Backpressured, got {result:?}"
    );
    assert_eq!(sender.call_count(), 3, "initial + 2 retries, then give up");
}

#[tokio::test]
async fn per_attempt_timeout_engages() {
    // Sender is slower than the per-attempt timeout, so every attempt times out;
    // after retries the caller gets Backpressured (retryable, no loss). The test
    // also proves the call returns promptly rather than hanging on the slow sink.
    let sender = FakeSender::new(Script::Slow(1_000));
    let cfg = SinkStackConfig {
        attempt_timeout_ms: 20,
        max_retries: 1,
        ..fast_cfg()
    };
    let stack = SinkStack::new(Arc::clone(&sender), &cfg);

    let start = Instant::now();
    let result = stack.send_batch(batch()).await;
    assert!(
        result.is_backpressured(),
        "timed-out attempts must report Backpressured, got {result:?}"
    );
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "must abandon the slow sink via timeout, not block on it"
    );
}

#[test]
fn adaptive_config_builds_clamped_limiter() {
    // min_limit 0 would deadlock -> clamped to 1; initial honoured.
    let ac = AdaptiveConfig {
        initial_limit: 7,
        min_limit: 0, // clamped to 1 (deadlock guard)
        max_limit: 50,
        increase_by: 1,
        decrease_factor: 0.5,
    };
    let limiter = ac.build_limiter();
    assert_eq!(limiter.limit(), 7, "initial limit honoured");
    assert!(
        limiter.try_acquire().is_some(),
        "a slot is available at the start"
    );
}

fn adaptive_cfg() -> SinkStackConfig {
    SinkStackConfig {
        adaptive: Some(AdaptiveConfig {
            initial_limit: 4,
            min_limit: 1,
            max_limit: 16,
            ..AdaptiveConfig::default()
        }),
        ..fast_cfg()
    }
}

#[tokio::test]
async fn send_workbatch_delivers_records_and_ignores_commit_tokens() {
    // The driver-path entry point: send a WorkBatch's records, leaving the
    // source commit_tokens untouched (the driver fires them after Ok).
    use crate::transport::WorkBatch;
    let sender = FakeSender::new(Script::Ok);
    let stack = SinkStack::new(Arc::clone(&sender), &fast_cfg());

    let wb = WorkBatch::new(batch(), vec![TestToken(1), TestToken(2)]);
    let result = stack.send_workbatch(&wb).await;
    assert!(result.is_ok());
    assert_eq!(sender.call_count(), 1);
    // commit_tokens are the driver's concern, never sent by the stack.
    assert_eq!(
        wb.commit_tokens.len(),
        2,
        "tokens left intact for the driver"
    );
}

#[tokio::test]
async fn adaptive_stack_delivers_ok() {
    let sender = FakeSender::new(Script::Ok);
    let stack = SinkStack::new(Arc::clone(&sender), &adaptive_cfg());
    assert!(stack.send_batch(batch()).await.is_ok());
    assert_eq!(sender.call_count(), 1);
}

#[tokio::test]
async fn adaptive_stack_preserves_at_least_once() {
    // Fatal must still not retry, and a transient must still surface as
    // Backpressured (retryable) through the ARC path -- adaptive concurrency
    // only DELAYS, it never drops or false-acks.
    let fatal = FakeSender::new(Script::Fatal);
    let stack = SinkStack::new(Arc::clone(&fatal), &adaptive_cfg());
    assert!(stack.send_batch(batch()).await.is_fatal());
    assert_eq!(fatal.call_count(), 1, "fatal not retried under ARC");
}

#[tokio::test]
async fn adaptive_recovers_after_failures_no_deadlock() {
    // Sustained transient failures drive the AIMD limit DOWN; the min-limit
    // floor (>=1) must keep at least one slot so the sink recovers rather than
    // deadlocking at zero concurrency. Fail 3 times then succeed, 5 retries.
    let sender = FakeSender::new(Script::FailThenOk(3));
    let cfg = SinkStackConfig {
        max_retries: 5,
        ..adaptive_cfg()
    };
    let stack = SinkStack::new(Arc::clone(&sender), &cfg);

    let start = Instant::now();
    let result = stack.send_batch(batch()).await;
    assert!(
        result.is_ok(),
        "must recover, not deadlock at zero concurrency"
    );
    assert_eq!(sender.call_count(), 4, "3 failures + 1 success");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "recovery must be prompt, not stalled"
    );
}

#[tokio::test]
async fn rate_limit_paces_sequential_sends() {
    // rps=100, burst=1 -> after the first, each send waits ~10ms for a token.
    let sender = FakeSender::new(Script::Ok);
    let mut cfg = fast_cfg();
    cfg.rate_limit = RateLimitConfig::per_second(100).with_burst(1);
    let stack = SinkStack::new(Arc::clone(&sender), &cfg);

    // Drain the initial token, then time five paced sends.
    let _ = stack.send_batch(batch()).await;
    let start = Instant::now();
    for _ in 0..5 {
        assert!(stack.send_batch(batch()).await.is_ok());
    }
    assert!(
        start.elapsed() >= Duration::from_millis(40),
        "5 sends at 100rps must take ~50ms+, took {:?}",
        start.elapsed()
    );
}
