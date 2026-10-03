// Project:   scalo
// File:      src/worker/engine/driver_tests.rs
// Purpose:   Tests for the WorkBatch engine run-loop driver
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Driver run-loop tests, split out of driver.rs to keep that file focused
//! on the run paths. A `#[path]` submodule of `driver`, so `super` resolves
//! to the driver module's items.

use super::*;
use crate::transport::memory::{MemoryConfig, MemoryTransport};
use crate::transport::{CommitToken, PayloadFormat, RecordMeta};
use crate::worker::engine::BatchProcessingConfig;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

fn default_engine() -> BatchEngine {
    BatchEngine::new(BatchProcessingConfig::default())
}

fn mem_transport(timeout_ms: u64) -> MemoryTransport {
    MemoryTransport::new(&MemoryConfig {
        recv_timeout_ms: timeout_ms,
        ..Default::default()
    })
    .expect("memory transport with valid config must construct")
}

/// Cancel `shutdown` after `ms` to stop the run loop cleanly.
fn cancel_after(shutdown: CancellationToken, ms: u64) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        shutdown.cancel();
    });
}

/// Clone one record into `factor` copies (1->N fan-out).
fn fan_out(records: Vec<Record>, factor: usize) -> Vec<Record> {
    let mut out = Vec::with_capacity(records.len() * factor);
    for r in records {
        for _ in 0..factor {
            out.push(r.clone());
        }
    }
    out
}

/// THE proving test: N source records, each with a distinct ack; a process
/// that fans 1->2; assert all 2N records hit the sink AND commit acked
/// EXACTLY N source tokens (committed_sequence advanced by the source acks,
/// not the doubled output count).
#[tokio::test]
async fn fan_out_commits_source_tokens_not_output_count() {
    let n = 5usize;
    let transport = mem_transport(50);
    for i in 0..n {
        transport
            .inject(None, format!(r#"{{"id":{i}}}"#).into_bytes())
            .await
            .unwrap();
    }

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let sink_records = Arc::new(AtomicUsize::new(0));
    let sink_tokens = Arc::new(AtomicUsize::new(0));
    let sr = Arc::clone(&sink_records);
    let st = Arc::clone(&sink_tokens);

    engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| Ok(batch.map_records(|recs| fan_out(recs, 2))),
            |out: &WorkBatch<_>| {
                let sr = Arc::clone(&sr);
                let st = Arc::clone(&st);
                let records = out.records.len();
                let tokens = out.commit_tokens.len();
                async move {
                    sr.fetch_add(records, Ordering::Relaxed);
                    st.fetch_add(tokens, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    // (a) all 2N records reached the sink.
    assert_eq!(
        sink_records.load(Ordering::Relaxed),
        2 * n,
        "all 2N records sunk"
    );
    // (b) the out-batch carried exactly N source tokens (fan-out did not
    // multiply the acks).
    assert_eq!(
        sink_tokens.load(Ordering::Relaxed),
        n,
        "N source tokens carried"
    );
    // (b cont.) commit acked exactly the N source tokens: MemoryToken seq is
    // 0..N, so committed_sequence (a fetch_max) lands on N-1.
    assert_eq!(
        transport.committed_sequence(),
        (n - 1) as u64,
        "commit advanced to the highest of the N source acks, not the 2N output count"
    );
}

/// On a sink error the commit must NOT fire (the block is re-delivered) AND
/// the run loop stops -- the sink error is a TERMINAL ack-barrier error.
#[tokio::test]
async fn sink_error_does_not_commit() {
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"id":1}"#.to_vec())
        .await
        .unwrap();

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let result = engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Err(EngineError::Sink("boom".into())) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;
    assert!(
        matches!(result, Err(EngineError::Sink(_))),
        "sink error is terminal: the run returns the sink error, got {result:?}"
    );

    // committed_sequence is a fetch_max seeded at 0 and the only injected
    // message had seq 0; a commit would still leave it at 0, so to PROVE the
    // commit did not fire we inject a higher-seq message that, if committed,
    // would advance the sequence past 0. Re-run with seq 1..=2.
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"a":1}"#.to_vec())
        .await
        .unwrap(); // seq 0
    transport
        .inject(None, br#"{"b":2}"#.to_vec())
        .await
        .unwrap(); // seq 1
    // drain seq 0 first so the failing block carries seq 1.
    let _ = transport.recv(1).await.unwrap();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);
    let result = engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Err(EngineError::Sink("boom".into())) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;
    assert!(result.is_err(), "sink error is terminal");
    assert_eq!(
        transport.committed_sequence(),
        0,
        "sink error must skip commit -- sequence stays at its initial 0"
    );
}

/// `CommitMode::Auto` commits after a successful sink.
#[tokio::test]
async fn auto_commits_after_sink_ok() {
    let transport = mem_transport(50);
    for i in 0..3u64 {
        transport
            .inject(None, format!(r#"{{"id":{i}}}"#).into_bytes())
            .await
            .unwrap();
    }

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    // Three messages seq 0..=2 -> committed sequence is 2.
    assert_eq!(transport.committed_sequence(), 2);
}

/// `CommitMode::SinkManaged` leaves the commit to the sink -- the engine
/// does not commit.
#[tokio::test]
async fn sink_managed_does_not_commit_in_engine() {
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"a":1}"#.to_vec())
        .await
        .unwrap(); // seq 0
    transport
        .inject(None, br#"{"b":2}"#.to_vec())
        .await
        .unwrap(); // seq 1
    // Drain seq 0 so the block carries seq 1 -- a commit would push the
    // sequence past its initial 0.
    let _ = transport.recv(1).await.unwrap();

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| Ok(batch),
            // Sink does NOT commit here -- it could, but we prove the engine
            // does not commit on its behalf.
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::SinkManaged,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(
        transport.committed_sequence(),
        0,
        "SinkManaged: engine must not commit -- sequence stays at initial 0"
    );
}

/// The ticker fires on its interval; shutdown stops the loop cleanly.
#[tokio::test]
async fn ticker_fires_and_shutdown_stops_loop() {
    let transport = mem_transport(50);
    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 350);

    let ticks = Arc::new(AtomicU64::new(0));
    let tc = Arc::clone(&ticks);

    let result = engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            Some((Duration::from_millis(100), move || {
                let tc = Arc::clone(&tc);
                async move {
                    tc.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            })),
        )
        .await;

    assert!(result.is_ok(), "shutdown stops the loop cleanly");
    assert!(
        ticks.load(Ordering::Relaxed) >= 2,
        "ticker fired at least twice over 350ms at 100ms interval"
    );
}

/// On-demand path: a transform that calls codec::parse reads the right field
/// and can rewrite the payload, all without the driver pre-parsing.
#[tokio::test]
async fn on_demand_transform_reads_field_via_codec_parse() {
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"_table":"events","id":1}"#.to_vec())
        .await
        .unwrap();

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let seen_table = Arc::new(std::sync::Mutex::new(String::new()));
    let st = Arc::clone(&seen_table);

    engine
        .run_workbatch(
            &transport,
            shutdown,
            move |batch| {
                let st = Arc::clone(&st);
                Ok(batch.map_records(move |recs| {
                    recs.into_iter()
                        .inspect(|r| {
                            // Parse ON DEMAND inside the transform.
                            let parsed =
                                codec::parse(&r.payload, r.metadata.format).expect("valid json");
                            if let Some(t) = parsed.field_str("_table") {
                                *st.lock().unwrap() = t.to_string();
                            }
                        })
                        .collect()
                }))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(*seen_table.lock().unwrap(), "events");
}

/// Batch-parse path: the driver pre-parses; the process closure sees aligned
/// parsed payloads, the interner dedups field names, and the logical result
/// matches the on-demand path.
#[tokio::test]
async fn parsed_path_pre_parses_and_interner_dedups() {
    let transport = mem_transport(50);
    for i in 0..4 {
        transport
            .inject(
                None,
                format!(r#"{{"_table":"events","id":{i}}}"#).into_bytes(),
            )
            .await
            .unwrap();
    }

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let tables = Arc::new(AtomicUsize::new(0));
    let tc = Arc::clone(&tables);

    engine
        .run_workbatch_parsed(
            &transport,
            shutdown,
            move |pb: ParsedBatch<'_, _>| {
                // Records are aligned 1:1 with parsed payloads.
                assert_eq!(pb.records.len(), pb.parsed.len());
                // Intern the routing-field name once for the whole block.
                let field = pb.intern("_table");
                let mut hits = 0;
                for parsed in &pb.parsed {
                    if parsed.field_str(&field) == Some("events") {
                        hits += 1;
                    }
                }
                tc.fetch_add(hits, Ordering::Relaxed);
                // Re-assemble a WorkBatch preserving the source acks.
                Ok(WorkBatch::new(pb.records, pb.commit_tokens).with_dlq_entries(pb.dlq_entries))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(
        tables.load(Ordering::Relaxed),
        4,
        "all 4 records routed on _table"
    );
    assert_eq!(transport.committed_sequence(), 3, "all 4 acks committed");
}

/// Parsed path no-silent-drop (default `ParseErrorAction::Dlq`): an
/// unparseable record is routed to the out-batch DLQ entries, the process
/// closure sees them, AND they reach the DLQ route point (a `Route` policy
/// sink) before commit -- not dropped -- while source acks stay intact.
#[tokio::test]
async fn parsed_path_routes_parse_failures_to_dlq() {
    use crate::worker::engine::FilterDlqPolicy;
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"id":1}"#.to_vec())
        .await
        .unwrap(); // seq 0 ok
    transport
        .inject(None, b"not json {{{".to_vec())
        .await
        .unwrap(); // seq 1 bad
    transport
        .inject(None, br#"{"id":3}"#.to_vec())
        .await
        .unwrap(); // seq 2 ok

    // A Route policy captures the entries that reach the DLQ route point.
    let routed = Arc::new(AtomicUsize::new(0));
    let rc = Arc::clone(&routed);
    let engine = default_engine().with_filter_dlq_policy(FilterDlqPolicy::Route(Arc::new(
        move |entries: Vec<crate::transport::filter::FilteredDlqEntry>| {
            rc.fetch_add(entries.len(), Ordering::Relaxed);
            Ok(())
        },
    )));
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let dlq_seen = Arc::new(AtomicUsize::new(0));
    let kept = Arc::new(AtomicUsize::new(0));
    let ds = Arc::clone(&dlq_seen);
    let kp = Arc::clone(&kept);

    engine
        .run_workbatch_parsed(
            &transport,
            shutdown,
            move |pb: ParsedBatch<'_, _>| {
                ds.fetch_add(pb.dlq_entries.len(), Ordering::Relaxed);
                kp.fetch_add(pb.records.len(), Ordering::Relaxed);
                Ok(WorkBatch::new(pb.records, pb.commit_tokens).with_dlq_entries(pb.dlq_entries))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(kept.load(Ordering::Relaxed), 2, "2 records parsed cleanly");
    assert_eq!(
        dlq_seen.load(Ordering::Relaxed),
        1,
        "1 parse failure carried to the process closure as a DLQ entry"
    );
    assert_eq!(
        routed.load(Ordering::Relaxed),
        1,
        "the parse-failure DLQ entry reached the DLQ route point before commit"
    );
    // All three source acks are still committed -- a parse failure does not
    // lose the source ack (at-least-once on the WHOLE block).
    assert_eq!(transport.committed_sequence(), 2);
}

/// Memory pressure / lease accounting on a WorkBatch.
#[cfg(feature = "memory")]
#[tokio::test]
async fn lease_ingress_batch_accounts_and_releases() {
    use crate::memory::{MemoryGuard, MemoryGuardConfig};

    let mut engine = default_engine();
    let guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1024 * 1024,
        ..Default::default()
    }));
    engine.set_memory_guard_for_test(Arc::clone(&guard));

    let payloads: Vec<Record> = (0..4)
        .map(|i| Record {
            payload: Bytes::from(format!(r#"{{"id":{i}}}"#)),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();
    let batch = WorkBatch::<MemTok>::from_records(payloads);
    let expected = batch.total_payload_bytes() as u64;

    // The lease counter, not process usage -- this is what the lease moves.
    assert_eq!(guard.reserved_bytes(), 0);
    {
        let _lease = engine.lease_ingress_batch(&batch).expect("guard present");
        assert_eq!(guard.reserved_bytes(), expected, "accounted while held");
    }
    assert_eq!(guard.reserved_bytes(), 0, "released on drop");
}

/// A minimal CommitToken for the memory-lease unit test (no transport recv).
#[cfg(feature = "memory")]
#[derive(Debug, Clone)]
struct MemTok;
#[cfg(feature = "memory")]
impl std::fmt::Display for MemTok {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("memtok")
    }
}
#[cfg(feature = "memory")]
impl CommitToken for MemTok {}

// ---- Ordered-commit ack barrier --------------------------------------
//
// Kafka (and MemoryTransport) commit is CUMULATIVE: `commit up to offset N`
// advances a watermark via fetch_max. So if a block carrying token 0 fails
// its sink/commit, a LATER block carrying token 1 must NEVER be committed --
// doing so advances the watermark past token 0's never-sent records, which
// silently skips them (data loss, at-least-once violated). These tests pin
// the ack barrier: the committed watermark never advances past the last
// successfully-sunk-and-committed block.

/// A real ORDERED receiver test double (real `Record`/`WorkBatch`/`MemoryToken`
/// types, no internal-code mock). It hands out ONE record per `recv` with
/// MONOTONIC tokens (seq 0, 1, 2, ...) and a CUMULATIVE commit -- the
/// committed watermark is `fetch_max` of the committed tokens, exactly like
/// a Kafka offset commit. This isolates the ordered-commit semantics from
/// MemoryTransport's channel batching (which would coalesce all pending
/// messages into a single block).
struct OrderedReceiver {
    /// Next seq to deliver; one record per recv until exhausted.
    next_seq: Arc<AtomicU64>,
    /// How many records to deliver before recv blocks (pending) forever.
    total: u64,
    /// Cumulative committed watermark (highest committed seq + 1, or 0 if
    /// nothing committed). `u64::MAX` sentinel means "no commit yet".
    committed_hwm: Arc<AtomicU64>,
    /// Count of commit calls (to prove a later block's commit did not fire).
    commit_calls: Arc<AtomicUsize>,
    /// If set, `commit` returns an error (broker commit failure) for any
    /// block whose highest token seq equals this value.
    fail_commit_on_seq: Option<u64>,
    /// How many more `recv` calls report the source busy before records flow.
    recv_backpressure: Arc<AtomicU64>,
    /// Set by `close()`; `recv` then reports `Closed`, as Kafka does.
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl OrderedReceiver {
    fn new(total: u64) -> Self {
        Self {
            next_seq: Arc::new(AtomicU64::new(0)),
            total,
            committed_hwm: Arc::new(AtomicU64::new(u64::MAX)),
            commit_calls: Arc::new(AtomicUsize::new(0)),
            fail_commit_on_seq: None,
            recv_backpressure: Arc::new(AtomicU64::new(0)),
            closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

impl crate::transport::TransportBase for OrderedReceiver {
    fn close(
        &self,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<()>> + Send {
        self.closed.store(true, Ordering::Relaxed);
        std::future::ready(Ok(()))
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "ordered-test"
    }
}

impl TransportReceiver for OrderedReceiver {
    type Token = crate::transport::memory::MemoryToken;

    fn recv(
        &self,
        _max: usize,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<WorkBatch<Self::Token>>>
    + Send {
        let next_seq = Arc::clone(&self.next_seq);
        let total = self.total;
        let recv_backpressure = Arc::clone(&self.recv_backpressure);
        let closed = self.closed.load(Ordering::Relaxed);
        async move {
            if closed {
                return Err(crate::transport::TransportError::Closed);
            }
            if recv_backpressure
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(crate::transport::TransportError::Backpressure);
            }
            let seq = next_seq.fetch_add(1, Ordering::Relaxed);
            if seq >= total {
                // Exhausted: block forever so the loop only exits on shutdown
                // (mirrors a quiet broker -- never an error/EOF).
                next_seq.fetch_sub(1, Ordering::Relaxed);
                std::future::pending::<()>().await;
            }
            let record = Record {
                payload: Bytes::from(format!(r#"{{"seq":{seq}}}"#)),
                key: None,
                headers: vec![],
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            };
            Ok(WorkBatch::new(
                vec![record],
                vec![crate::transport::memory::MemoryToken { seq }],
            ))
        }
    }

    async fn commit(&self, tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        let Some(max_seq) = tokens.iter().map(|t| t.seq).max() else {
            return Ok(());
        };
        if self.fail_commit_on_seq == Some(max_seq) {
            return Err(crate::transport::TransportError::Commit(format!(
                "broker commit failed for seq {max_seq}"
            )));
        }
        // Cumulative: watermark = max(current, this block's highest seq), with
        // the u64::MAX "nothing committed" sentinel replaced by the first commit.
        let _ = self
            .committed_hwm
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(if current == u64::MAX {
                    max_seq
                } else {
                    current.max(max_seq)
                })
            });
        Ok(())
    }
}

/// THE ack-barrier bug test (sink failure). Token 0's block fails at the
/// sink; token 1's block would succeed. With an ORDERED/cumulative commit,
/// the engine must NEVER commit token 1 (which would advance the watermark
/// past the never-sent token 0). Assert: the committed watermark never
/// advances past the last successfully-sunk block -- i.e. NOTHING is
/// committed, and the run STOPS (terminal) rather than draining token 1.
#[tokio::test]
async fn sink_error_blocks_later_ordered_commits() {
    let receiver = OrderedReceiver::new(3);
    let committed = Arc::clone(&receiver.committed_hwm);
    let commit_calls = Arc::clone(&receiver.commit_calls);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    // Safety net: if the loop wrongly continued, shutdown stops it so the
    // test cannot hang. The assertions still catch the data-loss advance.
    cancel_after(shutdown.clone(), 500);

    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&sink_calls);

    let result = engine
        .run_workbatch(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let sc = Arc::clone(&sc);
                // Fail the sink for the block carrying token 0.
                let carries_zero = out.commit_tokens.iter().any(|t| t.seq == 0);
                async move {
                    sc.fetch_add(1, Ordering::Relaxed);
                    if carries_zero {
                        Err(EngineError::Sink("boom on token 0".into()))
                    } else {
                        Ok(())
                    }
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    // The ack barrier: token 0 failed, so the watermark must NOT advance
    // past it. NOTHING may be committed (token 1 must never commit ahead).
    assert_eq!(
        committed.load(Ordering::Relaxed),
        u64::MAX,
        "sink error on token 0 must leave the committed watermark unmoved -- \
             a later token must NOT be committed past the failed offset"
    );
    assert_eq!(
        commit_calls.load(Ordering::Relaxed),
        0,
        "no commit may fire while token 0's block is unsent"
    );
    // The fix makes the sink error TERMINAL: the run returns Err and the
    // loop never advances to deliver token 1.
    assert!(
        result.is_err(),
        "sink failure under Auto must be a terminal engine error (ack barrier), \
             not a logged continue that drains later blocks"
    );
    assert_eq!(
        sink_calls.load(Ordering::Relaxed),
        1,
        "loop must stop at the failed block -- token 1 must not be fetched+sunk"
    );
}

/// A failed COMMIT does not stop the loop. Token 0's block was delivered
/// before its commit failed, and commits are cumulative, so the commit of
/// token 2 covers it: nothing is skipped, and at worst a restart re-delivers.
#[tokio::test]
async fn a_commit_error_is_reported_and_the_loop_carries_on() {
    let mut receiver = OrderedReceiver::new(3);
    receiver.fail_commit_on_seq = Some(0);
    let committed = Arc::clone(&receiver.committed_hwm);
    let commit_calls = Arc::clone(&receiver.commit_calls);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 500);

    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let s = Arc::clone(&sunk);

    let result = engine
        .run_workbatch(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let s = Arc::clone(&s);
                let seqs: Vec<u64> = out.commit_tokens.iter().map(|t| t.seq).collect();
                async move {
                    s.lock().extend(seqs);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(
        result.is_ok(),
        "a failed commit must not end the run: {result:?}"
    );
    assert_eq!(
        *sunk.lock(),
        vec![0, 1, 2],
        "every block delivered once, in order"
    );
    assert_eq!(
        commit_calls.load(Ordering::Relaxed),
        3,
        "every block's commit was attempted"
    );
    assert_eq!(
        committed.load(Ordering::Relaxed),
        2,
        "the later cumulative commit covers the block whose own commit failed"
    );
}

/// The driver-level outage test: the sink reports backpressure for 30 s. The
/// run survives, holds token 0's block the whole time -- nothing later is
/// fetched or committed past it -- then delivers it and every later block.
#[tokio::test(start_paused = true)]
async fn a_backpressured_sink_is_waited_out_and_the_block_delivered() {
    let receiver = OrderedReceiver::new(3);
    let committed = Arc::clone(&receiver.committed_hwm);
    let commit_calls = Arc::clone(&receiver.commit_calls);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    let outage = Duration::from_secs(30);
    cancel_after(shutdown.clone(), 60_000);

    let recovers_at = tokio::time::Instant::now() + outage;
    let attempts = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let delivered = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (a, d) = (Arc::clone(&attempts), Arc::clone(&delivered));
    let committed_during_outage = Arc::clone(&committed);

    let result = engine
        .run_workbatch(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let (a, d) = (Arc::clone(&a), Arc::clone(&d));
                let committed = Arc::clone(&committed_during_outage);
                let seqs: Vec<u64> = out.commit_tokens.iter().map(|t| t.seq).collect();
                async move {
                    a.lock().extend(seqs.iter().copied());
                    if tokio::time::Instant::now() < recovers_at {
                        assert_eq!(
                            committed.load(Ordering::Relaxed),
                            u64::MAX,
                            "nothing may commit while token 0's block is undelivered"
                        );
                        return Err(crate::transport::TransportError::Backpressure.into());
                    }
                    d.lock().extend(seqs);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(
        result.is_ok(),
        "a backpressured sink must not end the run: {result:?}"
    );
    let attempts = attempts.lock().clone();
    let retries = attempts.iter().filter(|seq| **seq == 0).count();
    assert!(
        retries > 5,
        "token 0's block should be re-sunk through the outage, got {retries} attempts"
    );
    assert!(
        retries < 60,
        "{retries} attempts in {outage:?} -- the retry is not backing off"
    );
    assert!(
        attempts.iter().take(retries).all(|seq| *seq == 0),
        "no later block reached the sink before token 0 was delivered: {attempts:?}"
    );
    assert_eq!(
        *delivered.lock(),
        vec![0, 1, 2],
        "every block delivered, in order"
    );
    assert_eq!(committed.load(Ordering::Relaxed), 2);
    assert_eq!(commit_calls.load(Ordering::Relaxed), 3);
}

/// Shutdown during a sink outage ends the run cleanly and leaves the held
/// block uncommitted, so the source re-delivers it.
#[tokio::test(start_paused = true)]
async fn shutdown_during_a_sink_outage_leaves_the_block_uncommitted() {
    let receiver = OrderedReceiver::new(3);
    let commit_calls = Arc::clone(&receiver.commit_calls);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 5_000);

    let result = engine
        .run_workbatch(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async {
                Err(crate::transport::TransportError::Backpressure.into())
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(result.is_ok(), "shutdown ends the run cleanly: {result:?}");
    assert_eq!(
        commit_calls.load(Ordering::Relaxed),
        0,
        "a block the sink never took must not be committed"
    );
    assert!(
        receiver.closed.load(Ordering::Relaxed),
        "the source is closed on the way out"
    );
    assert_eq!(
        receiver.next_seq.load(Ordering::Relaxed),
        1,
        "no block past the refused one is fetched: there is no drain after it"
    );
}

/// A source reporting backpressure is polled again after a backoff, not
/// treated as the end of the run.
#[tokio::test(start_paused = true)]
async fn a_backpressured_recv_is_waited_out() {
    let receiver = OrderedReceiver::new(2);
    receiver.recv_backpressure.store(8, Ordering::Relaxed);
    let committed = Arc::clone(&receiver.committed_hwm);
    let recv_backpressure = Arc::clone(&receiver.recv_backpressure);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 60_000);

    let result = engine
        .run_workbatch(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(
        result.is_ok(),
        "a backpressured source must not end the run: {result:?}"
    );
    assert_eq!(recv_backpressure.load(Ordering::Relaxed), 0);
    assert_eq!(
        committed.load(Ordering::Relaxed),
        1,
        "both records flowed once the source recovered"
    );
}

/// Streaming variant: a backpressured sub-block is held and re-sunk; the
/// block commits once, after its final sub-block is delivered.
#[tokio::test(start_paused = true)]
async fn streaming_backpressured_sub_block_is_held_and_retried() {
    let receiver = OrderedReceiver::new(3);
    let committed = Arc::clone(&receiver.committed_hwm);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 60_000);

    let recovers_at = tokio::time::Instant::now() + Duration::from_secs(10);
    let delivered = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let d = Arc::clone(&delivered);

    let result = engine
        .run_workbatch_streaming(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let d = Arc::clone(&d);
                let payloads: Vec<Bytes> = out.records.iter().map(|r| r.payload.clone()).collect();
                async move {
                    if tokio::time::Instant::now() < recovers_at {
                        return Err(crate::transport::TransportError::Timeout.into());
                    }
                    d.lock().extend(payloads);
                    Ok(())
                }
            },
            CommitMode::Auto,
            64,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    let delivered: Vec<Bytes> = delivered.lock().clone();
    assert_eq!(
        delivered,
        vec![
            Bytes::from_static(br#"{"seq":0}"#),
            Bytes::from_static(br#"{"seq":1}"#),
            Bytes::from_static(br#"{"seq":2}"#),
        ],
        "every sub-block delivered once, in order"
    );
    assert_eq!(committed.load(Ordering::Relaxed), 2);
}

/// Streaming variant of the ack barrier: a sink error on token 0's block
/// (streamed in sub-blocks) must block any later ordered commit. Mid-block
/// sink failure stops the block AND must not let a later block's commit
/// advance the watermark past it.
#[tokio::test]
async fn streaming_sink_error_blocks_later_ordered_commits() {
    let receiver = OrderedReceiver::new(3);
    let committed = Arc::clone(&receiver.committed_hwm);
    let commit_calls = Arc::clone(&receiver.commit_calls);

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 500);

    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&sink_calls);

    let result = engine
        .run_workbatch_streaming(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let sc = Arc::clone(&sc);
                // Streaming sub-block views carry EMPTY commit_tokens, so we
                // identify token 0's block by its payload bytes ({"seq":0}).
                let carries_zero = out
                    .records
                    .iter()
                    .any(|r| r.payload.as_ref() == br#"{"seq":0}"#);
                async move {
                    sc.fetch_add(1, Ordering::Relaxed);
                    if carries_zero {
                        Err(EngineError::Sink("boom on token 0 (streaming)".into()))
                    } else {
                        Ok(())
                    }
                }
            },
            CommitMode::Auto,
            64, // one record per sub-block (records are tiny)
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert_eq!(
        committed.load(Ordering::Relaxed),
        u64::MAX,
        "streaming sink error on token 0 must not let a later token commit ahead"
    );
    assert_eq!(
        commit_calls.load(Ordering::Relaxed),
        0,
        "no commit may fire while token 0's block is unsent (streaming)"
    );
    assert!(
        result.is_err(),
        "streaming sink failure under Auto must be a terminal ack-barrier error"
    );
    assert_eq!(
        sink_calls.load(Ordering::Relaxed),
        1,
        "streaming loop must stop at the failed block"
    );
}

// ---- Per-unit streaming ----------------------------------------------

/// split_into_sub_blocks unit coverage: byte-budget splitting + floor-1.
#[test]
fn split_groups_by_byte_target() {
    // Five 10-byte records, target 25 -> sub-blocks of {2,2,1} records
    // (20 <= 25; adding the 3rd would be 30 > 25 -> close at 2).
    let records: Vec<Record> = (0..5)
        .map(|_| Record {
            payload: Bytes::from_static(b"0123456789"), // 10 bytes
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();
    let sub = BatchEngine::split_into_sub_blocks(records, 25);
    let lens: Vec<usize> = sub.iter().map(Vec::len).collect();
    assert_eq!(lens, vec![2, 2, 1], "20<=25 per block, never overshoot 25");
}

#[test]
fn split_floor_one_oversized_record() {
    // A record larger than the target is still its own sub-block (no stall).
    let records = vec![
        Record {
            payload: Bytes::from_static(b"this-payload-is-way-over-the-target"),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
        Record {
            payload: Bytes::from_static(b"small"),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
    ];
    let sub = BatchEngine::split_into_sub_blocks(records, 4);
    let lens: Vec<usize> = sub.iter().map(Vec::len).collect();
    assert_eq!(lens, vec![1, 1], "oversized record floors to one-per-block");
}

#[test]
fn split_empty_yields_no_sub_blocks() {
    let sub = BatchEngine::split_into_sub_blocks(Vec::new(), 100);
    assert_eq!(sub, [] as [std::vec::Vec<crate::Record>; 0]);
}

#[test]
fn split_smaller_than_target_is_one_sub_block() {
    let records: Vec<Record> = (0..3)
        .map(|_| Record {
            payload: Bytes::from_static(b"abc"),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();
    let sub = BatchEngine::split_into_sub_blocks(records, 10_000);
    assert_eq!(sub.len(), 1, "whole batch under target -> single sub-block");
    assert_eq!(sub[0].len(), 3);
}

/// THE peak-memory proving test: a batch of N records totalling B bytes,
/// streamed with sub_block_bytes ~= B/4. The sink samples
/// `guard.reserved_bytes()` on EACH call (the sub-block lease is held during
/// the sink); the high-water must stay at ~one sub-block, NOT the whole batch
/// B. The contrast: drive_block would peak at B.
#[cfg(feature = "memory")]
#[tokio::test]
async fn streaming_peak_lease_bounded_to_one_sub_block() {
    use crate::memory::{MemoryGuard, MemoryGuardConfig};

    // 16 records of 64 bytes each = 1024 bytes total.
    const RECORD_BYTES: usize = 64;
    const N: usize = 16;
    let total: u64 = (RECORD_BYTES * N) as u64; // 1024
    let payload = vec![b'x'; RECORD_BYTES];

    let transport = mem_transport(50);
    for _ in 0..N {
        transport.inject(None, payload.clone()).await.unwrap();
    }

    let mut engine = default_engine();
    let guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1024 * 1024,
        ..Default::default()
    }));
    engine.set_memory_guard_for_test(Arc::clone(&guard));

    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    // Sub-block target ~= B/4 -> ~256 bytes -> 4 records per sub-block.
    let sub_block_bytes = total / 4; // 256
    let one_sub_block_bytes = sub_block_bytes; // 4 records * 64 = 256

    // High-water of the guard's accounted bytes, sampled while the sub-block
    // lease is held (the sink runs inside the leased window).
    let high_water = Arc::new(AtomicU64::new(0));
    let guard_for_sink = Arc::clone(&guard);
    let hw = Arc::clone(&high_water);

    engine
        .run_workbatch_streaming(
            &transport,
            shutdown,
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| {
                let guard = Arc::clone(&guard_for_sink);
                let hw = Arc::clone(&hw);
                async move {
                    let now = guard.reserved_bytes();
                    hw.fetch_max(now, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            sub_block_bytes,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    let peak = high_water.load(Ordering::Relaxed);
    // Peak in-flight lease is ONE sub-block, never the whole batch.
    assert!(
        peak <= one_sub_block_bytes,
        "peak lease {peak} exceeded one sub-block {one_sub_block_bytes} \
             (a whole-batch lease would be {total})"
    );
    assert!(
        peak > 0 && peak < total,
        "peak {peak} must be a partial sub-block, strictly less than the \
             whole batch {total}"
    );
    // Lease fully released after the run.
    assert_eq!(guard.reserved_bytes(), 0, "all leases released after run");
}

/// A counting receiver: delegates recv/lifecycle to an inner MemoryTransport,
/// but records EACH commit call (count + the tokens + how many sink calls had
/// happened by then) so the test can prove "commit fires exactly once, after
/// the final sub-block, with all N source tokens".
struct CountingReceiver {
    inner: MemoryTransport,
    commit_calls: Arc<AtomicUsize>,
    commit_token_count: Arc<AtomicUsize>,
    sink_calls: Arc<AtomicUsize>,
    sink_calls_at_commit: Arc<AtomicUsize>,
}

impl crate::transport::TransportBase for CountingReceiver {
    fn close(
        &self,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<()>> + Send {
        self.inner.close()
    }
    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }
    fn name(&self) -> &'static str {
        self.inner.name()
    }
}

impl TransportReceiver for CountingReceiver {
    type Token = <MemoryTransport as TransportReceiver>::Token;

    fn recv(
        &self,
        max: usize,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<WorkBatch<Self::Token>>>
    + Send {
        self.inner.recv(max)
    }

    async fn commit(&self, tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        self.commit_token_count
            .fetch_add(tokens.len(), Ordering::Relaxed);
        self.sink_calls_at_commit
            .store(self.sink_calls.load(Ordering::Relaxed), Ordering::Relaxed);
        self.inner.commit(tokens).await
    }
}

/// Commit-once-after-final: N source tokens streamed across multiple
/// sub-blocks. Commit must fire EXACTLY once, AFTER the last sub-block's sink,
/// carrying ALL N source tokens (at-least-once on the whole block).
#[tokio::test]
async fn streaming_commits_once_after_final_sub_block() {
    const N: usize = 12;
    const RECORD_BYTES: usize = 32;
    let payload = vec![b'y'; RECORD_BYTES];

    let inner = mem_transport(50);
    for _ in 0..N {
        inner.inject(None, payload.clone()).await.unwrap();
    }

    let commit_calls = Arc::new(AtomicUsize::new(0));
    let commit_token_count = Arc::new(AtomicUsize::new(0));
    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sink_calls_at_commit = Arc::new(AtomicUsize::new(0));
    let receiver = CountingReceiver {
        inner,
        commit_calls: Arc::clone(&commit_calls),
        commit_token_count: Arc::clone(&commit_token_count),
        sink_calls: Arc::clone(&sink_calls),
        sink_calls_at_commit: Arc::clone(&sink_calls_at_commit),
    };

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let sc = Arc::clone(&sink_calls);
    // ~3 records per sub-block (96 bytes) -> 4 sub-blocks for 12 records.
    let sub_block_bytes = (RECORD_BYTES * 3) as u64;

    engine
        .run_workbatch_streaming(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| {
                let sc = Arc::clone(&sc);
                async move {
                    sc.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            sub_block_bytes,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    let total_sinks = sink_calls.load(Ordering::Relaxed);
    assert!(
        total_sinks >= 4,
        "expected multiple sub-block sinks, got {total_sinks}"
    );
    // Commit fired exactly ONCE.
    assert_eq!(commit_calls.load(Ordering::Relaxed), 1, "commit fires once");
    // It carried ALL N source tokens.
    assert_eq!(
        commit_token_count.load(Ordering::Relaxed),
        N,
        "commit carried all N source tokens"
    );
    // It fired AFTER the final sub-block sink (all sinks done by commit time).
    assert_eq!(
        sink_calls_at_commit.load(Ordering::Relaxed),
        total_sinks,
        "commit fired after the last sub-block sink"
    );
}

/// A sink error on a MIDDLE sub-block stops the block and skips the commit
/// (the whole block is re-delivered -- at-least-once).
#[tokio::test]
async fn streaming_mid_sub_block_sink_error_skips_commit() {
    const N: usize = 9;
    const RECORD_BYTES: usize = 32;
    let payload = vec![b'z'; RECORD_BYTES];

    let inner = mem_transport(50);
    for _ in 0..N {
        inner.inject(None, payload.clone()).await.unwrap();
    }

    let commit_calls = Arc::new(AtomicUsize::new(0));
    let commit_token_count = Arc::new(AtomicUsize::new(0));
    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sink_calls_at_commit = Arc::new(AtomicUsize::new(0));
    let receiver = CountingReceiver {
        inner,
        commit_calls: Arc::clone(&commit_calls),
        commit_token_count: Arc::clone(&commit_token_count),
        sink_calls: Arc::clone(&sink_calls),
        sink_calls_at_commit: Arc::clone(&sink_calls_at_commit),
    };

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let sc = Arc::clone(&sink_calls);
    // ~3 records per sub-block -> 3 sub-blocks; fail on the 2nd (middle).
    let sub_block_bytes = (RECORD_BYTES * 3) as u64;

    let result = engine
        .run_workbatch_streaming(
            &receiver,
            shutdown,
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| {
                let sc = Arc::clone(&sc);
                async move {
                    let nth = sc.fetch_add(1, Ordering::Relaxed) + 1;
                    if nth == 2 {
                        Err(EngineError::Sink("boom on middle sub-block".into()))
                    } else {
                        Ok(())
                    }
                }
            },
            CommitMode::Auto,
            sub_block_bytes,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    // The sink error is TERMINAL (ack barrier): the run returns the error.
    assert!(
        matches!(result, Err(EngineError::Sink(_))),
        "mid sub-block sink error is terminal, got {result:?}"
    );
    // The block stopped at the failing sub-block: no commit, and the 3rd
    // sub-block was never sunk.
    assert_eq!(
        commit_calls.load(Ordering::Relaxed),
        0,
        "mid sub-block sink error must skip commit"
    );
    assert_eq!(
        sink_calls.load(Ordering::Relaxed),
        2,
        "stopped after the failing 2nd sub-block (3rd never sunk)"
    );
}

/// Floor case: a batch smaller than sub_block_bytes streams as ONE sub-block
/// and behaves like drive_block (all records sunk once, commit once).
#[tokio::test]
async fn streaming_small_batch_is_single_sub_block() {
    let transport = mem_transport(50);
    for i in 0..3u64 {
        transport
            .inject(None, format!(r#"{{"id":{i}}}"#).into_bytes())
            .await
            .unwrap();
    }

    let engine = default_engine();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sink_records = Arc::new(AtomicUsize::new(0));
    let scz = Arc::clone(&sink_calls);
    let srz = Arc::clone(&sink_records);

    engine
        .run_workbatch_streaming(
            &transport,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let scz = Arc::clone(&scz);
                let srz = Arc::clone(&srz);
                let n = out.records.len();
                async move {
                    scz.fetch_add(1, Ordering::Relaxed);
                    srz.fetch_add(n, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            10_000, // target far larger than the whole batch
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(
        sink_calls.load(Ordering::Relaxed),
        1,
        "under-target batch sinks once (single sub-block)"
    );
    assert_eq!(
        sink_records.load(Ordering::Relaxed),
        3,
        "all 3 records sunk"
    );
    assert_eq!(
        transport.committed_sequence(),
        2,
        "all 3 acks committed once"
    );
}

/// The streaming path cannot honour `SinkManaged`: its sub-block views carry
/// EMPTY commit tokens, so the sink never sees the block's source acks and
/// physically cannot own the commit. The driver must fail fast at startup
/// rather than silently commit nothing and freeze the source offset.
#[tokio::test]
async fn streaming_rejects_sink_managed_commit() {
    let transport = mem_transport(50);
    let engine = default_engine();
    let shutdown = CancellationToken::new();

    let result = engine
        .run_workbatch_streaming(
            &transport,
            shutdown,
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| async move { Ok(()) },
            CommitMode::SinkManaged,
            10_000,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(
        matches!(result, Err(EngineError::SinkManagedUnsupported)),
        "SinkManaged on the streaming path must fail fast, got {result:?}"
    );
}

// ---- Phase 3: governed run path (default-on self-regulation) ----------

/// Build a real governor over a MemoryGuard and wire its byte budget into
/// the engine, returning (engine, governor) so the test can inspect both.
#[cfg(feature = "governor")]
fn governed_engine() -> (BatchEngine, crate::governor::SelfRegulationGovernor) {
    use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
    // Pinned to the reservation counter so pressure is what the test drives,
    // not the host's own memory usage.
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 1024 * 1024,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let gov = crate::governor::SelfRegulationConfig::default()
        .build(guard)
        .expect("enabled by default");
    let mut engine = default_engine();
    engine.set_byte_budget(gov.budget());
    (engine, gov)
}

/// Governor ON: the governed driver streams the input end-to-end through a
/// MemoryTransport, all records reach the sink, the source acks commit, and
/// the AIMD budget moves (observe is folded in per block).
#[cfg(feature = "governor")]
#[tokio::test]
async fn governed_on_streams_and_commits_via_memory_transport() {
    let transport = mem_transport(50);
    for i in 0..6u64 {
        transport
            .inject(None, format!(r#"{{"id":{i}}}"#).into_bytes())
            .await
            .unwrap();
    }

    let (engine, _gov) = governed_engine();
    assert!(engine.is_self_regulated(), "budget wired -> governed path");

    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let sink_records = Arc::new(AtomicUsize::new(0));
    let sr = Arc::clone(&sink_records);

    engine
        .run_governed(
            &transport,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let sr = Arc::clone(&sr);
                let n = out.records.len();
                async move {
                    sr.fetch_add(n, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(
        sink_records.load(Ordering::Relaxed),
        6,
        "all records streamed to the sink under the governor"
    );
    assert_eq!(transport.committed_sequence(), 5, "all 6 acks committed");
}

/// Governor OFF: with no byte budget wired, run_governed delegates to the
/// whole-batch run_workbatch -- behaviour is unchanged (one sink call for
/// the whole block, commit once).
#[cfg(feature = "governor")]
#[tokio::test]
async fn governed_off_is_whole_batch_passthrough() {
    let transport = mem_transport(50);
    for i in 0..4u64 {
        transport
            .inject(None, format!(r#"{{"id":{i}}}"#).into_bytes())
            .await
            .unwrap();
    }

    // No set_byte_budget -> byte_budget is None -> OFF path.
    let engine = default_engine();
    assert!(
        !engine.is_self_regulated(),
        "no budget wired -> whole-batch path"
    );

    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sink_records = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&sink_calls);
    let sr = Arc::clone(&sink_records);

    engine
        .run_governed(
            &transport,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let sc = Arc::clone(&sc);
                let sr = Arc::clone(&sr);
                let n = out.records.len();
                async move {
                    sc.fetch_add(1, Ordering::Relaxed);
                    sr.fetch_add(n, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(
        sink_calls.load(Ordering::Relaxed),
        1,
        "OFF path = whole-batch: the block sinks ONCE (not per sub-block)"
    );
    assert_eq!(sink_records.load(Ordering::Relaxed), 4, "all records sunk");
    assert_eq!(transport.committed_sequence(), 3, "all 4 acks committed");
}

/// The shared pressure feeds an InboundGate: under high memory the gate
/// holds (Admit::Hold) and the budget shrinks; low memory admits and the
/// budget sits at start-big. Proves the gate + budget share one pressure.
#[cfg(feature = "governor")]
#[test]
fn governed_gate_and_budget_share_pressure() {
    use crate::governor::{Admit, InboundGate, NoopActuator};
    use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

    // Pinned to the reservation counter so pressure is what the test drives,
    // not the host's own memory usage.
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 1000,
            pressure_threshold: 0.80,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let gov = crate::governor::SelfRegulationConfig::default()
        .build(Arc::clone(&guard))
        .expect("enabled");

    let gate = InboundGate::new(gov.pressure(), Box::new(NoopActuator));
    let budget = gov.budget();
    let start = budget.byte_budget();

    // Low memory -> gate admits, budget unchanged on a slack observe.
    assert_eq!(gate.evaluate(), Admit::Yes, "low pressure admits");

    // Slam memory high -> the SAME pressure both holds the gate AND, through
    // observe(), shrinks the budget.
    guard.add_bytes(950); // 95% of limit
    assert_eq!(gate.evaluate(), Admit::Hold, "high pressure holds the gate");
    budget.observe(0, Duration::from_millis(1), Duration::from_millis(100));
    assert!(
        budget.byte_budget() < start,
        "high memory shrinks the shared budget (HARD override)"
    );
}

// ---- Phase 4: validation ---------------------------------------------

/// THE send-unaffected invariant: the OUTBOUND drain (sink) is NEVER gated
/// by pressure -- only the INBOUND recv side is. With a `UnifiedPressure`
/// pinned HARD-HIGH so `should_hold()` is true, the SAME transport's
/// `send` / `send_batch` still succeed. Gating the drain would deadlock the
/// pipeline (in-flight work could never leave), so the governor must never
/// touch it. MemoryTransport's send path consults no pressure governor by
/// construction; this test proves that holds even when a governor that the
/// inbound side WOULD obey is wired and saturated.
#[cfg(feature = "governor")]
#[tokio::test]
async fn send_unaffected_by_pressure_pinned_high() {
    use crate::governor::{Hysteresis, MemoryPressureSource, PressureSource, UnifiedPressure};
    use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
    use crate::transport::TransportSender;

    // Pin a REAL HARD memory source high so the latch holds (>= pause_above).
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 1000,
            pressure_threshold: 0.80,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    guard.add_bytes(950); // 95% -> HARD high
    let pressure = Arc::new(UnifiedPressure::new(
        vec![Arc::new(MemoryPressureSource::new(Arc::clone(&guard))) as Arc<dyn PressureSource>],
        Hysteresis::new(0.80, 0.65).expect("valid band"),
    ));
    assert!(
        pressure.should_hold(),
        "pinned-high governor must hold the INBOUND gate"
    );

    // The OUTBOUND sink: send / send_batch must still succeed under hold.
    let transport = mem_transport(50);

    let single = transport
        .send("k", Bytes::from_static(br#"{"id":1}"#))
        .await;
    assert!(
        single.is_ok(),
        "single send must succeed under pressure (sink never gated), got {single:?}"
    );

    let records: Vec<Record> = (0..5)
        .map(|i| Record {
            payload: Bytes::from(format!(r#"{{"id":{i}}}"#)),
            key: Some(Arc::from(format!("k{i}").as_str())),
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();
    let batch_res = transport.send_batch(&records).await;
    assert!(
        batch_res.is_ok(),
        "send_batch must succeed under pressure (sink never gated), got {batch_res:?}"
    );

    // Pressure is STILL high after the sends -- nothing about the send path
    // cleared or consulted it.
    assert!(
        pressure.should_hold(),
        "send does not touch the pressure latch"
    );

    // And the sent data is intact on the wire (the drain really ran).
    let got = transport.recv(10).await.unwrap().records;
    assert_eq!(got.len(), 6, "1 single + 5 batched records all drained");
}

/// Build a governed engine over a guard with a LOW limit, sharing ONE guard
/// between the governor (pressure + budget) and the engine's ingress-lease
/// accounting. Returns `(engine, governor, guard)`.
#[cfg(all(feature = "governor", feature = "memory"))]
fn governed_engine_low_limit(
    limit_bytes: u64,
) -> (
    BatchEngine,
    crate::governor::SelfRegulationGovernor,
    Arc<crate::memory::MemoryGuard>,
) {
    use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};
    // Pinned to the reservation counter: the invariants here are about the
    // in-flight lease against an 18 KiB synthetic limit, not the host's usage.
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes,
            pressure_threshold: 0.80,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    // The governor's pressure + AIMD budget run off THIS guard.
    let gov = crate::governor::SelfRegulationConfig::default()
        .build(Arc::clone(&guard))
        .expect("enabled by default");
    // A SMALL recv chunk so the load arrives over many blocks: the memory
    // override shrinks the budget block-to-block as pressure builds, rather
    // than pulling the whole load in one cold-budget block. This is the realistic streaming shape -- a real broker/source
    // delivers in poll-sized chunks, not one giant block.
    let mut engine = BatchEngine::new(BatchProcessingConfig {
        max_chunk_size: 16,
        ..Default::default()
    });
    engine.set_byte_budget(gov.budget());
    // The engine's ingress leases must account against the SAME guard so the
    // streaming peak-lease feeds back into the pressure the budget reads.
    engine.set_memory_guard_for_test(Arc::clone(&guard));
    (engine, gov, guard)
}

/// THE operational never-OOM test (in-process logical form).
///
/// Drives sustained, large load through `run_governed` over a real
/// `MemoryTransport`, governor ON, with a `MemoryGuard` on a LOW limit. It
/// proves the four never-OOM invariants without a cgroup harness:
///
///   1. the inbound GATE engages -- with the governor's pressure pinned by
///      sustained ingress, an `InboundGate` over the SAME pressure returns
///      `Admit::Hold` (the brake the transport would apply);
///   2. the sink/drain KEEPS RUNNING -- every record reaches the sink and
///      the source acks commit (the drain is never gated);
///   3. `MemoryGuard::reserved_bytes()` stays BOUNDED -- the streaming
///      peak-lease holds at most ~one shrunk sub-block in flight, well under
///      the whole-batch footprint, sampled at its high-water inside the sink;
///   4. the pipeline does NOT panic and the budget never collapses below its
///      floor (>= 1, never 0).
///
/// A full OS-level cgroup OOM-kill test (a memory-limited container + a real
/// broker or transport under load) needs a CI harness and is not covered here.
#[cfg(all(feature = "governor", feature = "memory"))]
#[tokio::test]
async fn operational_never_oom_governed_pipeline_bounds_memory() {
    use crate::governor::{Admit, InboundGate, NoopActuator};

    // LOW limit, sized so a SINGLE in-flight poll-chunk (16 x 1 KiB =
    // 16 KiB) sits above the 80% pressure threshold (16/18 ~= 0.89), so the
    // gate engages while a sub-block is leased -- yet the streaming
    // peak-lease keeps the in-flight footprint at one chunk, never the whole
    // load. This is the never-OOM shape: high pressure brakes inbound, but
    // memory stays bounded because only one sub-block is ever resident.
    const LIMIT: u64 = 18 * 1024; // 18 KiB
    // Records far larger than the floor; many of them -> sustained load.
    const RECORD_BYTES: usize = 1024; // 1 KiB each
    const N: usize = 256; // 256 KiB of payload total -- 14x the limit
    let payload = vec![b'q'; RECORD_BYTES];
    let total_payload: u64 = (RECORD_BYTES * N) as u64;

    let transport = mem_transport(50);
    for _ in 0..N {
        transport.inject(None, payload.clone()).await.unwrap();
    }

    let (engine, gov, guard) = governed_engine_low_limit(LIMIT);
    assert!(engine.is_self_regulated(), "budget wired -> governed path");

    // The gate the transport WOULD wire in, over the governor's shared
    // pressure. We evaluate it from inside the sink to observe the brake.
    let gate = Arc::new(InboundGate::new(gov.pressure(), Box::new(NoopActuator)));

    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 600);

    let sink_records = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicU64::new(0));
    let gate_held_ever = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let sr = Arc::clone(&sink_records);
    let hw = Arc::clone(&high_water);
    let geh = Arc::clone(&gate_held_ever);
    let guard_for_sink = Arc::clone(&guard);
    let gate_for_sink = Arc::clone(&gate);

    engine
        .run_governed(
            &transport,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                let sr = Arc::clone(&sr);
                let hw = Arc::clone(&hw);
                let geh = Arc::clone(&geh);
                let guard = Arc::clone(&guard_for_sink);
                let gate = Arc::clone(&gate_for_sink);
                let n = out.records.len();
                async move {
                    // (3) sample reserved_bytes() while the sub-block lease is
                    // held -- this is the in-flight high-water.
                    hw.fetch_max(guard.reserved_bytes(), Ordering::Relaxed);
                    // (1) evaluate the gate over the SAME pressure: under
                    // sustained ingress it engages (Hold).
                    if gate.evaluate() == Admit::Hold {
                        geh.store(true, Ordering::Relaxed);
                    }
                    // (2) the drain keeps running -- count every record sunk.
                    sr.fetch_add(n, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    // (2) The drain KEPT RUNNING: every record reached the sink and the
    // source acks committed -- the sink is never gated.
    assert_eq!(
        sink_records.load(Ordering::Relaxed),
        N,
        "all {N} records drained through the governed sink"
    );
    assert_eq!(
        transport.committed_sequence(),
        (N - 1) as u64,
        "all source acks committed (drain never stalled)"
    );

    // (1) The inbound gate ENGAGED at least once under the sustained load --
    // the brake the transport would apply did fire.
    assert!(
        gate_held_ever.load(Ordering::Relaxed),
        "inbound gate must engage (Admit::Hold) under sustained pressure"
    );

    // (3) Peak in-flight bytes stayed BOUNDED, NOT the whole payload. The
    // streaming peak-lease bounds it to ~one shrunk sub-block; allow generous
    // headroom but it must be a small fraction of the whole-batch footprint.
    let peak = high_water.load(Ordering::Relaxed);
    assert!(
        peak > 0,
        "some bytes must be accounted while a sub-block is in flight"
    );
    assert!(
        peak < total_payload / 2,
        "peak in-flight {peak} must stay well under half the whole payload \
             {total_payload} (streaming peak-lease bounds it, never OOM)"
    );

    // (4) Budget respected its floor (>= 1, never 0) and the run did not
    // panic (reaching here proves it). All leases released after the run.
    assert!(
        gov.budget().byte_budget() >= 1,
        "byte budget never collapses below its floor"
    );
    assert_eq!(
        guard.reserved_bytes(),
        0,
        "all ingress leases released after the run -- no leak"
    );
}

// ---- Byte-aware recv bounds RECEIVE memory ----------------------------
//
// The gap: the governed driver bounds memory by the
// post-recv SUB-BLOCK lease, but `recv(max)` is RECORD-bounded only -- a
// single poll can build a WorkBatch whose total bytes >> byte_budget BEFORE
// any sub-block split, so the byte budget did NOT bound RECEIVE memory. The
// fix routes the governed recv through `recv_limited(RecvLimits)` so the poll
// is bounded by BOTH the record cap AND the byte budget.

/// A REAL test transport (not a mock of internal code -- a concrete
/// `TransportReceiver` over owned `Record`/`WorkBatch`/`MemoryToken`) that
/// makes the gap observable:
///
/// - `recv(max)` is RECORD-bounded: it hands out up to `max` records in ONE
///   block regardless of their bytes -- exactly the pre-fix behaviour that
///   let a single poll retain bytes >> budget.
/// - `recv_limited(limits)` is BYTE-bounded: it accumulates records until the
///   payload bytes reach `limits.max_bytes`, FLOOR one record.
///
/// Every handed-out block's total payload bytes are folded into a shared
/// high-water so the test can assert the bytes RETAINED at recv time.
struct ByteAwareSource {
    /// Remaining records to hand out (front = next).
    remaining: std::sync::Mutex<std::collections::VecDeque<Record>>,
    /// High-water of the bytes handed out in any single recv/recv_limited.
    recv_high_water: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
    /// Set by `close()`; `recv` then reports `Closed`, as Kafka does.
    closed: std::sync::atomic::AtomicBool,
}

impl ByteAwareSource {
    fn new(records: Vec<Record>, recv_high_water: Arc<AtomicU64>) -> Self {
        Self {
            remaining: std::sync::Mutex::new(records.into_iter().collect()),
            recv_high_water,
            committed: Arc::new(AtomicU64::new(0)),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Pull a block (front records) bounded by an optional byte cap and a
    /// record cap, folding its total bytes into the high-water. Returns
    /// `None` when the source is exhausted (the caller then PENDS forever so
    /// the run loop parks until shutdown -- never a busy spin).
    fn pull(&self, max_records: usize, max_bytes: Option<u64>) -> Option<WorkBatch<MemTok2>> {
        let mut q = self.remaining.lock().unwrap();
        if q.is_empty() {
            return None;
        }
        let mut records = Vec::new();
        let mut bytes: u64 = 0;
        while records.len() < max_records {
            let Some(front) = q.front() else { break };
            let rb = front.payload.len() as u64;
            // Byte cap with floor-1: stop only once we already hold >= 1.
            if let Some(cap) = max_bytes
                && !records.is_empty()
                && bytes.saturating_add(rb) > cap
            {
                break;
            }
            bytes = bytes.saturating_add(rb);
            records.push(q.pop_front().expect("front exists"));
        }
        self.recv_high_water.fetch_max(bytes, Ordering::Relaxed);
        let n = records.len() as u64;
        let base = self.committed.load(Ordering::Relaxed);
        let tokens: Vec<MemTok2> = (0..n).map(|i| MemTok2 { seq: base + i }).collect();
        Some(WorkBatch::new(records, tokens))
    }

    /// [`pull`](Self::pull), or `Closed` once `close()` has run.
    fn pull_unless_closed(
        &self,
        max_records: usize,
        max_bytes: Option<u64>,
    ) -> crate::transport::TransportResult<Option<WorkBatch<MemTok2>>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(crate::transport::TransportError::Closed);
        }
        Ok(self.pull(max_records, max_bytes))
    }
}

#[derive(Debug, Clone, Copy)]
struct MemTok2 {
    seq: u64,
}
impl std::fmt::Display for MemTok2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memtok2:{}", self.seq)
    }
}
impl crate::transport::CommitToken for MemTok2 {}

impl crate::transport::TransportBase for ByteAwareSource {
    fn close(
        &self,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<()>> + Send {
        self.closed.store(true, Ordering::Relaxed);
        std::future::ready(Ok(()))
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "byte-aware-source"
    }
}

impl TransportReceiver for ByteAwareSource {
    type Token = MemTok2;

    fn recv(
        &self,
        max: usize,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<WorkBatch<Self::Token>>>
    + Send {
        // RECORD-bounded only -- ignores bytes. This is the pre-fix shape: a
        // single poll can retain bytes >> any budget.
        let pulled = self.pull_unless_closed(max, None);
        async move {
            match pulled? {
                Some(batch) => Ok(batch),
                // Exhausted: park forever so the loop only exits on shutdown
                // (mirrors a quiet source -- never a busy spin).
                None => std::future::pending().await,
            }
        }
    }

    fn recv_limited(
        &self,
        limits: crate::transport::RecvLimits,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<WorkBatch<Self::Token>>>
    + Send {
        // BYTE-bounded (floor one record): the fix path.
        let pulled = self.pull_unless_closed(limits.max_records, Some(limits.max_bytes));
        async move {
            match pulled? {
                Some(batch) => Ok(batch),
                None => std::future::pending().await,
            }
        }
    }

    async fn commit(&self, tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
        if let Some(max_seq) = tokens.iter().map(|t| t.seq).max() {
            self.committed.fetch_max(max_seq, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// THE reproduce/fix test: drive the GOVERNED loop over a source that could
/// deliver a block whose total bytes are FAR larger than the byte budget.
///
/// PRE-FIX (governed recv == `recv(record_cap)`): the source's record-bounded
/// `recv` hands out the whole big block in one poll, so the bytes RETAINED at
/// recv time = the whole block >> budget. The high-water assertion below
/// FAILS (this is the reproduction).
///
/// POST-FIX (governed recv == `recv_limited(record_cap, byte_budget)`): the
/// source's byte-bounded `recv_limited` caps each poll at the budget (+ one
/// record), so the retained bytes stay ~<= budget + one record.
#[cfg(feature = "governor")]
#[tokio::test]
async fn governed_recv_is_byte_bounded_not_record_bounded() {
    use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

    // 64 records of 4 KiB each = 256 KiB total available in the source.
    const RECORD_BYTES: usize = 4 * 1024;
    const N: usize = 64;
    // A SMALL byte budget: 16 KiB (4 records). The record cap is large (2000
    // default) so the count NEVER bounds the poll -- only the byte cap can.
    const BUDGET: u64 = 16 * 1024;

    let total: u64 = (RECORD_BYTES * N) as u64; // 256 KiB
    let payload = vec![b'b'; RECORD_BYTES];
    let records: Vec<Record> = (0..N)
        .map(|_| Record {
            payload: Bytes::from(payload.clone()),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();

    // Pinned to the reservation counter: this test bounds the recv by bytes,
    // and the host's own memory usage must not move the budget under it.
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: 1024 * 1024,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    let cfg = crate::governor::ByteBudgetConfig {
        start_bytes: BUDGET,
        max_bytes: BUDGET, // pin it so the budget cannot grow past BUDGET
        floor_records: 1,
        nominal_record_bytes: RECORD_BYTES as u64,
        record_cap: 4096, // far above N -- count never bounds the poll
        ..Default::default()
    };
    let pressure = crate::governor::SelfRegulationConfig::default()
        .build(Arc::clone(&guard))
        .expect("enabled")
        .pressure();
    let budget = Arc::new(crate::governor::ByteBudgetController::new(
        cfg,
        Arc::clone(&pressure),
    ));

    let recv_high_water = Arc::new(AtomicU64::new(0));
    let source = ByteAwareSource::new(records, Arc::clone(&recv_high_water));

    let mut engine = BatchEngine::new(BatchProcessingConfig {
        // Big chunk so config never bounds the poll either -- the byte budget
        // is the ONLY thing that can.
        max_chunk_size: 4096,
        ..Default::default()
    });
    engine.set_byte_budget(budget);

    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 250);

    engine
        .run_governed(
            &source,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    let peak = recv_high_water.load(Ordering::Relaxed);
    // The fix: a single governed recv retains at most the byte budget plus
    // one oversized-record floor -- NOT the whole 256 KiB block.
    assert!(
        peak <= BUDGET + RECORD_BYTES as u64,
        "governed recv retained {peak} bytes at recv time -- must be bounded \
             by the byte budget {BUDGET} (+ one record {RECORD_BYTES}), not the \
             whole {total}-byte block (record-bounded recv would retain all of it)"
    );
    assert!(
        peak > 0,
        "the source did hand out records (sanity: the loop ran)"
    );
}

/// The sub-block drain is LAZY: it yields one sub-block at a time and does
/// NOT allocate every sub-block up front. We assert incremental yield -- the
/// first `next_sub_block()` returns one budget-sized sub-block while records
/// for later sub-blocks remain un-pulled in the drain.
#[test]
fn sub_block_drain_yields_incrementally() {
    // 6 records of 10 bytes; target 25 -> sub-blocks {2, 2, 2}.
    let records: Vec<Record> = (0..6)
        .map(|_| Record {
            payload: Bytes::from_static(b"0123456789"),
            key: None,
            headers: vec![],
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();
    let mut drain = SubBlockDrain::new(records, 25);

    // First pull yields ONE sub-block (2 records); the remaining 4 are still
    // inside the drain, NOT pre-materialised into sub-block vectors.
    let first = drain.next_sub_block().expect("first sub-block");
    assert_eq!(first.len(), 2, "first sub-block is one budget's worth");
    // The drain still has records to give (proves it did not eagerly split).
    let second = drain.next_sub_block().expect("second sub-block");
    assert_eq!(second.len(), 2);
    let third = drain.next_sub_block().expect("third sub-block");
    assert_eq!(third.len(), 2);
    // Now exhausted.
    assert!(drain.next_sub_block().is_none(), "drain exhausted");
}

// ---- The byte budget under a backlog -----------------------------------

/// Payload bytes of each backlog record: four of them fill the 1 KiB floor.
#[cfg(feature = "governor")]
const BACKLOG_RECORD_BYTES: usize = 256;

/// The budget's record cap, so one full block.
#[cfg(feature = "governor")]
const BACKLOG_RECORD_CAP: usize = 64;

/// Records waiting at the source: ten full blocks.
#[cfg(feature = "governor")]
const BACKLOG_RECORDS: usize = BACKLOG_RECORD_CAP * 10;

/// The memory limit the backlog engine's pressure reads against.
#[cfg(feature = "governor")]
const BACKLOG_MEMORY_LIMIT: u64 = 1024 * 1024;

/// A sink call's future, boxed so one helper serves every run path.
#[cfg(feature = "governor")]
type BacklogSinkFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EngineError>> + Send>>;

/// A memory transport holding the whole backlog before the run starts.
#[cfg(feature = "governor")]
async fn backlog_source() -> MemoryTransport {
    let transport = MemoryTransport::new(&MemoryConfig {
        buffer_size: BACKLOG_RECORDS,
        recv_timeout_ms: 50,
        ..Default::default()
    })
    .expect("memory transport with valid config must construct");
    for _ in 0..BACKLOG_RECORDS {
        transport
            .inject(None, vec![b'r'; BACKLOG_RECORD_BYTES])
            .await
            .unwrap();
    }
    transport
}

/// A governed engine whose byte budget starts at one full block, over a memory
/// guard already holding `memory_used` bytes of its limit.
#[cfg(feature = "governor")]
fn backlog_engine(memory_used: u64) -> (BatchEngine, Arc<crate::governor::ByteBudgetController>) {
    use crate::memory::{MemoryGuard, MemoryGuardConfig, UsageSource};

    // Pinned to the reservation counter so the pressure is `memory_used`, not the host's.
    let guard = Arc::new(MemoryGuard::with_usage_source(
        MemoryGuardConfig {
            limit_bytes: BACKLOG_MEMORY_LIMIT,
            ..Default::default()
        },
        UsageSource::Reservations,
    ));
    guard.add_bytes(memory_used);
    let pressure = crate::governor::SelfRegulationConfig::default()
        .build(guard)
        .expect("enabled by default")
        .pressure();
    let full_block = (BACKLOG_RECORD_BYTES * BACKLOG_RECORD_CAP) as u64;
    let budget = Arc::new(crate::governor::ByteBudgetController::new(
        crate::governor::ByteBudgetConfig {
            start_bytes: full_block,
            max_bytes: full_block * 4,
            ai_step: full_block / 4,
            record_cap: BACKLOG_RECORD_CAP,
            ..Default::default()
        },
        pressure,
    ));
    let mut engine = default_engine();
    engine.set_byte_budget(Arc::clone(&budget));
    (engine, budget)
}

/// A sink that takes `per_call` to deliver, records how many records each call
/// carried, and stops the run once the whole backlog is delivered.
#[cfg(feature = "governor")]
fn backlog_sink(
    per_call: Duration,
    calls: &Arc<parking_lot::Mutex<Vec<usize>>>,
    shutdown: &CancellationToken,
) -> impl FnMut(&WorkBatch<crate::transport::memory::MemoryToken>) -> BacklogSinkFuture {
    let calls = Arc::clone(calls);
    let shutdown = shutdown.clone();
    move |out| {
        let mut calls = calls.lock();
        calls.push(out.records.len());
        if calls.iter().sum::<usize>() >= BACKLOG_RECORDS {
            shutdown.cancel();
        }
        Box::pin(async move {
            tokio::time::sleep(per_call).await;
            Ok(())
        })
    }
}

/// Every backlog record was delivered, at an average of at least half a full
/// block per sink call.
#[cfg(feature = "governor")]
fn assert_full_blocks(calls: &[usize]) {
    let delivered: usize = calls.iter().sum();
    assert_eq!(delivered, BACKLOG_RECORDS, "the whole backlog is delivered");
    let average = delivered / calls.len();
    assert!(
        average >= BACKLOG_RECORD_CAP / 2,
        "{average} records a sink call, from calls {calls:?}: with no memory pressure the budget \
         must keep blocks near the {BACKLOG_RECORD_CAP}-record cap, not shrink them to the floor"
    );
}

/// A backlog behind a slow sink, with no memory pressure, keeps full blocks
/// through the pipeline. A busy stage is not a reason to shrink the budget:
/// under a backlog it is busy at every block size.
#[cfg(feature = "governor")]
#[tokio::test]
async fn a_pipeline_backlog_behind_a_slow_sink_keeps_full_blocks() {
    let source = backlog_source().await;
    let (engine, budget) = backlog_engine(0);
    let start = budget.byte_budget();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 20_000);
    let calls = Arc::new(parking_lot::Mutex::new(Vec::new()));

    engine
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .sink_confirms(SinkConfirmation::Remote)
        .run(
            |batch| Ok(batch),
            backlog_sink(Duration::from_millis(20), &calls, &shutdown),
        )
        .await
        .expect("clean shutdown");

    assert_full_blocks(&calls.lock());
    assert!(
        budget.byte_budget() > start,
        "the budget grows without memory pressure"
    );
}

/// The same backlog through `run_governed` keeps full blocks too.
#[cfg(feature = "governor")]
#[tokio::test]
async fn a_governed_backlog_behind_a_slow_sink_keeps_full_blocks() {
    let source = backlog_source().await;
    let (engine, budget) = backlog_engine(0);
    let start = budget.byte_budget();
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 20_000);
    let calls = Arc::new(parking_lot::Mutex::new(Vec::new()));

    engine
        .run_governed(
            &source,
            shutdown.clone(),
            |batch| Ok(batch),
            backlog_sink(Duration::from_millis(20), &calls, &shutdown),
            CommitMode::Auto,
            no_ticker(),
        )
        .await
        .expect("clean shutdown");

    assert_full_blocks(&calls.lock());
    assert!(
        budget.byte_budget() > start,
        "the budget grows without memory pressure"
    );
}

/// Memory pressure still brakes the pipeline: with the latch held, the budget
/// halves every block down to its 1 KiB floor, and the sink calls shrink with
/// it to four records.
#[cfg(feature = "governor")]
#[tokio::test]
async fn memory_pressure_still_shrinks_pipeline_blocks_to_the_floor() {
    let source = backlog_source().await;
    // 95% of the limit, above pause_above (0.80), so the latch holds.
    let (engine, budget) = backlog_engine(BACKLOG_MEMORY_LIMIT * 95 / 100);
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 20_000);
    let calls = Arc::new(parking_lot::Mutex::new(Vec::new()));

    engine
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .sink_confirms(SinkConfirmation::Remote)
        .run(
            |batch| Ok(batch),
            backlog_sink(Duration::ZERO, &calls, &shutdown),
        )
        .await
        .expect("clean shutdown");

    let calls = calls.lock();
    assert_eq!(calls.iter().sum::<usize>(), BACKLOG_RECORDS);
    assert_eq!(
        calls[0], BACKLOG_RECORD_CAP,
        "the first block is sized to the start budget"
    );
    assert_eq!(
        budget.byte_budget(),
        1024,
        "the latch drives the budget to its floor"
    );
    assert!(
        calls
            .last()
            .is_some_and(|&n| n <= 1024 / BACKLOG_RECORD_BYTES),
        "the last sink calls carry the floor's four records: {calls:?}"
    );
}

// ---- DLQ + parse-error-action semantics -------------------------------
//
// Two findings the parsed/process paths had:
//   1. parse_block hardcoded route-to-DLQ, ignoring ParseErrorAction.
//   2. out_batch.dlq_entries from process were never routed before commit
//      (silent-drop path) -- only inbound-filter entries were routed.
// These tests pin the fixed contract: one route point, one policy, fallible
// route, parse_error_action honoured on the parsed path.

use crate::worker::engine::FilterDlqPolicy;
use crate::worker::engine::config::ParseErrorAction;

/// An engine with a specific `ParseErrorAction` (default config otherwise).
fn engine_with_parse_action(action: ParseErrorAction) -> BatchEngine {
    BatchEngine::new(BatchProcessingConfig {
        parse_error_action: action,
        ..Default::default()
    })
}

/// Finding 1 -- `ParseErrorAction::Skip`: a parse failure on the parsed path
/// is DROPPED silently (NO DLQ entry routed) yet the survivors are kept and
/// ALL source acks commit (the block's tokens are decoupled from records).
#[tokio::test]
async fn parsed_parse_error_skip_drops_without_dlq_and_commits_survivors() {
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"id":1}"#.to_vec())
        .await
        .unwrap(); // seq 0 ok
    transport
        .inject(None, b"not json {{{".to_vec())
        .await
        .unwrap(); // seq 1 bad
    transport
        .inject(None, br#"{"id":3}"#.to_vec())
        .await
        .unwrap(); // seq 2 ok

    // Route policy so we can PROVE no entry is routed under Skip.
    let routed = Arc::new(AtomicUsize::new(0));
    let rc = Arc::clone(&routed);
    let engine = engine_with_parse_action(ParseErrorAction::Skip).with_filter_dlq_policy(
        FilterDlqPolicy::Route(Arc::new(
            move |entries: Vec<crate::transport::filter::FilteredDlqEntry>| {
                rc.fetch_add(entries.len(), Ordering::Relaxed);
                Ok(())
            },
        )),
    );
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    let dlq_seen = Arc::new(AtomicUsize::new(0));
    let kept = Arc::new(AtomicUsize::new(0));
    let ds = Arc::clone(&dlq_seen);
    let kp = Arc::clone(&kept);

    engine
        .run_workbatch_parsed(
            &transport,
            shutdown,
            move |pb: ParsedBatch<'_, _>| {
                ds.fetch_add(pb.dlq_entries.len(), Ordering::Relaxed);
                kp.fetch_add(pb.records.len(), Ordering::Relaxed);
                Ok(WorkBatch::new(pb.records, pb.commit_tokens).with_dlq_entries(pb.dlq_entries))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(kept.load(Ordering::Relaxed), 2, "2 survivors kept");
    assert_eq!(
        dlq_seen.load(Ordering::Relaxed),
        0,
        "Skip: parse failure produces NO DLQ entry (dropped, not dead-lettered)"
    );
    assert_eq!(
        routed.load(Ordering::Relaxed),
        0,
        "Skip: nothing reaches the DLQ route point"
    );
    // All three source acks committed -- survivors and the dropped record's
    // ack alike (at-least-once on the whole block; Skip is opt-in loss).
    assert_eq!(transport.committed_sequence(), 2);
}

/// Finding 1 -- `ParseErrorAction::FailBatch`: a parse failure fails the
/// WHOLE block terminally (no commit), consistent with the ack barrier. The
/// run returns the terminal error and the source watermark does not advance.
#[tokio::test]
async fn parsed_parse_error_fail_batch_skips_commit() {
    // OrderedReceiver hands one record per recv with monotonic tokens and a
    // cumulative watermark, so we can prove the commit never fired.
    let receiver = OrderedReceiverBad::new();
    let committed = Arc::clone(&receiver.committed_hwm);

    let engine = engine_with_parse_action(ParseErrorAction::FailBatch);
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 500);

    let sink_calls = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&sink_calls);

    let result = engine
        .run_workbatch_parsed(
            &receiver,
            shutdown,
            |pb: ParsedBatch<'_, _>| {
                Ok(WorkBatch::new(pb.records, pb.commit_tokens).with_dlq_entries(pb.dlq_entries))
            },
            move |_out: &WorkBatch<_>| {
                let sc = Arc::clone(&sc);
                async move {
                    sc.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(
        matches!(result, Err(EngineError::ParseBatchFailed(_))),
        "FailBatch: a parse failure is a terminal engine error, got {result:?}"
    );
    assert_eq!(
        committed.load(Ordering::Relaxed),
        u64::MAX,
        "FailBatch: the whole block fails its commit -- watermark unmoved"
    );
    assert_eq!(
        sink_calls.load(Ordering::Relaxed),
        0,
        "FailBatch: the block never reaches the sink (parse fails first)"
    );
}

/// Finding 1 -- `ParseErrorAction::Dlq`: a parse failure routes to the DLQ
/// route point BEFORE commit, survivors are sunk, all source acks commit.
#[tokio::test]
async fn parsed_parse_error_dlq_routes_before_commit() {
    let transport = Arc::new(mem_transport(50));
    transport
        .inject(None, br#"{"id":1}"#.to_vec())
        .await
        .unwrap(); // seq 0 ok
    transport
        .inject(None, b"not json {{{".to_vec())
        .await
        .unwrap(); // seq 1 bad
    transport
        .inject(None, br#"{"id":3}"#.to_vec())
        .await
        .unwrap(); // seq 2 ok

    // Sample committed_sequence at DLQ-route time to prove route precedes
    // commit: when the route sink fires, the commit must NOT yet have run.
    let routed = Arc::new(AtomicUsize::new(0));
    let committed_at_route = Arc::new(AtomicU64::new(u64::MAX));
    let rc = Arc::clone(&routed);
    let car = Arc::clone(&committed_at_route);
    let transport_for_route = Arc::clone(&transport);
    let engine = engine_with_parse_action(ParseErrorAction::Dlq).with_filter_dlq_policy(
        FilterDlqPolicy::Route(Arc::new(
            move |entries: Vec<crate::transport::filter::FilteredDlqEntry>| {
                car.store(transport_for_route.committed_sequence(), Ordering::Relaxed);
                rc.fetch_add(entries.len(), Ordering::Relaxed);
                Ok(())
            },
        )),
    );
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    engine
        .run_workbatch_parsed(
            &*transport,
            shutdown,
            |pb: ParsedBatch<'_, _>| {
                Ok(WorkBatch::new(pb.records, pb.commit_tokens).with_dlq_entries(pb.dlq_entries))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert_eq!(
        routed.load(Ordering::Relaxed),
        1,
        "Dlq: the parse failure reached the DLQ route point"
    );
    // The route fired BEFORE the commit: MemoryTransport's committed_sequence
    // starts at 0; the block's highest seq is 2, so a commit would set it to
    // 2. At route time it must still be its pre-commit value (0).
    assert_eq!(
        committed_at_route.load(Ordering::Relaxed),
        0,
        "DLQ route ran BEFORE the source commit advanced the watermark"
    );
    assert_eq!(
        transport.committed_sequence(),
        2,
        "all 3 acks committed after"
    );
}

/// Finding 2 -- the STANDARD (on-demand) `run_workbatch` path must NOT
/// silently drop DLQ entries that `process` emits on the out-batch. A
/// process closure that attaches a dlq_entry has it ROUTED (reaches the DLQ
/// route point) before the sink-success leads to a source commit -- it does
/// not depend on the sink closure remembering to carry it.
#[tokio::test]
async fn standard_send_batch_sink_does_not_silently_drop_dlq_entries() {
    let transport = mem_transport(50);
    transport
        .inject(None, br#"{"id":1}"#.to_vec())
        .await
        .unwrap();
    transport
        .inject(None, br#"{"id":2}"#.to_vec())
        .await
        .unwrap();

    let routed = Arc::new(AtomicUsize::new(0));
    let rc = Arc::clone(&routed);
    let engine = default_engine().with_filter_dlq_policy(FilterDlqPolicy::Route(Arc::new(
        move |entries: Vec<crate::transport::filter::FilteredDlqEntry>| {
            rc.fetch_add(entries.len(), Ordering::Relaxed);
            Ok(())
        },
    )));
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 200);

    // The SINK ignores dlq_entries entirely (the realistic app shape). The
    // PROCESS closure emits a dlq_entry on the out-batch. Pre-fix this entry
    // would vanish; post-fix the driver routes it before commit.
    engine
        .run_workbatch(
            &transport,
            shutdown,
            |batch| {
                let dlq = vec![crate::transport::filter::FilteredDlqEntry {
                    payload: b"process-emitted dead-letter".to_vec(),
                    key: None,
                    reason: "process decided this record is bad".to_string(),
                }];
                let tokens = batch.commit_tokens;
                let records = batch.records;
                Ok(WorkBatch::new(records, tokens).with_dlq_entries(dlq))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await
        .unwrap();

    assert!(
        routed.load(Ordering::Relaxed) >= 1,
        "process-emitted DLQ entry must reach the DLQ route point, not be \
             silently dropped on the path to commit"
    );
    // Source acks still commit -- the dead-letter routing is independent of
    // the source ack (at-least-once on the whole block).
    assert_eq!(transport.committed_sequence(), 1);
}

/// Finding 3 -- a DLQ-route FAILURE under `Route` is a terminal ack-barrier
/// error: the source commit is skipped (no later ordered commit advances
/// past the undelivered dead-letters). Silent discard is opt-in only.
#[tokio::test]
async fn dlq_route_failure_is_terminal_and_blocks_commit() {
    let receiver = OrderedReceiverBad::without_parse_fail();
    let committed = Arc::clone(&receiver.committed_hwm);

    // A Route sink that FAILS, simulating a DLQ transport outage.
    let engine = default_engine().with_filter_dlq_policy(FilterDlqPolicy::Route(Arc::new(
        |_e: Vec<crate::transport::filter::FilteredDlqEntry>| {
            Err(EngineError::Sink("dlq transport down".into()))
        },
    )));
    let shutdown = CancellationToken::new();
    cancel_after(shutdown.clone(), 500);

    let result = engine
        .run_workbatch(
            &receiver,
            shutdown,
            |batch| {
                // process emits a dlq entry; routing it will fail.
                let dlq = vec![crate::transport::filter::FilteredDlqEntry {
                    payload: b"bad".to_vec(),
                    key: None,
                    reason: "process dlq".to_string(),
                }];
                Ok(WorkBatch::new(batch.records, batch.commit_tokens).with_dlq_entries(dlq))
            },
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            None::<(
                Duration,
                fn() -> std::future::Ready<Result<(), EngineError>>,
            )>,
        )
        .await;

    assert!(
        result.is_err(),
        "DLQ route failure must be a terminal ack-barrier error, got {result:?}"
    );
    assert_eq!(
        committed.load(Ordering::Relaxed),
        u64::MAX,
        "DLQ route failure must skip the commit -- watermark unmoved"
    );
}

/// An ordered receiver that delivers ONE bad (unparseable) record then parks.
/// Cumulative watermark via fetch_max, so a commit is observable. Used to
/// prove FailBatch / DLQ-route-failure leave the watermark unmoved.
struct OrderedReceiverBad {
    next: Arc<AtomicU64>,
    committed_hwm: Arc<AtomicU64>,
    good_payload: bool,
}

impl OrderedReceiverBad {
    fn new() -> Self {
        Self {
            next: Arc::new(AtomicU64::new(0)),
            committed_hwm: Arc::new(AtomicU64::new(u64::MAX)),
            good_payload: false,
        }
    }
    /// Delivers a PARSEABLE record (for the DLQ-route-failure test, where the
    /// dead-letter comes from the process closure, not a parse failure).
    fn without_parse_fail() -> Self {
        Self {
            next: Arc::new(AtomicU64::new(0)),
            committed_hwm: Arc::new(AtomicU64::new(u64::MAX)),
            good_payload: true,
        }
    }
}

impl crate::transport::TransportBase for OrderedReceiverBad {
    fn close(
        &self,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<()>> + Send {
        std::future::ready(Ok(()))
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "ordered-bad-test"
    }
}

impl TransportReceiver for OrderedReceiverBad {
    type Token = crate::transport::memory::MemoryToken;

    fn recv(
        &self,
        _max: usize,
    ) -> impl std::future::Future<Output = crate::transport::TransportResult<WorkBatch<Self::Token>>>
    + Send {
        let next = Arc::clone(&self.next);
        let good = self.good_payload;
        async move {
            let seq = next.fetch_add(1, Ordering::Relaxed);
            if seq >= 1 {
                next.fetch_sub(1, Ordering::Relaxed);
                std::future::pending::<()>().await;
            }
            let payload = if good {
                Bytes::from_static(br#"{"ok":1}"#)
            } else {
                Bytes::from_static(b"not json {{{")
            };
            let record = Record {
                payload,
                key: None,
                headers: vec![],
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            };
            Ok(WorkBatch::new(
                vec![record],
                vec![crate::transport::memory::MemoryToken { seq }],
            ))
        }
    }

    async fn commit(&self, tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
        if let Some(max_seq) = tokens.iter().map(|t| t.seq).max() {
            self.committed_hwm.fetch_max(max_seq, Ordering::Relaxed);
        }
        Ok(())
    }
}

// ---- Shutdown drain ----------------------------------------------------

/// Records a push source holds when shutdown lands after the first block.
const ACKED: u64 = 5;

/// The payload of the record a push source acknowledged as `seq`.
fn acked_payload(seq: u64) -> Bytes {
    Bytes::from(format!(r#"{{"seq":{seq}}}"#))
}

/// Every payload an [`AckedQueueSource::holding`]`(ACKED)` acknowledged.
fn acked_payloads() -> Vec<Bytes> {
    (0..ACKED).map(acked_payload).collect()
}

/// A run's `ticker` argument with no periodic callback.
type NoTicker = Option<(
    Duration,
    fn() -> std::future::Ready<Result<(), EngineError>>,
)>;

/// The `ticker` argument for a run with no periodic callback.
fn no_ticker() -> NoTicker {
    None
}

/// A push source shaped like the gRPC and HTTP servers: every record it holds
/// was acknowledged to its sender when it was queued. `close()` stops intake,
/// and `recv` returns what is still queued, one record a call, then `Closed`.
struct AckedQueueSource {
    queue: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Record>>,
    /// Keeps intake open until `close()`, as a listening server does.
    _intake: tokio::sync::mpsc::Sender<Record>,
    next_seq: AtomicU64,
    committed: Arc<parking_lot::Mutex<Vec<u64>>>,
}

impl AckedQueueSource {
    /// A source already holding `n` acknowledged records, seq 0 onwards.
    fn holding(n: u64) -> Self {
        let capacity = usize::try_from(n).expect("small test count").max(1);
        let (intake, queue) = tokio::sync::mpsc::channel(capacity);
        for seq in 0..n {
            intake
                .try_send(Record {
                    payload: acked_payload(seq),
                    key: None,
                    headers: vec![],
                    metadata: RecordMeta {
                        timestamp_ms: None,
                        format: PayloadFormat::Json,
                    },
                })
                .expect("capacity for every acked record");
        }
        Self {
            queue: tokio::sync::Mutex::new(queue),
            _intake: intake,
            next_seq: AtomicU64::new(0),
            committed: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }
}

impl crate::transport::TransportBase for AckedQueueSource {
    async fn close(&self) -> crate::transport::TransportResult<()> {
        self.queue.lock().await.close();
        Ok(())
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "acked-queue-test"
    }
}

impl TransportReceiver for AckedQueueSource {
    type Token = crate::transport::memory::MemoryToken;

    async fn recv(&self, _max: usize) -> crate::transport::TransportResult<WorkBatch<Self::Token>> {
        let Some(record) = self.queue.lock().await.recv().await else {
            return Err(crate::transport::TransportError::Closed);
        };
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        Ok(WorkBatch::new(
            vec![record],
            vec![crate::transport::memory::MemoryToken { seq }],
        ))
    }

    async fn commit(&self, tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
        self.committed.lock().extend(tokens.iter().map(|t| t.seq));
        Ok(())
    }
}

/// A sink that records what it takes and requests shutdown on its first block,
/// so the run loop stops with acknowledged records still queued at the source.
fn sink_stopping_after_first_block(
    shutdown: CancellationToken,
    sunk: Arc<parking_lot::Mutex<Vec<Bytes>>>,
) -> impl FnMut(
    &WorkBatch<crate::transport::memory::MemoryToken>,
) -> std::future::Ready<Result<(), EngineError>> {
    move |out| {
        sunk.lock()
            .extend(out.records.iter().map(|r| r.payload.clone()));
        shutdown.cancel();
        std::future::ready(Ok(()))
    }
}

/// Shutdown lands after the first block with four acknowledged records still
/// queued at a push source: `run_workbatch` closes the source and sinks them.
#[tokio::test]
async fn run_workbatch_drains_acked_records_after_shutdown() {
    let source = AckedQueueSource::holding(ACKED);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let result = default_engine()
        .run_workbatch(
            &source,
            shutdown.clone(),
            |batch| Ok(batch),
            sink_stopping_after_first_block(shutdown, Arc::clone(&sunk)),
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        *sunk.lock(),
        acked_payloads(),
        "every record the source acknowledged must reach the sink"
    );
    assert_eq!(
        *source.committed.lock(),
        (0..ACKED).collect::<Vec<_>>(),
        "and every drained block commits"
    );
}

/// The streaming loop drains the same way: every acknowledged record is sunk.
#[tokio::test]
async fn run_workbatch_streaming_drains_acked_records_after_shutdown() {
    let source = AckedQueueSource::holding(ACKED);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let result = default_engine()
        .run_workbatch_streaming(
            &source,
            shutdown.clone(),
            |batch| Ok(batch),
            sink_stopping_after_first_block(shutdown, Arc::clone(&sunk)),
            CommitMode::Auto,
            64,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(*sunk.lock(), acked_payloads());
}

/// The pre-parsing loop drains the same way: every acknowledged record is sunk.
#[tokio::test]
async fn run_workbatch_parsed_drains_acked_records_after_shutdown() {
    let source = AckedQueueSource::holding(ACKED);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let result = default_engine()
        .run_workbatch_parsed(
            &source,
            shutdown.clone(),
            |pb: ParsedBatch<'_, _>| {
                Ok(WorkBatch::new(pb.records, pb.commit_tokens).with_dlq_entries(pb.dlq_entries))
            },
            sink_stopping_after_first_block(shutdown, Arc::clone(&sunk)),
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(*sunk.lock(), acked_payloads());
}

/// `run_governed` with the governor off delegates to `run_workbatch`, drain
/// included.
#[cfg(feature = "governor")]
#[tokio::test]
async fn run_governed_off_drains_acked_records_after_shutdown() {
    let source = AckedQueueSource::holding(ACKED);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let result = default_engine()
        .run_governed(
            &source,
            shutdown.clone(),
            |batch| Ok(batch),
            sink_stopping_after_first_block(shutdown, Arc::clone(&sunk)),
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(*sunk.lock(), acked_payloads());
}

/// `run_governed` with the governor on drains through byte-budget sub-blocks.
#[cfg(feature = "governor")]
#[tokio::test]
async fn run_governed_on_drains_acked_records_after_shutdown() {
    let source = AckedQueueSource::holding(ACKED);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (engine, _gov) = governed_engine();

    let result = engine
        .run_governed(
            &source,
            shutdown.clone(),
            |batch| Ok(batch),
            sink_stopping_after_first_block(shutdown, Arc::clone(&sunk)),
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(*sunk.lock(), acked_payloads());
    assert_eq!(*source.committed.lock(), (0..ACKED).collect::<Vec<_>>());
}

/// The engine over the real HTTP server: every record the server answered 200
/// for before shutdown reaches the sink, though the loop had read none of them
/// when the token fired, and a POST after the run is refused retryably.
#[cfg(all(
    feature = "transport-http",
    feature = "http-server",
    feature = "governor"
))]
#[tokio::test]
async fn run_governed_drains_an_http_source_at_shutdown() {
    use crate::transport::TransportSender;
    use crate::transport::http::{HttpTransport, HttpTransportConfig};

    let source = HttpTransport::new(&HttpTransportConfig {
        listen: Some("127.0.0.1:0".to_string()),
        recv_timeout_ms: 100,
        ..Default::default()
    })
    .await
    .expect("receiver");
    let addr = source.local_addr().expect("receiver bound");
    let sender = HttpTransport::new(&HttpTransportConfig::sender(&format!(
        "http://{addr}/ingest"
    )))
    .await
    .expect("sender");
    for seq in 0..ACKED {
        let sent = sender.send("", acked_payload(seq)).await;
        assert!(sent.is_ok(), "{sent:?}");
    }

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let taken = Arc::clone(&sunk);

    let result = default_engine()
        .run_governed(
            &source,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                taken
                    .lock()
                    .extend(out.records.iter().map(|r| r.payload.clone()));
                std::future::ready(Ok(()))
            },
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        *sunk.lock(),
        acked_payloads(),
        "every record the HTTP server acked must reach the sink"
    );
    let after = sender.send("", acked_payload(ACKED)).await;
    assert!(
        after.is_backpressured(),
        "the engine closed the source, so a later POST must be refused retryably, got {after:?}"
    );
}

/// The engine over the real gRPC server, the shape of a direct-transport
/// pipeline: every record the server acked before shutdown reaches the sink,
/// and a push after the run is refused retryably.
#[cfg(all(feature = "transport-grpc", feature = "governor"))]
#[tokio::test]
async fn run_governed_drains_a_grpc_source_at_shutdown() {
    use crate::transport::TransportSender;
    use crate::transport::grpc::{GrpcConfig, GrpcTransport};

    let mut server_config = GrpcConfig::server("127.0.0.1:0");
    server_config.recv_timeout_ms = 100;
    let source = GrpcTransport::new(&server_config)
        .await
        .expect("gRPC listener");
    let addr = source.local_addr().expect("listener bound");
    let sender = GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
        .await
        .expect("gRPC client");
    for seq in 0..ACKED {
        let sent = sender.send("main", acked_payload(seq)).await;
        assert!(sent.is_ok(), "{sent:?}");
    }

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let taken = Arc::clone(&sunk);

    let result = default_engine()
        .run_governed(
            &source,
            shutdown,
            |batch| Ok(batch),
            move |out: &WorkBatch<_>| {
                taken
                    .lock()
                    .extend(out.records.iter().map(|r| r.payload.clone()));
                std::future::ready(Ok(()))
            },
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        *sunk.lock(),
        acked_payloads(),
        "every record the gRPC server acked must reach the sink"
    );
    let after = sender.send("main", acked_payload(ACKED)).await;
    assert!(
        after.is_backpressured(),
        "the engine closed the source, so a later push must be refused retryably, got {after:?}"
    );
}

/// What a scripted sink does with its `n`th call (from 1).
#[derive(Clone, Copy)]
enum SinkStep {
    Accept,
    Busy,
    Broken,
}

/// A sink that cancels `shutdown` on its first call and answers each call per
/// `script`, then `then` for every call past its end, recording every seq it
/// is offered and every payload it accepts.
fn scripted_sink(
    shutdown: CancellationToken,
    script: &'static [SinkStep],
    then: SinkStep,
    offered: Arc<parking_lot::Mutex<Vec<u64>>>,
    accepted: Arc<parking_lot::Mutex<Vec<Bytes>>>,
) -> impl FnMut(
    &WorkBatch<crate::transport::memory::MemoryToken>,
) -> std::future::Ready<Result<(), EngineError>> {
    let mut calls = 0_usize;
    move |out| {
        calls += 1;
        shutdown.cancel();
        offered
            .lock()
            .extend(out.commit_tokens.iter().map(|t| t.seq));
        let step = script.get(calls - 1).copied().unwrap_or(then);
        std::future::ready(match step {
            SinkStep::Accept => {
                accepted
                    .lock()
                    .extend(out.records.iter().map(|r| r.payload.clone()));
                Ok(())
            }
            SinkStep::Busy => Err(crate::transport::TransportError::Backpressure.into()),
            SinkStep::Broken => Err(EngineError::Sink("sink broken".into())),
        })
    }
}

/// Run `run_workbatch` over a push source holding `ACKED` records with a
/// scripted sink, returning the result, the seqs offered and the payloads
/// accepted.
async fn drain_with_script(
    source: &AckedQueueSource,
    script: &'static [SinkStep],
    then: SinkStep,
) -> (Result<(), EngineError>, Vec<u64>, Vec<Bytes>) {
    let shutdown = CancellationToken::new();
    let offered = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let accepted = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let result = default_engine()
        .run_workbatch(
            source,
            shutdown.clone(),
            |batch| Ok(batch),
            scripted_sink(
                shutdown,
                script,
                then,
                Arc::clone(&offered),
                Arc::clone(&accepted),
            ),
            CommitMode::Auto,
            no_ticker(),
        )
        .await;
    let offered = offered.lock().clone();
    let accepted = accepted.lock().clone();
    (result, offered, accepted)
}

/// A sink busy for a moment during the drain is retried, not abandoned: every
/// record the push source acknowledged is still delivered.
#[tokio::test(start_paused = true)]
async fn the_drain_retries_a_busy_sink_and_delivers_every_record() {
    use SinkStep::{Accept, Busy};
    let source = AckedQueueSource::holding(ACKED);

    let (result, offered, accepted) =
        drain_with_script(&source, &[Accept, Busy, Busy], Accept).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        accepted,
        acked_payloads(),
        "every acked record is delivered after the sink recovers; offered {offered:?}"
    );
    assert_eq!(*source.committed.lock(), (0..ACKED).collect::<Vec<_>>());
}

/// A block the sink is refusing when shutdown lands keeps being retried, and
/// the drain follows once it is delivered.
#[tokio::test(start_paused = true)]
async fn a_block_refused_as_shutdown_lands_is_retried_then_drained() {
    use SinkStep::{Accept, Busy};
    let source = AckedQueueSource::holding(ACKED);

    let (result, offered, accepted) = drain_with_script(&source, &[Busy, Busy], Accept).await;

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        accepted,
        acked_payloads(),
        "the refused block and everything queued behind it are delivered; offered {offered:?}"
    );
    assert_eq!(*source.committed.lock(), (0..ACKED).collect::<Vec<_>>());
}

/// A sink that fails permanently during the drain stops it at once: the run
/// returns the error and nothing commits past the failed block.
#[tokio::test(start_paused = true)]
async fn the_drain_stops_at_once_on_a_permanent_sink_failure() {
    use SinkStep::{Accept, Broken};
    let source = AckedQueueSource::holding(ACKED);

    let (result, offered, _accepted) = drain_with_script(&source, &[Accept, Broken], Accept).await;

    assert!(
        matches!(result, Err(EngineError::Sink(_))),
        "a permanent sink failure ends the run with its error, got {result:?}"
    );
    assert_eq!(offered, vec![0, 1], "seq 1 is offered once, seq 2 never");
    assert_eq!(
        *source.committed.lock(),
        vec![0],
        "nothing commits past the failed block"
    );
}

/// A sink still busy when the retry window after shutdown closes is given up
/// on at that point: the block stays uncommitted and nothing past it is sunk.
#[tokio::test(start_paused = true)]
async fn a_block_still_refused_when_the_retry_window_closes_is_abandoned() {
    use SinkStep::{Accept, Busy};
    let source = AckedQueueSource::holding(ACKED);
    let started = tokio::time::Instant::now();

    let (result, offered, accepted) = drain_with_script(&source, &[Accept], Busy).await;
    let took = started.elapsed();

    assert!(result.is_ok(), "{result:?}");
    assert_eq!(accepted, vec![acked_payload(0)]);
    assert!(
        offered.iter().skip(1).all(|seq| *seq == 1),
        "only seq 1 is retried, nothing past it is offered: {offered:?}"
    );
    assert_eq!(*source.committed.lock(), vec![0]);
    assert!(
        took >= SHUTDOWN_RETRY_LIMIT && took < SHUTDOWN_RETRY_LIMIT + Duration::from_secs(1),
        "retries should end at the {SHUTDOWN_RETRY_LIMIT:?} window, took {took:?}"
    );
}

/// A source that ignores `close()` and waits forever cannot hold shutdown:
/// the drain gives up once nothing has arrived for `DRAIN_IDLE_LIMIT`.
#[tokio::test(start_paused = true)]
async fn the_drain_gives_up_on_a_source_that_never_reports_closed() {
    struct NeverCloses;

    impl crate::transport::TransportBase for NeverCloses {
        async fn close(&self) -> crate::transport::TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "never-closes-test"
        }
    }

    impl TransportReceiver for NeverCloses {
        type Token = crate::transport::memory::MemoryToken;

        async fn recv(
            &self,
            _max: usize,
        ) -> crate::transport::TransportResult<WorkBatch<Self::Token>> {
            std::future::pending().await
        }

        async fn commit(&self, _tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
            Ok(())
        }
    }

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let started = tokio::time::Instant::now();

    let result = default_engine()
        .run_workbatch(
            &NeverCloses,
            shutdown,
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| async { Ok(()) },
            CommitMode::Auto,
            no_ticker(),
        )
        .await;

    let took = started.elapsed();
    assert!(result.is_ok(), "{result:?}");
    assert!(
        took >= DRAIN_IDLE_LIMIT && took < DRAIN_IDLE_LIMIT + Duration::from_secs(1),
        "the drain should give up at {DRAIN_IDLE_LIMIT:?}, took {took:?}"
    );
}

// ---- Pipeline: held source acknowledgements ------------------------------

use crate::transport::ack::{AckControl, AckKind, HeldAcks, SinkConfirmation};
use crate::transport::{DeliveryStatus, PieceFinalizer};
use crate::worker::engine::pipeline::BlockPieces;

/// One release a [`HeldSource`] saw: the seqs, the status, and when.
type Release = (Vec<u64>, DeliveryStatus, tokio::time::Instant);

/// Acknowledgement controls for a test source.
struct TestControl {
    enabled: bool,
    armed: std::sync::atomic::AtomicBool,
    kind: AckKind,
}

impl AckControl for TestControl {
    fn enabled(&self) -> bool {
        self.enabled
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }
    fn kind(&self) -> AckKind {
        self.kind
    }
    fn held(&self) -> HeldAcks {
        HeldAcks::default()
    }
}

/// A source that holds its acknowledgement: it hands out scripted blocks, one
/// per `recv`, then waits; it records every release and commit, and gives each
/// block a hold deadline when `hold_for` is set, as a push source does.
struct HeldSource {
    blocks: parking_lot::Mutex<std::collections::VecDeque<Vec<u64>>>,
    control: TestControl,
    releases: Arc<parking_lot::Mutex<Vec<Release>>>,
    commits: Arc<parking_lot::Mutex<Vec<u64>>>,
    hold_for: Option<Duration>,
    received: parking_lot::Mutex<std::collections::HashMap<u64, std::time::Instant>>,
    closed: std::sync::atomic::AtomicBool,
}

impl HeldSource {
    fn new(kind: AckKind, blocks: Vec<Vec<u64>>) -> Self {
        Self {
            blocks: parking_lot::Mutex::new(blocks.into()),
            control: TestControl {
                enabled: true,
                armed: std::sync::atomic::AtomicBool::new(false),
                kind,
            },
            releases: Arc::new(parking_lot::Mutex::new(Vec::new())),
            commits: Arc::new(parking_lot::Mutex::new(Vec::new())),
            hold_for: None,
            received: parking_lot::Mutex::new(std::collections::HashMap::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn holding_for(mut self, hold: Duration) -> Self {
        self.hold_for = Some(hold);
        self
    }

    fn acks_disabled(mut self) -> Self {
        self.control.enabled = false;
        self
    }

    /// The seqs and status of every release so far.
    fn released(&self) -> Vec<(Vec<u64>, DeliveryStatus)> {
        self.releases
            .lock()
            .iter()
            .map(|(seqs, status, _)| (seqs.clone(), *status))
            .collect()
    }
}

impl crate::transport::TransportBase for HeldSource {
    async fn close(&self) -> crate::transport::TransportResult<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "held-test"
    }
}

impl TransportReceiver for HeldSource {
    type Token = crate::transport::memory::MemoryToken;

    async fn recv(&self, _max: usize) -> crate::transport::TransportResult<WorkBatch<Self::Token>> {
        let next = self.blocks.lock().pop_front();
        let Some(seqs) = next else {
            if self.closed.load(Ordering::Acquire) {
                return Err(crate::transport::TransportError::Closed);
            }
            std::future::pending::<()>().await;
            unreachable!("pending never resolves");
        };
        let now = tokio::time::Instant::now().into_std();
        let mut received = self.received.lock();
        let records = seqs
            .iter()
            .map(|seq| {
                received.insert(*seq, now);
                Record {
                    payload: Bytes::from(format!(r#"{{"seq":{seq}}}"#)),
                    key: None,
                    headers: vec![],
                    metadata: RecordMeta {
                        timestamp_ms: None,
                        format: PayloadFormat::Json,
                    },
                }
            })
            .collect();
        let tokens = seqs
            .iter()
            .map(|seq| crate::transport::memory::MemoryToken { seq: *seq })
            .collect();
        Ok(WorkBatch::new(records, tokens))
    }

    async fn commit(&self, tokens: &[Self::Token]) -> crate::transport::TransportResult<()> {
        self.commits.lock().extend(tokens.iter().map(|t| t.seq));
        Ok(())
    }

    fn ack_control(&self) -> Option<&dyn AckControl> {
        Some(&self.control)
    }

    async fn release(
        &self,
        tokens: &[Self::Token],
        outcome: DeliveryStatus,
    ) -> crate::transport::TransportResult<()> {
        self.releases.lock().push((
            tokens.iter().map(|t| t.seq).collect(),
            outcome,
            tokio::time::Instant::now(),
        ));
        if outcome.should_commit() {
            self.commit(tokens).await?;
        }
        Ok(())
    }

    fn hold_deadline(&self, tokens: &[Self::Token]) -> Option<std::time::Instant> {
        let hold = self.hold_for?;
        let received = self.received.lock();
        tokens
            .iter()
            .filter_map(|t| received.get(&t.seq))
            .min()
            .map(|at| *at + hold)
    }
}

/// Poll `run` for `wait` without letting it finish.
async fn run_for<F: std::future::Future>(run: std::pin::Pin<&mut F>, wait: Duration)
where
    F::Output: std::fmt::Debug,
{
    tokio::select! {
        out = run => panic!("the pipeline ended early: {out:?}"),
        () = tokio::time::sleep(wait) => {}
    }
}

#[tokio::test(start_paused = true)]
async fn source_ack_waits_for_every_fanned_out_piece() {
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1]]);
    let shutdown = CancellationToken::new();
    let late: Arc<parking_lot::Mutex<Option<PieceFinalizer>>> =
        Arc::new(parking_lot::Mutex::new(None));
    let engine = default_engine();

    let sink_late = Arc::clone(&late);
    let run = engine
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .run_with_pieces(
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>, pieces: &BlockPieces<'_>| {
                *sink_late.lock() = Some(pieces.piece());
                std::future::ready(Ok(()))
            },
        );
    tokio::pin!(run);

    run_for(run.as_mut(), Duration::from_millis(100)).await;
    assert!(
        source.control.is_armed(),
        "the pipeline arms a source with acknowledgements on"
    );
    assert!(
        source.released().is_empty(),
        "the sink returned, but one piece is still out: nothing is released"
    );

    late.lock()
        .take()
        .expect("the sink took a piece")
        .report(DeliveryStatus::Delivered);
    run_for(run.as_mut(), Duration::from_millis(100)).await;
    assert_eq!(
        source.released(),
        vec![(vec![0, 1], DeliveryStatus::Delivered)],
        "released once, after both pieces, with the merged status"
    );
    assert_eq!(*source.commits.lock(), vec![0, 1]);

    shutdown.cancel();
    run.await.expect("clean shutdown");
}

#[tokio::test]
async fn one_errored_piece_withholds_the_source_ack() {
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1], vec![2]]);
    let engine = default_engine();

    let result = engine
        .pipeline(&source)
        .run_with_pieces(
            |batch| Ok(batch),
            |_out: &WorkBatch<_>, pieces: &BlockPieces<'_>| {
                pieces.piece().report(DeliveryStatus::Errored);
                std::future::ready(Ok(()))
            },
        )
        .await;

    assert!(
        matches!(result, Err(EngineError::Sink(_))),
        "a pull source's errored block stops the loop so it is read again: {result:?}"
    );
    assert_eq!(
        source.released(),
        vec![(vec![0, 1], DeliveryStatus::Errored)],
        "released Errored, and the later block is never fetched past it"
    );
    assert!(source.commits.lock().is_empty(), "nothing is committed");
}

#[tokio::test(start_paused = true)]
async fn push_block_is_abandoned_at_its_hold_deadline() {
    let hold = Duration::from_secs(1);
    let source = HeldSource::new(AckKind::Push, vec![vec![0], vec![1]]).holding_for(hold);
    let shutdown = CancellationToken::new();
    let engine = default_engine();
    let started = tokio::time::Instant::now();

    let run = engine.pipeline(&source).shutdown(shutdown.clone()).run(
        |batch| Ok(batch),
        |out: &WorkBatch<crate::transport::memory::MemoryToken>| {
            let refuse = out.commit_tokens.iter().any(|t| t.seq == 0);
            std::future::ready(if refuse {
                Err(EngineError::Transport(
                    crate::transport::TransportError::Backpressure,
                ))
            } else {
                Ok(())
            })
        },
    );
    tokio::pin!(run);
    run_for(run.as_mut(), Duration::from_secs(3)).await;

    let releases = source.releases.lock().clone();
    assert_eq!(releases.len(), 2, "both blocks released: {releases:?}");
    let (seqs, status, at) = &releases[0];
    assert_eq!(
        (seqs.as_slice(), *status),
        (&[0][..], DeliveryStatus::Errored)
    );
    assert!(
        at.duration_since(started) < hold,
        "the refused block is released before its sender's deadline, at {:?}",
        at.duration_since(started)
    );
    assert_eq!(
        (releases[1].0.as_slice(), releases[1].1),
        (&[1][..], DeliveryStatus::Delivered),
        "the loop goes on to the next block"
    );

    shutdown.cancel();
    run.await.expect("clean shutdown");
}

/// How a scripted pipeline run fails its first block.
#[derive(Debug, Clone, Copy)]
enum Failure {
    /// `process` returns an error.
    Process,
    /// The sink returns a permanent error.
    Sink,
    /// Inbound DLQ entries under the default `Reject` policy.
    UnroutedDeadLetter,
    /// The sink refuses transiently until the retry window after shutdown
    /// closes.
    RefusedThroughShutdown,
}

/// A push source's first block, `[0, 1]`, run to `failure`: the tokens the
/// source saw released.
async fn released_after(failure: Failure) -> Vec<(Vec<u64>, DeliveryStatus)> {
    let source = HeldSource::new(AckKind::Push, vec![vec![0, 1], vec![2]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let engine = default_engine();
    let process = move |batch: WorkBatch<crate::transport::memory::MemoryToken>| match failure {
        Failure::Process => Err(EngineError::Sink("process failed".into())),
        Failure::UnroutedDeadLetter => {
            Ok(
                batch.with_dlq_entries(vec![crate::transport::filter::FilteredDlqEntry {
                    payload: b"poison".to_vec(),
                    key: None,
                    reason: "filter".into(),
                }]),
            )
        }
        Failure::Sink | Failure::RefusedThroughShutdown => Ok(batch),
    };
    let sink = move |_out: &WorkBatch<crate::transport::memory::MemoryToken>| {
        std::future::ready(match failure {
            Failure::Sink => Err(EngineError::Sink("sink failed".into())),
            Failure::RefusedThroughShutdown => {
                stop.cancel();
                Err(EngineError::Transport(
                    crate::transport::TransportError::Backpressure,
                ))
            }
            Failure::Process | Failure::UnroutedDeadLetter => Ok(()),
        })
    };
    let _ = engine
        .pipeline(&source)
        .shutdown(shutdown)
        .run(process, sink)
        .await;
    source.released()
}

/// An armed push source holds every answer until release, so each path out of
/// a block -- an error, an abandon at shutdown -- releases what it took.
#[tokio::test(start_paused = true)]
async fn every_token_taken_is_released_on_every_path() {
    for failure in [
        Failure::Process,
        Failure::Sink,
        Failure::UnroutedDeadLetter,
        Failure::RefusedThroughShutdown,
    ] {
        assert_eq!(
            released_after(failure).await,
            vec![(vec![0, 1], DeliveryStatus::Errored)],
            "{failure:?}: the block's senders are answered Errored at once"
        );
    }

    // The shutdown drain releases every block it takes.
    let source = HeldSource::new(AckKind::Push, vec![vec![0], vec![1, 2], vec![3]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    default_engine()
        .pipeline(&source)
        .shutdown(shutdown)
        .run(
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| {
                stop.cancel();
                std::future::ready(Ok(()))
            },
        )
        .await
        .expect("clean shutdown");
    assert_eq!(
        source.released(),
        vec![
            (vec![0], DeliveryStatus::Delivered),
            (vec![1, 2], DeliveryStatus::Delivered),
            (vec![3], DeliveryStatus::Delivered),
        ],
        "every block, drained ones included, is released once"
    );
}

/// A push source whose sink never finishes a block, run for a while and then
/// dropped mid-block, as a caller's `select!` or an aborted task drops it.
#[tokio::test(start_paused = true)]
async fn a_pipeline_dropped_mid_block_releases_the_block_errored() {
    let source = HeldSource::new(AckKind::Push, vec![vec![0, 1]]);
    let engine = default_engine();
    {
        let run = engine.pipeline(&source).run(
            |batch| Ok(batch),
            |_out: &WorkBatch<crate::transport::memory::MemoryToken>| std::future::pending(),
        );
        tokio::pin!(run);
        run_for(run.as_mut(), Duration::from_millis(100)).await;
        assert!(
            source.released().is_empty(),
            "the sink still has the block, so nothing is released yet"
        );
    }
    assert_eq!(
        source.released(),
        vec![(vec![0, 1], DeliveryStatus::Errored)],
        "the dropped run released its block Errored, so its senders are answered"
    );
}

/// A hand-rolled loop's `SourceAck` dropped before its release, as a panic or
/// a dropped loop future drops it, releases its block `Errored`, and one that
/// was released is not released again on drop.
#[tokio::test]
async fn a_source_ack_dropped_unreleased_releases_its_block_errored() {
    let source = HeldSource::new(AckKind::Push, vec![vec![0, 1]]);
    let batch = source.recv(10).await.expect("a block");
    {
        let ack = crate::transport::SourceAck::new(&source, batch.commit_tokens.clone());
        let _unreported = ack.piece();
    }
    assert_eq!(
        source.released(),
        vec![(vec![0, 1], DeliveryStatus::Errored)]
    );

    let ack = crate::transport::SourceAck::new(&source, batch.commit_tokens);
    ack.piece().report(DeliveryStatus::Delivered);
    let status = ack.release().await.expect("released");
    assert_eq!(status, DeliveryStatus::Delivered);
    assert_eq!(
        source.released(),
        vec![
            (vec![0, 1], DeliveryStatus::Errored),
            (vec![0, 1], DeliveryStatus::Delivered),
        ],
        "released once, and not again when it drops"
    );
}

/// A push source's block `[0, 1]`, whose `process` or sink panics inside a
/// task: the releases the source saw.
async fn released_after_a_panic(in_process: bool) -> Vec<(Vec<u64>, DeliveryStatus)> {
    let source = Arc::new(HeldSource::new(AckKind::Push, vec![vec![0, 1]]));
    let run = tokio::spawn({
        let source = Arc::clone(&source);
        async move {
            default_engine()
                .pipeline(&*source)
                .run(
                    move |batch: WorkBatch<crate::transport::memory::MemoryToken>| {
                        assert!(!in_process, "process panicked");
                        Ok(batch)
                    },
                    move |_out: &WorkBatch<crate::transport::memory::MemoryToken>| async move {
                        assert!(in_process, "the sink panicked");
                        Ok(())
                    },
                )
                .await
        }
    });
    let joined = run.await;
    assert!(
        joined.as_ref().is_err_and(tokio::task::JoinError::is_panic),
        "the pipeline task panicked: {joined:?}"
    );
    source.released()
}

#[tokio::test]
async fn a_panic_in_process_or_the_sink_releases_the_block_errored() {
    for (in_process, stage) in [(true, "process"), (false, "the sink")] {
        assert_eq!(
            released_after_a_panic(in_process).await,
            vec![(vec![0, 1], DeliveryStatus::Errored)],
            "a panic in {stage} released the block Errored"
        );
    }
}

/// The guarantee a pull pipeline publishes when its sink declares `declared`
/// through `sink_confirms`, or nothing at all, with no sender.
#[cfg(feature = "metrics")]
async fn guarantee_published(declared: Option<SinkConfirmation>) -> Arc<GaugeCapture> {
    let capture = Arc::new(GaugeCapture::default());
    let recorder = metrics::set_default_local_recorder(&*capture);
    let source = HeldSource::new(AckKind::Pull, Vec::new());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let engine = default_engine();
    let pipeline = engine.pipeline(&source).shutdown(shutdown);
    let pipeline = match declared {
        Some(confirms) => pipeline.sink_confirms(confirms),
        None => pipeline,
    };
    pipeline
        .run(
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| std::future::ready(Ok(())),
        )
        .await
        .expect("clean shutdown");
    drop(recorder);
    capture
}

/// A sink that is not a transport declares what its `Ok` proves: it has no
/// transport screen to miss, so it is taken at its word.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn a_sink_that_declares_its_confirmation_is_taken_at_its_word() {
    let remote = guarantee_published(Some(SinkConfirmation::Remote)).await;
    assert_eq!(
        remote.gauge(
            "pipeline_delivery_guarantee",
            &[("guarantee", "at_least_once"), ("reason", "confirmed")]
        ),
        Some(1.0)
    );
    let local = guarantee_published(Some(SinkConfirmation::Local)).await;
    assert_eq!(
        local.gauge(
            "pipeline_delivery_guarantee",
            &[
                ("guarantee", "at_least_once_local"),
                ("reason", "sink_confirms_locally")
            ]
        ),
        Some(1.0)
    );
}

/// With neither a sender nor a declared confirmation, the sink's `Ok` proves
/// nothing and nothing screens the blocks: best effort.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn a_pipeline_that_declares_nothing_reports_best_effort() {
    let undeclared = guarantee_published(None).await;
    assert_eq!(
        undeclared.gauge(
            "pipeline_delivery_guarantee",
            &[
                ("guarantee", "best_effort"),
                ("reason", "sink_cannot_confirm")
            ]
        ),
        Some(1.0)
    );
    assert_eq!(
        undeclared.gauge(
            "pipeline_delivery_guarantee",
            &[("guarantee", "at_least_once")]
        ),
        None
    );
}

/// The labels of the `pipeline_delivery_guarantee` series a remote-confirming
/// pull pipeline publishes, named `listener` or not named at all.
#[cfg(feature = "metrics")]
async fn guarantee_series(listener: Option<&str>) -> Vec<Vec<(String, String)>> {
    let capture = GaugeCapture::default();
    let _recorder = metrics::set_default_local_recorder(&capture);
    let source = HeldSource::new(AckKind::Pull, Vec::new());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let engine = default_engine();
    let pipeline = engine
        .pipeline(&source)
        .shutdown(shutdown)
        .sink_confirms(SinkConfirmation::Remote);
    let pipeline = match listener {
        Some(name) => pipeline.listener(name),
        None => pipeline,
    };
    pipeline
        .run(
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| std::future::ready(Ok(())),
        )
        .await
        .expect("clean shutdown");
    capture.gauge_series("pipeline_delivery_guarantee")
}

/// A pipeline named with `.listener` publishes its guarantee under that
/// label and no unlabelled series beside it; one not named publishes the
/// unlabelled series as before.
#[cfg(feature = "metrics")]
#[tokio::test(flavor = "current_thread")]
async fn a_pipeline_named_for_its_listener_labels_its_guarantee() {
    let pair = |k: &str, v: &str| (k.to_string(), v.to_string());
    assert_eq!(
        guarantee_series(Some("syslog")).await,
        vec![vec![
            pair("guarantee", "at_least_once"),
            pair("listener", "syslog"),
            pair("reason", "confirmed"),
        ]]
    );
    assert_eq!(
        guarantee_series(None).await,
        vec![vec![
            pair("guarantee", "at_least_once"),
            pair("reason", "confirmed"),
        ]]
    );
}

/// An app with a source and sink pair per listener publishes one series per
/// listener, told apart by the `listener` label.
#[cfg(feature = "metrics")]
#[test]
fn a_guarantee_published_for_a_listener_carries_its_name() {
    use crate::transport::ack::EffectiveGuarantee;

    let capture = GaugeCapture::default();
    let _recorder = metrics::set_default_local_recorder(&capture);
    let source = HeldSource::new(AckKind::Pull, Vec::new());
    EffectiveGuarantee::of(source.ack_control(), SinkConfirmation::Remote).publish_for("syslog");
    EffectiveGuarantee::of(None, SinkConfirmation::Remote).publish_for("netflow");

    assert_eq!(
        capture.gauge(
            "pipeline_delivery_guarantee",
            &[
                ("guarantee", "at_least_once"),
                ("reason", "confirmed"),
                ("listener", "syslog")
            ]
        ),
        Some(1.0)
    );
    assert_eq!(
        capture.gauge(
            "pipeline_delivery_guarantee",
            &[
                ("guarantee", "best_effort"),
                ("reason", "source_cannot_ack"),
                ("listener", "netflow")
            ]
        ),
        Some(1.0)
    );
}

#[tokio::test]
async fn acknowledgements_disabled_releases_at_receipt() {
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1]]).acks_disabled();
    let shutdown = CancellationToken::new();
    let commits_at_sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = Arc::clone(&commits_at_sink);
    let commits = Arc::clone(&source.commits);
    let stop = shutdown.clone();

    default_engine()
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .run(
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| {
                seen.lock().extend(commits.lock().iter().copied());
                stop.cancel();
                std::future::ready(Ok(()))
            },
        )
        .await
        .expect("clean shutdown");

    assert!(!source.control.is_armed(), "a disabled source is not armed");
    assert_eq!(
        *commits_at_sink.lock(),
        vec![0, 1],
        "committed before the sink ran"
    );
    assert_eq!(
        source.released(),
        vec![(vec![0, 1], DeliveryStatus::Delivered)],
        "released once, at receipt"
    );
}

/// A gauge value captured by a local recorder, keyed by name and labels.
#[cfg(feature = "metrics")]
#[derive(Default)]
struct GaugeCapture {
    gauges: std::sync::Mutex<std::collections::HashMap<metrics::Key, Arc<AtomicU64>>>,
    counters: std::sync::Mutex<std::collections::HashMap<metrics::Key, Arc<AtomicU64>>>,
}

#[cfg(feature = "metrics")]
impl GaugeCapture {
    fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.gauges
            .lock()
            .expect("capture lock")
            .iter()
            .find(|(key, _)| key.name() == name && has_labels(key, labels))
            .map(|(_, cell)| f64::from_bits(cell.load(Ordering::Acquire)))
    }

    /// The labels of every gauge series named `name`, each sorted by key.
    fn gauge_series(&self, name: &str) -> Vec<Vec<(String, String)>> {
        self.gauges
            .lock()
            .expect("capture lock")
            .keys()
            .filter(|key| key.name() == name)
            .map(|key| {
                let mut labels: Vec<(String, String)> = key
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect();
                labels.sort();
                labels
            })
            .collect()
    }

    fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.counters
            .lock()
            .expect("capture lock")
            .iter()
            .filter(|(key, _)| key.name() == name && has_labels(key, labels))
            .map(|(_, cell)| cell.load(Ordering::Acquire))
            .sum()
    }
}

#[cfg(feature = "metrics")]
fn has_labels(key: &metrics::Key, labels: &[(&str, &str)]) -> bool {
    labels
        .iter()
        .all(|(k, v)| key.labels().any(|l| l.key() == *k && l.value() == *v))
}

#[cfg(feature = "metrics")]
impl metrics::Recorder for GaugeCapture {
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

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let cell = Arc::clone(
            self.counters
                .lock()
                .expect("capture lock")
                .entry(key.clone())
                .or_default(),
        );
        metrics::Counter::from_arc(cell)
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        let cell = Arc::clone(
            self.gauges
                .lock()
                .expect("capture lock")
                .entry(key.clone())
                .or_default(),
        );
        metrics::Gauge::from_arc(cell)
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// A governed pipeline writes each received block's bytes beside the byte
/// budget, as the governed driver does: the gauge holds one block's bytes
/// while the next is in the sink, and the last block's once the run ends.
#[cfg(all(feature = "metrics", feature = "governor"))]
#[tokio::test(flavor = "current_thread")]
async fn a_governed_pipeline_writes_each_received_block_bytes() {
    let payload_bytes = |seqs: &[u64]| -> f64 {
        seqs.iter()
            .map(|s| format!(r#"{{"seq":{s}}}"#).len() as f64)
            .sum()
    };
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let (engine, _gov) = governed_engine();
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2], vec![10]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let seen = Arc::clone(&capture);
    let during_second = Arc::new(parking_lot::Mutex::new(None));
    let noted = Arc::clone(&during_second);
    let mut blocks = 0;

    engine
        .pipeline(&source)
        .shutdown(shutdown)
        .sink_confirms(SinkConfirmation::Remote)
        .run(
            |batch| Ok(batch),
            move |_out: &WorkBatch<_>| {
                blocks += 1;
                if blocks == 2 {
                    *noted.lock() = seen.gauge("self_regulation_recv_block_bytes", &[]);
                    stop.cancel();
                }
                std::future::ready(Ok(()))
            },
        )
        .await
        .expect("clean shutdown");

    assert_eq!(*during_second.lock(), Some(payload_bytes(&[0, 1, 2])));
    assert_eq!(
        capture.gauge("self_regulation_recv_block_bytes", &[]),
        Some(payload_bytes(&[10]))
    );
}

#[cfg(all(feature = "metrics", feature = "transport-pipe"))]
#[tokio::test(flavor = "current_thread")]
async fn effective_guarantee_reports_best_effort_for_a_pipe_source() {
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let source = crate::transport::pipe::PipeTransport::new(
        &crate::transport::pipe::PipeTransportConfig::default(),
    );
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    default_engine()
        .pipeline(&source)
        .shutdown(shutdown)
        .sink_confirms(SinkConfirmation::Remote)
        .run(
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| std::future::ready(Ok(())),
        )
        .await
        .expect("clean shutdown");

    assert_eq!(
        capture.gauge(
            "pipeline_delivery_guarantee",
            &[
                ("guarantee", "best_effort"),
                ("reason", "source_cannot_ack")
            ]
        ),
        Some(1.0),
        "a pipe has no acknowledgement to hold, whatever the sink confirms"
    );
}

#[cfg(all(
    feature = "metrics",
    feature = "transport-kafka",
    feature = "transport-grpc"
))]
#[tokio::test(flavor = "current_thread")]
async fn effective_guarantee_reports_at_least_once_for_kafka_to_grpc() {
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    // No topics: a broker-free consumer, built without a subscribe.
    let source = crate::transport::kafka::KafkaTransport::new(
        &crate::transport::kafka::KafkaConfig::for_testing(
            "localhost:9092",
            "ack-test",
            Vec::new(),
        ),
    )
    .await
    .expect("broker-free kafka transport");
    // A lazily dialled client: nothing is sent, so no server is needed.
    let sink = crate::transport::grpc::GrpcTransport::new(
        &crate::transport::grpc::GrpcConfig::client("http://127.0.0.1:1"),
    )
    .await
    .expect("grpc client");
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    // The gRPC client confirms Remote: the next hop answers only once it holds the records.
    default_engine()
        .pipeline(&source)
        .shutdown(shutdown)
        .sender(&sink)
        .run(
            |batch| Ok(batch),
            |_out: &WorkBatch<_>| std::future::ready(Ok(())),
        )
        .await
        .expect("clean shutdown");

    assert_eq!(
        capture.gauge(
            "pipeline_delivery_guarantee",
            &[("guarantee", "at_least_once"), ("reason", "confirmed")]
        ),
        Some(1.0)
    );
}

/// The producer ceiling of [`small_ceiling_kafka_sender`].
#[cfg(feature = "transport-kafka")]
const SMALL_CEILING: usize = 2_000_000;

/// A broker-free Kafka sender whose producer refuses records over
/// [`SMALL_CEILING`] bytes.
#[cfg(feature = "transport-kafka")]
async fn small_ceiling_kafka_sender() -> crate::transport::kafka::KafkaTransport {
    let mut config =
        crate::transport::kafka::KafkaConfig::for_testing("localhost:9092", "ack-test", Vec::new());
    config.sizing.producer.message_max_bytes =
        Some(i32::try_from(SMALL_CEILING).expect("fits an i32"));
    crate::transport::kafka::KafkaTransport::new(&config)
        .await
        .expect("broker-free kafka transport")
}

/// A `process` that grows the record with seq 1 past [`SMALL_CEILING`].
#[cfg(feature = "transport-kafka")]
#[allow(clippy::unnecessary_wraps)] // the run loops' process returns a Result
fn grow_seq_one_past_the_ceiling(
    batch: WorkBatch<crate::transport::memory::MemoryToken>,
) -> Result<WorkBatch<crate::transport::memory::MemoryToken>, EngineError> {
    Ok(batch.map_records(|records| {
        records
            .into_iter()
            .map(|mut record| {
                if record.payload.as_ref() == br#"{"seq":1}"# {
                    record.payload = Bytes::from(vec![b'x'; SMALL_CEILING + 1]);
                }
                record
            })
            .collect()
    }))
}

#[cfg(all(feature = "metrics", feature = "transport-kafka"))]
#[tokio::test(flavor = "current_thread")]
async fn an_oversize_record_is_dropped_and_counted_never_delivered() {
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let sender = small_ceiling_kafka_sender().await;
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = Arc::clone(&sunk);
    let stop = shutdown.clone();

    default_engine()
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .sender(&sender)
        .run(grow_seq_one_past_the_ceiling, move |out: &WorkBatch<_>| {
            seen.lock()
                .extend(out.records.iter().map(|r| r.payload.len()));
            stop.cancel();
            std::future::ready(Ok(()))
        })
        .await
        .expect("clean shutdown");

    // One lock: a second in the failure message would deadlock on the first.
    let sunk = sunk.lock().clone();
    assert_eq!(
        sunk.len(),
        2,
        "the two good records reach the sink, the oversize one never does: {sunk:?}"
    );
    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Dropped)],
        "with no DLQ the block releases Dropped, never Delivered"
    );
    assert_eq!(
        capture.counter(
            "pipeline_dead_letters_dropped_total",
            &[("reason", "too_large")]
        ),
        1
    );
}

/// A routed sender answers the screen for the route each record's key selects,
/// so its Kafka route's ceiling reaches the pipeline. The default route has no
/// ceiling, so a key the routed sender ignored would send the record.
#[cfg(all(feature = "metrics", feature = "transport-kafka"))]
#[tokio::test(flavor = "current_thread")]
async fn an_oversize_record_through_a_routed_sender_is_screened_by_its_route() {
    use crate::transport::factory::AnySender;
    use crate::transport::routed::RoutedSender;

    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let routes = std::collections::HashMap::from([(
        "events.land".to_string(),
        AnySender::Kafka(small_ceiling_kafka_sender().await),
    )]);
    let sender = RoutedSender::new(routes, Some(AnySender::Memory(mem_transport(50))));
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = Arc::clone(&sunk);
    let stop = shutdown.clone();

    default_engine()
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .sender(&sender)
        .run(
            |batch| {
                let grown = grow_seq_one_past_the_ceiling(batch)?;
                Ok(grown.map_records(|records| {
                    records
                        .into_iter()
                        .map(|mut record| {
                            record.key = Some(Arc::from("events.land"));
                            record
                        })
                        .collect()
                }))
            },
            move |out: &WorkBatch<_>| {
                seen.lock()
                    .extend(out.records.iter().map(|r| r.payload.len()));
                stop.cancel();
                std::future::ready(Ok(()))
            },
        )
        .await
        .expect("clean shutdown");

    // One lock: a second in the failure message would deadlock on the first.
    let sunk = sunk.lock().clone();
    assert_eq!(
        sunk.len(),
        2,
        "the oversize record never reaches the sink: {sunk:?}"
    );
    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Dropped)]
    );
    assert_eq!(
        capture.counter(
            "pipeline_dead_letters_dropped_total",
            &[("reason", "too_large")]
        ),
        1
    );
}

/// A gRPC `send_batch` outside the pipeline leaves a record over its ceiling
/// out of a block it otherwise sends: that record is dropped, so it counts
/// with the dropped dead letters. A block of nothing but such records is
/// answered `FilteredDlq`, which every caller takes as handled, so those
/// count too.
#[cfg(all(feature = "metrics", feature = "transport-grpc"))]
#[tokio::test(flavor = "current_thread")]
async fn a_record_grpc_send_batch_leaves_out_counts_as_a_dropped_dead_letter() {
    use crate::transport::grpc::{GrpcConfig, GrpcTransport};
    use crate::transport::{SendResult, TransportSender};

    const LIMIT: usize = 1024;
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let server =
        GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0").with_max_message_size(LIMIT))
            .await
            .expect("server");
    let uri = format!("http://{}", server.local_addr().expect("bound"));
    let client = GrpcTransport::new(&GrpcConfig::client(&uri).with_max_message_size(LIMIT))
        .await
        .expect("client");
    let record = |len: usize| Record {
        payload: Bytes::from(vec![b'x'; len]),
        key: None,
        headers: vec![],
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    };
    let dropped = || {
        capture.counter(
            "pipeline_dead_letters_dropped_total",
            &[("reason", "too_large")],
        )
    };

    let sent = client
        .send_batch(&[record(10), record(LIMIT), record(10)])
        .await;
    assert!(matches!(sent, SendResult::Ok), "{sent:?}");
    let received = server.recv(10).await.expect("recv").records.len();
    assert_eq!(received, 2, "the two records within the ceiling arrive");
    assert_eq!(dropped(), 1, "the one left out is counted dropped");

    let refused = client.send_batch(&[record(LIMIT), record(LIMIT)]).await;
    assert!(refused.is_filtered_dlq(), "{refused:?}");
    assert_eq!(
        dropped(),
        3,
        "a block left out whole counts each record it drops"
    );
}

/// An armed gRPC server whose receive queue has no room refuses a push with
/// `ResourceExhausted` and counts it as refused, reason `full`.
#[cfg(all(feature = "metrics", feature = "transport-grpc"))]
#[tokio::test(flavor = "current_thread")]
async fn a_held_push_refused_by_a_full_queue_is_counted() {
    use crate::transport::grpc::{GrpcConfig, GrpcTransport};
    use crate::transport::{SendResult, TransportSender};

    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let mut config = GrpcConfig::server("127.0.0.1:0");
    config.recv_buffer_size = 1;
    let server = GrpcTransport::builder(&config)
        .armed(true)
        .start()
        .await
        .expect("server");
    let uri = format!("http://{}", server.local_addr().expect("bound"));
    let client = Arc::new(
        GrpcTransport::new(&GrpcConfig::client(&uri))
            .await
            .expect("client"),
    );

    // Held, and never taken by recv, so it fills the one-slot queue.
    let first = tokio::spawn({
        let client = Arc::clone(&client);
        async move {
            client
                .send("main", Bytes::from_static(b"{\"first\":1}"))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.ack_control().expect("held").held().count == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first push is held");

    let refused = client
        .send("main", Bytes::from_static(b"{\"second\":1}"))
        .await;
    assert!(matches!(refused, SendResult::Backpressured), "{refused:?}");
    assert_eq!(
        capture.counter(
            "transport_ack_refused_total",
            &[("transport", "grpc"), ("reason", "full")]
        ),
        1
    );
    drop(server);
    let _ = first.await;
}

/// A file DLQ writing under `dir/svc`.
#[cfg(feature = "dlq")]
fn file_dlq(dir: &std::path::Path, shutdown: &CancellationToken) -> Arc<crate::dlq::Dlq> {
    let config = crate::dlq::DlqConfig {
        file: crate::dlq::FileDlqConfig {
            enabled: true,
            path: dir.to_path_buf(),
            rotation: crate::dlq::RotationPeriod::Daily,
            max_age_days: 1,
            compress_rotated: false,
        },
        mode: crate::dlq::DlqMode::FileOnly,
        queue_capacity: 64,
        batch_size: 16,
        flush_interval_ms: 20,
        ..crate::dlq::DlqConfig::default()
    };
    Arc::new(crate::dlq::Dlq::spawn(&config, "svc", None, shutdown.clone()).expect("spawn dlq"))
}

/// Dead letters the file DLQ under `dir` holds.
#[cfg(feature = "dlq")]
async fn dlq_lines(dir: &std::path::Path) -> usize {
    tokio::fs::read_to_string(dir.join("svc/dlq.ndjson"))
        .await
        .map_or(0, |body| body.lines().count())
}

/// A `process` that dead-letters the record with seq 1.
#[cfg(feature = "dlq")]
#[allow(clippy::unnecessary_wraps)] // the run loops' process returns a Result
fn dead_letter_seq_one(
    batch: WorkBatch<crate::transport::memory::MemoryToken>,
) -> Result<WorkBatch<crate::transport::memory::MemoryToken>, EngineError> {
    let mut dead = Vec::new();
    let batch = batch.map_records(|records| {
        records
            .into_iter()
            .filter_map(|record| {
                if record.payload.as_ref() == br#"{"seq":1}"# {
                    dead.push(crate::transport::filter::FilteredDlqEntry {
                        payload: record.payload.to_vec(),
                        key: record.key.clone(),
                        reason: "poison".into(),
                    });
                    None
                } else {
                    Some(record)
                }
            })
            .collect()
    });
    Ok(batch.with_dlq_entries(dead))
}

/// A DLQ write that fails is retried with the block held, never released
/// `Errored` while the loop runs: once the DLQ takes it, the block releases
/// `Rejected`.
#[cfg(feature = "dlq")]
#[tokio::test]
async fn a_refused_dlq_write_is_retried_with_the_block_held() {
    let refusing = tempfile::tempdir().expect("tempdir");
    let dlq_shutdown = CancellationToken::new();
    let dlq = file_dlq(refusing.path(), &dlq_shutdown);
    let svc = refusing.path().join("svc");
    tokio::fs::remove_dir_all(&svc)
        .await
        .expect("remove dlq dir");
    tokio::fs::write(&svc, b"not a directory")
        .await
        .expect("plant file");
    let mended = svc.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        tokio::fs::remove_file(&mended)
            .await
            .expect("remove planted file");
        tokio::fs::create_dir(&mended)
            .await
            .expect("recreate dlq dir");
    });
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();

    tokio::time::timeout(
        Duration::from_secs(10),
        default_engine()
            .with_dlq(dlq)
            .pipeline(&source)
            .shutdown(shutdown)
            .run(dead_letter_seq_one, move |_out: &WorkBatch<_>| {
                stop.cancel();
                std::future::ready(Ok(()))
            }),
    )
    .await
    .expect("the DLQ recovers inside the wait")
    .expect("clean shutdown");

    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Rejected)],
        "held through the refusals, released once the DLQ holds it"
    );
    assert_eq!(dlq_lines(refusing.path()).await, 1);
    dlq_shutdown.cancel();
}

/// An entry no DLQ backend can ever hold is dropped and counted, not retried:
/// a Kafka-only DLQ whose ceiling the base64 entry is over.
#[cfg(all(feature = "dlq-kafka", feature = "metrics"))]
#[tokio::test(flavor = "current_thread")]
async fn a_dead_letter_no_dlq_backend_can_hold_is_dropped_and_counted() {
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let mut kafka =
        crate::transport::kafka::KafkaConfig::for_testing("127.0.0.1:1", "", Vec::new());
    kafka.sizing.producer.message_max_bytes =
        Some(i32::try_from(SMALL_CEILING).expect("fits an i32"));
    let config = crate::dlq::DlqConfig {
        mode: crate::dlq::DlqMode::KafkaOnly,
        file: crate::dlq::FileDlqConfig {
            enabled: false,
            ..crate::dlq::FileDlqConfig::default()
        },
        kafka: crate::dlq::KafkaDlqConfig {
            enabled: true,
            ..crate::dlq::KafkaDlqConfig::default()
        },
        ..crate::dlq::DlqConfig::default()
    };
    let dlq_shutdown = CancellationToken::new();
    let dlq = crate::dlq::Dlq::spawn(&config, "svc", Some(&kafka), dlq_shutdown.clone())
        .expect("spawn dlq");
    let sender = small_ceiling_kafka_sender().await;
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();

    tokio::time::timeout(
        Duration::from_secs(10),
        default_engine()
            .with_dlq(Arc::new(dlq))
            .pipeline(&source)
            .shutdown(shutdown)
            .sender(&sender)
            .run(grow_seq_one_past_the_ceiling, move |_out: &WorkBatch<_>| {
                stop.cancel();
                std::future::ready(Ok(()))
            }),
    )
    .await
    .expect("a refusal is not retried")
    .expect("clean shutdown");

    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Dropped)]
    );
    assert_eq!(
        capture.counter(
            "pipeline_dead_letters_dropped_total",
            &[("reason", "too_large")]
        ),
        1
    );
    dlq_shutdown.cancel();
}

#[cfg(feature = "dlq")]
#[tokio::test]
async fn dlq_routed_record_releases_after_the_dlq_confirms() {
    let dlq_shutdown = CancellationToken::new();

    // A DLQ that takes it: released Rejected once the write is confirmed.
    let holding = tempfile::tempdir().expect("tempdir");
    let dlq = file_dlq(holding.path(), &dlq_shutdown);
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();

    default_engine()
        .with_dlq(dlq)
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .run(dead_letter_seq_one, move |_out: &WorkBatch<_>| {
            stop.cancel();
            std::future::ready(Ok(()))
        })
        .await
        .expect("clean shutdown");

    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Rejected)]
    );
    assert_eq!(*source.commits.lock(), vec![0, 1, 2]);
    assert_eq!(
        dlq_lines(holding.path()).await,
        1,
        "the dead letter is in the DLQ"
    );
    dlq_shutdown.cancel();
}

/// A disabled DLQ drops every dead letter: the block releases `Dropped`, and
/// each counts in the pipeline's dropped counter as well as the DLQ's own.
#[cfg(all(feature = "dlq", feature = "metrics"))]
#[tokio::test(flavor = "current_thread")]
async fn a_disabled_dlq_counts_each_dead_letter_it_drops() {
    let capture = Arc::new(GaugeCapture::default());
    let _recorder = metrics::set_default_local_recorder(&*capture);
    let dlq = Arc::new(crate::dlq::Dlq::disabled());
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();

    default_engine()
        .with_dlq(Arc::clone(&dlq))
        .pipeline(&source)
        .shutdown(shutdown)
        .run(dead_letter_seq_one, move |_out: &WorkBatch<_>| {
            stop.cancel();
            std::future::ready(Ok(()))
        })
        .await
        .expect("clean shutdown");

    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Dropped)]
    );
    assert_eq!(
        capture.counter(
            "pipeline_dead_letters_dropped_total",
            &[("reason", "dead_letter")]
        ),
        1
    );
    assert_eq!(dlq.dropped(), 1, "the DLQ counts it too");
}

#[cfg(all(feature = "dlq", feature = "transport-kafka"))]
#[tokio::test]
async fn an_oversize_record_reaches_the_dlq_never_delivered() {
    let sender = small_ceiling_kafka_sender().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let dlq_shutdown = CancellationToken::new();
    let source = HeldSource::new(AckKind::Pull, vec![vec![0, 1, 2]]);
    let shutdown = CancellationToken::new();
    let sunk = Arc::new(parking_lot::Mutex::new(0_usize));
    let seen = Arc::clone(&sunk);
    let stop = shutdown.clone();

    default_engine()
        .with_dlq(file_dlq(dir.path(), &dlq_shutdown))
        .pipeline(&source)
        .shutdown(shutdown.clone())
        .sender(&sender)
        .run(grow_seq_one_past_the_ceiling, move |out: &WorkBatch<_>| {
            *seen.lock() += out.records.len();
            stop.cancel();
            std::future::ready(Ok(()))
        })
        .await
        .expect("clean shutdown");

    assert_eq!(*sunk.lock(), 2, "only the two good records reach the sink");
    assert_eq!(
        source.released(),
        vec![(vec![0, 1, 2], DeliveryStatus::Rejected)],
        "released once the DLQ holds the oversize record, never Delivered"
    );
    assert_eq!(dlq_lines(dir.path()).await, 1);
    dlq_shutdown.cancel();
}
