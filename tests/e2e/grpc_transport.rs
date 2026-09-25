// Project:   scalo
// File:      tests/e2e/grpc_transport.rs
// Purpose:   Integration tests for gRPC transport (bidirectional client/server)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the gRPC transport layer.
//!
//! These tests start real tonic gRPC servers and clients to verify
//! end-to-end message delivery. Run with `--test-threads=1` to avoid
//! port conflicts.
//!
//! `cargo test --test e2e_tests --features transport-grpc -- --test-threads=1`

use std::time::Duration;

use std::sync::Arc;

use scalo::transport::grpc::{GrpcConfig, GrpcToken, GrpcTransport};
use scalo::transport::{
    AcknowledgementsConfig, DeliveryStatus, PayloadFormat, Record, RecordMeta, SendResult,
    TransportBase, TransportError, TransportReceiver, TransportSender, WorkBatch,
};

/// Find an available port for testing.
async fn find_available_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind to ephemeral port");
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn test_close_frees_port_and_is_idempotent() {
    let port = find_available_port().await;
    let addr = format!("127.0.0.1:{port}");

    let server = GrpcTransport::new(&GrpcConfig::server(&addr))
        .await
        .expect("first server should bind");
    // close() must actually stop the listener (not just on Drop), and be
    // idempotent.
    server.close().await.expect("first close");
    server.close().await.expect("close is idempotent");

    // The serve task needs a moment to react to the shutdown signal and drop
    // the listener; poll-retry the rebind rather than sleep a fixed budget.
    let mut rebound = None;
    for _ in 0..40 {
        match GrpcTransport::new(&GrpcConfig::server(&addr)).await {
            Ok(s) => {
                rebound = Some(s);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    assert!(
        rebound.is_some(),
        "port {port} not freed within 2s after close() -- the listener did not stop"
    );
}

/// Create a server+client pair on a given port.
async fn create_pair(port: u16) -> (GrpcTransport, GrpcTransport) {
    let addr = format!("127.0.0.1:{port}");

    let server_config = GrpcConfig::server(&addr);
    // No post-construction sleep needed: GrpcTransport::new binds the
    // listener synchronously, so the server is accepting connections the
    // moment this returns.
    let server = GrpcTransport::new(&server_config)
        .await
        .expect("failed to create server");

    let client_config = GrpcConfig::client(&format!("http://{addr}"));
    let client = GrpcTransport::new(&client_config)
        .await
        .expect("failed to create client");

    (server, client)
}

#[tokio::test]
async fn test_send_and_receive() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    // Send a message
    let result = client
        .send("test-topic", bytes::Bytes::from_static(b"hello world"))
        .await;
    assert!(
        matches!(result, SendResult::Ok),
        "send should succeed: {result:?}"
    );

    // Receive the message
    tokio::time::sleep(Duration::from_millis(50)).await;
    let records = server.recv(10).await.expect("recv should succeed").records;

    assert_eq!(records.len(), 1, "should receive exactly one record");
    assert_eq!(records[0].payload.as_ref(), b"hello world");
    assert_eq!(records[0].key.as_deref(), Some("test-topic"));

    // Cleanup
    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_multiple_messages() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    // Send 10 messages
    for i in 0..10u32 {
        let payload = format!("message-{i}");
        let result = client.send("topic", bytes::Bytes::from(payload)).await;
        assert!(
            matches!(result, SendResult::Ok),
            "send {i} should succeed: {result:?}"
        );
    }

    // Receive all messages
    tokio::time::sleep(Duration::from_millis(100)).await;
    let records = server.recv(100).await.expect("recv should succeed").records;

    assert_eq!(records.len(), 10, "should receive all 10 records");

    // Verify ordering (sequence numbers should be monotonically increasing)
    for (i, record) in records.iter().enumerate() {
        let expected = format!("message-{i}");
        assert_eq!(
            record.payload,
            expected.as_bytes(),
            "record {i} payload mismatch"
        );
    }

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_large_payload() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    // Send 1MB payload
    let payload = vec![0xABu8; 1024 * 1024];
    let result = client.send("large", bytes::Bytes::from(payload)).await;
    assert!(
        matches!(result, SendResult::Ok),
        "large send should succeed: {result:?}"
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    let records = server.recv(10).await.expect("recv should succeed").records;

    assert_eq!(records.len(), 1, "should receive the large record");
    assert_eq!(records[0].payload.len(), 1024 * 1024);
    assert!(
        records[0].payload.iter().all(|&b| b == 0xAB),
        "payload should be intact"
    );

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_commit_is_noop() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    // Send and receive
    let _ = client
        .send("topic", bytes::Bytes::from_static(b"data"))
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let batch = server.recv(10).await.expect("recv should succeed");
    assert!(!batch.records.is_empty());

    // Commit tokens — should succeed (no-op)
    let result = server.commit(&batch.commit_tokens).await;
    assert!(result.is_ok(), "commit should succeed (no-op): {result:?}");

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_close_prevents_operations() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    // Close the transport
    client.close().await.expect("close should succeed");

    // Send after close should fail
    let result = client
        .send("topic", bytes::Bytes::from_static(b"data"))
        .await;
    assert!(
        matches!(result, SendResult::Fatal(_)),
        "send after close should fail: {result:?}"
    );

    // Close the server too
    server.close().await.expect("close should succeed");

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_health_check() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    assert!(server.is_healthy(), "server should be healthy before close");
    assert!(client.is_healthy(), "client should be healthy before close");

    server.close().await.expect("close should succeed");
    assert!(
        !server.is_healthy(),
        "server should not be healthy after close"
    );

    client.close().await.expect("close should succeed");
    assert!(
        !client.is_healthy(),
        "client should not be healthy after close"
    );
}

#[tokio::test]
async fn test_compression() {
    let port = find_available_port().await;
    let addr = format!("127.0.0.1:{port}");

    let server_config = GrpcConfig::server(&addr).with_compression();
    let server = GrpcTransport::new(&server_config)
        .await
        .expect("failed to create compressed server");

    let client_config = GrpcConfig::client(&format!("http://{addr}")).with_compression();
    let client = GrpcTransport::new(&client_config)
        .await
        .expect("failed to create compressed client");

    // Send and receive with compression
    let payload = b"compressed payload test data";
    let result = client
        .send("compressed", bytes::Bytes::from_static(payload))
        .await;
    assert!(
        matches!(result, SendResult::Ok),
        "compressed send should succeed: {result:?}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let records = server.recv(10).await.expect("recv should succeed").records;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload.as_ref(), payload);

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_route_batch_native_transport() {
    // Native batch transport: a whole WorkBatch's records cross the
    // wire in ONE RouteBatch RPC, payloads OPAQUE. Include a non-UTF8 binary
    // payload (NOT valid JSON or MsgPack) to prove no codec ran in transit.
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    let records = vec![
        Record {
            payload: bytes::Bytes::from_static(b"{\"a\":1}"),
            key: Some(Arc::from("events")),
            headers: vec![("trace".to_string(), b"abc".to_vec())],
            metadata: RecordMeta {
                timestamp_ms: Some(1_717_000_000_000),
                format: PayloadFormat::Json,
            },
        },
        Record {
            // Non-UTF8 binary: not JSON, not MsgPack -- must survive intact.
            payload: bytes::Bytes::from_static(&[0x00, 0xff, 0xfe, 0x80, 0x01]),
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Auto,
            },
        },
        Record {
            payload: bytes::Bytes::from_static(&[0x81, 0xa1, b'k', 0x07]),
            key: Some(Arc::from("metrics")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: Some(42),
                format: PayloadFormat::MsgPack,
            },
        },
    ];

    let result = client.send_batch(&records).await;
    assert!(
        matches!(result, SendResult::Ok),
        "send_batch should succeed: {result:?}"
    );

    // Records fan into the same mpsc channel the single-message path uses, so
    // the unchanged recv() trait path delivers them.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let received = server.recv(100).await.expect("recv should succeed").records;

    assert_eq!(received.len(), 3, "should receive all 3 batch records");

    // Record 0: JSON payload + key preserved.
    assert_eq!(received[0].payload.as_ref(), b"{\"a\":1}");
    assert_eq!(received[0].key.as_deref(), Some("events"));

    // Record 1: the non-UTF8 binary payload survived byte-for-byte (opaque).
    assert_eq!(
        received[1].payload.as_ref(),
        &[0x00, 0xff, 0xfe, 0x80, 0x01]
    );
    assert_eq!(received[1].key, None);

    // Record 2: MsgPack-lead payload + key preserved.
    assert_eq!(received[2].payload.as_ref(), &[0x81, 0xa1, b'k', 0x07]);
    assert_eq!(received[2].key.as_deref(), Some("metrics"));

    let _ = client.close().await;
    let _ = server.close().await;
}

/// Build a server with an explicit `recv_buffer_size` (channel capacity).
async fn create_pair_with_capacity(port: u16, capacity: usize) -> (GrpcTransport, GrpcTransport) {
    let addr = format!("127.0.0.1:{port}");

    let mut server_config = GrpcConfig::server(&addr);
    server_config.recv_buffer_size = capacity;
    let server = GrpcTransport::new(&server_config)
        .await
        .expect("failed to create server");

    let client_config = GrpcConfig::client(&format!("http://{addr}"));
    let client = GrpcTransport::new(&client_config)
        .await
        .expect("failed to create client");

    (server, client)
}

/// Atomicity: a `RouteBatch` larger than the free receiver capacity must be
/// rejected ALL-OR-NOTHING. With one of two slots taken and a 2-record batch,
/// the RPC errors (Backpressured) AND the receiver accepts ZERO of its records
/// -- no partial-acceptance window. This is the contract the doc-comment on
/// `send_batch` claims ("no partial-send window: the block is accepted or not
/// as a unit"). The failure mode it pins: the server enqueues record 0 then
/// errors on record 1, leaving 1 record stranded in the channel = partial
/// acceptance + duplicate-on-retry.
#[tokio::test]
async fn test_route_batch_is_atomic_under_capacity() {
    let port = find_available_port().await;
    let (server, client) = create_pair_with_capacity(port, 2).await;
    let ahead = client
        .send("events", bytes::Bytes::from_static(b"{\"ahead\":1}"))
        .await;
    assert!(matches!(ahead, SendResult::Ok), "{ahead:?}");

    let records = vec![
        Record {
            payload: bytes::Bytes::from_static(b"{\"r\":0}"),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
        Record {
            payload: bytes::Bytes::from_static(b"{\"r\":1}"),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
    ];

    // Batch of 2 with one slot free: cannot fit, must reject atomically.
    let result = client.send_batch(&records).await;
    assert!(
        matches!(result, SendResult::Backpressured),
        "over-capacity batch must surface as backpressure, got {result:?}"
    );

    // The receiver must hold only the record queued ahead -- none of the
    // batch. Any batch record present proves a partial-acceptance window.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let received = server.recv(10).await.expect("recv should succeed").records;
    assert_eq!(
        received.len(),
        1,
        "atomic batch must accept 0 records on rejection, got {} (partial acceptance)",
        received.len().saturating_sub(1)
    );
    assert_eq!(received[0].payload.as_ref(), b"{\"ahead\":1}");

    let _ = client.close().await;
    let _ = server.close().await;
}

/// Atomicity: a `RouteBatch` that FITS the free capacity succeeds and
/// the receiver accepts the whole batch. Capacity 2, batch 2 -> Ok + 2 records.
#[tokio::test]
async fn test_route_batch_fits_capacity_accepts_all() {
    let port = find_available_port().await;
    let (server, client) = create_pair_with_capacity(port, 2).await;

    let records = vec![
        Record {
            payload: bytes::Bytes::from_static(b"{\"r\":0}"),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
        Record {
            payload: bytes::Bytes::from_static(b"{\"r\":1}"),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
    ];

    let result = client.send_batch(&records).await;
    assert!(
        matches!(result, SendResult::Ok),
        "in-capacity batch should succeed: {result:?}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let received = server.recv(10).await.expect("recv should succeed").records;
    assert_eq!(received.len(), 2, "should receive both records");

    let _ = client.close().await;
    let _ = server.close().await;
}

/// Atomicity: a pressure-holding governor must reject the WHOLE
/// `RouteBatch` with `unavailable` BEFORE accepting ANY record -- consistent
/// with all-or-nothing. With the governor pinned high, a batch of 2 surfaces as
/// Backpressured and the receiver accepts ZERO records.
#[cfg(feature = "governor")]
#[tokio::test]
async fn test_route_batch_pressure_hold_accepts_nothing() {
    use scalo::governor::{Hysteresis, MemoryPressureSource, PressureSource, UnifiedPressure};
    use scalo::memory::{MemoryGuard, MemoryGuardConfig};

    let port = find_available_port().await;
    let addr = format!("127.0.0.1:{port}");

    let guard = Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1000,
        pressure_threshold: 0.80,
        ..Default::default()
    }));
    guard.add_bytes(950); // 95% -> hold
    let pressure = Arc::new(UnifiedPressure::new(
        vec![Arc::new(MemoryPressureSource::new(Arc::clone(&guard))) as Arc<dyn PressureSource>],
        Hysteresis::new(0.80, 0.65).expect("valid band"),
    ));
    assert!(pressure.should_hold(), "pinned-high governor must hold");

    // Server bound to the governor, ample channel capacity (so the rejection is
    // purely pressure-driven, NOT capacity-driven).
    let mut server_config = GrpcConfig::server(&addr);
    server_config.recv_buffer_size = 100;
    let server = GrpcTransport::with_pressure(&server_config, Some(Arc::clone(&pressure)))
        .await
        .expect("failed to create server");

    let client = GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
        .await
        .expect("failed to create client");

    let records = vec![
        Record {
            payload: bytes::Bytes::from_static(b"{\"r\":0}"),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
        Record {
            payload: bytes::Bytes::from_static(b"{\"r\":1}"),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        },
    ];

    let result = client.send_batch(&records).await;
    assert!(
        matches!(result, SendResult::Backpressured),
        "batch under pressure must surface as backpressure, got {result:?}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let received = server.recv(10).await.expect("recv should succeed").records;
    assert_eq!(
        received.len(),
        0,
        "pressure-held batch must accept 0 records, got {}",
        received.len()
    );

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_route_batch_empty() {
    // An empty batch is a valid, harmless no-op over the wire.
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    let result = client.send_batch(&[]).await;
    assert!(
        matches!(result, SendResult::Ok),
        "empty send_batch should succeed: {result:?}"
    );

    let _ = client.close().await;
    let _ = server.close().await;
}

fn json_record(seq: u32) -> Record {
    Record {
        payload: bytes::Bytes::from(format!("{{\"seq\":{seq}}}")),
        key: Some(Arc::from("main")),
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

/// Receive until the transport reports `Closed`, counting records. Bounded, so
/// a recv that never ends fails the test instead of hanging it.
async fn drain_until_closed(server: &GrpcTransport) -> Result<usize, String> {
    let mut delivered = 0;
    for _ in 0..1000 {
        match server.recv(100).await {
            Ok(batch) => delivered += batch.records.len(),
            Err(TransportError::Closed) => return Ok(delivered),
            Err(e) => return Err(format!("recv failed after {delivered} records: {e}")),
        }
    }
    Err(format!(
        "recv never reported Closed; {delivered} records so far"
    ))
}

/// Every record the server acked reaches `recv`, even when `close()` comes
/// before the consumer read it -- with a blocking and a non-blocking `recv`.
#[tokio::test]
async fn acked_records_reach_recv_after_close() {
    for recv_timeout_ms in [100, 0] {
        let port = find_available_port().await;
        let addr = format!("127.0.0.1:{port}");
        let mut server_config = GrpcConfig::server(&addr);
        server_config.recv_timeout_ms = recv_timeout_ms;
        let server = GrpcTransport::new(&server_config).await.expect("server");
        let client = GrpcTransport::new(&GrpcConfig::client(&format!("http://{addr}")))
            .await
            .expect("client");

        let batch = client
            .send_batch(&[json_record(162), json_record(163)])
            .await;
        assert!(matches!(batch, SendResult::Ok), "{batch:?}");
        let single = client
            .send("main", bytes::Bytes::from_static(b"{\"seq\":164}"))
            .await;
        assert!(matches!(single, SendResult::Ok), "{single:?}");

        server.close().await.expect("close");
        let delivered = drain_until_closed(&server).await;
        assert_eq!(
            delivered,
            Ok(3),
            "recv_timeout_ms={recv_timeout_ms}: the server acked 3 records and recv \
             returned {delivered:?} of them after close()"
        );
        assert!(
            matches!(server.recv(100).await, Err(TransportError::Closed)),
            "Closed must stay terminal once drained"
        );
        let _ = client.close().await;
    }
}

/// Senders still pushing while the server closes: every record acked to them
/// is one `recv` returns, whichever side of `close()` its RPC landed on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_acked_record_is_delivered_when_close_races_the_senders() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;
    let client = Arc::new(client);
    let stop = Arc::new(AtomicBool::new(false));

    let mut senders = tokio::task::JoinSet::new();
    for sender in 0..4_u32 {
        let client = Arc::clone(&client);
        let stop = Arc::clone(&stop);
        senders.spawn(async move {
            let mut acked = 0_usize;
            let mut seq = sender * 1_000_000;
            while !stop.load(Ordering::Relaxed) {
                if client
                    .send_batch(&[json_record(seq), json_record(seq + 1)])
                    .await
                    .is_ok()
                {
                    acked += 2;
                }
                seq += 2;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            acked
        });
    }

    // Acks pile up unread, as behind a consumer that stopped calling recv.
    tokio::time::sleep(Duration::from_millis(200)).await;
    server.close().await.expect("close");
    let delivered = drain_until_closed(&server).await.expect("drain");
    stop.store(true, Ordering::Relaxed);

    let mut acked = 0;
    while let Some(count) = senders.join_next().await {
        acked += count.expect("sender task");
    }
    assert!(acked > 0, "the senders never got an ack");
    assert_eq!(
        delivered, acked,
        "{acked} records acked to the senders, {delivered} returned by recv"
    );
}

/// A push that reaches the server after `close()` is refused with a status the
/// sender retries, never acked: nothing is left to deliver it.
#[tokio::test]
async fn a_push_after_close_is_refused_not_acked() {
    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;

    // Warm the connection, so the push after close() rides an open HTTP/2
    // connection rather than a fresh connect the stopped listener refuses.
    let warm = client
        .send("main", bytes::Bytes::from_static(b"{\"warm\":1}"))
        .await;
    assert!(matches!(warm, SendResult::Ok), "{warm:?}");
    assert_eq!(server.recv(10).await.expect("recv").records.len(), 1);

    server.close().await.expect("close");
    let after = client
        .send("main", bytes::Bytes::from_static(b"{\"after\":1}"))
        .await;
    let batch_after = client.send_batch(&[json_record(1)]).await;

    let delivered = drain_until_closed(&server).await;
    assert!(
        after.is_backpressured(),
        "a push after close() must come back retryable, got {after:?}"
    );
    assert!(
        batch_after.is_backpressured(),
        "a RouteBatch after close() must come back retryable, got {batch_after:?}"
    );
    assert_eq!(delivered, Ok(0), "nothing sent after close() may be queued");

    let _ = client.close().await;
}

/// Send `records` as one block, taking what the server queued between tries.
/// Returns the tries it took to land, or `None` if it never did.
async fn land_block(
    server: &GrpcTransport,
    client: &GrpcTransport,
    records: &[Record],
    taken: &mut Vec<Record>,
) -> Option<usize> {
    for attempt in 1..=5 {
        if matches!(client.send_batch(records).await, SendResult::Ok) {
            return Some(attempt);
        }
        taken.extend(server.recv(100).await.expect("recv").records);
    }
    None
}

/// A `RouteBatch` with more records than `recv_buffer_size` lands whole on the
/// first try, and `recv` hands all of it over in order, across calls and after
/// `close()`.
#[tokio::test]
async fn a_batch_larger_than_the_receive_buffer_lands_whole() {
    let port = find_available_port().await;
    let (server, client) = create_pair_with_capacity(port, 4).await;
    let block: Vec<Record> = (0..10).map(json_record).collect();

    let mut taken = Vec::new();
    let tries = land_block(&server, &client, &block, &mut taken).await;
    assert_eq!(
        tries,
        Some(1),
        "a 10-record block into a 4-record buffer never landed in 5 tries"
    );
    assert!(taken.is_empty(), "nothing lands before the block does");

    let first = server.recv(3).await.expect("recv").records;
    server.close().await.expect("close");
    let mut delivered = first;
    loop {
        match server.recv(3).await {
            Ok(batch) => delivered.extend(batch.records),
            Err(TransportError::Closed) => break,
            Err(e) => panic!("recv failed after {} records: {e}", delivered.len()),
        }
    }
    let payloads: Vec<_> = delivered.iter().map(|r| r.payload.clone()).collect();
    let sent: Vec<_> = block.iter().map(|r| r.payload.clone()).collect();
    assert_eq!(
        payloads, sent,
        "every record of the block, once each, in order"
    );

    let _ = client.close().await;
}

/// One oversize block waits at a time: a second is refused whole while the
/// first is still queued, and lands once `recv` has taken the first.
#[tokio::test]
async fn a_second_oversize_block_waits_for_the_first_to_be_taken() {
    let port = find_available_port().await;
    let (server, client) = create_pair_with_capacity(port, 2).await;
    let first: Vec<Record> = (0..5).map(json_record).collect();
    let second: Vec<Record> = (100..105).map(json_record).collect();

    let landed = client.send_batch(&first).await;
    assert!(matches!(landed, SendResult::Ok), "first block: {landed:?}");
    let refused = client.send_batch(&second).await;
    assert!(
        matches!(refused, SendResult::Backpressured),
        "second block while the first is queued: {refused:?}"
    );
    // A single record still has the record buffer to itself.
    let single = client
        .send("main", bytes::Bytes::from_static(b"{\"single\":1}"))
        .await;
    assert!(
        matches!(single, SendResult::Ok),
        "single record: {single:?}"
    );

    let taken = server.recv(100).await.expect("recv").records;
    assert_eq!(
        taken.len(),
        6,
        "the first block and the single record, none of the refused block"
    );
    let landed = client.send_batch(&second).await;
    assert!(
        matches!(landed, SendResult::Ok),
        "second block once the first was taken: {landed:?}"
    );
    assert_eq!(server.recv(100).await.expect("recv").records.len(), 5);

    let _ = client.close().await;
    let _ = server.close().await;
}

/// A loopback client that opens one Push stream and never sends its body, so
/// the server holds an in-flight RPC for as long as the socket stays open.
async fn stalled_push_stream(addr: &str) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;

    fn literal(out: &mut Vec<u8>, value: &str) {
        out.push(u8::try_from(value.len()).expect("short header value"));
        out.extend_from_slice(value.as_bytes());
    }
    // HPACK: indexed :method POST and :scheme http, then literal values for
    // :path, :authority and content-type against their static-table names.
    let mut block = vec![0x83, 0x86, 0x04];
    literal(&mut block, "/scalo.transport.v1.Transport/Push");
    block.push(0x01);
    literal(&mut block, addr);
    block.extend_from_slice(&[0x0f, 0x10]);
    literal(&mut block, "application/grpc");

    let mut frames = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    // Empty SETTINGS, then HEADERS on stream 1 with END_HEADERS and no
    // END_STREAM: the request body never follows.
    frames.extend_from_slice(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0]);
    let len = u32::try_from(block.len())
        .expect("short header block")
        .to_be_bytes();
    frames.extend_from_slice(&[len[1], len[2], len[3], 0x01, 0x04, 0, 0, 0, 1]);
    frames.extend_from_slice(&block);

    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&frames).await.expect("write request head");
    stream
}

/// Poll-rebind `addr` for up to 2 s; true once a new server can bind it.
async fn port_is_free(addr: &str) -> bool {
    for _ in 0..40 {
        if GrpcTransport::new(&GrpcConfig::server(addr)).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// `close()` stops the server even while a client holds an RPC open, without
/// waiting on that client, and the listener is free when it returns.
#[tokio::test]
async fn close_stops_the_server_while_a_client_holds_an_rpc_open() {
    let port = find_available_port().await;
    let addr = format!("127.0.0.1:{port}");
    let server = GrpcTransport::new(&GrpcConfig::server(&addr))
        .await
        .expect("server");
    let _stalled = stalled_push_stream(&addr).await;
    // Let the server read the request head and start the handler.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    let closing = tokio::time::timeout(Duration::from_secs(5), server.close()).await;
    let took = started.elapsed();
    assert!(
        matches!(closing, Ok(Ok(()))),
        "close() waited on the stalled client, got {closing:?}"
    );
    assert!(took < Duration::from_secs(1), "close() took {took:?}");
    assert!(
        GrpcTransport::new(&GrpcConfig::server(&addr)).await.is_ok(),
        "port {port} still bound when close() returned -- the server outlived close()"
    );
}

/// Dropping the transport without `close()` stops the server too: a dropped
/// `JoinHandle` detaches its task, so the serve task is aborted explicitly.
#[tokio::test]
async fn drop_stops_the_server_while_a_client_holds_an_rpc_open() {
    let port = find_available_port().await;
    let addr = format!("127.0.0.1:{port}");
    let server = GrpcTransport::new(&GrpcConfig::server(&addr))
        .await
        .expect("server");
    let _stalled = stalled_push_stream(&addr).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(server);
    assert!(
        port_is_free(&addr).await,
        "port {port} still bound after drop -- the serve task outlived the transport"
    );
}

/// A listener that completes TCP but never speaks, holding every connection,
/// and the number of connections it has accepted.
async fn silent_listener() -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&accepts);
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            held.push(stream);
        }
    });
    (addr, accepts)
}

/// A TLS client to `addr` that trusts a throwaway CA, with the given send
/// limit. The CA is read when the transport is built.
async fn tls_client(addr: std::net::SocketAddr, send_timeout_ms: u64) -> GrpcTransport {
    use std::io::Write;

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert");
    let mut ca = tempfile::NamedTempFile::new().expect("ca file");
    ca.write_all(cert.cert.pem().as_bytes()).expect("write ca");
    ca.flush().expect("flush ca");

    let mut config = GrpcConfig::client(&format!("https://{addr}"));
    config.tls_enabled = true;
    config.tls_ca_path = Some(ca.path().to_string_lossy().into_owned());
    config.tls_domain = Some("localhost".to_string());
    config.send_timeout_ms = send_timeout_ms;
    GrpcTransport::new(&config).await.expect("client")
}

/// A TLS client to a server that accepts and never answers waits in the TLS
/// handshake, before the RPC starts; `send_timeout_ms` still ends the send.
#[tokio::test]
async fn a_send_to_a_server_that_never_answers_ends_at_send_timeout() {
    let (addr, _accepts) = silent_listener().await;
    let client = tls_client(addr, 300).await;

    let started = std::time::Instant::now();
    let single = tokio::time::timeout(
        Duration::from_secs(5),
        client.send("main", bytes::Bytes::from_static(b"{}")),
    )
    .await;
    let batch =
        tokio::time::timeout(Duration::from_secs(5), client.send_batch(&[json_record(1)])).await;
    let elapsed = started.elapsed();

    assert!(
        matches!(single, Ok(SendResult::Backpressured)),
        "send must end at send_timeout_ms as backpressure, got {single:?}"
    );
    assert!(
        matches!(batch, Ok(SendResult::Backpressured)),
        "send_batch must end at send_timeout_ms as backpressure, got {batch:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "two sends at a 300 ms limit took {elapsed:?}"
    );
}

/// A TLS handshake the server never answers is abandoned inside
/// `send_timeout_ms`, so each send dials afresh rather than queueing behind
/// the stalled one.
///
/// The dial gives up a tenth of the limit before its send does; a 1 s limit
/// keeps that 100 ms margin clear of scheduler stalls on a busy host.
#[tokio::test]
async fn each_send_after_a_stalled_tls_handshake_dials_afresh() {
    let (addr, accepts) = silent_listener().await;
    let client = tls_client(addr, 1_000).await;

    for _ in 0..3 {
        let sent = tokio::time::timeout(
            Duration::from_secs(5),
            client.send("main", bytes::Bytes::from_static(b"{}")),
        )
        .await;
        assert!(
            matches!(sent, Ok(SendResult::Backpressured)),
            "send must end at send_timeout_ms as backpressure, got {sent:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        accepts.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "three sends to a server that never answers should each dial it once"
    );
}

/// Accepted gRPC records count as received, never as sent: in one process the
/// sender's counts are the only sends.
#[cfg(feature = "metrics")]
#[tokio::test]
async fn receipts_count_as_received_not_sent() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    // Current-thread runtime: the server's tasks run on this thread and see it.
    let _local = metrics::set_default_local_recorder(&recorder);

    let port = find_available_port().await;
    let (server, client) = create_pair(port).await;
    let single = client
        .send("main", bytes::Bytes::from_static(b"{\"seq\":1}"))
        .await;
    assert!(matches!(single, SendResult::Ok), "{single:?}");
    let batch = client.send_batch(&[json_record(2), json_record(3)]).await;
    assert!(matches!(batch, SendResult::Ok), "{batch:?}");
    assert_eq!(server.recv(10).await.expect("recv").records.len(), 3);

    let rendered = handle.render();
    let value = |name: &str, labels: &[&str]| -> Option<f64> {
        rendered
            .lines()
            .filter(|line| line.starts_with(&format!("{name}{{")))
            .filter(|line| {
                let set = &line[name.len()..line.find('}').map_or(line.len(), |i| i + 1)];
                labels.iter().all(|l| set.contains(l)) && set.matches('=').count() == labels.len()
            })
            .find_map(|line| line.rsplit(' ').next()?.parse().ok())
    };

    assert_eq!(
        value("transport_sent_total", &["transport=\"grpc\""]),
        Some(1.0),
        "one Push was sent:\n{rendered}"
    );
    assert_eq!(
        value(
            "transport_sent_total",
            &["transport=\"grpc\"", "path=\"batch\""]
        ),
        Some(2.0),
        "one RouteBatch of two records was sent:\n{rendered}"
    );
    assert_eq!(
        value("transport_received_events_total", &["transport=\"grpc\""]),
        Some(3.0),
        "three records were received:\n{rendered}"
    );

    let _ = client.close().await;
    let _ = server.close().await;
}

#[tokio::test]
async fn test_recv_timeout_returns_empty() {
    let port = find_available_port().await;
    let addr = format!("127.0.0.1:{port}");

    // Create server with short timeout
    let mut server_config = GrpcConfig::server(&addr);
    server_config.recv_timeout_ms = 50;
    let server = GrpcTransport::new(&server_config)
        .await
        .expect("failed to create server");

    // Recv with no messages sent — should return empty after timeout
    let records = server.recv(10).await.expect("recv should succeed").records;
    assert!(
        records.is_empty(),
        "recv with no messages should return empty, got {} records",
        records.len()
    );

    let _ = server.close().await;
}

// --- Held responses ---

/// A receive server armed, as the engine arms it, holding at most
/// `max_held_bytes`, and its URI.
async fn armed_server(max_held_bytes: u64, drain_deadline: Duration) -> (GrpcTransport, String) {
    let config = GrpcConfig::server("127.0.0.1:0");
    let server = GrpcTransport::builder(&config)
        .max_held_bytes(max_held_bytes)
        .drain_deadline(drain_deadline)
        .start()
        .await
        .expect("server");
    server
        .ack_control()
        .expect("a receive server can hold")
        .arm();
    let uri = format!("http://{}", server.local_addr().expect("bound"));
    (server, uri)
}

/// Receive until `n` records have arrived.
async fn recv_all(server: &GrpcTransport, n: usize) -> WorkBatch<GrpcToken> {
    let mut all = server.recv(n).await.expect("recv");
    while all.records.len() < n {
        let more = server.recv(n - all.records.len()).await.expect("recv");
        all.records.extend(more.records);
        all.commit_tokens.extend(more.commit_tokens);
    }
    all
}

fn filled(len: usize) -> bytes::Bytes {
    bytes::Bytes::from(vec![b'x'; len])
}

/// A client whose sends run in their own tasks, so the test can watch them.
async fn spawned_client(uri: &str) -> Arc<GrpcTransport> {
    Arc::new(
        GrpcTransport::new(&GrpcConfig::client(uri))
            .await
            .expect("client"),
    )
}

#[tokio::test]
async fn held_push_waits_for_release() {
    let (server, uri) = armed_server(1 << 20, Duration::from_secs(20)).await;
    let client = spawned_client(&uri).await;
    let sending = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send_batch(&[json_record(1), json_record(2)]).await }
    });

    let batch = recv_all(&server, 2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !sending.is_finished(),
        "the push was answered before its records were released"
    );

    server
        .release(&batch.commit_tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    let result = tokio::time::timeout(Duration::from_secs(1), sending)
        .await
        .expect("answered once released")
        .expect("send task");
    assert!(matches!(result, SendResult::Ok), "{result:?}");
    assert_eq!(server.ack_control().expect("held").held().count, 0);
}

#[tokio::test]
async fn held_push_answers_unavailable_on_errored_release() {
    let (server, uri) = armed_server(1 << 20, Duration::from_secs(20)).await;
    let client = spawned_client(&uri).await;
    let sending = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send("main", filled(8)).await }
    });

    let batch = recv_all(&server, 1).await;
    server
        .release(&batch.commit_tokens, DeliveryStatus::Errored)
        .await
        .expect("release");
    let result = sending.await.expect("send task");
    assert!(
        matches!(result, SendResult::Backpressured),
        "an Errored release must make the sender retry: {result:?}"
    );
}

#[tokio::test]
async fn held_push_answers_before_the_client_deadline() {
    use scalo::transport::grpc::proto;

    let (server, uri) = armed_server(1 << 20, Duration::from_secs(20)).await;

    // Through the transport: a 2 s send limit ends in a retry, not an OK.
    let mut config = GrpcConfig::client(&uri);
    config.send_timeout_ms = 2_000;
    let client = GrpcTransport::new(&config).await.expect("client");
    let started = std::time::Instant::now();
    let result = client.send("main", filled(8)).await;
    assert!(matches!(result, SendResult::Backpressured), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(2));

    // On the wire: Unavailable with the expiry trailer, never Cancelled or
    // DeadlineExceeded, which a sender cannot tell from a crash.
    let mut raw = proto::transport_client::TransportClient::connect(uri)
        .await
        .expect("raw client");
    let mut request = tonic::Request::new(proto::PushRequest {
        payload: filled(8),
        format: proto::Format::Auto.into(),
        metadata: std::collections::HashMap::new(),
    });
    request.set_timeout(Duration::from_secs(2));
    let started = std::time::Instant::now();
    let status = raw.push(request).await.expect_err("never released");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
    assert_eq!(
        status
            .metadata()
            .get("scalo-hold-expired")
            .and_then(|v| v.to_str().ok()),
        Some("1"),
        "{status:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));

    // The records stay held until released, then free their bytes.
    let batch = recv_all(&server, 2).await;
    server
        .release(&batch.commit_tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    assert_eq!(server.ack_control().expect("held").held().bytes, 0);
}

#[tokio::test]
async fn admission_refuses_past_the_held_byte_ceiling() {
    const CEILING: u64 = 1 << 20;
    let (server, uri) = armed_server(CEILING, Duration::from_secs(20)).await;
    let client = spawned_client(&uri).await;

    let first = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send("main", filled(800 << 10)).await }
    });
    let held = recv_all(&server, 1).await;
    let second = client.send("main", filled(800 << 10)).await;
    assert!(
        matches!(second, SendResult::Backpressured),
        "past the ceiling: {second:?}"
    );
    let control = server.ack_control().expect("held");
    assert!(control.held().bytes <= CEILING, "{:?}", control.held());

    server
        .release(&held.commit_tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    assert!(matches!(first.await.expect("send"), SendResult::Ok));

    // Nothing held: one push over the ceiling on its own is admitted.
    let oversized = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send("main", filled(2 << 20)).await }
    });
    let batch = recv_all(&server, 1).await;
    server
        .release(&batch.commit_tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    assert!(matches!(oversized.await.expect("send"), SendResult::Ok));
}

#[tokio::test]
async fn disabled_or_unarmed_push_is_answered_at_enqueue() {
    // Unarmed: acknowledgements default on, but nobody promised to release.
    let unarmed = GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0"))
        .await
        .expect("server");
    // Armed, but acknowledgements disabled.
    let config = GrpcConfig::server("127.0.0.1:0");
    let disabled = GrpcTransport::builder(&config)
        .acknowledgements(AcknowledgementsConfig::new(false))
        .start()
        .await
        .expect("server");
    disabled.ack_control().expect("control").arm();
    assert!(!disabled.ack_control().expect("control").enabled());

    for server in [unarmed, disabled] {
        let uri = format!("http://{}", server.local_addr().expect("bound"));
        let client = GrpcTransport::new(&GrpcConfig::client(&uri))
            .await
            .expect("client");
        let result =
            tokio::time::timeout(Duration::from_secs(2), client.send_batch(&[json_record(1)]))
                .await
                .expect("answered at enqueue, no release needed");
        assert!(matches!(result, SendResult::Ok), "{result:?}");
        assert_eq!(server.recv(10).await.expect("recv").records.len(), 1);
        assert_eq!(server.ack_control().expect("control").held().count, 0);
    }
}

#[tokio::test]
async fn oversize_block_is_split_by_encoded_size() {
    const LIMIT: usize = 1024;
    let server =
        GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0").with_max_message_size(LIMIT))
            .await
            .expect("server");
    let uri = format!("http://{}", server.local_addr().expect("bound"));
    let client = GrpcTransport::new(&GrpcConfig::client(&uri).with_max_message_size(LIMIT))
        .await
        .expect("client");
    let records: Vec<Record> = (0..10)
        .map(|_| Record {
            payload: filled(300),
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        })
        .collect();

    // Ten 300-byte records encode past 1 KiB, so they go as several requests.
    let result = client.send_batch(&records).await;
    assert!(matches!(result, SendResult::Ok), "{result:?}");
    assert_eq!(recv_all(&server, 10).await.records.len(), 10);

    // A record over the limit on its own is named for the dead-letter queue.
    let lone = Record {
        payload: filled(LIMIT),
        ..records[0].clone()
    };
    assert!(client.dead_letter_reason(&lone).is_some());
    assert!(client.dead_letter_reason(&records[0]).is_none());
}

#[tokio::test]
async fn close_answers_held_pushes_unavailable_at_the_drain_deadline() {
    let (server, uri) = armed_server(1 << 20, Duration::from_millis(300)).await;
    let client = spawned_client(&uri).await;
    let sending = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send("main", filled(8)).await }
    });
    let _never_released = recv_all(&server, 1).await;

    server.close().await.expect("close");
    let result = tokio::time::timeout(Duration::from_secs(5), sending)
        .await
        .expect("answered at the drain deadline, not after the 25 s hold budget")
        .expect("send task");
    assert!(matches!(result, SendResult::Backpressured), "{result:?}");
    assert_eq!(server.ack_control().expect("held").held().count, 0);
}

#[tokio::test]
async fn a_dropped_responder_never_answers_ok() {
    // The process goes away with a push held: never OK.
    let (server, uri) = armed_server(1 << 20, Duration::from_secs(20)).await;
    let client = spawned_client(&uri).await;
    let sending = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send("main", filled(8)).await }
    });
    let _held = recv_all(&server, 1).await;
    drop(server);
    let result = tokio::time::timeout(Duration::from_secs(5), sending)
        .await
        .expect("answered once the server is gone")
        .expect("send task");
    assert!(
        !matches!(result, SendResult::Ok),
        "a push whose server went away was answered OK"
    );

    // The sender goes away with a push held: its records still release and
    // free what they held, and the next push is held as normal.
    let (server, uri) = armed_server(1 << 20, Duration::from_secs(20)).await;
    let client = spawned_client(&uri).await;
    let abandoned = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.send("main", filled(8)).await }
    });
    let batch = recv_all(&server, 1).await;
    abandoned.abort();
    let _ = abandoned.await;
    server
        .release(&batch.commit_tokens, DeliveryStatus::Delivered)
        .await
        .expect("release");
    let control = server.ack_control().expect("held");
    assert_eq!(control.held().count, 0);
    assert_eq!(control.held().bytes, 0);
}

/// The engine's pipeline loop driving a gRPC receive server end to end: it
/// arms the server, sinks each block, and releases it with the sink's outcome.
#[cfg(feature = "worker-batch")]
mod engine_driven {
    use super::*;
    use scalo::worker::engine::BlockPieces;
    use scalo::worker::{BatchEngine, BatchProcessingConfig, EngineError};
    use tokio_util::sync::CancellationToken;

    /// A record the sink fails, as a fan-out destination that did not take it.
    const FAIL: &[u8] = b"{\"fail\":true}";

    /// Run the pipeline over `server` until `shutdown`, failing every block
    /// that carries a [`FAIL`] record through a piece reported `Errored`.
    fn run_pipeline(
        server: Arc<GrpcTransport>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<Result<(), EngineError>> {
        tokio::spawn(async move {
            let engine = BatchEngine::new(BatchProcessingConfig::default());
            engine
                .pipeline(&*server)
                .shutdown(shutdown)
                .run_with_pieces(
                    Ok,
                    |out: &WorkBatch<GrpcToken>, pieces: &BlockPieces<'_>| {
                        let failed = out.records.iter().any(|r| r.payload.as_ref() == FAIL);
                        if failed {
                            pieces.piece().report(DeliveryStatus::Errored);
                        }
                        std::future::ready(Ok(()))
                    },
                )
                .await
        })
    }

    /// Wait until the pipeline has armed `server`, as it does before its
    /// first `recv`.
    async fn armed_by_the_pipeline(server: &GrpcTransport) {
        let control = server.ack_control().expect("a receive server can hold");
        for _ in 0..200 {
            if control.is_armed() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the pipeline never armed the server");
    }

    #[tokio::test]
    async fn the_engine_answers_a_held_push_from_the_sink_outcome() {
        let server = Arc::new(
            GrpcTransport::new(&GrpcConfig::server("127.0.0.1:0"))
                .await
                .expect("server"),
        );
        let uri = format!("http://{}", server.local_addr().expect("bound"));
        let shutdown = CancellationToken::new();
        let running = run_pipeline(Arc::clone(&server), shutdown.clone());
        armed_by_the_pipeline(&server).await;
        let client = GrpcTransport::new(&GrpcConfig::client(&uri))
            .await
            .expect("client");

        // Sink Ok: the answer is OK, and only once the engine released it.
        let delivered = client.send("main", filled(8)).await;
        assert!(matches!(delivered, SendResult::Ok), "{delivered:?}");

        // A piece reported Errored: the answer is Unavailable, and the loop
        // goes on to the next push.
        let failed = client.send("main", bytes::Bytes::from_static(FAIL)).await;
        assert!(matches!(failed, SendResult::Backpressured), "{failed:?}");
        let after = client.send("main", filled(8)).await;
        assert!(matches!(after, SendResult::Ok), "{after:?}");

        shutdown.cancel();
        running
            .await
            .expect("pipeline task")
            .expect("clean shutdown");
        let held = server.ack_control().expect("held").held();
        assert_eq!((held.count, held.bytes), (0, 0), "{held:?}");
    }

    #[tokio::test]
    async fn with_acknowledgements_off_the_engine_answers_at_enqueue() {
        let config = GrpcConfig::server("127.0.0.1:0");
        let server = Arc::new(
            GrpcTransport::builder(&config)
                .acknowledgements(AcknowledgementsConfig::new(false))
                .start()
                .await
                .expect("server"),
        );
        let uri = format!("http://{}", server.local_addr().expect("bound"));
        let shutdown = CancellationToken::new();
        // Releases at receipt tokens the server never held, which it ignores.
        let running = run_pipeline(Arc::clone(&server), shutdown.clone());
        let client = GrpcTransport::new(&GrpcConfig::client(&uri))
            .await
            .expect("client");

        let result = client.send("main", bytes::Bytes::from_static(FAIL)).await;
        assert!(
            matches!(result, SendResult::Ok),
            "answered at enqueue, before the sink failed it: {result:?}"
        );

        shutdown.cancel();
        running
            .await
            .expect("pipeline task")
            .expect("clean shutdown");
        let control = server.ack_control().expect("control");
        assert!(!control.is_armed(), "a disabled source is not armed");
        assert_eq!(control.held().count, 0);
    }
}
