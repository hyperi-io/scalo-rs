// Project:   scalo
// File:      tests/fault_injection_redis.rs
// Purpose:   Real-broker fault-injection (no mocks) -- Redis via testcontainers
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-broker fault-injection tests against a Redis container.
//!
//! NO mocks: these exercise scalo's actual Redis transport + TieredSink against
//! a REAL Redis, injecting a genuine outage by pausing/unpausing the container
//! (the container stays put, so its host port is stable across the outage). They
//! prove the spill-on-failure -> drain-on-recovery cycle preserves every record
//! end to end.
//!
//! `#[ignore]` because they need a running Docker daemon -- run on a Docker host:
//!
//! ```text
//! cargo test --features transport-redis,tiered-sink,metrics,tracing \
//!     --test fault_injection_redis -- --ignored --nocapture
//! ```

#![cfg(all(feature = "transport-redis", feature = "tiered-sink"))]

use std::time::Duration;

use scalo::tiered_sink::{TieredSink, TieredSinkConfig};
use scalo::transport::redis_transport::{RedisTransport, RedisTransportConfig};
use scalo::transport::{PayloadFormat, Record, RecordMeta, TransportSender};
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const STREAM: &str = "scalo-fault-test";

/// Redis to test against.
///
/// Pinned here rather than taken from testcontainers-modules, whose default is
/// still 5.0 -- seven majors back and out of support since 2020. A crate's
/// default image tag is invisible to dependency review: Renovate reads
/// Cargo.toml, correctly reports the crate current, and never sees the image.
/// Hoisting the tag into our own source is what puts it back under review.
// renovate: datasource=docker depName=redis
const REDIS_TAG: &str = "8.8.1";

/// Start a Redis container and return it plus its connection URL.
async fn start_redis() -> (ContainerAsync<Redis>, String) {
    let node = Redis::default()
        .with_tag(REDIS_TAG)
        .start()
        .await
        .expect("start redis container");
    let port = node
        .get_host_port_ipv4(6379)
        .await
        .expect("redis host port");
    let url = format!("redis://127.0.0.1:{port}");
    (node, url)
}

fn rec(payload: &[u8]) -> Record {
    Record {
        payload: bytes::Bytes::copy_from_slice(payload),
        key: Some(std::sync::Arc::from(STREAM)),
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

fn redis_config(url: &str) -> RedisTransportConfig {
    RedisTransportConfig {
        url: url.to_string(),
        stream: Some(STREAM.to_string()),
        ..Default::default()
    }
}

/// Independent verification: how many entries are in the stream (XLEN), read via
/// a raw client so we never trust scalo's own read path to grade scalo's writes.
async fn stream_len(url: &str) -> usize {
    let client = redis::Client::open(url).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    redis::cmd("XLEN")
        .arg(STREAM)
        .query_async(&mut conn)
        .await
        .expect("XLEN")
}

/// Every payload currently in the stream (XRANGE - +), read via a raw client.
async fn stream_payloads(url: &str) -> Vec<Vec<u8>> {
    let client = redis::Client::open(url).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(STREAM)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .expect("XRANGE");
    reply
        .ids
        .iter()
        .filter_map(|id| id.get::<Vec<u8>>("payload"))
        .collect()
}

#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn redis_real_broker_roundtrip() {
    let (_node, url) = start_redis().await;
    let transport = RedisTransport::new(&redis_config(&url))
        .await
        .expect("connect redis transport");

    for i in 0..5 {
        let r = transport
            .send_batch(&[rec(format!("hot-{i}").as_bytes())])
            .await;
        assert!(r.is_ok(), "healthy send must succeed, got {r:?}");
    }

    assert_eq!(
        stream_len(&url).await,
        5,
        "all 5 records landed in the stream"
    );
}

#[tokio::test]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn tiered_sink_spills_on_redis_outage_and_drains_on_recovery() {
    let (node, url) = start_redis().await;
    let spool = tempfile::tempdir().expect("tempdir");

    let transport = RedisTransport::new(&redis_config(&url))
        .await
        .expect("connect redis transport");

    let mut cfg = TieredSinkConfig::new(spool.path().join("spill.queue"));
    cfg.circuit_failure_threshold = 1; // trip fast so we spill, not hammer
    cfg.circuit_reset_timeout_ms = 200; // probe recovery quickly
    cfg.drain_interval_ms = 10;
    cfg.send_timeout_ms = 500; // a paused server hangs -> timeout -> spill

    let tiered = TieredSink::new(transport, cfg).await.expect("tiered sink");

    // Phase 1 - healthy: records take the hot path straight to Redis.
    for i in 0..5 {
        tiered
            .send(&rec(format!("hot-{i}").as_bytes()))
            .await
            .unwrap();
    }
    // Give the hot path a moment, then confirm they reached the broker.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(stream_len(&url).await, 5, "hot-path records reached Redis");
    assert!(
        tiered.spool_is_empty().await,
        "nothing spilled while healthy"
    );

    // Phase 2 - OUTAGE: pause the container. The server freezes, so sends hang
    // and time out -> the records spill to the local disk cache.
    node.pause().await.expect("pause redis");
    for i in 0..5 {
        tiered
            .send(&rec(format!("cold-{i}").as_bytes()))
            .await
            .unwrap();
    }
    // The records could not reach the frozen broker, so they spilled to the
    // local cache. (We can't read XLEN here -- a paused server cannot answer;
    // the final post-recovery count proves nothing leaked + nothing was lost.)
    assert!(
        tiered.spool_len().await > 0,
        "records spilled during the outage"
    );

    // Phase 3 - RECOVERY: unpause. The drainer probes (half-open), recovers, and
    // replays the spooled records back to Redis.
    node.unpause().await.expect("unpause redis");
    let mut drained = false;
    for _ in 0..100 {
        if tiered.spool_is_empty().await {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(drained, "drainer must clear the spool after recovery");

    // At-least-once: EVERY record (5 hot + 5 spilled) must be present -- no loss.
    // Duplicates ARE allowed and expected: a send that timed out against the
    // frozen broker may have actually landed once the server unpaused AND been
    // replayed from the spool. That is the at-least-once contract (the dedup-key
    // hook is what a downstream uses to collapse such replays). We therefore
    // assert the UNIQUE set covers all 10, and the total is >= 10.
    let payloads = stream_payloads(&url).await;
    let unique: std::collections::BTreeSet<Vec<u8>> = payloads.iter().cloned().collect();
    let expected: std::collections::BTreeSet<Vec<u8>> = (0..5)
        .map(|i| format!("hot-{i}").into_bytes())
        .chain((0..5).map(|i| format!("cold-{i}").into_bytes()))
        .collect();
    assert_eq!(
        unique, expected,
        "every record delivered at least once -- no loss"
    );
    assert!(
        payloads.len() >= 10,
        "at-least-once: total >= 10 (duplicates allowed), got {}",
        payloads.len()
    );

    tiered.shutdown().await;
}
