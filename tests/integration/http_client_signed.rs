// Project:   scalo
// File:      tests/integration/http_client_signed.rs
// Purpose:   Signed-request path: one retry loop, re-signed per attempt
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The signing hook on the shared retry loop, against a real server.
//!
//! The fixture is an axum router on an ephemeral port, holding the listener it
//! was bound on rather than dropping it to hand the port over, so no second
//! process can take the address between bind and serve. Every request is
//! recorded, so an assertion is what went over the wire.
#![allow(unknown_lints)] // the next line's lint is newer than the MSRV toolchain
#![allow(clippy::unused_async_trait_impl)] // the trait contract stays async fn

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use reqwest::header::HeaderValue;
use scalo::http_client::{HttpClient, HttpClientConfig, RequestSigner, SignError, Unsigned};

/// What the fixture saw.
#[derive(Debug, Default)]
struct Recorded {
    flaky_hits: u32,
    throttled_hits: u32,
    plain_hits: u32,
    /// The `x-stamp` value of every request to the flaky route, in order.
    stamps: Vec<String>,
    /// The `authorization` value of every request to the plain route, in order.
    plain_auth: Vec<String>,
    /// Whether the signature on the digest route matched the body and query.
    digest_matched: Option<bool>,
}

type Shared = Arc<Mutex<Recorded>>;

/// A signer that stamps a fresh monotonic value on every attempt, so a test
/// can tell a re-signed retry from a replayed one.
struct StampSigner {
    next: AtomicU64,
}

impl RequestSigner for StampSigner {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        let stamp = self.next.fetch_add(1, Ordering::Relaxed);
        let value = HeaderValue::from_str(&stamp.to_string())
            .map_err(|e| SignError::with_cause("stamp is not a header value", e))?;
        request.headers_mut().insert("x-stamp", value);
        Ok(())
    }
}

/// A signer over the final request: the digest covers the query and the body as
/// they will be sent, which is the assertion a `RequestBuilder` cannot satisfy.
struct DigestSigner;

impl RequestSigner for DigestSigner {
    async fn sign(&self, request: &mut reqwest::Request) -> Result<(), SignError> {
        let query = request.url().query().unwrap_or_default().to_owned();
        let body = request
            .body()
            .and_then(reqwest::Body::as_bytes)
            .unwrap_or_default()
            .to_vec();
        let value = HeaderValue::from_str(&digest_of(&query, &body))
            .map_err(|e| SignError::with_cause("digest is not a header value", e))?;
        request.headers_mut().insert("x-signature", value);
        Ok(())
    }
}

/// FNV-1a over the query then the body. Both halves of the test compute it, so
/// it only has to agree with itself.
fn digest_of(query: &str, body: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in query.as_bytes().iter().chain(body) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// 503 once, then 200, recording the stamp each attempt carried.
async fn flaky(State(state): State<Shared>, headers: HeaderMap) -> (StatusCode, &'static str) {
    let mut recorded = state.lock().unwrap();
    recorded.flaky_hits += 1;
    recorded.stamps.push(header_value(&headers, "x-stamp"));
    if recorded.flaky_hits == 1 {
        (StatusCode::SERVICE_UNAVAILABLE, "come back")
    } else {
        (StatusCode::OK, "served")
    }
}

/// 429 with a one-second `Retry-After` once, then 200.
async fn throttled(State(state): State<Shared>) -> Response {
    let first = {
        let mut recorded = state.lock().unwrap();
        recorded.throttled_hits += 1;
        recorded.throttled_hits == 1
    };
    if first {
        (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            "slow down",
        )
            .into_response()
    } else {
        (StatusCode::OK, "served").into_response()
    }
}

/// 503 once, then 200, recording the credential the caller's own closure put on.
async fn plain(State(state): State<Shared>, headers: HeaderMap) -> (StatusCode, &'static str) {
    let mut recorded = state.lock().unwrap();
    recorded.plain_hits += 1;
    recorded
        .plain_auth
        .push(header_value(&headers, "authorization"));
    if recorded.plain_hits == 1 {
        (StatusCode::SERVICE_UNAVAILABLE, "come back")
    } else {
        (StatusCode::OK, "served")
    }
}

/// Recompute the digest over what arrived and record whether it matched.
async fn digest(
    State(state): State<Shared>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> StatusCode {
    let expected = digest_of(query.as_deref().unwrap_or_default(), &body);
    let seen = header_value(&headers, "x-signature");
    state.lock().unwrap().digest_matched = Some(seen == expected);
    StatusCode::OK
}

/// Bind, hold the listener, serve. The address is only handed out after the
/// server owns the socket.
async fn fixture() -> (SocketAddr, Shared) {
    let state: Shared = Arc::new(Mutex::new(Recorded::default()));
    let app = Router::new()
        .route("/flaky", get(flaky))
        .route("/throttled", get(throttled))
        .route("/plain", get(plain))
        .route("/digest", post(digest))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, state)
}

/// Retries fast enough that the schedule itself never dominates a test.
fn brisk_config() -> HttpClientConfig {
    HttpClientConfig {
        min_retry_interval_ms: 1,
        max_retry_interval_ms: 20,
        ..Default::default()
    }
}

/// The retry counter for the given method, as the global recorder holds it.
#[cfg(feature = "metrics")]
fn retries_total(manager: &scalo::metrics::MetricsManager, method: &str) -> u64 {
    let rendered = manager.render();
    rendered
        .lines()
        .find(|line| {
            line.starts_with("http_client_retries_total")
                && line.contains(&format!("method=\"{method}\""))
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
}

/// A retry re-runs the signer, so the second attempt carries a fresh
/// signature rather than a replay of the first.
///
/// The metric half needs the process-per-test isolation the repo's runner
/// (nextest) gives: the metrics recorder is global and installed once.
#[tokio::test]
async fn a_signed_request_is_re_signed_on_every_attempt() {
    #[cfg(feature = "metrics")]
    let manager = scalo::metrics::MetricsManager::with_config(scalo::metrics::MetricsConfig {
        namespace: String::new(),
        enable_process_metrics: false,
        enable_container_metrics: false,
        ..Default::default()
    });
    #[cfg(feature = "metrics")]
    let before = retries_total(&manager, "GET");

    let (addr, state) = fixture().await;
    let client = HttpClient::new(brisk_config()).unwrap();
    let signer = StampSigner {
        next: AtomicU64::new(0),
    };

    let response = client
        .get_signed(&format!("http://{addr}/flaky"), &signer)
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let recorded = state.lock().unwrap();
    assert_eq!(recorded.flaky_hits, 2, "the 503 must have been retried");
    assert_eq!(
        recorded.stamps,
        ["0", "1"],
        "each attempt carried its own signature"
    );

    #[cfg(feature = "metrics")]
    assert_eq!(
        retries_total(&manager, "GET"),
        before + 1,
        "the signed path shares the retry counter"
    );
}

/// The signature covers the request as it will be sent -- body and query
/// included -- which is why the hook runs on a built `Request`.
#[tokio::test]
async fn a_signature_covers_the_final_body_and_query() {
    let (addr, state) = fixture().await;
    let client = HttpClient::new(brisk_config()).unwrap();

    let response = client
        .send_signed(
            reqwest::Method::POST,
            &format!("http://{addr}/digest?tenant=acme&page=2"),
            Some(b"{\"query\":\"everything\"}".to_vec()),
            |request| request.header("content-type", "application/json"),
            &DigestSigner,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        state.lock().unwrap().digest_matched,
        Some(true),
        "the server recomputed the digest over what arrived"
    );
}

/// A `Retry-After` beats the exponential candidate, and is capped at the
/// configured maximum so an advertised hour cannot park the request.
#[tokio::test]
async fn retry_after_is_honoured_on_a_signed_call() {
    let (addr, state) = fixture().await;
    let client = HttpClient::new(HttpClientConfig {
        min_retry_interval_ms: 1,
        max_retry_interval_ms: 100,
        ..Default::default()
    })
    .unwrap();

    let started = Instant::now();
    let response = client
        .get_signed(&format!("http://{addr}/throttled"), &Unsigned)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.status(), 200);
    assert_eq!(state.lock().unwrap().throttled_hits, 2);
    assert!(
        elapsed >= Duration::from_millis(90),
        "the advertised delay beat the 1ms exponential candidate: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(900),
        "and was capped at the configured 100ms rather than the advertised second: {elapsed:?}"
    );
}

/// The unsigned path is the same loop: `get_with` still retries and still
/// reapplies the caller's own decoration on the second attempt. This is the
/// guard on the internal caller that migrated onto the signer.
#[tokio::test]
async fn an_unsigned_get_with_still_retries() {
    let (addr, state) = fixture().await;
    let client = HttpClient::new(brisk_config()).unwrap();
    let url = format!("http://{addr}/plain");

    let response = client
        .get_with(&url, |request| {
            request.basic_auth("account", Some("licence"))
        })
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    {
        let recorded = state.lock().unwrap();
        assert_eq!(recorded.plain_hits, 2, "the 503 must have been retried");
        assert_eq!(
            recorded.plain_auth.len(),
            2,
            "the credential is reapplied per attempt: {:?}",
            recorded.plain_auth
        );
        assert!(recorded.plain_auth.iter().all(|v| v.starts_with("Basic ")));
    }

    // The same route now answers 200 first time, so an explicitly unsigned
    // signed call goes through the loop untouched.
    let response = client.get_signed(&url, &Unsigned).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(state.lock().unwrap().plain_hits, 3);
}
