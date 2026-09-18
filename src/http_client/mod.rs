// Project:   scalo
// File:      src/http_client/mod.rs
// Purpose:   Production HTTP client with retry/backoff
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Production HTTP client with automatic retries and timeouts.
//!
//! Wraps [`reqwest`] with exponential backoff (the `backon` crate) for
//! transient failures (5xx, 429, 408, connect/timeout errors). Permanent
//! errors (other 4xx) return immediately.
//!
//! ## Retry safety
//!
//! Retries are restricted to **idempotent** methods (GET, HEAD, PUT, DELETE,
//! OPTIONS) by default: replaying a non-idempotent POST can duplicate side
//! effects downstream. Set `retry_non_idempotent = true` only when the
//! endpoint is known to dedupe (e.g. an idempotency key). A request the signer
//! could not sign never left, so a transient signing failure is retried
//! whatever the method. When a throttled downstream returns `Retry-After`, that
//! delay is honoured in preference to the exponential schedule, capped at
//! `max_retry_interval_ms`. It paces the attempts `max_retries` already grants
//! and never adds one, so a downstream that answers 429 plus the header forever
//! still ends the loop.
//!
//! After retries are exhausted the **last response is returned** (even a 5xx)
//! so the caller can inspect status/body -- a persistent server error is not
//! masked as a transport error.
//!
//! ## Signed requests
//!
//! [`RequestSigner`] is the hook that puts a credential on a request. It runs
//! on the built request, inside the retry loop, so a signature covers the final
//! body and query and is regenerated on every attempt.
//! [`Self::get_signed`](HttpClient::get_signed) and
//! [`Self::send_signed`](HttpClient::send_signed) are the signed surface; the
//! unsigned methods are the same loop with [`Unsigned`] in place of a signer.
//!
//! No error leaves the loop carrying the request URL: reqwest keeps a copy of
//! it on a transport error and renders it, and a credential placed in the query
//! is inside that URL. The error names what failed; the caller knows what it
//! called.
//!
//! ## Redirects
//!
//! reqwest follows redirects by default and carries the body and any custom
//! headers across a cross-origin hop, so a signed header follows the request to
//! whatever host the downstream names. Build a client whose calls are signed
//! with a header or a query parameter through
//! [`HttpClient::with_redirect_policy`] and a policy that refuses the hop, as
//! the token exchanges in [`crate::auth`] do for themselves.
//!
//! # Config Cascade
//!
//! When the `config` feature is enabled, config is auto-loaded from the
//! cascade under the `http_client` key:
//!
//! ```yaml
//! http_client:
//!   timeout_secs: 30
//!   connect_timeout_secs: 10
//!   max_retries: 3
//!   min_retry_interval_ms: 100
//!   max_retry_interval_ms: 30000
//!   retry_non_idempotent: false
//!   user_agent: "dfe-fetcher/1.0"
//! ```

pub mod config;
pub mod signer;

pub use config::HttpClientConfig;
pub use signer::{RequestSigner, SignError, Unsigned};

use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use reqwest::{Client, Method, RequestBuilder, Response, StatusCode};

/// HTTP client build error.
#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    /// Failed to build the underlying reqwest client.
    #[error("failed to build HTTP client: {0}")]
    BuildError(#[from] reqwest::Error),
}

/// Error from an HTTP request.
///
/// `Status` carries the [`Response`] so the caller can still inspect a
/// persistent server error after retries are exhausted; it is surfaced by the
/// public methods as `Ok(response)` (not `Err`), matching the historical
/// "caller checks the status code" contract.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// Transport-level failure (connect, timeout, TLS, dropped connection).
    #[error("HTTP transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// Server returned a retryable status (5xx / 429 / 408). Held internally
    /// during the retry loop; never escapes a public method as an `Err`.
    #[error("HTTP retryable status: {}", .0.status())]
    Status(Box<Response>),

    /// Request body could not be serialised to JSON (not retryable).
    #[error("JSON serialise failed: {0}")]
    Serialize(#[from] serde_json::Error),

    /// The signer could not put its credential on the request. Retried only
    /// when the signer says another attempt could succeed.
    #[error("signing failed: {0}")]
    Sign(#[from] SignError),
}

impl HttpError {
    /// Whether this error warrants a retry.
    ///
    /// `replayable` is whether a request that reached the wire may be sent
    /// again: the method is idempotent, or the caller opted in. A request the
    /// signer refused never left, so that decision does not apply to it.
    fn is_retryable(&self, replayable: bool) -> bool {
        match self {
            // Connect/timeout are transient; decode/redirect/body are not.
            Self::Transport(e) => replayable && (e.is_timeout() || e.is_connect()),
            // A retryable status was only constructed for the retryable set.
            Self::Status(_) => replayable,
            // A body that will not serialise is unchanged by sending the
            // request again.
            Self::Serialize(_) => false,
            // A credential endpoint that could not be reached, or answered
            // 408, 429 or 5xx, may answer the next attempt; any other refusal
            // will not.
            Self::Sign(e) => e.is_retryable(),
        }
    }

    /// The same failure with reqwest's copy of the request URL dropped.
    ///
    /// reqwest appends the request URL to what it renders, and a credential
    /// placed in the query rides out inside it. Matched variant by variant
    /// rather than through a catch-all, so a new variant carrying a URL has to
    /// answer this question before it compiles.
    #[must_use]
    pub(crate) fn without_url(self) -> Self {
        match self {
            Self::Transport(e) => Self::Transport(e.without_url()),
            // Handed back to the caller as a response rather than rendered as
            // an error: `Display` names only the status.
            Self::Status(response) => Self::Status(response),
            // Names the type that would not serialise, never a URL.
            Self::Serialize(e) => Self::Serialize(e),
            // The signer names its own endpoint, never the request it signed.
            Self::Sign(e) => Self::Sign(e),
        }
    }

    /// `Retry-After` delay advertised by the downstream, if any (delta-seconds
    /// form). HTTP-date form is not parsed -- the exponential schedule is used.
    fn retry_after(&self) -> Option<Duration> {
        let Self::Status(resp) = self else {
            return None;
        };
        let secs: u64 = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)?
            .to_str()
            .ok()?
            .trim()
            .parse()
            .ok()?;
        Some(Duration::from_secs(secs))
    }
}

/// Status codes worth retrying: throttling + transient server failures.
fn is_retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// How long to wait before the next attempt: the delay the downstream
/// advertised if it sent one, else the exponential candidate.
///
/// The advertised delay is capped at `max`, the configured retry ceiling. A
/// throttled downstream can advertise hours (`Retry-After: 86400` is legal), and
/// taking that whole parks the request inside the retry loop for a day.
///
/// No candidate means the retry budget is spent, and that answer stands
/// whatever the downstream advertises: a delay is how long to wait before an
/// attempt the schedule has already granted, not a grant of another one.
fn retry_delay(
    advertised: Option<Duration>,
    candidate: Option<Duration>,
    max: Duration,
) -> Option<Duration> {
    match (advertised, candidate) {
        (Some(delay), Some(_)) => Some(delay.min(max)),
        (_, candidate) => candidate,
    }
}

/// The metric label for a method, as a static string: a label must outlive the
/// call, and `Method::as_str` borrows the method.
fn method_label(method: &Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

/// Whether replaying the method is safe, matching what the typed methods
/// hardcode.
fn is_idempotent(method: &Method) -> bool {
    matches!(
        method.as_str(),
        "GET" | "HEAD" | "PUT" | "DELETE" | "OPTIONS"
    )
}

/// Production HTTP client with retry/backoff.
pub struct HttpClient {
    inner: Client,
    config: HttpClientConfig,
}

impl HttpClient {
    /// Create a new HTTP client with the given config.
    ///
    /// # Errors
    ///
    /// Returns [`HttpClientError::BuildError`] if the underlying reqwest
    /// client cannot be constructed (typically TLS backend init failure).
    pub fn new(config: HttpClientConfig) -> Result<Self, HttpClientError> {
        Self::with_redirect_policy(config, reqwest::redirect::Policy::default())
    }

    /// Create a new HTTP client with the given config and redirect policy.
    ///
    /// reqwest carries the body and any custom header across a cross-origin
    /// redirect, so a client whose calls are signed with a header or a query
    /// parameter should refuse the hop ([`reqwest::redirect::Policy::none`])
    /// or allow only the same origin through a custom policy.
    ///
    /// # Errors
    ///
    /// Returns [`HttpClientError::BuildError`] if the underlying reqwest
    /// client cannot be constructed (typically TLS backend init failure).
    pub fn with_redirect_policy(
        config: HttpClientConfig,
        redirects: reqwest::redirect::Policy,
    ) -> Result<Self, HttpClientError> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .redirect(redirects);

        if let Some(ref ua) = config.user_agent {
            builder = builder.user_agent(ua.clone());
        }

        Ok(Self {
            inner: builder.build()?,
            config,
        })
    }

    /// Create a client from the config cascade (or defaults).
    ///
    /// # Errors
    ///
    /// Returns [`HttpClientError::BuildError`] if the underlying reqwest
    /// client cannot be constructed.
    pub fn from_cascade() -> Result<Self, HttpClientError> {
        Self::new(HttpClientConfig::from_cascade())
    }

    /// Exponential backoff schedule from config (jittered to avoid sync'd
    /// retry storms across many clients hitting the same downstream).
    fn backoff(&self) -> ExponentialBuilder {
        ExponentialBuilder::new()
            .with_min_delay(Duration::from_millis(self.config.min_retry_interval_ms))
            .with_max_delay(Duration::from_millis(self.config.max_retry_interval_ms))
            .with_max_times(self.config.max_retries as usize)
            .with_jitter()
    }

    /// Send a request (built fresh per attempt by `make`), retrying transient
    /// failures when `idempotent` (or the non-idempotent opt-in) allows it.
    ///
    /// `make` is called once per attempt so each retry dispatches a fresh
    /// request -- `reqwest::RequestBuilder` is not `Clone`.
    async fn execute(
        &self,
        method: &'static str,
        idempotent: bool,
        make: impl Fn() -> RequestBuilder,
    ) -> Result<Response, HttpError> {
        self.execute_signed(method, idempotent, &Unsigned, || {
            make().build().map_err(HttpError::from)
        })
        .await
    }

    /// The one retry loop, with the signer run per attempt.
    ///
    /// `make` builds the request and `signer` puts the credential on it, in that
    /// order, on every attempt: a signature therefore covers the final body and
    /// query, and a per-request nonce or timestamp is fresh on a retry rather
    /// than replayed. `RequestBuilder::send` is `build` then `Client::execute`,
    /// so splitting the two changes nothing for the unsigned path.
    async fn execute_signed<S: RequestSigner>(
        &self,
        method: &'static str,
        idempotent: bool,
        signer: &S,
        make: impl Fn() -> Result<reqwest::Request, HttpError>,
    ) -> Result<Response, HttpError> {
        let attempt = || async {
            let mut request = make()?;
            signer.sign(&mut request).await?;
            let resp = self.inner.execute(request).await?;
            if is_retryable_status(resp.status()) {
                return Err(HttpError::Status(Box::new(resp)));
            }
            Ok(resp)
        };

        let replayable = idempotent || self.config.retry_non_idempotent;
        let max_delay = Duration::from_millis(self.config.max_retry_interval_ms);

        let result = if self.config.max_retries > 0 {
            attempt
                .retry(self.backoff())
                .when(move |e: &HttpError| e.is_retryable(replayable))
                .adjust(move |e: &HttpError, candidate| {
                    retry_delay(e.retry_after(), candidate, max_delay)
                })
                .sleep(tokio::time::sleep)
                .notify(|_e: &HttpError, _dur: Duration| Self::record_retry(method))
                .await
        } else {
            attempt().await
        };

        match result {
            Ok(resp) => Ok(resp),
            // Retries exhausted on a 5xx/429: hand back the last response so the
            // caller can read status/body, preserving the legacy contract.
            Err(HttpError::Status(resp)) => Ok(*resp),
            // A credential placed in the query is inside the URL reqwest keeps
            // on the error, so no error leaves this loop carrying one.
            Err(e) => Err(e.without_url()),
        }
    }

    /// Emit the retry counter (no-op without the `metrics` feature).
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn record_retry(method: &'static str) {
        #[cfg(feature = "metrics")]
        metrics::counter!("http_client_retries_total", "method" => method).increment(1);
    }

    /// Record request outcome metrics (no-op without the `metrics` feature).
    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn record(method: &'static str, ok: bool, start: std::time::Instant) {
        #[cfg(feature = "metrics")]
        {
            let status = if ok { "success" } else { "error" };
            metrics::counter!("http_client_requests_total", "method" => method, "status" => status)
                .increment(1);
            metrics::histogram!("http_client_duration_seconds", "method" => method)
                .record(start.elapsed().as_secs_f64());
        }
    }

    /// Send a GET request (idempotent: retried on transient failure).
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Transport`] on a persistent transport failure. A
    /// persistent server status (5xx) is returned as `Ok(response)`.
    pub async fn get(&self, url: &str) -> Result<Response, HttpError> {
        let start = std::time::Instant::now();
        let result = self.execute("GET", true, || self.inner.get(url)).await;
        Self::record("GET", result.is_ok(), start);
        result
    }

    /// Send a GET request, customising the request before dispatch.
    ///
    /// Idempotent and retried exactly like [`Self::get`]. `customise` runs once
    /// per attempt, so auth headers, query parameters and custom headers are
    /// reapplied on every retry.
    ///
    /// Use this rather than [`Self::client`] when the request needs decoration
    /// but should keep the retry/backoff and metrics behaviour.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Transport`] on a persistent transport failure. A
    /// persistent server status (5xx) is returned as `Ok(response)`.
    pub async fn get_with(
        &self,
        url: &str,
        customise: impl Fn(RequestBuilder) -> RequestBuilder,
    ) -> Result<Response, HttpError> {
        let start = std::time::Instant::now();
        let result = self
            .execute("GET", true, || customise(self.inner.get(url)))
            .await;
        Self::record("GET", result.is_ok(), start);
        result
    }

    /// Send a POST request with a JSON body.
    ///
    /// POST is **not** retried by default (not idempotent); enable
    /// `retry_non_idempotent` only for dedupe-safe endpoints.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Serialize`] if `body` cannot be encoded as JSON,
    /// or [`HttpError::Transport`] on a persistent transport failure.
    /// Previously a serialisation failure was silently substituted with an
    /// empty body -- the request would dispatch with no payload, hiding the
    /// bug at the caller.
    pub async fn post_json<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        body: &T,
    ) -> Result<Response, HttpError> {
        let start = std::time::Instant::now();
        let body_bytes = serde_json::to_vec(body)?;
        let result = self
            .execute("POST", false, || {
                self.inner
                    .post(url)
                    .header("content-type", "application/json")
                    .body(body_bytes.clone())
            })
            .await;
        Self::record("POST", result.is_ok(), start);
        result
    }

    /// Send a PUT request with a JSON body (idempotent: retried).
    ///
    /// # Errors
    ///
    /// See [`Self::post_json`] -- same serialise + transport error contract.
    pub async fn put_json<T: serde::Serialize + ?Sized>(
        &self,
        url: &str,
        body: &T,
    ) -> Result<Response, HttpError> {
        let start = std::time::Instant::now();
        let body_bytes = serde_json::to_vec(body)?;
        let result = self
            .execute("PUT", true, || {
                self.inner
                    .put(url)
                    .header("content-type", "application/json")
                    .body(body_bytes.clone())
            })
            .await;
        Self::record("PUT", result.is_ok(), start);
        result
    }

    /// Send a DELETE request (idempotent: retried on transient failure).
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Transport`] on a persistent transport failure.
    pub async fn delete(&self, url: &str) -> Result<Response, HttpError> {
        let start = std::time::Instant::now();
        let result = self
            .execute("DELETE", true, || self.inner.delete(url))
            .await;
        Self::record("DELETE", result.is_ok(), start);
        result
    }

    /// Send a GET request with `signer` putting the credential on it
    /// (idempotent: retried on transient failure).
    ///
    /// The signer runs per attempt on the built request. Pass [`Unsigned`] for
    /// a call that needs no credential.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Sign`] if the credential cannot be acquired or
    /// placed, or [`HttpError::Transport`] on a persistent transport failure. A
    /// persistent server status (5xx) is returned as `Ok(response)`.
    pub async fn get_signed<S: RequestSigner>(
        &self,
        url: &str,
        signer: &S,
    ) -> Result<Response, HttpError> {
        let start = std::time::Instant::now();
        let result = self
            .execute_signed("GET", true, signer, || {
                self.inner.get(url).build().map_err(HttpError::from)
            })
            .await;
        Self::record("GET", result.is_ok(), start);
        result
    }

    /// Send a request of any method with `signer` putting the credential on it.
    ///
    /// `body` is the raw request body, if any; `customise` decorates the builder
    /// (headers, query, a form) once per attempt, before signing. Retries follow
    /// the method: GET, HEAD, PUT, DELETE and OPTIONS are replayable, anything
    /// else needs the `retry_non_idempotent` opt-in, matching the typed methods.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Sign`] if the credential cannot be acquired or
    /// placed, or [`HttpError::Transport`] on a persistent transport failure. A
    /// persistent server status (5xx) is returned as `Ok(response)`.
    pub async fn send_signed<S: RequestSigner>(
        &self,
        method: Method,
        url: &str,
        body: Option<Vec<u8>>,
        customise: impl Fn(RequestBuilder) -> RequestBuilder,
        signer: &S,
    ) -> Result<Response, HttpError> {
        let label = method_label(&method);
        let idempotent = is_idempotent(&method);
        let start = std::time::Instant::now();
        // One buffer for the whole call: an attempt clones the handle, not the
        // body, so a retried upload does not copy itself again.
        let body = body.map(bytes::Bytes::from);
        let result = self
            .execute_signed(label, idempotent, signer, || {
                let mut builder = self.inner.request(method.clone(), url);
                if let Some(ref bytes) = body {
                    builder = builder.body(bytes.clone());
                }
                customise(builder).build().map_err(HttpError::from)
            })
            .await;
        Self::record(label, result.is_ok(), start);
        result
    }

    /// Access the underlying reqwest client for custom requests.
    ///
    /// Requests made directly through this handle bypass the retry/backoff
    /// wrapper -- use the typed methods for retried delivery.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.inner
    }

    /// Access the current config.
    #[must_use]
    pub fn config(&self) -> &HttpClientConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_status_set() {
        for code in [408, 429, 500, 502, 503, 504] {
            assert!(is_retryable_status(StatusCode::from_u16(code).unwrap()));
        }
        for code in [200, 201, 301, 400, 401, 404, 409, 501] {
            assert!(!is_retryable_status(StatusCode::from_u16(code).unwrap()));
        }
    }

    #[test]
    fn serialise_error_not_retryable() {
        // Build a serde_json error and confirm it never triggers a retry.
        let err = serde_json::from_str::<i32>("not a number").unwrap_err();
        let http_err = HttpError::Serialize(err);
        assert!(!http_err.is_retryable(true));
        assert!(http_err.retry_after().is_none());
    }

    #[test]
    fn a_signing_failure_is_retried_on_the_signers_word_alone() {
        let transient = HttpError::Sign(SignError::new("idp unreachable").retryable());
        assert!(
            transient.is_retryable(false),
            "the request never left, so replaying it is not the question"
        );
        let refused = HttpError::Sign(SignError::new("invalid_client"));
        assert!(!refused.is_retryable(true));
    }

    #[test]
    fn retry_after_is_capped_at_the_configured_maximum() {
        let max = Duration::from_millis(30_000);
        assert_eq!(
            retry_delay(
                Some(Duration::from_secs(86_400)),
                Some(Duration::from_millis(5)),
                max
            ),
            Some(max),
            "an advertised day must not park the request for a day"
        );
    }

    #[test]
    fn a_retry_after_under_the_cap_is_taken_whole() {
        assert_eq!(
            retry_delay(
                Some(Duration::from_secs(2)),
                Some(Duration::from_millis(5)),
                Duration::from_millis(30_000)
            ),
            Some(Duration::from_secs(2)),
            "the downstream's own pacing wins over the exponential candidate"
        );
    }

    #[test]
    fn an_advertised_delay_cannot_outlive_the_retry_budget() {
        assert_eq!(
            retry_delay(
                Some(Duration::from_secs(1)),
                None,
                Duration::from_millis(20)
            ),
            None,
            "no candidate means the budget is spent, whatever the downstream advertises"
        );
    }

    #[test]
    fn without_a_retry_after_the_exponential_candidate_stands() {
        assert_eq!(
            retry_delay(
                None,
                Some(Duration::from_millis(5)),
                Duration::from_millis(30_000)
            ),
            Some(Duration::from_millis(5))
        );
        assert_eq!(retry_delay(None, None, Duration::from_millis(1)), None);
    }

    #[test]
    fn only_replayable_methods_are_idempotent() {
        for method in [Method::GET, Method::HEAD, Method::PUT, Method::DELETE] {
            assert!(is_idempotent(&method), "{method}");
        }
        for method in [Method::POST, Method::PATCH] {
            assert!(!is_idempotent(&method), "{method}");
        }
    }

    #[test]
    fn method_labels_are_static() {
        assert_eq!(method_label(&Method::GET), "GET");
        assert_eq!(method_label(&Method::PATCH), "PATCH");
        assert_eq!(
            method_label(&Method::from_bytes(b"PROPFIND").unwrap()),
            "OTHER"
        );
    }

    #[tokio::test]
    async fn build_client_from_default_config() {
        let client = HttpClient::new(HttpClientConfig::default()).unwrap();
        assert_eq!(client.config().max_retries, 3);
        // The backoff honours config bounds.
        let _ = client.backoff();
    }
}
