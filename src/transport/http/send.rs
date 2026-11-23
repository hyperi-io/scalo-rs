// Project:   scalo
// File:      src/transport/http/send.rs
// Purpose:   HTTP send side: endpoints, credentials, body framing, failover, status mapping
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The HTTP send side.
//!
//! A block becomes one request body per run of records with the same
//! destination, framed by the configured [`Framing`] and gzipped when asked.
//! Each body goes to the endpoints in order until one of them takes it or
//! refuses it for good. The guide is `docs/transport/http.md`.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER,
};
use serde::{Deserialize, Serialize};

use super::HttpTransportConfig;
use crate::framing::Framing;
use crate::sensitive::SensitiveString;
use crate::transport::error::{TransportError, TransportResult};
use crate::transport::proxy::{self, ProxyDialer, ProxyRefused, Scheme};
use crate::transport::types::SendResult;

/// Longest same-origin redirect chain `follow_redirects` follows.
const MAX_REDIRECTS: usize = 5;

/// Credentials sent with every request to an endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HttpAuth {
    /// No credentials, or the ones in the endpoint URL.
    #[default]
    None,
    /// HTTP basic auth.
    Basic {
        /// The user name. It can't hold a colon.
        username: String,
        /// The password.
        password: SensitiveString,
    },
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The token.
        token: SensitiveString,
    },
    /// A key in a header the destination names, such as `X-Api-Key`.
    ApiKey {
        /// The header name.
        header: String,
        /// The key, sent as the whole header value.
        key: SensitiveString,
    },
}

impl HttpAuth {
    /// The kind, for logs.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Basic { .. } => "basic",
            Self::Bearer { .. } => "bearer",
            Self::ApiKey { .. } => "api_key",
        }
    }

    /// The header this puts on a request, marked sensitive.
    fn header(&self) -> TransportResult<Option<(HeaderName, HeaderValue)>> {
        let refused =
            |what: &str| TransportError::Config(format!("HTTP auth {}: {what}", self.kind()));
        let sensitive = |text: &str| {
            let mut value = HeaderValue::from_str(text)
                .map_err(|_| refused("the credential is not a valid header value"))?;
            value.set_sensitive(true);
            Ok::<_, TransportError>(value)
        };
        Ok(match self {
            Self::None => None,
            Self::Basic { username, password } => Some((
                AUTHORIZATION,
                crate::http_client::signer::basic_auth_value(username, Some(password.expose()))
                    .map_err(|e| refused(&e.to_string()))?,
            )),
            Self::Bearer { token } if token.is_empty() => {
                return Err(refused("the token is empty"));
            }
            Self::Bearer { token } => Some((
                AUTHORIZATION,
                sensitive(&format!("Bearer {}", token.expose()))?,
            )),
            Self::ApiKey { key, .. } if key.is_empty() => return Err(refused("the key is empty")),
            Self::ApiKey { header, key } => Some((header_name(header)?, sensitive(key.expose())?)),
        })
    }
}

/// An endpoint tried after `endpoint`, in list order.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpEndpoint {
    /// Base URL, as `endpoint` is.
    pub url: String,
    /// Credentials for this endpoint. Unset uses the transport's `auth`.
    #[serde(default)]
    pub auth: Option<HttpAuth>,
}

impl fmt::Debug for HttpEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpEndpoint")
            .field("url", &shown_text(&self.url))
            .field("auth", &self.auth)
            .finish()
    }
}

/// `url` as it may be logged, without a user name or password.
fn shown(url: &reqwest::Url) -> String {
    let mut url = url.clone();
    // Only a URL that cannot have userinfo refuses these, and it has none to remove.
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.to_string()
}

/// [`shown`] for text that may not parse, which is then not shown at all.
pub(super) fn shown_text(url: &str) -> String {
    reqwest::Url::parse(url).map_or_else(|_| "<not a URL>".to_string(), |u| shown(&u))
}

/// `url` with `destination` appended as a path segment, when there is one.
fn join(url: &str, destination: &str) -> String {
    if destination.is_empty() {
        return url.to_string();
    }
    format!(
        "{}/{}",
        url.trim_end_matches('/'),
        destination.trim_start_matches('/')
    )
}

fn header_name(name: &str) -> TransportResult<HeaderName> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| TransportError::Config(format!("HTTP header name {name:?} is not valid")))
}

/// The custom headers, every value marked sensitive.
fn header_map(headers: &BTreeMap<String, SensitiveString>) -> TransportResult<HeaderMap> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (configured, value) in headers {
        let mut value = HeaderValue::from_str(value.expose()).map_err(|_| {
            TransportError::Config(format!("HTTP header {configured}: the value is not valid"))
        })?;
        value.set_sensitive(true);
        map.insert(header_name(configured)?, value);
    }
    Ok(map)
}

/// No redirect, or same-origin ones only, so a credential never follows a
/// request to a host the configuration did not name.
fn redirect_policy(follow: bool) -> reqwest::redirect::Policy {
    if !follow {
        return reqwest::redirect::Policy::none();
    }
    reqwest::redirect::Policy::custom(|attempt| {
        let same_origin = attempt
            .previous()
            .first()
            .is_some_and(|first| first.origin() == attempt.url().origin());
        if same_origin && attempt.previous().len() <= MAX_REDIRECTS {
            attempt.follow()
        } else {
            attempt.stop()
        }
    })
}

/// An error and its causes, without the request URL reqwest keeps on it.
fn describe(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut text = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    text
}

fn gzip(body: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(body.len() / 4),
        flate2::Compression::fast(),
    );
    encoder.write_all(body)?;
    encoder.finish()
}

/// What a response status says about the body that drew it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Answer {
    /// Stored.
    Delivered,
    /// The endpoint is busy or failing, or took the body without storing it.
    Retry,
    /// The endpoint or its credentials are wrong, which says nothing about the body.
    Hold,
    /// The body can never be accepted.
    Reject,
}

/// Classify `status`. An empty `success` list takes any 2xx as stored.
pub(super) fn classify(status: StatusCode, success: &[StatusCode]) -> Answer {
    let stored = if success.is_empty() {
        status.is_success()
    } else {
        success.contains(&status)
    };
    if stored {
        return Answer::Delivered;
    }
    match status.as_u16() {
        400 | 413 | 422 => Answer::Reject,
        408 | 429 | 500..=599 => Answer::Retry,
        300..=499 => Answer::Hold,
        // 1xx, and a 2xx not listed: accepted is not stored.
        _ => Answer::Retry,
    }
}

/// The `Retry-After` wait, capped at `max`. A zero `max` ignores the header.
pub(super) fn retry_after(headers: &HeaderMap, max: Duration) -> Option<Duration> {
    if max.is_zero() {
        return None;
    }
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    crate::backoff::parse_retry_after(value).map(|wait| wait.min(max))
}

/// One endpoint, its URL checked and its credential built.
struct Target {
    url: String,
    shown: String,
    auth: Option<(HeaderName, HeaderValue)>,
    scheme: Scheme,
    host: String,
    port: u16,
}

impl Target {
    fn new(url: &str, auth: &HttpAuth) -> TransportResult<Self> {
        let parsed = reqwest::Url::parse(url)
            .map_err(|e| TransportError::Config(format!("HTTP endpoint is not a URL: {e}")))?;
        let shown = shown(&parsed);
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(TransportError::Config(format!(
                "HTTP endpoint {shown}: the scheme must be http or https"
            )));
        }
        let userinfo = !parsed.username().is_empty() || parsed.password().is_some();
        if userinfo && *auth != HttpAuth::None {
            return Err(TransportError::Config(format!(
                "HTTP endpoint {shown} has credentials in its URL and auth {} as well: use one",
                auth.kind()
            )));
        }
        let scheme = if parsed.scheme() == "https" {
            Scheme::Tls
        } else {
            Scheme::Plain
        };
        Ok(Self {
            url: url.to_string(),
            shown,
            auth: auth.header()?,
            scheme,
            host: parsed.host_str().unwrap_or_default().to_string(),
            port: parsed.port_or_known_default().unwrap_or(80),
        })
    }
}

/// The send half of an HTTP transport.
pub(super) struct Sender {
    client: reqwest::Client,
    dialer: ProxyDialer,
    targets: Vec<Target>,
    framing: Arc<dyn Framing>,
    gzip: bool,
    success: Vec<StatusCode>,
    max_retry_after: Duration,
    send_timeout: Duration,
}

impl Sender {
    /// Finish `builder`, which carries TLS and timeouts, for posting to
    /// `endpoint` and then `config.failover`.
    pub(super) fn new(
        config: &HttpTransportConfig,
        endpoint: &str,
        builder: reqwest::ClientBuilder,
    ) -> TransportResult<Self> {
        let targets = std::iter::once((endpoint, None))
            .chain(
                config
                    .failover
                    .iter()
                    .map(|e| (e.url.as_str(), e.auth.as_ref())),
            )
            .map(|(url, auth)| Target::new(url, auth.unwrap_or(&config.auth)))
            .collect::<TransportResult<Vec<Target>>>()?;
        let framing = config
            .framing
            .build()
            .map_err(|e| TransportError::Config(format!("HTTP body framing: {e}")))?;
        let success = config
            .success_status
            .iter()
            .map(|code| {
                StatusCode::from_u16(*code).map_err(|_| {
                    TransportError::Config(format!("HTTP success_status {code} is not a status"))
                })
            })
            .collect::<TransportResult<Vec<StatusCode>>>()?;

        let builder = builder
            .redirect(redirect_policy(config.follow_redirects))
            .default_headers(header_map(&config.headers)?);
        let builder = match &config.user_agent {
            Some(agent) => builder.user_agent(agent),
            None => builder,
        };
        let dialer = config.proxy.dialer()?;
        let client = proxy::reqwest_proxy(builder, &config.proxy, &dialer)
            .build()
            .map_err(|e| TransportError::Config(format!("HTTP client: {}", describe(e))))?;

        tracing::info!(
            endpoints = ?targets.iter().map(|t| t.shown.as_str()).collect::<Vec<&str>>(),
            auth = config.auth.kind(),
            headers = ?config.headers.keys().collect::<Vec<&String>>(),
            framing = %config.framing.framing_type,
            gzip = config.gzip,
            proxy = config.proxy.kind(),
            follow_redirects = config.follow_redirects,
            success_status = ?config.success_status,
            "HTTP sender ready"
        );
        Ok(Self {
            client,
            dialer,
            targets,
            framing,
            gzip: config.gzip,
            success,
            max_retry_after: Duration::from_millis(config.max_retry_after_ms),
            send_timeout: Duration::from_millis(config.send_timeout_ms),
        })
    }

    /// Send `(destination, payload)` pairs, one body per run of the same
    /// destination, or more where the framing holds fewer records. Without a
    /// `deadline`, each body has `send_timeout_ms` of its own.
    pub(super) async fn send(
        &self,
        records: &[(&str, &[u8])],
        deadline: Option<Instant>,
    ) -> SendResult {
        for run in records.chunk_by(|a, b| a.0 == b.0) {
            let destination = run[0].0;
            let payloads: Vec<&[u8]> = run
                .iter()
                .map(|(_, payload)| {
                    tracing::trace!(destination, bytes = payload.len(), "HTTP record framed");
                    *payload
                })
                .collect();
            let mut at = 0;
            while at < payloads.len() {
                let mut body = Vec::new();
                let taken = self.framing.encode(&payloads[at..], &mut body);
                if taken == 0 {
                    return SendResult::Fatal(TransportError::Internal(
                        "HTTP body framing took none of the records".into(),
                    ));
                }
                let by = deadline.unwrap_or_else(|| Instant::now() + self.send_timeout);
                let result = self.post(destination, body, taken, by).await;
                if !result.is_ok() {
                    return result;
                }
                at += taken;
            }
        }
        SendResult::Ok
    }

    /// Post one body to each endpoint in turn until one stores or rejects it.
    /// Each endpoint gets an equal share of the time left.
    async fn post(
        &self,
        destination: &str,
        framed: Vec<u8>,
        records: usize,
        deadline: Instant,
    ) -> SendResult {
        let framed_len = framed.len();
        let body = if self.gzip {
            match gzip(&framed) {
                Ok(zipped) => zipped,
                Err(e) => {
                    return SendResult::Fatal(TransportError::Internal(format!(
                        "HTTP gzip of a {framed_len}-byte body: {e}"
                    )));
                }
            }
        } else {
            framed
        };
        let body = bytes::Bytes::from(body);

        let mut retry = false;
        let mut refusal: Option<ProxyRefused> = None;
        let mut wait: Option<Duration> = None;
        let mut reasons = Vec::new();
        let total = self.targets.len();
        for (index, target) in self.targets.iter().enumerate() {
            let left = u32::try_from(total - index).unwrap_or(u32::MAX);
            let budget = deadline.saturating_duration_since(Instant::now()) / left;
            let url = join(&target.url, destination);
            let shown = join(&target.shown, destination);
            let mut request = self
                .client
                .post(&url)
                .timeout(budget)
                .header(CONTENT_TYPE, self.framing.content_type())
                .body(body.clone());
            if self.gzip {
                request = request.header(CONTENT_ENCODING, "gzip");
            }
            if let Some((name, value)) = &target.auth {
                request = request.header(name.clone(), value.clone());
            }
            #[cfg(feature = "transport-trace")]
            if let Some(tp) = crate::transport::propagation::current_traceparent() {
                request = request.header(crate::transport::propagation::TRACEPARENT_HEADER, tp);
            }

            #[cfg(feature = "metrics")]
            let started = Instant::now();
            let answered = request.send().await;
            #[cfg(feature = "metrics")]
            metrics::histogram!("transport_send_duration_seconds", "transport" => "http")
                .record(started.elapsed().as_secs_f64());

            let response = match answered {
                Ok(response) => response,
                Err(e) if e.is_builder() => {
                    let detail =
                        format!("HTTP request to {shown} cannot be built: {}", describe(e));
                    tracing::warn!(endpoint = %shown, records, "{detail}");
                    count_error();
                    return SendResult::Reject(TransportError::Send(detail));
                }
                Err(e) => {
                    let held = e.is_redirect();
                    let timed_out = e.is_timeout();
                    let detail = describe(e);
                    tracing::warn!(endpoint = %shown, records, error = %detail, "HTTP request failed");
                    count_error();
                    if !held
                        && !timed_out
                        && let Some(refused) = self.refused(target, deadline).await
                    {
                        reasons.push(format!("{shown}: {refused}"));
                        refusal = Some(refused);
                        continue;
                    }
                    retry |= !held;
                    reasons.push(format!("{shown}: {detail}"));
                    continue;
                }
            };
            let status = response.status();
            match classify(status, &self.success) {
                Answer::Delivered => {
                    tracing::debug!(
                        endpoint = %shown,
                        records,
                        bytes = body.len(),
                        status = status.as_u16(),
                        "HTTP batch delivered"
                    );
                    #[cfg(feature = "metrics")]
                    {
                        metrics::counter!("transport_sent_total", "transport" => "http")
                            .increment(records as u64);
                        metrics::counter!("transport_sent_bytes_total", "transport" => "http")
                            .increment(body.len() as u64);
                    }
                    return SendResult::Ok;
                }
                Answer::Reject => {
                    let detail = format!("HTTP {status} from {shown}");
                    tracing::warn!(endpoint = %shown, records, status = status.as_u16(), "HTTP batch rejected");
                    count_error();
                    return SendResult::Reject(if status == StatusCode::PAYLOAD_TOO_LARGE {
                        TransportError::MessageTooLarge {
                            bytes: framed_len,
                            detail,
                        }
                    } else {
                        TransportError::Send(detail)
                    });
                }
                Answer::Retry => {
                    let after = retry_after(response.headers(), self.max_retry_after);
                    tracing::warn!(
                        endpoint = %shown,
                        records,
                        status = status.as_u16(),
                        retry_after_ms = after.map(|w| u64::try_from(w.as_millis()).unwrap_or(u64::MAX)),
                        "HTTP endpoint busy"
                    );
                    #[cfg(feature = "metrics")]
                    metrics::counter!("transport_backpressured_total", "transport" => "http")
                        .increment(1);
                    retry = true;
                    wait = wait.max(after);
                    reasons.push(format!("{shown}: HTTP {status}"));
                }
                Answer::Hold => {
                    tracing::warn!(endpoint = %shown, records, status = status.as_u16(), "HTTP endpoint refused the request");
                    count_error();
                    reasons.push(format!("{shown}: HTTP {status}"));
                }
            }
        }

        let reason = reasons.join("; ");
        if retry {
            tracing::debug!(records, reason = %reason, "HTTP batch to be sent again");
            return wait.map_or(SendResult::Backpressured, SendResult::RetryAfter);
        }
        if let Some(refused) = refusal {
            return proxy::rejected(&refused, "http");
        }
        tracing::error!(
            records,
            reason = %reason,
            "HTTP: no endpoint takes the batch; fix the endpoint or its credentials"
        );
        SendResult::Fatal(TransportError::Config(format!(
            "HTTP: no endpoint takes the batch: {reason}"
        )))
    }
}

impl Sender {
    /// The proxy's refusal of a CONNECT to `target`. reqwest's tunnel error
    /// carries no status, so one CONNECT of our own reads it.
    async fn refused(&self, target: &Target, deadline: Instant) -> Option<ProxyRefused> {
        if target.scheme != Scheme::Tls {
            return None;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        self.dialer
            .refusal_of(target.scheme, &target.host, target.port, left)
            .await
    }
}

fn count_error() {
    #[cfg(feature = "metrics")]
    metrics::counter!("transport_send_errors_total", "transport" => "http").increment(1);
}
