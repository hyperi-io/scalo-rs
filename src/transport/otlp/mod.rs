// Project:   scalo
// File:      src/transport/otlp/mod.rs
// Purpose:   OTLP logs transport (send over gRPC or HTTP/protobuf)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # OTLP logs transport
//!
//! Sends records to an OpenTelemetry collector as OTLP log records, over gRPC
//! (`LogsService/Export`) or HTTP/protobuf (`POST /v1/logs`). Send only. A
//! block goes as one export request, and `Ok` means the collector answered
//! that it took every record.
//!
//! Each record's payload is the log body, a string when it is UTF-8 and bytes
//! otherwise. Its timestamp is the log time, the [`SEVERITY_HEADER`] header
//! its severity, and every other header an attribute. The resource carries
//! `service.name`, the replica identity and the configured attributes. An app
//! that needs another mapping registers a custom transport.
//!
//! ```rust,ignore
//! use scalo::transport::{OtlpConfig, OtlpTransport, TransportSender};
//!
//! let transport = OtlpTransport::new(&OtlpConfig::grpc("https://collector:4317"))?;
//! let result = transport.send_batch(&records).await; // SendResult
//! ```

mod config;
mod record;
#[cfg(test)]
mod tests;

pub use config::{OTLP_ACCEPTS, OtlpConfig, OtlpProtocol};
pub use record::SEVERITY_HEADER;

use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use prost::Message as _;
use tonic_types::StatusExt as _;

use super::ack::{DeadLetterReason, SinkConfirmation};
use super::core::{Lifecycle, Meter};
use super::delivery::DeliveryGuarantee;
use super::error::{TransportError, TransportResult};
use super::filter::{TransportFilterEngine, TransportFilterTierConfig};
use super::proxy::Scheme;
use super::traits::{TransportBase, TransportSender};
use super::types::{PayloadFormat, SendResult};
use super::work_batch::{Record, RecordMeta};
use crate::compression::Compression;
use record::LogResource;

/// The most characters of a collector's error text a result carries.
const MAX_DETAIL_CHARS: usize = 512;

/// Sends records as OTLP log records to one collector.
///
/// Every send answers within `timeout_ms`, and `send_batch_by` within its
/// deadline too, whichever comes first.
pub struct OtlpTransport {
    wire: Wire,
    resource: LogResource,
    timeout: Duration,
    max_retry_delay: Duration,
    /// The endpoint with any user name and password left out, for logs.
    endpoint: String,
    /// Closed, or `refusals` refused: either fails readiness.
    lifecycle: Lifecycle,
    /// Whether the proxy is refusing CONNECT, which lasts until an export goes through.
    refusals: crate::transport::proxy::Refusals,
    filter_engine: TransportFilterEngine,
}

/// The client for the configured protocol and what every request carries.
enum Wire {
    Grpc {
        client: LogsServiceClient<tonic::transport::Channel>,
        metadata: tonic::metadata::MetadataMap,
    },
    Http {
        client: reqwest::Client,
        url: String,
        headers: reqwest::header::HeaderMap,
        compression: Compression,
        dialer: crate::transport::ProxyDialer,
        /// The https collector's host and port, for reading a refused CONNECT.
        tunnel: Option<(String, u16)>,
    },
}

impl OtlpTransport {
    /// Build the transport. Nothing is dialled until the first send.
    ///
    /// # Errors
    ///
    /// `TransportError::Config` for an endpoint that is not an `http://` or
    /// `https://` URL, a TLS file that cannot be read, a header that is not a
    /// valid HTTP header or gRPC metadata, a `timeout_ms` of 0, a filter that
    /// does not compile, or a gRPC transport built outside a Tokio runtime.
    pub fn new(config: &OtlpConfig) -> TransportResult<Self> {
        config.validate()?;
        let url = config.url()?;
        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| config_error(format!("invalid endpoint: {e}")))?;
        let tls = config.client_tls(&url)?;
        let wire = match config.protocol {
            OtlpProtocol::Grpc => grpc_wire(config, &url, tls.as_ref())?,
            OtlpProtocol::Http => http_wire(config, url, tls.as_ref())?,
        };
        let filter_engine = TransportFilterEngine::new(
            &[],
            &config.filters_out,
            &TransportFilterTierConfig::from_cascade()?,
        )?;
        let refusals = crate::transport::proxy::Refusals::default();
        let lifecycle = {
            let refusals = refusals.clone();
            Lifecycle::new::<Self>(move || !refusals.is_refused())
        };

        let endpoint = loggable(&parsed);
        tracing::info!(
            endpoint = %endpoint,
            protocol = config.protocol.as_str(),
            compression = %config.compression,
            timeout_ms = config.timeout_ms,
            max_retry_delay_ms = config.max_retry_delay_ms,
            tls = tls.as_ref().map_or("none", |t| t.trust.as_str()),
            mutual_tls = tls.as_ref().is_some_and(|t| t.identity.is_some()),
            headers = config.headers.len(),
            guarantee = %DeliveryGuarantee::Acknowledged,
            "OTLP logs transport built"
        );
        Ok(Self {
            wire,
            resource: LogResource::new(config),
            timeout: Duration::from_millis(config.timeout_ms),
            max_retry_delay: Duration::from_millis(config.max_retry_delay_ms),
            endpoint,
            lifecycle,
            refusals,
            filter_engine,
        })
    }

    /// Send the records no outbound filter takes out as one export request,
    /// answering by `deadline` when there is one.
    async fn export(&self, records: &[Record], deadline: Option<Instant>) -> SendResult {
        if self.lifecycle.is_closed() {
            return SendResult::Fatal(TransportError::Closed);
        }
        let (to_send, handled) = self
            .filter_engine
            .partition_outbound(records, |r| &r.payload);
        if to_send.is_empty() {
            return handled;
        }
        let started = Instant::now();
        let timer = Self::send_timer();
        let budget = deadline.map_or(self.timeout, |d| {
            self.timeout.min(d.saturating_duration_since(started))
        });
        let request = self
            .resource
            .request(to_send.iter().copied(), record::now_unix_nano());
        let sent = to_send.len();
        let bytes: usize = to_send.iter().map(|r| r.payload.len()).sum();

        // tokio's timeout polls the export once before it looks at the clock,
        // so a deadline already past still takes an answer that is ready.
        let result = tokio::time::timeout(budget, self.export_on_wire(request, sent, budget))
            .await
            .unwrap_or(SendResult::Backpressured);
        self.observe(&result, sent, bytes, started, budget);
        timer.record();
        if result.is_ok() {
            self.refusals.clear("otlp");
            handled
        } else {
            result
        }
    }

    async fn export_on_wire(
        &self,
        request: ExportLogsServiceRequest,
        sent: usize,
        budget: Duration,
    ) -> SendResult {
        match &self.wire {
            Wire::Grpc { client, metadata } => {
                let mut call = tonic::Request::new(request);
                *call.metadata_mut() = metadata.clone();
                // Sent as `grpc-timeout`, so the collector gives up when the sender does.
                call.set_timeout(budget);
                match client.clone().export(call).await {
                    Ok(response) => accepted(response.into_inner().partial_success, sent),
                    Err(status) => match crate::transport::proxy::refusal(&status) {
                        Some(refused) => self.refusals.hold(refused, "otlp"),
                        None => grpc_failure(&status, self.max_retry_delay),
                    },
                }
            }
            Wire::Http {
                client,
                url,
                headers,
                compression,
                dialer,
                tunnel,
            } => {
                let body = request.encode_to_vec();
                let encoded_len = body.len();
                let body = if compression.is_none() {
                    body
                } else {
                    match gzip_body(&body) {
                        Ok(compressed) => compressed,
                        Err(e) => {
                            return SendResult::Fatal(TransportError::Internal(format!(
                                "OTLP {compression}: {e}"
                            )));
                        }
                    }
                };
                let response = client
                    .post(url)
                    .headers(headers.clone())
                    .timeout(budget)
                    .body(body)
                    .send()
                    .await;
                // reqwest's error text names the URL, which may carry a password.
                match response.map_err(reqwest::Error::without_url) {
                    Ok(response) => self.http_answer(response, sent, encoded_len).await,
                    Err(e) if e.is_builder() => SendResult::Fatal(TransportError::Send(format!(
                        "OTLP HTTP request could not be built: {e}"
                    ))),
                    // A refused certificate, name or key pin is the config's to fix. Refused,
                    // reset and timed-out connections clear when the collector is back.
                    Err(e) => {
                        if let Some(tls) = crate::tls::rustls_error(&e) {
                            return SendResult::Fatal(TransportError::Config(format!(
                                "OTLP HTTP: TLS with the collector failed: {tls}"
                            )));
                        }
                        if !e.is_timeout()
                            && let Some((host, port)) = tunnel
                            && let Some(refused) =
                                dialer.refusal_of(Scheme::Tls, host, *port, budget).await
                        {
                            return self.refusals.hold(&refused, "otlp");
                        }
                        tracing::debug!(endpoint = %self.endpoint, error = %e, "OTLP HTTP request failed");
                        SendResult::Backpressured
                    }
                }
            }
        }
    }

    /// The result an HTTP response to a request of `encoded_len` bytes,
    /// before compression, gives.
    async fn http_answer(
        &self,
        response: reqwest::Response,
        sent: usize,
        encoded_len: usize,
    ) -> SendResult {
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(crate::backoff::parse_retry_after);
        let protobuf = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("protobuf"));
        let body = response.bytes().await;
        if status.is_success() {
            // A body that cannot be read or decoded still follows a status that took the records.
            let partial = body
                .ok()
                .and_then(|b| ExportLogsServiceResponse::decode(b).ok())
                .and_then(|r| r.partial_success);
            return accepted(partial, sent);
        }
        let message = body.map_or_else(|_| String::new(), |b| error_text(&b, protobuf));
        http_failure(
            status.as_u16(),
            retry_after,
            self.max_retry_delay,
            encoded_len,
            format!("OTLP HTTP {status}: {message}"),
        )
    }

    /// Log and count one export's result.
    fn observe(
        &self,
        result: &SendResult,
        records: usize,
        bytes: usize,
        started: Instant,
        budget: Duration,
    ) {
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        match result {
            SendResult::Ok => {
                tracing::debug!(endpoint = %self.endpoint, records, bytes, elapsed_ms, "OTLP export accepted");
                Self::count_sent(records as u64, bytes as u64);
            }
            SendResult::Backpressured | SendResult::RetryAfter(_) => {
                // A refused tunnel is logged once per run, so each retry stays at debug.
                if self.refusals.is_refused() {
                    tracing::debug!(endpoint = %self.endpoint, records, "OTLP export held behind a refused proxy");
                } else {
                    tracing::warn!(
                        endpoint = %self.endpoint,
                        records,
                        elapsed_ms,
                        budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
                        retry_after_ms = result.retry_after().map(|w| u64::try_from(w.as_millis()).unwrap_or(u64::MAX)),
                        "OTLP export not taken; the records may have arrived and are sent again"
                    );
                }
                Self::count_backpressured();
            }
            SendResult::Reject(error) => {
                tracing::warn!(endpoint = %self.endpoint, records, error = %error, "OTLP export rejected; the records are dead-lettered");
                Self::count_send_error();
            }
            SendResult::Fatal(error) => {
                tracing::error!(endpoint = %self.endpoint, records, error = %error, "OTLP export failed; fix the transport config or the collector");
                Self::count_send_error();
            }
            SendResult::FilteredDlq => {}
        }
    }
}

impl TransportBase for OtlpTransport {
    /// Refuse further sends with `Fatal(Closed)`. Idempotent.
    async fn close(&self) -> TransportResult<()> {
        self.lifecycle.close();
        Ok(())
    }

    /// `false` after `close()`, and while the proxy refuses CONNECT. Does not
    /// probe the collector.
    fn is_healthy(&self) -> bool {
        self.lifecycle.is_healthy()
    }

    fn name(&self) -> &'static str {
        Self::TRANSPORT
    }

    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::Acknowledged
    }
}

impl Meter for OtlpTransport {
    const TRANSPORT: &'static str = "otlp";
}

impl TransportSender for OtlpTransport {
    /// Send `payload` as one log record with no headers. `destination` is not
    /// sent.
    async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
        let format = PayloadFormat::detect(&payload);
        let record = Record {
            payload,
            key: None,
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format,
            },
        };
        self.export(std::slice::from_ref(&record), None).await
    }

    async fn send_record(&self, record: &Record) -> SendResult {
        self.export(std::slice::from_ref(record), None).await
    }

    /// Send the block as one export request.
    async fn send_batch(&self, records: &[Record]) -> SendResult {
        self.export(records, None).await
    }

    /// Send the block as one export request, with the time left to `deadline`
    /// as its limit when that is under `timeout_ms`.
    async fn send_batch_by(&self, records: &[Record], deadline: Instant) -> SendResult {
        self.export(records, Some(deadline)).await
    }

    /// The collector answered that it took the records.
    fn confirms_delivery(&self) -> SinkConfirmation {
        SinkConfirmation::Remote
    }

    /// A record an outbound `dlq` filter matches.
    fn dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        self.filter_engine
            .outbound_dead_letter(&record.payload)
            .then_some(DeadLetterReason::OutboundFilter)
    }

    /// Its outbound filters, which `send_screened` then skips.
    fn screen(&self, record: &Record) -> super::ack::Screened {
        self.filter_engine.screen(&record.payload)
    }

    /// The records its screen kept, without running their outbound filters
    /// again.
    async fn send_screened(
        &self,
        records: &[Record],
        deadline: Option<std::time::Instant>,
    ) -> SendResult {
        self.filter_engine
            .screened(super::traits::send_within(self, records, deadline))
            .await
    }
}

/// The gRPC client for `url`, with every header as metadata.
fn grpc_wire(
    config: &OtlpConfig,
    url: &str,
    tls: Option<&crate::tls::ClientTls>,
) -> TransportResult<Wire> {
    use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};

    // The lazy channel spawns its connection task on the current runtime.
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(config_error(
            "a gRPC transport is built inside a Tokio runtime".to_string(),
        ));
    }
    let mut endpoint = tonic::transport::Endpoint::from_shared(url.to_string())
        .map_err(|e| config_error(format!("invalid endpoint: {e}")))?;
    if let Some(tls) = tls {
        let tls = crate::tls::tonic_client_tls(tls).map_err(|e| config_error(e.to_string()))?;
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| config_error(format!("TLS: {e}")))?;
    }
    let channel =
        super::tonic_channel::lazy_channel(endpoint, config.timeout_ms, config.proxy.dialer()?);
    let mut client = LogsServiceClient::new(channel);
    for &accepted in super::tonic_channel::ACCEPTED_ENCODINGS {
        client = client.accept_compressed(accepted);
    }
    if let Some(encoding) = super::tonic_channel::encoding(config.compression.codec) {
        client = client.send_compressed(encoding);
    }

    let mut metadata = MetadataMap::new();
    for (name, value) in &config.headers {
        let key = AsciiMetadataKey::from_bytes(name.as_bytes())
            .map_err(|_| config_error(format!("header {name:?} is not a gRPC metadata name")))?;
        // The value is never shown: it is commonly a credential.
        let mut value = AsciiMetadataValue::try_from(value.expose())
            .map_err(|_| config_error(format!("header {name:?} has a value gRPC cannot send")))?;
        value.set_sensitive(true);
        metadata.insert(key, value);
    }
    Ok(Wire::Grpc { client, metadata })
}

/// The HTTP client for `url`, redirects off, and the headers every request
/// carries.
fn http_wire(
    config: &OtlpConfig,
    url: String,
    tls: Option<&crate::tls::ClientTls>,
) -> TransportResult<Wire> {
    use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};

    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(config.timeout_ms))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(tls) = tls {
        let tls = crate::tls::build_client_tls(crate::crypto::CryptoProfile::default(), tls)
            .map_err(|e| config_error(e.to_string()))?;
        builder = builder.use_preconfigured_tls((*tls).clone());
    }
    let dialer = config.proxy.dialer()?;
    let client = crate::transport::proxy::reqwest_proxy(builder, &config.proxy, &dialer)
        .build()
        .map_err(|e| config_error(format!("HTTP client: {e}")))?;
    // reqwest's tunnel error carries no status, so a refused CONNECT is read
    // by one CONNECT of the transport's own to an https collector.
    let tunnel = reqwest::Url::parse(&url)
        .ok()
        .filter(|u| u.scheme() == "https")
        .and_then(|u| Some((u.host_str()?.to_string(), u.port_or_known_default()?)));

    let mut headers = HeaderMap::new();
    for (name, value) in &config.headers {
        let key = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| config_error(format!("header {name:?} is not an HTTP header name")))?;
        let mut value = HeaderValue::from_str(value.expose())
            .map_err(|_| config_error(format!("header {name:?} has a value HTTP cannot send")))?;
        value.set_sensitive(true);
        headers.insert(key, value);
    }
    // Set after the configured headers, so the body is always described truly.
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );
    if config.compression.is_none() {
        headers.remove(CONTENT_ENCODING);
    } else {
        headers.insert(
            CONTENT_ENCODING,
            HeaderValue::from_static(config.compression.codec.name()),
        );
    }
    Ok(Wire::Http {
        client,
        url,
        headers,
        compression: config.compression,
        dialer,
        tunnel,
    })
}

/// `Ok` when the collector took every record, `Reject` with its message when
/// it refused any.
fn accepted(partial: Option<ExportLogsPartialSuccess>, sent: usize) -> SendResult {
    match partial {
        Some(p) if p.rejected_log_records > 0 => SendResult::Reject(TransportError::Send(format!(
            "OTLP collector rejected {} of {sent} log records: {}",
            p.rejected_log_records,
            truncated(&p.error_message)
        ))),
        Some(p) if !p.error_message.is_empty() => {
            tracing::warn!(
                message = %truncated(&p.error_message),
                "OTLP collector took every record with a warning"
            );
            SendResult::Ok
        }
        _ => SendResult::Ok,
    }
}

/// The result for a failed gRPC export.
///
/// `UNAVAILABLE` and the other transient codes retry, `RESOURCE_EXHAUSTED`
/// only with `RetryInfo`, as the OTLP specification has it: without one it is
/// most often a request over the collector's size limit, which the same
/// request meets every time.
fn grpc_failure(status: &tonic::Status, max_retry_delay: Duration) -> SendResult {
    use tonic::Code;

    if let Some(tls) = crate::tls::rustls_error(status) {
        return SendResult::Fatal(TransportError::Config(format!(
            "OTLP gRPC: TLS with the collector failed: {tls}"
        )));
    }
    // tonic attaches a source error only when the client's own connection
    // failed or its stream was reset: the collector never answered.
    if std::error::Error::source(status).is_some() {
        return SendResult::Backpressured;
    }
    let hint = status
        .get_details_retry_info()
        .and_then(|info| info.retry_delay);
    let detail = || {
        format!(
            "OTLP gRPC {:?}: {}",
            status.code(),
            truncated(status.message())
        )
    };
    match status.code() {
        Code::Unavailable => retry(hint, max_retry_delay),
        Code::ResourceExhausted if hint.is_some() => retry(hint, max_retry_delay),
        Code::Cancelled
        | Code::DeadlineExceeded
        | Code::Aborted
        | Code::OutOfRange
        | Code::DataLoss => SendResult::Backpressured,
        Code::Unauthenticated | Code::PermissionDenied | Code::NotFound | Code::Unimplemented => {
            SendResult::Fatal(TransportError::Config(detail()))
        }
        _ => SendResult::Reject(TransportError::Send(detail())),
    }
}

/// The result for an HTTP status outside 2xx, for a request of `encoded_len`
/// bytes.
///
/// 429, 502, 503 and 504 retry, as the OTLP specification has it. A wrong
/// credential or endpoint is `Fatal(Config)`, which says nothing about the
/// records, and every other status refuses them for good.
fn http_failure(
    status: u16,
    retry_after: Option<Duration>,
    max_retry_delay: Duration,
    encoded_len: usize,
    detail: String,
) -> SendResult {
    match status {
        429 | 502 | 503 | 504 => retry(retry_after, max_retry_delay),
        401 | 403 | 404 | 405 | 407 | 410 | 421 => {
            SendResult::Fatal(TransportError::Config(detail))
        }
        413 => SendResult::Reject(TransportError::MessageTooLarge {
            bytes: encoded_len,
            detail,
        }),
        400..=599 => SendResult::Reject(TransportError::Send(detail)),
        // 1xx and an unfollowed 3xx: the endpoint is not one that takes an export.
        _ => SendResult::Fatal(TransportError::Config(detail)),
    }
}

/// `RetryAfter` with the collector's wait, capped at `max`, or `Backpressured`
/// when it named none or named no wait at all.
fn retry(hint: Option<Duration>, max: Duration) -> SendResult {
    match hint
        .map(|wait| wait.min(max))
        .filter(|wait| !wait.is_zero())
    {
        Some(wait) => SendResult::RetryAfter(wait),
        None => SendResult::Backpressured,
    }
}

/// The message of an OTLP/HTTP error body: a `google.rpc.Status` when it is
/// protobuf, the text otherwise.
fn error_text(body: &[u8], protobuf: bool) -> String {
    if protobuf {
        tonic_types::Status::decode(body)
            .map(|status| truncated(&status.message))
            .unwrap_or_default()
    } else {
        truncated(&String::from_utf8_lossy(body))
    }
}

/// At most [`MAX_DETAIL_CHARS`] of `text`, trimmed.
fn truncated(text: &str) -> String {
    text.trim().chars().take(MAX_DETAIL_CHARS).collect()
}

/// `body` gzipped, the one codec [`OTLP_ACCEPTS`] takes. flate2 directly,
/// so OTLP compiles no other codec.
fn gzip_body(body: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Write as _;
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(body.len() / 4),
        flate2::Compression::default(),
    );
    encoder.write_all(body)?;
    encoder.finish()
}

/// `url` with any user name and password left out.
fn loggable(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{}://{host}:{port}{}", url.scheme(), url.path()),
        None => format!("{}://{host}{}", url.scheme(), url.path()),
    }
}

fn config_error(message: impl std::fmt::Display) -> TransportError {
    TransportError::Config(format!("OTLP: {message}"))
}
