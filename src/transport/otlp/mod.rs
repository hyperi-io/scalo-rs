// Project:   scalo
// File:      src/transport/otlp/mod.rs
// Purpose:   OTLP transport: logs and metrics over gRPC or HTTP/protobuf
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! # OTLP transport
//!
//! Sends records to an OpenTelemetry collector as OTLP log records, over gRPC
//! (`LogsService/Export`) or HTTP/protobuf (`POST /v1/logs`). Send only. A
//! block goes as one export request, and `Ok` means the collector answered
//! that it took every record. [`OtlpMetricExporter`] sends an OpenTelemetry
//! SDK's metrics over the same client (`MetricsService/Export` or
//! `POST /v1/metrics`).
//!
//! Each record's payload is the log body, a string when it is UTF-8 and bytes
//! otherwise. Its [`TIME_UNIX_NANO_HEADER`] header, else its timestamp, is the
//! log time, the [`SEVERITY_HEADER`] header its severity, and every other
//! header an attribute. The resource carries
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
mod metrics;
mod record;
#[cfg(test)]
mod tests;

pub use crate::transport::TIME_UNIX_NANO_HEADER;
pub use config::{OTLP_ACCEPTS, OtlpConfig, OtlpProtocol};
pub use metrics::OtlpMetricExporter;
pub use record::SEVERITY_HEADER;

use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
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
use record::OtlpResource;

/// The most characters of a collector's error text a result carries.
const MAX_DETAIL_CHARS: usize = 512;

/// Sends records as OTLP log records to one collector, and metrics through
/// [`OtlpMetricExporter`].
///
/// Every send answers within `timeout_ms`, and `send_batch_by` within its
/// deadline too, whichever comes first.
pub struct OtlpTransport {
    wire: Wire,
    resource: OtlpResource,
    timeout: Duration,
    max_retry_delay: Duration,
    /// The endpoint with any user name and password left out, for logs.
    endpoint: String,
    /// Closed, or `refusals` refused: either fails readiness.
    lifecycle: Lifecycle,
    /// Whether the proxy is refusing CONNECT, which lasts until an export goes through.
    refusals: crate::transport::proxy::Refusals,
    filter_engine: TransportFilterEngine,
    /// The TLS settings of an `https://` collector, which names a handshake
    /// failure. `None` in plain text.
    settings: Option<crate::tls::ProfileSettings>,
}

/// The client for the configured protocol and what every request carries.
enum Wire {
    Grpc(Box<GrpcWire>),
    Http {
        client: reqwest::Client,
        logs_url: String,
        metrics_url: String,
        headers: reqwest::header::HeaderMap,
        compression: Compression,
        dialer: crate::transport::ProxyDialer,
        /// The https collector's host and port, for reading a refused CONNECT.
        tunnel: Option<(String, u16)>,
    },
}

/// The gRPC logs and metrics clients, on one channel, and the metadata every
/// call carries.
struct GrpcWire {
    logs: LogsServiceClient<tonic::transport::Channel>,
    metrics: MetricsServiceClient<tonic::transport::Channel>,
    metadata: tonic::metadata::MetadataMap,
}

/// The OTLP signals the transport sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    Logs,
    Metrics,
}

impl Signal {
    /// The OTLP/HTTP path, appended to the endpoint's base.
    const fn path(self) -> &'static str {
        match self {
            Self::Logs => "v1/logs",
            Self::Metrics => "v1/metrics",
        }
    }

    /// What the collector refused, from the body of an HTTP response that took the export.
    fn refused(self, body: &[u8]) -> Option<Refused> {
        match self {
            Self::Logs => ExportLogsServiceResponse::decode(body)
                .ok()?
                .partial_success
                .map(Refused::from),
            Self::Metrics => ExportMetricsServiceResponse::decode(body)
                .ok()?
                .partial_success
                .map(Refused::from),
        }
    }
}

/// One export request.
enum Export {
    /// Log records, and how many there are for the refusal text.
    Logs {
        request: ExportLogsServiceRequest,
        records: usize,
    },
    Metrics(ExportMetricsServiceRequest),
}

impl Export {
    fn signal(&self) -> Signal {
        match self {
            Self::Logs { .. } => Signal::Logs,
            Self::Metrics(_) => Signal::Metrics,
        }
    }

    fn counted(&self) -> Counted {
        match self {
            Self::Logs { records, .. } => Counted::LogRecords(*records),
            Self::Metrics(_) => Counted::DataPoints,
        }
    }

    fn encode_to_vec(&self) -> Vec<u8> {
        match self {
            Self::Logs { request, .. } => request.encode_to_vec(),
            Self::Metrics(request) => request.encode_to_vec(),
        }
    }
}

/// What a refusal's count is of, for its message.
#[derive(Debug, Clone, Copy)]
enum Counted {
    /// The records the export carried.
    LogRecords(usize),
    DataPoints,
}

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LogRecords(records) => write!(f, "of {records} log records"),
            Self::DataPoints => f.write_str("data points"),
        }
    }
}

/// What a collector that took an export said it refused of it.
struct Refused {
    count: i64,
    message: String,
}

impl From<ExportLogsPartialSuccess> for Refused {
    fn from(partial: ExportLogsPartialSuccess) -> Self {
        Self {
            count: partial.rejected_log_records,
            message: partial.error_message,
        }
    }
}

impl From<ExportMetricsPartialSuccess> for Refused {
    fn from(partial: ExportMetricsPartialSuccess) -> Self {
        Self {
            count: partial.rejected_data_points,
            message: partial.error_message,
        }
    }
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
        let url = config.url(Signal::Logs)?;
        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| config_error(format!("invalid endpoint: {e}")))?;
        let tls = config.client_tls(&url)?;
        let settings = tls
            .as_ref()
            .map(crate::tls::ClientTls::profile_settings)
            .transpose()
            .map_err(|e| config_error(e.to_string()))?;
        let connect_to = config.connect_to()?;
        let wire = match config.protocol {
            OtlpProtocol::Grpc => grpc_wire(config, &url, tls.as_ref(), connect_to)?,
            OtlpProtocol::Http => http_wire(
                config,
                url,
                config.url(Signal::Metrics)?,
                tls.as_ref(),
                connect_to,
            )?,
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
            connect_to = connect_to.map(|to| to.to_string()).as_deref(),
            protocol = config.protocol.as_str(),
            compression = %config.compression,
            timeout_ms = config.timeout_ms,
            max_retry_delay_ms = config.max_retry_delay_ms,
            tls = tls.as_ref().map_or("none", |t| t.trust.as_str()),
            mutual_tls = tls.as_ref().is_some_and(|t| t.identity.is_some()),
            headers = config.headers.len(),
            guarantee = %DeliveryGuarantee::Acknowledged,
            "OTLP transport built"
        );
        Ok(Self {
            wire,
            resource: OtlpResource::new(config),
            timeout: Duration::from_millis(config.timeout_ms),
            max_retry_delay: Duration::from_millis(config.max_retry_delay_ms),
            endpoint,
            lifecycle,
            refusals,
            filter_engine,
            settings,
        })
    }

    /// Why the TLS handshake behind `error` failed, named on this transport's
    /// profile. `None` when no TLS error is in it.
    fn tls_failure(&self, error: &(dyn std::error::Error + 'static)) -> Option<String> {
        match self.settings {
            Some(settings) => crate::tls::describe_failure(error, settings),
            None => crate::tls::rustls_error(error).map(ToString::to_string),
        }
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

        let request = Export::Logs {
            request,
            records: sent,
        };
        // tokio's timeout polls the export once before it looks at the clock,
        // so a deadline already past still takes an answer that is ready.
        let result = tokio::time::timeout(budget, self.export_on_wire(request, budget))
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

    /// Send one metric export request, answering within `timeout_ms`.
    ///
    /// A metric export carries no records, so no filter runs and no record
    /// counter moves. It is logged at debug: [`OtlpMetricExporter`] hands the
    /// result to the SDK, which reports it.
    async fn export_metrics(&self, request: ExportMetricsServiceRequest) -> SendResult {
        if self.lifecycle.is_closed() {
            return SendResult::Fatal(TransportError::Closed);
        }
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.timeout,
            self.export_on_wire(Export::Metrics(request), self.timeout),
        )
        .await
        .unwrap_or(SendResult::Backpressured);
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        tracing::debug!(endpoint = %self.endpoint, result = ?result, elapsed_ms, "OTLP metric export answered");
        if result.is_ok() {
            self.refusals.clear("otlp");
        }
        result
    }

    async fn export_on_wire(&self, request: Export, budget: Duration) -> SendResult {
        let counted = request.counted();
        match &self.wire {
            Wire::Grpc(grpc) => {
                let GrpcWire {
                    logs,
                    metrics,
                    metadata,
                } = grpc.as_ref();
                let answer = match request {
                    Export::Logs { request, .. } => logs
                        .clone()
                        .export(grpc_call(request, metadata, budget))
                        .await
                        .map(|response| response.into_inner().partial_success.map(Refused::from)),
                    Export::Metrics(request) => metrics
                        .clone()
                        .export(grpc_call(request, metadata, budget))
                        .await
                        .map(|response| response.into_inner().partial_success.map(Refused::from)),
                };
                match answer {
                    Ok(refused) => accepted(refused, counted),
                    Err(status) => match crate::transport::proxy::refusal(&status) {
                        Some(refused) => self.refusals.hold(refused, "otlp"),
                        None => {
                            grpc_failure(&status, self.max_retry_delay, self.tls_failure(&status))
                        }
                    },
                }
            }
            Wire::Http {
                client,
                logs_url,
                metrics_url,
                headers,
                compression,
                dialer,
                tunnel,
            } => {
                let signal = request.signal();
                let url = match signal {
                    Signal::Logs => logs_url,
                    Signal::Metrics => metrics_url,
                };
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
                    Ok(response) => {
                        self.http_answer(response, signal, counted, encoded_len)
                            .await
                    }
                    Err(e) if e.is_builder() => SendResult::Fatal(TransportError::Send(format!(
                        "OTLP HTTP request could not be built: {e}"
                    ))),
                    // A refused certificate, name or key pin is the config's to fix. Refused,
                    // reset and timed-out connections clear when the collector is back.
                    Err(e) => {
                        if let Some(tls) = self.tls_failure(&e) {
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

    /// The result an HTTP response to a `signal` request of `encoded_len`
    /// bytes, before compression, gives. `counted` names what a refusal's
    /// count is of.
    async fn http_answer(
        &self,
        response: reqwest::Response,
        signal: Signal,
        counted: Counted,
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
            // A body that cannot be read or decoded still follows a status that took the export.
            let refused = body.ok().and_then(|b| signal.refused(&b));
            return accepted(refused, counted);
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
}

/// The gRPC logs and metrics clients for `url` on one channel, dialling
/// `connect_to` when set, with every header as metadata.
fn grpc_wire(
    config: &OtlpConfig,
    url: &str,
    tls: Option<&crate::tls::ClientTls>,
    connect_to: Option<crate::transport::connect_to::ConnectTo>,
) -> TransportResult<Wire> {
    use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};

    // The lazy channel spawns its connection task on the current runtime.
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(config_error(
            "a gRPC transport is built inside a Tokio runtime".to_string(),
        ));
    }
    let channel_tls = tls
        .map(|tls| {
            let rustls = crate::tls::build_client_tls(tls).map_err(|e| e.to_string())?;
            let name = match &tls.server_name {
                Some(name) => name.clone(),
                None => super::tonic_channel::origin_host(url)?,
            };
            super::tonic_channel::ChannelTls::new(&rustls, &name)
        })
        .transpose()
        .map_err(config_error)?;
    let dial = connect_to
        .map(|to| {
            let parsed = reqwest::Url::parse(url).map_err(|e| format!("invalid endpoint: {e}"))?;
            to.dial_url(&parsed)
        })
        .transpose()
        .map_err(config_error)?;
    let endpoint = super::tonic_channel::endpoint(url, dial.as_deref(), channel_tls.is_some())
        .map_err(config_error)?;
    let channel = super::tonic_channel::lazy_channel(
        endpoint,
        config.timeout_ms,
        config.proxy.dialer()?,
        channel_tls,
    );
    let mut logs = LogsServiceClient::new(channel.clone());
    let mut metrics = MetricsServiceClient::new(channel);
    for &accepted in super::tonic_channel::ACCEPTED_ENCODINGS {
        logs = logs.accept_compressed(accepted);
        metrics = metrics.accept_compressed(accepted);
    }
    if let Some(encoding) = super::tonic_channel::encoding(config.compression.codec) {
        logs = logs.send_compressed(encoding);
        metrics = metrics.send_compressed(encoding);
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
    Ok(Wire::Grpc(Box::new(GrpcWire {
        logs,
        metrics,
        metadata,
    })))
}

/// The HTTP client for the logs and metrics URLs, redirects off, dialling
/// `connect_to` when set, and the headers every request carries.
fn http_wire(
    config: &OtlpConfig,
    mut logs_url: String,
    mut metrics_url: String,
    tls: Option<&crate::tls::ClientTls>,
    connect_to: Option<crate::transport::connect_to::ConnectTo>,
) -> TransportResult<Wire> {
    use reqwest::header::{
        CONTENT_ENCODING, CONTENT_TYPE, HOST, HeaderMap, HeaderName, HeaderValue,
    };

    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(config.timeout_ms))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(tls) = tls {
        let tls = crate::tls::build_client_tls(tls).map_err(|e| config_error(e.to_string()))?;
        builder = builder.use_preconfigured_tls((*tls).clone());
    }
    let mut host = None;
    if let Some(to) = connect_to {
        let parse = |url: &str| {
            reqwest::Url::parse(url).map_err(|e| config_error(format!("invalid endpoint: {e}")))
        };
        let logs = parse(&logs_url)?;
        builder = to.resolve(builder, &logs).map_err(config_error)?;
        host = Some(
            crate::transport::connect_to::ConnectTo::host_header(&logs).map_err(config_error)?,
        );
        logs_url = to.request_url(&logs).map_err(config_error)?;
        metrics_url = to
            .request_url(&parse(&metrics_url)?)
            .map_err(config_error)?;
    }
    let dialer = config.proxy.dialer()?;
    let client = crate::transport::proxy::reqwest_proxy(builder, &config.proxy, &dialer)
        .build()
        .map_err(|e| config_error(format!("HTTP client: {e}")))?;
    // reqwest's tunnel error carries no status, so a refused CONNECT is read
    // by one CONNECT of the transport's own to an https collector.
    let tunnel = reqwest::Url::parse(&logs_url)
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
    if let Some(host) = host {
        headers.insert(HOST, host);
    }
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
        logs_url,
        metrics_url,
        headers,
        compression: config.compression,
        dialer,
        tunnel,
    })
}

/// `message` as a gRPC call carrying `metadata`, limited to `budget`.
fn grpc_call<T>(
    message: T,
    metadata: &tonic::metadata::MetadataMap,
    budget: Duration,
) -> tonic::Request<T> {
    let mut call = tonic::Request::new(message);
    *call.metadata_mut() = metadata.clone();
    // Sent as `grpc-timeout`, so the collector gives up when the sender does.
    call.set_timeout(budget);
    call
}

/// `Ok` when the collector took all of an export, `Reject` with its message
/// when it refused any. `counted` names what the count is of.
fn accepted(refused: Option<Refused>, counted: Counted) -> SendResult {
    match refused {
        Some(r) if r.count > 0 => SendResult::Reject(TransportError::Send(format!(
            "OTLP collector rejected {} {counted}: {}",
            r.count,
            truncated(&r.message)
        ))),
        Some(r) if !r.message.is_empty() => {
            tracing::warn!(
                message = %truncated(&r.message),
                "OTLP collector took the export with a warning"
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
fn grpc_failure(
    status: &tonic::Status,
    max_retry_delay: Duration,
    tls: Option<String>,
) -> SendResult {
    use tonic::Code;

    if let Some(tls) = tls {
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
        tonic_types::Status::decode(body).map_or_default(|status| truncated(&status.message))
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
