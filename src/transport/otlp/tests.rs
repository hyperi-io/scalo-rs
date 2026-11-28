// Project:   scalo
// File:      src/transport/otlp/tests.rs
// Purpose:   The OTLP transport's logs and metrics against a local collector per protocol
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message as _;
use tonic::Code;
use tonic_types::{ErrorDetails, StatusExt as _};

use super::{OtlpConfig, OtlpProtocol};
use crate::compression::{Codec, Compression};
use crate::tls::test_pki::{Leaf, TestCa};
use crate::tls::{ClientTlsConfig, TrustChoice};
use crate::transport::filter::{FilterAction, FilterRule};
use crate::transport::{
    AnySender, PayloadFormat, Record, RecordMeta, TransportBase, TransportConfig, TransportSender,
    TransportType,
};

/// What the stub collector answers every export with.
#[derive(Debug, Clone, Copy)]
enum Answer {
    Accept,
    /// Accepted with this many records rejected.
    Partial(i64),
    /// An HTTP status and its `Retry-After`.
    Http(u16, Option<&'static str>),
    /// A gRPC code and its `RetryInfo` delay.
    Grpc(Code, Option<Duration>),
    /// Never answers.
    Hang,
    /// Nothing listens.
    Down,
}

/// What the stub collector received.
#[derive(Debug, Default)]
struct Seen {
    requests: Vec<ExportLogsServiceRequest>,
    metrics: Vec<ExportMetricsServiceRequest>,
    tenant: Vec<Option<String>>,
    gzip: Vec<bool>,
    /// The `Host` header of each HTTP export.
    host: Vec<Option<String>>,
}

type Shared = Arc<Mutex<Seen>>;

/// An export request the stub collector received.
enum Received {
    Logs(ExportLogsServiceRequest),
    Metrics(ExportMetricsServiceRequest),
}

fn note(seen: &Shared, request: Received, tenant: Option<&str>, gzip: bool) {
    let mut seen = seen.lock().unwrap();
    match request {
        Received::Logs(request) => seen.requests.push(request),
        Received::Metrics(request) => seen.metrics.push(request),
    }
    seen.tenant.push(tenant.map(str::to_string));
    seen.gzip.push(gzip);
}

fn partial(rejected: i64) -> ExportLogsServiceResponse {
    ExportLogsServiceResponse {
        partial_success: Some(ExportLogsPartialSuccess {
            rejected_log_records: rejected,
            error_message: "two were bad".to_string(),
        }),
    }
}

fn metrics_partial(rejected: i64) -> ExportMetricsServiceResponse {
    ExportMetricsServiceResponse {
        partial_success: Some(ExportMetricsPartialSuccess {
            rejected_data_points: rejected,
            error_message: "two were bad".to_string(),
        }),
    }
}

struct GrpcCollector {
    answer: Answer,
    seen: Shared,
}

impl GrpcCollector {
    /// Note `request` and give the configured answer, `partial` building a
    /// partial success.
    async fn take<T, R: Default>(
        &self,
        request: tonic::Request<T>,
        received: impl FnOnce(T) -> Received,
        partial: impl FnOnce(i64) -> R,
    ) -> Result<tonic::Response<R>, tonic::Status> {
        let metadata = request.metadata();
        let tenant = metadata
            .get("x-tenant")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let gzip = metadata
            .get("grpc-encoding")
            .is_some_and(|v| v.to_str().ok() == Some("gzip"));
        note(
            &self.seen,
            received(request.into_inner()),
            tenant.as_deref(),
            gzip,
        );
        match self.answer {
            Answer::Accept => Ok(tonic::Response::new(R::default())),
            Answer::Partial(rejected) => Ok(tonic::Response::new(partial(rejected))),
            Answer::Grpc(code, Some(delay)) => Err(tonic::Status::with_error_details(
                code,
                "collector says no",
                ErrorDetails::with_retry_info(Some(delay)),
            )),
            Answer::Grpc(code, None) => Err(tonic::Status::new(code, "collector says no")),
            Answer::Hang => std::future::pending().await,
            Answer::Http(..) | Answer::Down => unreachable!("not a gRPC answer"),
        }
    }
}

#[tonic::async_trait]
impl LogsService for GrpcCollector {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        self.take(request, Received::Logs, partial).await
    }
}

#[tonic::async_trait]
impl MetricsService for GrpcCollector {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.take(request, Received::Metrics, metrics_partial).await
    }
}

/// A gRPC collector on localhost taking logs and metrics, with TLS asking for
/// a client certificate from `ca` when there is one.
async fn grpc_collector(answer: Answer, seen: Shared, ca: Option<&TestCa>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let logs = LogsServiceServer::new(GrpcCollector {
        answer,
        seen: Arc::clone(&seen),
    })
    .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    let metrics = MetricsServiceServer::new(GrpcCollector { answer, seen })
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(logs)
            .add_service(metrics)
            .serve_with_incoming(incoming),
    );
    let Some(ca) = ca else {
        return addr;
    };
    let (tls, _files) = crate::tls::test_pki::server_tls_with(
        ca,
        crate::crypto::CryptoProfile::Strong,
        &[b"h2"],
        crate::tls::ClientAuth::Required,
    )
    .await;
    crate::tls::test_pki::Front::start(tls, addr).await.addr
}

/// An HTTP collector on localhost taking `POST /v1/logs` and `POST /v1/metrics`.
async fn http_collector(answer: Answer, seen: Shared) -> SocketAddr {
    use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
    use axum::response::{IntoResponse, Response};

    let handler = move |metrics: bool, headers: HeaderMap, body: axum::body::Bytes| {
        let seen = Arc::clone(&seen);
        async move {
            let gzip =
                headers.get(header::CONTENT_ENCODING) == Some(&HeaderValue::from_static("gzip"));
            let body = if gzip {
                let mut plain = Vec::new();
                let mut decoder = flate2::read::GzDecoder::new(&body[..]);
                std::io::Read::read_to_end(&mut decoder, &mut plain).unwrap();
                plain
            } else {
                body.to_vec()
            };
            assert_eq!(
                headers.get(header::CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/x-protobuf"))
            );
            let tenant = headers.get("x-tenant").and_then(|v| v.to_str().ok());
            let request = if metrics {
                Received::Metrics(ExportMetricsServiceRequest::decode(&body[..]).unwrap())
            } else {
                Received::Logs(ExportLogsServiceRequest::decode(&body[..]).unwrap())
            };
            note(&seen, request, tenant, gzip);
            let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
            seen.lock().unwrap().host.push(host.map(str::to_string));
            let protobuf = [(header::CONTENT_TYPE, "application/x-protobuf")];
            match answer {
                Answer::Accept => StatusCode::OK.into_response(),
                Answer::Partial(rejected) => {
                    let body = if metrics {
                        metrics_partial(rejected).encode_to_vec()
                    } else {
                        partial(rejected).encode_to_vec()
                    };
                    (StatusCode::OK, protobuf, body).into_response()
                }
                Answer::Http(code, retry_after) => {
                    let status = tonic_types::Status {
                        message: "bad record".to_string(),
                        ..tonic_types::Status::default()
                    };
                    let code = StatusCode::from_u16(code).unwrap();
                    let mut response = (code, protobuf, status.encode_to_vec()).into_response();
                    if let Some(wait) = retry_after {
                        response
                            .headers_mut()
                            .insert(header::RETRY_AFTER, HeaderValue::from_static(wait));
                    }
                    response
                }
                Answer::Hang => std::future::pending::<Response>().await,
                Answer::Grpc(..) | Answer::Down => unreachable!("not an HTTP answer"),
            }
        }
    };
    let logs = handler.clone();
    let app = axum::Router::new()
        .route(
            "/v1/logs",
            axum::routing::post(move |headers: HeaderMap, body: axum::body::Bytes| {
                logs(false, headers, body)
            }),
        )
        .route(
            "/v1/metrics",
            axum::routing::post(move |headers: HeaderMap, body: axum::body::Bytes| {
                handler(true, headers, body)
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

/// The block every case sends: a JSON record with a timestamp, a severity and
/// a header, a record that is not UTF-8, and one the `dead` filter matches.
fn block() -> Vec<Record> {
    let record = |payload: &'static [u8], headers: Vec<(String, Vec<u8>)>, ts| Record {
        payload: bytes::Bytes::from_static(payload),
        key: Some("ignored".into()),
        headers,
        metadata: RecordMeta {
            timestamp_ms: ts,
            format: PayloadFormat::Json,
        },
    };
    vec![
        record(
            br#"{"msg":"one"}"#,
            vec![
                ("severity".to_string(), b"error".to_vec()),
                ("source".to_string(), b"auth".to_vec()),
            ],
            Some(1_700_000_000_000),
        ),
        record(&[0xff, 0xfe], Vec::new(), None),
        record(br#"{"dead":true}"#, Vec::new(), None),
    ]
}

/// Check the records, the header and the resource the collector got.
fn check_seen(case: &str, seen: &Seen, records: usize, gzip: bool) {
    assert_eq!(seen.requests.len(), 1, "{case}: one request per block");
    assert_eq!(
        seen.tenant,
        vec![Some("blue".to_string())],
        "{case}: header"
    );
    assert_eq!(seen.gzip, vec![gzip], "{case}: compression");
    let logs = &seen.requests[0].resource_logs[0];
    let service = logs.resource.as_ref().and_then(|r| {
        r.attributes
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| kv.value.clone())
    });
    assert_eq!(
        service.and_then(|v| v.value),
        Some(Value::StringValue("stub-test".to_string())),
        "{case}: resource"
    );
    let sent = &logs.scope_logs[0].log_records;
    assert_eq!(sent.len(), records, "{case}: records");
    let first = &sent[0];
    assert_eq!(
        first.body.as_ref().and_then(|b| b.value.clone()),
        Some(Value::StringValue(r#"{"msg":"one"}"#.to_string())),
        "{case}"
    );
    assert_eq!(
        (first.severity_number, first.time_unix_nano),
        (17, 1_700_000_000_000_000_000),
        "{case}"
    );
    assert_eq!(
        first.attributes.len(),
        1,
        "{case}: severity is not an attribute"
    );
    assert_eq!(
        sent[1].body.as_ref().and_then(|b| b.value.clone()),
        Some(Value::BytesValue(vec![0xff, 0xfe])),
        "{case}"
    );
}

/// A config change a case makes.
type Tweak = fn(&mut OtlpConfig);

fn as_is(_: &mut OtlpConfig) {}

fn gzip(config: &mut OtlpConfig) {
    config.compression = Compression::new(Codec::Gzip);
}

fn ten_second_cap(config: &mut OtlpConfig) {
    config.max_retry_delay_ms = 10_000;
}

fn short_timeout(config: &mut OtlpConfig) {
    config.timeout_ms = 200;
}

fn dead_letter_filter(config: &mut OtlpConfig) {
    config.filters_out = vec![FilterRule {
        expression: "has(dead)".to_string(),
        action: FilterAction::Dlq,
    }];
}

/// Mutual TLS, presenting the client certificate or not, trusting another CA,
/// and with a pin on the collector's key, served by that key or by a leaf
/// re-keyed under the same CA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tls {
    Off,
    WithIdentity,
    WithoutIdentity,
    Untrusted,
    Pinned,
    Rekeyed,
}

struct Case {
    name: &'static str,
    protocol: OtlpProtocol,
    answer: Answer,
    tls: Tls,
    tweak: Tweak,
    /// The `send_batch_by` deadline, from now.
    deadline_ms: Option<u64>,
    /// The result, as its `Debug` text starts.
    want: &'static str,
    /// Text the result's error carries.
    detail: &'static str,
    /// Records the collector gets, 0 when it records nothing.
    records: usize,
}

const fn case(
    name: &'static str,
    protocol: OtlpProtocol,
    answer: Answer,
    want: &'static str,
) -> Case {
    let records = match answer {
        Answer::Hang | Answer::Down => 0,
        _ => 3,
    };
    Case {
        name,
        protocol,
        answer,
        tls: Tls::Off,
        tweak: as_is,
        deadline_ms: None,
        want,
        detail: "",
        records,
    }
}

const GRPC: OtlpProtocol = OtlpProtocol::Grpc;
const HTTP: OtlpProtocol = OtlpProtocol::Http;
const SECS_7: Option<Duration> = Some(Duration::from_secs(7));
const A_DAY: Option<Duration> = Some(Duration::from_hours(24));

const CASES: &[Case] = &[
    case("grpc accepted", GRPC, Answer::Accept, "Ok"),
    Case {
        detail: "rejected 2 of 3 log records: two were bad",
        ..case(
            "grpc partial success",
            GRPC,
            Answer::Partial(2),
            "Reject(Send",
        )
    },
    case(
        "grpc partial success with none rejected",
        GRPC,
        Answer::Partial(0),
        "Ok",
    ),
    case(
        "grpc unavailable",
        GRPC,
        Answer::Grpc(Code::Unavailable, None),
        "Backpressured",
    ),
    case(
        "grpc unavailable with RetryInfo",
        GRPC,
        Answer::Grpc(Code::Unavailable, SECS_7),
        "RetryAfter(7s)",
    ),
    case(
        "grpc RetryInfo over the cap",
        GRPC,
        Answer::Grpc(Code::Unavailable, A_DAY),
        "RetryAfter(300s)",
    ),
    Case {
        tweak: ten_second_cap,
        ..case(
            "grpc RetryInfo over a configured cap",
            GRPC,
            Answer::Grpc(Code::Unavailable, A_DAY),
            "RetryAfter(10s)",
        )
    },
    case(
        "grpc resource exhausted with RetryInfo",
        GRPC,
        Answer::Grpc(Code::ResourceExhausted, SECS_7),
        "RetryAfter(7s)",
    ),
    case(
        "grpc resource exhausted alone",
        GRPC,
        Answer::Grpc(Code::ResourceExhausted, None),
        "Reject(Send",
    ),
    Case {
        detail: "InvalidArgument: collector says no",
        ..case(
            "grpc invalid argument",
            GRPC,
            Answer::Grpc(Code::InvalidArgument, None),
            "Reject(Send",
        )
    },
    case(
        "grpc internal",
        GRPC,
        Answer::Grpc(Code::Internal, None),
        "Reject(Send",
    ),
    case(
        "grpc unauthenticated",
        GRPC,
        Answer::Grpc(Code::Unauthenticated, None),
        "Fatal(Config",
    ),
    case(
        "grpc deadline exceeded at the collector",
        GRPC,
        Answer::Grpc(Code::DeadlineExceeded, None),
        "Backpressured",
    ),
    Case {
        deadline_ms: Some(200),
        ..case("grpc deadline expiry", GRPC, Answer::Hang, "Backpressured")
    },
    Case {
        tweak: short_timeout,
        ..case("grpc timeout", GRPC, Answer::Hang, "Backpressured")
    },
    case("grpc collector down", GRPC, Answer::Down, "Backpressured"),
    Case {
        tweak: gzip,
        ..case("grpc gzip", GRPC, Answer::Accept, "Ok")
    },
    Case {
        tweak: dead_letter_filter,
        records: 2,
        ..case("grpc dlq filter", GRPC, Answer::Accept, "FilteredDlq")
    },
    Case {
        tls: Tls::WithIdentity,
        ..case("grpc mutual TLS", GRPC, Answer::Accept, "Ok")
    },
    // TLS 1.3 refuses a missing client certificate after the client's side of
    // the handshake is done, and tonic reports the closed connection.
    Case {
        tls: Tls::WithoutIdentity,
        records: 0,
        ..case(
            "grpc mutual TLS without the certificate",
            GRPC,
            Answer::Accept,
            "Backpressured",
        )
    },
    Case {
        tls: Tls::Untrusted,
        records: 0,
        detail: "does not chain to a trusted CA",
        ..case(
            "grpc collector from a CA the client does not trust",
            GRPC,
            Answer::Accept,
            "Fatal(Config",
        )
    },
    case("http accepted", HTTP, Answer::Accept, "Ok"),
    Case {
        detail: "rejected 2 of 3 log records: two were bad",
        ..case(
            "http partial success",
            HTTP,
            Answer::Partial(2),
            "Reject(Send",
        )
    },
    case("http 429", HTTP, Answer::Http(429, None), "Backpressured"),
    case(
        "http 429 with Retry-After",
        HTTP,
        Answer::Http(429, Some("7")),
        "RetryAfter(7s)",
    ),
    case(
        "http 503 with Retry-After over the cap",
        HTTP,
        Answer::Http(503, Some("86400")),
        "RetryAfter(300s)",
    ),
    Case {
        tweak: ten_second_cap,
        ..case(
            "http Retry-After over a configured cap",
            HTTP,
            Answer::Http(503, Some("86400")),
            "RetryAfter(10s)",
        )
    },
    case(
        "http Retry-After as a date is not read",
        HTTP,
        Answer::Http(503, Some("Wed, 21 Oct 2026 07:28:00 GMT")),
        "Backpressured",
    ),
    case("http 502", HTTP, Answer::Http(502, None), "Backpressured"),
    case("http 504", HTTP, Answer::Http(504, None), "Backpressured"),
    Case {
        detail: "400 Bad Request: bad record",
        ..case("http 400", HTTP, Answer::Http(400, None), "Reject(Send")
    },
    case(
        "http 413",
        HTTP,
        Answer::Http(413, None),
        "Reject(MessageTooLarge",
    ),
    case("http 500", HTTP, Answer::Http(500, None), "Reject(Send"),
    case("http 401", HTTP, Answer::Http(401, None), "Fatal(Config"),
    case("http 302", HTTP, Answer::Http(302, None), "Fatal(Config"),
    Case {
        deadline_ms: Some(200),
        ..case("http deadline expiry", HTTP, Answer::Hang, "Backpressured")
    },
    case("http collector down", HTTP, Answer::Down, "Backpressured"),
    Case {
        tweak: gzip,
        ..case("http gzip", HTTP, Answer::Accept, "Ok")
    },
    Case {
        tweak: dead_letter_filter,
        records: 2,
        ..case("http dlq filter", HTTP, Answer::Accept, "FilteredDlq")
    },
    Case {
        tls: Tls::WithIdentity,
        records: 0,
        ..case("http mutual TLS", HTTP, Answer::Accept, "Ok")
    },
    Case {
        tls: Tls::WithoutIdentity,
        records: 0,
        detail: "client certificate",
        ..case(
            "http mutual TLS without the certificate",
            HTTP,
            Answer::Accept,
            "Fatal(Config",
        )
    },
    Case {
        tls: Tls::Untrusted,
        records: 0,
        detail: "does not chain to a trusted CA",
        ..case(
            "http collector from a CA the client does not trust",
            HTTP,
            Answer::Accept,
            "Fatal(Config",
        )
    },
    Case {
        tls: Tls::Pinned,
        records: 0,
        ..case(
            "http pin on the collector's key",
            HTTP,
            Answer::Accept,
            "Ok",
        )
    },
    Case {
        tls: Tls::Rekeyed,
        records: 0,
        detail: "matches no configured pin_sha256",
        ..case(
            "http collector re-keyed off the pin",
            HTTP,
            Answer::Accept,
            "Fatal(Config",
        )
    },
];

/// Files for a client that trusts `ca` and presents a certificate from it, and
/// another CA's certificate.
struct ClientFiles {
    _dir: tempfile::TempDir,
    ca: PathBuf,
    other_ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
}

fn client_files(ca: &TestCa) -> ClientFiles {
    let dir = tempfile::tempdir().unwrap();
    let leaf = ca.client("otlp client");
    let write = |name: &str, text: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, text).unwrap();
        path
    };
    ClientFiles {
        ca: write("ca.pem", &ca.pem()),
        other_ca: write("other-ca.pem", &TestCa::new("otlp other CA").pem()),
        cert: write("client.pem", &leaf.cert_pem),
        key: write("client.key", &leaf.key_pem),
        _dir: dir,
    }
}

/// The endpoint a case sends to, with the collector it starts. An HTTPS
/// collector presents `pinned`, or a fresh leaf for [`Tls::Rekeyed`].
async fn endpoint(case: &Case, seen: &Shared, ca: &TestCa, pinned: &Leaf) -> String {
    if matches!(case.answer, Answer::Down) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        return format!("http://{}", listener.local_addr().unwrap());
    }
    let shared = Arc::clone(seen);
    match (case.protocol, case.tls) {
        (OtlpProtocol::Grpc, Tls::Off) => {
            format!("http://{}", grpc_collector(case.answer, shared, None).await)
        }
        (OtlpProtocol::Grpc, _) => {
            let addr = grpc_collector(case.answer, shared, Some(ca)).await;
            format!("https://localhost:{}", addr.port())
        }
        (OtlpProtocol::Http, Tls::Off) => {
            format!("http://{}", http_collector(case.answer, shared).await)
        }
        (OtlpProtocol::Http, tls) => {
            let rekeyed;
            let leaf = if tls == Tls::Rekeyed {
                rekeyed = ca.server("otlp collector");
                &rekeyed
            } else {
                pinned
            };
            let addr = crate::tls::test_pki::mtls_ok_server(ca, leaf).await;
            format!("https://localhost:{}", addr.port())
        }
    }
}

/// Every answer a collector gives, per protocol, maps onto the result the
/// caller acts on, and the collector gets the block as one request.
#[tokio::test]
async fn each_collector_answer_maps_onto_a_send_result() {
    let ca = TestCa::new("otlp test CA");
    let files = client_files(&ca);
    let pinned = ca.server("otlp collector");
    for case in CASES {
        let seen = Shared::default();
        let mut config = OtlpConfig {
            endpoint: endpoint(case, &seen, &ca, &pinned).await,
            protocol: case.protocol,
            service_name: Some("stub-test".to_string()),
            headers: [("x-tenant".to_string(), "blue".into())]
                .into_iter()
                .collect(),
            ..OtlpConfig::default()
        };
        if case.tls != Tls::Off {
            let identity = case.tls != Tls::WithoutIdentity;
            let pin = matches!(case.tls, Tls::Pinned | Tls::Rekeyed);
            config.tls = Some(ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_paths: vec![if case.tls == Tls::Untrusted {
                    files.other_ca.clone()
                } else {
                    files.ca.clone()
                }],
                client_cert_path: identity.then(|| files.cert.clone()),
                client_key_path: identity.then(|| files.key.clone()),
                pin_sha256: pin.then(|| pinned.pin()).into_iter().collect(),
                ..ClientTlsConfig::default()
            });
        }
        (case.tweak)(&mut config);
        let gzip = config.compression.codec == Codec::Gzip;
        let transport_config = TransportConfig {
            transport_type: TransportType::Otlp,
            otlp: Some(config),
            ..TransportConfig::default()
        };
        // check_seen asserts the collector got the real value.
        assert!(
            !format!("{transport_config:?}").contains("blue"),
            "{}: Debug redacts header values",
            case.name
        );
        let sender = AnySender::from_transport_config(&transport_config)
            .await
            .unwrap_or_else(|e| panic!("{}: build: {e}", case.name));

        let started = Instant::now();
        let result = match case.deadline_ms {
            Some(ms) => {
                sender
                    .send_batch_by(
                        &block(),
                        tokio::time::Instant::now() + Duration::from_millis(ms),
                    )
                    .await
            }
            None => sender.send_batch(&block()).await,
        };
        let got = format!("{result:?}");
        assert!(got.starts_with(case.want), "{}: {got}", case.name);
        assert!(got.contains(case.detail), "{}: {got}", case.name);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{}: answered in {:?}",
            case.name,
            started.elapsed()
        );
        if case.records > 0 {
            let seen = seen.lock().unwrap();
            check_seen(case.name, &seen, case.records, gzip);
        }
    }
}

/// Settings no transport can be built from fail the build, naming what is
/// wrong, and a built transport refuses sends once closed.
#[tokio::test]
async fn a_bad_setting_fails_the_build_and_a_closed_transport_refuses() {
    let missing = PathBuf::from("/nonexistent/otlp-ca.pem");
    let pinned = |protocol, trust| OtlpConfig {
        protocol,
        tls: Some(ClientTlsConfig {
            trust,
            ca_paths: vec![missing.clone()],
            ..ClientTlsConfig::default()
        }),
        ..OtlpConfig::grpc("https://collector:4317")
    };
    let refused = [
        (
            "a missing CA file on gRPC",
            pinned(GRPC, TrustChoice::Platform),
            "otlp-ca.pem",
        ),
        (
            "a missing CA file",
            pinned(HTTP, TrustChoice::Bundle),
            "otlp-ca.pem",
        ),
        (
            "a header name gRPC cannot send",
            OtlpConfig {
                headers: [("bad header".to_string(), "x".into())]
                    .into_iter()
                    .collect(),
                ..OtlpConfig::default()
            },
            "\"bad header\" is not a gRPC metadata name",
        ),
        (
            "a header value HTTP cannot send",
            OtlpConfig {
                headers: [("x-key".to_string(), "line\nbreak".into())]
                    .into_iter()
                    .collect(),
                ..OtlpConfig::http("http://collector:4318")
            },
            "\"x-key\" has a value HTTP cannot send",
        ),
        (
            "a filter that does not parse",
            OtlpConfig {
                filters_out: vec![FilterRule {
                    expression: "has(".to_string(),
                    action: FilterAction::Drop,
                }],
                ..OtlpConfig::default()
            },
            "",
        ),
        (
            "connect_to that is not an address",
            OtlpConfig {
                connect_to: Some("collector-2:4317".to_string()),
                ..OtlpConfig::default()
            },
            "is not an IP address or IP:port",
        ),
        (
            "connect_to beside a proxy",
            OtlpConfig {
                connect_to: Some("10.0.0.5".to_string()),
                proxy: crate::transport::ProxyConfig::System,
                ..OtlpConfig::default()
            },
            "connect_to cannot be set with a proxy",
        ),
        (
            "connect_to with an endpoint naming no host",
            OtlpConfig {
                connect_to: Some("10.0.0.5".to_string()),
                ..OtlpConfig::http("http://192.168.1.9:4318")
            },
            "needs an endpoint that names its host",
        ),
    ];
    for (name, config, text) in refused {
        let error = super::OtlpTransport::new(&config)
            .err()
            .map(|e| e.to_string());
        assert!(
            error.as_deref().is_some_and(|e| e.contains(text)),
            "{name}: {error:?}"
        );
        assert!(
            !error.unwrap_or_default().contains("line\nbreak"),
            "{name}: a header value is never shown"
        );
    }

    let seen = Shared::default();
    let addr = grpc_collector(Answer::Accept, Arc::clone(&seen), None).await;
    let transport = super::OtlpTransport::new(&OtlpConfig {
        service_name: Some("stub-test".to_string()),
        ..OtlpConfig::grpc(&format!("http://{addr}"))
    })
    .unwrap();
    assert_eq!(transport.name(), "otlp");
    assert_eq!(
        transport.delivery_guarantee(),
        crate::transport::DeliveryGuarantee::Acknowledged
    );
    let one = transport
        .send("not sent", bytes::Bytes::from_static(b"plain text"))
        .await;
    assert!(one.is_ok(), "{one:?}");
    assert_eq!(
        seen.lock().unwrap().requests[0].resource_logs[0].scope_logs[0]
            .log_records
            .len(),
        1
    );

    transport.close().await.unwrap();
    assert!(!transport.is_healthy());
    let closed = transport.send_batch(&block()).await;
    assert!(
        matches!(
            closed,
            crate::transport::SendResult::Fatal(crate::transport::TransportError::Closed)
        ),
        "{closed:?}"
    );
}

/// Metrics an SDK collected, as its reader hands them to an exporter: a
/// counter at 3 and a gauge at 7, under a resource naming `stub-test`.
fn collected_metrics() -> opentelemetry_sdk::metrics::data::ResourceMetrics {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("stub-test")
                .build(),
        )
        .build();
    let meter = provider.meter("otlp-test");
    meter.u64_counter("requests_total").build().add(3, &[]);
    meter.f64_gauge("queue_depth").build().record(7.0, &[]);
    provider.force_flush().unwrap();
    exporter.get_finished_metrics().unwrap().remove(0)
}

/// Check the metric export carries the meter provider's resource, not the
/// transport's, and the counter and gauge values.
fn check_metrics(case: &str, request: &ExportMetricsServiceRequest) {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as Number;

    let resource = &request.resource_metrics[0];
    let service = resource
        .resource
        .as_ref()
        .and_then(|r| r.attributes.iter().find(|kv| kv.key == "service.name"))
        .and_then(|kv| kv.value.clone())
        .and_then(|v| v.value);
    assert_eq!(
        service,
        Some(Value::StringValue("stub-test".to_string())),
        "{case}: resource"
    );
    let mut values: Vec<(String, Option<Number>)> = resource
        .scope_metrics
        .iter()
        .flat_map(|scope| &scope.metrics)
        .map(|metric| {
            let point = match &metric.data {
                Some(Data::Sum(sum)) => sum.data_points.first(),
                Some(Data::Gauge(gauge)) => gauge.data_points.first(),
                other => panic!("{case}: {other:?}"),
            };
            (metric.name.clone(), point.and_then(|p| p.value))
        })
        .collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        values,
        vec![
            ("queue_depth".to_string(), Some(Number::AsDouble(7.0))),
            ("requests_total".to_string(), Some(Number::AsInt(3))),
        ],
        "{case}: metrics"
    );
}

/// (case, protocol, answer, gzip, text of the export's error, `None` for `Ok`)
type MetricCase = (
    &'static str,
    OtlpProtocol,
    Answer,
    bool,
    Option<&'static str>,
);

const METRIC_CASES: &[MetricCase] = &[
    ("grpc accepted", GRPC, Answer::Accept, false, None),
    ("grpc gzip", GRPC, Answer::Accept, true, None),
    (
        "grpc partial success",
        GRPC,
        Answer::Partial(2),
        false,
        Some("rejected 2 data points: two were bad"),
    ),
    (
        "grpc unavailable",
        GRPC,
        Answer::Grpc(Code::Unavailable, None),
        false,
        Some("unavailable or did not answer in time"),
    ),
    (
        "grpc unavailable with RetryInfo",
        GRPC,
        Answer::Grpc(Code::Unavailable, SECS_7),
        false,
        Some("asked for a wait of 7s"),
    ),
    (
        "grpc unauthenticated",
        GRPC,
        Answer::Grpc(Code::Unauthenticated, None),
        false,
        Some("Unauthenticated: collector says no"),
    ),
    (
        "grpc collector down",
        GRPC,
        Answer::Down,
        false,
        Some("unavailable or did not answer in time"),
    ),
    ("http accepted", HTTP, Answer::Accept, false, None),
    ("http gzip", HTTP, Answer::Accept, true, None),
    (
        "http partial success",
        HTTP,
        Answer::Partial(2),
        false,
        Some("rejected 2 data points: two were bad"),
    ),
    (
        "http 503 with Retry-After",
        HTTP,
        Answer::Http(503, Some("7")),
        false,
        Some("asked for a wait of 7s"),
    ),
    (
        "http 400",
        HTTP,
        Answer::Http(400, None),
        false,
        Some("400 Bad Request: bad record"),
    ),
    (
        "http collector down",
        HTTP,
        Answer::Down,
        false,
        Some("unavailable or did not answer in time"),
    ),
];

/// Metrics go over the transport's own client to the collector that takes its
/// logs, on each protocol, and each answer maps onto the export's result. A
/// closed transport and a shut-down exporter refuse.
#[tokio::test]
async fn metrics_go_through_the_transport_to_its_collector() {
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter as _;

    let collected = collected_metrics();
    let ca = TestCa::new("otlp metrics test CA");
    let pinned = ca.server("otlp collector");
    for &(name, protocol, answer, gzip, error) in METRIC_CASES {
        let seen = Shared::default();
        let mut config = OtlpConfig {
            endpoint: endpoint(&case(name, protocol, answer, ""), &seen, &ca, &pinned).await,
            protocol,
            service_name: Some("the-transport-resource".to_string()),
            headers: [("x-tenant".to_string(), "blue".into())]
                .into_iter()
                .collect(),
            ..OtlpConfig::default()
        };
        if gzip {
            config.compression = Compression::new(Codec::Gzip);
        }
        let transport = Arc::new(super::OtlpTransport::new(&config).unwrap());
        let logs_sent = usize::from(error.is_none());
        if error.is_none() {
            let logs = transport.send_batch(&block()).await;
            assert!(logs.is_ok(), "{name}: logs {logs:?}");
        }
        let exporter = super::OtlpMetricExporter::new(Arc::clone(&transport)).unwrap();
        let result = exporter.export(&collected).await;
        match error {
            None => assert!(result.is_ok(), "{name}: {result:?}"),
            Some(text) => assert!(
                result.as_ref().is_err_and(|e| e.to_string().contains(text)),
                "{name}: {result:?}"
            ),
        }
        if matches!(answer, Answer::Down) {
            continue;
        }
        let seen = seen.lock().unwrap();
        assert_eq!(seen.metrics.len(), 1, "{name}: one metric export");
        assert_eq!(
            seen.requests.len(),
            logs_sent,
            "{name}: logs, same collector"
        );
        assert!(
            seen.tenant.iter().all(|t| t.as_deref() == Some("blue")),
            "{name}: header {:?}",
            seen.tenant
        );
        assert!(seen.gzip.iter().all(|&g| g == gzip), "{name}: compression");
        check_metrics(name, &seen.metrics[0]);
    }

    let seen = Shared::default();
    let addr = grpc_collector(Answer::Accept, Arc::clone(&seen), None).await;
    let transport =
        Arc::new(super::OtlpTransport::new(&OtlpConfig::grpc(&format!("http://{addr}"))).unwrap());
    let exporter = super::OtlpMetricExporter::new(Arc::clone(&transport)).unwrap();
    transport.close().await.unwrap();
    let closed = exporter.export(&collected).await;
    assert!(
        closed
            .as_ref()
            .is_err_and(|e| e.to_string().contains("transport closed")),
        "{closed:?}"
    );
    exporter.shutdown().unwrap();
    assert!(matches!(
        exporter.export(&collected).await,
        Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown)
    ));
    assert_eq!(seen.lock().unwrap().metrics, Vec::new());
}

/// `connect_to` dials the collector while the endpoint names a host that
/// does not resolve and a port nothing listens on: both protocols, in plain
/// text and with mutual TLS checked against the endpoint's host name. HTTP
/// sends the endpoint's host and port as `Host`.
#[tokio::test]
async fn connect_to_dials_the_address_and_keeps_the_endpoint_name() {
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter as _;

    let ca = TestCa::new("otlp connect_to test CA");
    let files = client_files(&ca);
    let pinned = ca.server("otlp collector");
    let collected = collected_metrics();
    for (protocol, tls) in [
        (GRPC, Tls::Off),
        (GRPC, Tls::WithIdentity),
        (HTTP, Tls::Off),
        (HTTP, Tls::WithIdentity),
    ] {
        let name = format!("{protocol:?} {tls:?}");
        let seen = Shared::default();
        let probe = Case {
            tls,
            ..case("connect_to", protocol, Answer::Accept, "Ok")
        };
        let collector = reqwest::Url::parse(&endpoint(&probe, &seen, &ca, &pinned).await).unwrap();
        let mut config = OtlpConfig {
            // target.test resolves nowhere, and the test certificate names it.
            endpoint: format!("{}://target.test:1", collector.scheme()),
            protocol,
            connect_to: Some(format!("127.0.0.1:{}", collector.port().unwrap())),
            ..OtlpConfig::default()
        };
        if tls != Tls::Off {
            config.tls = Some(ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_paths: vec![files.ca.clone()],
                client_cert_path: Some(files.cert.clone()),
                client_key_path: Some(files.key.clone()),
                ..ClientTlsConfig::default()
            });
        }
        let transport = Arc::new(super::OtlpTransport::new(&config).unwrap());
        let logs = transport.send_batch(&block()).await;
        assert!(logs.is_ok(), "{name}: logs {logs:?}");
        let exporter = super::OtlpMetricExporter::new(Arc::clone(&transport)).unwrap();
        let metrics = exporter.export(&collected).await;
        assert!(metrics.is_ok(), "{name}: metrics {metrics:?}");
        // The mutual TLS collector for HTTP notes nothing.
        if (protocol, tls) == (HTTP, Tls::WithIdentity) {
            continue;
        }
        let seen = seen.lock().unwrap();
        assert_eq!(
            (seen.requests.len(), seen.metrics.len()),
            (1, 1),
            "{name}: one logs and one metrics export"
        );
        if protocol == HTTP {
            assert_eq!(
                seen.host,
                vec![Some("target.test:1".to_string()); 2],
                "{name}: Host"
            );
        }
    }
}

/// Both protocols export logs and metrics through a real Squid's CONNECT
/// tunnel, gRPC in plain text and HTTP with TLS inside it. The target name
/// resolves only inside Squid, so an export that arrives went through it.
#[cfg(all(feature = "transport-proxy-test-support", unix))]
#[tokio::test]
async fn exports_go_through_a_connect_proxy() {
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter as _;

    use crate::sensitive::SensitiveString;
    use crate::transport::proxy::squid::{Squid, TARGET_HOST};
    use crate::transport::proxy::test_front::{plain_front, tls_front};
    use crate::transport::{ProxyConfig, SendResult};

    let Some(squid) = Squid::shared().await else {
        return;
    };
    let grpc_seen = Shared::default();
    let grpc_addr = grpc_collector(Answer::Accept, Arc::clone(&grpc_seen), None).await;
    let grpc_front = plain_front(grpc_addr).await;
    let http_seen = Shared::default();
    let http_addr = http_collector(Answer::Accept, Arc::clone(&http_seen)).await;
    let (front, ca) = tls_front(http_addr, &[b"http/1.1"]).await;
    let ca_file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(ca_file.path(), ca).await.unwrap();

    for protocol in [GRPC, HTTP] {
        let (endpoint, tls) = if protocol == GRPC {
            (format!("http://{TARGET_HOST}:{}", grpc_front.port()), None)
        } else {
            let tls = ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_paths: vec![ca_file.path().to_path_buf()],
                ..ClientTlsConfig::default()
            };
            (format!("https://{TARGET_HOST}:{}", front.port()), Some(tls))
        };
        let transport = super::OtlpTransport::new(&OtlpConfig {
            endpoint,
            protocol,
            timeout_ms: 10_000,
            tls,
            proxy: ProxyConfig::Url(SensitiveString::new(squid.open())),
            ..OtlpConfig::default()
        })
        .unwrap();
        let result = transport.send_batch(&block()).await;
        assert!(
            matches!(result, SendResult::Ok),
            "{protocol:?}: got {result:?}"
        );
        let exporter = super::OtlpMetricExporter::new(Arc::new(transport)).unwrap();
        let metrics = exporter.export(&collected_metrics()).await;
        assert!(metrics.is_ok(), "{protocol:?}: metrics {metrics:?}");
    }
    for (protocol, seen) in [("gRPC", grpc_seen), ("HTTP", http_seen)] {
        let seen = seen.lock().unwrap();
        assert_eq!(
            (seen.requests.len(), seen.metrics.len()),
            (1, 1),
            "one {protocol} logs export and one metrics export"
        );
    }
}

/// The `tls` section reaches the handshake under both protocols: a cnsa2
/// client against a cnsa2 collector agrees ML-KEM-1024 and AES-256, where the
/// default strong client would agree P-384.
#[tokio::test]
async fn the_tls_section_reaches_the_handshake() {
    use crate::tls::test_pki::{Agreed, Front, server_tls};

    let ca = TestCa::new("otlp handshake ca");
    let ca_file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(ca_file.path(), ca.pem()).await.unwrap();
    for (protocol, alpn) in [(GRPC, b"h2".as_slice()), (HTTP, b"http/1.1".as_slice())] {
        let seen = Shared::default();
        let collector = if protocol == GRPC {
            grpc_collector(Answer::Accept, Arc::clone(&seen), None).await
        } else {
            http_collector(Answer::Accept, Arc::clone(&seen)).await
        };
        let (tls, _files) = server_tls(&ca, crate::crypto::CryptoProfile::Cnsa2, &[alpn]).await;
        let front = Front::start(tls, collector).await;
        let transport = super::OtlpTransport::new(&OtlpConfig {
            endpoint: format!("https://localhost:{}", front.addr.port()),
            protocol,
            tls: Some(ClientTlsConfig {
                profile: crate::crypto::CryptoProfile::Cnsa2,
                trust: TrustChoice::Bundle,
                ca_paths: vec![ca_file.path().to_path_buf()],
                ..ClientTlsConfig::default()
            }),
            ..OtlpConfig::default()
        })
        .unwrap();
        let result = transport.send_batch(&block()).await;
        assert!(result.is_ok(), "{protocol:?}: {result:?}");
        assert_eq!(
            front.agreed(),
            [Agreed::tls13(
                rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
                rustls::NamedGroup::MLKEM1024
            )],
            "{protocol:?}"
        );
    }
}
