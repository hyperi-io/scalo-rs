// Project:   scalo
// File:      src/transport/otlp/tests.rs
// Purpose:   The OTLP transport against a local collector stub per protocol
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
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use prost::Message as _;
use tonic::Code;
use tonic_types::{ErrorDetails, StatusExt as _};

use super::{OtlpCompression, OtlpConfig, OtlpProtocol};
use crate::tls::test_pki::TestCa;
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
    tenant: Vec<Option<String>>,
    gzip: Vec<bool>,
}

type Shared = Arc<Mutex<Seen>>;

fn note(seen: &Shared, request: ExportLogsServiceRequest, tenant: Option<&str>, gzip: bool) {
    let mut seen = seen.lock().unwrap();
    seen.requests.push(request);
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

struct GrpcCollector {
    answer: Answer,
    seen: Shared,
}

#[tonic::async_trait]
impl LogsService for GrpcCollector {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        let metadata = request.metadata();
        let tenant = metadata.get("x-tenant").and_then(|v| v.to_str().ok());
        let gzip = metadata
            .get("grpc-encoding")
            .is_some_and(|v| v.to_str().ok() == Some("gzip"));
        note(&self.seen, request.get_ref().clone(), tenant, gzip);
        match self.answer {
            Answer::Accept => Ok(tonic::Response::new(ExportLogsServiceResponse::default())),
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

/// A gRPC collector on localhost, with TLS asking for a client certificate
/// from `ca` when there is one.
async fn grpc_collector(answer: Answer, seen: Shared, ca: Option<&TestCa>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = LogsServiceServer::new(GrpcCollector { answer, seen })
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    let mut server = tonic::transport::Server::builder();
    if let Some(ca) = ca {
        crate::tls::install_default_provider();
        let leaf = ca.server("otlp collector");
        let tls = tonic::transport::ServerTlsConfig::new()
            .identity(tonic::transport::Identity::from_pem(
                &leaf.cert_pem,
                &leaf.key_pem,
            ))
            .client_ca_root(tonic::transport::Certificate::from_pem(ca.pem()));
        server = server.tls_config(tls).unwrap();
    }
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    tokio::spawn(server.add_service(service).serve_with_incoming(incoming));
    addr
}

/// An HTTP collector on localhost taking `POST /v1/logs`.
async fn http_collector(answer: Answer, seen: Shared) -> SocketAddr {
    use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
    use axum::response::{IntoResponse, Response};

    let handler = move |headers: HeaderMap, body: axum::body::Bytes| {
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
            let request = ExportLogsServiceRequest::decode(&body[..]).unwrap();
            note(&seen, request, tenant, gzip);
            let protobuf = [(header::CONTENT_TYPE, "application/x-protobuf")];
            match answer {
                Answer::Accept => StatusCode::OK.into_response(),
                Answer::Partial(rejected) => {
                    (StatusCode::OK, protobuf, partial(rejected).encode_to_vec()).into_response()
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
    let app = axum::Router::new().route("/v1/logs", axum::routing::post(handler));
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
    config.compression = OtlpCompression::Gzip;
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

/// Mutual TLS, presenting the client certificate or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tls {
    Off,
    WithIdentity,
    WithoutIdentity,
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
const A_DAY: Option<Duration> = Some(Duration::from_secs(86_400));

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
        ..case(
            "http mutual TLS without the certificate",
            HTTP,
            Answer::Accept,
            "Backpressured",
        )
    },
];

/// Files for a client that trusts `ca` and presents a certificate from it.
struct ClientFiles {
    _dir: tempfile::TempDir,
    ca: PathBuf,
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
        cert: write("client.pem", &leaf.cert_pem),
        key: write("client.key", &leaf.key_pem),
        _dir: dir,
    }
}

/// The endpoint a case sends to, with the collector it starts.
async fn endpoint(case: &Case, seen: &Shared, ca: &TestCa) -> String {
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
        (OtlpProtocol::Http, _) => {
            let addr = crate::tls::test_pki::mtls_ok_server(ca, &ca.server("otlp collector")).await;
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
    for case in CASES {
        let seen = Shared::default();
        let mut config = OtlpConfig {
            endpoint: endpoint(case, &seen, &ca).await,
            protocol: case.protocol,
            service_name: Some("stub-test".to_string()),
            headers: [("x-tenant".to_string(), "blue".to_string())]
                .into_iter()
                .collect(),
            ..OtlpConfig::default()
        };
        if case.tls != Tls::Off {
            config.tls = Some(ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_path: Some(files.ca.clone()),
                client_cert_path: (case.tls == Tls::WithIdentity).then(|| files.cert.clone()),
                client_key_path: (case.tls == Tls::WithIdentity).then(|| files.key.clone()),
            });
        }
        (case.tweak)(&mut config);
        let gzip = config.compression == OtlpCompression::Gzip;
        let sender = AnySender::from_transport_config(&TransportConfig {
            transport_type: TransportType::Otlp,
            otlp: Some(config),
            ..TransportConfig::default()
        })
        .await
        .unwrap_or_else(|e| panic!("{}: build: {e}", case.name));

        let started = Instant::now();
        let result = match case.deadline_ms {
            Some(ms) => {
                sender
                    .send_batch_by(&block(), Instant::now() + Duration::from_millis(ms))
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
            ca_path: Some(missing.clone()),
            ..ClientTlsConfig::default()
        }),
        ..OtlpConfig::grpc("https://collector:4317")
    };
    let refused = [
        (
            "platform trust on gRPC",
            pinned(GRPC, TrustChoice::Platform),
            "not `platform`",
        ),
        (
            "a missing CA file",
            pinned(HTTP, TrustChoice::Bundle),
            "otlp-ca.pem",
        ),
        (
            "a header name gRPC cannot send",
            OtlpConfig {
                headers: [("bad header".to_string(), "x".to_string())]
                    .into_iter()
                    .collect(),
                ..OtlpConfig::default()
            },
            "\"bad header\" is not a gRPC metadata name",
        ),
        (
            "a header value HTTP cannot send",
            OtlpConfig {
                headers: [("x-key".to_string(), "line\nbreak".to_string())]
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
