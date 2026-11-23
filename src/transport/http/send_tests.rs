// Project:   scalo
// File:      src/transport/http/send_tests.rs
// Purpose:   HTTP send-side tests against local servers that record each request
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{HttpAuth, HttpEndpoint, HttpProxy, HttpTransport, HttpTransportConfig};
use crate::framing::FramingConfig;
use crate::sensitive::SensitiveString;
use crate::transport::error::TransportError;
use crate::transport::traits::TransportSender;
use crate::transport::types::{PayloadFormat, SendResult};
use crate::transport::{Record, RecordMeta};

/// One request as a [`Server`] read it.
#[derive(Debug, Clone)]
pub(super) struct Seen {
    /// The request line, such as `POST /ingest HTTP/1.1`.
    pub(super) line: String,
    /// Header names in lowercase, in order.
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

impl Seen {
    pub(super) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    pub(super) fn path(&self) -> &str {
        self.line.split(' ').nth(1).unwrap_or_default()
    }
}

type Reply = Arc<dyn Fn(&Seen) -> String + Send + Sync>;

/// A loopback server that records every request and answers with `reply`,
/// one request per connection.
pub(super) struct Server {
    pub(super) addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Server {
    pub(super) async fn start(reply: impl Fn(&Seen) -> String + Send + Sync + 'static) -> Self {
        let reply: Reply = Arc::new(reply);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let reply = Arc::clone(&reply);
                let log = Arc::clone(&log);
                tokio::spawn(async move {
                    let Some(request) = read_request(&mut stream).await else {
                        return;
                    };
                    let answer = reply(&request);
                    log.lock().unwrap().push(request);
                    let _ = stream.write_all(answer.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { addr, seen }
    }

    /// Answers every request with `status` and no body.
    pub(super) async fn status(status: u16) -> Self {
        Self::start(move |_| reply(status, &[])).await
    }

    /// Accepts connections and never answers.
    async fn hang() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        Self {
            addr,
            seen: Arc::default(),
        }
    }

    pub(super) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub(super) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

/// A response with `status`, `headers` and no body.
pub(super) fn reply(status: u16, headers: &[(&str, &str)]) -> String {
    let mut text =
        format!("HTTP/1.1 {status} Canned\r\ncontent-length: 0\r\nconnection: close\r\n");
    for (name, value) in headers {
        let _ = write!(text, "{name}: {value}\r\n");
    }
    text.push_str("\r\n");
    text
}

/// Read one request head and the body its `Content-Length` declares.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<Seen> {
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 8192];
    let head_end = loop {
        if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break at;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let line = lines.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(n, _)| n == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    while buf.len() < head_end + 4 + length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Some(Seen {
        line,
        headers,
        body: buf[head_end + 4..head_end + 4 + length].to_vec(),
    })
}

fn record(key: Option<&str>, payload: &str) -> Record {
    Record {
        payload: bytes::Bytes::copy_from_slice(payload.as_bytes()),
        key: key.map(Arc::from),
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

fn secret(text: &str) -> SensitiveString {
    SensitiveString::new(text)
}

/// A free loopback port nothing listens on.
fn refused_url() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .unwrap()
        .port();
    format!("http://127.0.0.1:{port}")
}

/// What a send came to, coarse enough to tabulate.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Got {
    Ok,
    Backpressured,
    RetryAfter(Duration),
    Hold,
    Reject,
    TooLarge,
    Other(String),
}

fn got(result: &SendResult) -> Got {
    match result {
        SendResult::Ok => Got::Ok,
        SendResult::Backpressured => Got::Backpressured,
        SendResult::RetryAfter(wait) => Got::RetryAfter(*wait),
        SendResult::Fatal(TransportError::Config(_)) => Got::Hold,
        SendResult::Reject(TransportError::MessageTooLarge { .. }) => Got::TooLarge,
        SendResult::Reject(_) => Got::Reject,
        other => Got::Other(format!("{other:?}")),
    }
}

async fn sender(config: &HttpTransportConfig) -> HttpTransport {
    HttpTransport::new(config).await.unwrap()
}

async fn build_error(config: &HttpTransportConfig) -> String {
    match HttpTransport::new(config).await {
        Ok(_) => "built".to_string(),
        Err(e) => e.to_string(),
    }
}

/// Each credential kind, custom headers and the User-Agent arrive as set.
#[tokio::test]
async fn each_auth_kind_and_header_reaches_the_endpoint() {
    // (name, auth, userinfo in the URL, extra config, header wanted)
    type Row = (
        &'static str,
        HttpAuth,
        Option<&'static str>,
        fn(&mut HttpTransportConfig),
        Vec<(&'static str, Option<&'static str>)>,
    );
    let basic = HttpAuth::Basic {
        username: "u".into(),
        password: secret("pw-basic"),
    };
    let rows: [Row; 6] = [
        (
            "none",
            HttpAuth::None,
            None,
            |_| {},
            vec![("authorization", None), ("user-agent", None)],
        ),
        (
            "basic",
            basic.clone(),
            None,
            |_| {},
            vec![("authorization", Some("Basic dTpwdy1iYXNpYw=="))],
        ),
        (
            "bearer",
            HttpAuth::Bearer {
                token: secret("tok-1"),
            },
            None,
            |_| {},
            vec![("authorization", Some("Bearer tok-1"))],
        ),
        (
            "api key",
            HttpAuth::ApiKey {
                header: "X-Api-Key".into(),
                key: secret("key-1"),
            },
            None,
            |_| {},
            vec![("x-api-key", Some("key-1")), ("authorization", None)],
        ),
        (
            "userinfo in the URL is basic auth",
            HttpAuth::None,
            Some("u:pw-basic"),
            |_| {},
            vec![("authorization", Some("Basic dTpwdy1iYXNpYw=="))],
        ),
        (
            "custom headers and user agent",
            HttpAuth::None,
            None,
            |c| {
                c.headers.insert("X-Tenant".into(), secret("t1"));
                c.user_agent = Some("app/1.0".into());
            },
            vec![("x-tenant", Some("t1")), ("user-agent", Some("app/1.0"))],
        ),
    ];
    for (name, auth, userinfo, extra, want) in rows {
        let server = Server::status(200).await;
        let url = match userinfo {
            Some(info) => format!("http://{info}@{}", server.addr),
            None => server.url(),
        };
        let mut config = HttpTransportConfig {
            auth,
            ..HttpTransportConfig::sender(&url)
        };
        extra(&mut config);
        let result = sender(&config).await.send("ingest", "{}".into()).await;
        assert_eq!(got(&result), Got::Ok, "{name}");
        let seen = server.seen();
        assert_eq!(seen.len(), 1, "{name}");
        for (header, value) in want {
            assert_eq!(seen[0].header(header), value, "{name}: {header}");
        }
    }

    // A failover endpoint with its own credential sends that one.
    let busy = Server::status(503).await;
    let spare = Server::status(200).await;
    let config = HttpTransportConfig {
        auth: basic,
        failover: vec![HttpEndpoint {
            url: spare.url(),
            auth: Some(HttpAuth::Bearer {
                token: secret("tok-2"),
            }),
        }],
        ..HttpTransportConfig::sender(&busy.url())
    };
    let result = sender(&config).await.send("", "{}".into()).await;
    assert_eq!(got(&result), Got::Ok);
    assert_eq!(
        busy.seen()[0].header("authorization"),
        Some("Basic dTpwdy1iYXNpYw==")
    );
    assert_eq!(
        spare.seen()[0].header("authorization"),
        Some("Bearer tok-2")
    );
}

/// A credential or header that can't be sent fails the build, naming no secret.
#[tokio::test]
async fn a_credential_that_cannot_be_sent_fails_the_build() {
    let bad: [(&str, HttpTransportConfig, &str); 5] = [
        (
            "empty bearer",
            HttpTransportConfig {
                auth: HttpAuth::Bearer { token: secret("") },
                ..HttpTransportConfig::sender("http://127.0.0.1:9")
            },
            "token is empty",
        ),
        (
            "userinfo and auth",
            HttpTransportConfig {
                auth: HttpAuth::Bearer {
                    token: secret("tok-3"),
                },
                ..HttpTransportConfig::sender("http://u:pw-url@127.0.0.1:9")
            },
            "use one",
        ),
        (
            "a colon in the basic user",
            HttpTransportConfig {
                auth: HttpAuth::Basic {
                    username: "a:b".into(),
                    password: secret("pw-colon"),
                },
                ..HttpTransportConfig::sender("http://127.0.0.1:9")
            },
            "colon",
        ),
        (
            "a header value with a newline",
            HttpTransportConfig {
                headers: BTreeMap::from([("X-Key".to_string(), secret("key\nbad"))]),
                ..HttpTransportConfig::sender("http://127.0.0.1:9")
            },
            "X-Key",
        ),
        (
            "a bad header name",
            HttpTransportConfig {
                auth: HttpAuth::ApiKey {
                    header: "bad header".into(),
                    key: secret("key-2"),
                },
                ..HttpTransportConfig::sender("http://127.0.0.1:9")
            },
            "not valid",
        ),
    ];
    for (name, config, want) in bad {
        let error = build_error(&config).await;
        assert!(error.contains(want), "{name}: {error}");
        for leaked in ["tok-3", "pw-url", "pw-colon", "key\nbad", "key-2"] {
            assert!(!error.contains(leaked), "{name} leaked {leaked}: {error}");
        }
    }
}

/// A registered framing for the body test.
#[derive(Debug)]
struct Piped;

impl crate::framing::Framing for Piped {
    fn content_type(&self) -> &'static str {
        "text/plain"
    }
    fn appends(&self) -> bool {
        true
    }
    fn encode(&self, records: &[&[u8]], out: &mut Vec<u8>) -> usize {
        for record in records {
            out.extend_from_slice(record);
            out.push(b'|');
        }
        records.len()
    }
}

/// Each framing shapes the body and its `Content-Type`, one body per run of
/// records with the same key, and gzip compresses it.
#[tokio::test]
async fn each_framing_shapes_the_body_and_gzip_compresses_it() {
    use std::io::Read as _;

    // (framing, key, gzip, records, (path, content type, body) per request)
    type Row = (
        &'static str,
        &'static str,
        bool,
        Vec<Record>,
        Vec<(&'static str, &'static str, &'static str)>,
    );
    let _ = crate::framing::register_framing("piped_http_test", Arc::new(Piped));
    let two = || vec![record(None, r#"{"a":1}"#), record(None, r#"{"b":2}"#)];
    let rows: Vec<Row> = vec![
        (
            "ndjson",
            "",
            false,
            two(),
            vec![("/", "application/x-ndjson", "{\"a\":1}\n{\"b\":2}\n")],
        ),
        (
            "json_array",
            "",
            false,
            two(),
            vec![("/", "application/json", r#"[{"a":1},{"b":2}]"#)],
        ),
        (
            "json_wrapped",
            "events",
            false,
            two(),
            vec![("/", "application/json", r#"{"events":[{"a":1},{"b":2}]}"#)],
        ),
        (
            "json_concat",
            "",
            false,
            two(),
            vec![("/", "application/json", r#"{"a":1}{"b":2}"#)],
        ),
        (
            "single",
            "",
            false,
            two(),
            vec![
                ("/", "application/octet-stream", r#"{"a":1}"#),
                ("/", "application/octet-stream", r#"{"b":2}"#),
            ],
        ),
        (
            "piped_http_test",
            "",
            false,
            two(),
            vec![("/", "text/plain", r#"{"a":1}|{"b":2}|"#)],
        ),
        (
            "ndjson",
            "",
            true,
            two(),
            vec![("/", "application/x-ndjson", "{\"a\":1}\n{\"b\":2}\n")],
        ),
        (
            "json_array",
            "",
            false,
            vec![
                record(Some("x"), "1"),
                record(Some("x"), "2"),
                record(Some("y"), "3"),
                record(Some("x"), "4"),
            ],
            vec![
                ("/x", "application/json", "[1,2]"),
                ("/y", "application/json", "[3]"),
                ("/x", "application/json", "[4]"),
            ],
        ),
    ];
    for (framing, key, gzip, records, want) in rows {
        let name = format!("{framing} gzip={gzip}");
        let server = Server::status(200).await;
        let config = HttpTransportConfig {
            framing: FramingConfig {
                framing_type: framing.into(),
                key: if key.is_empty() {
                    "records".into()
                } else {
                    key.into()
                },
            },
            gzip,
            ..HttpTransportConfig::sender(&server.url())
        };
        let result = sender(&config).await.send_batch(&records).await;
        assert_eq!(got(&result), Got::Ok, "{name}");
        let seen = server.seen();
        assert_eq!(seen.len(), want.len(), "{name}: {seen:?}");
        for (request, (path, content_type, body)) in seen.iter().zip(want) {
            assert_eq!(request.path(), path, "{name}");
            assert_eq!(request.header("content-type"), Some(content_type), "{name}");
            let sent = if gzip {
                assert_eq!(request.header("content-encoding"), Some("gzip"), "{name}");
                let mut inflated = Vec::new();
                flate2::read::GzDecoder::new(request.body.as_slice())
                    .read_to_end(&mut inflated)
                    .unwrap();
                inflated
            } else {
                assert_eq!(request.header("content-encoding"), None, "{name}");
                request.body.clone()
            };
            assert_eq!(String::from_utf8_lossy(&sent), body, "{name}");
        }
    }

    let unknown = HttpTransportConfig {
        framing: FramingConfig {
            framing_type: "no_such_framing".into(),
            ..FramingConfig::default()
        },
        ..HttpTransportConfig::sender("http://127.0.0.1:9")
    };
    assert!(build_error(&unknown).await.contains("no_such_framing"));
}

/// Each status maps to stored, sent again (after `Retry-After`, capped),
/// held, or rejected for good.
#[tokio::test]
async fn statuses_map_to_ok_retry_hold_and_reject() {
    let s = Duration::from_secs;
    // (status, Retry-After, success_status, max_retry_after_ms, wanted)
    let rows: Vec<(u16, Option<&str>, Vec<u16>, u64, Got)> = vec![
        (200, None, vec![], 60_000, Got::Ok),
        (201, None, vec![], 60_000, Got::Ok),
        (204, None, vec![], 60_000, Got::Ok),
        (202, None, vec![], 60_000, Got::Ok),
        (202, None, vec![200, 201, 204], 60_000, Got::Backpressured),
        (202, None, vec![202], 60_000, Got::Ok),
        (400, None, vec![], 60_000, Got::Reject),
        (422, None, vec![], 60_000, Got::Reject),
        (413, None, vec![], 60_000, Got::TooLarge),
        (408, None, vec![], 60_000, Got::Backpressured),
        (429, None, vec![], 60_000, Got::Backpressured),
        (500, None, vec![], 60_000, Got::Backpressured),
        (501, None, vec![], 60_000, Got::Backpressured),
        (503, None, vec![], 60_000, Got::Backpressured),
        (599, None, vec![], 60_000, Got::Backpressured),
        (429, Some("7"), vec![], 60_000, Got::RetryAfter(s(7))),
        (503, Some("600"), vec![], 60_000, Got::RetryAfter(s(60))),
        (503, Some("600"), vec![], 0, Got::Backpressured),
        (
            503,
            Some("Wed, 21 Oct 2026 07:28:00 GMT"),
            vec![],
            60_000,
            Got::Backpressured,
        ),
        (301, None, vec![], 60_000, Got::Hold),
        (307, None, vec![], 60_000, Got::Hold),
        (401, None, vec![], 60_000, Got::Hold),
        (403, None, vec![], 60_000, Got::Hold),
        (404, None, vec![], 60_000, Got::Hold),
        (405, None, vec![], 60_000, Got::Hold),
        (407, None, vec![], 60_000, Got::Hold),
        (409, None, vec![], 60_000, Got::Hold),
        (415, None, vec![], 60_000, Got::Hold),
        (401, None, vec![401], 60_000, Got::Ok),
    ];
    for (status, retry_after, success_status, max_retry_after_ms, want) in rows {
        let server = Server::start(move |_| match retry_after {
            Some(value) => reply(status, &[("retry-after", value)]),
            None => reply(status, &[]),
        })
        .await;
        let config = HttpTransportConfig {
            success_status: success_status.clone(),
            max_retry_after_ms,
            ..HttpTransportConfig::sender(&server.url())
        };
        let result = sender(&config).await.send("ingest", "{}".into()).await;
        assert_eq!(
            got(&result),
            want,
            "HTTP {status} retry-after={retry_after:?} success={success_status:?}: {result:?}"
        );
    }

    let bad = HttpTransportConfig {
        success_status: vec![42],
        ..HttpTransportConfig::sender("http://127.0.0.1:9")
    };
    assert!(build_error(&bad).await.contains("success_status 42"));
}

/// Endpoints are tried in order, from the first on every send, until one
/// stores or rejects the body. A busy one anywhere makes the batch a retry.
#[tokio::test]
async fn failover_tries_each_endpoint_in_order() {
    #[derive(Clone, Copy)]
    enum End {
        Status(u16, Option<&'static str>),
        Down,
    }
    use End::{Down, Status};
    let ok = Status(200, None);
    // (endpoints, wanted, requests each endpoint saw)
    let rows: Vec<(Vec<End>, Got, Vec<usize>)> = vec![
        (vec![ok, ok], Got::Ok, vec![1, 0]),
        (vec![Status(503, None), ok, ok], Got::Ok, vec![1, 1, 0]),
        (vec![Status(401, None), ok], Got::Ok, vec![1, 1]),
        (vec![Down, ok], Got::Ok, vec![0, 1]),
        (vec![Status(400, None), ok], Got::Reject, vec![1, 0]),
        (
            vec![Status(503, Some("5")), Status(429, Some("9"))],
            Got::RetryAfter(Duration::from_secs(9)),
            vec![1, 1],
        ),
        (vec![Down, Down], Got::Backpressured, vec![0, 0]),
        (
            vec![Status(401, None), Down],
            Got::Backpressured,
            vec![1, 0],
        ),
        (
            vec![Status(401, None), Status(404, None)],
            Got::Hold,
            vec![1, 1],
        ),
    ];
    for (ends, want, hits) in rows {
        let mut servers = Vec::new();
        let mut urls = Vec::new();
        for end in &ends {
            match *end {
                Status(code, retry_after) => {
                    let server = Server::start(move |_| match retry_after {
                        Some(value) => reply(code, &[("retry-after", value)]),
                        None => reply(code, &[]),
                    })
                    .await;
                    urls.push(server.url());
                    servers.push(Some(server));
                }
                Down => {
                    urls.push(refused_url());
                    servers.push(None);
                }
            }
        }
        let config = HttpTransportConfig {
            failover: urls[1..]
                .iter()
                .map(|url| HttpEndpoint {
                    url: url.clone(),
                    auth: None,
                })
                .collect(),
            ..HttpTransportConfig::sender(&urls[0])
        };
        let transport = sender(&config).await;
        let result = transport.send("", "{}".into()).await;
        assert_eq!(got(&result), want, "{urls:?}: {result:?}");
        let seen: Vec<usize> = servers
            .iter()
            .map(|s| s.as_ref().map_or(0, |s| s.seen().len()))
            .collect();
        assert_eq!(seen, hits, "{urls:?}");

        // The next send starts at the first endpoint again.
        let _ = transport.send("", "{}".into()).await;
        if let Some(Some(first)) = servers.first() {
            assert_eq!(first.seen().len(), hits[0] * 2, "{urls:?}");
        }
    }

    let orphan = HttpTransportConfig {
        failover: vec![HttpEndpoint {
            url: "http://127.0.0.1:9".into(),
            auth: None,
        }],
        ..HttpTransportConfig::default()
    };
    assert!(build_error(&orphan).await.contains("need an endpoint"));
}

/// A redirect is held, never followed, unless `follow_redirects` is set, and
/// then only within the endpoint's origin.
#[tokio::test]
async fn redirects_are_refused_unless_followed_within_the_origin() {
    let elsewhere = Server::status(200).await;
    let away = format!("{}/moved", elsewhere.url());
    // (follow, Location is on the same server, wanted)
    let rows = [
        (false, true, Got::Hold),
        (true, true, Got::Ok),
        (true, false, Got::Hold),
    ];
    for (follow, same_origin, want) in rows {
        let away = away.clone();
        let server = Server::start(move |seen| {
            if seen.path() == "/moved" {
                reply(200, &[])
            } else if same_origin {
                reply(307, &[("location", "/moved")])
            } else {
                reply(307, &[("location", &away)])
            }
        })
        .await;
        let config = HttpTransportConfig {
            follow_redirects: follow,
            auth: HttpAuth::Bearer {
                token: secret("tok-redirect"),
            },
            ..HttpTransportConfig::sender(&server.url())
        };
        let result = sender(&config).await.send("ingest", "{}".into()).await;
        let name = format!("follow={follow} same_origin={same_origin}");
        assert_eq!(got(&result), want, "{name}: {result:?}");
        let seen = server.seen();
        if want == Got::Ok {
            assert_eq!(seen.len(), 2, "{name}");
            assert_eq!(seen[1].path(), "/moved", "{name}");
            assert_eq!(seen[1].body, b"{}", "{name}: a 307 keeps the body");
        } else {
            assert_eq!(seen.len(), 1, "{name}");
        }
    }
    assert!(
        elsewhere.seen().is_empty(),
        "a redirect to another origin was followed"
    );
}

/// `none` ignores the proxy environment, `system` uses it, and a proxy URL
/// carries its own basic auth.
#[tokio::test]
async fn proxy_none_system_and_url() {
    let proxy = Server::status(200).await;
    let target = Server::status(200).await;
    let proxy_url = proxy.url();
    let with_auth = format!("http://pu:pp-proxy@{}", proxy.addr);
    // (setting, environment proxy set, request reaches the proxy)
    let rows = [
        (HttpProxy::None, true, false),
        (HttpProxy::System, true, true),
        (HttpProxy::System, false, false),
        (HttpProxy::Url(secret(&with_auth)), false, true),
    ];
    for (setting, env, proxied) in rows {
        let name = format!("{setting:?} env={env}");
        let before = (proxy.seen().len(), target.seen().len());
        let env_proxy = env.then_some(proxy_url.as_str());
        let config = HttpTransportConfig {
            proxy: setting.clone(),
            ..HttpTransportConfig::sender(&target.url())
        };
        let result = temp_env::async_with_vars(
            [
                ("HTTP_PROXY", env_proxy),
                ("http_proxy", env_proxy),
                ("HTTPS_PROXY", None),
                ("https_proxy", None),
                ("ALL_PROXY", None),
                ("all_proxy", None),
                ("NO_PROXY", None),
                ("no_proxy", None),
            ],
            async { sender(&config).await.send("ingest", "{}".into()).await },
        )
        .await;
        assert_eq!(got(&result), Got::Ok, "{name}");
        let after = (proxy.seen().len(), target.seen().len());
        if proxied {
            assert_eq!(after, (before.0 + 1, before.1), "{name}");
            let request = proxy.seen().pop().unwrap();
            assert_eq!(
                request.line,
                format!("POST {}/ingest HTTP/1.1", target.url()),
                "{name}"
            );
            if matches!(setting, HttpProxy::Url(_)) {
                // base64 of pu:pp-proxy
                assert_eq!(
                    request.header("proxy-authorization"),
                    Some("Basic cHU6cHAtcHJveHk="),
                    "{name}"
                );
            }
        } else {
            assert_eq!(after, (before.0, before.1 + 1), "{name}");
        }
    }

    let shown = format!("{:?}", HttpProxy::Url(secret(&with_auth)));
    assert!(!shown.contains("pp-proxy"), "{shown}");
    let bad = HttpTransportConfig {
        proxy: HttpProxy::Url(secret("not a url pp-bad")),
        ..HttpTransportConfig::sender("http://127.0.0.1:9")
    };
    let error = build_error(&bad).await;
    assert!(
        error.contains("proxy") && !error.contains("pp-bad"),
        "{error}"
    );
}

/// A send answers by its deadline, and a hung endpoint gets only its share
/// of it, so the next endpoint still gets a turn.
#[tokio::test]
async fn a_deadline_is_shared_across_the_endpoints() {
    let hung = Server::hang().await;
    let spare = Server::status(200).await;
    let budget = Duration::from_millis(1500);
    // (endpoints, wanted)
    let rows = [
        (vec![hung.url(), spare.url()], Got::Ok),
        (vec![hung.url(), hung.url()], Got::Backpressured),
    ];
    for (urls, want) in rows {
        let config = HttpTransportConfig {
            failover: vec![HttpEndpoint {
                url: urls[1].clone(),
                auth: None,
            }],
            ..HttpTransportConfig::sender(&urls[0])
        };
        let transport = sender(&config).await;
        let started = Instant::now();
        let result = transport
            .send_batch_by(&[record(None, "{}")], started + budget)
            .await;
        let took = started.elapsed();
        assert_eq!(got(&result), want, "{urls:?}");
        assert!(
            took < budget + Duration::from_millis(500),
            "{urls:?}: answered after {took:?}"
        );
    }
}

/// No user name, password, token, key or header value reaches the config's
/// `Debug`, a send error or the logs.
#[cfg(feature = "logger")]
#[tokio::test]
async fn secrets_never_reach_debug_errors_or_logs() {
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let refused = Server::status(401).await;
    let config = HttpTransportConfig {
        failover: vec![HttpEndpoint {
            url: format!("http://fu:pw-failover@{}", refused.addr),
            auth: None,
        }],
        headers: BTreeMap::from([("X-Api-Key".to_string(), secret("key-header"))]),
        proxy: HttpProxy::None,
        ..HttpTransportConfig::sender(&format!("http://eu:pw-endpoint@{}", refused.addr))
    };
    let secrets = ["pw-failover", "key-header", "pw-endpoint", "pp-proxy"];

    let shown = format!(
        "{config:?} {:?}",
        HttpProxy::Url(secret("http://pu:pp-proxy@proxy.example:3128"))
    );
    for (i, leaked) in secrets.iter().enumerate() {
        assert!(!shown.contains(leaked), "Debug leaked secret {i}");
    }
    assert!(shown.contains("X-Api-Key") && shown.contains("proxy.example:3128"));

    let logs = Buffer::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(move || writer.clone())
            .with_ansi(false),
    );
    let result = {
        let _guard = tracing::subscriber::set_default(
            subscriber.with(tracing_subscriber::filter::LevelFilter::TRACE),
        );
        sender(&config).await.send("ingest", "{}".into()).await
    };
    let error = format!("{result:?}");
    assert_eq!(got(&result), Got::Hold, "{error}");
    let logged = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
    assert!(logged.contains("HTTP sender ready"), "{logged}");
    for (i, leaked) in secrets.iter().enumerate() {
        assert!(!error.contains(leaked), "the error leaked secret {i}");
        assert!(!logged.contains(leaked), "the logs leaked secret {i}");
    }
}

/// Every send key reaches the config through the cascade, from YAML and
/// from the environment, which wins.
#[cfg(feature = "config")]
#[test]
fn send_keys_reach_the_config_through_the_cascade() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("settings.yaml"),
        "transport:
  http:
    endpoint: https://a.example/ingest
    failover:
      - url: https://b.example/ingest
        auth: { type: bearer, token: tok-b }
      - url: https://c.example/ingest
    auth: { type: basic, username: u, password: pw-yaml }
    headers: { X-Api-Key: key-yaml }
    user_agent: yaml/1
    framing: { type: json_wrapped, key: events }
    gzip: false
    success_status: [200, 201, 204]
    proxy: http://proxy.example:3128
    follow_redirects: false
    max_retry_after_ms: 1000
",
    )
    .unwrap();
    temp_env::with_vars(
        [
            ("HTTPSEND__TRANSPORT__HTTP__GZIP", Some("true")),
            ("HTTPSEND__TRANSPORT__HTTP__USER_AGENT", Some("env/2")),
            ("HTTPSEND__TRANSPORT__HTTP__PROXY", Some("system")),
            ("HTTPSEND__TRANSPORT__HTTP__FOLLOW_REDIRECTS", Some("true")),
            (
                "HTTPSEND__TRANSPORT__HTTP__MAX_RETRY_AFTER_MS",
                Some("2500"),
            ),
            ("HTTPSEND__TRANSPORT__HTTP__AUTH__TYPE", Some("bearer")),
            ("HTTPSEND__TRANSPORT__HTTP__AUTH__TOKEN", Some("tok-env")),
            ("HTTPSEND__TRANSPORT__HTTP__FRAMING__TYPE", Some("ndjson")),
        ],
        || {
            crate::config::setup(crate::config::ConfigOptions {
                env_prefix: "HTTPSEND".to_string(),
                config_paths: vec![dir.path().to_path_buf()],
                search_standard_paths: false,
                load_dotenv: false,
                ..crate::config::ConfigOptions::default()
            })
            .unwrap();
        },
    );
    let config = HttpTransportConfig::from_cascade().unwrap();
    assert_eq!(config.endpoint.as_deref(), Some("https://a.example/ingest"));
    assert_eq!(
        config.failover,
        vec![
            HttpEndpoint {
                url: "https://b.example/ingest".into(),
                auth: Some(HttpAuth::Bearer {
                    token: secret("tok-b")
                }),
            },
            HttpEndpoint {
                url: "https://c.example/ingest".into(),
                auth: None,
            },
        ]
    );
    assert_eq!(
        config.auth,
        HttpAuth::Bearer {
            token: secret("tok-env")
        }
    );
    assert_eq!(
        config.headers,
        BTreeMap::from([("X-Api-Key".to_string(), secret("key-yaml"))])
    );
    assert_eq!(config.user_agent.as_deref(), Some("env/2"));
    assert_eq!(
        config.framing,
        FramingConfig {
            framing_type: "ndjson".into(),
            key: "events".into(),
        }
    );
    assert!(config.gzip);
    assert_eq!(config.success_status, [200, 201, 204]);
    assert_eq!(config.proxy, HttpProxy::System);
    assert!(config.follow_redirects);
    assert_eq!(config.max_retry_after_ms, 2500);
}

/// Unset send keys take their defaults: one record per request, any 2xx,
/// no proxy, no redirects, no gzip.
#[test]
fn send_keys_default_to_the_conservative_choice() {
    let config: HttpTransportConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(config.framing.framing_type, "single");
    assert_eq!(config.success_status, [] as [u16; 0]);
    assert_eq!(config.proxy, HttpProxy::None);
    assert!(!config.follow_redirects);
    assert!(!config.gzip);
    assert_eq!(config.auth, HttpAuth::None);
    assert_eq!(config.max_retry_after_ms, 60_000);

    let proxy: HttpProxy = serde_json::from_str("null").unwrap();
    assert_eq!(proxy, HttpProxy::None);
    let written = serde_json::to_string(&HttpProxy::Url(secret("http://u:p@h:1"))).unwrap();
    assert!(!written.contains("u:p"), "{written}");
}

/// Outbound filters run before framing: a dropped record is never sent, a
/// dead-lettered one makes the block `FilteredDlq` while the rest is sent,
/// and a block left with no record sends nothing.
#[tokio::test]
async fn outbound_filters_run_before_the_body_is_framed() {
    use crate::transport::filter::{FilterAction, FilterRule};

    let rule = |status: &str, action| FilterRule {
        expression: format!(r#"status == "{status}""#),
        action,
    };
    // (records, wanted, body sent, if any)
    let rows = [
        (
            vec![r#"{"status":"keep"}"#, r#"{"status":"drop"}"#],
            Got::Ok,
            Some("[{\"status\":\"keep\"}]"),
        ),
        (
            vec![r#"{"status":"keep"}"#, r#"{"status":"dlq"}"#],
            Got::Other("FilteredDlq".into()),
            Some("[{\"status\":\"keep\"}]"),
        ),
        (vec![r#"{"status":"drop"}"#], Got::Ok, None),
    ];
    for (payloads, want, body) in rows {
        let server = Server::status(200).await;
        let config = HttpTransportConfig {
            filters_out: vec![
                rule("drop", FilterAction::Drop),
                rule("dlq", FilterAction::Dlq),
            ],
            framing: FramingConfig {
                framing_type: "json_array".into(),
                ..FramingConfig::default()
            },
            ..HttpTransportConfig::sender(&server.url())
        };
        let records: Vec<Record> = payloads.iter().map(|p| record(None, p)).collect();
        let result = sender(&config).await.send_batch(&records).await;
        assert_eq!(got(&result), want, "{payloads:?}");
        let bodies: Vec<String> = server
            .seen()
            .iter()
            .map(|s| String::from_utf8_lossy(&s.body).into_owned())
            .collect();
        assert_eq!(
            bodies,
            body.into_iter().collect::<Vec<&str>>(),
            "{payloads:?}"
        );
    }
}
