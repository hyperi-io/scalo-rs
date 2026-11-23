// Project:   scalo
// File:      src/transport/syslog/tests.rs
// Purpose:   Syslog transport tests against local receivers and a RELP peer
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Notify;

use super::relp::{encode_frame, parse_frame};
use super::*;
use crate::framing::{DecodeLimits, Frame, StreamDecoder};
use crate::tls::test_pki::{TestCa, write};
use crate::tls::{ClientAuth, ClientTlsConfig, ServerTls, ServerTlsConfig, TrustChoice};

/// 2026-10-11T22:14:15.003Z
const WHEN: i64 = 1_791_756_855_003;
const STAMP: &str = "2026-10-11T22:14:15.003000Z";

fn record(payload: &str) -> Record {
    Record {
        payload: Bytes::from(payload.to_string()),
        key: None,
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: Some(WHEN),
            format: PayloadFormat::Json,
        },
    }
}

fn records(payloads: &[&str]) -> Vec<Record> {
    payloads.iter().map(|p| record(p)).collect()
}

/// The RFC 5424 message the test config writes for `payload`.
fn message(payload: &str) -> String {
    format!("<14>1 {STAMP} host1 app 42 - - {payload}")
}

fn config(address: SocketAddr, protocol: SyslogProtocol) -> SyslogConfig {
    SyslogConfig {
        hostname: Some("host1".into()),
        app_name: Some("app".into()),
        proc_id: Some("42".into()),
        connect_timeout_ms: 2_000,
        send_timeout_ms: 5_000,
        ..SyslogConfig::new(address.to_string(), protocol)
    }
}

// ---------------------------------------------------------------------------
// Receivers
// ---------------------------------------------------------------------------

trait Conn: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Conn for T {}

/// What a stream receiver does with each connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamMode {
    /// Read until the sender closes, or `close` is notified.
    Keep,
    /// Close as soon as the first bytes arrive, leaving the rest unread.
    CloseOnFirstRead,
}

/// A TCP or TLS receiver keeping the bytes of each connection.
struct StreamPeer {
    addr: SocketAddr,
    received: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    /// Closes the open connection.
    close: Arc<Notify>,
    /// Notified once a connection is dropped.
    closed: Arc<Notify>,
}

impl StreamPeer {
    async fn start(tls: Option<ServerTls>, mode: StreamMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let close = Arc::new(Notify::new());
        let closed = Arc::new(Notify::new());
        let (store, on_close, on_closed) = (received.clone(), close.clone(), closed.clone());
        tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut conn: Box<dyn Conn> = match &tls {
                    None => Box::new(tcp),
                    Some(tls) => match tls.accept(tcp, None).await {
                        Ok(stream) => Box::new(stream),
                        Err(_) => continue,
                    },
                };
                let index = {
                    let mut all = store.lock().unwrap();
                    all.push(Vec::new());
                    all.len() - 1
                };
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    tokio::select! {
                        read = conn.read(&mut buf) => match read {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                store.lock().unwrap()[index].extend_from_slice(&buf[..n]);
                                if mode == StreamMode::CloseOnFirstRead {
                                    break;
                                }
                            }
                        },
                        () = on_close.notified() => break,
                    }
                }
                drop(conn);
                on_closed.notify_one();
            }
        });
        Self {
            addr,
            received,
            close,
            closed,
        }
    }

    /// Wait until `count` messages arrived over all connections, decoded with
    /// `framing`, and return them by connection.
    async fn messages(&self, framing: SyslogFraming, count: usize) -> Vec<Vec<String>> {
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let decoded: Vec<Vec<String>> = self
                .received
                .lock()
                .unwrap()
                .iter()
                .map(|bytes| decode(framing, bytes))
                .collect();
            if decoded.iter().map(Vec::len).sum::<usize>() >= count || Instant::now() > until {
                return decoded;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The messages in one connection's bytes, decoded by scalo's stream framing.
fn decode(framing: SyslogFraming, bytes: &[u8]) -> Vec<String> {
    let framing: Arc<dyn Framing> = match framing {
        SyslogFraming::OctetCounting => Arc::new(OctetCounting),
        SyslogFraming::Lf => Arc::new(Lf),
    };
    let mut decoder = StreamDecoder::new(framing, DecodeLimits::UNBOUNDED);
    decoder.push(bytes);
    decoder.finish();
    let mut out = Vec::new();
    while let Some(frame) = decoder.next_frame().unwrap() {
        if let Frame::Record(record) = frame {
            out.push(String::from_utf8(record).unwrap());
        }
    }
    out
}

/// How the RELP test peer answers.
#[derive(Debug, Clone, Copy)]
enum RelpMode {
    /// `200 OK` to every message.
    AckAll,
    /// `500` to a message holding this text, `200` otherwise.
    Nack(&'static str),
    /// On the first connection, answer `answered` messages, read `read` in
    /// all, then close. Later connections answer everything.
    DropFirst { read: usize, answered: usize },
    /// Answer `batch` messages, then send `serverclose` and close.
    ServerClose { batch: usize },
    /// Take the session, never answer a message.
    Silent,
}

/// A RELP receiver keeping each connection's messages.
struct RelpPeer {
    addr: SocketAddr,
    received: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
}

impl RelpPeer {
    async fn start(tls: Option<ServerTls>, mode: RelpMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let store = received.clone();
        tokio::spawn(async move {
            let mut connection = 0usize;
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let conn: Box<dyn Conn> = match &tls {
                    None => Box::new(tcp),
                    Some(tls) => match tls.accept(tcp, None).await {
                        Ok(stream) => Box::new(stream),
                        Err(_) => continue,
                    },
                };
                let index = {
                    let mut all = store.lock().unwrap();
                    all.push(Vec::new());
                    all.len() - 1
                };
                relp_session(conn, mode, connection == 0, index, store.clone()).await;
                connection += 1;
            }
        });
        Self { addr, received }
    }

    fn messages(&self) -> Vec<Vec<String>> {
        self.received.lock().unwrap().clone()
    }
}

async fn relp_session(
    mut conn: Box<dyn Conn>,
    mode: RelpMode,
    first: bool,
    index: usize,
    store: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
) {
    let mut inbound = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut messages = 0usize;
    loop {
        let (frame, len) = loop {
            if let Some(found) = parse_frame(&inbound).unwrap() {
                break found;
            }
            match conn.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => inbound.extend_from_slice(&buf[..n]),
            }
        };
        inbound.drain(..len);
        let mut out = Vec::new();
        match frame.command.as_str() {
            "open" => encode_frame(
                frame.txnr,
                "rsp",
                b"200 OK\nrelp_version=0\nrelp_software=test\ncommands=syslog",
                &mut out,
            ),
            "close" => {
                encode_frame(frame.txnr, "rsp", b"", &mut out);
                let _ = conn.write_all(&out).await;
                return;
            }
            "syslog" => {
                messages += 1;
                let text = String::from_utf8(frame.data).unwrap();
                store.lock().unwrap()[index].push(text.clone());
                let answer: Option<&[u8]> = match mode {
                    RelpMode::Nack(marker) if text.contains(marker) => Some(b"500 refused"),
                    RelpMode::DropFirst { read, answered } if first => {
                        if messages == read {
                            return;
                        }
                        (messages <= answered).then_some(b"200 OK")
                    }
                    RelpMode::Silent => None,
                    RelpMode::AckAll
                    | RelpMode::ServerClose { .. }
                    | RelpMode::Nack(_)
                    | RelpMode::DropFirst { .. } => Some(b"200 OK"),
                };
                if let Some(answer) = answer {
                    encode_frame(frame.txnr, "rsp", answer, &mut out);
                }
                if let RelpMode::ServerClose { batch } = mode
                    && messages == batch
                {
                    encode_frame(0, "serverclose", b"", &mut out);
                    let _ = conn.write_all(&out).await;
                    return;
                }
            }
            other => panic!("RELP peer got `{other}`"),
        }
        if conn.write_all(&out).await.is_err() {
            return;
        }
    }
}

/// A CA, a server certificate for `localhost`, and a client certificate,
/// written under `dir`, and a server requiring a client certificate.
struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    async fn start() -> (Self, ServerTls) {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new("syslog test CA");
        let server = ca.server("receiver");
        let client = ca.client("sender");
        let path = |name: &str| dir.path().join(name);
        write(&path("ca.pem"), &ca.pem());
        write(&path("server.pem"), &server.cert_pem);
        write(&path("server.key"), &server.key_pem);
        write(&path("client.pem"), &client.cert_pem);
        write(&path("client.key"), &client.key_pem);
        let other = TestCa::new("unrelated CA");
        write(&path("other.pem"), &other.pem());
        let server_tls = ServerTls::load(&ServerTlsConfig {
            ca_file: Some(path("ca.pem")),
            client_auth: ClientAuth::Required,
            reload_interval_secs: 0,
            ..ServerTlsConfig::from_files(path("server.pem"), path("server.key"))
        })
        .await
        .unwrap();
        (Self { dir }, server_tls)
    }

    /// Client TLS trusting `ca` (a file under the dir), with the client cert.
    fn client(&self, ca: &str) -> SyslogTlsConfig {
        let path = |name: &str| self.dir.path().join(name);
        SyslogTlsConfig {
            client: ClientTlsConfig {
                trust: TrustChoice::Bundle,
                ca_path: Some(path(ca)),
                client_cert_path: Some(path("client.pem")),
                client_key_path: Some(path("client.key")),
            },
            server_name: Some("localhost".into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_stream_protocol_and_framing_round_trips() {
    let (pki, server_tls) = Pki::start().await;
    // (case, protocol, framing, TLS, payloads, messages the receiver decodes)
    let cases: Vec<(
        &str,
        SyslogProtocol,
        SyslogFraming,
        bool,
        Vec<&str>,
        Vec<String>,
    )> = vec![
        (
            "tcp octet counting",
            SyslogProtocol::Tcp,
            SyslogFraming::OctetCounting,
            false,
            vec!["{\"a\":1}", "two\nlines"],
            vec![message("{\"a\":1}"), message("two\nlines")],
        ),
        (
            "tcp lf",
            SyslogProtocol::Tcp,
            SyslogFraming::Lf,
            false,
            vec!["one", "two\nlines\n"],
            vec![message("one"), message("two#012lines")],
        ),
        (
            "tls with a client certificate",
            SyslogProtocol::Tls,
            SyslogFraming::OctetCounting,
            true,
            vec!["secret", "x"],
            vec![message("secret"), message("x")],
        ),
    ];
    for (case, protocol, framing, tls, payloads, want) in cases {
        let peer = StreamPeer::start(tls.then(|| server_tls.clone()), StreamMode::Keep).await;
        let mut cfg = config(peer.addr, protocol);
        cfg.framing = framing;
        cfg.tls = tls.then(|| pki.client("ca.pem"));
        let sender = SyslogTransport::new(&cfg).unwrap();
        let result = sender.send_batch(&records(&payloads)).await;
        assert!(result.is_ok(), "{case}: {result:?}");
        let got = peer.messages(framing, want.len()).await.concat();
        assert_eq!(got, want, "{case}");
        sender.close().await.unwrap();
    }
}

#[tokio::test]
async fn relp_round_trips_plain_and_over_tls() {
    let (pki, server_tls) = Pki::start().await;
    for (case, tls) in [("plain", false), ("tls", true)] {
        let peer = RelpPeer::start(tls.then(|| server_tls.clone()), RelpMode::AckAll).await;
        let mut cfg = config(peer.addr, SyslogProtocol::Relp);
        cfg.relp_window = 2;
        cfg.tls = tls.then(|| pki.client("ca.pem"));
        let sender = SyslogTransport::new(&cfg).unwrap();
        assert_eq!(sender.delivery_guarantee(), DeliveryGuarantee::Acknowledged);
        let payloads = ["a", "b", "c", "d", "e"];
        assert!(
            sender.send_batch(&records(&payloads)).await.is_ok(),
            "{case}"
        );
        assert!(sender.send_batch(&records(&["f"])).await.is_ok(), "{case}");
        let want: Vec<String> = ["a", "b", "c", "d", "e", "f"].map(message).to_vec();
        assert_eq!(peer.messages(), vec![want], "{case}: one session");
        sender.close().await.unwrap();
    }
}

#[tokio::test]
async fn udp_sends_one_datagram_per_message_and_cuts_long_ones() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config(socket.local_addr().unwrap(), SyslogProtocol::Udp);
    cfg.max_message_bytes = Some(480);
    let sender = SyslogTransport::new(&cfg).unwrap();
    assert_eq!(
        sender.delivery_guarantee(),
        DeliveryGuarantee::FireAndForget
    );
    // A 51-byte header, so the 2-byte character sits at bytes 479 and 480.
    let long = format!("{}\u{e9}{}", "x".repeat(428), "y".repeat(200));
    let result = sender.send_batch(&records(&["short", &long])).await;
    assert!(result.is_ok(), "{result:?}");
    assert!(
        sender.dead_letter_reason(&record(&long)).is_none(),
        "UDP cuts, never dead-letters"
    );
    let mut buf = vec![0u8; 2048];
    let n = socket.recv(&mut buf).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&buf[..n]), message("short"));
    let n = socket.recv(&mut buf).await.unwrap();
    let cut = std::str::from_utf8(&buf[..n]).expect("cut on a character boundary");
    assert_eq!(n, 479, "the 2-byte character at byte 479 is left out whole");
    assert!(cut.starts_with(&message("xxx")));
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_peer_close_between_batches_reconnects_and_loses_nothing() {
    for protocol in [SyslogProtocol::Tcp, SyslogProtocol::Relp] {
        let (first, second) = (records(&["one"]), records(&["two"]));
        let result = if protocol == SyslogProtocol::Relp {
            let peer = RelpPeer::start(None, RelpMode::ServerClose { batch: 1 }).await;
            let sender = SyslogTransport::new(&config(peer.addr, protocol)).unwrap();
            assert!(sender.send_batch(&first).await.is_ok());
            tokio::time::sleep(Duration::from_millis(50)).await;
            let result = sender.send_batch(&second).await;
            assert_eq!(
                peer.messages(),
                vec![vec![message("one")], vec![message("two")]]
            );
            result
        } else {
            let peer = StreamPeer::start(None, StreamMode::Keep).await;
            let sender = SyslogTransport::new(&config(peer.addr, protocol)).unwrap();
            assert!(sender.send_batch(&first).await.is_ok());
            peer.messages(SyslogFraming::OctetCounting, 1).await;
            peer.close.notify_one();
            peer.closed.notified().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            let result = sender.send_batch(&second).await;
            let got = peer.messages(SyslogFraming::OctetCounting, 2).await;
            assert_eq!(got, vec![vec![message("one")], vec![message("two")]]);
            result
        };
        assert!(result.is_ok(), "{protocol:?}: {result:?}");
    }
}

#[tokio::test]
async fn a_peer_close_mid_write_is_a_retry_never_ok() {
    let peer = StreamPeer::start(None, StreamMode::CloseOnFirstRead).await;
    let sender = SyslogTransport::new(&config(peer.addr, SyslogProtocol::Tcp)).unwrap();
    // 32 MiB, past what the socket buffers on both ends hold.
    let payload = "z".repeat(8_000);
    let batch: Vec<Record> = (0..4_096).map(|_| record(&payload)).collect();
    let result = sender.send_batch(&batch).await;
    assert!(result.is_backpressured(), "{result:?}");
}

#[tokio::test]
async fn relp_resends_only_the_unanswered_after_a_drop_mid_batch() {
    // Every frame is read before the close, so it is a FIN and not a reset.
    let peer = RelpPeer::start(
        None,
        RelpMode::DropFirst {
            read: 4,
            answered: 2,
        },
    )
    .await;
    let mut cfg = config(peer.addr, SyslogProtocol::Relp);
    cfg.relp_window = 8;
    let sender = SyslogTransport::new(&cfg).unwrap();
    let result = sender.send_batch(&records(&["a", "b", "c", "d"])).await;
    assert!(result.is_ok(), "{result:?}");
    let got = peer.messages();
    assert_eq!(
        got[0][..2],
        [message("a"), message("b")],
        "answered on the first session"
    );
    assert_eq!(
        got[1],
        ["c", "d"].map(message).to_vec(),
        "only the unanswered went again"
    );
}

#[tokio::test]
async fn sends_answer_each_failure_with_the_result_the_caller_acts_on() {
    let (pki, server_tls) = Pki::start().await;
    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let nack = RelpPeer::start(None, RelpMode::Nack("refuse-me")).await;
    let silent = RelpPeer::start(None, RelpMode::Silent).await;
    let tls_peer = StreamPeer::start(Some(server_tls), StreamMode::Keep).await;
    let big = "q".repeat(9_000);

    let mut untrusted = config(tls_peer.addr, SyslogProtocol::Tls);
    untrusted.tls = Some(pki.client("other.pem"));
    // (case, config, payloads, deadline, result as text)
    let cases: Vec<(&str, SyslogConfig, Vec<&str>, Duration, &str)> = vec![
        (
            "nobody listening",
            config(closed_port, SyslogProtocol::Tcp),
            vec!["x"],
            Duration::from_secs(5),
            "Backpressured",
        ),
        (
            "receiver certificate from another CA",
            untrusted,
            vec!["x"],
            Duration::from_secs(5),
            "Fatal(Config(",
        ),
        (
            "RELP nack",
            config(nack.addr, SyslogProtocol::Relp),
            vec!["ok", "refuse-me", "after"],
            Duration::from_secs(5),
            "Reject(Send(\"RELP receiver",
        ),
        (
            "RELP never answers by the deadline",
            config(silent.addr, SyslogProtocol::Relp),
            vec!["x"],
            Duration::from_millis(300),
            "Backpressured",
        ),
        (
            "only an oversize record sends nothing",
            config(closed_port, SyslogProtocol::Tcp),
            vec![big.as_str()],
            Duration::from_secs(5),
            "FilteredDlq",
        ),
    ];
    for (case, cfg, payloads, budget, want) in cases {
        let sender = SyslogTransport::new(&cfg).unwrap();
        let result = sender
            .send_batch_by(&records(&payloads), Instant::now() + budget)
            .await;
        assert!(
            format!("{result:?}").starts_with(want),
            "{case}: {result:?}"
        );
    }
    assert_eq!(
        nack.messages()[0].len(),
        3,
        "the batch is sent whole, then refused"
    );
}

#[tokio::test]
async fn an_oversize_record_is_dead_lettered_and_the_rest_sent() {
    let peer = StreamPeer::start(None, StreamMode::Keep).await;
    let sender = SyslogTransport::new(&config(peer.addr, SyslogProtocol::Tcp)).unwrap();
    let big = "q".repeat(9_000);
    let reason = sender.dead_letter_reason(&record(&big));
    assert!(
        matches!(reason, Some(DeadLetterReason::TooLarge { limit: 8192, .. })),
        "{reason:?}"
    );
    assert!(sender.dead_letter_reason(&record("small")).is_none());
    let result = sender.send_batch(&records(&["a", &big, "b"])).await;
    assert!(result.is_filtered_dlq(), "{result:?}");
    let got = peer
        .messages(SyslogFraming::OctetCounting, 2)
        .await
        .concat();
    assert_eq!(got, vec![message("a"), message("b")]);
}

#[tokio::test]
async fn a_closed_sender_is_fatal_and_healthcheck_connects() {
    let peer = RelpPeer::start(None, RelpMode::AckAll).await;
    let sender = SyslogTransport::new(&config(peer.addr, SyslogProtocol::Relp)).unwrap();
    sender.healthcheck().await.unwrap();
    assert!(sender.send("login", Bytes::from_static(b"m")).await.is_ok());
    assert_eq!(
        peer.messages(),
        vec![vec![format!(
            "<14>1 {} host1 app 42 login - m",
            peer_stamp(&peer)
        )]]
    );
    sender.close().await.unwrap();
    assert!(!sender.is_healthy());
    assert!(sender.send("", Bytes::from_static(b"m")).await.is_fatal());

    let nobody = config("127.0.0.1:9".parse().unwrap(), SyslogProtocol::Tcp);
    let sender = SyslogTransport::new(&nobody).unwrap();
    assert!(sender.healthcheck().await.is_err());
}

/// The timestamp the peer's one message carries, taken from the message.
fn peer_stamp(peer: &RelpPeer) -> String {
    peer.messages()[0][0]
        .split(' ')
        .nth(1)
        .unwrap_or_default()
        .to_string()
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[test]
fn configs_that_cannot_send_are_refused_at_build() {
    let base = || SyslogConfig::new("logs.example:514", SyslogProtocol::Tcp);
    // (case, config, error contains)
    let cases: Vec<(&str, SyslogConfig, &str)> = vec![
        ("no address", SyslogConfig::default(), "address"),
        (
            "no port",
            SyslogConfig::new("logs.example", SyslogProtocol::Tcp),
            "address",
        ),
        (
            "tls on tcp",
            SyslogConfig {
                tls: Some(SyslogTlsConfig::default()),
                ..base()
            },
            "use protocol tls",
        ),
        (
            "tiny max",
            SyslogConfig {
                max_message_bytes: Some(100),
                ..base()
            },
            "under 480",
        ),
        (
            "udp over a datagram",
            SyslogConfig {
                max_message_bytes: Some(70_000),
                ..SyslogConfig::new("logs.example:514", SyslogProtocol::Udp)
            },
            "largest UDP",
        ),
        (
            "window 0",
            SyslogConfig {
                relp_window: 0,
                ..base()
            },
            "relp_window",
        ),
        (
            "bad sd id",
            SyslogConfig {
                structured_data: [("has space".to_string(), BTreeMap::default())].into(),
                ..base()
            },
            "structured_data id",
        ),
        (
            "tls file missing",
            SyslogConfig {
                tls: Some(SyslogTlsConfig {
                    client: ClientTlsConfig {
                        trust: TrustChoice::Bundle,
                        ca_path: Some("/nonexistent/ca.pem".into()),
                        ..ClientTlsConfig::default()
                    },
                    server_name: None,
                }),
                ..SyslogConfig::new("logs.example:6514", SyslogProtocol::Tls)
            },
            "ca.pem",
        ),
        (
            "half a client identity",
            SyslogConfig {
                tls: Some(SyslogTlsConfig {
                    client: ClientTlsConfig {
                        client_cert_path: Some("/etc/tls/client.pem".into()),
                        ..ClientTlsConfig::default()
                    },
                    server_name: None,
                }),
                ..SyslogConfig::new("logs.example:6514", SyslogProtocol::Tls)
            },
            "mutual TLS needs both",
        ),
    ];
    for (case, cfg, want) in cases {
        let err = SyslogTransport::new(&cfg).err().map(|e| e.to_string());
        assert!(
            err.as_deref().is_some_and(|e| e.contains(want)),
            "{case}: {err:?}"
        );
    }
    for address in ["logs.example:514", "10.0.0.5:514", "[2001:db8::1]:6514"] {
        assert!(
            SyslogTransport::new(&SyslogConfig::new(address, SyslogProtocol::Udp)).is_ok(),
            "{address}"
        );
    }
}

/// Every key reaches the config through the cascade, from YAML and from the
/// environment, which wins.
#[cfg(feature = "config")]
#[test]
fn every_key_reaches_the_config_through_the_cascade() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("settings.yaml"),
        "transport:\n  syslog:\n    address: logs.example:6514\n    protocol: relp\n    \
         framing: lf\n    format: rfc3164\n    passthrough: true\n    facility: local3\n    \
         severity: warning\n    hostname: h1\n    app_name: a1\n    proc_id: p1\n    \
         msg_id: m1\n    structured_data:\n      meta@1:\n        k: v\n    \
         max_message_bytes: 4096\n    connect_timeout_ms: 7\n    send_timeout_ms: 8\n    \
         relp_window: 9\n    tls:\n      trust: bundle\n      ca_path: /etc/tls/ca.pem\n      \
         client_cert_path: /etc/tls/c.pem\n      client_key_path: /etc/tls/c.key\n      \
         server_name: logs.internal\n    filters_out:\n      - expression: 'level == \"debug\"'\n        \
         action: drop\n",
    )
    .unwrap();
    temp_env::with_vars(
        [
            ("SYSLOGT__TRANSPORT__SYSLOG__SEVERITY", Some("err")),
            ("SYSLOGT__TRANSPORT__SYSLOG__TLS__TRUST", Some("platform")),
        ],
        || {
            crate::config::setup(crate::config::ConfigOptions {
                env_prefix: "SYSLOGT".to_string(),
                config_paths: vec![dir.path().to_path_buf()],
                search_standard_paths: false,
                load_dotenv: false,
                ..crate::config::ConfigOptions::default()
            })
            .unwrap();
        },
    );
    let cfg = SyslogConfig::from_cascade().unwrap();
    let want = SyslogConfig {
        address: "logs.example:6514".into(),
        protocol: SyslogProtocol::Relp,
        framing: SyslogFraming::Lf,
        format: SyslogFormat::Rfc3164,
        passthrough: true,
        facility: Facility::Local3,
        severity: Severity::Err,
        hostname: Some("h1".into()),
        app_name: Some("a1".into()),
        proc_id: Some("p1".into()),
        msg_id: Some("m1".into()),
        structured_data: [(
            "meta@1".to_string(),
            [("k".to_string(), "v".to_string())].into(),
        )]
        .into(),
        max_message_bytes: Some(4096),
        connect_timeout_ms: 7,
        send_timeout_ms: 8,
        relp_window: 9,
        tls: Some(SyslogTlsConfig {
            client: ClientTlsConfig {
                trust: TrustChoice::Platform,
                ca_path: Some("/etc/tls/ca.pem".into()),
                client_cert_path: Some("/etc/tls/c.pem".into()),
                client_key_path: Some("/etc/tls/c.key".into()),
            },
            server_name: Some("logs.internal".into()),
        }),
        filters_out: cfg.filters_out.clone(),
    };
    assert_eq!(cfg, want);
    assert_eq!(cfg.filters_out.len(), 1);
}

#[tokio::test]
async fn the_factory_builds_syslog_and_holds_it_to_the_delivery_key() {
    use crate::transport::{
        AnyReceiver, AnySender, DeliveryRequirement, TransportConfig, TransportType,
    };
    let peer = RelpPeer::start(None, RelpMode::AckAll).await;
    let section = |protocol| Some(config(peer.addr, protocol));
    // (case, protocol, delivery, builds)
    let cases = [
        (
            "relp meets ack",
            SyslogProtocol::Relp,
            DeliveryRequirement::Ack,
            true,
        ),
        (
            "tcp is below ack",
            SyslogProtocol::Tcp,
            DeliveryRequirement::Ack,
            false,
        ),
        (
            "tcp meets flush",
            SyslogProtocol::Tcp,
            DeliveryRequirement::Flush,
            true,
        ),
        (
            "udp is below flush",
            SyslogProtocol::Udp,
            DeliveryRequirement::Flush,
            false,
        ),
        (
            "udp meets none",
            SyslogProtocol::Udp,
            DeliveryRequirement::None,
            true,
        ),
    ];
    for (case, protocol, delivery, builds) in cases {
        let cfg = TransportConfig {
            transport_type: TransportType::Syslog,
            delivery,
            syslog: section(protocol),
            ..TransportConfig::default()
        };
        let sender = AnySender::from_transport_config(&cfg).await;
        assert_eq!(sender.is_ok(), builds, "{case}");
        if let Ok(sender) = sender {
            assert_eq!(sender.name(), "syslog", "{case}");
        }
    }
    let cfg = TransportConfig {
        transport_type: TransportType::Syslog,
        syslog: section(SyslogProtocol::Relp),
        ..TransportConfig::default()
    };
    let err = AnyReceiver::from_transport_config(&cfg)
        .await
        .err()
        .map(|e| e.to_string());
    assert!(err.is_some_and(|e| e.contains("sends only")));
    let missing = TransportConfig {
        transport_type: TransportType::Syslog,
        ..TransportConfig::default()
    };
    assert!(AnySender::from_transport_config(&missing).await.is_err());
}

#[test]
fn utf8_floor_never_ends_inside_a_character() {
    let text = "a\u{e9}\u{1f600}b".as_bytes(); // 1 + 2 + 4 + 1
    let cases = [
        (0, 0),
        (1, 1),
        (2, 1),
        (3, 3),
        (4, 3),
        (6, 3),
        (7, 7),
        (8, 8),
        (99, 8),
    ];
    for (limit, want) in cases {
        assert_eq!(utf8_floor(text, limit), want, "limit {limit}");
    }
}
