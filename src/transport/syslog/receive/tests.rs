// Project:   scalo
// File:      src/transport/syslog/receive/tests.rs
// Purpose:   Syslog receiver tests: the framing table, timeouts and real sockets
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};

use super::tcp::{self, Closed, Connection};
use super::*;
use crate::framing::{CarriageReturns, Frame, StreamDecoder};
use crate::tls::test_pki::{TestCa, connector, write};
use crate::tls_config::ServerTlsConfig;

fn keep_cr() -> SyslogListenConfig {
    SyslogListenConfig {
        carriage_returns: CarriageReturns::Keep,
        ..SyslogListenConfig::default()
    }
}

/// The records `data` frames into on a connection under `config`, fed
/// `chunk` bytes at a time and read to the end of the stream when `eof`,
/// with the count of messages skipped as oversize.
fn decode_chunked(
    config: &SyslogListenConfig,
    data: &[u8],
    chunk: usize,
    eof: bool,
) -> (Vec<Bytes>, u64) {
    let connection = Connection::new(config);
    let mut decoder = StreamDecoder::new(connection.framing, connection.limits);
    let (mut records, mut oversize) = (Vec::new(), 0);
    let mut take = |decoder: &mut StreamDecoder| {
        while let Some(frame) = decoder.next_frame().unwrap() {
            match frame {
                Frame::Record(record) => records.push(record),
                Frame::Oversize { .. } => oversize += 1,
            }
        }
    };
    for piece in data.chunks(chunk.max(1)) {
        decoder.push(piece);
        take(&mut decoder);
    }
    if eof {
        decoder.finish();
        take(&mut decoder);
    }
    (records, oversize)
}

/// The bytes sent, whether the stream then ends, the records expected and
/// how many messages are skipped for their size.
struct Case {
    name: &'static str,
    data: Vec<u8>,
    eof: bool,
    records: &'static [&'static [u8]],
    oversize: u64,
}

fn case(
    name: &'static str,
    data: impl Into<Vec<u8>>,
    eof: bool,
    records: &'static [&'static [u8]],
    oversize: u64,
) -> Case {
    Case {
        name,
        data: data.into(),
        eof,
        records,
        oversize,
    }
}

/// The framing table with the default limits, 65536-byte messages and counts
/// trusted to 262144: the cases on an open stream.
fn stream_cases() -> Vec<Case> {
    let big_octet = format!("70000 <14>{}", "a".repeat(70_000 - 4));
    let big_line = format!("<14>{}\n", "b".repeat(70_000));
    vec![
        case("lf", *b"<14>hello world\n", false, &[b"<14>hello world"], 0),
        case("octet", *b"11 <14>hello w", false, &[b"<14>hello w"], 0),
        case(
            "octet_pair",
            *b"11 <14>msg one11 <14>msg two",
            false,
            &[b"<14>msg one", b"<14>msg two"],
            0,
        ),
        case(
            "blank_lines",
            *b"\n\n<14>hello\n\n",
            false,
            &[b"<14>hello"],
            0,
        ),
        case(
            "lf_not_utf8",
            *b"<14>caf\xe9\n<14>next\n",
            false,
            &[b"<14>caf\xe9", b"<14>next"],
            0,
        ),
        case(
            "octet_not_utf8",
            *b"9 <14>caf\xe9!9 <14>next1",
            false,
            &[b"<14>caf\xe9!", b"<14>next1"],
            0,
        ),
        case(
            "line_opens_with_a_year",
            *b"2026 Oct 11 22:14:15 host app: no PRI\n<14>next\n",
            false,
            &[b"2026 Oct 11 22:14:15 host app: no PRI", b"<14>next"],
            0,
        ),
        case(
            "line_opens_with_a_sequence_number",
            *b"123456: Mar  1 18:46:11.407: %SYS-5-CONFIG_I\n<14>next\n",
            false,
            &[b"123456: Mar  1 18:46:11.407: %SYS-5-CONFIG_I", b"<14>next"],
            0,
        ),
        case(
            "digits_space_no_pri",
            *b"11 hello world\n<14>x\n",
            false,
            &[b"11 hello world", b"<14>x"],
            0,
        ),
        case(
            "digits_two_spaces",
            *b"11  <14>hello\n",
            false,
            &[b"11  <14>hello"],
            0,
        ),
        case("zero_count", *b"0 <14>x\n", false, &[b"0 <14>x"], 0),
        case(
            "oversize_octet_is_skipped",
            format!("{big_octet}11 <14>hello w"),
            false,
            &[b"<14>hello w"],
            1,
        ),
        case(
            "oversize_line_is_skipped",
            format!("{big_line}<14>ok\n"),
            false,
            &[b"<14>ok"],
            1,
        ),
        case(
            "two_oversize_lines",
            format!("{big_line}{big_line}<14>ok\n"),
            false,
            &[b"<14>ok"],
            2,
        ),
        case(
            "untrusted_count_is_a_line",
            *b"9999999 <14>hello\n<14>next\n",
            false,
            &[b"9999999 <14>hello", b"<14>next"],
            0,
        ),
        case(
            "overflowing_count_is_a_line",
            *b"99999999999999999999999 <14>x\n",
            false,
            &[b"99999999999999999999999 <14>x"],
            0,
        ),
    ]
}

/// The cases that end the stream, then the carriage returns kept.
fn eof_and_cr_cases() -> Vec<Case> {
    vec![
        case(
            "last_line_no_lf",
            *b"<14>one\n<14>last",
            true,
            &[b"<14>one", b"<14>last"],
            0,
        ),
        case(
            "last_line_no_pri",
            *b"2026-10-31T10:00:00Z host app: no PRI",
            true,
            &[b"2026-10-31T10:00:00Z host app: no PRI"],
            0,
        ),
        case(
            "partial_octet_at_eof",
            *b"<14>one\n20 <14>cut",
            true,
            &[b"<14>one"],
            0,
        ),
        case(
            "trailing_whitespace_at_eof",
            *b"<14>one\n\n \t",
            true,
            &[b"<14>one"],
            0,
        ),
        case(
            "lone_number_at_eof",
            *b"<14>one\n2026",
            true,
            &[b"<14>one", b"2026"],
            0,
        ),
        case(
            "oversize_line_cut_by_eof",
            format!("<14>{}", "c".repeat(70_000)),
            true,
            &[],
            1,
        ),
        // A CR before LF stays in the record, and a line of one CR is a record.
        case(
            "crlf_lines",
            *b"<14>one\r\n<14>two\r\n",
            false,
            &[b"<14>one\r", b"<14>two\r"],
            0,
        ),
        case(
            "cr_only_line",
            *b"<14>one\n\r\n<14>two\n",
            false,
            &[b"<14>one", b"\r", b"<14>two"],
            0,
        ),
    ]
}

/// Each case frames the same whether its bytes arrive at once, in small
/// pieces or one at a time.
#[test]
fn records_match_the_framing_table_in_any_pieces() {
    let config = keep_cr();
    let mut failures = Vec::new();
    let mut runs = 0;
    for c in stream_cases().into_iter().chain(eof_and_cr_cases()) {
        for chunk in [c.data.len(), 7, 1] {
            runs += 1;
            let (records, oversize) = decode_chunked(&config, &c.data, chunk, c.eof);
            let records: Vec<&[u8]> = records.iter().map(|r| &r[..]).collect();
            if records != c.records || oversize != c.oversize {
                failures.push(format!(
                    "{} (chunk {chunk}): got {:?}, oversize {oversize}",
                    c.name,
                    records
                        .iter()
                        .map(|r| String::from_utf8_lossy(&r[..r.len().min(40)]))
                        .collect::<Vec<_>>(),
                ));
            }
        }
    }
    assert_eq!(runs, 72);
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));

    // The frame limit is never below the message limit, so every count a
    // message may carry is trusted.
    let small = SyslogListenConfig {
        max_message_bytes: 100,
        max_frame_bytes: 10,
        ..keep_cr()
    };
    let message = format!("<14>{}", "x".repeat(96));
    let (records, oversize) = decode_chunked(&small, format!("100 {message}").as_bytes(), 3, false);
    assert_eq!((records, oversize), (vec![Bytes::from(message)], 0));

    assert_eq!(decode_chunked(&config, b"", 1, false), (Vec::new(), 0));
}

/// What a sender does next: send bytes, or wait.
enum Step {
    Send(&'static [u8]),
    Wait(Duration),
}

const LIMIT: Duration = Duration::from_millis(500);
const PEER: &str = "192.0.2.1:514";

/// Run `steps` against one connection's reader under `config`, with a record
/// already queued ahead and the queue (room for one) taken one record per
/// `hold`. The records read, and why the reader closed the connection.
async fn run(
    config: &SyslogListenConfig,
    steps: Vec<Step>,
    hold: Duration,
) -> (Vec<String>, Option<Closed>) {
    let (mut client, server) = tokio::io::duplex(1024);
    let (tx, mut rx) = mpsc::channel(1);
    let inbox = Inbox {
        tx,
        sequence: AtomicU64::new(0),
    };
    let peer: SocketAddr = PEER.parse().unwrap();
    inbox
        .tx
        .try_send(inbox.message(Bytes::from_static(b"ahead"), peer, 0))
        .unwrap();
    let consumer = tokio::spawn(async move {
        let mut got = Vec::new();
        loop {
            tokio::time::sleep(hold).await;
            match rx.recv().await {
                Some(m) => got.push(String::from_utf8_lossy(&m.payload).into_owned()),
                None => return got,
            }
        }
    });
    let sender = tokio::spawn(async move {
        for step in steps {
            match step {
                Step::Send(bytes) => {
                    if client.write_all(bytes).await.is_err() {
                        return;
                    }
                }
                Step::Wait(wait) => tokio::time::sleep(wait).await,
            }
        }
    });
    let connection = Connection::new(config);
    let closed = tcp::read(server, peer, &connection, &inbox, &CancellationToken::new()).await;
    sender.abort();
    drop(inbox);
    let mut got = consumer.await.unwrap();
    assert_eq!(got.remove(0), "ahead");
    (got, closed)
}

/// A frame that stops part way, or trickles in slower than the frame timeout,
/// closes the connection. Waiting between whole frames, after a blank line,
/// or while the queue is full does not count. A connection with no frame for
/// the idle timeout closes.
#[tokio::test(start_paused = true)]
async fn only_a_frame_in_progress_runs_the_frame_clock() {
    let config = SyslogListenConfig {
        frame_timeout_ms: 500,
        idle_timeout_ms: 10_000,
        ..keep_cr()
    };
    let quiet = LIMIT * 3;
    let trickle = |first: &'static [u8], each: &'static [u8]| -> Vec<Step> {
        std::iter::once(Step::Send(first))
            .chain((0..12).flat_map(|_| [Step::Send(each), Step::Wait(LIMIT / 4)]))
            .collect()
    };
    let long_line: &'static [u8] = Box::leak(vec![b'b'; 70_000].into_boxed_slice());
    let frame_timeout = Some(Closed::FrameTimeout);
    // (case, steps, hold, records, why the connection closed)
    let cases: [(&str, Vec<Step>, Duration, &[&str], Option<Closed>); 8] = [
        (
            "an oversize octet frame trickled in while it is skipped",
            trickle(b"70000 <", b"x"),
            Duration::ZERO,
            &[],
            frame_timeout,
        ),
        (
            "an oversize line trickled in while it is skipped",
            trickle(long_line, b"b"),
            Duration::ZERO,
            &[],
            frame_timeout,
        ),
        (
            "a frame that stops part way",
            vec![Step::Send(b"a\npar"), Step::Wait(quiet)],
            Duration::ZERO,
            &["a"],
            frame_timeout,
        ),
        (
            "a frame trickled in slower than the limit",
            trickle(b"x", b"x"),
            Duration::ZERO,
            &[],
            frame_timeout,
        ),
        (
            "quiet between whole frames",
            vec![Step::Send(b"a\n"), Step::Wait(quiet), Step::Send(b"b\n")],
            Duration::ZERO,
            &["a", "b"],
            None,
        ),
        (
            "quiet after a blank line",
            vec![
                Step::Send(b"a\n"),
                Step::Wait(quiet),
                Step::Send(b"\n"),
                Step::Wait(quiet),
                Step::Send(b"b\n"),
            ],
            Duration::ZERO,
            &["a", "b"],
            None,
        ),
        (
            "the next frame begun while the queue is full",
            vec![
                Step::Send(b"a\nb"),
                Step::Wait(quiet + LIMIT / 5),
                Step::Send(b"\n"),
            ],
            quiet,
            &["a", "b"],
            None,
        ),
        (
            "no frame for the idle timeout",
            vec![Step::Send(b"a\n"), Step::Wait(Duration::from_secs(30))],
            Duration::ZERO,
            &["a"],
            Some(Closed::Idle),
        ),
    ];
    let mut failed = Vec::new();
    for (name, steps, hold, records, closed) in cases {
        let got = run(&config, steps, hold).await;
        if got.0 != records || got.1 != closed {
            failed.push(format!("{name}: got {got:?}, want {records:?} {closed:?}"));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

/// Records queued so far, waiting until `count` arrive or 5 s pass.
async fn take(receiver: &SyslogReceiver, count: usize) -> Vec<(Bytes, SyslogToken)> {
    let until = Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    while got.len() < count && Instant::now() < until {
        let batch = receiver.recv(count).await.unwrap();
        got.extend(
            batch
                .records
                .into_iter()
                .map(|r| r.payload)
                .zip(batch.commit_tokens),
        );
    }
    got
}

/// Octet-counted frames and LF lines share a TCP connection, a datagram is
/// one record and an empty one none, each as sent and tagged with its
/// sender, and close hands over what is queued then reports `Closed`.
#[tokio::test]
async fn tcp_and_udp_records_arrive_as_sent() {
    let receiver = SyslogReceiver::new(&SyslogListenConfig {
        tcp_address: Some("127.0.0.1:0".into()),
        udp_address: Some("127.0.0.1:0".into()),
        ..keep_cr()
    })
    .await
    .unwrap();
    assert_eq!(
        receiver.delivery_guarantee(),
        DeliveryGuarantee::FireAndForget
    );

    let mut tcp = TcpStream::connect(receiver.tcp_addr().unwrap())
        .await
        .unwrap();
    tcp.write_all(b"11 <14>hello w<14>line\r\n\r\n<14>last")
        .await
        .unwrap();
    tcp.shutdown().await.unwrap();
    let from = take(&receiver, 4).await;
    let records: Vec<&[u8]> = from.iter().map(|(r, _)| &r[..]).collect();
    assert_eq!(
        records,
        [&b"<14>hello w"[..], b"<14>line\r", b"\r", b"<14>last"]
    );
    let sender = tcp.local_addr().unwrap();
    assert!(from.iter().all(|(_, token)| token.peer == sender));

    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let to = receiver.udp_addr().unwrap();
    for datagram in [&b""[..], b"<14>one datagram", b"<14>two\r\n"] {
        udp.send_to(datagram, to).await.unwrap();
    }
    let from = take(&receiver, 2).await;
    let records: Vec<&[u8]> = from.iter().map(|(r, _)| &r[..]).collect();
    assert_eq!(records, [&b"<14>one datagram"[..], b"<14>two\r\n"]);
    let sender = udp.local_addr().unwrap();
    assert!(from.iter().all(|(_, token)| token.peer == sender));

    udp.send_to(b"<14>queued", to).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    receiver.close().await.unwrap();
    let queued = receiver.recv(10).await.unwrap();
    assert_eq!(queued.records.len(), 1, "queued before close, handed over");
    assert!(matches!(
        receiver.recv(10).await,
        Err(TransportError::Closed)
    ));
    assert!(!receiver.is_healthy());
}

/// A TLS connection frames as TCP does, behind the handshake.
#[tokio::test]
async fn tls_records_arrive_after_the_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new("syslog receiver test CA");
    let server = ca.server("receiver");
    let (cert, key) = (dir.path().join("server.pem"), dir.path().join("server.key"));
    write(&cert, &server.cert_pem);
    write(&key, &server.key_pem);
    let receiver = SyslogReceiver::new(&SyslogListenConfig {
        tls_address: Some("127.0.0.1:0".into()),
        tls: Some(ServerTlsConfig {
            reload_interval_secs: 0,
            ..ServerTlsConfig::from_files(cert, key)
        }),
        ..SyslogListenConfig::default()
    })
    .await
    .unwrap();
    assert_eq!(receiver.delivery_guarantee(), DeliveryGuarantee::Written);

    let tcp = TcpStream::connect(receiver.tls_addr().unwrap())
        .await
        .unwrap();
    let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector(&ca.pem(), None, &[])
        .connect(name, tcp)
        .await
        .unwrap();
    tls.write_all(b"6 <14>ab<14>cd\r\n").await.unwrap();
    tls.shutdown().await.unwrap();
    let records: Vec<Bytes> = take(&receiver, 2)
        .await
        .into_iter()
        .map(|(r, _)| r)
        .collect();
    assert_eq!(records, [&b"<14>ab"[..], b"<14>cd"], "CRs strip by default");
}

/// A config with nothing to listen on, or a TLS listener without its TLS, is
/// refused naming what to set.
#[tokio::test]
async fn a_config_that_cannot_listen_is_refused() {
    let tls = Some(ServerTlsConfig::from_files("/nowhere.pem", "/nowhere.key"));
    let local = Some("127.0.0.1:0".to_string());
    // (case, config, error text)
    let cases = [
        (
            "no listener",
            SyslogListenConfig::default(),
            "set tcp_address",
        ),
        (
            "tls address alone",
            SyslogListenConfig {
                tls_address: local.clone(),
                ..SyslogListenConfig::default()
            },
            "go together",
        ),
        (
            "tls section alone",
            SyslogListenConfig {
                tcp_address: local.clone(),
                tls,
                ..SyslogListenConfig::default()
            },
            "go together",
        ),
        (
            "not an address",
            SyslogListenConfig {
                udp_address: Some("logs:514".into()),
                ..SyslogListenConfig::default()
            },
            "udp_address `logs:514` is not ip:port",
        ),
        (
            "no queue",
            SyslogListenConfig {
                tcp_address: local,
                recv_buffer_size: 0,
                ..SyslogListenConfig::default()
            },
            "must be above 0",
        ),
    ];
    for (case, config, want) in cases {
        let err = SyslogReceiver::new(&config)
            .await
            .err()
            .map(|e| e.to_string());
        assert!(
            err.as_deref().is_some_and(|e| e.contains(want)),
            "{case}: {err:?}"
        );
    }
}

/// `type: syslog` with a `listen` section builds the receiver.
#[tokio::test]
async fn the_factory_builds_a_syslog_receiver_from_listen() {
    use crate::transport::{AnyReceiver, SyslogConfig, TransportConfig, TransportType};
    let config = TransportConfig {
        transport_type: TransportType::Syslog,
        syslog: Some(SyslogConfig {
            listen: Some(SyslogListenConfig {
                tcp_address: Some("127.0.0.1:0".into()),
                ..SyslogListenConfig::default()
            }),
            ..SyslogConfig::default()
        }),
        ..TransportConfig::default()
    };
    let receiver = AnyReceiver::from_transport_config(&config).await.unwrap();
    assert_eq!(
        (receiver.name(), receiver.delivery_guarantee()),
        ("syslog", DeliveryGuarantee::Written)
    );
    receiver.close().await.unwrap();
}

/// Every key reaches the config through the cascade, from YAML and from the
/// environment, which wins.
#[cfg(feature = "config")]
#[test]
fn every_listen_key_reaches_the_config_through_the_cascade() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("settings.yaml"),
        "transport:\n  syslog:\n    listen:\n      tcp_address: 0.0.0.0:1514\n      \
         tls_address: 0.0.0.0:6514\n      udp_address: 0.0.0.0:514\n      tls:\n        \
         cert_file: /etc/tls/s.pem\n        key_file: /etc/tls/s.key\n      \
         max_message_bytes: 1000\n      max_frame_bytes: 2000\n      carriage_returns: keep\n      \
         tcp_keepalive_secs: 3\n      idle_timeout_ms: 4\n      frame_timeout_ms: 5\n      \
         udp_recv_buffer_bytes: 6\n      udp_batch_size: 7\n      recv_buffer_size: 8\n      \
         recv_timeout_ms: 9\n      filters_in:\n        - expression: 'level == \"debug\"'\n          \
         action: drop\n",
    )
    .unwrap();
    temp_env::with_vars(
        [
            (
                "SYSLOGR__TRANSPORT__SYSLOG__LISTEN__UDP_BATCH_SIZE",
                Some("70"),
            ),
            (
                "SYSLOGR__TRANSPORT__SYSLOG__LISTEN__CARRIAGE_RETURNS",
                Some("strip"),
            ),
        ],
        || {
            crate::config::setup(crate::config::ConfigOptions {
                env_prefix: "SYSLOGR".to_string(),
                config_paths: vec![dir.path().to_path_buf()],
                search_standard_paths: false,
                load_dotenv: false,
                ..crate::config::ConfigOptions::default()
            })
            .unwrap();
        },
    );
    let cfg = SyslogListenConfig::from_cascade().unwrap();
    let want = SyslogListenConfig {
        tcp_address: Some("0.0.0.0:1514".into()),
        tls_address: Some("0.0.0.0:6514".into()),
        udp_address: Some("0.0.0.0:514".into()),
        tls: Some(ServerTlsConfig::from_files(
            "/etc/tls/s.pem",
            "/etc/tls/s.key",
        )),
        max_message_bytes: 1000,
        max_frame_bytes: 2000,
        carriage_returns: CarriageReturns::Strip,
        tcp_keepalive_secs: 3,
        idle_timeout_ms: 4,
        frame_timeout_ms: 5,
        udp_recv_buffer_bytes: 6,
        udp_batch_size: 70,
        recv_buffer_size: 8,
        recv_timeout_ms: 9,
        filters_in: cfg.filters_in.clone(),
    };
    assert_eq!(cfg, want);
    assert_eq!(cfg.filters_in.len(), 1);
    let section = crate::transport::SyslogConfig::from_cascade().unwrap();
    assert_eq!(section.listen, Some(want));
}
