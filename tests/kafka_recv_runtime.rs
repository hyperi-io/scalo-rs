// Project:   scalo
// File:      tests/kafka_recv_runtime.rs
// Purpose:   Real-broker proof that a Kafka recv loop leaves the Tokio runtime free
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Real-broker tests that a task looping on `KafkaTransport::recv` gives its
//! thread back to the Tokio runtime between polls.
//!
//! The loop under test is the one `docs/core-pillars/shutdown.md` prescribes:
//! `select!` on the shutdown token, then `recv`. A `recv` that never pends keeps
//! its worker, so nothing turns the runtime's IO and timer driver and every other
//! task on it stops -- a health probe connects and never gets a byte back. Each
//! scenario runs the loop on a runtime of its own, in a thread of its own, and
//! probes that runtime from outside with a deadline, so a captured runtime fails
//! the test instead of hanging it.
//!
//! NO mocks: a real broker, filled by a raw rdkafka producer.
//!
//! `#[ignore]` because they need a running Docker daemon -- run on a Docker host:
//!
//! ```text
//! cargo nextest run --features transport-kafka --test kafka_recv_runtime \
//!     --run-ignored only --no-capture
//! ```

#![cfg(feature = "transport-kafka")]

use std::io::Read as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::{TransportBase, TransportReceiver};
use testcontainers_modules::kafka::apache::{self, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::io::AsyncWriteExt as _;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Kafka to test against, pinned by digest. The JVM image: `apache/kafka-native` before 4.4.0
/// segfaults in `getpwuid` on ~2% of starts.
// renovate: datasource=docker depName=apache/kafka
const KAFKA_TAG: &str = "4.3.1";

/// Digest of `KAFKA_TAG`, apart from it because the Renovate regex stops at a colon.
const KAFKA_DIGEST: &str =
    "sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

/// A JVM broker takes 5-12 s to become ready, longer on a busy runner, so 60 s is too tight.
const KAFKA_STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

/// How long the watched runtime gets to answer each probe.
const ANSWER_WITHIN: Duration = Duration::from_secs(3);

/// Group join plus the first fetch, on a cold single-node broker.
const FIRST_RECORD_WITHIN: Duration = Duration::from_secs(60);

/// Filler that brings each record to about 200 bytes, a small log event.
const PAD: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
                   0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
                   0123456789abcdef0123456789abcdef";

/// Start a single-node KRaft broker and return it plus its bootstrap address.
async fn start_kafka() -> (ContainerAsync<Kafka>, String) {
    let node = Kafka::default()
        .with_jvm_image()
        .with_tag(format!("{KAFKA_TAG}@{KAFKA_DIGEST}"))
        .with_startup_timeout(KAFKA_STARTUP_TIMEOUT)
        .start()
        .await
        .expect("start kafka container");
    let port = node
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka host port");
    (node, format!("127.0.0.1:{port}"))
}

/// Create a one-partition topic and wait until the broker's metadata lists it.
async fn create_topic(bootstrap: &str, topic: &'static str) {
    let admin = KafkaAdmin::new(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..Default::default()
    })
    .expect("kafka admin");
    admin
        .create_topics(&[(topic, 1, 1)])
        .await
        .expect("create topic");
    // A consumer that subscribes before the metadata lists the topic is told it
    // does not exist.
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        while admin
            .describe_topic(topic)
            .map_or(true, |t| t.partition_count == 0)
        {
            assert!(
                Instant::now() < deadline,
                "topic {topic} never appeared in the broker's metadata"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    })
    .await
    .expect("metadata wait");
}

/// Write `records` records to `topic`, `{"seq":N,...}` for N in `0..records`,
/// and wait for every delivery.
async fn fill(bootstrap: &str, topic: &str, records: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "20")
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .expect("raw producer");
    let mut deliveries = Vec::with_capacity(records);
    for seq in 0..records {
        let payload = format!("{{\"seq\":{seq},\"pad\":\"{PAD}\"}}");
        loop {
            match producer.send_result(FutureRecord::<(), str>::to(topic).payload(&payload)) {
                Ok(delivery) => {
                    deliveries.push(delivery);
                    break;
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err((e, _)) => panic!("enqueue record {seq}: {e}"),
            }
        }
    }
    for delivery in deliveries {
        delivery
            .await
            .expect("delivery report")
            .unwrap_or_else(|(e, _)| panic!("deliver: {e}"));
    }
}

/// A consumer transport on `topic` in its own group, reading from the start.
async fn consumer(bootstrap: &str, topic: &str, group: &str) -> KafkaTransport {
    KafkaTransport::new(&KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: group.to_string(),
        topics: vec![topic.to_string()],
        ..Default::default()
    })
    .await
    .expect("kafka consumer")
}

/// The `seq` field of a record written by [`fill`], read without a JSON parse so
/// the throughput figures measure `recv` rather than the test.
fn seq_of(payload: &[u8]) -> usize {
    let digits = payload
        .strip_prefix(b"{\"seq\":")
        .expect("a record written by fill");
    let end = digits
        .iter()
        .position(|b| !b.is_ascii_digit())
        .expect("seq is followed by more fields");
    std::str::from_utf8(&digits[..end])
        .expect("ASCII digits")
        .parse()
        .expect("seq fits usize")
}

/// The loop `docs/core-pillars/shutdown.md` prescribes, counting what it receives.
///
/// `first` is signalled inside the runtime and `first_out` outside it when the
/// first records arrive, so the observer learns of them even from a runtime the
/// loop has captured.
async fn recv_loop(
    transport: KafkaTransport,
    max: usize,
    stop: CancellationToken,
    received: Arc<AtomicUsize>,
    first: Arc<Notify>,
    first_out: std::sync::mpsc::Sender<()>,
) {
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => break,
            batch = transport.recv(max) => {
                let records = batch.expect("recv on a healthy broker").records.len();
                if records > 0 && received.fetch_add(records, Ordering::Relaxed) == 0 {
                    first.notify_one();
                    let _ = first_out.send(());
                }
            }
        }
    }
    transport.close().await.expect("close");
}

/// Answer every connection with one byte, the way a `/livez` handler would.
async fn answer_each_connection(listener: tokio::net::TcpListener) {
    while let Ok((mut stream, _)) = listener.accept().await {
        let _ = stream.write_all(b"1").await;
    }
}

/// How the runtime under a recv loop answered its probes.
#[derive(Debug)]
struct Answered {
    /// How long the runtime's timer took to fire, `None` when it had not fired
    /// within [`ANSWER_WITHIN`].
    timer: Option<Duration>,
    /// Records the loop had received when the timer fired.
    received_at_timer: usize,
    /// Whether a TCP client served by that runtime got its byte back.
    served_tcp: bool,
}

/// Run a recv loop on `runtime`, with a timer and a TCP responder beside it, and
/// probe both from outside.
///
/// The timer is armed at once, or once the first records arrive when
/// `arm_on_first_records` is set, so a busy loop is measured while records are
/// still flowing. A runtime that did not answer is left running: joining it would
/// hang the test.
fn probe_beside_recv_loop(
    runtime: Runtime,
    transport: KafkaTransport,
    max: usize,
    timer: Duration,
    arm_on_first_records: bool,
) -> Answered {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the probe listener");
    listener
        .set_nonblocking(true)
        .expect("non-blocking probe listener");
    let address = listener.local_addr().expect("probe listener address");
    let (fired_tx, fired_rx) = std::sync::mpsc::channel();
    let (first_tx, first_rx) = std::sync::mpsc::channel();
    let stop = CancellationToken::new();
    let stop_loop = stop.clone();

    let runner = std::thread::spawn(move || {
        runtime.block_on(async move {
            let received = Arc::new(AtomicUsize::new(0));
            let first = Arc::new(Notify::new());
            let looping = tokio::spawn(recv_loop(
                transport,
                max,
                stop_loop.clone(),
                Arc::clone(&received),
                Arc::clone(&first),
                first_tx,
            ));
            let listener =
                tokio::net::TcpListener::from_std(listener).expect("probe listener on the runtime");
            tokio::spawn(answer_each_connection(listener));

            if arm_on_first_records {
                first.notified().await;
            }
            let armed = Instant::now();
            tokio::time::sleep(timer).await;
            let _ = fired_tx.send((armed.elapsed(), received.load(Ordering::Relaxed)));

            stop_loop.cancelled().await;
            looping.await.expect("recv loop");
        });
    });

    if arm_on_first_records {
        first_rx
            .recv_timeout(FIRST_RECORD_WITHIN)
            .expect("the consumer received its first records");
    }
    let fired = fired_rx.recv_timeout(ANSWER_WITHIN).ok();

    let served_tcp = std::net::TcpStream::connect_timeout(&address, ANSWER_WITHIN)
        .and_then(|mut stream| {
            stream.set_read_timeout(Some(ANSWER_WITHIN))?;
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte)
        })
        .is_ok();

    stop.cancel();
    if fired.is_some() && served_tcp {
        runner.join().expect("runtime thread");
    }

    Answered {
        timer: fired.map(|(took, _)| took),
        received_at_timer: fired.map_or(0, |(_, received)| received),
        served_tcp,
    }
}

/// Assert the runtime answered both probes, and return the answer.
fn assert_answered(answered: Answered, runtime: &str) -> Answered {
    assert!(
        answered.timer.is_some(),
        "{runtime}: a timer beside the recv loop did not fire within {ANSWER_WITHIN:?} -- \
         the loop is holding the runtime: {answered:?}"
    );
    assert!(
        answered.served_tcp,
        "{runtime}: a TCP client got no byte back within {ANSWER_WITHIN:?} -- the IO \
         driver is not being turned: {answered:?}"
    );
    answered
}

/// The archiver's case: an idle consumer group, where every poll comes back empty.
async fn idle_loop_leaves_the_runtime_free(runtime: Runtime, label: &str, group: &str) {
    let (_node, bootstrap) = start_kafka().await;
    create_topic(&bootstrap, "idle").await;
    let transport = consumer(&bootstrap, "idle", group).await;

    let answered = tokio::task::spawn_blocking(move || {
        probe_beside_recv_loop(runtime, transport, 100, Duration::from_millis(100), false)
    })
    .await
    .expect("probe task");
    let answered = assert_answered(answered, label);
    eprintln!("{label}: idle recv loop, answered {answered:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn an_idle_recv_loop_leaves_a_current_thread_runtime_free() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    idle_loop_leaves_the_runtime_free(runtime, "current_thread", "idle-current-thread").await;
}

/// One worker, so a captured worker leaves nothing to turn the driver: the
/// deterministic form of the two-worker freeze, where it depends on which worker
/// held the driver when the loop took the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn an_idle_recv_loop_leaves_a_one_worker_runtime_free() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("one-worker runtime");
    idle_loop_leaves_the_runtime_free(runtime, "multi_thread(1)", "idle-one-worker").await;
}

/// A loop that always finds records must yield too, not only once the topic
/// runs dry: the timer has to fire while records are still arriving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn a_busy_recv_loop_leaves_the_runtime_free() {
    const RECORDS: usize = 400_000;
    let (_node, bootstrap) = start_kafka().await;
    create_topic(&bootstrap, "busy").await;
    fill(&bootstrap, "busy", RECORDS).await;
    let transport = consumer(&bootstrap, "busy", "busy").await;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let answered = tokio::task::spawn_blocking(move || {
        probe_beside_recv_loop(runtime, transport, 100, Duration::from_millis(20), true)
    })
    .await
    .expect("probe task");
    let answered = assert_answered(answered, "current_thread");
    eprintln!("busy recv loop, answered {answered:?}");
    assert!(
        answered.received_at_timer < RECORDS,
        "the timer fired only after the loop had drained all {RECORDS} records -- the loop \
         yields when the topic is empty but holds the runtime while records flow"
    );
}

/// A `recv` dropped while its poll is in flight must not cost the records that
/// poll took off librdkafka's queue: a later commit would skip them for good.
///
/// Every other `select!` below drops `recv` mid-poll, the way a `BatchEngine`
/// ticker arm does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn a_recv_dropped_mid_poll_loses_no_record() {
    const RECORDS: usize = 50_000;
    let (_node, bootstrap) = start_kafka().await;
    create_topic(&bootstrap, "dropped").await;
    fill(&bootstrap, "dropped", RECORDS).await;
    let transport = consumer(&bootstrap, "dropped", "dropped").await;

    let mut times_seen = vec![0_u32; RECORDS];
    let mut received = 0_usize;
    let mut dropped = 0_u32;
    let deadline = Instant::now() + Duration::from_secs(120);
    while received < RECORDS {
        assert!(
            Instant::now() < deadline,
            "received {received} of {RECORDS} records within 120 s"
        );
        tokio::select! {
            biased;
            () = tokio::task::yield_now() => dropped += 1,
            batch = transport.recv(500) => {
                for record in batch.expect("recv on a healthy broker").records {
                    times_seen[seq_of(&record.payload)] += 1;
                    received += 1;
                }
            }
        }
    }
    // A short spell more, so a record delivered twice has the chance to show.
    let settle = Instant::now() + Duration::from_millis(500);
    while Instant::now() < settle {
        for record in transport.recv(500).await.expect("recv").records {
            times_seen[seq_of(&record.payload)] += 1;
        }
    }

    eprintln!("{RECORDS} records received with {dropped} recv calls dropped mid-select");
    assert!(dropped > 0, "no recv was dropped, so nothing was tested");
    let missing: Vec<usize> = (0..RECORDS).filter(|&s| times_seen[s] == 0).collect();
    let repeated: Vec<usize> = (0..RECORDS).filter(|&s| times_seen[s] > 1).collect();
    assert!(
        missing.is_empty(),
        "{} records lost to a dropped recv, first {:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
    assert!(
        repeated.is_empty(),
        "{} records delivered twice, first {:?}",
        repeated.len(),
        &repeated[..repeated.len().min(10)]
    );
}

/// Records per second through `recv` on a filled topic, for three batch sizes.
///
/// A number for the record rather than a gate: the assertion is that every
/// record arrives exactly once. The consumer prefetches the whole topic into
/// librdkafka's local queue before the clock starts, so the figure is the cost
/// of `recv` itself rather than of the broker round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Docker daemon (run on a Docker host with --ignored)"]
async fn recv_throughput_on_a_filled_topic() {
    const RECORDS: usize = 500_000;
    let (_node, bootstrap) = start_kafka().await;
    create_topic(&bootstrap, "throughput").await;
    let filled = Instant::now();
    fill(&bootstrap, "throughput", RECORDS).await;
    eprintln!("filled {RECORDS} records in {:?}", filled.elapsed());

    for max in [100_usize, 2_000, 10_000] {
        let config = KafkaConfig {
            brokers: vec![bootstrap.clone()],
            group: format!("throughput-{max}"),
            topics: vec!["throughput".to_string()],
            ..Default::default()
        }
        .with_overrides(&[
            ("fetch.min.bytes", "1"),
            ("queued.min.messages", "10000000"),
            ("queued.max.messages.kbytes", "2097151"),
        ]);
        let transport = KafkaTransport::new(&config).await.expect("kafka consumer");
        let mut times_seen = vec![0_u32; RECORDS];
        let mut received = 0_usize;
        let deadline = Instant::now() + Duration::from_secs(180);
        while received == 0 {
            assert!(Instant::now() < deadline, "recv({max}) received nothing");
            for record in transport.recv(max).await.expect("recv").records {
                times_seen[seq_of(&record.payload)] += 1;
                received += 1;
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;

        let timed = RECORDS - received;
        let mut calls = 0_u32;
        let started = Instant::now();
        while received < RECORDS {
            assert!(
                Instant::now() < deadline,
                "recv({max}) received {received} of {RECORDS} records within 180 s"
            );
            let records = transport.recv(max).await.expect("recv").records;
            calls += 1;
            for record in records {
                times_seen[seq_of(&record.payload)] += 1;
                received += 1;
            }
        }
        let elapsed = started.elapsed();
        eprintln!(
            "recv({max}): {timed} prefetched records in {} ms over {calls} calls = {:.0} records/s",
            elapsed.as_millis(),
            f64::from(u32::try_from(timed).expect("record count fits u32")) / elapsed.as_secs_f64()
        );
        assert!(
            times_seen.iter().all(|&n| n == 1),
            "recv({max}) lost or repeated a record"
        );
        transport.close().await.expect("close");
    }
}
