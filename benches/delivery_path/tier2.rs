// Project:   scalo
// File:      benches/delivery_path/tier2.rs
// Purpose:   Tier 2: the bench on a real Kafka broker and real gRPC senders, and calibration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The broker is the pinned Kafka image from `transport::kafka::test_support`,
//! started alone by the parent process and removed when [`Stack`] drops, on
//! success and on a panic. Each cell builds its own senders and gRPC listeners.
//!
//! Faults on the real transports: a gRPC listener that answers after 100 ms
//! (W4), an endpoint nothing listens on (W5), a Kafka producer queue of
//! [`W6_QUEUE_BYTES`] (W6), and a gRPC sender whose `max_message_size` is below
//! W7's refused record.
//!
//! `--calibrate` measures the real senders and prints the preset values to set
//! in `src/transport/test_support/shaped.rs`. `--order-check` sends one batch
//! of mixed record sizes through a Kafka producer with a small queue and reads
//! the partition back, to see whether an approach keeps offer order.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as _;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use scalo::deployment::test_support::shared_service::{
    OwnedContainer, Suite, container_name, start_alone,
};
use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::kafka::test_support::{KAFKA_IMAGE, KAFKA_PORT, kafka_spec, require_docker};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::test_support::delivery::{
    Answer, CallerBatch, DeliveryApproach, DestinationId, Destinations, Outcome, RoutedRecord,
    approach,
};
use scalo::transport::{
    AnySender, PayloadFormat, Record, RecordMeta, SendResult, TransportBase, TransportSender,
};

use crate::listener::Listener;
use crate::loads::{GRPC_A, GRPC_B, KAFKA, Load, REFUSING_LIMIT, TOPIC};

const SUITE: Suite = Suite {
    repo: "scalo",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

/// Spreads the bench topic over more than one partition.
const PARTITIONS: i32 = 3;

/// W6's Kafka producer queue on tier 2, so a W3 load fills it in bursts.
pub const W6_QUEUE_BYTES: u64 = 4 * 1024 * 1024;

/// W4's slow gRPC destination answers each call after this.
const W4_DELAY: Duration = Duration::from_millis(100);

/// The broker container, removed on drop.
pub struct Stack {
    pub bootstrap: String,
    _broker: OwnedContainer,
}

impl Stack {
    /// Start the broker, on `cpus` when given.
    pub async fn start(cpus: Option<&str>) -> Option<Self> {
        if !require_docker() {
            eprintln!("delivery_path: tier 2 needs a Docker daemon");
            return None;
        }
        let name = container_name(&SUITE, Some("delivery-path"), "kafka");
        let spec = kafka_spec(None);
        println!(
            "# tier2 broker_image {} (pinned {KAFKA_IMAGE}) librdkafka {}",
            spec.image,
            rdkafka::util::get_rdkafka_version().1
        );
        let broker = tokio::task::spawn_blocking(move || start_alone(&SUITE, &spec, &name))
            .await
            .ok()?;
        let bootstrap = broker.container.addr(KAFKA_PORT);
        println!("# tier2 broker {} at {bootstrap}", broker.container.name);
        if let Some(cpus) = cpus {
            let updated = std::process::Command::new("docker")
                .args(["update", "--cpuset-cpus", cpus, &broker.container.name])
                .output()
                .ok()?;
            if !updated.status.success() {
                eprintln!(
                    "delivery_path: set the broker's cpuset to {cpus}: {}",
                    String::from_utf8_lossy(&updated.stderr)
                );
                return None;
            }
        }
        create_topic(&bootstrap, TOPIC, PARTITIONS).await?;
        Some(Self {
            bootstrap,
            _broker: broker,
        })
    }
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) -> Option<()> {
    let admin = KafkaAdmin::new(&producer_config(bootstrap)).ok()?;
    if let Err(e) = admin.create_topics(&[(topic, partitions, 1)]).await {
        eprintln!("delivery_path: create {topic}: {e}");
        return None;
    }
    Some(())
}

fn producer_config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..KafkaConfig::default()
    }
}

fn guard() -> Arc<MemoryGuard> {
    Arc::new(MemoryGuard::new(MemoryGuardConfig {
        limit_bytes: 1 << 40,
        ..MemoryGuardConfig::default()
    }))
}

/// One cell's real senders and the listeners behind them.
pub struct RealDestinations {
    pub destinations: Arc<Destinations<AnySender>>,
    listeners: Vec<Listener>,
}

impl RealDestinations {
    /// The three destinations with `load`'s faults, sending to `bootstrap`.
    pub async fn start(load: Load, bootstrap: &str, guard: Arc<MemoryGuard>) -> Option<Self> {
        let mut destinations = Destinations::new(guard);
        let mut kafka = producer_config(bootstrap);
        if load == Load::W6 {
            kafka.sizing.producer.buffer_memory_bytes = Some(W6_QUEUE_BYTES);
        }
        let kafka = KafkaTransport::new(&kafka).await.ok()?;
        destinations.add(KAFKA, AnySender::Kafka(kafka));

        let mut listeners = Vec::new();
        let (a_endpoint, a_listener) = Listener::scalo().await?;
        let mut a_config = GrpcConfig::client(&a_endpoint);
        if load == Load::W7 {
            a_config.max_message_size = REFUSING_LIMIT;
        }
        listeners.push(a_listener);
        let a_client = GrpcTransport::new(&a_config).await.ok()?;
        destinations.add(GRPC_A, AnySender::Grpc(a_client));

        let (b_endpoint, b_listener) = match load {
            Load::W4 => Listener::slow(W4_DELAY).await?,
            Load::W5 => Listener::down().await?,
            _ => Listener::scalo().await?,
        };
        listeners.push(b_listener);
        let b_client = GrpcTransport::new(&GrpcConfig::client(&b_endpoint))
            .await
            .ok()?;
        destinations.add(GRPC_B, AnySender::Grpc(b_client));
        Some(Self {
            destinations: Arc::new(destinations),
            listeners,
        })
    }

    pub async fn close(self) {
        for (_, _, sender) in self.destinations.iter() {
            let _ = sender.close().await;
        }
        for listener in self.listeners {
            listener.close().await;
        }
    }
}

/// Send one batch of mixed sizes through a Kafka producer with a 32 KiB queue
/// with each approach, read the one-partition topic back, and print whether the
/// partition holds the records in offer order.
pub async fn order_check(stack: &Stack) {
    for name in ["per_record_futures", "inline_sequential"] {
        let Some(approach) = approach(name) else {
            continue;
        };
        let topic = format!("order-{}", name.replace('_', "-"));
        if create_topic(&stack.bootstrap, &topic, 1).await.is_none() {
            return;
        }
        let mut config = producer_config(&stack.bootstrap);
        config.sizing.producer.buffer_memory_bytes = Some(32 * 1024);
        let Ok(producer) = KafkaTransport::new(&config).await else {
            eprintln!("order-check: producer did not start");
            return;
        };
        let mut destinations = Destinations::new(guard());
        let id = destinations.add(KAFKA, AnySender::Kafka(producer));
        let records = order_records(&topic, id);
        let offered = records.len();
        let started = Instant::now();
        let outcomes = approach
            .deliver(
                &destinations,
                CallerBatch {
                    records,
                    answer: Answer::PerRecord,
                },
            )
            .await;
        let took = started.elapsed();
        for (_, _, sender) in destinations.iter() {
            let _ = sender.close().await;
        }
        let sent = outcomes.iter().filter(|o| **o == Outcome::Sent).count();
        let seqs = read_seqs(&stack.bootstrap, &topic);
        let mut highest: Option<usize> = None;
        let mut inversions = 0usize;
        let mut first: Option<(usize, usize)> = None;
        for &seq in &seqs {
            if let Some(h) = highest
                && seq < h
            {
                inversions += 1;
                first.get_or_insert((seq, h));
            }
            highest = Some(highest.map_or(seq, |h| h.max(seq)));
        }
        println!(
            "order-check approach={name} offered={offered} answered_sent={sent} in_partition={} records_after_a_later_one={inversions} first={} took_ms={}",
            seqs.len(),
            first.map_or_else(|| "none".to_string(), |(a, b)| format!("{a}_after_{b}")),
            took.as_millis()
        );
    }
}

/// 300 records to `topic`, every seventh 8 KiB and the rest 100 B, each
/// starting with its sequence number.
fn order_records(topic: &str, id: DestinationId) -> Vec<RoutedRecord> {
    let key: Arc<str> = Arc::from(topic);
    let routes: Arc<[DestinationId]> = Arc::from([id]);
    (0..300usize)
        .map(|seq| {
            let size = if seq % 7 == 3 { 8 * 1024 } else { 100 };
            let mut payload = format!("{{\"seq\":{seq},\"pad\":\"").into_bytes();
            payload.resize(size, b'x');
            payload.extend_from_slice(b"\"}");
            RoutedRecord {
                record: Record {
                    payload: bytes::Bytes::from(payload),
                    key: Some(Arc::clone(&key)),
                    headers: Vec::new(),
                    metadata: RecordMeta {
                        timestamp_ms: None,
                        format: PayloadFormat::Json,
                    },
                },
                routes: Arc::clone(&routes),
            }
        })
        .collect()
}

/// The `seq` of every record on the one-partition `topic`, in partition order.
fn read_seqs(bootstrap: &str, topic: &str) -> Vec<usize> {
    let Ok(consumer) = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("order-{topic}"))
        .set("enable.auto.commit", "false")
        .create::<BaseConsumer>()
    else {
        return Vec::new();
    };
    let Ok((low, high)) = consumer.fetch_watermarks(topic, 0, Duration::from_secs(10)) else {
        return Vec::new();
    };
    let mut assignment = TopicPartitionList::new();
    if assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .is_err()
        || consumer.assign(&assignment).is_err()
    {
        return Vec::new();
    }
    let expected = usize::try_from(high - low).unwrap_or(0);
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seqs = Vec::with_capacity(expected);
    while seqs.len() < expected && Instant::now() < deadline {
        if let Some(Ok(message)) = consumer.poll(Duration::from_millis(200)) {
            let text = String::from_utf8_lossy(message.payload().unwrap_or_default()).into_owned();
            let seq = text
                .strip_prefix("{\"seq\":")
                .and_then(|rest| rest.split(',').next())
                .and_then(|n| n.parse().ok());
            if let Some(seq) = seq {
                seqs.push(seq);
            }
        }
    }
    seqs
}

fn records(n: usize, size: usize) -> Vec<Record> {
    let topic: Arc<str> = Arc::from(TOPIC);
    (0..n)
        .map(|i| {
            let mut payload = format!("{{\"i\":{i},\"pad\":\"").into_bytes();
            payload.resize(size.saturating_sub(2).max(payload.len()), b'x');
            payload.extend_from_slice(b"\"}");
            Record {
                payload: bytes::Bytes::from(payload),
                key: Some(Arc::clone(&topic)),
                headers: Vec::new(),
                metadata: RecordMeta {
                    timestamp_ms: None,
                    format: PayloadFormat::Json,
                },
            }
        })
        .collect()
}

fn median(samples: &[Duration]) -> Duration {
    samples.get(samples.len() / 2).copied().unwrap_or_default()
}

/// The sample at quantile `q` of sorted `samples`.
fn quantile(samples: &[Duration], q: f64) -> Duration {
    let last = samples.len().saturating_sub(1);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let at = ((last as f64) * q).round() as usize;
    samples.get(at.min(last)).copied().unwrap_or_default()
}

/// Median time of `reps` calls of `send_batch` with `block`.
async fn time_batches<S: TransportSender>(sender: &S, block: &[Record], reps: usize) -> Duration {
    median(&sample_batches(sender, block, reps).await)
}

/// Sorted times of `reps` calls of `send_batch` with `block`.
async fn sample_batches<S: TransportSender>(
    sender: &S,
    block: &[Record],
    reps: usize,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let result = sender.send_batch(block).await;
        samples.push(start.elapsed());
        if !result.is_ok() {
            eprintln!(
                "calibrate: {} block of {} answered {result:?}",
                sender.name(),
                block.len()
            );
        }
    }
    samples.sort();
    samples
}

/// Least squares for `t = a + b x1 + c x2` weighted by `1 / t^2`, so every
/// point counts by its relative error, by the normal equations.
fn fit(points: &[(f64, f64, f64)]) -> Option<(f64, f64, f64)> {
    let mut m = [[0.0f64; 4]; 3];
    for &(x1, x2, t) in points {
        let row = [1.0, x1, x2];
        let weight = 1.0 / (t * t).max(f64::EPSILON);
        for (i, cells) in m.iter_mut().enumerate() {
            for (j, cell) in cells.iter_mut().take(3).enumerate() {
                *cell += weight * row[i] * row[j];
            }
            cells[3] += weight * row[i] * t;
        }
    }
    for col in 0..3 {
        let pivot = (col..3).max_by(|&a, &b| m[a][col].abs().total_cmp(&m[b][col].abs()))?;
        m.swap(col, pivot);
        if m[col][col].abs() < f64::MIN_POSITIVE {
            return None;
        }
        let pivot_row = m[col];
        for (row, cells) in m.iter_mut().enumerate() {
            if row != col {
                let factor = cells[col] / pivot_row[col];
                for (cell, p) in cells.iter_mut().zip(pivot_row).skip(col) {
                    *cell -= factor * p;
                }
            }
        }
    }
    Some((m[0][3] / m[0][0], m[1][3] / m[1][1], m[2][3] / m[2][2]))
}

/// Points further than this fraction from the first fit are left out of the
/// second and printed, since one linear model cannot carry them.
const OUTLIER: f64 = 0.5;

/// Fit, drop the points the fit misses by more than [`OUTLIER`], fit again.
fn robust_fit(points: &[(f64, f64, f64)]) -> Option<(f64, f64, f64)> {
    let (a, b, c) = fit(points)?;
    let (kept, dropped): (Vec<(f64, f64, f64)>, Vec<(f64, f64, f64)>) = points
        .iter()
        .partition(|&&(x1, x2, t)| ((a + b * x1 + c * x2 - t) / t).abs() <= OUTLIER);
    for (n, kib, t) in dropped {
        println!(
            "# calibrate grpc NOT MODELLED n={n} kib={kib:.0} median_us={t:.0} first_fit_us={:.0}",
            a + b * n + c * kib
        );
    }
    fit(&kept)
}

fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000_000.0
}

pub async fn calibrate(stack: &Stack) {
    println!("# calibrate: every figure is a median of repeated calls on this host");

    // Kafka: the default (throughput) profile, and the same with no linger.
    let Ok(kafka) = KafkaTransport::new(&producer_config(&stack.bootstrap)).await else {
        eprintln!("calibrate: kafka producer did not start");
        return;
    };
    let mut no_linger = producer_config(&stack.bootstrap);
    no_linger.sizing.producer.linger_ms = Some(0);
    let Ok(kafka_no_linger) = KafkaTransport::new(&no_linger).await else {
        eprintln!("calibrate: kafka producer with no linger did not start");
        return;
    };
    let one = records(1, 200);
    let _ = time_batches(&kafka, &one, 20).await;
    let lone = time_batches(&kafka, &one, 200).await;
    let round_trip = time_batches(&kafka_no_linger, &one, 200).await;
    let window = lone.saturating_sub(round_trip);
    let mut points = Vec::new();
    for n in [100usize, 1_000, 10_000] {
        let block = records(n, 200);
        let t = time_batches(&kafka, &block, 7).await;
        println!(
            "# calibrate kafka send_batch n={n} size=200 median_us={:.0}",
            micros(t)
        );
        points.push((n as f64, t));
    }
    let slope_us = match (points.first(), points.last()) {
        (Some((n0, t0)), Some((n1, t1))) if n1 > n0 => (micros(*t1) - micros(*t0)) / (n1 - n0),
        _ => 0.0,
    };
    println!(
        "# calibrate kafka lone_record_us={:.0} no_linger_record_us={:.0}",
        micros(lone),
        micros(round_trip)
    );
    let _ = kafka.close().await;
    let _ = kafka_no_linger.close().await;

    let queue_full = kafka_queue_full().await;

    // gRPC: one RouteBatch call to scalo's own server against batch size and bytes.
    let Some((endpoint, listener)) = Listener::scalo().await else {
        eprintln!("calibrate: grpc listener did not start");
        return;
    };
    let Ok(client) = GrpcTransport::new(&GrpcConfig::client(&endpoint)).await else {
        eprintln!("calibrate: grpc client did not start");
        return;
    };
    let mut grpc_points = Vec::new();
    for size in [200usize, 1024] {
        for n in [1usize, 10, 100, 1_000, 5_000] {
            let block = records(n, size);
            let _ = time_batches(&client, &block, 3).await;
            let samples = sample_batches(&client, &block, 101).await;
            let t = median(&samples);
            let kib = (n * size) as f64 / 1024.0;
            println!(
                "# calibrate grpc send_batch n={n} size={size} median_us={:.0} p99_us={:.0}",
                micros(t),
                micros(quantile(&samples, 0.99))
            );
            grpc_points.push((n as f64, kib, micros(t)));
        }
    }
    let _ = client.close().await;
    listener.close().await;
    let grpc = robust_fit(&grpc_points);

    println!("\n# preset values to set in src/transport/test_support/shaped.rs");
    println!("kafka_like.call_latency_us = {:.0}", micros(round_trip));
    println!("kafka_like.confirm_window_us = {:.0}", micros(window));
    println!("kafka_like.record_cost_ns = {:.0}", slope_us * 1000.0);
    match queue_full {
        Some((wait, result)) => println!(
            "kafka_like.queue_full_wait_ms = {} (answered {result:?})",
            wait.as_millis()
        ),
        None => println!("kafka_like.queue_full_wait_ms = unmeasured"),
    }
    match grpc {
        Some((a, b, c)) => {
            println!("grpc_like.call_latency_us = {a:.0}");
            println!("grpc_like.record_cost_ns = {:.0}", b * 1000.0);
            println!("grpc_like.kib_cost_ns = {:.0}", c * 1000.0);
        }
        None => println!("grpc_like: fit failed"),
    }
}

/// Time from a `send` to its answer once the producer queue is full: a
/// producer with a 64 KiB queue and no reachable broker, filled first.
async fn kafka_queue_full() -> Option<(Duration, SendResult)> {
    let mut config = producer_config("127.0.0.1:1");
    config.sizing.producer.buffer_memory_bytes = Some(64 * 1024);
    config
        .librdkafka_overrides
        .insert("message.timeout.ms".into(), "8000".into());
    let producer = Arc::new(KafkaTransport::new(&config).await.ok()?);
    let mut fillers = Vec::new();
    for record in records(128, 1024) {
        let producer = Arc::clone(&producer);
        fillers.push(tokio::spawn(async move {
            producer.send(TOPIC, record.payload).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let start = Instant::now();
    let result = producer
        .send(TOPIC, bytes::Bytes::from_static(b"{\"probe\":1}"))
        .await;
    let waited = start.elapsed();
    for filler in fillers {
        filler.abort();
    }
    Some((waited, result))
}
