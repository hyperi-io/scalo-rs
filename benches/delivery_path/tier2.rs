// Project:   scalo
// File:      benches/delivery_path/tier2.rs
// Purpose:   Tier 2: the bench on a real Kafka broker and real gRPC listeners, and calibration
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The broker is the pinned Kafka image from `transport::kafka::test_support`,
//! started alone for this process and removed when [`Stack`] drops, on success
//! and on a panic. The gRPC destinations are scalo listeners in this process,
//! drained as fast as they fill.
//!
//! `--calibrate` measures the real senders and prints the preset values to set
//! in `src/transport/test_support/shaped.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use scalo::deployment::test_support::shared_service::{
    OwnedContainer, Suite, container_name, start_alone,
};
use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::transport::grpc::{GrpcConfig, GrpcTransport};
use scalo::transport::kafka::test_support::{KAFKA_PORT, kafka_spec, require_docker};
use scalo::transport::kafka::{KafkaAdmin, KafkaConfig, KafkaTransport};
use scalo::transport::test_support::delivery::{Destinations, approach};
use scalo::transport::{
    AnySender, PayloadFormat, Record, RecordMeta, SendResult, TransportBase, TransportReceiver,
    TransportSender,
};
use tokio::task::JoinHandle;

use crate::args::Args;
use crate::drive;
use crate::loads::{self, GRPC_A, GRPC_B, KAFKA};
use crate::report::RunResult;

const SUITE: Suite = Suite {
    repo: "scalo",
    manifest_dir: env!("CARGO_MANIFEST_DIR"),
    tmp_dir: option_env!("CARGO_TARGET_TMPDIR"),
};

const TOPIC: &str = "bench-events";

/// Spreads the bench topic over more than one partition.
const PARTITIONS: i32 = 3;

/// The broker container and the gRPC listeners, torn down on drop.
struct Stack {
    bootstrap: String,
    servers: Vec<Arc<GrpcTransport>>,
    endpoints: Vec<String>,
    stop: Arc<AtomicBool>,
    drains: Vec<JoinHandle<()>>,
    _broker: OwnedContainer,
}

impl Drop for Stack {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for drain in &self.drains {
            drain.abort();
        }
    }
}

impl Stack {
    async fn start() -> Option<Self> {
        if !require_docker() {
            eprintln!("delivery_path: tier 2 needs a Docker daemon");
            return None;
        }
        let name = container_name(&SUITE, Some("delivery-path"), "kafka");
        let broker =
            tokio::task::spawn_blocking(move || start_alone(&SUITE, &kafka_spec(None), &name))
                .await
                .ok()?;
        let bootstrap = broker.container.addr(KAFKA_PORT);
        println!("# tier2 broker {} at {bootstrap}", broker.container.name);

        let admin = KafkaAdmin::new(&producer_config(&bootstrap)).ok()?;
        if let Err(e) = admin.create_topics(&[(TOPIC, PARTITIONS, 1)]).await {
            eprintln!("delivery_path: create {TOPIC}: {e}");
            return None;
        }

        let stop = Arc::new(AtomicBool::new(false));
        let mut servers = Vec::new();
        let mut endpoints = Vec::new();
        let mut drains = Vec::new();
        for _ in 0..2 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
            let addr = listener.local_addr().ok()?;
            drop(listener);
            let server = Arc::new(
                GrpcTransport::new(&GrpcConfig::server(&addr.to_string()))
                    .await
                    .ok()?,
            );
            drains.push(tokio::spawn(drain(Arc::clone(&server), Arc::clone(&stop))));
            servers.push(server);
            endpoints.push(format!("http://{addr}"));
        }
        Some(Self {
            bootstrap,
            servers,
            endpoints,
            stop,
            drains,
            _broker: broker,
        })
    }

    async fn destinations(&self) -> Option<Destinations<AnySender>> {
        let mut destinations = Destinations::new(Arc::new(MemoryGuard::new(MemoryGuardConfig {
            limit_bytes: 1 << 40,
            ..MemoryGuardConfig::default()
        })));
        let kafka = KafkaTransport::new(&producer_config(&self.bootstrap))
            .await
            .ok()?;
        destinations.add(KAFKA, AnySender::Kafka(kafka));
        for (name, endpoint) in [GRPC_A, GRPC_B].iter().zip(&self.endpoints) {
            let client = GrpcTransport::new(&GrpcConfig::client(endpoint))
                .await
                .ok()?;
            destinations.add(name, AnySender::Grpc(client));
        }
        Some(destinations)
    }

    async fn close(self) {
        self.stop.store(true, Ordering::Relaxed);
        for server in &self.servers {
            let _ = server.close().await;
        }
    }
}

/// Take what a listener received as fast as it arrives.
async fn drain(server: Arc<GrpcTransport>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        match server.recv(10_000).await {
            Ok(batch) if batch.records.is_empty() => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(batch) => {
                let _ = server.commit(&batch.commit_tokens).await;
            }
            Err(_) => break,
        }
    }
}

fn producer_config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        brokers: vec![bootstrap.to_string()],
        group: String::new(),
        ..KafkaConfig::default()
    }
}

/// Run the loads with no fault, or calibrate when `--calibrate` is given.
pub async fn run(args: &Args) -> Option<Vec<RunResult>> {
    let stack = Stack::start().await?;
    let outcome = if args.calibrate {
        calibrate(&stack).await;
        Some(Vec::new())
    } else {
        loads(args, &stack).await
    };
    stack.close().await;
    outcome
}

async fn loads(args: &Args, stack: &Stack) -> Option<Vec<RunResult>> {
    let destinations = Arc::new(stack.destinations().await?);
    let mut results = Vec::new();
    for load in &args.loads {
        if load.has_fault() {
            println!(
                "# {} skipped: faults are injected on tier 1 only",
                load.id()
            );
            continue;
        }
        let groups = loads::groups(*load, destinations.as_ref());
        for run in 0..args.runs {
            for name in &args.approaches {
                let approach = approach(name)?;
                let raw = drive::run(approach, &destinations, &groups, args.duration()).await?;
                let result = RunResult::new(args, *load, run, approach, &groups, &raw, None);
                println!("{}", result.json_line());
                results.push(result);
            }
        }
    }
    for (_, _, sender) in destinations.iter() {
        let _ = sender.close().await;
    }
    Some(results)
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

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples.get(samples.len() / 2).copied().unwrap_or_default()
}

/// Median time of `reps` calls of `send_batch` with `block`.
async fn time_batches<S: TransportSender>(sender: &S, block: &[Record], reps: usize) -> Duration {
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
    median(samples)
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

async fn calibrate(stack: &Stack) {
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

    let queue_full = kafka_queue_full(&stack.bootstrap).await;

    // gRPC: one RouteBatch call against batch size and bytes.
    let Ok(client) = GrpcTransport::new(&GrpcConfig::client(&stack.endpoints[0])).await else {
        eprintln!("calibrate: grpc client did not start");
        return;
    };
    let mut grpc_points = Vec::new();
    for size in [200usize, 1024] {
        for n in [1usize, 10, 100, 1_000, 5_000] {
            let block = records(n, size);
            let _ = time_batches(&client, &block, 3).await;
            let t = time_batches(&client, &block, 21).await;
            let kib = (n * size) as f64 / 1024.0;
            println!(
                "# calibrate grpc send_batch n={n} size={size} median_us={:.0}",
                micros(t)
            );
            grpc_points.push((n as f64, kib, micros(t)));
        }
    }
    let _ = client.close().await;
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
async fn kafka_queue_full(_bootstrap: &str) -> Option<(Duration, SendResult)> {
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
