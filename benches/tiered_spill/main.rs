// Project:   scalo
// File:      benches/tiered_spill/main.rs
// Purpose:   The tiered sink's batched spill: CPU for disk, and records released then killed
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The tiered sink's batched spill, measured.
//!
//! `matrix DIR` fills a spill under DIR from a sender answering `Backpressured`,
//! then drains it to one answering `Ok`: record size by batch cap by codec,
//! five runs a cell, each run in a child process of its own. One CSV row a run.
//!
//! `kill DIR` runs a writer that spills sequenced records with
//! `send_batch_acked` and saves a cursor past each released one, SIGKILLs it
//! fifty times a cell, then drains to a counting sink. Missing must be 0.
//!
//! `writer DIR` and `verify DIR` are the two halves of a power cut: run the
//! writer, cut the power (`qm reset` on a lab VM), boot, run the verifier.
//!
//! ```text
//! cargo bench --bench tiered_spill --features tiered-sink,metrics -- matrix /mnt/under-test
//! cargo bench --bench tiered_spill --features tiered-sink,metrics -- kill /mnt/under-test
//! ```

#![allow(clippy::cast_precision_loss, reason = "rates print as f64")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use scalo::compression::{Codec, Compression};
use scalo::tiered_sink::{DrainStrategy, TieredSink, TieredSinkConfig};
use scalo::transport::{
    BatchFinalizer, DeliveryGuarantee, DeliveryStatus, PayloadFormat, Record, RecordMeta,
    SendResult, TransportBase, TransportResult, TransportSender,
};

/// Input bytes one matrix run spills, unless `TIERED_SPILL_BYTES` says otherwise.
const RUN_BYTES: u64 = 256 << 20;
const RUNS: u32 = 5;
const KILLS: u32 = 50;
/// Records one `send_batch_acked` call carries.
const CALL_RECORDS: usize = 64;
/// Concurrent senders, so per-record spills group-commit.
const SENDERS: usize = 4;

/// A sender that answers `Backpressured` while shut and `Ok` once open,
/// keeping the sequence number of each record it takes.
#[derive(Default)]
struct Gate {
    open: AtomicBool,
    taken: Mutex<Vec<u64>>,
}

impl TransportBase for Gate {
    fn close(&self) -> impl Future<Output = TransportResult<()>> + Send {
        std::future::ready(Ok(()))
    }
    fn is_healthy(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "bench-gate"
    }
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::Acknowledged
    }
}

impl TransportSender for Gate {
    fn send(&self, _destination: &str, _payload: Bytes) -> impl Future<Output = SendResult> + Send {
        std::future::ready(self.answer(&[]))
    }

    fn send_batch(&self, records: &[Record]) -> impl Future<Output = SendResult> + Send {
        std::future::ready(self.answer(records))
    }
}

impl Gate {
    fn answer(&self, records: &[Record]) -> SendResult {
        if !self.open.load(Ordering::Acquire) {
            return SendResult::Backpressured;
        }
        let mut taken = self
            .taken
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        taken.extend(records.iter().filter_map(seq_of));
        SendResult::Ok
    }
}

/// A beats-shaped JSON envelope of about `size` bytes, its message log text
/// from a fixed seed, so a rerun writes the same bytes.
fn envelope(seq: u64, size: usize) -> Vec<u8> {
    const WORDS: [&str; 16] = [
        "GET",
        "POST",
        "/api/v1/events",
        "200",
        "404",
        "user=alice",
        "user=bob",
        "latency_ms=12",
        "src=10.1.2.3",
        "dst=10.4.5.6",
        "proto=tcp",
        "action=allow",
        "action=deny",
        "bytes=512",
        "session",
        "closed",
    ];
    let mut state = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut message = String::new();
    while message.len() + 180 < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        message.push_str(WORDS[usize::try_from(state % 16).unwrap_or(0)]);
        message.push(' ');
    }
    format!(
        "{{\"seq\":{seq},\"agent\":{{\"type\":\"filebeat\",\"name\":\"edge-01\",\"version\":\"8.0.0\"}},\
         \"data_stream\":{{\"dataset\":\"cisco_ios.log\",\"namespace\":\"ep\",\"type\":\"logs\"}},\
         \"input\":{{\"type\":\"tcp\"}},\"message\":\"{message}\"}}"
    )
    .into_bytes()
}

fn record(seq: u64, size: usize) -> Record {
    Record {
        payload: Bytes::from(envelope(seq, size)),
        key: None,
        headers: Vec::new(),
        metadata: RecordMeta {
            timestamp_ms: None,
            format: PayloadFormat::Json,
        },
    }
}

/// The `seq` an envelope starts with.
fn seq_of(record: &Record) -> Option<u64> {
    let rest = record.payload.strip_prefix(b"{\"seq\":")?;
    let end = rest.iter().position(|b| !b.is_ascii_digit())?;
    std::str::from_utf8(&rest[..end]).ok()?.parse().ok()
}

fn config(dir: &Path, codec: Codec, batch_bytes: Option<u64>) -> TieredSinkConfig {
    let mut config = TieredSinkConfig::new(dir);
    config.compression = Compression::new(codec);
    config.circuit_failure_threshold = 1;
    config.circuit_reset_timeout_ms = 3_600_000;
    config.drain_strategy = DrainStrategy::Greedy;
    match batch_bytes {
        Some(bytes) => config.spill_batch_max_bytes = bytes,
        None => config.spill_batch_max_records = 1,
    }
    config
}

/// CPU seconds this process has used, user and system.
#[cfg(unix)]
#[allow(unsafe_code)]
fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage fills the struct it is given for RUSAGE_SELF.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    // The field widths differ by platform, so go through Duration.
    let seconds = |t: libc::timeval| {
        (Duration::from_secs(u64::try_from(t.tv_sec).unwrap_or(0))
            + Duration::from_micros(u64::try_from(t.tv_usec).unwrap_or(0)))
        .as_secs_f64()
    };
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

#[cfg(not(unix))]
fn cpu_seconds() -> f64 {
    f64::NAN
}

/// This process's peak resident set in MiB, from `/proc`. NaN off Linux.
fn peak_rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|kib| kib.trim().trim_end_matches("kB").trim().parse::<f64>().ok())
        })
        .map_or(f64::NAN, |kib| kib / 1024.0)
}

/// The bytes the `.q` segment files under `dir` hold.
fn segment_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "q"))
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum()
    })
}

fn percentile(values: &mut [f64], p: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    values.sort_by(f64::total_cmp);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let at = ((values.len() - 1) as f64 * p).round() as usize;
    values[at]
}

/// One matrix run, in this process: fill, measure, drain, print a CSV row.
async fn cell(dir: &Path, size: usize, batch_bytes: Option<u64>, codec: Codec, run: u32) {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _ = metrics::set_global_recorder(recorder);
    let gate = Gate::default();
    let tiered = Arc::new(
        TieredSink::new(gate, config(dir, codec, batch_bytes))
            .await
            .expect("open the spill"),
    );
    let target = std::env::var("TIERED_SPILL_BYTES")
        .ok()
        .and_then(|bytes| bytes.parse().ok())
        .unwrap_or(RUN_BYTES);
    let calls = target / (size * CALL_RECORDS) as u64 / SENDERS as u64;
    let released: Arc<Mutex<Vec<f64>>> = Arc::default();
    let (cpu, started) = (cpu_seconds(), Instant::now());
    let mut senders = Vec::new();
    for sender in 0..SENDERS as u64 {
        let (tiered, released) = (Arc::clone(&tiered), Arc::clone(&released));
        senders.push(tokio::spawn(async move {
            for call in 0..calls {
                let first = (sender * calls + call) * CALL_RECORDS as u64;
                let records: Vec<Record> = (first..first + CALL_RECORDS as u64)
                    .map(|seq| record(seq, size))
                    .collect();
                let (sent, released) = (Instant::now(), Arc::clone(&released));
                let finalizer = BatchFinalizer::new(move |status| {
                    if status == DeliveryStatus::Delivered {
                        released
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(sent.elapsed().as_secs_f64());
                    }
                });
                let piece = finalizer.piece();
                finalizer.seal();
                tiered
                    .send_batch_acked(&records, piece)
                    .await
                    .expect("spill a batch");
            }
        }));
    }
    for sender in senders {
        sender.await.expect("a sender task");
    }
    tiered.sync().await.expect("the last sync");
    let filled = started.elapsed().as_secs_f64();
    let fill_cpu = cpu_seconds() - cpu;
    let input = calls * SENDERS as u64 * CALL_RECORDS as u64 * size as u64;
    let on_disk = segment_bytes(dir);

    let drain_started = Instant::now();
    tiered.inner().open.store(true, Ordering::Release);
    tiered.reset_circuit().await;
    while !tiered.spool_is_empty().await {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let drained = drain_started.elapsed().as_secs_f64();
    let sync_p99 = handle
        .render()
        .lines()
        .find_map(|line| {
            line.strip_prefix("spool_sync_seconds{store=\"tiered_sink\",quantile=\"0.99\"} ")
        })
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(f64::NAN);
    let release_p99 = percentile(
        &mut released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        0.99,
    );
    let mib = input as f64 / f64::from(1 << 20);
    println!(
        "{size},{},{codec},{run},{:.1},{:.2},{:.3},{:.4},{:.4},{:.1},{:.1}",
        batch_bytes.map_or_else(|| "record".to_string(), |b| b.to_string()),
        mib / filled,
        fill_cpu,
        on_disk as f64 / input as f64,
        sync_p99,
        release_p99,
        mib / drained,
        peak_rss_mib(),
    );
    if let Ok(tiered) = Arc::try_unwrap(tiered) {
        tiered.shutdown().await;
    }
}

/// Every cell, five runs each, each run a child process on a fresh directory.
fn matrix(root: &Path) {
    println!(
        "size,batch,codec,run,fill_mib_s,fill_cpu_s,disk_per_input,sync_p99_s,release_p99_s,drain_mib_s,peak_rss_mib"
    );
    let sizes = [200_usize, 1024, 16 * 1024];
    let batches = [None, Some(64 << 10), Some(256 << 10), Some(1 << 20)];
    for size in sizes {
        for batch in batches {
            for codec in [Codec::Zstd, Codec::None] {
                for run in 0..RUNS {
                    let dir = root.join(format!("{size}-{}-{codec}-{run}", batch.unwrap_or(0_u64)));
                    let status = Command::new(std::env::current_exe().expect("this binary"))
                        .args(["cell", &dir.display().to_string(), &size.to_string()])
                        .arg(batch.map_or_else(|| "record".to_string(), |b| b.to_string()))
                        .args([codec.name(), &run.to_string()])
                        .status()
                        .expect("run a cell");
                    assert!(status.success(), "cell {} failed", dir.display());
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
        }
    }
}

/// The cursor file: one past the highest sequence released, contiguously.
fn cursor_path(dir: &Path) -> PathBuf {
    dir.with_extension("cursor")
}

fn read_cursor(dir: &Path) -> u64 {
    std::fs::read_to_string(cursor_path(dir))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

/// Saves the cursor through a synced temporary file and a rename, as a
/// source commits its offset, so it holds through a power cut.
fn save_cursor(dir: &Path, cursor: u64) {
    use std::io::Write as _;
    let path = cursor_path(dir);
    let temporary = path.with_extension("cursor.tmp");
    let mut file = std::fs::File::create(&temporary).expect("create the cursor");
    writeln!(file, "{cursor}").expect("write the cursor");
    file.sync_all().expect("sync the cursor");
    std::fs::rename(&temporary, &path).expect("rename the cursor");
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        let _ = std::fs::File::open(parent).and_then(|d| d.sync_all());
    }
}

/// Spills 1 KiB records from the saved cursor on, forever, against a shut
/// gate, saving the cursor past each batch once it is released.
async fn writer(dir: &Path) {
    let tiered = TieredSink::new(Gate::default(), config(dir, Codec::Zstd, Some(256 << 10)))
        .await
        .expect("open the spill");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
    let saver = {
        let dir = dir.to_path_buf();
        tokio::spawn(async move {
            while let Some(cursor) = rx.recv().await {
                save_cursor(&dir, cursor);
            }
        })
    };
    let mut next = read_cursor(dir);
    loop {
        let records: Vec<Record> = (next..next + CALL_RECORDS as u64)
            .map(|seq| record(seq, 1024))
            .collect();
        next += CALL_RECORDS as u64;
        let (tx, past) = (tx.clone(), next);
        let finalizer = BatchFinalizer::new(move |status| {
            if status == DeliveryStatus::Delivered {
                let _ = tx.send(past);
            }
        });
        let piece = finalizer.piece();
        finalizer.seal();
        if tiered.send_batch_acked(&records, piece).await.is_err() {
            break;
        }
    }
    drop(tx);
    let _ = saver.await;
}

/// Drains the spill under `dir` to a counting sink and checks every sequence
/// below the cursor arrived. Returns (cursor, missing, duplicates).
async fn verify(dir: &Path) -> (u64, u64, u64) {
    let cursor = read_cursor(dir);
    let gate = Gate::default();
    gate.open.store(true, Ordering::Release);
    let tiered = TieredSink::new(gate, config(dir, Codec::Zstd, Some(256 << 10)))
        .await
        .expect("reopen the spill");
    while !tiered.spool_is_empty().await {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut seen: BTreeMap<u64, u64> = BTreeMap::new();
    for seq in tiered
        .inner()
        .taken
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
    {
        *seen.entry(*seq).or_default() += 1;
    }
    tiered.shutdown().await;
    let missing = (0..cursor).filter(|seq| !seen.contains_key(seq)).count() as u64;
    let duplicates = seen.values().map(|n| n.saturating_sub(1)).sum();
    (cursor, missing, duplicates)
}

/// SIGKILLs a writer `KILLS` times at seeded points, then verifies.
fn kill(root: &Path) {
    let dir = root.join("kill");
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    for _ in 0..KILLS {
        let mut child = Command::new(std::env::current_exe().expect("this binary"))
            .args(["writer", &dir.display().to_string()])
            .spawn()
            .expect("spawn a writer");
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        std::thread::sleep(Duration::from_millis(50 + state % 450));
        child.kill().expect("SIGKILL the writer");
        child.wait().expect("reap the writer");
    }
    let status = Command::new(std::env::current_exe().expect("this binary"))
        .args(["verify", &dir.display().to_string()])
        .status()
        .expect("run the verifier");
    assert!(status.success(), "records released before a kill were lost");
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["matrix", root] => matrix(Path::new(root)),
        ["kill", root] => kill(Path::new(root)),
        ["cell", dir, size, batch, codec, run] => {
            let codec = match *codec {
                "none" => Codec::None,
                _ => Codec::Zstd,
            };
            runtime.block_on(cell(
                Path::new(dir),
                size.parse().expect("a record size"),
                batch.parse().ok(),
                codec,
                run.parse().expect("a run number"),
            ));
        }
        ["writer", dir] => runtime.block_on(writer(Path::new(dir))),
        ["verify", dir] => {
            let (cursor, missing, duplicates) = runtime.block_on(verify(Path::new(dir)));
            println!("cursor={cursor} missing={missing} duplicates={duplicates}");
            if missing > 0 {
                std::process::exit(1);
            }
        }
        _ => eprintln!("usage: tiered_spill matrix DIR | kill DIR | writer DIR | verify DIR"),
    }
}
