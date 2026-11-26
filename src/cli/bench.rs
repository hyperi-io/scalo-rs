// Project:   scalo
// File:      src/cli/bench.rs
// Purpose:   The bench-memory subcommand: the app's pipeline under the standard load
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `<app> bench-memory`: the app's own pipeline under a fixed synthetic load,
//! with the process's memory sampled over a fixed window.
//!
//! Producers offer JSON records of `--record-bytes` to an in-memory source at
//! `--records-per-second`, and a drain discards whatever reaches the sink.
//! After `--warmup-seconds` the figure [`UsageSource::detect`] picks, the one
//! the memory guard reads by default, is sampled every `--sample-interval-ms`
//! for `--window-seconds`, and the profile goes to `--output` for
//! `generate-artefacts --memory-profile`.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::app::{ServiceApp, artefact_json, start_service};
use super::commands::BenchMemoryArgs;
use super::error::CliError;
use super::output;
use crate::deployment::{BenchLoad, MemoryProfile, MemoryReading};
use crate::memory::UsageSource;
use crate::transport::{MemoryConfig, MemoryTransport, TransportReceiver};

/// The two transports `bench-memory` runs an app's pipeline between.
#[derive(Clone)]
#[non_exhaustive]
pub struct BenchPipeline {
    /// Where the load arrives: JSON records of the configured size.
    pub source: Arc<MemoryTransport>,
    /// Where the pipeline sends. A drain discards whatever arrives.
    pub sink: Arc<MemoryTransport>,
}

/// How often a producer tops the source up to its rate.
const PRODUCER_TICK: Duration = Duration::from_millis(10);

/// How long a receive waits for its first record, so a pipeline looping on an
/// empty source sleeps rather than spins.
const RECV_WAIT_MS: u64 = 100;

/// The most records the drain takes from the sink at once.
const DRAIN_BATCH: usize = 1000;

/// How long the pipeline has to return once the window ends.
const STOP_GRACE: Duration = Duration::from_secs(30);

/// The bytes of the smallest record, `{"body":""}`.
const RECORD_FRAME: usize = 11;

/// Run the app's bench pipeline under the load `args` describe and write the
/// profile to `args.output`.
pub(super) async fn run<A: ServiceApp>(
    app: &A,
    args: &BenchMemoryArgs,
    config_path: Option<&str>,
) -> Result<(), CliError> {
    let plan = Plan::from_args(args)?;
    let usage = UsageSource::detect();
    let reading = memory_reading(&usage).ok_or_else(|| {
        CliError::Service(
            "this platform reports no memory figure for the process, so there is nothing to \
             sample"
                .into(),
        )
    })?;

    let (config, runtime) = start_service(app, config_path).await?;
    let shutdown = runtime.shutdown.clone();
    let bench = BenchPipeline {
        source: Arc::new(transport()?),
        sink: Arc::new(transport()?),
    };
    let ends = bench.clone();
    let Some(pipeline) = app.bench_pipeline(config, runtime, bench) else {
        shutdown.cancel();
        let message = format!(
            "`{}` has no bench pipeline -- override ServiceApp::bench_pipeline to run it \
             between the bench source and sink",
            app.name()
        );
        output::print_error(&message);
        return Err(CliError::Service(message));
    };
    tokio::pin!(pipeline);

    let load = Load::start(&plan, &ends);
    let samples = tokio::select! {
        stopped = &mut pipeline => {
            load.stop().await?;
            return Err(stopped.err().unwrap_or_else(|| {
                CliError::Service("the bench pipeline returned before the window ended".into())
            }));
        }
        samples = sample(&usage, &plan) => samples,
    };
    load.stop_producers();
    shutdown.cancel();
    let stopped = tokio::time::timeout(STOP_GRACE, &mut pipeline).await;
    load.stop().await?;
    stopped.map_err(|_| {
        CliError::Service(format!(
            "the bench pipeline did not return within {} s of shutdown",
            STOP_GRACE.as_secs()
        ))
    })??;

    let profile = samples?.into_profile(&plan, reading)?;
    tokio::fs::write(&args.output, artefact_json(&profile, "memory profile")?)
        .await
        .map_err(|e| CliError::Service(format!("failed to write {}: {e}", args.output)))?;
    output::print_success(&format!(
        "memory profile of {} samples written to {}",
        profile.samples, args.output
    ));
    for (name, bytes) in [
        ("min", profile.min_bytes),
        ("avg", profile.avg_bytes),
        ("max", profile.max_bytes),
    ] {
        output::print_kv(name, &format_args!("{bytes} bytes"));
    }
    Ok(())
}

/// What a sample read, or `None` where the OS gives the process no figure.
fn memory_reading(usage: &UsageSource) -> Option<MemoryReading> {
    match usage {
        UsageSource::CgroupV2(_) => Some(MemoryReading::CgroupV2),
        UsageSource::CgroupV1(_) => Some(MemoryReading::CgroupV1),
        UsageSource::ProcStatus(_) => Some(MemoryReading::ProcStatus),
        UsageSource::WorkingSet => Some(MemoryReading::WorkingSet),
        UsageSource::TaskInfo => Some(MemoryReading::TaskInfo),
        UsageSource::Reservations => None,
    }
}

/// A source or sink whose receive waits for a record rather than returning
/// empty at once.
fn transport() -> Result<MemoryTransport, CliError> {
    MemoryTransport::new(&MemoryConfig {
        recv_timeout_ms: RECV_WAIT_MS,
        ..MemoryConfig::default()
    })
    .map_err(|e| CliError::Service(format!("bench transport failed: {e}")))
}

/// One run's load and sampling, checked against each other.
#[derive(Debug, Clone)]
struct Plan {
    load: BenchLoad,
    record: Arc<Vec<u8>>,
    warmup_seconds: u32,
    window_seconds: u32,
    interval: Duration,
    samples: u32,
}

impl Plan {
    fn from_args(args: &BenchMemoryArgs) -> Result<Self, CliError> {
        let invalid = |message: String| Err(CliError::InvalidArgument(message));
        if args.sources == 0 || u64::from(args.sources) > args.records_per_second {
            return invalid(format!(
                "--sources {} must be 1 to --records-per-second {}, so every producer offers \
                 records",
                args.sources, args.records_per_second
            ));
        }
        let record_bytes = usize::try_from(args.record_bytes).unwrap_or(usize::MAX);
        if record_bytes < RECORD_FRAME || record_bytes == usize::MAX {
            return invalid(format!(
                "--record-bytes {} must hold a {RECORD_FRAME}-byte JSON record and fit in memory",
                args.record_bytes
            ));
        }
        let window_ms = u64::from(args.window_seconds) * 1000;
        let samples = window_ms / args.sample_interval_ms.max(1);
        let Ok(samples @ 1..) = u32::try_from(samples) else {
            return invalid(format!(
                "--sample-interval-ms {} must give 1 to {} samples over the {} s window",
                args.sample_interval_ms,
                u32::MAX,
                args.window_seconds
            ));
        };
        let mut record = br#"{"body":""#.to_vec();
        record.resize(record_bytes - 2, b'x');
        record.extend_from_slice(br#""}"#);
        Ok(Self {
            load: BenchLoad {
                records_per_second: args.records_per_second,
                record_bytes: args.record_bytes,
                sources: args.sources,
            },
            record: Arc::new(record),
            warmup_seconds: args.warmup_seconds,
            window_seconds: args.window_seconds,
            interval: Duration::from_millis(args.sample_interval_ms),
            samples,
        })
    }
}

/// The producers feeding the source and the drain emptying the sink.
struct Load {
    producers: Vec<JoinHandle<()>>,
    drain: JoinHandle<()>,
    stop_producers: CancellationToken,
    stop_drain: CancellationToken,
}

impl Load {
    /// Start one producer per source, each offering an even share of the rate.
    fn start(plan: &Plan, bench: &BenchPipeline) -> Self {
        let stop_producers = CancellationToken::new();
        let stop_drain = CancellationToken::new();
        let sources = u64::from(plan.load.sources);
        let (share, extra) = (
            plan.load.records_per_second / sources,
            plan.load.records_per_second % sources,
        );
        let producers = (0..sources)
            .map(|index| {
                let rate = share + u64::from(index < extra);
                tokio::spawn(produce(
                    Arc::clone(&bench.source),
                    rate,
                    Arc::clone(&plan.record),
                    stop_producers.clone(),
                ))
            })
            .collect();
        let drain = tokio::spawn(drain(Arc::clone(&bench.sink), stop_drain.clone()));
        Self {
            producers,
            drain,
            stop_producers,
            stop_drain,
        }
    }

    /// Stop offering records, leaving the drain running.
    fn stop_producers(&self) {
        self.stop_producers.cancel();
    }

    /// Stop the producers and the drain and wait for both.
    async fn stop(self) -> Result<(), CliError> {
        self.stop_producers.cancel();
        self.stop_drain.cancel();
        for task in self.producers.into_iter().chain([self.drain]) {
            task.await
                .map_err(|e| CliError::Service(format!("a bench load task failed: {e}")))?;
        }
        Ok(())
    }
}

/// Offer `record` to `source` at `rate` a second until `stop`, catching up
/// after a tick that ran late and waiting while the source is full.
async fn produce(
    source: Arc<MemoryTransport>,
    rate: u64,
    record: Arc<Vec<u8>>,
    stop: CancellationToken,
) {
    let start = Instant::now();
    let mut sent: u64 = 0;
    let mut tick = tokio::time::interval(PRODUCER_TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = tick.tick() => {}
        }
        let owed = u128::from(rate) * start.elapsed().as_nanos() / 1_000_000_000;
        let due = u64::try_from(owed).unwrap_or(u64::MAX);
        while sent < due {
            tokio::select! {
                () = stop.cancelled() => return,
                offered = source.inject(None, record.to_vec()) => {
                    if offered.is_err() {
                        return;
                    }
                }
            }
            sent += 1;
        }
    }
}

/// Take and drop whatever reaches `sink` until `stop` or the sink closes.
async fn drain(sink: Arc<MemoryTransport>, stop: CancellationToken) {
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            received = sink.recv(DRAIN_BATCH) => {
                if received.is_err() {
                    return;
                }
            }
        }
    }
}

/// Wait out the warm-up, then read `usage` at the end of each interval of
/// the window.
async fn sample(usage: &UsageSource, plan: &Plan) -> Result<Samples, CliError> {
    tokio::time::sleep(Duration::from_secs(u64::from(plan.warmup_seconds))).await;
    let mut tick = tokio::time::interval(plan.interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;
    let mut samples = Samples::default();
    for _ in 0..plan.samples {
        tick.tick().await;
        let bytes = usage.read().ok_or_else(|| {
            CliError::Service(format!(
                "the {} memory figure stopped reading partway through the window",
                usage.name()
            ))
        })?;
        samples.add(bytes);
    }
    Ok(samples)
}

/// The running lowest, highest and total of the samples, so a long window
/// holds no list of them.
#[derive(Debug, Default)]
struct Samples {
    min: u64,
    max: u64,
    sum: u128,
    count: u32,
}

impl Samples {
    fn add(&mut self, bytes: u64) {
        self.min = if self.count == 0 {
            bytes
        } else {
            self.min.min(bytes)
        };
        self.max = self.max.max(bytes);
        self.sum += u128::from(bytes);
        self.count = self.count.saturating_add(1);
    }

    fn into_profile(self, plan: &Plan, reading: MemoryReading) -> Result<MemoryProfile, CliError> {
        let avg_bytes = self
            .sum
            .checked_div(u128::from(self.count))
            .and_then(|avg| u64::try_from(avg).ok())
            .unwrap_or(0);
        let profile = MemoryProfile {
            min_bytes: self.min,
            avg_bytes,
            max_bytes: self.max,
            load: plan.load.clone(),
            warmup_seconds: plan.warmup_seconds,
            window_seconds: plan.window_seconds,
            samples: self.count,
            reading,
        };
        match profile.fault() {
            None => Ok(profile),
            Some((field, reason)) => Err(CliError::Service(format!(
                "the samples make no profile: {field} {reason}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{CommonArgs, ServiceRuntime, StandardCommand, VersionInfo, run_app};
    use crate::transport::TransportSender;

    /// A service whose bench pipeline forwards every record from the source
    /// to the sink, or that has no bench pipeline at all.
    struct ForwardApp {
        common: CommonArgs,
        command: StandardCommand,
        has_pipeline: bool,
    }

    impl ServiceApp for ForwardApp {
        type Config = ();

        fn name(&self) -> &'static str {
            "forward-app"
        }
        fn env_prefix(&self) -> &'static str {
            "FORWARD_APP"
        }
        fn version_info(&self) -> VersionInfo {
            VersionInfo::new("forward-app", "1.2.3")
        }
        fn common_args(&self) -> &CommonArgs {
            &self.common
        }
        fn command(&self) -> Option<&StandardCommand> {
            Some(&self.command)
        }
        fn load_config(&self, _path: Option<&str>) -> Result<(), CliError> {
            Ok(())
        }
        fn run_service(
            &self,
            _config: (),
            _runtime: ServiceRuntime,
        ) -> impl std::future::Future<Output = Result<(), CliError>> + Send {
            std::future::ready(Ok(()))
        }
        fn bench_pipeline(
            &self,
            _config: (),
            runtime: ServiceRuntime,
            bench: BenchPipeline,
        ) -> Option<impl std::future::Future<Output = Result<(), CliError>> + Send> {
            self.has_pipeline.then(move || forward(runtime, bench))
        }
    }

    async fn forward(runtime: ServiceRuntime, bench: BenchPipeline) -> Result<(), CliError> {
        let failed = |e: String| CliError::Service(e);
        loop {
            let batch = tokio::select! {
                () = runtime.shutdown.cancelled() => return Ok(()),
                batch = bench.source.recv(500) => batch.map_err(|e| failed(e.to_string()))?,
            };
            let sent = bench.sink.send_batch(&batch.records).await;
            if sent.is_fatal() {
                return Err(failed(format!("{sent:?}")));
            }
            bench
                .source
                .commit(&batch.commit_tokens)
                .await
                .map_err(|e| failed(e.to_string()))?;
        }
    }

    fn bench_args(output: &std::path::Path) -> BenchMemoryArgs {
        BenchMemoryArgs {
            records_per_second: 2000,
            record_bytes: 512,
            sources: 2,
            warmup_seconds: 0,
            window_seconds: 2,
            sample_interval_ms: 100,
            output: output.to_str().expect("a UTF-8 tempdir").to_owned(),
        }
    }

    fn app(output: &std::path::Path, has_pipeline: bool) -> ForwardApp {
        ForwardApp {
            common: CommonArgs {
                config: None,
                log_level: None,
                log_format: None,
                metrics_addr: Some("127.0.0.1:0".into()),
                verbose: false,
                quiet: true,
            },
            command: StandardCommand::BenchMemory(bench_args(output)),
            has_pipeline,
        }
    }

    /// A short bench over a forwarding pipeline writes a profile whose
    /// figures rise from min to max above zero, with the load it ran under.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_memory_writes_a_profile_of_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("memory-profile.json");
        run_app(app(&output, true)).await.expect("the bench runs");

        let json = tokio::fs::read_to_string(&output).await.unwrap();
        let profile: MemoryProfile = serde_json::from_str(&json).unwrap();
        assert!(
            0 < profile.min_bytes
                && profile.min_bytes <= profile.avg_bytes
                && profile.avg_bytes <= profile.max_bytes,
            "{profile:?}"
        );
        assert_eq!(profile.samples, 20);
        assert_eq!(
            profile.load,
            BenchLoad {
                records_per_second: 2000,
                record_bytes: 512,
                sources: 2,
            }
        );
        assert_eq!((profile.warmup_seconds, profile.window_seconds), (0, 2));
        assert_eq!(
            Some(profile.reading),
            memory_reading(&UsageSource::detect())
        );
    }

    /// An app with no bench pipeline exits non-zero and writes no profile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bench_memory_without_a_bench_pipeline_exits_non_zero() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("memory-profile.json");
        let err = run_app(app(&output, false))
            .await
            .expect_err("no bench pipeline");
        assert!(err.to_string().contains("has no bench pipeline"), "{err}");
        assert!(!tokio::fs::try_exists(&output).await.unwrap());
    }

    /// A plan whose load or sampling cannot be run is rejected at its flag,
    /// and every record is the size asked for.
    #[test]
    fn a_bench_plan_rejects_a_load_it_cannot_run() {
        let base = bench_args(std::path::Path::new("profile.json"));
        let plan = Plan::from_args(&base).unwrap();
        assert_eq!(plan.record.len(), 512);
        assert!(serde_json::from_slice::<serde_json::Value>(&plan.record).is_ok());

        let cases: [(fn(&mut BenchMemoryArgs), &str); 5] = [
            (|a| a.sources = 0, "--sources"),
            (|a| a.sources = 2001, "--sources"),
            (|a| a.record_bytes = 10, "--record-bytes"),
            (|a| a.sample_interval_ms = 2001, "--sample-interval-ms"),
            (|a| a.window_seconds = 0, "--sample-interval-ms"),
        ];
        for (change, flag) in cases {
            let mut args = base.clone();
            change(&mut args);
            let err = Plan::from_args(&args).expect_err(flag);
            assert!(err.to_string().contains(flag), "{flag}: {err}");
        }
    }
}
