// Project:   scalo
// File:      tests/drain_deadline.rs
// Purpose:   A stuck drain ends at its deadline or a second signal, read off a real process
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A service whose drain never finishes exits at `shutdown.drain_deadline_secs`,
//! or at once on a second SIGTERM or Ctrl-C, with its held records left
//! unacknowledged. Read off a real process the test signals with `kill`.
//!
//! The test binary re-runs itself as that process. A test finding
//! [`PROBE_CONFIG`] in its environment is the re-run: it runs the probe service,
//! which only the signal handler ends.

#![cfg(all(unix, feature = "cli-service", feature = "transport"))]

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use clap::Parser;
use scalo::cli::{CliError, CommonArgs, ServiceApp, ServiceRuntime, VersionInfo};
use scalo::transport::ack::Tickets;

/// The probe's settings file, set only in the re-run.
const PROBE_CONFIG: &str = "DRAIN_PROBE_CONFIG";

/// Records the probe holds and never releases.
const HELD: u64 = 3;

#[derive(Parser)]
#[command(name = "drain-probe")]
struct ProbeCli {
    #[command(flatten)]
    common: CommonArgs,
}

/// A service that holds [`HELD`] records and, once shutdown begins, drains
/// for ever.
struct ProbeApp {
    cli: ProbeCli,
}

impl ServiceApp for ProbeApp {
    type Config = ();

    fn name(&self) -> &'static str {
        "drain-probe"
    }

    fn env_prefix(&self) -> &'static str {
        "DRAIN_PROBE"
    }

    fn version_info(&self) -> VersionInfo {
        VersionInfo::new("drain-probe", "1.2.3")
    }

    fn common_args(&self) -> &CommonArgs {
        &self.cli.common
    }

    fn load_config(&self, _path: Option<&str>) -> Result<(), CliError> {
        Ok(())
    }

    async fn run_service(&self, _config: (), runtime: ServiceRuntime) -> Result<(), CliError> {
        let tickets = Tickets::new("drain-probe", 1 << 20);
        let answer_by = tokio::time::Instant::now() + Duration::from_secs(3600);
        let _held: Vec<_> = (0..HELD)
            .map(|_| tickets.admit(100, answer_by).expect("the probe admits"))
            .collect();
        println!("serving");
        runtime.shutdown.cancelled().await;
        println!("draining");
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// In the re-run, run the probe and say so. Anywhere else, do nothing.
fn ran_as_probe() -> bool {
    let Ok(settings) = std::env::var(PROBE_CONFIG) else {
        return false;
    };
    let command_line = [
        "drain-probe",
        "--config",
        &settings,
        "--log-format",
        "json",
        "--metrics-addr",
        "127.0.0.1:0",
    ];
    let app = ProbeApp {
        cli: ProbeCli::try_parse_from(command_line).expect("the probe's own arguments parse"),
    };
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime for the probe")
        .block_on(scalo::cli::run_app(app));
    panic!("only the signal handler ends the probe, but run_app returned {result:?}");
}

/// Re-run this test binary as the probe, with `deadline_secs` as its drain
/// deadline, and wait for it to say it is serving.
fn start_probe(dir: &tempfile::TempDir, deadline_secs: u64) -> (Child, mpsc::Receiver<String>) {
    let settings = dir.path().join("settings.yaml");
    std::fs::write(
        &settings,
        format!(
            "shutdown:\n  drain_deadline_secs: {deadline_secs}\nmetrics:\n  otel:\n    enabled: false\n\
             otel_tracing:\n  enabled: false\nversion_check:\n  enabled: false\n"
        ),
    )
    .expect("write settings.yaml");
    let mut child = Command::new(std::env::current_exe().expect("the test binary"))
        .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(PROBE_CONFIG, &settings)
        .env("PRESTOP_DELAY_SECS", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the test binary re-runs");
    let stdout = child.stdout.take().expect("piped stdout");
    let (lines, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    await_line(&rx, "serving");
    (child, rx)
}

/// Wait up to 30 s for the probe to print `want`, at the end of a line because
/// the harness prints the test's name on the line the first output lands on.
fn await_line(lines: &mpsc::Receiver<String>, want: &str) {
    let until = Instant::now() + Duration::from_secs(30);
    while let Some(left) = until.checked_duration_since(Instant::now()) {
        match lines.recv_timeout(left) {
            Ok(line) if line.ends_with(want) => return,
            Ok(_) => {}
            Err(e) => panic!("the probe never printed {want:?}: {e}"),
        }
    }
    panic!("the probe never printed {want:?}");
}

fn signal(child: &Child, name: &str) {
    let sent = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(child.id().to_string())
        .status()
        .expect("kill runs");
    assert!(sent.success(), "kill -{name} failed");
}

/// Wait up to `bound` for the probe to exit, killing it past that.
fn await_exit(child: &mut Child, bound: Duration) -> Option<ExitStatus> {
    let until = Instant::now() + bound;
    while Instant::now() < until {
        if let Some(status) = child.try_wait().expect("the probe's status") {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    None
}

const TEST: &str = "a_stuck_drain_ends_at_its_deadline_or_a_second_signal";

#[test]
fn a_stuck_drain_ends_at_its_deadline_or_a_second_signal() {
    if ran_as_probe() {
        return;
    }
    let deadline = Duration::from_secs(2);
    let at_once = Duration::from_secs(2);
    // Name, drain deadline, the second signal, the WARN line's message, how
    // long after the last signal the probe may take to exit.
    let cases = [
        (
            "the deadline",
            deadline.as_secs(),
            None,
            "drain deadline",
            deadline + at_once,
        ),
        (
            "a second SIGTERM",
            600,
            Some("TERM"),
            "second shutdown signal",
            at_once,
        ),
        (
            "Ctrl-C after SIGTERM",
            600,
            Some("INT"),
            "second shutdown signal",
            at_once,
        ),
    ];
    for (name, deadline_secs, second, message, bound) in cases {
        let dir = tempfile::tempdir().expect("config dir");
        let (mut child, lines) = start_probe(&dir, deadline_secs);

        let mut last = Instant::now();
        signal(&child, "TERM");
        if let Some(second) = second {
            await_line(&lines, "draining");
            last = Instant::now();
            signal(&child, second);
        }
        let status = await_exit(&mut child, bound);
        let took = last.elapsed();
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stderr.take() {
            pipe.read_to_string(&mut stderr)
                .expect("the probe's stderr");
        }

        assert_eq!(
            status.and_then(|s| s.code()),
            Some(0),
            "{name}: took {took:?}\n{stderr}"
        );
        if second.is_none() {
            assert!(
                took >= deadline,
                "{name}: exited {took:?} after SIGTERM, before the deadline"
            );
        }
        let warned: Vec<&str> = stderr.lines().filter(|l| l.contains(message)).collect();
        assert_eq!(warned.len(), 1, "{name}: one WARN line\n{stderr}");
        assert!(
            warned[0].contains(&format!("\"held_records\":{HELD}")),
            "{name}: the WARN line counts the {HELD} records left unacknowledged: {}",
            warned[0]
        );
    }
}
