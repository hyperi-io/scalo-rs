// Project:   scalo
// File:      tests/logger_exit_flush.rs
// Purpose:   Queued log lines reach stderr when the process exits or panics
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The logger writes from a queue, so the lines logged just before an exit or
//! a panic are the ones most at risk. Each test runs itself again as a child
//! process and reads the child's stderr slowly, so lines are still queued when
//! the child ends.

#![cfg(all(unix, feature = "logger"))]

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::Duration;

use scalo::logger::{self, LogFormat, LoggerOptions};

/// Tells a re-run of this binary which ending to act out.
const CHILD: &str = "LOGGER_EXIT_FLUSH_CHILD";

/// Logging threads.
const THREADS: usize = 8;

/// Lines in all, under the queue's capacity so none is dropped.
const LINES: usize = 8_000;

fn log_lines() {
    logger::setup(LoggerOptions {
        format: LogFormat::Text,
        add_source: false,
        ..LoggerOptions::default()
    })
    .expect("logger setup");
    std::thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                for i in 0..LINES / THREADS {
                    tracing::info!(i, "queued line");
                }
            });
        }
    });
}

/// Run `test` in a child process told to end by `ending`, and return its
/// stderr, read a chunk at a time with a pause between reads.
fn child_stderr(test: &str, ending: &str) -> String {
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", test, "--nocapture", "--test-threads", "1"])
        .env(CHILD, ending)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run the child");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let (mut out, mut chunk) = (Vec::new(), [0_u8; 4096]);
    loop {
        let read = stderr.read(&mut chunk).expect("read the child's stderr");
        if read == 0 {
            break;
        }
        out.extend_from_slice(&chunk[..read]);
        std::thread::sleep(Duration::from_millis(2));
    }
    child.wait().expect("the child ends");
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn lines_queued_at_process_exit_reach_stderr() {
    if std::env::var(CHILD).as_deref() == Ok("exit") {
        log_lines();
        std::process::exit(0);
    }
    let stderr = child_stderr("lines_queued_at_process_exit_reach_stderr", "exit");
    assert_eq!(stderr.matches("queued line").count(), LINES, "{stderr}");
}

#[test]
fn lines_queued_before_a_panic_come_before_its_message() {
    if std::env::var(CHILD).as_deref() == Ok("panic") {
        log_lines();
        panic!("ending on a panic");
    }
    let stderr = child_stderr(
        "lines_queued_before_a_panic_come_before_its_message",
        "panic",
    );
    assert_eq!(stderr.matches("queued line").count(), LINES, "{stderr}");
    let last_line = stderr.rfind("queued line").expect("a logged line");
    let message = stderr.find("ending on a panic").expect("the panic message");
    assert!(last_line < message, "every line precedes the panic message");
}
