// Project:   scalo
// File:      tests/spool_crash.rs
// Purpose:   A record a spool acknowledged survives the process being killed
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A record `Spool::push` acknowledged is still there after the process is
//! killed with SIGKILL, with no drop, no flush and no clean shutdown.
//!
//! The test binary re-runs itself as the writer: a test finding [`WRITER`] in
//! its environment pushes records to the spool at that path and prints each
//! sequence number once its push has returned. The test reads the numbers,
//! kills the writer part way, reopens the spool and reads the records back.
//!
//! This covers a process kill. A power loss is what `Spool::sync` and
//! `sync_on_push` are for, and no test can cut the power.

#![cfg(all(feature = "spool", unix))]

use std::io::{BufRead as _, BufReader, Write as _};
use std::process::{Command, Stdio};

use scalo::spool::{Spool, SpoolConfig};

/// The spool path, set only in the re-run.
const WRITER: &str = "SPOOL_CRASH_WRITER";

/// Set in the re-run to push with `sync_on_push`.
const WRITER_SYNC: &str = "SPOOL_CRASH_WRITER_SYNC";

/// Pushes the test waits to see acknowledged before it kills the writer.
const ACKED: usize = 300;

/// The writer stops by itself here, so a test that fails to kill it ends.
const WRITER_LIMIT: usize = 100_000;

const TEST: &str = "acknowledged_records_survive_a_killed_process";

fn record(seq: usize) -> Vec<u8> {
    format!("record-{seq}").into_bytes()
}

/// In the re-run, push records and print each sequence number after its push
/// returns, then return true. Anywhere else, do nothing.
fn ran_as_writer() -> bool {
    let Ok(path) = std::env::var(WRITER) else {
        return false;
    };
    let sync = std::env::var_os(WRITER_SYNC).is_some();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let mut spool = Spool::open(SpoolConfig::new(path).sync_on_push(sync))
            .await
            .expect("open the spool");
        let mut out = std::io::stdout().lock();
        for seq in 0..WRITER_LIMIT {
            spool.push(&record(seq)).await.expect("push");
            writeln!(out, "{seq}").expect("report the push");
            out.flush().expect("flush the report");
        }
    });
    true
}

/// Run the writer until `ACKED` pushes are acknowledged, kill it, and return
/// how many it reported.
fn run_and_kill_writer(path: &std::path::Path, sync: bool) -> usize {
    let mut writer = Command::new(std::env::current_exe().expect("test binary"));
    writer
        .args(["--exact", TEST, "--nocapture"])
        .env(WRITER, path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if sync {
        writer.env(WRITER_SYNC, "1");
    }
    let mut child = writer.spawn().expect("spawn the writer");
    let stdout = child.stdout.take().expect("the writer's stdout");

    // The test harness prints its own lines too, so only a bare number counts.
    let mut acked = 0;
    for line in BufReader::new(stdout).lines() {
        if line.expect("read the writer").parse::<usize>().is_ok() {
            acked += 1;
        }
        if acked == ACKED {
            break;
        }
    }
    child.kill().expect("SIGKILL the writer");
    child.wait().expect("reap the writer");
    acked
}

#[test]
fn acknowledged_records_survive_a_killed_process() {
    if ran_as_writer() {
        return;
    }
    for sync in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("spool");
        let acked = run_and_kill_writer(&path, sync);
        assert_eq!(acked, ACKED, "sync_on_push {sync}: the writer ended early");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let mut spool = Spool::open(SpoolConfig::new(&path))
                .await
                .expect("a killed writer's spool opens");
            let mut recovered = Vec::new();
            while let Some(data) = spool.pop_front().await.expect("pop") {
                recovered.push(data);
            }
            assert!(
                recovered.len() >= acked,
                "sync_on_push {sync}: {} of {acked} acknowledged records survived",
                recovered.len()
            );
            for (seq, data) in recovered.iter().enumerate() {
                assert_eq!(data, &record(seq), "sync_on_push {sync}: record {seq}");
            }
        });
    }
}
