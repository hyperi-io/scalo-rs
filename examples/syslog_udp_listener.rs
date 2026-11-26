// Project:   scalo
// File:      examples/syslog_udp_listener.rs
// Purpose:   Count the datagrams the syslog UDP listener receives
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Binds the syslog receiver's UDP listener, drains `recv` and prints the
//! datagrams received a second. The receiving half of the
//! `syslog_udp_sender` example.
//!
//! ```sh
//! cargo run --release --features transport-syslog --example syslog_udp_listener -- 0.0.0.0:5514
//! ```
//!
//! It exits once `SYSLOG_UDP_LISTEN_QUIET_SECS` (default 2) pass with no
//! datagram, or `SYSLOG_UDP_LISTEN_IDLE_SECS` (default 30) with none at all.
//! The rate runs from the first batch to the last. `SYSLOG_UDP_RECV_BUFFER_BYTES` sets
//! `SO_RCVBUF` and `SYSLOG_UDP_QUEUE` the records queued for `recv`, each at the
//! listener's default where unset.
#![allow(clippy::cast_precision_loss, reason = "rates print as f64")]

use std::process::ExitCode;
use std::time::{Duration, Instant};

use scalo::transport::syslog::{SyslogListenConfig, SyslogReceiver};
use scalo::transport::{TransportBase, TransportReceiver};

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> ExitCode {
    let Some(bind) = std::env::args().nth(1) else {
        eprintln!("usage: syslog_udp_listener <ip:port>");
        return ExitCode::from(2);
    };
    let defaults = SyslogListenConfig::default();
    let config = SyslogListenConfig {
        udp_address: Some(bind),
        udp_recv_buffer_bytes: usize::try_from(setting(
            "SYSLOG_UDP_RECV_BUFFER_BYTES",
            defaults.udp_recv_buffer_bytes as u64,
        ))
        .unwrap_or(defaults.udp_recv_buffer_bytes),
        recv_buffer_size: usize::try_from(setting(
            "SYSLOG_UDP_QUEUE",
            defaults.recv_buffer_size as u64,
        ))
        .unwrap_or(defaults.recv_buffer_size),
        ..defaults
    };
    let receiver = match SyslogReceiver::new(&config).await {
        Ok(receiver) => receiver,
        Err(e) => {
            eprintln!("syslog_udp_listener: {e}");
            return ExitCode::FAILURE;
        }
    };
    let listening = receiver.udp_addr();
    println!("syslog_udp_listener: listening on {listening:?}");
    let quiet = Duration::from_secs(setting("SYSLOG_UDP_LISTEN_QUIET_SECS", 2));
    let idle = Duration::from_secs(setting("SYSLOG_UDP_LISTEN_IDLE_SECS", 30));

    let waiting = Instant::now();
    let mut total = 0_u64;
    let mut first: Option<(Instant, u64)> = None;
    let mut last = waiting;
    loop {
        let batch = match receiver.recv(10_000).await {
            Ok(batch) => batch,
            Err(e) => {
                eprintln!("syslog_udp_listener: recv: {e}");
                return ExitCode::FAILURE;
            }
        };
        let got = batch.records.len() as u64;
        if got > 0 {
            total += got;
            last = Instant::now();
            first.get_or_insert((last, got));
        } else {
            let (limit, silent) = match first {
                Some(_) => (quiet, last.elapsed()),
                None => (idle, waiting.elapsed()),
            };
            if silent >= limit {
                break;
            }
        }
    }
    let (elapsed, counted) = first.map_or((0.0, 0), |(at, head)| {
        (last.duration_since(at).as_secs_f64(), total - head)
    });
    println!(
        "syslog_udp_listener: {total} datagrams received, {counted} of them over {elapsed:.2} s \
         from the first batch to the last, {:.0}/s",
        if elapsed > 0.0 {
            counted as f64 / elapsed
        } else {
            0.0
        }
    );
    if let Err(e) = receiver.close().await {
        eprintln!("syslog_udp_listener: close: {e}");
    }
    ExitCode::SUCCESS
}
