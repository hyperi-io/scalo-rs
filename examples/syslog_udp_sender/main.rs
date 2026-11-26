// Project:   scalo
// File:      examples/syslog_udp_sender/main.rs
// Purpose:   Send a UDP syslog flood at any host:port and print the send rate
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Floods one address with RFC 3164 datagrams as fast as the senders can, so
//! the same traffic can be aimed at any UDP syslog listener.
//!
//! ```sh
//! cargo run --release --features transport-syslog --example syslog_udp_sender -- 127.0.0.1:5514
//! ```
//!
//! `SYSLOG_UDP_RATE_SENDERS` (default 4), `SYSLOG_UDP_RATE_SECS` (default 5)
//! and `SYSLOG_UDP_RATE_BYTES` (default 200) set the sender threads, the length
//! of the run and the datagram size. It prints the datagrams sent and the rate,
//! and what arrived is the listener's to say.
#![allow(clippy::cast_precision_loss, reason = "rates print as f64")]

mod flood;

use std::net::ToSocketAddrs;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use flood::{Flood, datagram, setting};

fn main() -> ExitCode {
    let Some(target) = std::env::args().nth(1) else {
        eprintln!("usage: syslog_udp_sender <host:port>");
        return ExitCode::from(2);
    };
    let to = match target.to_socket_addrs().map(|mut addrs| addrs.next()) {
        Ok(Some(to)) => to,
        Ok(None) => {
            eprintln!("syslog_udp_sender: {target} resolves to no address");
            return ExitCode::from(2);
        }
        Err(e) => {
            eprintln!("syslog_udp_sender: {target}: {e}");
            return ExitCode::from(2);
        }
    };
    let senders = setting("SYSLOG_UDP_RATE_SENDERS", 4);
    let secs = setting("SYSLOG_UDP_RATE_SECS", 5);
    let bytes = usize::try_from(setting("SYSLOG_UDP_RATE_BYTES", 200)).unwrap_or(200);

    let start = Instant::now();
    let flood = Flood::start(to, senders, &datagram(bytes));
    std::thread::sleep(Duration::from_secs(secs));
    let sent = flood.finish();
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "syslog_udp_sender: {sent} datagrams sent to {to} in {elapsed:.2} s, {:.0}/s, \
         {senders} senders, {bytes} bytes each",
        sent as f64 / elapsed
    );
    ExitCode::SUCCESS
}
