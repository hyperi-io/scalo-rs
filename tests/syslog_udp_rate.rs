// Project:   scalo
// File:      tests/syslog_udp_rate.rs
// Purpose:   Syslog UDP receive rate on loopback, in datagrams per second
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! How many datagrams a second the syslog receiver's UDP listener hands to
//! `recv` on loopback, with senders flooding it as fast as they can.
//!
//! `#[ignore]` by default, as it is a measurement and not a check. Run it in
//! release, on a quiet host:
//!
//! ```sh
//! cargo nextest run --release --features transport-syslog --run-ignored only \
//!     --no-capture -E 'test(syslog_udp_rate)'
//! ```
//!
//! `SYSLOG_UDP_RATE_SENDERS` (default 4), `SYSLOG_UDP_RATE_SECS` (default 5)
//! and `SYSLOG_UDP_RATE_BYTES` (default 200) set the senders, the length of the
//! run and the datagram size.
#![cfg(feature = "transport-syslog")]
#![allow(clippy::cast_precision_loss, reason = "rates print as f64")]

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use scalo::transport::syslog::{SyslogListenConfig, SyslogReceiver};
use scalo::transport::{TransportBase, TransportReceiver};

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: cargo nextest run --release --features transport-syslog --run-ignored only --no-capture -E 'test(syslog_udp_rate)'"]
async fn syslog_udp_rate() {
    let senders = setting("SYSLOG_UDP_RATE_SENDERS", 4);
    let secs = setting("SYSLOG_UDP_RATE_SECS", 5);
    let bytes = usize::try_from(setting("SYSLOG_UDP_RATE_BYTES", 200)).unwrap();
    let receiver = SyslogReceiver::new(&SyslogListenConfig {
        udp_address: Some("127.0.0.1:0".into()),
        recv_buffer_size: 100_000,
        ..SyslogListenConfig::default()
    })
    .await
    .unwrap();
    let to = receiver.udp_addr().unwrap();
    let mut line = b"<14>Nov 26 12:00:00 host app[1]: ".to_vec();
    line.resize(bytes.max(line.len()), b'x');
    let line: Arc<[u8]> = line.into();

    let stop = Arc::new(AtomicBool::new(false));
    let sent_total = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..senders)
        .map(|_| {
            let (stop, total, line) = (
                Arc::clone(&stop),
                Arc::clone(&sent_total),
                Arc::clone(&line),
            );
            std::thread::spawn(move || {
                let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
                socket.connect(to).unwrap();
                let mut count = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    if socket.send(&line).is_ok() {
                        count += 1;
                    }
                }
                total.fetch_add(count, Ordering::Relaxed);
            })
        })
        .collect();

    let run = Duration::from_secs(secs);
    let start = Instant::now();
    let mut delivered = 0_u64;
    while start.elapsed() < run {
        delivered += receiver.recv(10_000).await.unwrap().records.len() as u64;
    }
    let elapsed = start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    for thread in threads {
        thread.join().unwrap();
    }
    let sent = sent_total.load(Ordering::Relaxed);
    let rate = delivered as f64 / elapsed;
    println!(
        "syslog_udp_rate: {rate:.0} datagrams/s received ({delivered} in {elapsed:.2} s), \
         {:.0}/s sent by {senders} senders, {bytes} bytes each",
        sent as f64 / elapsed
    );
    receiver.close().await.unwrap();
    assert!(delivered > 0, "no datagram arrived");
}
