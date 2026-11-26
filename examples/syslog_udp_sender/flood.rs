// Project:   scalo
// File:      examples/syslog_udp_sender/flood.rs
// Purpose:   UDP syslog flood: sender threads that send as fast as they can
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Sender threads for the syslog UDP rate measurements. Shared by the
//! `syslog_udp_sender` example and the `syslog_udp_rate` test, so both send the
//! same datagrams.

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

/// `name` from the environment as a number, or `default` where it is unset or
/// not one.
pub fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// An RFC 3164 line padded with `x` to `bytes`, or its own length where that is
/// longer.
pub fn datagram(bytes: usize) -> Arc<[u8]> {
    let mut line = b"<14>Nov 26 12:00:00 host app[1]: ".to_vec();
    line.resize(bytes.max(line.len()), b'x');
    line.into()
}

/// Sender threads sending `line` to one address until `finish`.
pub struct Flood {
    stop: Arc<AtomicBool>,
    sent: Arc<AtomicU64>,
    threads: Vec<JoinHandle<()>>,
}

impl Flood {
    /// Start `senders` threads, each with its own socket, sending `line` to `to`.
    ///
    /// # Panics
    ///
    /// A thread panics where its socket cannot be bound or connected.
    pub fn start(to: SocketAddr, senders: u64, line: &Arc<[u8]>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let sent = Arc::new(AtomicU64::new(0));
        let bind = if to.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let threads = (0..senders)
            .map(|_| {
                let (stop, sent, line) = (Arc::clone(&stop), Arc::clone(&sent), Arc::clone(line));
                std::thread::spawn(move || {
                    let socket = UdpSocket::bind(bind).unwrap();
                    socket.connect(to).unwrap();
                    let mut count = 0_u64;
                    while !stop.load(Ordering::Relaxed) {
                        if socket.send(&line).is_ok() {
                            count += 1;
                        }
                    }
                    sent.fetch_add(count, Ordering::Relaxed);
                })
            })
            .collect();
        Self {
            stop,
            sent,
            threads,
        }
    }

    /// Stop the senders and return how many datagrams they sent.
    pub fn finish(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads {
            thread.join().unwrap();
        }
        self.sent.load(Ordering::Relaxed)
    }
}
