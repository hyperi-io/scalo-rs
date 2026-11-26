// Project:   scalo
// File:      src/transport/syslog/receive/kernel_drops.rs
// Purpose:   Datagrams the kernel drops for the syslog UDP socket, from /proc/net/udp
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A UDP socket whose receive buffer is full loses datagrams in the kernel,
//! before any read sees them. Linux counts them per socket in the `drops`
//! column of `/proc/net/udp` and `/proc/net/udp6`, and this reads that count
//! for the listener's socket, found by its inode. Elsewhere it does nothing.

use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

/// Count the datagrams the kernel drops for `socket` in
/// `transport_dropped_total{reason="kernel"}`, read every 10 s until `shutdown`.
#[cfg(target_os = "linux")]
pub(super) fn watch(socket: &UdpSocket, shutdown: CancellationToken) {
    use std::os::fd::AsRawFd;
    use std::time::Duration;

    use crate::concurrency::PeriodicWorker;

    match linux::socket_inode(socket.as_raw_fd()) {
        Ok(inode) => {
            PeriodicWorker::spawn(
                linux::KernelDrops::new(inode),
                Duration::from_secs(10),
                shutdown,
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "syslog UDP: socket inode unreadable, kernel drops not counted");
        }
    }
}

/// Kernel drops are read on Linux only.
#[cfg(not(target_os = "linux"))]
pub(super) fn watch(_socket: &UdpSocket, _shutdown: CancellationToken) {}

#[cfg(target_os = "linux")]
mod linux {
    use std::os::unix::fs::MetadataExt;

    use crate::concurrency::{PeriodicTask, TickError};

    /// The inode of the socket open on `fd`, as `/proc/net/udp` names it.
    pub(super) fn socket_inode(fd: i32) -> std::io::Result<u64> {
        std::fs::metadata(format!("/proc/self/fd/{fd}")).map(|m| m.ino())
    }

    /// The `drops` of the socket with `inode` in a `/proc/net/udp` table.
    ///
    /// Columns: `sl local_address rem_address st tx_queue:rx_queue tr:tm->when
    /// retrnsmt uid timeout inode ref pointer drops`.
    pub(super) fn drops_in(table: &str, inode: u64) -> Option<u64> {
        table.lines().skip(1).find_map(|line| {
            let mut columns = line.split_whitespace();
            if columns.nth(9)?.parse::<u64>().ok()? != inode {
                return None;
            }
            columns.nth(2)?.parse().ok()
        })
    }

    /// The kernel's drop count for the socket with `inode`, or `None` once
    /// neither table lists it.
    pub(super) fn drops(inode: u64) -> Option<u64> {
        ["/proc/net/udp", "/proc/net/udp6"]
            .into_iter()
            .filter_map(|path| std::fs::read_to_string(path).ok())
            .find_map(|table| drops_in(&table, inode))
    }

    /// One socket's kernel drops, counted as they rise.
    pub(super) struct KernelDrops {
        inode: u64,
        seen: u64,
    }

    impl KernelDrops {
        /// The socket with `inode`, its count taken to start at 0.
        pub(super) const fn new(inode: u64) -> Self {
            Self { inode, seen: 0 }
        }

        /// The drops since the last call.
        pub(super) fn take(&mut self) -> u64 {
            let Some(drops) = drops(self.inode) else {
                return 0;
            };
            let rise = drops.saturating_sub(self.seen);
            self.seen = drops;
            rise
        }
    }

    impl PeriodicTask for KernelDrops {
        async fn tick(&mut self) -> Result<(), TickError> {
            let rise = self.take();
            if rise > 0 {
                metrics::counter!(
                    "transport_dropped_total",
                    "transport" => "syslog",
                    "reason" => "kernel"
                )
                .increment(rise);
            }
            Ok(())
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::os::fd::AsRawFd;

    use super::linux::{KernelDrops, drops_in, socket_inode};

    const TABLE: &str = "\
   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  191: 00000000:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4242 2 0000000000000000 17
  192: 00000000:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4243 2 0000000000000000 3
  193: 00000000:1234 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4244 2 0000000000000000 x
  194: short row
";

    /// Only the row of the inode asked for counts, sharing a port or not, and
    /// a row with no number or too few columns is none.
    #[test]
    fn the_drops_are_those_of_the_inode_asked_for() {
        // (inode, drops)
        let cases = [(4242, Some(17)), (4243, Some(3)), (4244, None), (9, None)];
        for (inode, want) in cases {
            assert_eq!(drops_in(TABLE, inode), want, "inode {inode}");
        }
    }

    /// A socket with the smallest buffer, flooded and never read, shows its
    /// drops in the live table under its own inode, each counted once.
    #[tokio::test]
    async fn a_flooded_socket_counts_its_drops_once() {
        let socket = super::super::udp::bind("127.0.0.1:0".parse().unwrap(), 1).unwrap();
        let to = socket.local_addr().unwrap();
        let mut drops = KernelDrops::new(socket_inode(socket.as_raw_fd()).unwrap());
        assert_eq!(drops.take(), 0, "an unread socket that nothing was sent to");
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        for _ in 0..500 {
            sender.send_to(&[b'x'; 512], to).unwrap();
        }
        assert!(drops.take() > 0, "no drops after flooding a tiny buffer");
        assert_eq!(drops.take(), 0, "the same drops counted twice");
    }
}
