// Project:   scalo
// File:      benches/delivery_path/host.rs
// Purpose:   Host readings for the bench: load average, peak RSS, process CPU time
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Linux procfs readings. Each is `None` or `"unknown"` where procfs is absent.

use std::sync::OnceLock;

/// The 1, 5 and 15 minute load averages, as procfs prints them.
pub fn load_average() -> String {
    std::fs::read_to_string("/proc/loadavg").map_or_else(
        |_| "unknown".to_string(),
        |text| {
            text.split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" ")
        },
    )
}

/// Peak resident set of this process, in bytes: `VmHWM`.
pub fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

/// User plus system CPU time of this process, every thread it has had
/// included, in nanoseconds: `utime + stime` from `/proc/self/stat`.
pub fn cpu_ns() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the command name, which is in parentheses and may hold spaces.
    let rest = &stat[stat.rfind(')')? + 2..];
    let mut fields = rest.split_whitespace();
    // utime and stime are fields 14 and 15, so 12th and 13th after the name.
    let utime: u64 = fields.nth(11)?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some((utime + stime) * (1_000_000_000 / ticks_per_second()?))
}

/// Clock ticks per second, as `getconf CLK_TCK` reports them.
fn ticks_per_second() -> Option<u64> {
    static TICKS: OnceLock<Option<u64>> = OnceLock::new();
    *TICKS.get_or_init(|| {
        let out = std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .ok()
            .filter(|t| *t > 0)
    })
}
