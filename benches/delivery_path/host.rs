// Project:   scalo
// File:      benches/delivery_path/host.rs
// Purpose:   Host readings for the bench: load average, peak RSS, process CPU time
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Linux procfs readings. Each is `None` or `"unknown"` where procfs is absent.

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

/// Restart the kernel's peak-RSS counter for this process.
pub fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// Peak resident set since the last reset, in bytes.
pub fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

/// CPU time every thread of this process has run, in nanoseconds.
pub fn cpu_ns() -> Option<u64> {
    let mut total = 0u64;
    for task in std::fs::read_dir("/proc/self/task").ok()? {
        let path = task.ok()?.path().join("schedstat");
        // A thread that exits between the listing and the read is skipped.
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        total += text.split_whitespace().next()?.parse::<u64>().ok()?;
    }
    Some(total)
}
