// Project:   scalo
// File:      src/clock.rs
// Purpose:   System-clock epoch milliseconds from std
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The system clock as Unix epoch milliseconds, for message and DLQ stamps.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch. A clock set before 1970 reads as 0.
pub(crate) fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
