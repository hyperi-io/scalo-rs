// Project:   scalo
// File:      src/config/watch.rs
// Purpose:   Config-change trigger source (file mtime poll, periodic, SIGHUP)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The one place a service learns its config file changed.
//!
//! [`ConfigWatch`] owns the three triggers a data-plane service reacts to --
//! a config-file mtime change, a periodic tick, and SIGHUP -- and hands back
//! which one fired. Two callers share it:
//!
//! - `ConfigReloader` (`config-reload` feature), which reloads and
//!   validates on each trigger and updates its `SharedConfig<T>`.
//! - `IdleGate` (`lifecycle` feature), which parks a service that has
//!   no work to do until a change might give it some.
//!
//! ```rust,no_run
//! # use std::path::PathBuf;
//! # use std::time::Duration;
//! use scalo::config::watch::{ConfigTrigger, ConfigWatch};
//!
//! # async fn example() {
//! let mut watch = ConfigWatch::new(Some(PathBuf::from("config.yaml")), Duration::from_secs(5))
//!     .with_sighup(true);
//! match watch.next_trigger().await {
//!     ConfigTrigger::FileChanged => { /* re-read the file */ }
//!     ConfigTrigger::Periodic | ConfigTrigger::Sighup => { /* re-read anyway */ }
//! }
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Which trigger fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigTrigger {
    /// The watched file's mtime moved forward.
    FileChanged,
    /// The periodic interval elapsed.
    Periodic,
    /// SIGHUP was delivered (Unix only).
    Sighup,
}

/// Trigger source for config changes: file mtime polling, a periodic tick and
/// SIGHUP, in any combination.
///
/// [`next_trigger`](Self::next_trigger) resolves only on a REAL change: a poll
/// tick whose mtime is unchanged loops internally rather than waking the
/// caller.
pub struct ConfigWatch {
    path: Option<PathBuf>,
    last_modified: Option<SystemTime>,
    poll_timer: Option<tokio::time::Interval>,
    periodic_timer: Option<tokio::time::Interval>,
    #[cfg(unix)]
    sighup: Option<tokio::signal::unix::Signal>,
}

impl ConfigWatch {
    /// Watch `path` (when given) every `poll_interval`. SIGHUP is off and the
    /// periodic tick is disabled until asked for.
    ///
    /// The initial mtime is read lazily on the first poll tick, so construction
    /// never blocks and never touches the filesystem.
    #[must_use]
    pub fn new(path: Option<PathBuf>, poll_interval: Duration) -> Self {
        let poll_timer = path.as_ref().map(|_| tokio::time::interval(poll_interval));
        Self {
            path,
            last_modified: None,
            poll_timer,
            periodic_timer: None,
            #[cfg(unix)]
            sighup: None,
        }
    }

    /// Seed the baseline mtime so the first change AFTER now is the first
    /// trigger. Without this the first poll of an existing file reads `None ->
    /// Some` and counts as a change.
    pub async fn prime(&mut self) {
        if let Some(ref path) = self.path {
            self.last_modified = file_mtime(path).await;
        }
    }

    /// Fire [`ConfigTrigger::Periodic`] every `interval`. [`Duration::ZERO`]
    /// disables it.
    #[must_use]
    pub fn with_periodic(mut self, interval: Duration) -> Self {
        self.periodic_timer = (interval > Duration::ZERO).then(|| tokio::time::interval(interval));
        self
    }

    /// Fire [`ConfigTrigger::Sighup`] on SIGHUP. Ignored off Unix.
    ///
    /// # Panics
    /// Panics if the process cannot register a SIGHUP handler.
    #[must_use]
    pub fn with_sighup(mut self, enabled: bool) -> Self {
        #[cfg(unix)]
        {
            self.sighup = enabled.then(|| {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                    .expect("failed to register SIGHUP handler")
            });
        }
        #[cfg(not(unix))]
        let _ = enabled;
        self
    }

    /// Whether any trigger at all is enabled. With none, `next_trigger` never
    /// resolves.
    #[must_use]
    pub fn is_armed(&self) -> bool {
        #[cfg(unix)]
        let sighup = self.sighup.is_some();
        #[cfg(not(unix))]
        let sighup = false;
        self.poll_timer.is_some() || self.periodic_timer.is_some() || sighup
    }

    /// Wait for the next REAL trigger.
    ///
    /// A file poll that finds the mtime unmoved does not resolve -- it keeps
    /// waiting -- so every return is a change the caller should act on.
    pub async fn next_trigger(&mut self) -> ConfigTrigger {
        loop {
            match self.select_trigger().await {
                ConfigTrigger::FileChanged => {
                    if let Some(ref path) = self.path {
                        let current = file_mtime(path).await;
                        let changed = match (&self.last_modified, &current) {
                            (Some(last), Some(now)) => now > last,
                            (None, Some(_)) => true,
                            _ => false,
                        };
                        if changed {
                            self.last_modified = current;
                            return ConfigTrigger::FileChanged;
                        }
                    }
                    // mtime unmoved -- keep waiting.
                }
                other => return other,
            }
        }
    }

    /// Select over every enabled trigger; a disabled one parks forever.
    async fn select_trigger(&mut self) -> ConfigTrigger {
        #[cfg(unix)]
        {
            tokio::select! {
                () = tick(self.poll_timer.as_mut()) => ConfigTrigger::FileChanged,
                () = tick(self.periodic_timer.as_mut()) => ConfigTrigger::Periodic,
                () = async {
                    match self.sighup.as_mut() {
                        Some(sig) => { sig.recv().await; },
                        None => std::future::pending::<()>().await,
                    }
                } => ConfigTrigger::Sighup,
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                () = tick(self.poll_timer.as_mut()) => ConfigTrigger::FileChanged,
                () = tick(self.periodic_timer.as_mut()) => ConfigTrigger::Periodic,
            }
        }
    }
}

/// Await one tick of an optional timer; `None` parks forever.
async fn tick(timer: Option<&mut tokio::time::Interval>) {
    match timer {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Modification time of a file, or `None` when it cannot be read. Async so a
/// poll never blocks the runtime thread.
pub(crate) async fn file_mtime(path: &Path) -> Option<SystemTime> {
    tokio::fs::metadata(path)
        .await
        .ok()
        .and_then(|m| m.modified().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn unarmed_watch_reports_no_triggers() {
        let watch = ConfigWatch::new(None, Duration::from_millis(1));
        assert!(!watch.is_armed());
    }

    #[tokio::test]
    async fn periodic_fires_without_a_file() {
        let mut watch = ConfigWatch::new(None, Duration::from_millis(1))
            .with_periodic(Duration::from_millis(1));
        assert!(watch.is_armed());
        assert_eq!(watch.next_trigger().await, ConfigTrigger::Periodic);
    }

    #[tokio::test]
    async fn file_change_fires_once_per_change() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, "value: 1").expect("write");

        let mut watch = ConfigWatch::new(Some(path.clone()), Duration::from_millis(10));
        watch.prime().await;

        // Rewrite with a later mtime; the poll must see it.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut f = std::fs::File::create(&path).expect("recreate");
        f.write_all(b"value: 2").expect("write");
        drop(f);

        let trigger = tokio::time::timeout(Duration::from_secs(5), watch.next_trigger())
            .await
            .expect("a changed file must trigger");
        assert_eq!(trigger, ConfigTrigger::FileChanged);

        // No further change: the poll must not wake the caller again.
        assert!(
            tokio::time::timeout(Duration::from_millis(120), watch.next_trigger())
                .await
                .is_err(),
            "an unchanged mtime must not resolve next_trigger"
        );
    }

    #[tokio::test]
    async fn file_mtime_reads_existing_and_missing_paths() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("test.txt");
        std::fs::write(&path, "content").expect("write");

        assert!(file_mtime(&path).await.is_some());
        assert!(file_mtime(&dir.path().join("absent.txt")).await.is_none());
    }

    #[tokio::test]
    async fn missing_file_never_triggers() {
        let dir = TempDir::new().expect("tempdir");
        let mut watch = ConfigWatch::new(
            Some(dir.path().join("absent.yaml")),
            Duration::from_millis(5),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(60), watch.next_trigger())
                .await
                .is_err()
        );
    }
}
