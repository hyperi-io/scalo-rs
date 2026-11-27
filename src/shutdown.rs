// Project:   scalo
// File:      src/shutdown.rs
// Purpose:   Unified graceful shutdown with global CancellationToken
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Unified graceful shutdown manager.
//!
//! Provides a global [`CancellationToken`] that all modules can listen on
//! for coordinated graceful shutdown. One place handles SIGTERM/SIGINT,
//! all modules drain gracefully.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use scalo::shutdown;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Install the signal handler once at startup
//!     let token = shutdown::install_signal_handler();
//!
//!     // Pass token to workers, or they can call shutdown::token() directly
//!     tokio::spawn(async move {
//!         loop {
//!             tokio::select! {
//!                 _ = token.cancelled() => {
//!                     // drain and exit
//!                     break;
//!                 }
//!                 _ = do_work() => {}
//!             }
//!         }
//!     });
//! }
//!
//! async fn do_work() {
//!     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//! }
//! ```
//!
//! The drain is bounded. A service still running `shutdown.drain_deadline_secs`
//! after the first signal, or one that gets a second signal, exits with what it
//! holds unacknowledged, so its source delivers those records again.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

static TOKEN: OnceLock<CancellationToken> = OnceLock::new();

/// What the exit after a drain deadline needs before SIGKILL: the WARN line, a
/// flush of up to 2 s each for metrics and spans, and the exit itself.
const EXIT_MARGIN: Duration = Duration::from_secs(10);

/// The `shutdown` section of the config cascade.
///
/// ```yaml
/// shutdown:
///   drain_deadline_secs: 35
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct ShutdownConfig {
    /// Seconds from the first SIGTERM or SIGINT, pre-stop delay included, after
    /// which the service stops waiting for its drain and exits with held records
    /// unacknowledged, for their source to deliver again. Unset is the pod's
    /// termination grace less 10 s, and 0 waits for the drain however long it
    /// takes.
    pub(crate) drain_deadline_secs: Option<u64>,
}

impl ShutdownConfig {
    /// Read the `shutdown` section, or its defaults when it is absent.
    ///
    /// # Errors
    ///
    /// [`CascadeError`](crate::cascade::CascadeError) when the section is
    /// present but does not parse.
    pub(crate) fn from_cascade() -> Result<Self, crate::cascade::CascadeError> {
        crate::cascade::section("shutdown")
    }

    /// The drain deadline under a termination grace of `grace`, or `None` for
    /// no deadline. A grace no longer than the exit margin leaves none.
    fn drain_deadline(self, grace: Duration) -> Option<Duration> {
        let deadline = self
            .drain_deadline_secs
            .map_or_else(|| grace.saturating_sub(EXIT_MARGIN), Duration::from_secs);
        (!deadline.is_zero()).then_some(deadline)
    }
}

/// The termination grace of a pod whose deployment contract names none.
pub(crate) fn default_termination_grace() -> Duration {
    Duration::from_secs(u64::from(crate::env::DEFAULT_TERMINATION_GRACE_SECONDS))
}

/// Get the global shutdown token.
///
/// All modules should clone this token and listen for cancellation
/// in their main loops via `token.cancelled().await`.
///
/// The token is created lazily on first access.
pub fn token() -> CancellationToken {
    TOKEN.get_or_init(CancellationToken::new).clone()
}

/// Check if shutdown has been requested.
pub fn is_shutdown() -> bool {
    TOKEN.get().is_some_and(CancellationToken::is_cancelled)
}

/// Trigger shutdown programmatically.
///
/// Cancels the global token. All modules listening on it will
/// begin their drain/cleanup sequence.
pub fn trigger() {
    if let Some(t) = TOKEN.get() {
        t.cancel();
    }
}

/// Wait for SIGTERM or SIGINT, then trigger shutdown.
///
/// Call this once at application startup, from inside a tokio runtime. It
/// listens for the OS signals from the moment it returns and spawns a task
/// that cancels the global token on the first one.
///
/// **K8s pre-stop compliance:** When running in Kubernetes (detected via
/// [`crate::env::runtime_context`]), sleeps for `PRESTOP_DELAY_SECS`
/// (default 5) before cancelling the token. This gives K8s time to
/// remove the pod from Service endpoints before the app starts draining.
/// On bare metal / Docker, the delay is 0 (immediate shutdown).
///
/// **Bounded drain:** the process exits with status 0, with nothing held
/// acknowledged, once `shutdown.drain_deadline_secs` has passed since the
/// first signal (default 35, under a 45 s termination grace), or at once on a
/// second signal. `ServiceRuntime` sets the default from the app's deployment
/// contract instead. The `shutdown` section is read when the first signal
/// arrives, so the handler can be installed before `config::setup`.
///
/// Returns the token for use in `tokio::select!` or other async
/// shutdown coordination.
#[must_use]
pub fn install_signal_handler() -> CancellationToken {
    install_signal_handler_with_grace(default_termination_grace())
}

/// [`install_signal_handler`] for a pod whose termination grace is `grace`,
/// which sets the default drain deadline.
pub(crate) fn install_signal_handler_with_grace(grace: Duration) -> CancellationToken {
    let t = token();
    let cancel = t.clone();
    let mut signals = Signals::listen();

    tokio::spawn(async move {
        signals.next().await;
        let config = ShutdownConfig::from_cascade().unwrap_or_else(|e| {
            tracing::warn!(error = %e, "The shutdown section does not parse, so its defaults apply");
            ShutdownConfig::default()
        });
        if let Some(deadline) = config.drain_deadline(grace) {
            start_drain_deadline(deadline);
        }
        tokio::select! {
            biased;
            () = signals.next() => abandon_drain(DrainEnd::SecondSignal),
            () = begin_drain(&cancel) => {}
        }
        signals.next().await;
        abandon_drain(DrainEnd::SecondSignal);
    });

    t
}

/// Run the pre-stop delay, count the eviction, then cancel `cancel`.
async fn begin_drain(cancel: &CancellationToken) {
    // K8s pre-stop: delay before draining to allow endpoint removal.
    // Without this, K8s routes traffic to a pod that's already shutting down.
    let prestop_delay = prestop_delay_secs();
    if prestop_delay > 0 {
        #[cfg(feature = "logger")]
        tracing::info!(
            delay_secs = prestop_delay,
            "Pre-stop delay: waiting for K8s endpoint removal"
        );
        tokio::time::sleep(Duration::from_secs(prestop_delay)).await;
    }

    // Emit eviction metric when SIGTERM received in K8s
    #[cfg(any(feature = "metrics", feature = "otel-metrics"))]
    if crate::env::runtime_context().is_kubernetes() {
        metrics::counter!("pod_eviction_received_total").increment(1);
    }

    cancel.cancel();

    #[cfg(feature = "logger")]
    tracing::info!("Shutdown signal received, cancelling all tasks");
}

/// What ended a drain before it finished.
#[derive(Debug, Clone, Copy)]
enum DrainEnd {
    /// The drain deadline passed.
    Deadline(Duration),
    /// A second shutdown signal arrived.
    SecondSignal,
}

/// Set by the first [`abandon_drain`], so the WARN line and the flush happen once.
static DRAIN_ABANDONED: AtomicBool = AtomicBool::new(false);

/// End the drain `deadline` after the first signal, from a thread of its own
/// so it fires with every runtime worker stuck.
fn start_drain_deadline(deadline: Duration) {
    let started = std::thread::Builder::new()
        .name("drain-deadline".into())
        .spawn(move || {
            std::thread::sleep(deadline);
            abandon_drain(DrainEnd::Deadline(deadline));
        });
    if let Err(e) = started {
        tracing::error!(error = %e, "Failed to start the drain deadline, so the drain runs until it finishes");
    }
}

/// Log what the drain leaves held, flush telemetry after a deadline, and exit
/// with status 0.
///
/// No destructor runs, so nothing held is released: a Kafka offset stays
/// uncommitted and a push sender gets no answer, and the source delivers those
/// records again. A second signal during the flush exits at once.
fn abandon_drain(end: DrainEnd) -> ! {
    if !DRAIN_ABANDONED.swap(true, Ordering::AcqRel) {
        let (held_records, held_bytes) = held_by_sources();
        match end {
            DrainEnd::Deadline(deadline) => {
                tracing::warn!(
                    deadline_secs = deadline.as_secs(),
                    held_records,
                    held_bytes,
                    "The service did not stop by its drain deadline, so it exits with held \
                     records unacknowledged for their source to deliver again"
                );
                flush_telemetry();
            }
            DrainEnd::SecondSignal => tracing::warn!(
                held_records,
                held_bytes,
                "A second shutdown signal ended the drain, so the service exits with held \
                 records unacknowledged for their source to deliver again"
            ),
        }
    }
    std::process::exit(0)
}

/// Records and bytes the live sources hold unreleased.
fn held_by_sources() -> (u64, u64) {
    #[cfg(feature = "transport")]
    {
        let held = crate::transport::ack::held_by_sources();
        (held.count, held.bytes)
    }
    #[cfg(not(feature = "transport"))]
    (0, 0)
}

/// Flush queued OTLP metrics and spans before the process exits. Each flush is
/// bounded at 2 s, and safe when nothing was wired up.
pub(crate) fn flush_telemetry() {
    #[cfg(feature = "otel-metrics")]
    crate::metrics::shutdown_otel_export();
    #[cfg(feature = "otel-tracing")]
    crate::otel_tracing::shutdown();
}

/// Determine the pre-stop delay in seconds.
///
/// - `PRESTOP_DELAY_SECS` env var overrides (for tuning in deployment manifests)
/// - K8s detected: default 5 seconds
/// - Bare metal / Docker: default 0 (no delay)
fn prestop_delay_secs() -> u64 {
    if let Ok(val) = std::env::var("PRESTOP_DELAY_SECS")
        && let Ok(secs) = val.parse::<u64>()
    {
        return secs;
    }
    if crate::env::runtime_context().is_kubernetes() {
        5
    } else {
        0
    }
}

/// The shutdown signals: SIGTERM and SIGINT, or on Windows Ctrl+C, a console
/// close and a system shutdown. Held from install to exit, so a second signal
/// is seen however soon it follows the first.
struct Signals {
    #[cfg(unix)]
    interrupt: Option<tokio::signal::unix::Signal>,
    #[cfg(unix)]
    terminate: Option<tokio::signal::unix::Signal>,
    #[cfg(windows)]
    ctrl_c: Option<tokio::signal::windows::CtrlC>,
    #[cfg(windows)]
    close: Option<tokio::signal::windows::CtrlClose>,
    #[cfg(windows)]
    shutdown: Option<tokio::signal::windows::CtrlShutdown>,
}

impl Signals {
    /// Start listening. A handler that cannot be installed is logged and left out.
    fn listen() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Self {
                interrupt: signal(SignalKind::interrupt())
                    .inspect_err(
                        |e| tracing::error!(error = %e, "Failed to install Ctrl+C handler"),
                    )
                    .ok(),
                terminate: signal(SignalKind::terminate())
                    .inspect_err(|e| {
                        tracing::error!(
                            error = %e,
                            "Failed to install SIGTERM handler, only Ctrl+C will trigger shutdown"
                        );
                    })
                    .ok(),
            }
        }
        #[cfg(windows)]
        {
            use tokio::signal::windows::{ctrl_c, ctrl_close, ctrl_shutdown};
            Self {
                ctrl_c: ctrl_c()
                    .inspect_err(|e| tracing::error!(error = %e, "Failed to install Ctrl+C handler"))
                    .ok(),
                close: ctrl_close()
                    .inspect_err(|e| {
                        tracing::error!(error = %e, "Failed to install the console close handler");
                    })
                    .ok(),
                shutdown: ctrl_shutdown()
                    .inspect_err(|e| {
                        tracing::error!(error = %e, "Failed to install the system shutdown handler");
                    })
                    .ok(),
            }
        }
        #[cfg(not(any(unix, windows)))]
        Self {}
    }

    /// Wait for the next signal.
    async fn next(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            () = received(self.interrupt.as_mut().map(tokio::signal::unix::Signal::recv)) => {},
            () = received(self.terminate.as_mut().map(tokio::signal::unix::Signal::recv)) => {},
        }
        #[cfg(windows)]
        tokio::select! {
            () = received(self.ctrl_c.as_mut().map(tokio::signal::windows::CtrlC::recv)) => {},
            () = received(self.close.as_mut().map(tokio::signal::windows::CtrlClose::recv)) => {},
            () = received(self.shutdown.as_mut().map(tokio::signal::windows::CtrlShutdown::recv)) => {},
        }
        #[cfg(not(any(unix, windows)))]
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "Failed to install Ctrl+C handler");
            std::future::pending::<()>().await;
        }
    }
}

/// Resolve once `recv` yields a signal. A handler that is missing or has
/// closed never resolves.
async fn received(recv: Option<impl Future<Output = Option<()>>>) {
    if let Some(recv) = recv
        && recv.await.is_some()
    {
        return;
    }
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_not_cancelled_initially() {
        // Use a fresh token (not the global) to avoid test pollution
        let t = CancellationToken::new();
        assert!(!t.is_cancelled());
    }

    #[test]
    fn trigger_cancels_token() {
        let t = CancellationToken::new();
        assert!(!t.is_cancelled());
        t.cancel();
        assert!(t.is_cancelled());
    }

    #[test]
    fn token_is_cloneable_and_shared() {
        let t = CancellationToken::new();
        let c1 = t.clone();
        let c2 = t.clone();

        assert!(!c1.is_cancelled());
        assert!(!c2.is_cancelled());

        t.cancel();

        assert!(c1.is_cancelled());
        assert!(c2.is_cancelled());
    }

    #[test]
    fn multiple_triggers_are_idempotent() {
        let t = CancellationToken::new();
        t.cancel();
        t.cancel(); // second cancel should not panic
        assert!(t.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_future_resolves_after_cancel() {
        let t = CancellationToken::new();
        let c = t.clone();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            c.cancel();
        });

        // This should resolve once the token is cancelled
        t.cancelled().await;
        assert!(t.is_cancelled());
    }

    #[test]
    fn the_drain_deadline_sits_under_the_grace_unless_config_sets_it() {
        let secs = Duration::from_secs;
        let cases = [
            (
                "the default grace",
                None,
                default_termination_grace(),
                Some(secs(35)),
            ),
            ("a contract's longer grace", None, secs(90), Some(secs(80))),
            ("a grace no longer than the margin", None, secs(10), None),
            ("set in config", Some(20), secs(45), Some(secs(20))),
            ("set past the grace", Some(60), secs(45), Some(secs(60))),
            ("0 waits for the drain", Some(0), secs(45), None),
        ];
        for (name, configured, grace, want) in cases {
            let config = ShutdownConfig {
                drain_deadline_secs: configured,
            };
            assert_eq!(config.drain_deadline(grace), want, "{name}");
        }
    }

    #[tokio::test]
    async fn child_token_cancelled_by_parent() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        assert!(!child.is_cancelled());
        parent.cancel();
        assert!(child.is_cancelled());
    }
}
