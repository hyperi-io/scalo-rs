// Project:   scalo
// File:      src/lifecycle/mod.rs
// Purpose:   Idle-until-configured gate for services deployed before they have work
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Idle until configured.
//!
//! A data-plane app is often deployed before anything has given it work: an
//! archiver with no archived source, a fetcher with no sources, a transform
//! whose source has not been written yet. Such an app must START, pass
//! readiness, serve health and metrics, hold no broker or endpoint connection
//! it has no use for, and pick up the first config that gives it work -- rather
//! than refuse to boot and crash-loop with no probe surface.
//!
//! That is ONE mechanism, here, not per-app tolerance of an empty config.
//!
//! ## The split every app makes
//!
//! | Config is | Behaviour |
//! |---|---|
//! | structurally invalid (bad address, unparseable, contradictory) | refuse loudly, exit non-zero |
//! | valid but EMPTY of work (no sources, no topics, no destination) | [`WorkState::Idle`] -- start, stay Ready, wait |
//!
//! The emptiness predicate belongs to the app (it is the only thing that knows
//! what "no work" means); the behaviour belongs to scalo. An app names its
//! predicate by implementing `ServiceApp::work_state` (`cli-service` feature).
//!
//! ## What idle looks like from outside
//!
//! - `/livez` and `/readyz`: serving, and READY. With the `health` feature a
//!   `work_config` component is registered `Degraded` -- ready, not healthy --
//!   so a deploy's readiness gate passes while an operator (and `/healthz`)
//!   can still see the app has nothing to do.
//! - `pipeline_idle` gauge: 1 while idle, 0 once work arrives.
//! - No transport is constructed, so no broker connection, no consumer group,
//!   no listener socket.
//!
//! The gate then parks on the config file (mtime poll and SIGHUP, via
//! [`ConfigWatch`](crate::config::watch::ConfigWatch)) and re-evaluates the
//! app's predicate on every change, so the first config that gives the app work
//! starts it without a restart.

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// How often the gate re-reads the config file while parked. Matches the
/// reloader's default poll so an idle app and a running one see a change at the
/// same rate; SIGHUP short-circuits it either way.
pub const IDLE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Name of the health component the gate registers.
pub const WORK_COMPONENT: &str = "work_config";

/// Whether an app's configuration currently gives it work to do.
///
/// Returned by `ServiceApp::work_state` (`cli-service` feature).
/// The default for every app is [`Active`](WorkState::Active): an app that has
/// not named an emptiness predicate behaves exactly as it did before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkState {
    /// The config names work -- run the service.
    Active,
    /// The config is valid but empty of work. The string is the operator-facing
    /// reason, logged and reported on `/healthz`.
    Idle(Cow<'static, str>),
}

impl WorkState {
    /// Idle with a reason, e.g. `WorkState::idle("no enabled sources")`.
    #[must_use]
    pub fn idle(reason: impl Into<Cow<'static, str>>) -> Self {
        Self::Idle(reason.into())
    }

    /// [`Idle`](WorkState::Idle) when `empty` holds, else
    /// [`Active`](WorkState::Active) -- the shape almost every predicate takes.
    #[must_use]
    pub fn idle_if(empty: bool, reason: impl Into<Cow<'static, str>>) -> Self {
        if empty {
            Self::idle(reason)
        } else {
            Self::Active
        }
    }

    /// Whether this state is idle.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle(_))
    }

    /// The idle reason, or `None` when active.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Active => None,
            Self::Idle(reason) => Some(reason),
        }
    }
}

/// Idle is [`Degraded`](crate::health::HealthStatus::Degraded) -- READY but not
/// healthy -- so a deploy's readiness gate passes an app with nothing to do
/// while `/healthz` still shows why it is not working.
#[cfg(feature = "health")]
#[must_use]
fn work_health(idle: bool) -> crate::health::HealthStatus {
    if idle {
        crate::health::HealthStatus::Degraded
    } else {
        crate::health::HealthStatus::Healthy
    }
}

/// Why the gate stopped waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateWake {
    /// The configuration changed -- re-evaluate the work predicate.
    ConfigChanged,
    /// Shutdown was requested while idle -- exit cleanly, do not start work.
    ShuttingDown,
}

/// The idle gate: health component, `pipeline_idle` gauge, and the park.
///
/// One per process, held by `run_app` (`cli-service` feature) across the
/// work-state loop. The health
/// component is registered on the FIRST idle only: an app that never idles adds
/// nothing to `/healthz`.
pub struct IdleGate {
    idle: Arc<AtomicBool>,
    registered: bool,
}

impl Default for IdleGate {
    fn default() -> Self {
        Self::new()
    }
}

impl IdleGate {
    /// A gate in the active state, with nothing registered yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            idle: Arc::new(AtomicBool::new(false)),
            registered: false,
        }
    }

    /// Whether the gate currently reports idle.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.idle.load(Ordering::Relaxed)
    }

    /// Enter the idle state: flip the gauge, register the health component the
    /// first time, and log the reason once per transition.
    pub fn enter_idle(&mut self, reason: &str) {
        if self.idle.swap(true, Ordering::Relaxed) {
            return;
        }
        set_idle_gauge(true);

        if !self.registered {
            self.registered = true;
            #[cfg(feature = "health")]
            {
                let idle = Arc::clone(&self.idle);
                crate::health::HealthRegistry::register(WORK_COMPONENT, move || {
                    work_health(idle.load(Ordering::Relaxed))
                });
            }
        }

        tracing::warn!(
            reason,
            "no work configured -- idling until the configuration gives this service work"
        );
    }

    /// Leave the idle state: flip the gauge back and log the transition.
    pub fn leave_idle(&mut self) {
        if !self.idle.swap(false, Ordering::Relaxed) {
            return;
        }
        set_idle_gauge(false);
        tracing::info!("configuration now names work -- starting the service");
    }

    /// The status the registered health component reports right now.
    #[cfg(feature = "health")]
    #[must_use]
    pub fn health_status(&self) -> crate::health::HealthStatus {
        work_health(self.is_idle())
    }
}

/// Publish the idle gauge: 1 while the service has no work, 0 once it has.
fn set_idle_gauge(idle: bool) {
    #[cfg(feature = "metrics")]
    metrics::gauge!("pipeline_idle").set(if idle { 1.0 } else { 0.0 });
    #[cfg(not(feature = "metrics"))]
    let _ = idle;
}

/// Park until the configuration changes or shutdown is requested.
///
/// `config_path` is the file the service was started with; with no file the
/// gate still waits on SIGHUP and shutdown, so an env-only deployment can be
/// nudged rather than restarted.
pub async fn wait_for_config_change(config_path: Option<&std::path::Path>) -> GateWake {
    use crate::config::watch::ConfigWatch;

    let mut watch = ConfigWatch::new(
        config_path.map(std::path::Path::to_path_buf),
        IDLE_POLL_INTERVAL,
    )
    .with_sighup(true);
    watch.prime().await;

    #[cfg(feature = "shutdown")]
    {
        let token = crate::shutdown::token();
        tokio::select! {
            _ = watch.next_trigger() => GateWake::ConfigChanged,
            () = token.cancelled() => GateWake::ShuttingDown,
        }
    }
    #[cfg(not(feature = "shutdown"))]
    {
        watch.next_trigger().await;
        GateWake::ConfigChanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_state_helpers() {
        assert_eq!(WorkState::idle_if(false, "unused"), WorkState::Active);
        let idle = WorkState::idle_if(true, "no enabled sources");
        assert!(idle.is_idle());
        assert_eq!(idle.reason(), Some("no enabled sources"));
        assert_eq!(WorkState::Active.reason(), None);
    }

    #[test]
    fn gate_transitions_are_idempotent() {
        let mut gate = IdleGate::new();
        assert!(!gate.is_idle());

        gate.enter_idle("no sources");
        gate.enter_idle("no sources");
        assert!(gate.is_idle());

        gate.leave_idle();
        gate.leave_idle();
        assert!(!gate.is_idle());
    }

    /// The gate's own status, not the global registry's: a shared-static
    /// assertion would race the health module's own tests.
    #[cfg(feature = "health")]
    #[test]
    fn idle_is_ready_but_not_healthy() {
        use crate::health::HealthStatus;

        let mut gate = IdleGate::new();
        assert_eq!(gate.health_status(), HealthStatus::Healthy);

        gate.enter_idle("no sources");
        assert_eq!(
            gate.health_status(),
            HealthStatus::Degraded,
            "idle must be Ready (Degraded), never Unhealthy -- a readiness gate fails the deploy"
        );

        gate.leave_idle();
        assert_eq!(gate.health_status(), HealthStatus::Healthy);
    }
}
