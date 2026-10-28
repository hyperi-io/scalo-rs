// Project:   scalo
// File:      src/metrics/readiness.rs
// Purpose:   What /readyz answers: the service phase, the app's check and the health registry
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! The single home of the `/readyz` answer.
//!
//! Both metrics listeners (the hand-rolled responder and the axum router) read
//! [`Readiness::answer`], so they cannot disagree about what ready means.
//!
//! Ready needs all three of:
//!
//! - the service PHASE: a runtime-built service starts in
//!   [`ServicePhase::Starting`] and answers 503 until the runtime hands control
//!   to the app. A standalone [`MetricsManager`](super::MetricsManager) starts
//!   [`Running`](ServicePhase::Running), so nothing changes for one.
//! - the app's readiness check, once the service is running. An idle service
//!   has no work to be ready for, so its check is not consulted.
//! - the [`HealthRegistry`](crate::health::HealthRegistry), with the `health`
//!   feature: no registered component may be `Unhealthy`.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use super::ReadinessFn;

/// Where the service is in its lifecycle, as `/readyz` sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ServicePhase {
    /// Serving work: the app's check and the health registry decide.
    Running = 0,
    /// The runtime is up and the app has not been handed control yet.
    Starting = 1,
    /// Valid config with no work in it: ready, check not consulted.
    Idle = 2,
}

impl ServicePhase {
    const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Starting,
            2 => Self::Idle,
            _ => Self::Running,
        }
    }
}

/// The `/readyz` verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadyAnswer {
    /// 200 `{"status":"ready"}`.
    Ready,
    /// 503 `{"status":"starting"}`: the app has not been handed control yet.
    Starting,
    /// 503 `{"status":"not_ready"}`: the app's check or a health component says no.
    NotReady,
}

impl ReadyAnswer {
    /// `true` for a 200, with the JSON body both listeners send.
    #[must_use]
    pub(crate) const fn wire(self) -> (bool, &'static str) {
        match self {
            Self::Ready => (true, r#"{"status":"ready"}"#),
            Self::Starting => (false, r#"{"status":"starting"}"#),
            Self::NotReady => (false, r#"{"status":"not_ready"}"#),
        }
    }
}

/// The state `/readyz` answers from, shared between the manager and its
/// listener so a check or phase set after the listener started is honoured.
#[derive(Default)]
pub(crate) struct Readiness {
    check: RwLock<Option<ReadinessFn>>,
    phase: AtomicU8,
}

impl Readiness {
    /// Replace the app's readiness check.
    pub(crate) fn set_check(&self, check: ReadinessFn) {
        *self
            .check
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(check);
    }

    /// Move the service to `phase`.
    #[cfg(any(test, feature = "cli-service"))]
    pub(crate) fn set_phase(&self, phase: ServicePhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    /// The service's current phase.
    #[must_use]
    pub(crate) fn phase(&self) -> ServicePhase {
        ServicePhase::from_code(self.phase.load(Ordering::Acquire))
    }

    /// What `/readyz` answers right now.
    #[must_use]
    pub(crate) fn answer(&self) -> ReadyAnswer {
        let app_ready = match self.phase() {
            ServicePhase::Starting => return ReadyAnswer::Starting,
            ServicePhase::Idle => true,
            ServicePhase::Running => self
                .check
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .is_none_or(|check| check()),
        };
        if app_ready && registry_ready() {
            ReadyAnswer::Ready
        } else {
            ReadyAnswer::NotReady
        }
    }
}

#[cfg(feature = "health")]
fn registry_ready() -> bool {
    crate::health::HealthRegistry::is_ready()
}

#[cfg(not(feature = "health"))]
const fn registry_ready() -> bool {
    true
}

/// Shared handle the manager and its listener both hold.
pub(crate) type ReadinessSlot = Arc<Readiness>;

#[cfg(test)]
mod tests {
    use super::*;

    fn state(phase: ServicePhase, check: Option<bool>) -> Readiness {
        let readiness = Readiness::default();
        readiness.set_phase(phase);
        if let Some(value) = check {
            readiness.set_check(Arc::new(move || value));
        }
        readiness
    }

    #[test]
    fn the_answer_follows_phase_then_check() {
        use ReadyAnswer::{NotReady, Ready, Starting};
        use ServicePhase::{Idle, Running};
        let cases = [
            ("starting, no check", ServicePhase::Starting, None, Starting),
            (
                "starting, check true",
                ServicePhase::Starting,
                Some(true),
                Starting,
            ),
            ("idle ignores a false check", Idle, Some(false), Ready),
            ("running, no check", Running, None, Ready),
            ("running, check true", Running, Some(true), Ready),
            ("running, check false", Running, Some(false), NotReady),
        ];
        for (name, phase, check, want) in cases {
            assert_eq!(state(phase, check).answer(), want, "{name}");
        }
    }

    #[test]
    fn a_fresh_state_is_running_so_a_standalone_manager_is_unchanged() {
        assert_eq!(Readiness::default().phase(), ServicePhase::Running);
        assert_eq!(Readiness::default().answer(), ReadyAnswer::Ready);
    }

    #[test]
    fn only_ready_is_a_200() {
        assert!(ReadyAnswer::Ready.wire().0);
        assert!(!ReadyAnswer::Starting.wire().0);
        assert!(!ReadyAnswer::NotReady.wire().0);
        assert_eq!(ReadyAnswer::Starting.wire().1, r#"{"status":"starting"}"#);
    }
}
