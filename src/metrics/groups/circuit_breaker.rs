// Project:   scalo
// File:      src/metrics/groups/circuit_breaker.rs
// Purpose:   Circuit breaker metrics group
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Circuit breaker metrics.

use super::super::MetricsManager;
use super::super::manifest::{MetricDescriptor, MetricType};

/// Circuit breaker metrics for per-target failure tracking.
///
/// State values: 0=closed (healthy), 1=open (failing), 2=half-open (probing).
#[derive(Clone)]
pub struct CircuitBreakerMetrics {
    _private: (),
}

impl CircuitBreakerMetrics {
    #[must_use]
    pub fn new(manager: &MetricsManager) -> Self {
        // BARE names -- the recorder prefix layer and registry apply the namespace.

        // circuit_breaker_state -- label-based, register descriptor manually
        metrics::describe_gauge!(
            "circuit_breaker_state",
            "Circuit breaker state (0=closed, 1=open, 2=half-open)"
        );
        manager.registry().push(MetricDescriptor {
            name: "circuit_breaker_state".into(),
            metric_type: MetricType::Gauge,
            description: "Circuit breaker state (0=closed, 1=open, 2=half-open)".into(),
            unit: String::new(),
            labels: vec!["target".into()],
            group: "circuit_breaker".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        // circuit_breaker_transitions_total -- label-based
        metrics::describe_counter!(
            "circuit_breaker_transitions_total",
            "Circuit breaker state transitions"
        );
        manager.registry().push(MetricDescriptor {
            name: "circuit_breaker_transitions_total".into(),
            metric_type: MetricType::Counter,
            description: "Circuit breaker state transitions".into(),
            unit: String::new(),
            labels: vec!["target".into(), "to_state".into()],
            group: "circuit_breaker".into(),
            buckets: None,
            use_cases: vec![],
            dashboard_hint: None,
        });

        Self { _private: () }
    }

    /// Set circuit breaker state for a target.
    #[inline]
    pub fn set_state(&self, target: &str, state: u8) {
        metrics::gauge!("circuit_breaker_state", "target" => target.to_string())
            .set(f64::from(state));
    }

    /// Record a state transition.
    #[inline]
    pub fn record_transition(&self, target: &str, to_state: &str) {
        metrics::counter!(
            "circuit_breaker_transitions_total",
            "target" => target.to_string(),
            "to_state" => to_state.to_string()
        )
        .increment(1);
    }
}
