// Project:   scalo
// File:      src/scaling/mod.rs
// Purpose:   Scaling pressure calculation for KEDA autoscaling
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Scaling pressure calculation for autoscaler integration.
//!
//! Produces a 0.0-100.0 composite metric based on weighted application
//! signals with two hard gates (circuit breaker, memory pressure).
//! Designed for KEDA but works with any autoscaler that reads Prometheus
//! gauges.
//!
//! ## Architecture
//!
//! ```text
//! App signals --> ScalingPressure::calculate() --> GET /scaling/pressure
//!                  |- Gate: circuit breaker open -> 0.0
//!                  |- Gate: memory >= threshold -> 100.0
//!                  `- Weighted composite -> 0.0-100.0
//! ```
//!
//! `MetricsManager` serves `calculate()` as plain text at `/scaling/pressure`
//! once a pressure is attached with `set_scaling_pressure`, which
//! `ServiceRuntime` does. Nothing copies it into the `scaling_pressure` gauge:
//! an app that scales on the gauge sets it itself, with
//! `ServiceMetrics::scaling_pressure(pressure.calculate())`.
//!
//! ## Usage
//!
//! 1. Define components with weights and saturation points
//! 2. Create `ScalingPressure` with base config + components
//! 3. Update component values from your pipeline (lock-free)
//! 4. Attach it to the `MetricsManager`, and call `calculate()` wherever a
//!    value is wanted: the endpoint, or the gauge
//!
//! ```rust
//! use scalo::scaling::{ScalingPressure, ScalingPressureConfig, ScalingComponent};
//!
//! let pressure = ScalingPressure::new(
//!     ScalingPressureConfig::default(),
//!     vec![
//!         ScalingComponent::new("kafka_lag", 0.35, 100_000.0),
//!         ScalingComponent::new("buffer_depth", 0.25, 10_000.0),
//!         ScalingComponent::new("memory", 0.40, 1.0),
//!     ],
//! );
//!
//! // Update from pipeline (lock-free, call from any thread)
//! pressure.set_component("kafka_lag", 50_000.0);
//! pressure.set_memory(400_000_000, 1_000_000_000);
//!
//! // Render in Prometheus endpoint
//! let value = pressure.calculate();
//! assert!(value >= 0.0 && value <= 100.0);
//! ```
//!
//! ## CPU Scaling
//!
//! CPU is intentionally **not** included in the composite. KEDA's native
//! CPU trigger reads from the Kubernetes metrics-server (container-level
//! CPU utilisation). Configure both triggers independently in your
//! KEDA `ScaledObject`:
//!
//! - `scaling_pressure` gauge, set by the app -> Prometheus scaler
//!   (app-level signals)
//! - CPU utilisation -> CPU scaler (container-level, via metrics-server)
//!
//! KEDA scales to the MAX of all triggers.

mod config;
mod pressure;
mod rate_window;

pub use config::{ScalingComponent, ScalingPressureConfig};
pub use pressure::{ComponentSnapshot, GateType, PressureSnapshot, ScalingPressure};
pub use rate_window::RateWindow;
