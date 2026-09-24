// Project:   scalo
// File:      src/metrics/groups/mod.rs
// Purpose:   DFE-specific metric groups
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Composable DFE metric groups.
//!
//! Opt-in metric structs for DFE pipeline applications. Each group registers
//! standardised metrics with BARE names (e.g. `buffer_bytes`). When the
//! [`MetricsManager`](super::MetricsManager) has a non-empty namespace, the
//! prefix layer on the global recorder and the manifest registry add a single
//! `{namespace}_` prefix uniformly (e.g. `dfe_buffer_bytes`).
//!
//! Feature-gated behind `service-metrics`. Non-DFE apps are unaffected.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use scalo::metrics::{FlushTrigger, MetricsManager, ServiceMetrics};
//! use scalo::metrics::groups::*;
//!
//! let mgr = MetricsManager::new("loader");
//! let svc = ServiceMetrics::register(&mgr);
//! let app = AppMetrics::new(&mgr, env!("CARGO_PKG_VERSION"), "abc123");
//! let buffer = BufferMetrics::new(&mgr);
//! let consumer = ConsumerMetrics::new(&mgr);
//! let sink = SinkMetrics::new(&mgr);
//! let cb = CircuitBreakerMetrics::new(&mgr);
//! let bp = BackpressureMetrics::new(&mgr);
//!
//! // records_received_total is one series; ServiceMetrics counts it.
//! svc.records_received(100);
//! app.record_processed(100);
//! buffer.record_flush(0.042, FlushTrigger::Size);
//! consumer.set_lag("events", 3, 1500);
//! sink.record_duration("clickhouse", 0.015);
//! cb.record_transition("db.events", "open");
//! bp.record_event();
//! ```

mod app;
mod backpressure;
mod buffer;
mod circuit_breaker;
mod consumer;
mod enrichment;
mod schema_cache;
mod sink;

pub use app::AppMetrics;
pub use backpressure::BackpressureMetrics;
pub use buffer::BufferMetrics;
pub use circuit_breaker::CircuitBreakerMetrics;
pub use consumer::ConsumerMetrics;
pub use enrichment::EnrichmentMetrics;
pub use schema_cache::SchemaCacheMetrics;
pub use sink::SinkMetrics;
