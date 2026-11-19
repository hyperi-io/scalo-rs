// Project:   scalo
// File:      src/transport/test_support/mod.rs
// Purpose:   Test and bench fixtures for the send path, behind transport-test-support
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Fixtures for testing and measuring how records reach their destinations,
//! behind the `transport-test-support` feature.
//!
//! - [`shaped`]: a sender whose latency, batching, backpressure and faults are
//!   set by config, with Kafka-like and gRPC-like presets.
//! - [`delivery`]: delivery approaches behind one trait, a registry that names
//!   them, and the leases and outcomes they share.
//!
//! The `delivery_path` bench and `tests/delivery_invariants.rs` are built on
//! both. An app's own tests use the presets so a fixture there and the bench
//! here describe the same destination.

pub mod delivery;
pub mod shaped;
